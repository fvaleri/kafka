/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.mirror;

import kafka.server.KafkaConfig;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StopMirrorTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.MirrorConfigNotAvailableException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig.MirrorAclRule;
import org.apache.kafka.coordinator.mirror.MirrorMetadataCache;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import com.google.re2j.Pattern;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Periodically syncs source cluster metadata on every broker.
 * Created by MirrorMetadataManager.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
class MirrorSourceSyncer {
    private final Logger log;
    private final KafkaConfig brokerConfig;
    private final int nodeId;

    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private final NodeToControllerChannelManager controllerClient;
    private final MirrorMetadataCache mirrorCache;
    private final TransitionHandler transitionHandler;
    private final Consumer<String> tombstoneWriter;
    private final EpochBumpHandler epochBumpHandler;

    private final KafkaScheduler periodicScheduler;
    private final KafkaScheduler onDemandScheduler;
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private volatile ScheduledFuture<?> periodicTaskSchedule;

    private final ConcurrentHashMap<String, CompletableFuture<List<SourceTopicMetadata>>>
            pendingTopicMetadataSyncs = new ConcurrentHashMap<>();
    private final Set<String> pendingTopicCreations = ConcurrentHashMap.newKeySet();
    private final Map<String, Set<String>> sourceDeletions = new ConcurrentHashMap<>();

    private volatile Map<String, Admin> srcAdmins;
    private volatile Admin dstAdmin;

    private final KafkaMetricsGroup metricsGroup;
    private final Meter metadataRefreshError;
    private final Meter topicConfigSyncError;
    private final Meter consumerGroupOffsetSyncError;
    private final Meter shareGroupOffsetSyncError;
    private final Meter aclSyncError;

    MirrorSourceSyncer(
        KafkaConfig brokerConfig,
        Supplier<ReplicaManager> replicaManagerSupplier,
        NodeToControllerChannelManager controllerClient,
        MirrorMetadataCache mirrorCache,
        TransitionHandler transitionHandler,
        Consumer<String> tombstoneWriter,
        EpochBumpHandler epochBumpHandler
    ) {
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        String name = "[" + MirrorSourceSyncer.class.getSimpleName() + " brokerId=" + nodeId + "] ";
        this.log = new LogContext(name).logger(MirrorSourceSyncer.class);

        this.replicaManagerSupplier = replicaManagerSupplier;
        this.controllerClient = controllerClient;
        this.mirrorCache = mirrorCache;
        this.transitionHandler = transitionHandler;
        this.tombstoneWriter = tombstoneWriter;
        this.epochBumpHandler = epochBumpHandler;

        this.periodicScheduler = new KafkaScheduler(1, true, "mirror-syncer-per");
        this.periodicScheduler.startup();

        this.onDemandScheduler = new KafkaScheduler(1, true, "mirror-syncer-ond-");
        this.onDemandScheduler.startup();

        this.metricsGroup = new KafkaMetricsGroup(MirrorSourceSyncer.class);
        this.metadataRefreshError = metricsGroup.newMeter("TopicMetadataRefreshError", "errors", TimeUnit.SECONDS);
        this.topicConfigSyncError = metricsGroup.newMeter("TopicConfigSyncError", "errors", TimeUnit.SECONDS);
        this.consumerGroupOffsetSyncError = metricsGroup.newMeter("ConsumerGroupOffsetSyncError", "errors", TimeUnit.SECONDS);
        this.shareGroupOffsetSyncError = metricsGroup.newMeter("ShareGroupOffsetSyncError", "errors", TimeUnit.SECONDS);
        this.aclSyncError = metricsGroup.newMeter("AclSyncError", "errors", TimeUnit.SECONDS);
    }

    void startup() {
        scheduleSourceClusterMetadataSync(brokerConfig.mirrorConfig().metadataRefreshIntervalMs());
    }

    void shutdown() {
        try {
            periodicScheduler.shutdown();
            onDemandScheduler.shutdown();
            pendingTopicMetadataSyncs.clear();
            pendingTopicCreations.clear();
            sourceDeletions.clear();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down sync schedulers", e);
        }
        if (srcAdmins != null) {
            srcAdmins.values().forEach(admin -> admin.close(Duration.ZERO));
        }
        if (dstAdmin != null) {
            dstAdmin.close(Duration.ZERO);
        }
        metricsGroup.removeMetric("TopicMetadataRefreshError");
        metricsGroup.removeMetric("TopicConfigSyncError");
        metricsGroup.removeMetric("ConsumerGroupOffsetSyncError");
        metricsGroup.removeMetric("ShareGroupOffsetSyncError");
        metricsGroup.removeMetric("AclSyncError");
    }

    void updateMetadataImage(MetadataImage newImage) {
        this.metadataImage = newImage;
    }

    private boolean isSyncLeaderFor(String mirrorName) {
        MetadataImage image = metadataImage;
        if (image.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null) {
            int partition = Utils.abs(mirrorName.hashCode())
                    % brokerConfig.mirrorConfig().stateTopicNumPartitions();
            int leader = image.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(partition).leader;
            return leader == nodeId;
        }
        return false;
    }

    Admin getOrCreateSourceAdmin(String mirrorName) {
        if (srcAdmins == null) {
            srcAdmins = new ConcurrentHashMap<>();
        }
        return srcAdmins.computeIfAbsent(mirrorName, k -> {
            Properties props = mirrorCache.getResourceConfig(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, k));
            if (!props.containsKey(BOOTSTRAP_SERVERS_CONFIG)) {
                throw new MirrorConfigNotAvailableException(BOOTSTRAP_SERVERS_CONFIG + " is not available");
            }
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-syncer-src-" + k + "-" + nodeId);
            return Admin.create(props);
        });
    }

    void closeAndRemoveSourceAdmins() {
        if (srcAdmins != null) {
            srcAdmins.keySet().forEach(this::closeAndRemoveSourceAdmins);
        }
    }

    void closeAndRemoveSourceAdmins(String mirrorName) {
        if (srcAdmins != null) {
            Admin admin = srcAdmins.remove(mirrorName);
            if (admin != null) {
                admin.close(Duration.ZERO);
            }
        }
    }

    private Admin getOrCreateDestAdmin() {
        if (dstAdmin == null) {
            Properties props = buildDestAdminClientProps(brokerConfig);
            if (props.getProperty(BOOTSTRAP_SERVERS_CONFIG).endsWith(":0")) {
                ListenerName listenerName = brokerConfig.mirrorAdminListenerName();
                mirrorCache.getAliveBrokerNode(nodeId, listenerName).ifPresent(node ->
                        props.put(BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port()));
            }
            dstAdmin = Admin.create(props);
        }
        return dstAdmin;
    }

    private Properties buildDestAdminClientProps(KafkaConfig brokerConfig) {
        ListenerName mirrorAdminListener = brokerConfig.mirrorAdminListenerName();
        Endpoint endpoint = (Endpoint) brokerConfig.effectiveAdvertisedBrokerListeners()
                .filter(e -> e.listener().equals(mirrorAdminListener.value()))
                .head();

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-sync-dst-" + brokerConfig.nodeId());

        SecurityProtocol securityProtocol = endpoint.securityProtocol();
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);

        Map<String, ?> configs = ChannelBuilders.channelBuilderConfigs(brokerConfig, mirrorAdminListener);

        // Get all the security configs
        ConfigDef securityConfigDef = new ConfigDef().withClientSaslSupport().withClientSslSupport();
        Set<String> securityConfigs = new HashSet<>(securityConfigDef.configKeys().keySet());

        String mirrorAdminSaslMechanism = brokerConfig.saslMechanismMirrorAdminProtocol();
        if (securityProtocol == SecurityProtocol.SASL_SSL || securityProtocol == SecurityProtocol.SASL_PLAINTEXT) {
            props.put(SaslConfigs.SASL_MECHANISM, mirrorAdminSaslMechanism);
        }

        String saslMechanismConfigPrefix = mirrorAdminListener.saslMechanismConfigPrefix(mirrorAdminSaslMechanism);
        Map<String, ?> saslMechanismConfigs = brokerConfig.originalsWithPrefix(saslMechanismConfigPrefix, true);

        securityConfigs.forEach(key -> {
            if (key.equals(SaslConfigs.SASL_MECHANISM)) return;
            if (saslMechanismConfigs.containsKey(key)) {
                props.put(key, saslMechanismConfigs.get(key));
            } else if (configs.containsKey(key)) {
                Object value = configs.get(key);
                if (value == null) {
                    return;
                }
                props.put(key, value);
            }
        });

        return props;
    }

    private void updateMirrorTopicMetrics(String mirrorName) {
        metricsGroup.newGauge("MirrorTopicCount",
                () -> mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING)).size(),
                Map.of("mirrorName", mirrorName));
    }

    void removeMirrorTopicMetrics(String mirrorName) {
        metricsGroup.removeMetric("MirrorTopicCount", Map.of("mirrorName", mirrorName));
    }

    private List<String> listSourceGroupIds(Admin srcAdmin, ListGroupsOptions options,
                                            Pattern groupsIncludePattern, Pattern groupsExcludePattern) throws Exception {
        return srcAdmin.listGroups(options).all()
                .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS).stream()
                .map(GroupListing::groupId)
                .filter(id -> groupsIncludePattern == null || groupsIncludePattern.matcher(id).matches())
                .filter(id -> groupsExcludePattern == null || !groupsExcludePattern.matcher(id).matches())
                .toList();
    }

    // ==== SCHEDULING =================================================================================================

    /** Schedules an on-demand source topic metadata sync for the given mirror. */
    void scheduleOnceSourceTopicMetadataSync(String mirrorName) {
        if (!pendingTopicMetadataSyncs.containsKey(mirrorName)) {
            onDemandScheduler.scheduleOnce("source-topic-metadata-sync",
                    () -> syncSourceTopicMetadata(mirrorName));
        }
    }

    /** Schedules the periodic source metadata sync at the given interval. */
    void scheduleSourceClusterMetadataSync(long intervalMs) {
        ScheduledFuture<?> oldFuture = periodicTaskSchedule;
        if (oldFuture != null) {
            oldFuture.cancel(false);
        }
        periodicTaskSchedule = periodicScheduler.schedule("source-cluster-sync",
                this::doSourceClusterMetadataSync, intervalMs, intervalMs);
        log.info("Scheduled source cluster sync with interval {} ms", intervalMs);
    }

    // ==== OPERATIONS =================================================================================================

    /** Runs a full source cluster metadata sync. */
    private void doSourceClusterMetadataSync() {
        retryPendingTombstoneWrites();

        Set<String> mirrors = mirrorCache.getMirrorNames();
        if (mirrors.isEmpty()) {
            return;
        }

        log.info("Running metadata update for mirrors {}", mirrors);

        for (String mirrorName : mirrors) {
            try {
                validateSourceClusterId(mirrorName);
                updateMirrorTopicMetrics(mirrorName);
                var topicState = syncSourceTopicMetadata(mirrorName);
                syncSourceTopicConfigsGroupsAcls(mirrorName, topicState);
            } catch (Exception e) {
                log.error("Failed to refresh metadata for mirror {}", mirrorName, e);
            }
        }
    }

    /**
     * Syncs topics metadata from the source cluster.
     * Runs on every broker.
     */
    List<SourceTopicMetadata> syncSourceTopicMetadata(String mirrorName) {
        log.info("Syncing source topic metadata for mirror {}", mirrorName);
        var future = new CompletableFuture<List<SourceTopicMetadata>>();
        var existing = pendingTopicMetadataSyncs.putIfAbsent(mirrorName, future);
        if (existing != null) {
            log.info("Source topic metadata sync in progress for mirror {}, waiting for result", mirrorName);
            try {
                return existing.get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                log.warn("Source topic metadata sync failed for mirror {}", mirrorName, e.getCause());
                return List.of();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            } catch (TimeoutException e) {
                log.warn("Timed out waiting for source topic metadata sync for mirror {}", mirrorName);
                return List.of();
            }
        }
        try {
            var result = doSyncSourceTopicMetadata(mirrorName);
            future.complete(result);
            return result;
        } catch (Exception e) {
            future.completeExceptionally(e);
            return List.of();
        } finally {
            pendingTopicMetadataSyncs.remove(mirrorName, future);
        }
    }

    /**
     * Syncs topic configs, group offsets and ACLs from the source cluster.
     * Runs only on the coordinator broker.
     */
    private void syncSourceTopicConfigsGroupsAcls(String mirrorName, List<SourceTopicMetadata> sourceTopicMetadata) {
        if (!isSyncLeaderFor(mirrorName)) {
            return;
        }

        try {
            log.info("Syncing source topic configs, groups offsets and ACLs for mirror {}", mirrorName);
            ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(
                    mirrorCache.getResourceConfig(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName)));
            syncTopicConfigs(mirrorName, mirrorConfig);
            syncGroupOffsets(mirrorName, mirrorConfig);
            syncAcls(mirrorName, mirrorConfig);
            if (!mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING)).isEmpty()) {
                epochBumpHandler.maybeBumpLeaderEpochs(mirrorName, sourceTopicMetadata, Set.of());
            }
            discoverTopicsByPattern(mirrorName, mirrorConfig);
            enforceExcludePatterns(mirrorName, mirrorConfig);
        } catch (Exception e) {
            log.error("Failed to sync source configs and offsets for mirror {}", mirrorName, e);
            metadataRefreshError.mark();
        }
    }

    private void retryPendingTombstoneWrites() {
        Set<String> configuredMirrors = mirrorCache.getMirrorNames();
        Set<String> staleMirrors = mirrorCache.getMirrorPartitions(false).stream()
                .map(MirrorPartition::mirrorName)
                .filter(name -> !configuredMirrors.contains(name))
                .collect(Collectors.toSet());
        for (String mirrorName : staleMirrors) {
            log.info("Found stale partition states for deleted mirror {}. Writing tombstones.", mirrorName);
            tombstoneWriter.accept(mirrorName);
            metricsGroup.removeMetric("MirrorTopicCount", Map.of("mirrorName", mirrorName));
        }
    }

    private void validateSourceClusterId(String mirrorName) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);
        try {
            var clusterResult = srcAdmin.describeCluster();
            String newClusterId = clusterResult.clusterId().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            if (newClusterId != null && !newClusterId.isEmpty()) {
                String previousClusterId = mirrorCache.getSourceClusterId(mirrorName);
                if (previousClusterId != null && !previousClusterId.equals(newClusterId)) {
                    String errMsg = "Source cluster ID changed for mirror " + mirrorName
                            + ": expected " + previousClusterId + ", got " + newClusterId
                            + ". This may indicate a misconfiguration or that the source cluster has been replaced. "
                            + "Moving all partitions to non-retryable failed state.";
                    log.error(errMsg);

                    Set<String> mirrorTopics = mirrorCache.getMirrorTopics(mirrorName,
                            EnumSet.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED));
                    if (!mirrorTopics.isEmpty()) {
                        Set<TopicPartition> mirrorLeaderPartitions = new HashSet<>();
                        for (String topic : mirrorTopics) {
                            TopicImage topicImage = metadataImage.topics().getTopic(topic);
                            if (topicImage != null) {
                                topicImage.partitions().forEach((partitionId, partition) -> {
                                    if (partition.leader == nodeId) {
                                        MirrorPartition key = new MirrorPartition(mirrorName, topicImage.id(),  partitionId);
                                        MirrorPartitionMetadata mpm = MirrorPartitionMetadata.orEmpty(mirrorCache.getPartitionMetadata(key, false));
                                        if (mpm.state() == MirrorPartitionState.FAILED && mpm.retryAttempt() == MirrorPartitionMetadata.NON_RETRYABLE_ERROR) {
                                            log.debug("Skipping transition to FAILED state for partition {}-{} . Reason: Already in this state due to: {}.",
                                                    topic, partitionId, mpm.errorMessage());
                                        } else {
                                            mirrorLeaderPartitions.add(new TopicPartition(topic, partitionId));
                                        }
                                    }
                                });
                            }
                        }
                        if (!mirrorLeaderPartitions.isEmpty()) {
                            transitionHandler.transitionTo(mirrorName, mirrorLeaderPartitions, MirrorPartitionState.FAILED, errMsg, true);
                        }
                    }
                }
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Failed to describe source cluster for mirror {}", mirrorName, e);
            metadataRefreshError.mark();
        }
    }

    private List<SourceTopicMetadata> doSyncSourceTopicMetadata(String mirrorName) {
        Set<String> topics = mirrorCache.getMirrorTopics(mirrorName,
                EnumSet.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED));
        if (topics.isEmpty()) {
            return List.of();
        }

        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        try {
            Map<String, KafkaFuture<TopicDescription>> futures = srcAdmin.describeTopics(topics).topicNameValues();
            List<SourceTopicMetadata> result = new ArrayList<>();

            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                try {
                    TopicDescription td = entry.getValue().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    List<SourcePartitionState> partitions = td.partitions().stream()
                            .map(pi -> new SourcePartitionState(
                                    new TopicPartition(td.name(), pi.partition()),
                                    pi.leader(),
                                    pi.leaderEpoch()))
                            .toList();
                    result.add(new SourceTopicMetadata(td.name(), td.topicId(), true, partitions));
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        result.add(new SourceTopicMetadata(entry.getKey(), Uuid.ZERO_UUID, false, List.of()));
                    } else {
                        log.warn("Failed to describe topic {}", entry.getKey(), e);
                    }
                }
            }

            processSourceTopicMetadata(mirrorName, result);
            return result;
        } catch (Exception e) {
            log.warn("Failed to sync source topic state for mirror {}", mirrorName, e);
            metadataRefreshError.mark();
            return List.of();
        }
    }

    private void processSourceTopicMetadata(String mirrorName, List<SourceTopicMetadata> sourceTopicMetadata) {
        var creatableTopics = new ArrayList<CreateTopicsRequestData.CreatableTopic>();
        var createPartitionsTopics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();

        sourceTopicMetadata.forEach(ti -> {
            int sourcePartitionCount = ti.partitions().size();

            ti.partitions().forEach(pi -> {
                if (pi.leader() != null) {
                    mirrorCache.updateSourceClusterLeader(mirrorName, pi.topicPartition(),
                            new MirrorMetadataCache.SourceClusterLeader(Optional.of(pi.leader()), pi.leaderEpoch().orElse(0)));
                }
            });

            // Pre-KIP-516 sources (Kafka < 2.8) return ZERO_UUID; fall back to name-based lookup.
            // Name-based lookup cannot detect topic delete-and-recreate on the source. If this
            // happens, the operator must stop mirroring and delete the destination topic manually.
            TopicImage destTopic = !ti.topicId().equals(Uuid.ZERO_UUID)
                    ? metadataImage.topics().getTopic(ti.topicId())
                    : metadataImage.topics().getTopic(ti.topic());

            if (destTopic != null && destTopic.partitions().size() < sourcePartitionCount) {
                createPartitionsTopics.add(new CreatePartitionsRequestData.CreatePartitionsTopic()
                        .setName(ti.topic())
                        .setCount(sourcePartitionCount)
                        .setAssignments(null)
                );
            } else if (destTopic == null &&
                    metadataImage.topics().getTopic(ti.topic()) == null &&
                    ti.exists() && sourcePartitionCount > 0) {
                if (pendingTopicCreations.add(ti.topic())) {
                    creatableTopics.add(new CreateTopicsRequestData.CreatableTopic()
                            .setName(ti.topic())
                            .setNumPartitions(sourcePartitionCount)
                            .setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR)
                            .setMirrorInfo(new CreateTopicsRequestData.MirrorInfo().setTopicId(
                                    ti.topicId().equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : ti.topicId())));
                }
            } else if (destTopic == null &&
                    metadataImage.topics().getTopic(ti.topic()) != null &&
                    ti.exists()) {
                log.error("Topic {} exists on destination cluster with ID {} but source cluster has ID {}. "
                                + "Delete the topic on destination and let auto-creation recreate it with the correct ID.",
                        ti.topic(), metadataImage.topics().getTopic(ti.topic()).id(), ti.topicId());
            }
        });

        if (isSyncLeaderFor(mirrorName)) {
            if (!creatableTopics.isEmpty()) {
                createMirrorTopics(creatableTopics);
            }
            maybeScalePartitions(createPartitionsTopics);
        }

        maybeFailDeletedTopics(mirrorName, sourceTopicMetadata);
    }

    /**
     * Creates mirror topics on the destination with the source's TopicId,
     * preserving topic identity across clusters. Called during periodic metadata
     * sync when topics have mirror.name config but don't exist on the destination yet.
     * Once created, onMetadataUpdate will detect them and start the mirror state machine.
     */
    private void createMirrorTopics(List<CreateTopicsRequestData.CreatableTopic> creatableTopics) {
        var topicNames = creatableTopics.stream().map(CreateTopicsRequestData.CreatableTopic::name).toList();
        creatableTopics.forEach(t -> log.info("Creating mirror topic {} on destination (partitions={}, topicId={})",
                t.name(), t.numPartitions(), t.mirrorInfo().topicId()));
        var createTopicsData = new CreateTopicsRequestData().setTimeoutMs(brokerConfig.requestTimeoutMs());
        creatableTopics.forEach(createTopicsData.topics()::add);
        ControllerRequestCompletionHandler requestCompletionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                topicNames.forEach(name -> pendingTopicCreations.remove(name));
                log.warn("Create mirror topics timed out for {}", topicNames);
                metadataRefreshError.mark();
            }

            @Override
            public void onComplete(ClientResponse response) {
                topicNames.forEach(name -> pendingTopicCreations.remove(name));
                if (response.versionMismatch() != null || response.authenticationException() != null || response.wasDisconnected()) {
                    metadataRefreshError.mark();
                    return;
                }
                if (response.responseBody() instanceof CreateTopicsResponse createTopicsResponse) {
                    createTopicsResponse.data().topics().forEach(topic -> {
                        var error = Errors.forCode(topic.errorCode());
                        if (error != Errors.NONE) {
                            log.warn("Failed to create mirror topic {}: {}", topic.name(), error.message());
                            metadataRefreshError.mark();
                        }
                    });
                }
            }
        };
        controllerClient.sendRequest(
                new CreateTopicsRequest.Builder(createTopicsData),
                requestCompletionHandler);
    }

    private void maybeScalePartitions(CreatePartitionsRequestData.CreatePartitionsTopicCollection topics) {
        if (!topics.isEmpty()) {
            log.debug("Detected partition count change, sending CreatePartitionsRequest: {}", topics);
            controllerClient.sendRequest(new CreatePartitionsRequest.Builder(
                    new CreatePartitionsRequestData()
                            .setTopics(topics)
                            .setValidateOnly(false)
                            .setTimeoutMs(3000)
            ), new TimeoutHandler(log, metadataRefreshError));
        }
    }

    /*
     * Cross-checks describeTopics (per topic) with listTopics (cluster-wide) to detect
     * deleted source topics. Requires two consecutive observations to avoid false positives
     * during transient metadata unavailability (e.g. unclean leader election on ZK sources).
     */
    private void maybeFailDeletedTopics(String mirrorName, List<SourceTopicMetadata> sourceTopicMetadata) {
        List<String> deletedSourceTopicNames = new ArrayList<>(sourceTopicMetadata.stream()
                .filter(ti -> !ti.exists())
                .map(SourceTopicMetadata::topic).toList());

        if (deletedSourceTopicNames.isEmpty()) {
            return;
        }

        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);
        try {
            Set<String> allSourceTopics = srcAdmin.listTopics().names().get();
            log.debug("Source topic name list: {}", allSourceTopics);
            deletedSourceTopicNames.removeAll(allSourceTopics);
        } catch (Exception e) {
            log.warn("Failed to list topics for mirror {}, skipping deleted topic detection: {}", mirrorName, e.getMessage());
            metadataRefreshError.mark();
            return;
        }

        Set<String> allMirrorTopics = mirrorCache.getMirrorTopics(mirrorName,
                EnumSet.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED));
        allMirrorTopics.forEach(name -> {
            if (deletedSourceTopicNames.contains(name)) {
                Set<String> topics = sourceDeletions.get(mirrorName);
                if (topics != null && topics.contains(name)) {
                    topics.remove(name);
                    TopicImage topicImage = metadataImage.topics().getTopic(name);
                    if (topicImage != null) {
                        topicImage.partitions().forEach((partitionId, partition) -> {
                            if (partition.leader == nodeId) {
                                MirrorPartition key = new MirrorPartition(mirrorName, topicImage.id(), partitionId);
                                MirrorPartitionMetadata mpm = MirrorPartitionMetadata.orEmpty(mirrorCache.getPartitionMetadata(key, false));
                                if (mpm.state() == MirrorPartitionState.FAILED && mpm.retryAttempt() == MirrorPartitionMetadata.NON_RETRYABLE_ERROR) {
                                    log.debug("Skipping transition to FAILED state for partition {}. Already in this state due to: {}", name, partition, mpm.errorMessage());
                                } else {
                                    log.info("Detected topic {} deleted in source cluster {}, marking partitions as failed (non retryable).", name, mirrorName);
                                    transitionHandler.transitionTo(mirrorName, Set.of(new TopicPartition(name, partitionId)),
                                            MirrorPartitionState.FAILED, "The source topic is deleted", true);
                                }
                            }
                        });
                    }
                } else {
                    log.debug("Topic {} not found in source cluster {}, pending deletion confirmation on next sync", name, mirrorName);
                    sourceDeletions.computeIfAbsent(mirrorName, k -> ConcurrentHashMap.newKeySet()).add(name);
                }
            } else {
                Set<String> topics = sourceDeletions.get(mirrorName);
                if (topics != null) {
                    topics.remove(name);
                }
            }
        });
    }

    private void syncTopicConfigs(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        Set<String> topics = mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING));
        log.debug("Describing topic configs for topics {}", topics);

        Collection<ConfigResource> resources = topics.stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                .toList();

        try {
            Map<ConfigResource, Config> sourceConfigs = srcAdmin.describeConfigs(resources)
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<String, Map<String, String>> configsToChange = detectConfigurationChanges(sourceConfigs, mirrorConfig);
            applyConfigurationChanges(configsToChange);
        } catch (Exception e) {
            log.warn("Failed to describe topic configs for mirror {}: {}", mirrorName, e.getMessage());
            topicConfigSyncError.mark();
        }
    }

    private Map<String, Map<String, String>> detectConfigurationChanges(
            Map<ConfigResource, Config> sourceConfigs, ClusterMirrorConfig mirrorConfig) {
        Map<String, Map<String, String>> configsToChange = new HashMap<>();
        Pattern excludePattern = mirrorConfig.topicPropertiesExcludePattern();

        sourceConfigs.forEach((resource, config) -> {
            if (resource.type() == ConfigResource.Type.TOPIC) {
                Properties props = mirrorCache.getTopicConfig(resource.name());
                Map<String, String> conChange = new HashMap<>();

                config.entries().forEach(entry -> {
                    if (entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
                            && (excludePattern == null || !excludePattern.matcher(entry.name()).matches())) {
                        if (props.containsKey(entry.name())) {
                            if (!props.get(entry.name()).equals(entry.value())) {
                                conChange.put(entry.name(), entry.value());
                            }
                        } else {
                            conChange.put(entry.name(), entry.value());
                        }
                    }
                });

                if (!conChange.isEmpty()) {
                    configsToChange.put(resource.name(), conChange);
                }
            }
        });

        return configsToChange;
    }

    private void applyConfigurationChanges(Map<String, Map<String, String>> configsToChange) {
        if (configsToChange.isEmpty()) {
            return;
        }

        log.debug("Applying configuration changes {}", configsToChange);

        var resources = configsToChange.entrySet().stream()
                .map(topicEntry -> {
                    var configs = topicEntry.getValue().entrySet().stream()
                            .map(configEntry -> new IncrementalAlterConfigsRequestData.AlterableConfig()
                                    .setName(configEntry.getKey())
                                    .setValue(configEntry.getValue())
                                    .setConfigOperation(AlterConfigOp.OpType.SET.id()))
                            .collect(Collectors.toCollection(IncrementalAlterConfigsRequestData.AlterableConfigCollection::new));
                    return new IncrementalAlterConfigsRequestData.AlterConfigsResource()
                            .setResourceType(ConfigResource.Type.TOPIC.id())
                            .setResourceName(topicEntry.getKey())
                            .setConfigs(configs);
                })
                .collect(Collectors.toCollection(IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection::new));

        var data = new IncrementalAlterConfigsRequestData()
                .setValidateOnly(false)
                .setResources(resources);
        controllerClient.sendRequest(new IncrementalAlterConfigsRequest.Builder(data), new TimeoutHandler(log, topicConfigSyncError));
    }

    /**
     * Syncs group offsets from the source cluster to the destination in two phases:
     * consumer groups first, then share groups. Keeping them separate avoids cross-type
     * conflicts where a consumer group on the source could overwrite a share group with
     * the same name on the destination (or vice versa).
     */
    private void syncGroupOffsets(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        Set<String> mirrorTopics = mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING));
        if (mirrorTopics.isEmpty()) {
            return;
        }

        Pattern groupsIncludePattern = mirrorConfig.groupsIncludePattern();
        Pattern groupsExcludePattern = mirrorConfig.groupsExcludePattern();

        syncConsumerGroupOffsets(srcAdmin, mirrorName, mirrorTopics, groupsIncludePattern, groupsExcludePattern);
        syncShareGroupOffsets(srcAdmin, mirrorName, mirrorTopics, groupsIncludePattern, groupsExcludePattern);
    }

    private void syncConsumerGroupOffsets(Admin srcAdmin, String mirrorName, Set<String> mirrorTopics,
                                          Pattern groupsIncludePattern, Pattern groupsExcludePattern) {
        try {
            List<String> sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forConsumerGroups(),
                    groupsIncludePattern, groupsExcludePattern);
            if (sourceGroupIds.isEmpty()) {
                return;
            }
            log.info("Syncing consumer group offsets for mirror {}, groups={}", mirrorName, sourceGroupIds);

            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = sourceGroupIds.stream()
                    .collect(Collectors.toMap(id -> id, id -> new ListConsumerGroupOffsetsSpec()));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets = srcAdmin
                    .listConsumerGroupOffsets(groupSpecs).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            // Resolve log offsets once for all mirror topic partitions across all groups
            Set<TopicPartition> allMirrorPartitions = allOffsets.values().stream()
                    .flatMap(m -> m.keySet().stream())
                    .filter(tp -> mirrorTopics.contains(tp.topic()))
                    .collect(Collectors.toSet());
            Map<TopicPartition, PartitionLogInfo> logInfoMap = resolvePartitionLogInfo(allMirrorPartitions);

            for (var entry : allOffsets.entrySet()) {
                String groupId = entry.getKey();

                Map<TopicPartition, OffsetAndMetadata> filtered = new HashMap<>();
                entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .forEach(ent -> {
                            TopicPartition topicPartition = ent.getKey();
                            PartitionLogInfo plog = logInfoMap.get(topicPartition);
                            if (plog == null) {
                                log.debug("Cannot resolve log offsets for partition {}, skip consumer group sync for it", topicPartition);
                                return;
                            }
                            OffsetAndMetadata sourceGroupOffsetAndMetadata = ent.getValue();

                            // Clamp to the range [logStartOffset, logEndOffset]
                            long finalOffset = Math.max(plog.logStartOffset(), Math.min(sourceGroupOffsetAndMetadata.offset(), plog.logEndOffset()));

                            if (finalOffset == sourceGroupOffsetAndMetadata.offset()) {
                                filtered.put(topicPartition, sourceGroupOffsetAndMetadata);
                            } else if (finalOffset == plog.logEndOffset()) {
                                if (plog.logEndEpoch() < 0) {
                                    log.debug("Cannot get the log end epoch for partition {}, skip consumer group sync for it", topicPartition);
                                } else {
                                    filtered.put(topicPartition, new OffsetAndMetadata(plog.logEndOffset(), Optional.of(plog.logEndEpoch()), ""));
                                }
                            } else {
                                if (plog.logStartEpoch() < 0) {
                                    log.debug("Cannot get the log start epoch for partition {}, skip consumer group sync for it", topicPartition);
                                } else {
                                    filtered.put(topicPartition, new OffsetAndMetadata(plog.logStartOffset(), Optional.of(plog.logStartEpoch()), ""));
                                }
                            }
                        });

                if (filtered.isEmpty()) {
                    continue;
                }

                // No pre-filtering of active destination groups: the group coordinator
                // rejects commits for groups with active members, so per-group error
                // handling is sufficient and avoids a racy ListGroups RPC every cycle.
                try {
                    log.debug("Committing consumer group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    getOrCreateDestAdmin().alterConsumerGroupOffsets(groupId, filtered)
                        .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    if (ex instanceof ExecutionException && ex.getCause() instanceof UnknownMemberIdException) {
                        log.debug("Skipped consumer group offset sync for active group {}", groupId);
                    } else {
                        log.warn("Failed to commit consumer group offsets for group {}", groupId, ex);
                        consumerGroupOffsetSyncError.mark();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to sync consumer group offsets for mirror {}", mirrorName, e);
            consumerGroupOffsetSyncError.mark();
        }
    }

    private void syncShareGroupOffsets(Admin srcAdmin, String mirrorName, Set<String> mirrorTopics,
                                       Pattern groupsIncludePattern, Pattern groupsExcludePattern) {
        try {
            List<String> sourceGroupIds;
            try {
                sourceGroupIds = listSourceGroupIds(srcAdmin, ListGroupsOptions.forShareGroups(),
                        groupsIncludePattern, groupsExcludePattern);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnsupportedVersionException) {
                    log.debug("The source cluster doesn't support share group, skipping share group offset sync");
                    return;
                } else {
                    throw e;
                }
            }
            if (sourceGroupIds.isEmpty()) {
                return;
            }
            log.info("Syncing share group offsets for mirror {}, groups={}", mirrorName, sourceGroupIds);

            Map<String, ListShareGroupOffsetsSpec> groupSpecs = sourceGroupIds.stream()
                    .collect(Collectors.toMap(id -> id, id -> new ListShareGroupOffsetsSpec()));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets = srcAdmin
                    .listShareGroupOffsets(groupSpecs).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            // Resolve log offsets once for all mirror topic partitions across all groups
            Set<TopicPartition> allMirrorPartitions = allOffsets.values().stream()
                    .flatMap(m -> m.keySet().stream())
                    .filter(tp -> mirrorTopics.contains(tp.topic()))
                    .collect(Collectors.toSet());
            Map<TopicPartition, PartitionLogInfo> logInfoMap = resolvePartitionLogInfo(allMirrorPartitions);

            for (var entry : allOffsets.entrySet()) {
                String groupId = entry.getKey();

                Map<TopicPartition, Long> filtered = new HashMap<>();
                entry.getValue().entrySet().stream()
                        .filter(e -> mirrorTopics.contains(e.getKey().topic()))
                        .forEach(ent -> {
                            TopicPartition topicPartition = ent.getKey();
                            PartitionLogInfo plog = logInfoMap.get(topicPartition);
                            if (plog == null) {
                                log.debug("Cannot resolve log offsets for partition {}, skip share group sync for it", topicPartition);
                                return;
                            }
                            OffsetAndMetadata sourceGroupOffsetAndMetadata = ent.getValue();
                            // Clamp to the range [logStartOffset, logEndOffset]
                            long finalOffset = Math.max(plog.logStartOffset(), Math.min(sourceGroupOffsetAndMetadata.offset(), plog.logEndOffset()));
                            filtered.put(topicPartition, finalOffset);
                        });
                if (filtered.isEmpty()) {
                    continue;
                }

                // No pre-filtering of active destination groups: the group coordinator
                // rejects commits for groups with active members, so per-group error
                // handling is sufficient and avoids a racy ListGroups RPC every cycle.
                try {
                    log.debug("Committing share group offsets for group {} on destination, partitions={}", groupId, filtered.keySet());
                    getOrCreateDestAdmin().alterShareGroupOffsets(groupId, filtered).all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    if (ex instanceof ExecutionException && ex.getCause() instanceof GroupNotEmptyException) {
                        log.error("Skipped share group offset sync for active group {}", groupId);
                    } else {
                        log.warn("Failed to commit share group offsets for group {}", groupId, ex);
                        shareGroupOffsetSyncError.mark();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to sync share group offsets for mirror {}: {}", mirrorName, e);
            shareGroupOffsetSyncError.mark();
        }
    }

    /**
     * Resolves log start offset, log end offset, and their leader epochs for each partition.
     * Tries the local ReplicaManager first (in-memory). For partitions without a local replica,
     * falls back to dstAdmin.listOffsets() so offset sync works from any coordinator broker.
     */
    private Map<TopicPartition, PartitionLogInfo> resolvePartitionLogInfo(Set<TopicPartition> partitions)
            throws Exception {
        Map<TopicPartition, PartitionLogInfo> result = new HashMap<>();
        Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();

        for (TopicPartition tp : partitions) {
            Option<UnifiedLog> localLog = replicaManagerSupplier.get().getLog(tp);
            if (localLog.isDefined()) {
                UnifiedLog ulog = localLog.get();
                long startOffset = ulog.logStartOffset();
                long endOffset = ulog.logEndOffset();
                int startEpoch = ulog.leaderEpochCache().epochForOffset(startOffset).orElse(-1);
                int endEpoch = ulog.leaderEpochCache().epochForOffset(endOffset).orElse(-1);
                result.put(tp, new PartitionLogInfo(startOffset, startEpoch, endOffset, endEpoch));
            } else {
                earliestSpecs.put(tp, OffsetSpec.earliest());
                latestSpecs.put(tp, OffsetSpec.latest());
            }
        }

        if (!earliestSpecs.isEmpty()) {
            Admin dstAdmin = getOrCreateDestAdmin();
            ListOffsetsResult earliestResult = dstAdmin.listOffsets(earliestSpecs);
            ListOffsetsResult latestResult = dstAdmin.listOffsets(latestSpecs);

            for (TopicPartition tp : earliestSpecs.keySet()) {
                try {
                    var earliest = earliestResult.partitionResult(tp)
                        .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    var latest = latestResult.partitionResult(tp)
                        .get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    result.put(tp, new PartitionLogInfo(
                        earliest.offset(), earliest.leaderEpoch().orElse(-1),
                        latest.offset(), latest.leaderEpoch().orElse(-1)));
                } catch (ExecutionException e) {
                    log.debug("Failed to fetch offsets for partition {} via Admin: {}", tp, e.getMessage());
                }
            }
        }

        return result;
    }

    private void syncAcls(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        try {
            Collection<AclBinding> sourceAcls = srcAdmin.describeAcls(AclBindingFilter.ANY)
                    .values().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            log.debug("Describe ACLs response from remote cluster {}: {}", mirrorName, sourceAcls);

            List<MirrorAclRule> aclIncludeRules = mirrorConfig.aclIncludeRules();
            List<MirrorAclRule> aclExcludeRules = mirrorConfig.aclExcludeRules();
            var allSourceAcls = sourceAcls.stream()
                    .filter(acl -> aclIncludeRules.stream().anyMatch(rule -> rule.matches(acl))
                            && aclExcludeRules.stream().noneMatch(rule -> rule.matches(acl)))
                    .toList();
            var aclChanges = detectAclChanges(allSourceAcls);
            applyAclChanges(mirrorName, aclChanges);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SecurityDisabledException) {
                log.debug("ACL sync skipped for mirror {} because security is disabled", mirrorName);
            } else {
                log.warn("Failed to describe ACLs for mirror {}", mirrorName, e);
                aclSyncError.mark();
            }
        } catch (Exception e) {
            log.warn("Failed to sync ACLs for mirror {}", mirrorName, e);
            aclSyncError.mark();
        }
    }

    private SourceAclChanges detectAclChanges(List<AclBinding> sourceAcls) {
        var addACLsList = new ArrayList<AclBinding>();
        var deleteACLsList = new ArrayList<AclBinding>();
        var current = metadataImage.acls().acls().values();

        sourceAcls.forEach(acl -> {
            if (current.stream().map(StandardAcl::toBinding).noneMatch(a -> a.equals(acl))) {
                addACLsList.add(acl);
            }
        });

        metadataImage.acls().acls().values().forEach(acl -> {
            if (acl.resourceType() != ResourceType.CLUSTER_MIRROR && !sourceAcls.contains(acl.toBinding())) {
                deleteACLsList.add(acl.toBinding());
            }
        });

        return new SourceAclChanges(addACLsList, deleteACLsList);
    }

    private void applyAclChanges(String mirrorName, SourceAclChanges aclChanges) {
        if (!aclChanges.aclsToAdd().isEmpty()) {
            log.debug("Adding {} ACLs from remote cluster {}", aclChanges.aclsToAdd().size(), mirrorName);
            var requestData = aclChanges.aclsToAdd().stream().map(
                            aclBinding -> new CreateAclsRequestData.AclCreation()
                                    .setResourceType(aclBinding.pattern().resourceType().code())
                                    .setResourceName(aclBinding.pattern().name())
                                    .setResourcePatternType(aclBinding.pattern().patternType().code())
                                    .setPrincipal(aclBinding.entry().principal())
                                    .setHost(aclBinding.entry().host())
                                    .setOperation(aclBinding.entry().operation().code())
                                    .setPermissionType(aclBinding.entry().permissionType().code()))
                    .toList();
            controllerClient.sendRequest(
                    new CreateAclsRequest.Builder(new CreateAclsRequestData().setCreations(requestData)),
                    new TimeoutHandler(log, aclSyncError)
            );
        }

        if (!aclChanges.aclsToDelete().isEmpty()) {
            log.debug("Removing {} ACLs from remote cluster {}", aclChanges.aclsToDelete().size(), mirrorName);
            var requestData = aclChanges.aclsToDelete().stream().map(
                            aclBinding -> new org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter()
                                    .setResourceTypeFilter(aclBinding.pattern().resourceType().code())
                                    .setResourceNameFilter(aclBinding.pattern().name())
                                    .setPatternTypeFilter(aclBinding.pattern().patternType().code())
                                    .setPrincipalFilter(aclBinding.entry().principal())
                                    .setHostFilter(aclBinding.entry().host())
                                    .setOperation(aclBinding.entry().operation().code())
                                    .setPermissionType(aclBinding.entry().permissionType().code()))
                    .toList();
            controllerClient.sendRequest(
                    new DeleteAclsRequest.Builder(new DeleteAclsRequestData().setFilters(requestData)),
                    new TimeoutHandler(log, aclSyncError)
            );
        }
    }

    private void discoverTopicsByPattern(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        final Pattern topicsIncludePattern = mirrorConfig.topicsIncludePattern();
        if (topicsIncludePattern == null) {
            return;
        }

        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        Set<String> configuredTopics = mirrorCache.getMirrorTopics(mirrorName,
                EnumSet.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED, MirrorPartitionState.STOPPED));
        final Pattern topicsExcludePattern = mirrorConfig.topicsExcludePattern();

        List<StartMirrorTopicsRequestData.TopicMetadata> newTopics;
        try {
            Set<String> allSourceTopics = srcAdmin.listTopics()
                    .names().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            List<String> candidates = allSourceTopics.stream()
                    .filter(name -> topicsIncludePattern.matcher(name).matches())
                    .filter(name -> topicsExcludePattern == null || !topicsExcludePattern.matcher(name).matches())
                    .filter(name -> !configuredTopics.contains(name))
                    .toList();

            if (candidates.isEmpty()) {
                return;
            }

            Map<String, TopicDescription> descriptions = srcAdmin.describeTopics(candidates)
                    .allTopicNames().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);

            cacheSourceLeaders(mirrorName, descriptions.values());

            newTopics = descriptions.values().stream()
                    .map(td -> new StartMirrorTopicsRequestData.TopicMetadata()
                            .setTopicName(td.name())
                            .setTopicId(td.topicId())
                            .setNumPartitions(td.partitions().size()))
                    .toList();
        } catch (Exception e) {
            log.warn("Failed to discover topics by pattern for mirror {}", mirrorName, e);
            metadataRefreshError.mark();
            return;
        }

        if (newTopics.isEmpty()) {
            return;
        }

        log.info("Discovered {} new topic(s) matching mirror.topics.include pattern for mirror {}: {}",
                newTopics.size(), mirrorName, newTopics.stream().map(StartMirrorTopicsRequestData.TopicMetadata::topicName).toList());

        try {
            List<String> topicNames = newTopics.stream()
                    .map(StartMirrorTopicsRequestData.TopicMetadata::topicName).toList();
            getOrCreateDestAdmin().startMirrorTopics(
                    mirrorName, topicNames, new StartMirrorTopicsOptions())
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Failed to start discovered topics for mirror {}: {}", mirrorName, e.getMessage());
            metadataRefreshError.mark();
        }
    }

    /**
     * Checks if any active mirror topics now match the exclude pattern and sends
     * StopMirrorTopicsRequest to stop them. Catches cases where exclude was updated
     * via incrementalAlterConfigs outside the startMirrorTopics/stopMirrorTopics flow.
     */
    private void enforceExcludePatterns(String mirrorName, ClusterMirrorConfig mirrorConfig) {
        Pattern excludePattern = mirrorConfig.topicsExcludePattern();
        if (excludePattern == null) return;

        Set<String> activeTopics = mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING));
        Set<String> excludedTopics = activeTopics.stream()
                .filter(topic -> excludePattern.matcher(topic).matches())
                .collect(Collectors.toSet());

        if (excludedTopics.isEmpty()) return;

        log.info("Stopping {} topic(s) matching mirror.topics.exclude for mirror {}: {}",
                excludedTopics.size(), mirrorName, excludedTopics);

        try {
            List<String> topicNames = new ArrayList<>(excludedTopics);
            getOrCreateDestAdmin().stopMirrorTopics(
                    mirrorName, topicNames, new StopMirrorTopicsOptions())
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Failed to stop excluded topics for mirror {}: {}", mirrorName, e.getMessage());
            metadataRefreshError.mark();
        }
    }

    /**
     * Pre-populates sourceLeaders for discovered topics so that when onMetadataUpdate fires
     * after the destination topic is created, the fetcher can connect to the correct source
     * broker immediately. Without this, the fetcher starts with a bootstrap broker, gets a
     * redirect it cannot resolve, and cycles through FAILED/retry until the next periodic
     * syncSourceTopicState populates the cache.
     */
    private void cacheSourceLeaders(String mirrorName, Collection<TopicDescription> descriptions) {
        descriptions.forEach(td -> td.partitions().forEach(pi -> {
            if (pi.leader() != null) {
                mirrorCache.updateSourceClusterLeader(mirrorName, new TopicPartition(td.name(), pi.partition()),
                        new MirrorMetadataCache.SourceClusterLeader(Optional.of(pi.leader()), pi.leaderEpoch().orElse(0)));
            }
        }));
    }

    record TimeoutHandler(Logger log, Meter errorMeter) implements ControllerRequestCompletionHandler {
        @Override
        public void onTimeout() {
            log.warn("Controller request timed out");
            maybeMarkError();
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.authenticationException() != null || response.wasDisconnected() || response.versionMismatch() != null) {
                maybeMarkError();
            }
            AbstractResponse abstractResponse = response.responseBody();
            Map<Errors, Integer> errors = abstractResponse.errorCounts();
            int numErrors = errors == null ? 0 : errors.entrySet().stream().filter(e -> e.getKey() != Errors.NONE).mapToInt(Map.Entry::getValue).sum();
            if (numErrors > 0) {
                maybeMarkError();
            }
            log.debug("Controller request completed: {}", response);
        }

        private void maybeMarkError() {
            if (errorMeter != null) {
                errorMeter.mark();
            }
        }
    }

    @FunctionalInterface
    interface TransitionHandler {
        void transitionTo(String mirrorName, Set<TopicPartition> topicPartitions,
                          MirrorPartitionState targetState, String errorMessage, boolean nonRetryable);
    }

    @FunctionalInterface
    interface EpochBumpHandler {
        CompletableFuture<Void> maybeBumpLeaderEpochs(String mirrorName,
                                                      List<SourceTopicMetadata> sourceTopicMetadata,
                                                      Set<TopicPartition> topicPartitions);
    }

    record SourceTopicMetadata(String topic, Uuid topicId, boolean exists, List<SourcePartitionState> partitions) { }
    record SourcePartitionState(TopicPartition topicPartition, Node leader, Optional<Integer> leaderEpoch) { }
    record SourceAclChanges(List<AclBinding> aclsToAdd, List<AclBinding> aclsToDelete) { }
    record PartitionLogInfo(long logStartOffset, int logStartEpoch, long logEndOffset, int logEndEpoch) { }
}
