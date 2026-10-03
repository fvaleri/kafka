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
import kafka.server.NetworkUtils;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.MirrorConfigNotAvailableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BumpLeaderEpochsRequestData;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ReadMirrorOffsetsRequestData;
import org.apache.kafka.common.message.ReadMirrorOffsetsResponseData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.BumpLeaderEpochsRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ReadMirrorOffsetsRequest;
import org.apache.kafka.common.requests.ReadMirrorOffsetsResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinatorService.MirrorStateWrite;
import org.apache.kafka.coordinator.mirror.MetadataManagerBridge;
import org.apache.kafka.coordinator.mirror.MirrorMetadataCache;
import org.apache.kafka.image.ConfigurationDelta;
import org.apache.kafka.image.LocalReplicaChanges;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MirrorUtils;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import com.google.re2j.Pattern;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.server.mirror.MirrorPartitionMetadata.NON_RETRYABLE_ATTEMPT;

/**
 * Reacts to KRaft metadata changes, decides mirror partition state transitions,
 * persists them, and executes the resulting side effects (actions).
 * Schedule periodic source cluster synchronization.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataManagerBridge, MetadataPublisher, AutoCloseable {
    static final int LEADER_EPOCH_BUMP_THRESHOLD = 3;
    static final int LEADER_EPOCH_BUMP_INCREMENT = 10;

    // Mirror config keys that do not affect source connections (no reconnect needed)
    private static final Set<String> SKIP_RECONNECT_MIRROR_CONFIGS = Set.of(
            ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.ACLS_INCLUDE_CONFIG);

    // Topic config keys that trigger mirror partition re-evaluation on change
    private static final Set<String> WATCHED_TOPIC_CONFIGS = Set.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);

    // Backoff duration in milliseconds before retrying a state write when the coordinator is loading
    private static final long COORD_LOADING_RETRY_BACKOFF_MS = 1000L;

    private final Logger log;
    private final String clusterId;
    private final KafkaConfig brokerConfig;
    private final String name;
    private final int nodeId;

    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private final NodeToControllerChannelManager controllerClient;
    private final MirrorMetadataCache mirrorCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;
    private final Time time;
    
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private volatile boolean isInitialized = false;

    private volatile MirrorInterBrokerSender interBrokerSender;
    private volatile MirrorSourceSyncer sourceSyncer;
    private volatile Map<String, Admin> srcAdmins;
    private volatile Admin dstAdmin;

    private Optional<Function<MirrorPartition, Integer>> coordPartFinder = Optional.empty();
    private Optional<MetadataManagerBridge.CoordinatorReader> coordinatorReader = Optional.empty();
    private Optional<MetadataManagerBridge.CoordinatorWriter> coordinatorWriter = Optional.empty();

    private final Map<TopicPartition, MirrorPartitionState> pendingStateTransitions = new ConcurrentHashMap<>();
    private final Set<PendingLeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();

    private final KafkaMetricsGroup metricsGroup;
    private final Meter metadataRefreshError;
    private final Meter topicConfigSyncError;
    private final Meter consumerGroupOffsetSyncError;
    private final Meter shareGroupOffsetSyncError;
    private final Meter aclSyncError;

    public MirrorMetadataManager(
        String clusterId,
        KafkaConfig brokerConfig,
        NodeToControllerChannelManager controllerClient,
        Supplier<ReplicaManager> replicaManagerSupplier,
        MirrorMetadataCache mirrorCache,
        Metrics metrics,
        Time time
    ) {
        this.clusterId = clusterId;
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " brokerId=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);

        this.controllerClient = controllerClient;
        this.replicaManagerSupplier = replicaManagerSupplier;
        this.mirrorCache = mirrorCache;

        this.scheduler = new KafkaScheduler(1, true, "mirror-manager-");
        this.scheduler.startup();
        this.metrics = metrics;
        this.time = time;

        this.metricsGroup = new KafkaMetricsGroup(this.getClass());
        this.metadataRefreshError = metricsGroup.newMeter("TopicMetadataRefreshError", "errors", TimeUnit.SECONDS);
        this.topicConfigSyncError = metricsGroup.newMeter("TopicConfigSyncError", "errors", TimeUnit.SECONDS);
        this.consumerGroupOffsetSyncError = metricsGroup.newMeter("ConsumerGroupOffsetSyncError", "errors", TimeUnit.SECONDS);
        this.shareGroupOffsetSyncError = metricsGroup.newMeter("ShareGroupOffsetSyncError", "errors", TimeUnit.SECONDS);
        this.aclSyncError = metricsGroup.newMeter("AclSyncError", "errors", TimeUnit.SECONDS);

        metricsGroup.newGauge("LogAlignmentPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.LOG_ALIGNMENT));
        metricsGroup.newGauge("UleRecoveryPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.ULE_RECOVERY));
        metricsGroup.newGauge("EpochFencingPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.EPOCH_FENCING));
        metricsGroup.newGauge("MirroringPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.MIRRORING));
        metricsGroup.newGauge("PausingPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.PAUSING));
        metricsGroup.newGauge("PausedPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.PAUSED));
        metricsGroup.newGauge("StoppingPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.STOPPING));
        metricsGroup.newGauge("StoppedPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.STOPPED));
        metricsGroup.newGauge("FailedPartitionState", () -> this.mirrorCache.countPartitionsInState(MirrorPartitionState.FAILED));
    }

    @Override
    public String name() {
        return name;
    }

    private boolean isLocalCoordinatorFor(String mirrorName, Uuid topicId, int partition) {
        if (metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null && coordPartFinder.isPresent()) {
            int coordinatorBroker = metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartFinder.get().apply(
                            MirrorPartition.of(mirrorName, topicId, partition))).leader;
            return coordinatorBroker == brokerConfig.nodeId();
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
                // Because we know the bootstrap.server must be set when creating the cluster mirror,
                // throw a retryable exception here and retry later when the metadata log is not propagated to this broker.
                throw new MirrorConfigNotAvailableException();
            }
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-src-admin-" + k + "-" + nodeId);
            return Admin.create(props);
        });
    }

    Admin getOrCreateDestAdmin() {
        if (dstAdmin == null) {
            Properties props = buildDestAdminClientProps(brokerConfig);
            // Fall back to metadataCache when the advertised port is unresolved (e.g. ephemeral port 0 in tests)
            if (props.getProperty(BOOTSTRAP_SERVERS_CONFIG).endsWith(":0")) {
                ListenerName listenerName = brokerConfig.mirrorAdminListenerName();
                mirrorCache.getAliveBrokerNode(nodeId, listenerName).ifPresent(node ->
                        props.put(BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port()));
            }
            dstAdmin = Admin.create(props);
        }
        return dstAdmin;
    }

    // Visible for testing
    static Properties buildDestAdminClientProps(KafkaConfig brokerConfig) {
        ListenerName mirrorAdminListener = brokerConfig.mirrorAdminListenerName();
        Endpoint endpoint = (Endpoint) brokerConfig.effectiveAdvertisedBrokerListeners()
                .filter(e -> e.listener().equals(mirrorAdminListener.value()))
                .head();

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "mirror-dst-admin-" + brokerConfig.nodeId());

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

    // ===== LIFECYCLE MANAGEMENT ======================================================================================

    @Override
    public void onBrokerStartup(Function<MirrorPartition, Integer> coordPartFinder,
                                MetadataManagerBridge.CoordinatorReader coordinatorReader,
                                MetadataManagerBridge.CoordinatorWriter coordinatorWriter) {
        this.interBrokerSender = new MirrorInterBrokerSender(MirrorInterBrokerSender.class.getSimpleName(),
                NetworkUtils.buildNetworkClient(MirrorMetadataManager.class.getSimpleName(), brokerConfig, metrics, time, new LogContext(name())),
                brokerConfig.requestTimeoutMs(), Time.SYSTEM);
        interBrokerSender.start();

        this.sourceSyncer = new MirrorSourceSyncer(brokerConfig, clusterId, this, replicaManagerSupplier,
                controllerClient, mirrorCache, metricsGroup, metadataRefreshError, topicConfigSyncError,
                consumerGroupOffsetSyncError, shareGroupOffsetSyncError, aclSyncError);
        sourceSyncer.scheduleSourceClusterSync(brokerConfig.mirrorConfig().metadataRefreshIntervalMs());

        this.coordPartFinder = Optional.of(coordPartFinder);
        this.coordinatorWriter = Optional.of(coordinatorWriter);
        this.coordinatorReader = Optional.of(coordinatorReader);

        this.isInitialized = true;
    }

    /**
     * Called after a coordinator shard finishes loading.
     * Re-evaluates mirror leaders that map to this coordinator partition.
     */
    @Override
    public void onShardLoaded(int coordPartition) {
        log.debug("Coordinator shard {} loaded", coordPartition);
        if (!isInitialized || metadataImage == null || coordPartFinder.isEmpty()) {
            return;
        }
        Set<TopicPartition> mirrorLeaders = new HashSet<>();
        metadataImage.topics().topicsByName().forEach((topicName, topicImage) -> {
            if (topicImage.mirrorName() != null) {
                topicImage.partitions().forEach((partitionId, partition) -> {
                    if (partition.leader == nodeId) {
                        TopicPartition tp = new TopicPartition(topicName, partitionId);
                        MirrorPartition mp = MirrorPartition.of(
                                topicImage.mirrorName(), mirrorCache.getTopicId(topicName), partitionId);
                        if (coordPartFinder.get().apply(mp) == coordPartition) {
                            mirrorLeaders.add(tp);
                        }
                    }
                });
            }
        });
        if (!mirrorLeaders.isEmpty()) {
            processStateTransitions(mirrorLeaders, metadataImage);
        }
    }

    /**
     * Called when a coordinator shard is unloaded.
     * Clears cached state for partitions that mapped to this shard.
     */
    @Override
    public void onShardUnloaded(int coordPartIndex, int numPartitions) {
        log.debug("Coordinator shard {} unloaded", coordPartIndex);
        mirrorCache.removePartitionMetadata(coordPartIndex, numPartitions);
    }

    @Override
    public void onBrokerShutdown() {
        if (srcAdmins != null) {
            srcAdmins.values().forEach(admin -> admin.close(Duration.ZERO));
        }
    }

    @Override
    public void close() throws Exception {
        if (interBrokerSender != null) {
            interBrokerSender.shutdown();
        }
        if (sourceSyncer != null) {
            sourceSyncer.close();
        }
        scheduler.shutdown();
        pendingStateTransitions.clear();
        pendingLeaderEpochBumps.clear();
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
        metricsGroup.removeMetric("LogAlignmentPartitionState");
        metricsGroup.removeMetric("UleRecoveryPartitionState");
        metricsGroup.removeMetric("EpochFencingPartitionState");
        metricsGroup.removeMetric("MirroringPartitionState");
        metricsGroup.removeMetric("PausingPartitionState");
        metricsGroup.removeMetric("PausedPartitionState");
        metricsGroup.removeMetric("StoppingPartitionState");
        metricsGroup.removeMetric("StoppedPartitionState");
        metricsGroup.removeMetric("FailedPartitionState");
    }

    private void closeAndRemoveSourceAdmin(String mirrorName) {
        if (srcAdmins != null) {
            Admin admin = srcAdmins.remove(mirrorName);
            if (admin != null) {
                admin.close(Duration.ZERO);
            }
        }
    }

    // ===== METADATA UPDATE ===========================================================================================

    /**
     * Called when cluster metadata is updated in the KRaft metadata publisher thread.
     * Updates the metadata image and processes any state transitions.
     */
    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        this.metadataImage = newImage;
        mirrorCache.updateMetadataImage(newImage);
        if (sourceSyncer != null) {
            sourceSyncer.updateMetadataImage(newImage);
        }

        if (!isInitialized) {
            return;
        }

        Set<TopicPartition> partitionsToTransition = collectPartitionsToTransition(delta, newImage);

        if (partitionsToTransition.isEmpty()) {
            return;
        }

        log.info("Cluster metadata updated for partitions {}", partitionsToTransition);

        processStateTransitions(partitionsToTransition, newImage);
        maybeCompletePendingEpochBumps();
    }

    /**
     * Collects mirror partitions that need a state transition and handles
     * mirror config side effects (connection teardown, tombstones).
     * <p>
     * Sources:
     *   1. Gained leader partitions belonging to a configured mirror
     *   2. Desired mirror state changes (start/stop/pause/resume)
     *   3. Mirror connection config changes (teardown + reconnect)
     *   4. Watched topic config changes (e.g. remote.storage.enable)
     */
    private Set<TopicPartition> collectPartitionsToTransition(MetadataDelta delta, MetadataImage image) {
        Set<String> mirrors = mirrorCache.getMirrorNames();

        Set<TopicPartition> topicsResult = collectFromTopicsDelta(delta, image, mirrors);
        Set<TopicPartition> configsResult = collectFromConfigsDelta(delta, image, mirrors);

        topicsResult.addAll(configsResult);
        return topicsResult;
    }

    private Set<TopicPartition> collectFromTopicsDelta(MetadataDelta delta,
                                                       MetadataImage image,
                                                       Set<String> configuredMirrors) {
        Set<TopicPartition> result = new HashSet<>();

        if (delta.topicsDelta() == null) {
            return result;
        }

        LocalReplicaChanges localReplicaChanges = delta.topicsDelta().localChanges(nodeId);

        // [1] This broker became leader for a mirror partition
        localReplicaChanges.leaders().keySet().forEach(tp -> {
            String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
            if (mirrorName != null && configuredMirrors.contains(mirrorName)) {
                result.add(tp);
            }
        });

        // [2] Controller wrote a new desired state for a mirror topic
        localReplicaChanges.mirrorTopicStates().keySet().forEach(topicId ->
                addMirrorLeaderPartitions(image.topics().getTopic(topicId), configuredMirrors, result));

        clearLostLeadershipCache(localReplicaChanges, image);

        return result;
    }

    private void addMirrorLeaderPartitions(TopicImage topicImage,
                                           Set<String> configuredMirrors,
                                           Set<TopicPartition> result) {
        if (topicImage == null || topicImage.mirrorName() == null
                || !configuredMirrors.contains(topicImage.mirrorName())) {
            return;
        }
        topicImage.partitions().forEach((partitionId, partition) -> {
            if (partition.leader == nodeId) {
                result.add(new TopicPartition(topicImage.name(), partitionId));
            }
        });
    }

    private void clearLostLeadershipCache(LocalReplicaChanges changes, MetadataImage image) {
        changes.followers().keySet().forEach(tp -> {
            String mirrorName = image.topics().getTopic(tp.topic()).mirrorName();
            if (mirrorName == null) {
                return;
            }
            pendingStateTransitions.remove(tp);
            pendingLeaderEpochBumps.removeIf(bump -> {
                bump.partitionToEpoch().remove(tp);
                if (bump.partitionToEpoch().isEmpty()) {
                    bump.future().complete(null);
                    return true;
                }
                return false;
            });
            if (!isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
                MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
                mirrorCache.removePartitionMetadata(mp);
            }
        });
    }

    private Set<TopicPartition> collectFromConfigsDelta(MetadataDelta delta,
                                                        MetadataImage image,
                                                        Set<String> configuredMirrors) {
        Set<TopicPartition> result = new HashSet<>();

        if (delta.configsDelta() == null) {
            return result;
        }

        Set<String> mirrorsToReconnect = new HashSet<>();

        for (var entry : delta.configsDelta().changes().entrySet()) {
            ConfigResource resource = entry.getKey();

            // [3] Mirror config changed or deleted: tear down connections
            if (resource.type() == ConfigResource.Type.CLUSTER_MIRROR) {
                handleMirrorConfigChange(resource, entry.getValue(), delta.image(), image)
                        .ifPresent(mirrorsToReconnect::add);

            // [4] Watched topic config changed
            } else if (resource.type() == ConfigResource.Type.TOPIC
                    && entry.getValue().changes().keySet().stream().anyMatch(WATCHED_TOPIC_CONFIGS::contains)) {
                addMirrorLeaderPartitions(image.topics().getTopic(resource.name()), configuredMirrors, result);
            }
        }

        if (!mirrorsToReconnect.isEmpty()) {
            mirrorCache.getMirrorPartitions().forEach(mp -> {
                MirrorPartitionMetadata cacheEntry = mirrorCache.getPartitionMetadata(mp);
                if (cacheEntry != null && mirrorsToReconnect.contains(mp.mirrorName())
                        && cacheEntry.state() == MirrorPartitionState.MIRRORING) {
                    mirrorCache.getTopicName(mp.topicId()).ifPresent(topicName ->
                            result.add(new TopicPartition(topicName, mp.partition())));
                }
            });
        }

        return result;
    }

    /** Returns the mirror name if it needs reconnection, empty if deleted or unchanged. */
    private Optional<String> handleMirrorConfigChange(ConfigResource resource,
                                                      ConfigurationDelta configDelta,
                                                      MetadataImage prevImage,
                                                      MetadataImage image) {
        String mirrorName = resource.name();
        boolean mirrorDeleted = image.configs().configProperties(resource).isEmpty();
        if (mirrorDeleted) {
            log.info("Mirror '{}' has been deleted. Writing tombstone records.", mirrorName);
            writeTombstoneRecords(mirrorName);
            metricsGroup.removeMetric("MirrorTopicCount", Map.of("mirrorName", mirrorName));
        }

        boolean isNewMirror = prevImage.configs().configProperties(resource).isEmpty();
        boolean needsReconnection = !isNewMirror && configDelta.changes().keySet().stream()
                .anyMatch(key -> !SKIP_RECONNECT_MIRROR_CONFIGS.contains(key));
        if (needsReconnection) {
            log.info("Mirror '{}' has config changes. Recreating connections.", mirrorName);
        }

        if (needsReconnection || mirrorDeleted) {
            mirrorCache.removeSourceClusterLeaders(mirrorName);
            closeAndRemoveSourceAdmin(mirrorName);
            var mirrorFetcherManager = replicaManagerSupplier.get().mirrorFetcherManager();
            mirrorFetcherManager.removeFetchersForMirror(mirrorName);
            mirrorFetcherManager.shutdownIdleFetcherThreads();
        }

        return (needsReconnection && !mirrorDeleted) ? Optional.of(mirrorName) : Optional.empty();
    }

    /**
     * Writes tombstone records for all locally coordinated partitions
     * of a deleted mirror, then removes the mirror's cache entries.
     */
    void writeTombstoneRecords(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> states = mirrorCache.getPartitionStates(mirrorName);
        Map<Integer, Set<TopicPartition>> coordPartitionToMirrorPartitions = new HashMap<>();
        states.forEach((tp, state) -> {
            if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
                coordPartitionToMirrorPartitions.computeIfAbsent(
                        coordPartFinder.get().apply(
                                MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())),
                        v -> new HashSet<>()).add(tp);
            }
        });

        mirrorCache.removeMirrorPartitions(mirrorName);
        pendingLeaderEpochBumps.removeIf(bump -> {
            bump.partitionToEpoch().keySet().removeAll(states.keySet());
            if (bump.partitionToEpoch().isEmpty()) {
                bump.future().cancel(false);
                return true;
            }
            return false;
        });

        if (coordPartitionToMirrorPartitions.isEmpty()) {
            states.keySet().forEach(tp ->
                    mirrorCache.removePartitionMetadata(
                            MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())));
            return;
        }

        Set<TopicPartition> allTps = coordPartitionToMirrorPartitions.values().stream()
                .flatMap(Set::stream).collect(Collectors.toSet());
        coordinatorWriter.get().writeMirrorTombstones(mirrorName, allTps)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to write tombstone for mirror {}: {}. Will retry later.",
                                mirrorName, ex.getMessage());
                    } else {
                        allTps.forEach(tp -> mirrorCache.removePartitionMetadata(
                                MirrorPartition.of(mirrorName,
                                        mirrorCache.getTopicId(tp.topic()), tp.partition())));
                    }
                });
    }

    // ===== PARTITION STATE TRANSITIONS ===============================================================================

    /**
     * Applies state transitions for the given mirror partitions. Local and remote coordinator
     * partitions are batched by mirror and transitioned after reading current state.
     */
    private void processStateTransitions(Set<TopicPartition> topicPartitions, MetadataImage metadataImage) {
        Map<String, Map<String, Set<Integer>>> localPartitions = new HashMap<>();
        Map<String, Map<String, Set<Integer>>> remotePartitions = new HashMap<>();

        // Phase 1: Group partitions by coordinator location (local vs remote)
        topicPartitions.forEach(tp -> {
            TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
            String mirrorName = topicImage.mirrorName();

            if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
                localPartitions
                        .computeIfAbsent(mirrorName, k -> new HashMap<>())
                        .computeIfAbsent(tp.topic(), k -> new HashSet<>())
                        .add(tp.partition());
            } else {
                remotePartitions
                        .computeIfAbsent(mirrorName, k -> new HashMap<>())
                        .computeIfAbsent(tp.topic(), k -> new HashSet<>())
                        .add(tp.partition());
            }
        });

        // Phase 2: Read current state from local coordinators and apply transitions
        localPartitions.forEach((mirrorName, partitions) ->
                readStateFromLocalCoordinator(mirrorName, partitions).thenAccept(res ->
                        res.data().topics().forEach(topic ->
                                topic.partitions().forEach(partition -> {
                                    if (partition.errorCode() != Errors.NONE.code()) {
                                        log.warn("Error reading local mirror state for partition {}-{}: {}",
                                                topic.topicName(), partition.partitionIndex(),
                                                Errors.forCode(partition.errorCode()));
                                        return;
                                    }
                                    TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                                    TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
                                    MirrorPartitionState desiredState = topicImage != null ?
                                            MirrorPartitionState.fromValue(topicImage.desiredMirrorState()) : MirrorPartitionState.UNKNOWN;
                                    MirrorPartitionState currentState = MirrorPartitionState.fromValue(partition.state());
                                    applyStateTransition(mirrorName, tp, currentState, desiredState, null);
                                }))));

        // Phase 3: Read current state from remote coordinators and apply transitions
        remotePartitions.forEach((mirrorName, partitions) ->
                readStateFromRemoteCoordinator(mirrorName, partitions).thenAccept(res ->
                        res.data().topics().forEach(topic ->
                                topic.partitions().forEach(partition -> {
                                    if (partition.errorCode() != Errors.NONE.code()) {
                                        log.warn("Error reading remote mirror state for partition {}-{}: {}",
                                                topic.topicName(), partition.partitionIndex(),
                                                Errors.forCode(partition.errorCode()));
                                        return;
                                    }
                                    TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                                    TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
                                    MirrorPartitionState desiredState = topicImage != null ?
                                            MirrorPartitionState.fromValue(topicImage.desiredMirrorState()) : MirrorPartitionState.UNKNOWN;
                                    MirrorPartitionMetadata mpm = mirrorCache.getPartitionMetadata(
                                            MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition()));
                                    MirrorPartitionState currentState = mpm != null ? mpm.state() : MirrorPartitionState.UNKNOWN;
                                    MirrorPartitionState fetchedState = MirrorPartitionState.fromValue(partition.state());
                                    applyStateTransition(mirrorName, tp, currentState, desiredState, fetchedState);
                                }))));
    }

    /** Completes epoch bump futures whose requested epochs are now reflected in the metadata image. */
    void maybeCompletePendingEpochBumps() {
        pendingLeaderEpochBumps.removeIf(bumpLeaderEpoch -> {
            Set<TopicPartition> pendingPartitions = bumpLeaderEpoch.partitionToEpoch().entrySet().stream().filter(entry -> {
                TopicPartition tp = entry.getKey();
                int epoch = entry.getValue();
                var topicImage = metadataImage.topics().getTopic(tp.topic());
                if (topicImage == null) return false;
                var partitionReg = topicImage.partitions().get(tp.partition());
                if (partitionReg == null) return false;
                return partitionReg.leaderEpoch <= epoch;
            }).map(Map.Entry::getKey).collect(Collectors.toSet());
            if (pendingPartitions.isEmpty()) {
                bumpLeaderEpoch.future().complete(null);
                return true;
            } else {
                log.info("bumpLeaderEpoch is pending for partitions: {}, all: {}",
                        pendingPartitions, bumpLeaderEpoch.partitionToEpoch().keySet());
                return false;
            }
        });
    }

    /**
     * Applies the appropriate state transition based on current state and desired state.
     * <p>
     * The mirror partition state machine handles explicit transitions (stop/pause requests)
     * and automatic transitions (start mirroring, fail on errors). For automatic transitions,
     * the fetchedState parameter allows syncing with remote coordinator state.
     */
    private void applyStateTransition(String mirrorName,
                                      TopicPartition tp,
                                      MirrorPartitionState currentState,
                                      MirrorPartitionState desiredState,
                                      MirrorPartitionState fetchedState) {
        var log = replicaManagerSupplier.get().getLog(tp);
        if (log.isDefined() && log.get().remoteLogEnabled()) {
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED,
                "Mirroring is not supported for partitions with tiered storage enabled", true);
            return;
        }

        boolean stopRequested = desiredState == MirrorPartitionState.STOPPED;
        boolean pauseRequested = desiredState == MirrorPartitionState.PAUSED;

        if (currentState == MirrorPartitionState.FAILED) {
            MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
            MirrorPartitionMetadata mpm = mirrorCache.getPartitionMetadata(mp);
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, mpm.errorMessage(), mpm.retryAttempt() == NON_RETRYABLE_ATTEMPT);
        } else if (stopRequested) {
            if (currentState != MirrorPartitionState.STOPPED) {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPING, null, false);
            } else {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED, null, false);
            }
        } else if (pauseRequested) {
            if (currentState != MirrorPartitionState.PAUSED) {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSING, null, false);
            } else {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED, null, false);
            }
        } else if (currentState == MirrorPartitionState.PAUSED) {
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null, false);
        } else if (currentState == MirrorPartitionState.UNKNOWN
                || currentState == MirrorPartitionState.STOPPED) {
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.LOG_ALIGNMENT, null, false);
        } else {
            // The remote state is authoritative, and we must align with it to avoid state divergence
            var targetState = fetchedState != null ? fetchedState : currentState;
            transitionTo(mirrorName, Set.of(tp), targetState, null, false);
        }
    }

    /**
     * Writes a targetState transition for each partition, routing to either the local coordinator
     * shard (via {@link MetadataManagerBridge.CoordinatorWriter}) or a remote coordinator
     * (via {@link #writeStateToRemoteCoordinator}). On successful write, dispatches side
     * effects through {@link #onStateTransition}.
     */
    public void transitionTo(String mirrorName,
                             Set<TopicPartition> topicPartitions,
                             MirrorPartitionState targetState,
                             String errorMessage,
                             boolean nonRetryable) {
        Map<String, Set<MirrorStateWrite>> localWrites = new HashMap<>();
        Map<String, Set<MirrorStateWrite>> remoteWrites = new HashMap<>();

        // Phase 1: Validate and prepare state writes, batching by coordinator location (local or remote)
        for (TopicPartition tp : topicPartitions) {
            MirrorPartitionMetadata entry = mirrorCache.getPartitionMetadata(
                    MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition()));
            MirrorPartitionState currentState = entry != null ? entry.state() : null;
            MirrorPartitionState pendingState = pendingStateTransitions.get(tp);
            if (pendingState != MirrorPartitionState.MIRRORING && pendingState == targetState) {
                log.debug("Skipping state transition for partition {}. Reason: Already transitioning to {}.",
                        tp, pendingState);
                continue;
            }
            pendingStateTransitions.put(tp, targetState);

            if (!MirrorPartitionMetadata.isValidStateTransition(currentState, targetState)) {
                log.warn("Skipping state transition for partition {}. Reason: Transition from {} to {} is invalid.",
                        tp, currentState, targetState);
                continue;
            }
            if (targetState == MirrorPartitionState.FAILED) {
                log.info("Transitioning partition {} from {} to {} due to {}{}",
                        tp, currentState, targetState, errorMessage,
                        nonRetryable ? " (non-retryable error)" : " (retryable error)");
            } else {
                log.info("Transitioning partition {} from {} to {}", tp, currentState, targetState);
            }

            MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
            MirrorStateWrite write = new MirrorStateWrite(tp.partition(), targetState,
                    mirrorCache.getLeaderEpoch(tp),
                    mirrorCache.getPartitionMetadata(mp).stateEpoch(),
                    null, errorMessage, nonRetryable);

            if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
                localWrites.computeIfAbsent(tp.topic(), k -> new HashSet<>()).add(write);
            } else {
                remoteWrites.computeIfAbsent(tp.topic(), k -> new HashSet<>()).add(write);
            }
        }

        // Phase 2: Write to local coordinator and handle completion
        if (!localWrites.isEmpty()) {
            writeStateToLocalCoordinator(mirrorName, localWrites)
                .thenCompose(data -> {
                    data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                        TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                        Throwable partitionEx = null;
                        if (partition.errorCode() != Errors.NONE.code()) {
                            partitionEx = Errors.forCode(partition.errorCode()).exception();
                        }
                        onLocalWriteComplete(mirrorName, tp, targetState,  errorMessage, nonRetryable, partitionEx);
                    }));
                    return CompletableFuture.completedFuture(null);
                });
        }

        // Phase 3: Write to remote coordinator and handle completion
        if (!remoteWrites.isEmpty()) {
            writeStateToRemoteCoordinator(mirrorName, remoteWrites, Set.of())
                .thenCompose(res -> {
                    res.data().topics().forEach(topic -> topic.partitions().forEach(partition -> {
                        TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                        onRemoteWriteComplete(mirrorName, tp, targetState, errorMessage, nonRetryable, partition);
                    }));
                    return CompletableFuture.completedFuture(null);
                });
        }
    }

    /** Triggers per-partition side effects (actions) after local coordinator write completes. */
    private void onLocalWriteComplete(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                      String errorMessage, boolean nonRetryable, Throwable ex) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        if (ex != null) {
            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            if (cause instanceof CoordinatorLoadInProgressException) {
                log.debug("Local coordinator write for partition {} failed. Reason: Controller loading. Retrying in {} ms.",
                        tp, COORD_LOADING_RETRY_BACKOFF_MS);
                scheduler.scheduleOnce("write-retry-" + tp,
                        () -> persistState(mirrorName, tp, state, errorMessage, nonRetryable),
                        COORD_LOADING_RETRY_BACKOFF_MS);
                return;
            }
            if (cause instanceof FencedLeaderEpochException || cause instanceof FencedStateEpochException) {
                log.debug("Local coordinator write for partition {} failed. Reason: Stale epoch. Retrying.", tp);
                readAndRetryTransition(mirrorName, tp, state, errorMessage, nonRetryable);
                return;
            }
            if (state != MirrorPartitionState.FAILED) {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage(), false);
            }
            return;
        }
        if (mirrorCache.getPartitionMetadata(mp).state() == state) {
            onStateTransition(mirrorName, tp, state);
        }
    }

    /** Triggers per-partition side effects (actions) after remote coordinator write completes. */
    private void onRemoteWriteComplete(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                       String errorMessage, boolean nonRetryable,
                                       WriteMirrorStatesResponseData.PartitionResult part) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        if (part.errorCode() == Errors.NONE.code()) {
            MirrorPartitionMetadata existing = mirrorCache.getPartitionMetadata(mp);
            mirrorCache.updatePartitionMetadata(mp,
                    new MirrorPartitionMetadata.Builder(existing)
                            .withState(state)
                            .withStateEpoch(part.stateEpoch())
                            .build());
            onStateTransition(mirrorName, tp, state);
        } else if (part.errorCode() == Errors.COORDINATOR_LOAD_IN_PROGRESS.code()) {
            log.debug("Remote coordinator write for partition {} failed. Reason: Controller loading. Retrying in {} ms.",
                    tp, COORD_LOADING_RETRY_BACKOFF_MS);
            scheduler.scheduleOnce("write-retry-" + tp,
                    () -> persistState(mirrorName, tp, state, errorMessage, nonRetryable),
                    COORD_LOADING_RETRY_BACKOFF_MS);
        } else if (part.errorCode() == Errors.FENCED_LEADER_EPOCH.code()
                || part.errorCode() == Errors.FENCED_STATE_EPOCH.code()) {
            log.debug("Remote coordinator write for partition {} failed. Reason: Stale epoch. Retrying.", tp);
            readAndRetryTransition(mirrorName, tp, state, errorMessage, nonRetryable);
        } else {
            log.error("Remote coordinator write for partition {} failed with error code {}", tp, part.errorCode());
        }
    }

    private void persistState(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                              String errorMessage, boolean nonRetryable) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        var curState = mirrorCache.getPartitionMetadata(mp);
        int leaderEpoch = mirrorCache.getLeaderEpoch(tp);
        MirrorStateWrite write = new MirrorStateWrite(tp.partition(), state, leaderEpoch, curState.stateEpoch(),
                null, errorMessage, nonRetryable);
        if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            writeStateToLocalCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)))
                .thenCompose(data -> {
                    data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                        Throwable partitionEx = null;
                        if (partition.errorCode() != Errors.NONE.code()) {
                            partitionEx = Errors.forCode(partition.errorCode()).exception();
                        }
                        onLocalWriteComplete(mirrorName, tp, state, errorMessage, nonRetryable, partitionEx);
                    }));
                    return CompletableFuture.completedFuture(null);
                });
        } else {
            writeStateToRemoteCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)), Set.of())
                .thenCompose(res -> {
                    res.data().topics().forEach(topic -> topic.partitions().forEach(partition -> {
                        onRemoteWriteComplete(mirrorName, tp, state, errorMessage, nonRetryable, partition);
                    }));
                    return CompletableFuture.completedFuture(null);
                });
        }
    }

    private void readAndRetryTransition(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                        String errorMessage, boolean nonRetryable) {
        Consumer<ReadMirrorStatesResponse> onReadComplete = res ->
                res.data().topics().forEach(topic -> topic.partitions().forEach(partition -> {
                    if (partition.errorCode() == Errors.NONE.code()) {
                        var curState = pendingStateTransitions.get(tp);
                        if (curState != state) {
                            log.debug("Skipping transition to {} for partition {}. Reason: Already transitioning to {}.",
                                    curState, tp, state);
                            return;
                        }
                        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
                        mirrorCache.updatePartitionMetadata(mp,
                                new MirrorPartitionMetadata.Builder()
                                        .withState(MirrorPartitionState.fromValue(partition.state()))
                                        .withStateEpoch(partition.stateEpoch())
                                        .withLastPosition(new EpochOffset(partition.lastMirrorEpoch(), partition.lastMirrorOffset()))
                                        .withErrorMessage(partition.errorMessage())
                                        .withRetryAttempt(partition.retryAttempt())
                                        .withPrevState(MirrorPartitionState.fromValue(partition.previousState()))
                                        .build());
                        persistState(mirrorName, tp, state, errorMessage, nonRetryable);
                    }
                }));

        Map<String, Set<Integer>> partitions = Map.of(tp.topic(), Set.of(tp.partition()));
        if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            coordinatorReader.ifPresent(reader ->
                    reader.readPartitionStates(mirrorName, partitions).whenComplete((data, ex) -> {
                        onReadComplete.accept(new ReadMirrorStatesResponse(data));
                    }));
        } else {
            readStateFromRemoteCoordinator(mirrorName, partitions).whenComplete((data, ex) -> {
                onReadComplete.accept(data);
            });
        }
    }

    /**
     * Dispatches side effects after a coordinator write.
     * Each state triggers a specific action.
     */
    private void onStateTransition(String mirrorName, TopicPartition tp, MirrorPartitionState newState) {
        switch (newState) {
            case LOG_ALIGNMENT:
                scheduleTruncation(mirrorName, tp);
                break;
            case EPOCH_FENCING:
                handleEpochFencing(mirrorName, tp);
                break;
            case MIRRORING:
                handleMirroring(mirrorName, tp);
                break;
            case ULE_RECOVERY:
                handleUleRecovery(mirrorName, tp);
                break;
            case PAUSING:
                pausePartition(mirrorName, tp);
                break;
            case STOPPING:
                stopPartition(mirrorName, tp);
                break;
            case PAUSED, STOPPED:
                break;
            case FAILED:
                schedulePartitionRetry(mirrorName, tp);
                break;
            default:
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    // ===== STATE TRANSITION ACTIONS ==================================================================================

    private void scheduleTruncation(String mirrorName, TopicPartition topicPartition) {
        final Consumer<TopicPartition> callback =
                tp -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null, false);
        scheduler.scheduleOnce("truncation-" + mirrorName + "-" + topicPartition,
                () -> {
                    try {
                        var sourceMirrors = sourceSyncer.listSourceClusterMirrors(mirrorName);
                        if (sourceSyncer.hasMirrorLoop(mirrorName, topicPartition, sourceMirrors)) {
                            transitionTo(mirrorName, Set.of(topicPartition), MirrorPartitionState.FAILED,
                                    "Detected mirror loop for mirror: " + mirrorName, false);
                            return;
                        }
                        sendLastMirrorEpochLookup(mirrorName, topicPartition, sourceMirrors)
                                .whenComplete((offsetEpochs, rawError) -> {
                                    if (rawError != null) {
                                        Throwable error = rawError instanceof CompletionException && rawError.getCause() != null
                                                ? rawError.getCause() : rawError;
                                        Throwable root = error.getCause() != null ? error.getCause() : error;
                                        if (error instanceof UnsupportedVersionException
                                                || root instanceof UnsupportedVersionException) {
                                            log.warn("The source cluster doesn't support DescribeClusterMirror API. " +
                                                    "Replication will be one-way without failback.");
                                            replicaManagerSupplier.get().maybeTruncateForLeaderEpoch(
                                                    Map.of(topicPartition, new EpochOffset(-1, -1)), callback);
                                        } else {
                                            log.warn("Failed to truncate {} to last known position", topicPartition, error);
                                            transitionTo(mirrorName, Set.of(topicPartition), MirrorPartitionState.FAILED,
                                                    error.getMessage(), false);
                                        }
                                        return;
                                    }
                                    if (!offsetEpochs.containsKey(topicPartition)) {
                                        log.info("No epoch returned for {}, mirroring from scratch", topicPartition);
                                        offsetEpochs.put(topicPartition, new EpochOffset(-1, -1));
                                    }
                                    replicaManagerSupplier.get().maybeTruncateForLeaderEpoch(
                                            offsetEpochs, callback);
                                });
                    } catch (Exception e) {
                        log.warn("Failed to truncate to last known position for mirror {}", mirrorName, e);
                        transitionTo(mirrorName, Set.of(topicPartition), MirrorPartitionState.FAILED, e.getMessage(), false);
                    }
                }, 0);
    }

    private void handleEpochFencing(String mirrorName, TopicPartition tp) {
        scheduleBumpLeaderEpoch(mirrorName, tp)
                .thenRun(() -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null, false))
                .exceptionally(ex -> {
                    transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage(), false);
                    return null;
                });
    }

    private void handleMirroring(String mirrorName, TopicPartition tp) {
        replicaManagerSupplier.get().maybeCreateMirrorFetchers(mirrorName, Set.of(tp));
    }

    private void handleUleRecovery(String mirrorName, TopicPartition tp) {
        replicaManagerSupplier.get().awaitReplicaConvergence(tp)
                .thenRun(() -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null, false));
    }

    private void pausePartition(String mirrorName, TopicPartition tp) {
        replicaManagerSupplier.get().mirrorFetcherManager()
            .removeFetcherForPartitions(CollectionConverters.asScala(Set.of(tp)));
        transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED, null, false);
    }

    /**
     * Handles the STOPPING lifecycle: removes fetchers, updates the last mirror position,
     * bumps the leader epoch, aborts ongoing transactions, writes PID reset record,
     * and finally transitions to STOPPED. On any failure, transitions to FAILED.
     */
    private void stopPartition(String mirrorName, TopicPartition tp) {
        ReplicaManager rm = replicaManagerSupplier.get();
        rm.mirrorFetcherManager().removeFetcherForPartitions(CollectionConverters.asScala(Set.of(tp)));
        var logOpt = rm.getPartitionOrException(tp).log();
        int latestEpoch = logOpt.isDefined() ? logOpt.get().latestEpoch().orElse(-1) : -1;
        long latestOffset = logOpt.isDefined() ? logOpt.get().logEndOffset() : -1L;
        EpochOffset lastMirrorPosition = new EpochOffset(latestEpoch, latestOffset);

        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        MirrorPartitionMetadata existing = mirrorCache.getPartitionMetadata(mp);
        mirrorCache.updatePartitionMetadata(mp,
                new MirrorPartitionMetadata.Builder(existing)
                        .withLastPosition(lastMirrorPosition)
                        .build());
        CompletableFuture<Void> writePositionFuture;
        if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            writePositionFuture = coordinatorWriter.get().writeLastMirrorPositions(mirrorName, Map.of(tp, lastMirrorPosition));
        } else {
            writeStateToRemoteCoordinator(mirrorName,
                Map.of(tp.topic(), Set.of(new MirrorStateWrite(tp.partition(), null, -1, -1, lastMirrorPosition, null, false))),
                Set.of());
            writePositionFuture = CompletableFuture.completedFuture(null);
        }

        var latestLocalEpoch = replicaManagerSupplier.get().logManager().getLog(tp, false).get().latestEpoch().orElse(-1);
        writePositionFuture
            .thenCompose(v -> sendBumpLeaderEpochs(Map.of(tp, latestLocalEpoch)))
            .thenCompose(v -> abortOngoingTransactions(tp))
            .thenCompose(v -> writePidResetRecord(mirrorName, tp))
            .thenAccept(v -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED, null, false))
            .exceptionally(ex -> {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage(), false);
                return null;
            });
    }

    public CompletableFuture<Void> abortOngoingTransactions(TopicPartition tp) {
        ReplicaManager rm = replicaManagerSupplier.get();
        var record = rm.getLog(tp).map(UnifiedLog::buildEndTransactionRecords);
        if (!record.isDefined() || record.get().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (MemoryRecords memRecords : record.get()) {
            CompletableFuture<Void> batchFuture = new CompletableFuture<>();
            rm.appendRecords(
                    5000L,
                    (short) -1,
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(rm.topicIdPartition(tp), memRecords)),
                    partitionResponses -> {
                        batchFuture.complete(null);
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of()));
            futures.add(batchFuture);
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> writePidResetRecord(String mirrorName, TopicPartition tp) {
        String sourceClusterId = mirrorCache.getSourceClusterId(mirrorName);
        if (sourceClusterId == null) {
            log.warn("Source cluster ID not available for mirror {}. Skipping PID reset record.", mirrorName);
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        appendPidResetRecord(tp, time.milliseconds())
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("Failed to write PID reset record for partition {}", tp, ex);
                        scheduler.scheduleOnce("pid-reset-retry-" + tp,
                                () -> writePidResetRecord(mirrorName, tp).thenAccept(r -> result.complete(null)), 5000);
                    } else {
                        result.complete(null);
                    }
                });
        return result;
    }

    public CompletableFuture<Void> appendPidResetRecord(TopicPartition tp, long timestampMs) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ReplicaManager rm = replicaManagerSupplier.get();
        CompletableFuture<ProduceResponse.PartitionResponse> future = new CompletableFuture<>();
        MirrorPidResetRecord pidResetRecord = new MirrorPidResetRecord()
                .setVersion(ControlRecordUtils.MIRROR_PID_RESET_CURRENT_VERSION);
        try {
            var topicIdPartition = rm.topicIdPartition(tp);
            int bufferSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + 256;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            MemoryRecords records = MemoryRecords.withMirrorPidResetRecord(
                    0, timestampMs, 0, buffer, pidResetRecord);
            rm.appendRecords(
                    5000L,
                    (short) -1,
                    true,
                    AppendOrigin.COORDINATOR,
                    CollectionConverters.asScala(Map.of(topicIdPartition, records)),
                    partitionResponses -> {
                        partitionResponses.foreach(partitionRes -> {
                            future.complete(partitionRes._2);
                            return null;
                        });
                        return null;
                    },
                    ignored -> null,
                    RequestLocal.noCaching(),
                    CollectionConverters.asScala(Map.of()));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        future.whenComplete((pr, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else if (pr == null || pr.error.code() != 0) {
                String errorMsg = pr != null ? pr.error.message() : "no response";
                result.completeExceptionally(new RuntimeException(
                        "PID reset record error for partition " + tp + ": " + errorMsg));
            } else {
                result.complete(null);
            }
        });
        return result;
    }

    private void schedulePartitionRetry(String mirrorName, TopicPartition tp) {
        ClusterMirrorConfig mirrorConfig = brokerConfig.mirrorConfig();
        int maxAttempts = mirrorConfig.failedRetryMaxAttempts();
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        MirrorPartitionMetadata mpm = mirrorCache.getPartitionMetadata(mp);
        int attempt = mpm.retryAttempt() != 0 ? mpm.retryAttempt() : 1;
        if (attempt == NON_RETRYABLE_ATTEMPT) {
            log.debug("Skipping retry for partition {} (non-retryable error)", tp);
            return;
        }
        if (attempt >= maxAttempts) {
            log.error("Partition {} exceeded max retry attempts ({}), requires manual intervention",
                tp, maxAttempts);
            return;
        }
        ExponentialBackoff backoff = new ExponentialBackoff(
            mirrorConfig.failedRetryInitialBackoffMs(),
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            mirrorConfig.failedRetryMaxBackoffMs(),
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        long delay = backoff.backoff(attempt);
        MirrorPartitionState targetState = (mpm.prevState() == null || mpm.prevState() == MirrorPartitionState.UNKNOWN)
            ? MirrorPartitionState.LOG_ALIGNMENT : mpm.prevState();
        log.info("Scheduling retry #{} for partition {} in {} ms targeting {}", attempt, tp, delay, targetState);
        scheduler.scheduleOnce("failed-retry-" + tp,
            () -> transitionTo(mirrorName, Set.of(tp), targetState, null, false), delay);
    }

    // ===== COORDINATOR OPERATIONS ==========================================================================================

    /**
     * Reads mirror partition states from the local coordinator via
     * {@link MetadataManagerBridge.CoordinatorReader}, then applies appropriate state transitions.
     * If a shard is still loading, the coordinator responds with
     * {@link CoordinatorLoadInProgressException} and the transition is skipped; it will be
     * retried once {@link #onShardLoaded} re-evaluates local leader partitions for that shard.
     */
    private CompletableFuture<ReadMirrorStatesResponse> readStateFromLocalCoordinator(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        return coordinatorReader.map(reader -> reader.readPartitionStates(mirrorName, partitions)
                .thenApply(ReadMirrorStatesResponse::new)
                .exceptionally(ex -> {
                    Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
                    if (cause instanceof CoordinatorLoadInProgressException) {
                        log.debug("Failed to read local state for partitions {} (shard loading).", partitions);
                    } else {
                        log.warn("Failed to read local state for partitions {}. {}", partitions, cause.getMessage());
                    }
                    return new ReadMirrorStatesResponse(new ReadMirrorStatesResponseData());
                })).orElseGet(() -> CompletableFuture.completedFuture(new ReadMirrorStatesResponse(new ReadMirrorStatesResponseData())));

    }

    /** Writes mirror partition states to local coordinator, batching all writes. */
    private CompletableFuture<WriteMirrorStatesResponseData> writeStateToLocalCoordinator(
            String mirrorName, Map<String, Set<MirrorStateWrite>> stateWrites) {
        log.debug("Writing states to local coordinator for mirror {}", mirrorName);

        if (coordinatorWriter.isEmpty()) {
            return CompletableFuture.completedFuture(new WriteMirrorStatesResponseData());
        }

        return coordinatorWriter.get().writePartitionStates(mirrorName, stateWrites);
    }

    /**
     * Resolves the coordinator node for a partition via local metadata.
     * Returns {@link Node#noNode()} if metadata is unavailable.
     */
    private Node findCoordinatorNode(MirrorPartition mp) {
        try {
            if (coordPartFinder.isEmpty() || !mirrorCache.clusterHasTopic(MIRROR_STATE_TOPIC_NAME)) {
                return Node.noNode();
            }

            var listenerName = brokerConfig.interBrokerListenerName();
            List<MetadataResponseData.MetadataResponseTopic> topicMetadata = mirrorCache.getTopicMetadata(
                    Set.of(MIRROR_STATE_TOPIC_NAME), listenerName, false, false);

            if (topicMetadata == null || topicMetadata.isEmpty() || topicMetadata.get(0).errorCode() != Errors.NONE.code()) {
                return Node.noNode();
            }

            int partition = coordPartFinder.get().apply(mp);
            return topicMetadata.get(0).partitions().stream()
                    .filter(p -> p.partitionIndex() == partition && p.leaderId() != MetadataResponse.NO_LEADER_ID)
                    .findFirst()
                    .flatMap(p -> mirrorCache.getAliveBrokerNode(p.leaderId(), listenerName))
                    .orElse(Node.noNode());
        } catch (Exception e) {
            log.warn("Exception while getting mirror coordinator", e);
            return Node.noNode();
        }
    }

    /**
     * Read mirror partition states from remote coordinators, batching requests per coordinator node.
     * Updates the local {@link MirrorMetadataCache} with each response, then returns a merged response
     * after all nodes have replied.
     */
    public CompletableFuture<ReadMirrorStatesResponse> readStateFromRemoteCoordinator(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        log.debug("Reading states from remote coordinator: {} {}", mirrorName, partitions);

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<ReadMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(topic), part);
                Node coordinatorNode = findCoordinatorNode(mp);
                if (coordinatorNode.equals(Node.noNode())) {
                    log.warn("Coordinator is not available for partition {}-{}", topic, part);
                    return;
                }

                ReadMirrorStatesRequestData.PartitionData partitionData = new ReadMirrorStatesRequestData.PartitionData();
                partitionData.setPartitionIndex(part);

                nodeToTopicPartitions
                        .computeIfAbsent(coordinatorNode, k -> new HashMap<>())
                        .computeIfAbsent(topic, k -> new ArrayList<>())
                        .add(partitionData);
            });
        });

        if (nodeToTopicPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(new ReadMirrorStatesResponse(new ReadMirrorStatesResponseData()));
        }

        // Collect all node responses, complete future once with merged result
        ReadMirrorStatesResponseData merged = new ReadMirrorStatesResponseData();
        AtomicInteger remaining = new AtomicInteger(nodeToTopicPartitions.size());
        CompletableFuture<ReadMirrorStatesResponse> future = new CompletableFuture<>();

        // Send one batched request per coordinator node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            ReadMirrorStatesRequestData data = new ReadMirrorStatesRequestData().setMirrorName(mirrorName);
            List<ReadMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                    topicDataList.add(new ReadMirrorStatesRequestData.TopicMetadata()
                            .setTopicName(topic)
                            .setPartitions(partitionDataList)));

            data.setTopics(topicDataList);

            interBrokerSender.enqueue(new RequestAndCompletionHandler(
                    time.milliseconds(),
                    node,
                    new ReadMirrorStatesRequest.Builder(data),
                    response -> {
                        if (response.responseBody() instanceof ReadMirrorStatesResponse readMirrorStatesResponse) {
                            log.debug("Read states from remote coordinator completed: {}", response.responseBody());

                            readMirrorStatesResponse.data().topics().forEach(topic ->
                                topic.partitions().forEach(partition -> {
                                    MirrorPartition mp = MirrorPartition.of(
                                            mirrorName, mirrorCache.getTopicId(topic.topicName()), partition.partitionIndex());
                                    mirrorCache.updatePartitionMetadata(mp,
                                            new MirrorPartitionMetadata.Builder()
                                                    .withState(MirrorPartitionState.fromValue(partition.state()))
                                                    .withStateEpoch(partition.stateEpoch())
                                                    .withLastPosition(new EpochOffset(partition.lastMirrorEpoch(),
                                                            partition.lastMirrorOffset()))
                                                    .withErrorMessage(partition.errorMessage())
                                                    .withRetryAttempt(partition.retryAttempt())
                                                    .withPrevState(MirrorPartitionState.fromValue(partition.previousState()))
                                                    .build());
                                }));

                            synchronized (merged) {
                                merged.topics().addAll(readMirrorStatesResponse.data().topics());
                            }
                        } else {
                            log.warn("Unexpected response type from coordinator {}: {}", node, response.responseBody());
                        }

                        if (remaining.decrementAndGet() == 0) {
                            future.complete(new ReadMirrorStatesResponse(merged));
                        }
                    }
            ));
        });

        return future;
    }

    /** Reads mirror offsets from remote leaders, batching requests per leader node. */
    public CompletableFuture<ReadMirrorOffsetsResponse> readOffsetsFromRemoteLeaders(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        log.debug("Reading offsets from remote leaders: {} {}", mirrorName, partitions);

        // Group partitions by leader node for batching
        ListenerName listenerName = brokerConfig.interBrokerListenerName();
        Map<Node, Map<String, List<Integer>>> nodeToTopicPartitions = new HashMap<>();

        partitions.forEach((topic, parts) -> {
            parts.forEach(part -> {
                Optional<Node> leaderOpt = mirrorCache.getLeaderEndpoint(topic, part, listenerName);
                if (leaderOpt.isEmpty() || leaderOpt.get().equals(Node.noNode())) {
                    log.warn("Leader is not available for partition {}-{}", topic, part);
                    return;
                }

                nodeToTopicPartitions
                        .computeIfAbsent(leaderOpt.get(), k -> new HashMap<>())
                        .computeIfAbsent(topic, k -> new ArrayList<>())
                        .add(part);
            });
        });

        if (nodeToTopicPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(new ReadMirrorOffsetsResponse(new ReadMirrorOffsetsResponseData()));
        }

        CompletableFuture<ReadMirrorOffsetsResponse> resultFuture = new CompletableFuture<>();

        // Collect all node responses, complete future once with merged result
        ReadMirrorOffsetsResponseData merged = new ReadMirrorOffsetsResponseData();
        AtomicInteger remaining = new AtomicInteger(nodeToTopicPartitions.size());

        // Send one batched request per leader node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            ReadMirrorOffsetsRequestData data = new ReadMirrorOffsetsRequestData().setMirrorName(mirrorName);
            List<ReadMirrorOffsetsRequestData.TopicData> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionList) ->
                    topicDataList.add(new ReadMirrorOffsetsRequestData.TopicData()
                            .setTopicName(topic)
                            .setPartitions(partitionList)));

            data.setTopics(topicDataList);

            interBrokerSender.enqueue(new RequestAndCompletionHandler(
                    time.milliseconds(),
                    node,
                    new ReadMirrorOffsetsRequest.Builder(data),
                    response -> {
                        if (response.responseBody() instanceof ReadMirrorOffsetsResponse readOffsetsResponse) {
                            log.debug("Read offsets from remote leader completed: {}", response.responseBody());

                            synchronized (merged) {
                                merged.topics().addAll(readOffsetsResponse.data().topics());
                            }
                        } else {
                            log.warn("Unexpected response type from leader {}: {}", node, response.responseBody());
                        }

                        if (remaining.decrementAndGet() == 0) {
                            resultFuture.complete(new ReadMirrorOffsetsResponse(merged));
                        }
                    }
            ));
        });

        return resultFuture;
    }

    /** Writes mirror partition states to remote coordinators, batching requests per coordinator node. */
    public CompletableFuture<WriteMirrorStatesResponse> writeStateToRemoteCoordinator(
            String mirrorName, Map<String, Set<MirrorStateWrite>> topicMetadata, Set<String> stoppedTopics) {
        log.debug("Writing states to remote coordinators for mirror {}. Topic metadata: {}, Stopped topics: {}.",
                mirrorName, topicMetadata, stoppedTopics);

        CompletableFuture<WriteMirrorStatesResponse> resultFuture = new CompletableFuture<>();

        // Group partitions by coordinator node for batching
        Map<Node, Map<String, List<WriteMirrorStatesRequestData.PartitionData>>> nodeToTopicPartitions = new HashMap<>();

        topicMetadata.forEach((topic, metadata) -> {
            metadata.forEach(m -> {
                MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(topic), m.partition());
                Node coordinatorNode = findCoordinatorNode(mp);
                if (coordinatorNode.equals(Node.noNode())) {
                    log.error("Coordinator not available for partition {}-{}", topic, m.partition());
                    return;
                }

                var partitionData = new WriteMirrorStatesRequestData.PartitionData();
                partitionData.setState(m.state() == null ? MirrorPartitionState.UNKNOWN.value() : m.state().value());
                partitionData.setLeaderEpoch(m.leaderEpoch());
                partitionData.setStateEpoch(m.stateEpoch());
                EpochOffset lm = m.lastMirrorPosition();
                partitionData.setLastMirrorEpoch(lm != null ? lm.epoch() : -1);
                partitionData.setLastMirrorOffset(lm != null ? lm.offset() : -1L);
                partitionData.setPartitionIndex(m.partition());
                partitionData.setErrorMessage(m.errorMessage());
                partitionData.setNonRetryable(m.nonRetryable());

                nodeToTopicPartitions
                    .computeIfAbsent(coordinatorNode, k -> new HashMap<>())
                    .computeIfAbsent(topic, k -> new ArrayList<>())
                    .add(partitionData);
            });
        });

        if (nodeToTopicPartitions.isEmpty()) {
            resultFuture.complete(new WriteMirrorStatesResponse(new WriteMirrorStatesResponseData()));
            return resultFuture;
        }

        // Send one batched request per coordinator node
        nodeToTopicPartitions.forEach((node, topicPartitionsMap) -> {
            WriteMirrorStatesRequestData data = new WriteMirrorStatesRequestData().setMirrorName(mirrorName);
            List<WriteMirrorStatesRequestData.TopicMetadata> topicDataList = new ArrayList<>();

            topicPartitionsMap.forEach((topic, partitionDataList) ->
                topicDataList.add(new WriteMirrorStatesRequestData.TopicMetadata()
                    .setTopicName(topic)
                    .setPartitions(partitionDataList)));

            data.setTopics(topicDataList);

            interBrokerSender.enqueue(new RequestAndCompletionHandler(
                time.milliseconds(),
                node,
                new WriteMirrorStatesRequest.Builder(data),
                response -> {
                    log.debug("Write states to remote coordinator completed: {}", response.responseBody());
                    if (response.responseBody() instanceof WriteMirrorStatesResponse writeMirrorStatesResponse) {
                        resultFuture.complete(writeMirrorStatesResponse);
                    }
                }
            ));
        });

        return resultFuture;
    }

    // ==== OTHER OPERATIONS ===========================================================================================

    public void scheduleSourceTopicMetadataRefresh(String mirrorName) {
        sourceSyncer.scheduleSourceTopicMetadataRefresh(mirrorName);
    }

    public void scheduleSourceClusterSync(long intervalMs) {
        sourceSyncer.scheduleSourceClusterSync(intervalMs);
    }

    private Map<String, Set<Integer>> collectRemotePartitions(String mirrorName,
                                                              Set<String> topics,
                                                              MetadataImage image) {
        Map<String, Set<Integer>> remotePartitions = new HashMap<>();
        for (String topic : topics) {
            TopicImage topicImage = image.topics().getTopic(topic);
            if (topicImage != null) {
                Uuid topicId = mirrorCache.getTopicId(topic);
                for (int i = 0; i < topicImage.partitions().size(); i++) {
                    if (!isLocalCoordinatorFor(mirrorName, topicId, i)) {
                        remotePartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(i);
                    }
                }
            }
        }
        return remotePartitions;
    }

    /** Resolves topic patterns from source cluster and fetches their descriptions. */
    public CompletableFuture<Map<String, TopicDescription>> resolvePatternsFromSrc(
            String mirrorName, List<String> topicPatterns) {
        if (topicPatterns == null || topicPatterns.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        Admin srcAdmin = getOrCreateSourceAdmin(mirrorName);

        return srcAdmin.listTopics().names().toCompletionStage().toCompletableFuture()
            .thenCompose(allSourceTopics -> {
                Set<String> matched = resolvePatterns(allSourceTopics, topicPatterns);
                if (matched.isEmpty()) {
                    return CompletableFuture.completedFuture(Map.of());
                }

                // Filter out topics matching the mirror's topics.exclude config
                ClusterMirrorConfig mirrorConfig = ClusterMirrorConfig.fromProperties(
                        mirrorCache.getResourceConfig(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName)));
                Pattern excludePattern = mirrorConfig.topicsExcludePattern();
                if (excludePattern != null) {
                    matched.removeIf(t -> excludePattern.matcher(t).matches());
                    if (matched.isEmpty()) {
                        return CompletableFuture.completedFuture(Map.of());
                    }
                }

                return srcAdmin.describeTopics(matched).allTopicNames()
                        .toCompletionStage().toCompletableFuture();
            });
    }

    /** Resolves topic patterns from destination cluster, filtered by state and excluding existing names. */
    public Set<String> resolvePatternsFromDst(String mirrorName, List<String> topicPatterns,
            Set<MirrorPartitionState> states, Set<String> existingNames) {
        if (topicPatterns == null || topicPatterns.isEmpty()) {
            return Set.of();
        }
        Set<String> mirrorTopics = mirrorCache.getMirrorTopics(mirrorName, states);
        Set<String> resolved = resolvePatterns(mirrorTopics, topicPatterns);
        resolved.removeAll(existingNames);
        return resolved;
    }

    private Set<String> resolvePatterns(Set<String> allTopics, List<String> topicPatterns) {
        MirrorUtils.validateRe2jPatterns(topicPatterns);
        Pattern compiled = MirrorUtils.compilePatternList(topicPatterns);
        if (compiled == null) {
            return Set.of();
        }
        return allTopics.stream()
                .filter(t -> compiled.matcher(t).matches())
                .collect(Collectors.toSet());
    }

    public CompletableFuture<Optional<Errors>> validateDeleteMirrorStates(DeleteClusterMirrorRequestData data) {
        Set<String> topics = mirrorCache.getMirrorTopics(data.mirrorName(),
                EnumSet.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED, MirrorPartitionState.STOPPED));
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED), false)
            .thenApply(result -> {
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validateStartMirrorStates(StartMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(StartMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED, MirrorPartitionState.UNKNOWN), true)
            .thenApply(result -> {
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validateStopMirrorStates(StopMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(StopMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED), false)
            .thenApply(result -> {
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validatePauseMirrorStates(PauseMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(PauseMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING), false)
            .thenApply(result -> {
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validateResumeMirrorStates(ResumeMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(ResumeMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.PAUSED), false)
            .thenApply(result -> {
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    /**
     * Validates partition states on the broker before forwarding to the controller.
     * The controller uses this offset for optimistic locking, rejecting the request
     * if any mirror state changed after the broker's validation.
     */
    private CompletableFuture<ValidationResult> validateMirrorStates(
            String mirrorName,
            Set<String> topicNames,
            Set<MirrorPartitionState> validStates,
            boolean skipMissingTopics) {
        MetadataImage currentImage = metadataImage;
        long validationOffset = currentImage.offset();
        Map<String, Set<Integer>> remotePartitions = new HashMap<>();

        Optional<Errors> localError = validateLocalPartitions(
                mirrorName, topicNames, validStates, skipMissingTopics, currentImage, remotePartitions);
        if (localError.isPresent()) {
            return CompletableFuture.completedFuture(new ValidationResult(validationOffset, localError));
        }

        if (remotePartitions.isEmpty()) {
            return CompletableFuture.completedFuture(new ValidationResult(validationOffset, Optional.empty()));
        }

        return readStateFromRemoteCoordinator(mirrorName, remotePartitions).thenApply(response ->
            new ValidationResult(validationOffset, validateRemotePartitions(response, validStates))
        );
    }

    private Optional<Errors> validateLocalPartitions(
            String mirrorName,
            Set<String> topicNames,
            Set<MirrorPartitionState> validStates,
            boolean skipMissingTopics,
            MetadataImage currentImage,
            Map<String, Set<Integer>> remotePartitions) {
        Set<Byte> validDesiredStateValues = validStates.stream()
                .map(MirrorPartitionState::value).collect(Collectors.toSet());

        for (String topic : topicNames) {
            TopicImage topicImage = currentImage.topics().getTopic(topic);
            if (topicImage == null) {
                if (!skipMissingTopics) {
                    log.error("Topic {} not found in metadata image", topic);
                    return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATE);
                }
                continue;
            }
            if (!validDesiredStateValues.contains(topicImage.desiredMirrorState())) {
                log.error("Topic {} desired state is {}, expected one of {}",
                        topic, MirrorPartitionState.fromValue(topicImage.desiredMirrorState()), validStates);
                return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATE);
            }
            for (int i = 0; i < topicImage.partitions().size(); i++) {
                if (isLocalCoordinatorFor(mirrorName, topicImage.id(), i)) {
                    MirrorPartitionMetadata cachedEntry = mirrorCache.getPartitionMetadata(
                            MirrorPartition.of(mirrorName, topicImage.id(), i));
                    MirrorPartitionState state = cachedEntry != null && cachedEntry.state() != null
                            ? cachedEntry.state() : MirrorPartitionState.UNKNOWN;
                    if (!validStates.contains(state)) {
                        log.error("Partition {}-{} is in {} state, expected one of {}", topic, i, state, validStates);
                        return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATE);
                    }
                } else if (metadataImage.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null) {
                    remotePartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(i);
                } else {
                    log.info("Topic {} is not created completely. Mirror state is UNKNOWN, passing validation.",
                            MIRROR_STATE_TOPIC_NAME);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Errors> validateRemotePartitions(
            ReadMirrorStatesResponse response,
            Set<MirrorPartitionState> validStates) {
        if (response.data().errorCode() != Errors.NONE.code()) {
            log.error("Error reading states from remote coordinator. Error code: {} and message: {}.",
                    response.data().errorCode(), response.data().errorMessage());
            return Optional.of(Errors.forCode(response.data().errorCode()));
        }
        for (var topicResult : response.data().topics()) {
            for (var partitionResult : topicResult.partitions()) {
                if (partitionResult.errorCode() != Errors.NONE.code()) {
                    log.error("Error reading state from remote coordinator for partition {}-{}. Error code: {}.",
                            topicResult.topicName(), partitionResult.partitionIndex(), partitionResult.errorCode());
                    return Optional.of(Errors.forCode(partitionResult.errorCode()));
                }
                MirrorPartitionState remoteState = MirrorPartitionState.fromValue(partitionResult.state());
                if (!validStates.contains(remoteState)) {
                    log.error("Remote partition {}-{} is in {} state, expected one of {}",
                            topicResult.topicName(), partitionResult.partitionIndex(), remoteState, validStates);
                    return Optional.of(Errors.INVALID_CLUSTER_MIRROR_STATE);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Validates if records in the given TopicPartition can be safely deleted.
     * @return true if the topic has no active mirror
     */
    public boolean validateDeleteRecords(TopicPartition tp) {
        MetadataImage currentImage = metadataImage;
        TopicImage topicImage = currentImage.topics().getTopic(tp.topic());

        // This shouldn't happen because we validate topic existence before. We perform the check anyway to avoid an NPE
        if (topicImage == null) {
            return false;
        }

        if (topicImage.mirrorName() == null || topicImage.mirrorName().isBlank()) {
            return true;
        }

        if (topicImage.desiredMirrorState() != MirrorPartitionState.STOPPED.value()) {
            return false;
        }

        Map<TopicPartition, MirrorPartitionState> currentState = mirrorCache.getPartitionStates(topicImage.mirrorName());
        return currentState.getOrDefault(tp, MirrorPartitionState.UNKNOWN) == MirrorPartitionState.STOPPED;
    }

    /**
     * Clears failed info from cache and persists the state to the appropriate coordinator (local or remote).
     * Only succeeds if partition is in MIRRORING state.
     */
    public void clearFailedStateAndPersist(String mirrorName, TopicPartition tp) {
        MirrorPartitionMetadata curState = mirrorCache.getPartitionMetadata(
                MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition()));
        MirrorPartitionState state = curState != null ? curState.state() : null;
        if (state != MirrorPartitionState.MIRRORING) {
            log.debug("Skipping clearing failed state for partition {}. Reason: Current state is {}.", tp, state);
            return;
        }

        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        MirrorPartitionMetadata existing = mirrorCache.getPartitionMetadata(mp);
        mirrorCache.updatePartitionMetadata(mp,
                new MirrorPartitionMetadata.Builder(existing)
                    .withErrorMessage(null)
                    .withRetryAttempt(0)
                    .withPrevState(null)
                    .build());

        int stateEpoch = mirrorCache.getPartitionMetadata(mp).stateEpoch();
        int leaderEpoch = mirrorCache.getLeaderEpoch(tp);
        MirrorStateWrite write = new MirrorStateWrite(tp.partition(), state, leaderEpoch, stateEpoch,
                null, curState.errorMessage(), curState.retryAttempt() == NON_RETRYABLE_ATTEMPT);

        if (isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            writeStateToLocalCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)))
                    .whenComplete((data, ex) -> onLocalWriteComplete(
                            mirrorName, tp, state, curState.errorMessage(),
                            curState.retryAttempt() == NON_RETRYABLE_ATTEMPT, ex));
        } else {
            writeStateToRemoteCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)), Set.of())
                    .thenAccept(res -> res.data().topics().forEach(topic -> topic.partitions().forEach(partition ->
                            onRemoteWriteComplete(mirrorName, tp, state, curState.errorMessage(),
                                    curState.retryAttempt() == NON_RETRYABLE_ATTEMPT, partition))));
        }
    }

    /**
     * Sends a last mirror epoch (LME) lookup request to the source cluster.
     * The log output is used by test_failover_failback test to verify failback behavior.
     */
    public CompletionStage<Map<TopicPartition, EpochOffset>> sendLastMirrorEpochLookup(
            String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors) {
        Admin admin = getOrCreateSourceAdmin(mirrorName);
        log.info("Sending LME lookup request for partition {} in mirror {}", tp, mirrorName);

        Map<String, List<Integer>> topicPartitions = Map.of(tp.topic(), List.of(tp.partition()));
        DescribeClusterMirrorsOptions options = new DescribeClusterMirrorsOptions()
                .clusterId(clusterId)
                .includeMirrorState(true);
        DescribeClusterMirrorsResult result = admin.describeClusterMirrors(null, topicPartitions, options);

        var describeFuture = result.allDescriptions().toCompletionStage().toCompletableFuture();
        var lastMirrorPositionFuture = result.lastMirrorPositions().toCompletionStage().toCompletableFuture();
        return describeFuture.thenApply(desc -> {
            validateSourcePartitionIsStopped(desc, sourceMirrors, tp);
            return null;
        })
        .thenCompose(__ -> lastMirrorPositionFuture)
        .thenApply(lastMirrorPositions -> {
            log.info("Received LME lookup response for partition {} in mirror {}: {}",
                    mirrorName, tp, lastMirrorPositions);
            return lastMirrorPositions;
        })
        .orTimeout(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Validates that all partitions about to be mirrored are in STOPPED state on the source cluster,
     * for any source mirror that was previously mirroring from this local cluster. This prevents
     * starting replication while the reverse direction is still active.
     */
    private void validateSourcePartitionIsStopped(
            Map<String, ClusterMirrorDescription> sourceDescription,
            Collection<ClusterMirrorListing> sourceMirrors,
            TopicPartition tp) {
        List<String> localClusterSourceMirrors = sourceMirrors.stream()
                .filter(sm -> sm.sourceClusterId().equals(clusterId))
                .map(ClusterMirrorListing::mirrorName)
                .toList();

        for (String mirrorName : localClusterSourceMirrors) {
            ClusterMirrorDescription desc = sourceDescription.get(mirrorName);
            if (desc == null) {
                continue;
            }
            Set<ClusterMirrorDescription.LeaderStateDescription> leaderStates = desc.leaderStates().get(tp.topic());
            if (leaderStates == null) {
                continue;
            }
            boolean notStopped = leaderStates.stream()
                    .anyMatch(lsd -> lsd.topicPartition().equals(tp)
                            && !MirrorPartitionState.STOPPED.name().equals(lsd.state()));
            if (notStopped) {
                log.error("Source mirror(s) {} mirroring from this cluster ({}) have not stopped for partition {}",
                        localClusterSourceMirrors, clusterId, tp);
                throw new IllegalStateException("Source mirror(s) " + localClusterSourceMirrors
                        + " mirroring from this cluster (" + clusterId + ") have not stopped for partition " + tp);
            }
        }
    }

    /**
     * Writes coordinator records to recover FAILED mirror partitions before the
     * metadata change is processed. For each FAILED partition with a prevState,
     * writes the prevState with retryAttempt reset to 0, ensuring MMM reads the
     * correct state on the next metadata update without exhausted retry attempts.
     */
    public CompletableFuture<Void> writeRecoveryRecords(String mirrorName, Set<String> topics) {
        if (coordinatorWriter.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Map<String, Set<Integer>> remotePartitions = collectRemotePartitions(mirrorName, topics, metadataImage);

        CompletableFuture<Void> readRemoteFuture;
        if (!remotePartitions.isEmpty()) {
            readRemoteFuture = readStateFromRemoteCoordinator(mirrorName, remotePartitions).thenApply(v -> null);
        } else {
            readRemoteFuture = CompletableFuture.completedFuture(null);
        }

        return readRemoteFuture.thenCompose(v -> executeRecoveryWrites(mirrorName, topics, metadataImage));
    }

    private CompletableFuture<Void> executeRecoveryWrites(String mirrorName, Set<String> topics, MetadataImage image) {
        Map<String, Set<MirrorStateWrite>> localWrites = new HashMap<>();
        Map<String, Set<MirrorStateWrite>> remoteWrites = new HashMap<>();

        for (String topic : topics) {
            TopicImage topicImage = image.topics().getTopic(topic);
            if (topicImage == null) {
                continue;
            }
            for (int i = 0; i < topicImage.partitions().size(); i++) {
                MirrorPartition mp = MirrorPartition.of(mirrorName, topicImage.id(), i);
                MirrorPartitionMetadata mpm = mirrorCache.getPartitionMetadata(mp);
                if (mpm.state() == MirrorPartitionState.FAILED && mpm.prevState() != null) {
                    MirrorPartitionState targetState = mpm.prevState();
                    MirrorStateWrite write = new MirrorStateWrite(i, targetState,
                            mpm.lastPosition().epoch(), mpm.stateEpoch(), null, null, false);
                    if (isLocalCoordinatorFor(mirrorName, topicImage.id(), i)) {
                        localWrites.computeIfAbsent(topic, k -> new HashSet<>()).add(write);
                    } else {
                        remoteWrites.computeIfAbsent(topic, k -> new HashSet<>()).add(write);
                    }
                }
            }
        }

        CompletableFuture<Void> localWriteFuture = localWrites.isEmpty() ?
                CompletableFuture.completedFuture(null) :
                coordinatorWriter.get().writePartitionStates(mirrorName, localWrites)
                        .exceptionally(ex -> {
                            log.warn("Failed to write recover states for mirror {}: {}", mirrorName, ex.getMessage());
                            return null;
                        })
                        .thenApply(v -> null);

        if (remoteWrites.isEmpty()) {
            return localWriteFuture;
        }

        return localWriteFuture.thenCompose(res -> writeStateToRemoteCoordinator(mirrorName, remoteWrites, Set.of())
                .thenApply(response -> {
                    response.data().topics().forEach(t -> t.partitions().forEach(p -> {
                        if (p.errorCode() != Errors.NONE.code()) {
                            log.warn("Failed to write recover state for partition {}-{}: {}",
                                    t.topicName(), p.partitionIndex(), Errors.forCode(p.errorCode()));
                        }
                    }));
                    return null;
                }));
    }

    /** Schedules a source topic state sync followed by a leader epoch bump request. */
    public CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.scheduleOnce("bump-leader-epoch-" + tp, () -> {
            List<SourceTopicState> sourceTopicStates = sourceSyncer.refreshSourceTopicMetadata(mirrorName);
            maybeBumpLeaderEpochs(mirrorName, sourceTopicStates, Set.of(tp))
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            future.completeExceptionally(ex);
                        } else {
                            future.complete(null);
                        }
                    });
        });
        return future;
    }

    /** Sends a bump leader epoch request for the given source topic states and target partitions. */
    CompletableFuture<Void> maybeBumpLeaderEpochs(String mirrorName,
                                                  List<SourceTopicState> sourceTopicStates,
                                                  Set<TopicPartition> topicPartitions) {
        return sendBumpLeaderEpochs(buildSourceEpochBumpTargets(mirrorName, sourceTopicStates, topicPartitions))
                .whenComplete((v, ex) -> {
                    if (ex != null) log.warn("Failed to bump leader epoch for mirror {}", mirrorName, ex);
                });
    }

    private CompletableFuture<Void> sendBumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        if (partitionMinEpochs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.info("Sending bump leader epoch request: {}", partitionMinEpochs);
        CompletableFuture<Void> future = new CompletableFuture<>();

        List<BumpLeaderEpochsRequestData.TopicState> topicStates = new ArrayList<>();
        Map<String, Set<Integer>> partitions = new HashMap<>();
        partitionMinEpochs.keySet().forEach(
                tp -> partitions.computeIfAbsent(tp.topic(), key -> new HashSet<>()).add(tp.partition()));
        partitions.forEach((topic, parts) -> {
            BumpLeaderEpochsRequestData.TopicState topicState = new BumpLeaderEpochsRequestData.TopicState();
            List<BumpLeaderEpochsRequestData.LeaderEpochState> topicLeaderEpoch = new ArrayList<>();
            parts.forEach(partitionId -> {
                TopicPartition tp = new TopicPartition(topic, partitionId);
                topicLeaderEpoch.add(new BumpLeaderEpochsRequestData.LeaderEpochState()
                        .setMinLeaderEpoch(partitionMinEpochs.get(tp)).setPartitionIndex(partitionId));
            });
            topicState.setTopicName(topic).setPartitions(topicLeaderEpoch);
            topicStates.add(topicState);
        });

        pendingLeaderEpochBumps.add(new PendingLeaderEpochBump(future, new ConcurrentHashMap<>(partitionMinEpochs)));
        maybeCompletePendingEpochBumps();

        controllerClient.sendRequest(new BumpLeaderEpochsRequest.Builder(
                new BumpLeaderEpochsRequestData().setTopics(topicStates)
        ), new ControllerRequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                log.debug("Bump leader epoch response: {}", response);
            }

            @Override
            public void onTimeout() {
                log.warn("Bump leader epoch request timed out");
            }
        });
        return future;
    }

    private Map<TopicPartition, Integer> buildSourceEpochBumpTargets(String mirrorName,
                                                                     List<SourceTopicState> sourceTopicStates,
                                                                     Set<TopicPartition> topicPartitions) {
        Set<String> mirrorTopics = topicPartitions.isEmpty()
                ? mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING))
                : Set.of();
        Map<TopicPartition, Integer> leaderEpochFromMetadata = new HashMap<>();
        for (SourceTopicState ts : sourceTopicStates) {
            if (!ts.exists()) {
                continue;
            }
            if (!mirrorTopics.isEmpty() && !mirrorTopics.contains(ts.topic())) {
                continue;
            }
            collectEpochBumpTargets(ts, topicPartitions, leaderEpochFromMetadata);
        }
        if (!leaderEpochFromMetadata.isEmpty()) {
            log.info("Bumping leader epoch for partitions {}", leaderEpochFromMetadata);
        }
        return leaderEpochFromMetadata;
    }

    private void collectEpochBumpTargets(SourceTopicState topicInfo,
                                         Set<TopicPartition> topicPartitions,
                                         Map<TopicPartition, Integer> leaderEpochFromMetadata) {
        for (SourcePartitionState ps : topicInfo.partitions()) {
            TopicPartition tp = ps.topicPartition();
            if (!topicPartitions.isEmpty() && !topicPartitions.contains(tp)) {
                continue;
            }
            if (ps.leaderEpoch().isEmpty()) {
                continue;
            }
            TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
            if (topicImage == null || topicImage.partitions().get(tp.partition()) == null) {
                continue;
            }
            int epoch = ps.leaderEpoch().get();
            int localEpoch = topicImage.partitions().get(tp.partition()).leaderEpoch;
            if (epoch > localEpoch - LEADER_EPOCH_BUMP_THRESHOLD) {
                int newEpoch = Math.addExact(epoch, LEADER_EPOCH_BUMP_INCREMENT);
                leaderEpochFromMetadata.put(tp, newEpoch);
            }
        }
    }

    record ValidationResult(long stateOffset, Optional<Errors> error) { }
    record SourceTopicState(String topic, Uuid topicId, boolean exists, List<SourcePartitionState> partitions) { }
    record SourcePartitionState(TopicPartition topicPartition, Node leader, Optional<Integer> leaderEpoch) { }
    record PendingLeaderEpochBump(CompletableFuture<Void> future, Map<TopicPartition, Integer> partitionToEpoch) { }
}
