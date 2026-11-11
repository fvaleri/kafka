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
import kafka.server.mirror.MirrorSourceSyncer.SourcePartitionState;
import kafka.server.mirror.MirrorSourceSyncer.SourceTopicMetadata;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsResult;
import org.apache.kafka.clients.admin.ListClusterMirrorsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BumpLeaderEpochsRequestData;
import org.apache.kafka.common.message.DeleteClusterMirrorRequestData;
import org.apache.kafka.common.message.MirrorPidResetRecord;
import org.apache.kafka.common.message.PauseMirrorTopicsRequestData;
import org.apache.kafka.common.message.ResumeMirrorTopicsRequestData;
import org.apache.kafka.common.message.StartMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.BumpLeaderEpochsRequest;
import org.apache.kafka.common.requests.BumpLeaderEpochsResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ReadMirrorOffsetsResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinator.MirrorStateWrite;
import org.apache.kafka.coordinator.mirror.MetadataManagerBridge;
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
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MirrorUtils;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.server.mirror.MirrorPartitionMetadata.NON_RETRYABLE_ERROR;

/**
 * Handles mirror partition state transitions.
 * Starts periodic source cluster metadata synchronization.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class MirrorMetadataManager implements MetadataManagerBridge, MetadataPublisher, AutoCloseable {
    // Mirror config keys that do not affect source connections (no reconnect needed)
    private static final Set<String> SKIP_RECONNECT_MIRROR_CONFIGS = Set.of(
            ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG,
            ClusterMirrorConfig.ACLS_INCLUDE_CONFIG, ClusterMirrorConfig.ACLS_EXCLUDE_CONFIG);

    // Topic config keys that trigger mirror partition re-evaluation on change
    private static final Set<String> WATCHED_TOPIC_CONFIGS = Set.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);

    private static final long RETRY_BACKOFF_MS = 1_000;
    private static final int LEADER_EPOCH_BUMP_INCREMENT = 10;
    static final int LEADER_EPOCH_BUMP_THRESHOLD = 3;

    private final Logger log;
    private final String clusterId;
    private final KafkaConfig brokerConfig;
    private final int nodeId;
    private final String name;

    private final Supplier<ReplicaManager> replicaManagerSupplier;
    private final NodeToControllerChannelManager controllerClient;
    private final MirrorLeaderCache mirrorCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;
    private final Time time;

    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private volatile MirrorCoordinatorSender coordSender;
    private volatile MirrorSourceSyncer sourceSyncer;
    private volatile boolean isInitialized = false;

    private final Map<TopicPartition, MirrorPartitionState> pendingStateTransitions = new ConcurrentHashMap<>();
    private final Set<PendingLeaderEpochBump> pendingLeaderEpochBumps = ConcurrentHashMap.newKeySet();

    public MirrorMetadataManager(
        String clusterId,
        KafkaConfig brokerConfig,
        NodeToControllerChannelManager controllerClient,
        Supplier<ReplicaManager> replicaManagerSupplier,
        MirrorLeaderCache mirrorCache,
        Metrics metrics,
        Time time
    ) {
        this.clusterId = clusterId;
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        this.name = "[" + MirrorMetadataManager.class.getSimpleName() + " brokerId=" + brokerConfig.nodeId() + "] ";
        this.log = new LogContext(name).logger(MirrorMetadataManager.class);

        this.replicaManagerSupplier = replicaManagerSupplier;
        this.controllerClient = controllerClient;
        this.mirrorCache = mirrorCache;
        this.scheduler = new KafkaScheduler(1, true, "mirror-manager-");
        this.scheduler.startup();
        this.metrics = metrics;
        this.time = time;
    }

    @Override
    public String name() {
        return name;
    }

    // ===== LIFECYCLE MANAGEMENT ======================================================================================

    /**
     * Called by the MirrorCoordinator when startup is invoked.
     * Initializes source cluster metadata syncer and coordinator sender,
     * then processes any mirror leader partitions whose metadata deltas
     * arrived before initialization (and were therefore skipped by onMetadataUpdate).
     */
    @Override
    public void onCoordStartup(Function<MirrorPartition, Integer> coordPartFinder,
                               MetadataManagerBridge.CoordinatorReader coordinatorReader,
                               MetadataManagerBridge.CoordinatorWriter coordinatorWriter) {
        log.debug("Coordinator is starting up");
        this.coordSender = new MirrorCoordinatorSender(brokerConfig, () -> metadataImage, mirrorCache,
                coordPartFinder, coordinatorReader, coordinatorWriter, metrics, time);
        coordSender.startup();

        this.sourceSyncer = new MirrorSourceSyncer(brokerConfig, replicaManagerSupplier, controllerClient,
                mirrorCache, this::transitionTo, this::writeTombstoneRecords,
                coordSender::listLocalMirrorNames, this::maybeBumpLeaderEpochs);
        sourceSyncer.startup();

        this.isInitialized = true;

        processMirrorLeadersFromMetadata();
    }

    /**
     * Called after a MirrorCoordinator shard finishes loading.
     * Re-evaluates mirror leaders that map to this coordinator partition.
     */
    @Override
    public void onShardLoaded(int coordPartition) {
        log.debug("Coordinator shard {} loaded", coordPartition);
        if (!isInitialized || metadataImage == null || coordSender.coordPartFinder().isEmpty()) {
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
                        if (coordSender.coordPartFinder().get().apply(mp) == coordPartition) {
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
     * Called when cluster metadata is updated by the KRaft metadata publisher thread.
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
     * Called after a MirrorCoordinator shard finishes unloading.
     */
    @Override
    public void onShardUnloaded(int coordPartIndex, int numPartitions) {
        log.debug("Coordinator shard {} unloaded", coordPartIndex);
    }

    /**
     * Called by the MirrorCoordinator when shutdown is invoked.
     * Closes source Admin clients as they may hold pending operations.
     */
    @Override
    public void onCoordShutdown() {
        log.debug("Coordinator is shutting down");
        if (sourceSyncer != null) {
            sourceSyncer.closeAndRemoveSourceAdmins();
        }
    }

    /**
     * Called when broker is shutdown.
     * Closes all resources cleanly.
     */
    @Override
    public void close() throws Exception {
        coordSender.shutdown();
        if (sourceSyncer != null) {
            sourceSyncer.shutdown();
        }
        scheduler.shutdown();
        pendingStateTransitions.clear();
        clearAllPendingEpochBumps();
    }

    // ===== STATE TRANSITION OPERATIONS ===============================================================================

    /**
     * Scans the current metadata image for all mirror leader partitions and
     * processes state transitions for them. This catches partitions whose
     * leadership deltas arrived via onMetadataUpdate before isInitialized was
     * set, which would have been skipped.
     */
    private void processMirrorLeadersFromMetadata() {
        if (metadataImage == null) {
            return;
        }
        Set<String> configuredMirrors = mirrorCache.getConfiguredMirrorNames();
        Set<TopicPartition> mirrorLeaders = new HashSet<>();
        metadataImage.topics().topicsByName().forEach((topicName, topicImage) -> {
            if (topicImage.mirrorName() != null && configuredMirrors.contains(topicImage.mirrorName())) {
                topicImage.partitions().forEach((partitionId, partition) -> {
                    if (partition.leader == nodeId) {
                        mirrorLeaders.add(new TopicPartition(topicName, partitionId));
                    }
                });
            }
        });
        if (!mirrorLeaders.isEmpty()) {
            log.info("Processing mirror leader partition(s) from metadata: {}", mirrorLeaders);
            processStateTransitions(mirrorLeaders, metadataImage);
        }
    }


    /**
     * Collects mirror partitions that need a state transition and handles
     * mirror configuration side effects (connection teardown, tombstones).
     * <p>
     * Sources:
     *   1. Gained leader partitions belonging to a configured mirror
     *   2. Desired mirror state changes (start/stop/pause/resume)
     *   3. Mirror connection config changes (teardown + reconnect)
     *   4. Watched topic config changes (e.g. remote.storage.enable)
     */
    private Set<TopicPartition> collectPartitionsToTransition(MetadataDelta delta, MetadataImage image) {
        Set<String> mirrors = mirrorCache.getConfiguredMirrorNames();

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
            removePendingEpochBumpPartitions(Set.of(tp));
            if (!coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
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
                    mirrorCache.getTopicName(mp.topicId()).ifPresent(topicName -> {
                        TopicImage topicImage = image.topics().getTopic(topicName);
                        if (topicImage != null) {
                            var partitionReg = topicImage.partitions().get(mp.partition());
                            if (partitionReg != null && partitionReg.leader == nodeId) {
                                result.add(new TopicPartition(topicName, mp.partition()));
                            }
                        }
                    });
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
            if (sourceSyncer != null) {
                sourceSyncer.removeMirrorTopicMetrics(mirrorName);
            }
        }

        boolean isNewMirror = prevImage.configs().configProperties(resource).isEmpty();
        boolean needsReconnection = !isNewMirror && configDelta.changes().keySet().stream()
                .anyMatch(key -> !SKIP_RECONNECT_MIRROR_CONFIGS.contains(key));
        if (needsReconnection) {
            log.info("Mirror '{}' has config changes. Recreating connections.", mirrorName);
        }

        if (needsReconnection || mirrorDeleted) {
            mirrorCache.removeSourceClusterLeaders(mirrorName);
            if (sourceSyncer != null) {
                sourceSyncer.closeAndRemoveSourceAdmins(mirrorName);
            }
            var mirrorFetcherManager = replicaManagerSupplier.get().mirrorFetcherManager();
            mirrorFetcherManager.removeFetchersForMirror(mirrorName);
            mirrorFetcherManager.shutdownIdleFetcherThreads();
        }

        return (needsReconnection && !mirrorDeleted) ? Optional.of(mirrorName) : Optional.empty();
    }

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

            if (coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
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
        localPartitions.forEach(this::readLocalPartitionStates);

        // Phase 3: Read current state from remote coordinators and apply transitions
        remotePartitions.forEach(this::readRemotePartitionStates);
    }

    private void readLocalPartitionStates(String mirrorName, Map<String, Set<Integer>> partitions) {
        coordSender.readStateFromLocalCoordinator(mirrorName, partitions).whenComplete((res, ex) -> {
            if (ex != null) {
                scheduleRetry(mirrorName, () -> readLocalPartitionStates(mirrorName, partitions),
                        RETRY_BACKOFF_MS, "Local coordinator read failed: " + ex.getMessage());
                return;
            }
            if (res.data().errorCode() != Errors.NONE.code()) {
                log.warn("Local coordinator read failed with top-level error: {}",
                        Errors.forCode(res.data().errorCode()));
                scheduleRetry(mirrorName, () -> readLocalPartitionStates(mirrorName, partitions),
                        RETRY_BACKOFF_MS, "Local read top-level error: " + res.data().errorMessage());
                return;
            }
            Map<String, Set<Integer>> partitionsToRetry = new HashMap<>();
            res.data().topics().forEach(topic ->
                    topic.partitions().forEach(partition -> {
                        if (partition.errorCode() != Errors.NONE.code()) {
                            log.warn("Error reading local mirror state for partition {}-{}: {}",
                                    topic.topicName(), partition.partitionIndex(),
                                    Errors.forCode(partition.errorCode()));
                            partitionsToRetry.computeIfAbsent(topic.topicName(), k -> new HashSet<>())
                                    .add(partition.partitionIndex());
                            return;
                        }
                        handleReadStateResponse(mirrorName, topic.topicName(), partition.partitionIndex(),
                                MirrorPartitionState.fromValue(partition.state()));
                    }));
            if (!partitionsToRetry.isEmpty()) {
                scheduleRetry(mirrorName, () -> readLocalPartitionStates(mirrorName, partitionsToRetry),
                        RETRY_BACKOFF_MS, "Local read returned partition errors");
            }
        });
    }

    private void readRemotePartitionStates(String mirrorName, Map<String, Set<Integer>> partitions) {
        coordSender.readStateFromRemoteCoordinator(mirrorName, partitions).whenComplete((res, ex) -> {
            if (ex != null) {
                scheduleRetry(mirrorName, () -> readRemotePartitionStates(mirrorName, partitions),
                        RETRY_BACKOFF_MS, "Remote coordinator read failed: " + ex.getMessage());
                return;
            }
            if (res.data().errorCode() != Errors.NONE.code()) {
                log.warn("Remote coordinator read failed with top-level error: {}",
                        Errors.forCode(res.data().errorCode()));
                scheduleRetry(mirrorName, () -> readRemotePartitionStates(mirrorName, partitions),
                        RETRY_BACKOFF_MS, "Remote read top-level error: " + res.data().errorMessage());
                return;
            }
            Map<String, Set<Integer>> partitionsToRetry = new HashMap<>();
            res.data().topics().forEach(topic ->
                    topic.partitions().forEach(partition -> {
                        if (partition.errorCode() != Errors.NONE.code()) {
                            log.warn("Error reading remote mirror state for partition {}-{}: {}",
                                    topic.topicName(), partition.partitionIndex(),
                                    Errors.forCode(partition.errorCode()));
                            partitionsToRetry.computeIfAbsent(topic.topicName(), k -> new HashSet<>())
                                    .add(partition.partitionIndex());
                            return;
                        }
                        handleReadStateResponse(mirrorName, topic.topicName(), partition.partitionIndex(), MirrorPartitionState.fromValue(partition.state()));
                    }));
            if (!partitionsToRetry.isEmpty()) {
                scheduleRetry(mirrorName, () -> readRemotePartitionStates(mirrorName, partitionsToRetry),
                        RETRY_BACKOFF_MS, "Remote read returned partition errors");
            }
        });
    }

    private void handleReadStateResponse(String mirrorName, String topicName, int partitionInd, MirrorPartitionState currentState) {
        TopicPartition tp = new TopicPartition(topicName, partitionInd);
        TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
        MirrorPartitionState desiredState = topicImage != null ?
                MirrorPartitionState.fromValue(topicImage.desiredMirrorState()) : MirrorPartitionState.UNKNOWN;
        applyStateTransition(mirrorName, tp, currentState, desiredState);
    }

    /**
     * Applies the appropriate state transition based on current state and desired state.
     * The mirror partition state machine handles explicit transitions (stop/pause requests)
     * and automatic transitions (start mirroring, fail on errors).
     */
    private void applyStateTransition(String mirrorName,
                                      TopicPartition tp,
                                      MirrorPartitionState currentState,
                                      MirrorPartitionState desiredState) {
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
            transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, mpm.errorMessage(), mpm.retryAttempt() == NON_RETRYABLE_ERROR);
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
            // Re-assert the current state to trigger side effects and keep the state machine progressing
            transitionTo(mirrorName, Set.of(tp), currentState, null, false);
        }
    }

    /**
     * Writes a targetState transition for each partition, routing to either the local or remote
     * coordinator via {@link MirrorCoordinatorSender}. On successful write, dispatches side effects
     * through {@link #onStateTransition}.
     */
    public void transitionTo(String mirrorName,
                             Set<TopicPartition> topicPartitions,
                             MirrorPartitionState targetState,
                             String errorMessage,
                             boolean nonRetryable) {
        Map<String, Set<MirrorStateWrite>> localWrites = new HashMap<>();
        Map<String, Set<MirrorStateWrite>> remoteWrites = new HashMap<>();

        // Phase 1: Validate and prepare state writes, batching by coordinator location (local or remote)
        prepareStateWrites(mirrorName, topicPartitions, targetState,
                errorMessage, nonRetryable, localWrites, remoteWrites);

        // Phase 2: Write to local coordinator and handle completion
        if (!localWrites.isEmpty()) {
            coordSender.writeStateToLocalCoordinator(mirrorName, localWrites)
                .whenComplete((data, ex) -> {
                    if (ex != null) {
                        localWrites.forEach((topic, writes) -> writes.forEach(write ->
                            refreshAndPersistState(mirrorName, new TopicPartition(topic, write.partition()),
                                targetState, errorMessage, nonRetryable, ex.getMessage())));
                        return;
                    }
                    if (data.errorCode() != Errors.NONE.code()) {
                        log.warn("Local coordinator write failed with top-level error: {}",
                                Errors.forCode(data.errorCode()));
                        localWrites.forEach((topic, writes) -> writes.forEach(write -> {
                            TopicPartition tp = new TopicPartition(topic, write.partition());
                            scheduleRetry(tp.toString(),
                                () -> refreshAndPersistState(mirrorName, tp,
                                    targetState, errorMessage, nonRetryable, data.errorMessage()),
                                RETRY_BACKOFF_MS, "Local write top-level error: " + data.errorMessage());
                        }));
                        return;
                    }
                    data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                        TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                        Throwable partitionEx = null;
                        if (partition.errorCode() != Errors.NONE.code()) {
                            partitionEx = Errors.forCode(partition.errorCode()).exception();
                        }
                        onLocalWriteComplete(mirrorName, tp, targetState, errorMessage, nonRetryable, partition.stateEpoch(), partitionEx);
                    }));
                });
        }

        // Phase 3: Write to remote coordinator and handle completion
        if (!remoteWrites.isEmpty()) {
            coordSender.writeStateToRemoteCoordinator(mirrorName, remoteWrites)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        remoteWrites.forEach((topic, writes) -> writes.forEach(write ->
                            refreshAndPersistState(mirrorName, new TopicPartition(topic, write.partition()),
                                targetState, errorMessage, nonRetryable, ex.getMessage())));
                        return;
                    }
                    if (res.data().errorCode() != Errors.NONE.code()) {
                        log.warn("Remote coordinator write failed with top-level error: {}",
                                Errors.forCode(res.data().errorCode()));
                        remoteWrites.forEach((topic, writes) -> writes.forEach(write -> {
                            TopicPartition tp = new TopicPartition(topic, write.partition());
                            scheduleRetry(tp.toString(),
                                () -> refreshAndPersistState(mirrorName, tp,
                                    targetState, errorMessage, nonRetryable, res.data().errorMessage()),
                                RETRY_BACKOFF_MS, "Remote write top-level error: " + res.data().errorMessage());
                        }));
                        return;
                    }
                    res.data().topics().forEach(topic -> topic.partitions().forEach(partition -> {
                        TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                        onRemoteWriteComplete(mirrorName, tp, targetState, errorMessage, nonRetryable, partition);
                    }));
                });
        }
    }

    private void prepareStateWrites(String mirrorName,
                                    Set<TopicPartition> topicPartitions,
                                    MirrorPartitionState targetState,
                                    String errorMessage,
                                    boolean nonRetryable,
                                    Map<String, Set<MirrorStateWrite>> localWrites,
                                    Map<String, Set<MirrorStateWrite>> remoteWrites) {
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
                    null, errorMessage, -1, nonRetryable);

            if (coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
                localWrites.computeIfAbsent(tp.topic(), k -> new HashSet<>()).add(write);
            } else {
                remoteWrites.computeIfAbsent(tp.topic(), k -> new HashSet<>()).add(write);
            }
        }
    }

    /** Triggers per-partition side effects (actions) after local coordinator write completes. */
    private void onLocalWriteComplete(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                      String errorMessage, boolean nonRetryable, int stateEpoch, Throwable ex) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        if (ex != null) {
            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            if (cause instanceof CoordinatorLoadInProgressException) {
                scheduleRetry(tp.toString(),
                        () -> persistState(mirrorName, tp, state, errorMessage, nonRetryable),
                        RETRY_BACKOFF_MS, "Coordinator loading");
                return;
            }
            if (cause instanceof FencedLeaderEpochException || cause instanceof FencedStateEpochException) {
                refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, "Stale epoch");
                return;
            }
            if (state != MirrorPartitionState.FAILED) {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage(), false);
            }
            return;
        }
        MirrorPartitionMetadata existing = mirrorCache.getPartitionMetadata(mp);
        int maxAttempts = new ClusterMirrorConfig(brokerConfig).failedRetryMaxAttempts();
        mirrorCache.updatePartitionMetadata(mp,
                new MirrorPartitionMetadata.Builder(existing)
                        // We must resolve the error info before state change because
                        // we'll set the prevState based on current state
                        .withResolvedErrorInfo(state, errorMessage, -1, nonRetryable, maxAttempts)
                        .withState(state)
                        .withStateEpoch(stateEpoch)
                        .build());
        onStateTransition(mirrorName, tp, state);
    }

    /** Triggers per-partition side effects (actions) after remote coordinator write completes. */
    private void onRemoteWriteComplete(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                       String errorMessage, boolean nonRetryable,
                                       WriteMirrorStatesResponseData.PartitionResult part) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        if (part.errorCode() == Errors.NONE.code()) {
            MirrorPartitionMetadata existing = mirrorCache.getPartitionMetadata(mp);
            int maxAttempts = new ClusterMirrorConfig(brokerConfig).failedRetryMaxAttempts();
            mirrorCache.updatePartitionMetadata(mp,
                    new MirrorPartitionMetadata.Builder(existing)
                            // We must resolve the error info before state change because
                            // we'll set the prevState based on current state
                            .withResolvedErrorInfo(state, errorMessage, -1, nonRetryable, maxAttempts)
                            .withState(state)
                            .withStateEpoch(part.stateEpoch())
                            .build());
            onStateTransition(mirrorName, tp, state);
        } else if (part.errorCode() == Errors.COORDINATOR_LOAD_IN_PROGRESS.code()) {
            scheduleRetry(tp.toString(),
                    () -> persistState(mirrorName, tp, state, errorMessage, nonRetryable),
                    RETRY_BACKOFF_MS, "Coordinator loading");
        } else if (part.errorCode() == Errors.FENCED_LEADER_EPOCH.code()
                || part.errorCode() == Errors.FENCED_STATE_EPOCH.code()) {
            refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, "Stale epoch");
        } else {
            log.error("Remote coordinator write for partition {} failed with error code {}", tp, part.errorCode());
        }
    }

    /**
     * Dispatches side effects after a coordinator write.
     * Each state triggers a specific action.
     */
    private void onStateTransition(String mirrorName, TopicPartition tp, MirrorPartitionState newState) {
        switch (newState) {
            case LOG_ALIGNMENT:
                handleLogAlignment(mirrorName, tp);
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
                handlePausing(mirrorName, tp);
                break;
            case STOPPING:
                handleStopping(mirrorName, tp);
                break;
            case PAUSED, STOPPED:
                break;
            case FAILED:
                handleFailed(mirrorName, tp);
                break;
            default:
                throw new IllegalArgumentException("Illegal state transition to " + newState);
        }
    }

    private void handleLogAlignment(String mirrorName, TopicPartition topicPartition) {
        final Consumer<TopicPartition> callback =
                tp -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null, false);
        log.info("Scheduling truncation to last known position for partition {}", topicPartition);
        scheduler.scheduleOnce("truncation-" + mirrorName + "-" + topicPartition,
                () -> {
                    try {
                        var sourceMirrors = listSourceClusterMirrors(mirrorName);
                        if (hasMirrorLoop(mirrorName, topicPartition, sourceMirrors)) {
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
                                            replicaManagerSupplier.get().truncateForMirrorAlignment(
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
                                    replicaManagerSupplier.get().truncateForMirrorAlignment(
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
        replicaManagerSupplier.get().waitForMirrorReplicaConvergence(tp)
                .thenRun(() -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.MIRRORING, null, false));
    }

    private void handlePausing(String mirrorName, TopicPartition tp) {
        replicaManagerSupplier.get().mirrorFetcherManager()
            .removeFetcherForPartitions(CollectionConverters.asScala(Set.of(tp)));
        transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.PAUSED, null, false);
    }

    /**
     * Handles the STOPPING lifecycle: removes fetchers, updates the last mirror position,
     * bumps the leader epoch, aborts ongoing transactions, writes PID reset record,
     * and finally transitions to STOPPED. On any failure, transitions to FAILED.
     */
    private void handleStopping(String mirrorName, TopicPartition tp) {
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
        if (coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            writePositionFuture = coordSender.writeLastMirrorPositions(mirrorName, Map.of(tp, lastMirrorPosition));
        } else {
            coordSender.writeStateToRemoteCoordinator(mirrorName,
                Map.of(tp.topic(), Set.of(new MirrorStateWrite(tp.partition(), null, -1, -1, lastMirrorPosition, null, -1, false))));
            writePositionFuture = CompletableFuture.completedFuture(null);
        }

        var latestLocalEpoch = replicaManagerSupplier.get().logManager().getLog(tp, false).get().latestEpoch().orElse(-1);
        writePositionFuture
            .thenCompose(v -> sendBumpLeaderEpochs(Map.of(tp, Math.addExact(latestLocalEpoch, 1))))
            .thenCompose(v -> abortOngoingTransactions(tp))
            .thenCompose(v -> writePidResetRecord(mirrorName, tp))
            .thenAccept(v -> transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.STOPPED, null, false))
            .exceptionally(ex -> {
                transitionTo(mirrorName, Set.of(tp), MirrorPartitionState.FAILED, ex.getMessage(), false);
                return null;
            });
    }

    private void handleFailed(String mirrorName, TopicPartition tp) {
        log.info("Scheduling failed state recovery for partition {}", tp);
        ClusterMirrorConfig mirrorConfig = brokerConfig.mirrorConfig();
        int maxAttempts = mirrorConfig.failedRetryMaxAttempts();
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        MirrorPartitionMetadata mpm = mirrorCache.getPartitionMetadata(mp);
        int attempt = mpm.retryAttempt() != 0 ? mpm.retryAttempt() : 1;
        if (attempt == NON_RETRYABLE_ERROR) {
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
        scheduleRetry(tp.toString(),
            () -> transitionTo(mirrorName, Set.of(tp), targetState, null, false),
            delay, "Retry #" + attempt + " targeting " + targetState);
    }

    /**
     * Removes the mirror's data leader cache entries and writes tombstone
     * records for all locally coordinated partitions of a deleted mirror.
     */
    void writeTombstoneRecords(String mirrorName) {
        mirrorCache.removeMirrorPartitions(mirrorName);
        mirrorCache.removeSourceClusterLeaders(mirrorName);

        coordSender.writeMirrorTombstones(mirrorName)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to write tombstone for mirror {}: {}. Will retry later.",
                                mirrorName, ex.getMessage());
                    }
                });
    }

    /**
     * Lists the cluster mirrors configured on the source cluster, including their active topic names.
     * Used before mirroring to detect mirror loops and during truncation to validate that source
     * partitions are stopped.
     *
     * @param mirrorName the name of the local mirror whose source cluster to query
     * @return the cluster mirror listings from the source cluster, or an empty list if the source
     *         cluster does not support the ListClusterMirrors API
     * @throws IllegalStateException if the request fails
     */
    Collection<ClusterMirrorListing> listSourceClusterMirrors(String mirrorName) {
        Admin srcAdmin = sourceSyncer.getOrCreateSourceAdmin(mirrorName);
        try {
            return srcAdmin
                    .listClusterMirrors(new ListClusterMirrorsOptions())
                    .all().get(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnsupportedVersionException) {
                log.info("Source cluster does not support listClusterMirrors for mirror {}. " +
                        "Skipping mirror loop check.", mirrorName);
                return List.of();
            }
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException("Failed to list cluster mirrors from source for mirror "
                    + mirrorName, cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to list cluster mirrors from source for mirror "
                    + mirrorName, e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to list cluster mirrors from source for mirror "
                    + mirrorName, e);
        }
    }

    /**
     * Checks whether mirroring the given partitions from the source cluster would create
     * a mirror loop, based on the mirrors currently configured on the source cluster.
     *
     * @param mirrorName      the local mirror being started
     * @param topicPartition  the partition about to be mirrored
     * @param sourceMirrors   mirrors configured on the source cluster
     * @return true if the given partitions would create a mirror loop, false otherwise
     */
    boolean hasMirrorLoop(String mirrorName, TopicPartition topicPartition,
                          Collection<ClusterMirrorListing> sourceMirrors) {
        if (sourceMirrors.isEmpty()) {
            return false;
        }
        String topicName = topicPartition.topic();
        for (ClusterMirrorListing sourceMirror : sourceMirrors) {
            if (!clusterId.equals(sourceMirror.sourceClusterId())) {
                continue;
            }
            if (sourceMirror.topicNames().contains(topicName)) {
                log.error("Mirror loop detected for mirror {}: source mirror {} is already mirroring topic {}",
                        mirrorName, sourceMirror.mirrorName(), topicName);
                return true;
            }
        }
        return false;
    }

    /**
     * Sends a last mirror epoch (LME) lookup request to the source cluster.
     * The log output is used by test_failover_failback test to verify failback behavior.
     */
    public CompletionStage<Map<TopicPartition, EpochOffset>> sendLastMirrorEpochLookup(
            String mirrorName, TopicPartition tp, Collection<ClusterMirrorListing> sourceMirrors) {
        Admin admin = sourceSyncer.getOrCreateSourceAdmin(mirrorName);
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
            // This log format is used in cluster_mirroring_test.test_failove_failback,
            // so the system test needs to be updated in case of changes
            log.info("Received LME lookup response for partition {} in mirror {}: {}",
                    tp, mirrorName, lastMirrorPositions);
            return lastMirrorPositions;
        })
        .orTimeout(brokerConfig.requestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private Map<String, Set<Integer>> collectLocalPartitions(String mirrorName,
                                                             Set<String> topics,
                                                             MetadataImage image) {
        Map<String, Set<Integer>> localPartitions = new HashMap<>();
        for (String topic : topics) {
            TopicImage topicImage = image.topics().getTopic(topic);
            if (topicImage != null) {
                Uuid topicId = mirrorCache.getTopicId(topic);
                for (int i = 0; i < topicImage.partitions().size(); i++) {
                    if (coordSender.isLocalCoordinatorFor(mirrorName, topicId, i)) {
                        localPartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(i);
                    }
                }
            }
        }
        return localPartitions;
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
                    if (!coordSender.isLocalCoordinatorFor(mirrorName, topicId, i)) {
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
        Admin srcAdmin = sourceSyncer.getOrCreateSourceAdmin(mirrorName);

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
        // Validate
        for (String p : topicPatterns) {
            String pattern = p.trim();
            if (!pattern.isEmpty()) {
                try {
                    Pattern.compile(pattern);
                } catch (PatternSyntaxException e) {
                    throw new InvalidRegularExpression("Invalid pattern: " + pattern);
                }
            }
        }

        // Resolve
        Pattern compiled = MirrorUtils.compilePatternList(topicPatterns);
        if (compiled == null) {
            return Set.of();
        }
        return allTopics.stream()
                .filter(t -> compiled.matcher(t).matches())
                .collect(Collectors.toSet());
    }

    /** Schedules a source topic metadata refresh followed by a leader epoch bump request. */
    public CompletableFuture<Void> scheduleBumpLeaderEpoch(String mirrorName, TopicPartition tp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.scheduleOnce("bump-leader-epoch-" + tp, () -> {
            List<SourceTopicMetadata> sourceTopicMetadata = sourceSyncer.syncSourceTopicMetadata(mirrorName);
            maybeBumpLeaderEpochs(mirrorName, sourceTopicMetadata, Set.of(tp))
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
                                                  List<SourceTopicMetadata> sourceTopicMetadata,
                                                  Set<TopicPartition> topicPartitions) {
        return sendBumpLeaderEpochs(buildSourceEpochBumpTargets(mirrorName, sourceTopicMetadata, topicPartitions))
                .whenComplete((v, ex) -> {
                    if (ex != null) log.warn("Failed to bump leader epoch for mirror {}", mirrorName, ex);
                });
    }

    private CompletableFuture<Void> sendBumpLeaderEpochs(Map<TopicPartition, Integer> partitionMinEpochs) {
        if (partitionMinEpochs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Filter out partitions that already have an inflight bump with an equal or higher target epoch
        Map<TopicPartition, Integer> filteredPartitions = new HashMap<>(partitionMinEpochs);
        for (PendingLeaderEpochBump pending : pendingLeaderEpochBumps) {
            if (pending.future().isDone()) {
                continue;
            }
            filteredPartitions.entrySet().removeIf(entry -> {
                Integer inflightEpoch = pending.partitionToEpoch().get(entry.getKey());
                return inflightEpoch != null && inflightEpoch >= entry.getValue();
            });
        }

        if (filteredPartitions.isEmpty()) {
            log.debug("All partitions in {} already have inflight bump leader epoch requests, skipping", partitionMinEpochs.keySet());
            return CompletableFuture.completedFuture(null);
        }

        if (filteredPartitions.size() < partitionMinEpochs.size()) {
            log.debug("Filtered out {} partitions with inflight bump leader epoch requests, sending for {}",
                    partitionMinEpochs.size() - filteredPartitions.size(), filteredPartitions.keySet());
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        addPendingEpochBump(future, filteredPartitions);
        maybeCompletePendingEpochBumps();

        sendBumpLeaderEpochsRequest(filteredPartitions, future);
        return future;
    }

    @SuppressWarnings("NPathComplexity")
    private void sendBumpLeaderEpochsRequest(Map<TopicPartition, Integer> partitionMinEpochs,
                                             CompletableFuture<Void> future) {
        if (future.isDone()) {
            return;
        }

        log.info("Sending bump leader epoch request: {}", partitionMinEpochs);

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

        controllerClient.sendRequest(new BumpLeaderEpochsRequest.Builder(
                new BumpLeaderEpochsRequestData().setTopics(topicStates)
        ), new ControllerRequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                if (future.isDone()) {
                    return;
                }
                if (response.versionMismatch() != null) {
                    handleBumpLeaderEpochError(future, response.versionMismatch());
                    return;
                }
                if (response.authenticationException() != null) {
                    handleBumpLeaderEpochError(future, response.authenticationException());
                    return;
                }
                if (response.wasDisconnected()) {
                    scheduleRetry("bump-leader-epoch-" + partitionMinEpochs.keySet(),
                            () -> sendBumpLeaderEpochsRequest(partitionMinEpochs, future),
                            RETRY_BACKOFF_MS, "Bump leader epoch request was disconnected");
                    return;
                }
                BumpLeaderEpochsResponse bumpResponse = (BumpLeaderEpochsResponse) response.responseBody();
                Errors topLevelError = Errors.forCode(bumpResponse.data().errorCode());
                if (topLevelError != Errors.NONE) {
                    if (topLevelError.exception() instanceof RetriableException) {
                        scheduleRetry("bump-leader-epoch-" + partitionMinEpochs.keySet(),
                                () -> sendBumpLeaderEpochsRequest(partitionMinEpochs, future),
                                RETRY_BACKOFF_MS, "Bump leader epoch response has top-level error: " + topLevelError);
                        return;
                    } else {
                        handleBumpLeaderEpochError(future, topLevelError.exception());
                        return;
                    }
                }

                Map<TopicPartition, Integer> retriableFailures = new HashMap<>();
                Set<TopicPartition> nonRetriableFailures = new HashSet<>();
                Set<TopicPartition> successfulPartitions = new HashSet<>();

                bumpResponse.data().topics().forEach(topic ->
                        topic.partitions().forEach(partition -> {
                            Errors partitionError = Errors.forCode(partition.errorCode());
                            TopicPartition tp = new TopicPartition(topic.topicName(), partition.partitionIndex());
                            if (partitionError != Errors.NONE) {
                                log.warn("Bump leader epoch partition-level error for {}: {}", tp, partitionError);
                                Integer minEpoch = partitionMinEpochs.get(tp);
                                if (minEpoch != null) {
                                    if (partitionError.exception() instanceof RetriableException) {
                                        retriableFailures.put(tp, minEpoch);
                                    } else {
                                        nonRetriableFailures.add(tp);
                                    }
                                }
                            } else {
                                successfulPartitions.add(tp);
                            }
                        }));

                if (!nonRetriableFailures.isEmpty()) {
                    log.error("Bump leader epoch response has non-retriable errors for partitions: {}", nonRetriableFailures);
                    removePendingEpochBumpPartitions(nonRetriableFailures);
                }

                if (!retriableFailures.isEmpty()) {
                    scheduleRetry("bump-leader-epoch-" + retriableFailures.keySet(),
                            () -> sendBumpLeaderEpochsRequest(retriableFailures, future),
                            RETRY_BACKOFF_MS, "Bump leader epoch response has partition-level errors");
                }

                if (!successfulPartitions.isEmpty()) {
                    log.info("Bump leader epoch response success for partitions: {}", successfulPartitions);
                    maybeCompletePendingEpochBumps();
                }
            }

            @Override
            public void onTimeout() {
                if (future.isDone()) {
                    return;
                }
                scheduleRetry("bump-leader-epoch-" + partitionMinEpochs.keySet(),
                        () -> sendBumpLeaderEpochsRequest(partitionMinEpochs, future),
                        RETRY_BACKOFF_MS, "Bump leader epoch timeout");
            }
        });
    }

    private void handleBumpLeaderEpochError(CompletableFuture<Void> future, Exception fatalError) {
        if (fatalError != null) {
            log.error("Bump leader epoch request failed", fatalError);
            future.completeExceptionally(fatalError);
            removePendingEpochBump(future);
        }
    }

    @SuppressWarnings("NPathComplexity")
    private Map<TopicPartition, Integer> buildSourceEpochBumpTargets(String mirrorName,
                                                                     List<SourceTopicMetadata> sourceTopicMetadata,
                                                                     Set<TopicPartition> topicPartitions) {
        Set<String> mirrorTopics = topicPartitions.isEmpty()
                ? mirrorCache.getMirrorTopics(mirrorName, EnumSet.of(MirrorPartitionState.MIRRORING))
                : Set.of();
        Map<TopicPartition, Integer> leaderEpochFromMetadata = new HashMap<>();
        for (SourceTopicMetadata ts : sourceTopicMetadata) {
            if (!ts.exists()) {
                continue;
            }
            if (!mirrorTopics.isEmpty() && !mirrorTopics.contains(ts.topic())) {
                continue;
            }
            for (SourcePartitionState ps : ts.partitions()) {
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
                int sle = ps.leaderEpoch().get();
                int dle = topicImage.partitions().get(tp.partition()).leaderEpoch;
                if (sle > dle - LEADER_EPOCH_BUMP_THRESHOLD) {
                    int targetEpoch = Math.addExact(sle, LEADER_EPOCH_BUMP_INCREMENT);
                    leaderEpochFromMetadata.put(tp, targetEpoch);
                }
            }
        }
        if (!leaderEpochFromMetadata.isEmpty()) {
            log.info("Bumping leader epoch for partitions {}", leaderEpochFromMetadata);
        }
        return leaderEpochFromMetadata;
    }

    private void addPendingEpochBump(CompletableFuture<Void> future, Map<TopicPartition, Integer> partitionMinEpochs) {
        pendingLeaderEpochBumps.add(new PendingLeaderEpochBump(future, new ConcurrentHashMap<>(partitionMinEpochs)));
    }

    void maybeCompletePendingEpochBumps() {
        pendingLeaderEpochBumps.removeIf(bump -> {
            Set<TopicPartition> pendingPartitions = bump.partitionToEpoch().entrySet().stream().filter(entry -> {
                TopicPartition tp = entry.getKey();
                int expectedEpoch = entry.getValue();
                var topicImage = metadataImage.topics().getTopic(tp.topic());
                if (topicImage == null) return false;
                var partitionReg = topicImage.partitions().get(tp.partition());
                if (partitionReg == null) return false;
                return partitionReg.leaderEpoch < expectedEpoch;
            }).map(Map.Entry::getKey).collect(Collectors.toSet());
            if (pendingPartitions.isEmpty()) {
                bump.future().complete(null);
                return true;
            } else {
                log.info("Bump leader epoch is pending for partitions: {}, all: {}",
                        pendingPartitions, bump.partitionToEpoch().keySet());
                return false;
            }
        });
    }

    private void removePendingEpochBump(CompletableFuture<Void> future) {
        pendingLeaderEpochBumps.removeIf(bump -> bump.future() == future);
    }

    private void removePendingEpochBumpPartitions(Set<TopicPartition> partitionsToRemove) {
        pendingLeaderEpochBumps.removeIf(bump -> {
            bump.partitionToEpoch().keySet().removeAll(partitionsToRemove);
            if (bump.partitionToEpoch().isEmpty()) {
                bump.future().complete(null);
                return true;
            }
            return false;
        });
    }

    private void clearAllPendingEpochBumps() {
        pendingLeaderEpochBumps.forEach(bump -> bump.future().complete(null));
        pendingLeaderEpochBumps.clear();
    }

    private void scheduleRetry(String name, Runnable callback, long delayMs, String reason) {
        log.warn("Scheduling retry for {} in {} ms. Reason: {}", name, delayMs, reason);
        scheduler.scheduleOnce("retry-" + name, callback, delayMs);
    }

    public void scheduleOnceSourceTopicMetadataSync(String mirrorName) {
        sourceSyncer.scheduleOnceSourceTopicMetadataSync(mirrorName);
    }

    public void scheduleSourceClusterSync(long intervalMs) {
        sourceSyncer.scheduleSourceClusterMetadataSync(intervalMs);
    }

    public CompletableFuture<ReadMirrorStatesResponse> readStateFromRemoteCoordinator(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        return coordSender.readStateFromRemoteCoordinator(mirrorName, partitions);
    }

    public CompletableFuture<ReadMirrorOffsetsResponse> readOffsetsFromRemoteLeaders(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        return coordSender.readOffsetsFromRemoteLeaders(mirrorName, partitions);
    }

    /**
     * Re-reads partition state from the coordinator to refresh cached epochs,
     * then re-attempts to write via {@link #persistState}. Read failures are
     * retried on a backoff schedule. This is the common recovery path for
     * write failures caused by stale epochs or transient coordinator errors.
     */
    private void refreshAndPersistState(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                        String errorMessage, boolean nonRetryable, String reason) {
        log.warn("Refreshing and retrying state write for partition {}. Reason: {}", tp, reason);
        Map<String, Set<Integer>> partitions = Map.of(tp.topic(), Set.of(tp.partition()));
        CompletableFuture<ReadMirrorStatesResponse> readFuture;
        if (coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            readFuture = coordSender.readStateFromLocalCoordinator(mirrorName, partitions);
        } else {
            readFuture = coordSender.readStateFromRemoteCoordinator(mirrorName, partitions);
        }
        readFuture.whenComplete((data, ex) -> {
            if (ex != null) {
                scheduleRetry(tp.toString(),
                        () -> refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, reason),
                        RETRY_BACKOFF_MS, "Coordinator read failed: " + ex.getMessage());
                return;
            }
            handleRefreshResponse(mirrorName, tp, state, errorMessage, nonRetryable, data, reason);
        });
    }

    private void handleRefreshResponse(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                                       String errorMessage, boolean nonRetryable, ReadMirrorStatesResponse res,
                                       String reason) {
        if (res.data().errorCode() != Errors.NONE.code()) {
            log.warn("Coordinator read for refresh failed with top-level error: {}",
                    Errors.forCode(res.data().errorCode()));
            scheduleRetry(tp.toString(),
                    () -> refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, reason),
                    RETRY_BACKOFF_MS, "Refresh read top-level error: " + res.data().errorMessage());
            return;
        }
        res.data().topics().forEach(topic -> topic.partitions().forEach(partition -> {
            if (partition.errorCode() != Errors.NONE.code()) {
                scheduleRetry(tp.toString(),
                        () -> refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, reason),
                        RETRY_BACKOFF_MS, "Refresh read returned no usable state");
                return;
            }
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
        }));
    }

    private void persistState(String mirrorName, TopicPartition tp, MirrorPartitionState state,
                              String errorMessage, boolean nonRetryable) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        var curState = mirrorCache.getPartitionMetadata(mp);
        int leaderEpoch = mirrorCache.getLeaderEpoch(tp);
        MirrorStateWrite write = new MirrorStateWrite(tp.partition(), state, leaderEpoch, curState.stateEpoch(),
                null, errorMessage, -1, nonRetryable);
        if (coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            coordSender.writeStateToLocalCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)))
                    .whenComplete((data, ex) -> {
                        if (ex != null) {
                            refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, ex.getMessage());
                            return;
                        }
                        if (data.errorCode() != Errors.NONE.code()) {
                            scheduleRetry(tp.toString(),
                                    () -> refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable,
                                            "Local write top-level error: " + data.errorMessage()),
                                    RETRY_BACKOFF_MS, "Local write top-level error: " + data.errorMessage());
                            return;
                        }
                        data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                            Throwable partitionEx = null;
                            if (partition.errorCode() != Errors.NONE.code()) {
                                partitionEx = Errors.forCode(partition.errorCode()).exception();
                            }
                            onLocalWriteComplete(mirrorName, tp, state, errorMessage, nonRetryable, partition.stateEpoch(), partitionEx);
                        }));
                    });
        } else {
            coordSender.writeStateToRemoteCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)))
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable, ex.getMessage());
                            return;
                        }
                        if (res.data().errorCode() != Errors.NONE.code()) {
                            scheduleRetry(tp.toString(),
                                    () -> refreshAndPersistState(mirrorName, tp, state, errorMessage, nonRetryable,
                                            "Remote write top-level error: " + res.data().errorMessage()),
                                    RETRY_BACKOFF_MS, "Remote write top-level error: " + res.data().errorMessage());
                            return;
                        }
                        res.data().topics().forEach(topic -> topic.partitions().forEach(partition -> {
                            onRemoteWriteComplete(mirrorName, tp, state, errorMessage, nonRetryable, partition);
                        }));
                    });
        }
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
                        scheduleRetry(tp.toString() + "-pid-reset",
                                () -> writePidResetRecord(mirrorName, tp).thenAccept(r -> result.complete(null)),
                                5000, "PID reset record write failed: " + ex.getMessage());
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

    public CompletableFuture<Optional<Errors>> validateDeleteMirrorStates(DeleteClusterMirrorRequestData data) {
        Set<String> topics = mirrorCache.getMirrorTopics(data.mirrorName(),
                EnumSet.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED, MirrorPartitionState.STOPPED));
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED), false)
            .handle((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to validate delete mirror states", ex);
                    return Optional.of(Errors.forException(ex));
                }
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validateStartMirrorStates(StartMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(StartMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.STOPPED, MirrorPartitionState.UNKNOWN), true)
            .handle((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to validate start mirror states", ex);
                    return Optional.of(Errors.forException(ex));
                }
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validateStopMirrorStates(StopMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(StopMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING, MirrorPartitionState.PAUSED), false)
            .handle((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to validate stop mirror states", ex);
                    return Optional.of(Errors.forException(ex));
                }
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validatePauseMirrorStates(PauseMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(PauseMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.MIRRORING), false)
            .handle((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to validate pause mirror states", ex);
                    return Optional.of(Errors.forException(ex));
                }
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    public CompletableFuture<Optional<Errors>> validateResumeMirrorStates(ResumeMirrorTopicsRequestData data) {
        Set<String> topics = data.topics().stream()
                .map(ResumeMirrorTopicsRequestData.TopicMetadata::topicName).collect(Collectors.toSet());
        return validateMirrorStates(data.mirrorName(), topics,
                Set.of(MirrorPartitionState.PAUSED), false)
            .handle((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to validate resume mirror states", ex);
                    return Optional.of(Errors.forException(ex));
                }
                data.setStateOffset(result.stateOffset());
                return result.error();
            });
    }

    /**
     * Validates partition states on the broker before forwarding to the controller.
     * The controller uses this offset for optimistic locking, rejecting the request
     * if any mirror state changed after the broker's validation.
     * Routes all coordinator reads through the coordinator API for read-committed semantics.
     */
    private CompletableFuture<ValidationResult> validateMirrorStates(
            String mirrorName,
            Set<String> topicNames,
            Set<MirrorPartitionState> validStates,
            boolean skipMissingTopics) {
        MetadataImage currentImage = metadataImage;
        long validationOffset = currentImage.offset();
        Map<String, Set<Integer>> localPartitions = new HashMap<>();
        Map<String, Set<Integer>> remotePartitions = new HashMap<>();

        Optional<Errors> metadataError = classifyPartitionsForValidation(
                mirrorName, topicNames, validStates, skipMissingTopics, currentImage,
                localPartitions, remotePartitions);
        if (metadataError.isPresent()) {
            return CompletableFuture.completedFuture(new ValidationResult(validationOffset, metadataError));
        }

        // Read local and remote coordinator state through the coordinator API
        CompletableFuture<Optional<Errors>> localFuture = localPartitions.isEmpty()
                ? CompletableFuture.completedFuture(Optional.empty())
                : coordSender.readStateFromLocalCoordinator(mirrorName, localPartitions)
                        .handle((response, ex) -> {
                            if (ex != null) {
                                log.error("Failed to read local coordinator state for mirror {}", mirrorName, ex);
                                return Optional.of(Errors.COORDINATOR_NOT_AVAILABLE);
                            }
                            return validateRemotePartitions(response, validStates);
                        });
        CompletableFuture<Optional<Errors>> remoteFuture = remotePartitions.isEmpty()
                ? CompletableFuture.completedFuture(Optional.empty())
                : coordSender.readStateFromRemoteCoordinator(mirrorName, remotePartitions)
                        .handle((response, ex) -> {
                            if (ex != null) {
                                log.error("Failed to read remote coordinator state for mirror {}", mirrorName, ex);
                                return Optional.of(Errors.COORDINATOR_NOT_AVAILABLE);
                            }
                            return validateRemotePartitions(response, validStates);
                        });

        return localFuture.thenCombine(remoteFuture, (localErr, remoteErr) ->
                new ValidationResult(validationOffset, localErr.isPresent() ? localErr : remoteErr));
    }

    /**
     * Classifies partitions by metadata-image checks (desired state, topic existence)
     * and splits them into local vs remote for coordinator reads.
     */
    private Optional<Errors> classifyPartitionsForValidation(
            String mirrorName,
            Set<String> topicNames,
            Set<MirrorPartitionState> validStates,
            boolean skipMissingTopics,
            MetadataImage currentImage,
            Map<String, Set<Integer>> localPartitions,
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
                if (coordSender.isLocalCoordinatorFor(mirrorName, topicImage.id(), i)) {
                    localPartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(i);
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
    public void maybeClearFailedStateAndPersist(String mirrorName, TopicPartition tp) {
        MirrorPartition mp = MirrorPartition.of(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition());
        MirrorPartitionMetadata existing = mirrorCache.getPartitionMetadata(mp);
        MirrorPartitionState state = existing.state();
        if (state != MirrorPartitionState.MIRRORING) {
            log.debug("Skipping failed state clearance for partition {}. Reason: Current state is {}.", tp, state);
            return;
        }

        if (existing.retryAttempt() == 0 && existing.errorMessage() == null && existing.prevState() == null) {
            log.debug("Skipping failed state clearance for partition {}. Reason: Current state is up-to-date.", tp);
            return;
        }
        mirrorCache.updatePartitionMetadata(mp,
                new MirrorPartitionMetadata.Builder(existing)
                    .withErrorMessage(null)
                    .withRetryAttempt(0)
                    .withPrevState(null)
                    .build());

        int stateEpoch = mirrorCache.getPartitionMetadata(mp).stateEpoch();
        int leaderEpoch = mirrorCache.getLeaderEpoch(tp);
        MirrorStateWrite write = new MirrorStateWrite(tp.partition(), state, leaderEpoch, stateEpoch,
                null, null, 0, false);

        if (coordSender.isLocalCoordinatorFor(mirrorName, mirrorCache.getTopicId(tp.topic()), tp.partition())) {
            coordSender.writeStateToLocalCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)))
                    .whenComplete((data, ex) -> {
                        if (ex != null) {
                            scheduleRetry(tp.toString(),
                                    () -> refreshAndPersistState(mirrorName, tp, state, null, false, ex.getMessage()),
                                    RETRY_BACKOFF_MS, "Recovery write exception: " + ex.getMessage());
                            return;
                        }
                        if (data.errorCode() != Errors.NONE.code()) {
                            scheduleRetry(tp.toString(),
                                    () -> refreshAndPersistState(mirrorName, tp, state, null, false,
                                            "Recovery write top-level error: " + data.errorMessage()),
                                    RETRY_BACKOFF_MS, "Recovery write top-level error: " + data.errorMessage());
                            return;
                        }
                        data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                            Throwable partitionEx = null;
                            if (partition.errorCode() != Errors.NONE.code()) {
                                partitionEx = Errors.forCode(partition.errorCode()).exception();
                            }
                            onLocalWriteComplete(mirrorName, tp, state, null, false, partition.stateEpoch(), partitionEx);
                        }));
                    });
        } else {
            coordSender.writeStateToRemoteCoordinator(mirrorName, Map.of(tp.topic(), Set.of(write)))
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            scheduleRetry(tp.toString(),
                                    () -> refreshAndPersistState(mirrorName, tp, state, null, false, ex.getMessage()),
                                    RETRY_BACKOFF_MS, "Recovery write exception: " + ex.getMessage());
                            return;
                        }
                        if (res.data().errorCode() != Errors.NONE.code()) {
                            scheduleRetry(tp.toString(),
                                    () -> refreshAndPersistState(mirrorName, tp, state, null, false,
                                            "Recovery write top-level error: " + res.data().errorMessage()),
                                    RETRY_BACKOFF_MS, "Recovery write top-level error: " + res.data().errorMessage());
                            return;
                        }
                        res.data().topics().forEach(topic -> topic.partitions().forEach(partition ->
                                onRemoteWriteComplete(mirrorName, tp, state, null,
                                        false, partition)));
                    });
        }
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
     *
     * Reads both local and remote coordinator state through the coordinator API
     * to populate the data leader cache before building recovery writes.
     */
    public CompletableFuture<Void> writeRecoveryRecords(String mirrorName, Set<String> topics) {
        if (!coordSender.isInitialized()) {
            return CompletableFuture.completedFuture(null);
        }

        Map<String, Set<Integer>> localPartitions = collectLocalPartitions(mirrorName, topics, metadataImage);
        Map<String, Set<Integer>> remotePartitions = collectRemotePartitions(mirrorName, topics, metadataImage);

        CompletableFuture<Void> readLocalFuture = localPartitions.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : coordSender.readStateFromLocalCoordinator(mirrorName, localPartitions).thenApply(v -> null);
        CompletableFuture<Void> readRemoteFuture = remotePartitions.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : coordSender.readStateFromRemoteCoordinator(mirrorName, remotePartitions).thenApply(v -> null);

        return CompletableFuture.allOf(readLocalFuture, readRemoteFuture)
                .thenCompose(v -> executeRecoveryWrites(mirrorName, topics, metadataImage));
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
                // Both local and remote coordinator state have been read into the data leader cache
                // by writeRecoveryRecords before this method is called.
                MirrorPartitionMetadata mpm = mirrorCache.getPartitionMetadata(mp);
                if (mpm.state() == MirrorPartitionState.FAILED && mpm.prevState() != null) {
                    MirrorStateWrite write = new MirrorStateWrite(i, mpm.prevState(),
                            mpm.lastPosition().epoch(), mpm.stateEpoch(), null, null, 0, false);
                    if (coordSender.isLocalCoordinatorFor(mirrorName, topicImage.id(), i)) {
                        localWrites.computeIfAbsent(topic, k -> new HashSet<>()).add(write);
                    } else {
                        remoteWrites.computeIfAbsent(topic, k -> new HashSet<>()).add(write);
                    }
                }
            }
        }

        CompletableFuture<Void> localWriteFuture = localWrites.isEmpty() ?
                CompletableFuture.completedFuture(null) :
                coordSender.writeStateToLocalCoordinator(mirrorName, localWrites)
                        .exceptionally(ex -> {
                            log.warn("Failed to write recover states for mirror {}: {}", mirrorName, ex.getMessage());
                            return null;
                        })
                        .thenApply(v -> null);

        if (remoteWrites.isEmpty()) {
            return localWriteFuture;
        }

        return localWriteFuture.thenCompose(res -> coordSender.writeStateToRemoteCoordinator(mirrorName, remoteWrites)
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

    record ValidationResult(long stateOffset, Optional<Errors> error) { }
    record PendingLeaderEpochBump(CompletableFuture<Void> future, Map<TopicPartition, Integer> partitionToEpoch) { }
}
