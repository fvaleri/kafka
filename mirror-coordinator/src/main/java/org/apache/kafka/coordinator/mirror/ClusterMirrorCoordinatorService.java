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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.mirror.metrics.ClusterMirrorCoordinatorMetrics;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Persistence layer for the cluster mirror coordinator.
 * Implements the {@link ClusterMirrorCoordinator} lifecycle interface
 * and delegates record persistence to the {@link CoordinatorRuntime}.
 */
public class ClusterMirrorCoordinatorService implements ClusterMirrorCoordinator {
    private enum State { INITIAL, STARTING, STARTED, SHUTDOWN }

    private final Logger log;
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);
    private final CountDownLatch latch = new CountDownLatch(1);
    private final ClusterMirrorConfig mirrorConfig;
    private final CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime;
    private final MetadataManagerBridge mirrorManager;
    private final MetadataCache metadataCache;
    private final KafkaScheduler scheduler;
    private final Metrics metrics;

    public static class Builder {
        private final int nodeId;
        private final ClusterMirrorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time;
        private Timer timer;
        private CoordinatorRuntimeMetrics runtimeMetrics;
        private ClusterMirrorCoordinatorMetrics coordinatorMetrics;
        private MetadataManagerBridge mirrorManager;
        private MetadataCache metadataCache;
        private Metrics metrics;

        public Builder(int nodeId, ClusterMirrorConfig config) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<CoordinatorRecord> loader) {
            this.loader = loader;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics runtimeMetrics) {
            this.runtimeMetrics = runtimeMetrics;
            return this;
        }

        public Builder withCoordinatorMetrics(ClusterMirrorCoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        public Builder withMetadataManager(MetadataManagerBridge metadataManager) {
            this.mirrorManager = metadataManager;
            return this;
        }

        public Builder withMetadataCache(MetadataCache metadataCache) {
            this.metadataCache = metadataCache;
            return this;
        }

        public Builder withMetrics(Metrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public ClusterMirrorCoordinatorService build() {
            int numPartitions = config.stateTopicNumPartitions();

            var logContext = new LogContext("[ClusterMirrorCoordinator brokerId=" + nodeId + "] ");
            var eventProcessor = new MultiThreadedEventProcessor(
                    logContext,
                    "cluster-mirror-coordinator-event-processor-",
                    config.coordinatorNumThreads(),
                    time,
                    runtimeMetrics);

            var runtime = new CoordinatorRuntime.Builder<ClusterMirrorCoordinatorShard, CoordinatorRecord>()
                    .withTime(time)
                    .withTimer(timer)
                    .withEventProcessor(eventProcessor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(
                            () -> new ClusterMirrorCoordinatorShard.Builder(config, mirrorManager, metadataCache, numPartitions))
                    .withDefaultWriteTimeOut(Duration.ofMillis(config.coordinatorWriteTimeoutMs()))
                    .withCoordinatorRuntimeMetrics(runtimeMetrics)
                    .withCoordinatorMetrics(coordinatorMetrics)
                    .withSerializer(new MirrorCoordinatorRecordSerde())
                    .withAppendLingerMs(config.coordinatorAppendLingerMs())
                    .withExecutorService(Executors.newSingleThreadExecutor())
                    .build();

            return new ClusterMirrorCoordinatorService(
                    nodeId, config, runtime, mirrorManager, metadataCache, metrics);
        }
    }

    private ClusterMirrorCoordinatorService(
        int nodeId,
        ClusterMirrorConfig mirrorConfig,
        CoordinatorRuntime<ClusterMirrorCoordinatorShard, CoordinatorRecord> runtime,
        MetadataManagerBridge mirrorManager,
        MetadataCache metadataCache,
        Metrics metrics
    ) {
        String name = "[" + ClusterMirrorCoordinatorService.class.getSimpleName() + " id=" + nodeId + "] ";
        this.log = new LogContext(name).logger(ClusterMirrorCoordinatorService.class);
        this.mirrorConfig = mirrorConfig;
        this.runtime = runtime;
        this.mirrorManager = mirrorManager;
        this.metadataCache = metadataCache;
        this.scheduler = new KafkaScheduler(1, true, "mirror-coordinator-");
        this.scheduler.startup();
        this.metrics = metrics;
    }

    @Override
    public void startup() {
        if (!state.compareAndSet(State.INITIAL, State.STARTING)) {
            log.warn("Not starting up as state is not INITIAL");
            return;
        }

        log.info("Starting up");
        try {
            mirrorManager.onCoordStartup(
                this::partitionFor,
                new MetadataManagerBridge.CoordinatorReader() {
                    @Override
                    public CompletableFuture<ReadMirrorStatesResponseData> readPartitionStates(
                            String mirrorName,
                            Map<String, Set<Integer>> partitions) {
                        return ClusterMirrorCoordinatorService.this.readPartitionStates(mirrorName, partitions);
                    }

                    @Override
                    public CompletableFuture<Set<String>> listLocalMirrorNames() {
                        return ClusterMirrorCoordinatorService.this.listLocalMirrorNames();
                    }
                },
                new MetadataManagerBridge.CoordinatorWriter() {
                    @Override
                    public CompletableFuture<WriteMirrorStatesResponseData> writePartitionStates(
                            String mirrorName,
                            Map<String, Set<MirrorStateWrite>> states) {
                        return ClusterMirrorCoordinatorService.this.writePartitionStates(mirrorName, states);
                    }

                    @Override
                    public CompletableFuture<Void> writeLastMirrorPositions(
                            String mirrorName,
                            Map<TopicPartition, EpochOffset> positions) {
                        return ClusterMirrorCoordinatorService.this.writeLastMirrorPositions(mirrorName, positions);
                    }

                    @Override
                    public CompletableFuture<Void> writeMirrorTombstones(String mirrorName) {
                        return ClusterMirrorCoordinatorService.this.writeMirrorTombstones(mirrorName);
                    }
                });
            log.info("Startup complete");
        } finally {
            state.set(State.STARTED);
        }
    }

    @Override
    public void shutdown() {
        State prev = state.getAndSet(State.SHUTDOWN);
        if (prev == State.SHUTDOWN) {
            log.warn("Shutdown already invoked");
            return;
        }
        if (prev == State.STARTING) {
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    log.warn("Timed out waiting for startup to complete. Shutting down directly.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for startup to complete. Shutting down directly.", e);
            }
        } else if (prev == State.INITIAL) {
            log.info("Shutting down before the service was initialized");
        }
        log.info("Shutting down");
        mirrorManager.onCoordShutdown();
        try {
            scheduler.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down ClusterMirrorCoordinator scheduler", e);
        }
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(metrics, "coordinator metrics");
        log.info("Shutdown complete");
    }

    @Override
    public void onElection(int partitionIndex, int partitionLeaderEpoch) {
        TopicPartition tp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, partitionIndex);
        runtime.scheduleLoadOperation(tp, partitionLeaderEpoch);
    }

    @Override
    public void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch) {
        TopicPartition tp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, partitionIndex);
        runtime.scheduleUnloadOperation(tp, partitionLeaderEpoch);
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
        runtime.onNewMetadataImage(newImage, delta);
    }

    private void throwIfNotActive() {
        if (state.get() != State.STARTED) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }

    @Override
    public int partitionFor(MirrorPartition key) {
        throwIfNotActive();
        return key.coordinatorPartition(mirrorConfig.stateTopicNumPartitions());
    }

    @Override
    public Properties mirrorStateTopicConfigs() {
        Properties properties = new Properties();
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, mirrorConfig.stateTopicSegmentBytes());
        properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, mirrorConfig.stateTopicMinISR());
        properties.put(TopicConfig.RETENTION_MS_CONFIG, -1);
        return properties;
    }

    /**
     * Lists distinct mirror names present in committed state across all locally hosted
     * {@code __mirror_state} shards.
     */
    public CompletableFuture<Set<String>> listLocalMirrorNames() {
        throwIfNotActive();

        List<CompletableFuture<Set<String>>> futures = runtime.scheduleReadAllOperation(
                "list-local-mirror-names",
                (shard, offset) -> shard.listMirrorNames(offset)
            ).stream()
            .map(future -> future.exceptionally(ex -> {
                Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
                if (cause instanceof NotCoordinatorException || cause instanceof CoordinatorLoadInProgressException) {
                    return Set.of();
                }
                throw new CompletionException(cause);
            }))
            .toList();

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .thenApply(v -> {
                Set<String> mirrorNames = new HashSet<>();
                futures.forEach(f -> mirrorNames.addAll(f.join()));
                return mirrorNames;
            });
    }

    @Override
    public CompletableFuture<ReadMirrorStatesResponseData> readPartitionStates(
            String mirrorName,
            Map<String, Set<Integer>> partitions
    ) {
        throwIfNotActive();

        // Phase 1: Group partitions by coordinator shard.
        // Each partition maps to a coordinator shard based on mirror name, topic id, and partition.
        Map<Integer, Map<String, Set<Integer>>> byCoordPartition = new HashMap<>();
        partitions.forEach((topic, parts) -> parts.forEach(part -> {
            int cp = partitionFor(MirrorPartition.of(mirrorName, metadataCache.getTopicId(topic), part));
            byCoordPartition.computeIfAbsent(cp, k -> new HashMap<>())
                .computeIfAbsent(topic, k -> new HashSet<>()).add(part);
        }));

        // Phase 2: Issue batched read operations to each coordinator shard.
        // All partitions for a given shard are read in a single operation.
        List<CompletableFuture<ReadMirrorStatesResponseData>> futures = new ArrayList<>();
        byCoordPartition.forEach((cp, tps) -> {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleReadOperation("read-mirror-states", mirrorStateTp,
                (shard, offset) -> shard.readPartitionStates(mirrorName, tps, offset)));
        });

        // Phase 3: Wait for all shard reads to complete, then merge results by topic.
        CompletableFuture<ReadMirrorStatesResponseData> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((v, e) -> {
                ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to read mirror states for {}", mirrorName, e);
                    data.setErrorCode(Errors.forException(e.getCause()).code());
                    data.setErrorMessage(e.getCause().getMessage());
                } else {
                    // Merge results: a topic's partitions span multiple shards, so group by topic name.
                    Map<String, List<ReadMirrorStatesResponseData.PartitionResult>> merged = new HashMap<>();
                    futures.forEach(f -> f.join().topics().forEach(topicResult ->
                        merged.computeIfAbsent(topicResult.topicName(), k -> new ArrayList<>())
                            .addAll(topicResult.partitions())));
                    List<ReadMirrorStatesResponseData.TopicResult> topicResults = merged.entrySet().stream()
                        .map(topicEntry -> new ReadMirrorStatesResponseData.TopicResult()
                            .setTopicName(topicEntry.getKey())
                            .setPartitions(topicEntry.getValue()))
                        .toList();
                    data.setTopics(topicResults);
                }
                resultFuture.complete(data);
            });
        return resultFuture;
    }

    @Override
    public CompletableFuture<WriteMirrorStatesResponseData> writePartitionStates(
            String mirrorName,
            Map<String, Set<MirrorStateWrite>> stateWrites
    ) {
        throwIfNotActive();

        // Phase 1: Group state writes by coordinator shard and track partition indices per topic.
        // Each partition maps to a coordinator shard based on mirror name, topic id, and partition.
        Map<Integer, Map<String, Set<MirrorStateWrite>>> byCoordPartition = new HashMap<>();
        Map<String, Set<Integer>> tps = new HashMap<>();

        stateWrites.forEach((topic, partitions) -> {
            Set<Integer> partitionIndices = new HashSet<>();
            partitions.forEach(partition -> {
                partitionIndices.add(partition.partition());
                int cp = partitionFor(MirrorPartition.of(
                    mirrorName, metadataCache.getTopicId(topic), partition.partition()));
                byCoordPartition.computeIfAbsent(cp, k -> new HashMap<>())
                    .computeIfAbsent(topic, k -> new HashSet<>()).add(partition);
            });
            tps.put(topic, partitionIndices);
        });

        // Phase 2: Issue batched write operations to each coordinator shard.
        // All state writes for a given shard are persisted in a single operation.
        List<CompletableFuture<Map<TopicPartition, ClusterMirrorCoordinatorShard.PartitionWriteResult>>> futures = new ArrayList<>();
        byCoordPartition.forEach((cp, states) -> {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleWriteOperation("write-mirror-states", mirrorStateTp,
                Duration.ofMillis(mirrorConfig.coordinatorWriteTimeoutMs()),
                shard -> shard.writePartitionStates(mirrorName, states)));
        });

        // Phase 3: Wait for all shard writes to complete, then merge results by topic.
        // Each shard returns per-partition write results (error code and new state epoch).
        CompletableFuture<WriteMirrorStatesResponseData> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((v, e) -> {
                WriteMirrorStatesResponseData data = new WriteMirrorStatesResponseData();
                if (e != null) {
                    log.error("Failed to write mirror states for {}", mirrorName, e);
                    data.setErrorCode(Errors.forException(e.getCause()).code());
                    data.setErrorMessage(e.getCause().getMessage());
                } else {
                    // Collect all partition write results from all shards.
                    Map<TopicPartition, ClusterMirrorCoordinatorShard.PartitionWriteResult> allResults = new HashMap<>();
                    futures.forEach(f -> allResults.putAll(f.join()));

                    // Build response: organize results by topic, preserving partition order.
                    List<WriteMirrorStatesResponseData.TopicResult> topicResults = new ArrayList<>();
                    tps.forEach((topic, indices) -> {
                        List<WriteMirrorStatesResponseData.PartitionResult> partitionResults = new ArrayList<>();
                        indices.forEach(i -> {
                            TopicPartition tp = new TopicPartition(topic, i);
                            ClusterMirrorCoordinatorShard.PartitionWriteResult pwr = allResults.get(tp);
                            WriteMirrorStatesResponseData.PartitionResult pr =
                                    new WriteMirrorStatesResponseData.PartitionResult();
                            pr.setPartitionIndex(i);
                            if (pwr != null) {
                                pr.setStateEpoch(pwr.stateEpoch());
                                pr.setErrorCode(pwr.error().code());
                            }
                            partitionResults.add(pr);
                        });
                        topicResults.add(new WriteMirrorStatesResponseData.TopicResult()
                            .setTopicName(topic).setPartitions(partitionResults));
                    });
                    data.setTopics(topicResults);
                }
                resultFuture.complete(data);
            });
        return resultFuture;
    }

    /**
     * Persists the last mirror positions (epoch and offset) across coordinator shards.
     *
     * @param mirrorName          the name of the cluster mirror
     * @param lastMirrorPositions map of topic partition to its last mirrored position
     * @return future completed when all records are persisted
     */
    public CompletableFuture<Void> writeLastMirrorPositions(
            String mirrorName, Map<TopicPartition, EpochOffset> lastMirrorPositions
    ) {
        throwIfNotActive();

        // Phase 1: Group positions by coordinator shard.
        Map<Integer, Map<TopicPartition, EpochOffset>> byCoordPartition = new HashMap<>();
        lastMirrorPositions.forEach((tp, position) -> {
            int cp = partitionFor(MirrorPartition.of(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition()));
            byCoordPartition.computeIfAbsent(cp, k -> new HashMap<>()).put(tp, position);
        });

        // Phase 2: Issue batched write operations to each coordinator shard and wait for completion.
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        byCoordPartition.forEach((cp, positions) -> {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleWriteOperation("write-mirror-positions", mirrorStateTp,
                Duration.ofMillis(mirrorConfig.coordinatorWriteTimeoutMs()),
                shard -> shard.writeLastMirrorPositions(mirrorName, positions)));
        });
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Writes tombstone records for all partitions of a mirror across all local coordinator shards.
     * Each shard iterates its own state to find entries for the mirror.
     *
     * @param mirrorName the name of the cluster mirror
     * @return future completed when all tombstone records are persisted
     */
    public CompletableFuture<Void> writeMirrorTombstones(String mirrorName) {
        throwIfNotActive();

        int numPartitions = mirrorConfig.stateTopicNumPartitions();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int cp = 0; cp < numPartitions; cp++) {
            TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, cp);
            futures.add(runtime.scheduleWriteOperation("write-mirror-tombstones", mirrorStateTp,
                Duration.ofMillis(mirrorConfig.coordinatorWriteTimeoutMs()),
                shard -> shard.writeMirrorTombstones(mirrorName)));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }
}
