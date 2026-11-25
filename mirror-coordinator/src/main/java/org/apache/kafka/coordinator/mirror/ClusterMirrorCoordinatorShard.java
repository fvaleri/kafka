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

import org.apache.kafka.common.LastMirror;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.mirror.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.coordinator.mirror.metrics.ClusterMirrorCoordinatorMetrics;
import org.apache.kafka.coordinator.mirror.metrics.ClusterMirrorCoordinatorMetricsShard;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shard (state machine) for the cluster mirror coordinator.
 * One instance per __mirror_state partition, managed by the CoordinatorRuntime.
 *
 * Responsibilities:
 * - Maintains in-memory state of mirror partition transitions (MIRRORING, PAUSED, STOPPED, FAILED)
 * - Persists partition state transitions and last mirror positions to the log
 * - Applies state transitions based on external requests (via record replay)
 * - Tracks leader and state epochs to detect stale write attempts
 *
 * All operations are executed sequentially by the runtime's single-threaded executor.
 */
public class ClusterMirrorCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final Logger log;
    private final ClusterMirrorConfig mirrorConfig;
    private final MetadataManagerBridge mirrorManager;
    private final MetadataCache metadataCache;
    private final TopicPartition topicPartition;
    private final int numPartitions;

    // Partition metadata is stored in a TimelineHashMap so that reads at the
    // committed offset see only committed state and rollbacks auto-revert
    private final TimelineHashMap<MirrorPartition, MirrorPartitionMetadata> partitionMetadata;
    private final TimelineHashMap<MirrorPartition, Integer> leaderEpochMap;
    private final TimelineHashMap<MirrorPartition, Integer> stateEpochMap;

    private final CoordinatorMetrics coordinatorMetrics;
    private final ClusterMirrorCoordinatorMetricsShard metricsShard;

    public static class Builder implements CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> {
        private final ClusterMirrorConfig config;
        private final MetadataManagerBridge mirrorManager;
        private final MetadataCache metadataCache;
        private final int numPartitions;
        private LogContext logContext;
        private TopicPartition topicPartition;
        private SnapshotRegistry snapshotRegistry;
        private CoordinatorMetrics coordinatorMetrics;

        public Builder(ClusterMirrorConfig config, MetadataManagerBridge mirrorManager, MetadataCache metadataCache, int numPartitions) {
            this.config = config;
            this.mirrorManager = mirrorManager;
            this.metadataCache = metadataCache;
            this.numPartitions = numPartitions;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withTime(Time time) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ClusterMirrorCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        public ClusterMirrorCoordinatorShard build() {
            if (logContext == null) throw new IllegalArgumentException("LogContext must not be null");
            if (topicPartition == null) throw new IllegalArgumentException("TopicPartition must not be null");
            if (snapshotRegistry == null) throw new IllegalArgumentException("SnapshotRegistry must not be null");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof ClusterMirrorCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type ClusterMirrorCoordinatorMetrics");

            ClusterMirrorCoordinatorMetricsShard metricsShard =
                    ((ClusterMirrorCoordinatorMetrics) coordinatorMetrics).newMetricsShard(snapshotRegistry, topicPartition);

            return new ClusterMirrorCoordinatorShard(logContext, config, mirrorManager, metadataCache,
                    topicPartition, numPartitions, snapshotRegistry, coordinatorMetrics, metricsShard);
        }
    }

    private ClusterMirrorCoordinatorShard(
        LogContext logContext,
        ClusterMirrorConfig mirrorConfig,
        MetadataManagerBridge mirrorManager,
        MetadataCache metadataCache,
        TopicPartition topicPartition,
        int numPartitions,
        SnapshotRegistry snapshotRegistry,
        CoordinatorMetrics coordinatorMetrics,
        ClusterMirrorCoordinatorMetricsShard metricsShard
    ) {
        this.log = logContext.logger(ClusterMirrorCoordinatorShard.class);
        this.mirrorConfig = mirrorConfig;
        this.mirrorManager = mirrorManager;
        this.metadataCache = metadataCache;
        this.topicPartition = topicPartition;
        this.numPartitions = numPartitions;
        this.partitionMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.leaderEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.stateEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
        coordinatorMetrics.activateMetricsShard(metricsShard);
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) {
        ApiMessage key = record.key();
        ApiMessageAndVersion value = record.value();

        try {
            switch (CoordinatorRecordType.fromId(key.apiKey())) {
                case MIRROR_PARTITION_STATE:
                    replayPartitionState((MirrorPartitionStateKey) key, value);
                    break;
                case LAST_MIRROR_EPOCHS:
                    replayLastMirrorEpoch((LastMirrorEpochsKey) key, value);
                    break;
                default:
                    break;
            }
        } catch (UnsupportedVersionException ex) {
            // Ignore unsupported versions during replay
        }
    }

    private void replayPartitionState(MirrorPartitionStateKey key, ApiMessageAndVersion value) {
        MirrorPartition mp = new MirrorPartition(key.mirrorName(), key.topicId(), key.partition());
        if (value != null) {
            MirrorPartitionStateValue stateValue = (MirrorPartitionStateValue) value.message();
            MirrorPartitionState state = MirrorPartitionState.fromValue(stateValue.state());
            MirrorPartitionState previousState = MirrorPartitionState.fromValue(stateValue.previousState());
            maybeUpdateLeaderEpochMap(mp, stateValue.leaderEpoch());
            maybeUpdateStateEpochMap(mp, stateValue.stateEpoch());
            MirrorPartitionMetadata existing = MirrorPartitionMetadata.orEmpty(partitionMetadata.get(mp));

            // Update per-state metrics counters
            MirrorPartitionState oldState = existing.state();
            if (oldState != null && oldState != MirrorPartitionState.UNKNOWN && oldState != state) {
                metricsShard.decrementPartitionStateCount(oldState);
            }
            if (state != MirrorPartitionState.UNKNOWN && oldState != state) {
                metricsShard.incrementPartitionStateCount(state);
            }

            partitionMetadata.put(mp, new MirrorPartitionMetadata.Builder(existing)
                    .withState(state)
                    .withStateEpoch(stateValue.stateEpoch())
                    .withErrorMessage(stateValue.errorMessage())
                    .withRetryAttempt(stateValue.retryAttempt())
                    .withPrevState(previousState == MirrorPartitionState.UNKNOWN ? null : previousState)
                    .build());
        } else {
            // Update per-state metrics counters
            MirrorPartitionMetadata removed = partitionMetadata.get(mp);
            if (removed != null && removed.state() != null && removed.state() != MirrorPartitionState.UNKNOWN) {
                metricsShard.decrementPartitionStateCount(removed.state());
            }

            partitionMetadata.remove(mp);
            leaderEpochMap.remove(mp);
            stateEpochMap.remove(mp);
        }
    }

    private void replayLastMirrorEpoch(LastMirrorEpochsKey key, ApiMessageAndVersion value) {
        MirrorPartition mp = new MirrorPartition(key.mirrorName(), key.topicId(), key.partition());
        if (value != null) {
            LastMirrorEpochsValue lme = (LastMirrorEpochsValue) value.message();
            maybeUpdateLeaderEpochMap(mp, lme.leaderEpoch());
            maybeUpdateStateEpochMap(mp, lme.stateEpoch());
            MirrorPartitionMetadata existing = MirrorPartitionMetadata.orEmpty(partitionMetadata.get(mp));

            partitionMetadata.put(mp,
                    new MirrorPartitionMetadata.Builder(existing)
                        .withLastMirrorEpoch(new LastMirror(lme.lastMirrorOffset(), lme.lastMirrorEpoch()))
                        .withStateEpoch(lme.stateEpoch())
                        .build());
        } else {
            partitionMetadata.remove(mp);
        }
    }

    @Override
    public void onLoaded(CoordinatorMetadataImage newImage) {
        log.info("Loaded shard for partition {}", topicPartition);
        mirrorManager.onShardLoaded(topicPartition.partition());
    }

    @Override
    public void onUnloaded() {
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
        mirrorManager.onShardUnloaded(topicPartition.partition(), numPartitions);
        log.info("Unloaded shard for partition {}", topicPartition);
    }

    @Override
    public void onNewMetadataImage(CoordinatorMetadataImage newImage, CoordinatorMetadataDelta delta) {
    }

    /**
     * Reads the committed state of mirror partitions from the shard's timeline-backed store.
     * Uses epoch-aware reads so that only committed data is visible to callers, preventing
     * uncommitted writes from leaking to concurrent readers.
     */
    public ReadMirrorStatesResponseData readPartitionStates(
            String mirrorName, Map<String, Set<Integer>> partitions, long committedOffset
    ) {
        ReadMirrorStatesResponseData data = new ReadMirrorStatesResponseData();
        ReadMirrorStatesResponseData.TopicResultCollection topicResults = new ReadMirrorStatesResponseData.TopicResultCollection();
        partitions.forEach((topic, parts) -> {
            ReadMirrorStatesResponseData.PartitionResultCollection partitionResults = new ReadMirrorStatesResponseData.PartitionResultCollection();
            parts.forEach(part -> {
                MirrorPartition mp = new MirrorPartition(mirrorName, metadataCache.getTopicId(topic), part);
                // Read at committed offset to ensure only committed state is returned
                MirrorPartitionMetadata mpm = MirrorPartitionMetadata.orEmpty(
                        partitionMetadata.get(mp, committedOffset));
                Integer epoch = leaderEpochMap.get(mp, committedOffset);
                int leaderEpoch = epoch != null ? epoch : -1;
                ReadMirrorStatesResponseData.PartitionResult pr = new ReadMirrorStatesResponseData.PartitionResult()
                        .setPartitionIndex(part)
                        .setState(mpm.state().value())
                        .setLeaderEpoch(leaderEpoch)
                        .setStateEpoch(mpm.stateEpoch())
                        .setPreviousState(mpm.prevState() != null ?
                                mpm.prevState().value() : MirrorPartitionState.UNKNOWN.value())
                        .setLastMirrorEpoch(mpm.lastMirrorEpoch().epoch())
                        .setLastMirrorOffset(mpm.lastMirrorEpoch().offset())
                        .setRetryAttempt(mpm.retryAttempt())
                        .setErrorMessage(mpm.errorMessage());
                partitionResults.add(pr);
            });
            topicResults.add(new ReadMirrorStatesResponseData.TopicResult()
                    .setTopicName(topic).setPartitions(partitionResults));
        });
        data.setTopics(topicResults);
        return data;
    }

    /**
     * Returns the distinct mirror names present in this shard's committed partition metadata.
     */
    public Set<String> readMirrorNames(long committedOffset) {
        Set<String> mirrorNames = new HashSet<>();
        for (MirrorPartition mp : partitionMetadata.keySet(committedOffset)) {
            mirrorNames.add(mp.mirrorName());
        }
        return mirrorNames;
    }

    /**
     * Writes partition state transitions and optional last mirror positions in a batch.
     */
    public CoordinatorResult<Map<TopicPartition, PartitionWriteResult>, CoordinatorRecord> writePartitionStates(
        String mirrorName,
        Map<String, Set<ClusterMirrorCoordinatorService.MirrorStateWrite>> stateWrites
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        Map<TopicPartition, PartitionWriteResult> results = new HashMap<>();
        // Tracks in-flight state across partitions within this request so that a second write
        // to the same partition sees the result of the first, without mutating the timeline maps
        Map<MirrorPartition, PendingPartitionState> pendingStates = new HashMap<>();
        stateWrites.forEach((topic, partitions) -> partitions.forEach(partition -> {
            TopicPartition tp = new TopicPartition(topic, partition.partition());
            if (partition.state() != null && partition.state() != MirrorPartitionState.UNKNOWN) {
                try {
                    CoordinatorResult<Integer, CoordinatorRecord> result =
                            writePartitionState(mirrorName, tp, partition.state(),
                                    partition.leaderEpoch(), partition.stateEpoch(),
                                    partition.errorMessage(),
                                    partition.nonRetryable(), pendingStates);
                    records.addAll(result.records());
                    results.put(tp, new PartitionWriteResult(Errors.NONE, result.response()));
                } catch (FencedLeaderEpochException e) {
                    results.put(tp, new PartitionWriteResult(Errors.FENCED_LEADER_EPOCH, -1));
                    return;
                } catch (FencedStateEpochException e) {
                    results.put(tp, new PartitionWriteResult(Errors.FENCED_STATE_EPOCH, -1));
                    return;
                }
            }
            LastMirror lm = partition.lastMirrorEpoch();
            if (lm != null && (lm.epoch() != -1 || lm.offset() != -1)) {
                try {
                    CoordinatorResult<Integer, CoordinatorRecord> lmeResult =
                            writeLastMirrorEpoch(mirrorName, tp, lm,
                                    partition.leaderEpoch(), partition.stateEpoch(), pendingStates);
                    records.addAll(lmeResult.records());
                    results.put(tp, new PartitionWriteResult(Errors.NONE, lmeResult.response()));
                } catch (FencedLeaderEpochException e) {
                    results.put(tp, new PartitionWriteResult(Errors.FENCED_LEADER_EPOCH, -1));
                } catch (FencedStateEpochException e) {
                    results.put(tp, new PartitionWriteResult(Errors.FENCED_STATE_EPOCH, -1));
                }
            }
        }));
        return new CoordinatorResult<>(records, results);
    }

    /**
     * Writes validate against the latest replayed state.
     * Timeline mutations belong in replay.
     */
    private CoordinatorResult<Integer, CoordinatorRecord> writePartitionState(
            String mirrorName, TopicPartition tp, MirrorPartitionState state, int leaderEpoch,
            int stateEpoch, String errorMessage, boolean nonRetryable,
            Map<MirrorPartition, PendingPartitionState> pendingStates
    ) {
        MirrorPartition mp = new MirrorPartition(mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
        PendingPartitionState current = pendingStates.get(mp);
        if (current == null) {
            current = new PendingPartitionState(
                    MirrorPartitionMetadata.orEmpty(partitionMetadata.get(mp)),
                    leaderEpochMap.getOrDefault(mp, -1), stateEpochMap.getOrDefault(mp, 0));
        }
        if (leaderEpoch != -1 && current.leaderEpoch() > leaderEpoch) {
            log.info("MirrorPartitionState write fenced for partition {} because leader epoch {} < current {}",
                    tp, leaderEpoch, current.leaderEpoch());
            throw Errors.FENCED_LEADER_EPOCH.exception();
        }
        if (stateEpoch != -1 && current.stateEpoch() > stateEpoch) {
            log.info("MirrorPartitionState write fenced for {} because current epoch {} > expected {}",
                    tp, current.stateEpoch(), stateEpoch);
            throw Errors.FENCED_STATE_EPOCH.exception();
        }
        MirrorPartitionMetadata existing = current.metadata();
        if (!MirrorPartitionMetadata.isValidStateTransition(existing.state(), state)) {
            log.warn("Skipping invalid partition {} transition from {} to {}", tp, existing.state(), state);
            return new CoordinatorResult<>(List.of(), current.stateEpoch());
        }

        MirrorPartitionMetadata updated = new MirrorPartitionMetadata.Builder(existing)
                .withTransitionErrorFields(state, errorMessage,
                        nonRetryable, mirrorConfig.failedRetryMaxAttempts())
                .withState(state)
                .build();
        int newLeaderEpoch = leaderEpoch != -1 ? leaderEpoch : current.leaderEpoch();
        int newStateEpoch = current.stateEpoch() + 1;
        pendingStates.put(mp, new PendingPartitionState(updated, newLeaderEpoch, newStateEpoch));

        var key = new MirrorPartitionStateKey()
                .setMirrorName(mirrorName)
                .setTopicId(mp.topicId())
                .setPartition(mp.partition());
        var val = new MirrorPartitionStateValue()
                .setState(state.value())
                .setLeaderEpoch(leaderEpoch)
                .setStateEpoch(newStateEpoch)
                .setPreviousState(updated.prevState() != null ?
                        updated.prevState().value() : MirrorPartitionState.UNKNOWN.value())
                .setRetryAttempt(updated.retryAttempt())
                .setErrorMessage(updated.errorMessage());
        CoordinatorRecord record = CoordinatorRecord.record(key,
                new ApiMessageAndVersion(val, MirrorPartitionStateValue.HIGHEST_SUPPORTED_VERSION));
        log.debug("Writing partitionState record to partition {} with content: {}", tp, record);
        return new CoordinatorResult<>(List.of(record), newStateEpoch);
    }

    /**
     * Validates leader epoch, bumps state epoch, and writes a last mirror epoch record.
     * Mirrors the fencing logic in {@link #writePartitionState}.
     */
    private CoordinatorResult<Integer, CoordinatorRecord> writeLastMirrorEpoch(
            String mirrorName, TopicPartition tp, LastMirror lastMirror,
            int leaderEpoch, int stateEpoch, Map<MirrorPartition,
            PendingPartitionState> pendingStates
    ) {
        MirrorPartition mp = new MirrorPartition(
                mirrorName, metadataCache.getTopicId(tp.topic()), tp.partition());
        boolean alreadyValidated = pendingStates.containsKey(mp);
        PendingPartitionState current = pendingStates.get(mp);
        if (current == null) {
            current = new PendingPartitionState(
                    MirrorPartitionMetadata.orEmpty(partitionMetadata.get(mp)),
                    leaderEpochMap.getOrDefault(mp, -1), stateEpochMap.getOrDefault(mp, 0));
        }
        if (!alreadyValidated && leaderEpoch != -1 && current.leaderEpoch() > leaderEpoch) {
            log.info("LastMirrorEpoch write fenced for partition {} because leader epoch {} < current {}",
                    tp, leaderEpoch, current.leaderEpoch());
            throw Errors.FENCED_LEADER_EPOCH.exception();
        }
        if (!alreadyValidated && stateEpoch != -1 && current.stateEpoch() > stateEpoch) {
            log.info("LastMirrorEpoch write fenced for {} because current epoch {} > expected {}",
                    tp, current.stateEpoch(), stateEpoch);
            throw Errors.FENCED_STATE_EPOCH.exception();
        }
        int newLeaderEpoch = leaderEpoch != -1 ? leaderEpoch : current.leaderEpoch();
        int newStateEpoch = current.stateEpoch() + 1;
        pendingStates.put(mp, new PendingPartitionState(current.metadata(), newLeaderEpoch, newStateEpoch));

        var key = new LastMirrorEpochsKey()
                .setMirrorName(mp.mirrorName())
                .setTopicId(mp.topicId())
                .setPartition(mp.partition());
        var val = new LastMirrorEpochsValue()
                .setLastMirrorEpoch(lastMirror.epoch())
                .setLastMirrorOffset(lastMirror.offset())
                .setLeaderEpoch(newLeaderEpoch)
                .setStateEpoch(newStateEpoch);
        CoordinatorRecord record = CoordinatorRecord.record(key,
                new ApiMessageAndVersion(val, LastMirrorEpochsValue.HIGHEST_SUPPORTED_VERSION));
        return new CoordinatorResult<>(List.of(record), newStateEpoch);
    }

    /**
     * Writes tombstone records for all partitions of a mirror that this shard owns.
     * Iterates the shard's own partition metadata to find entries.
     * No epoch bump: tombstones delete the partition entirely, so there is no surviving state to version.
     */
    public CoordinatorResult<Void, CoordinatorRecord> writeMirrorTombstones(String mirrorName) {
        List<CoordinatorRecord> records = new ArrayList<>();
        partitionMetadata.keySet().forEach(mp -> {
            if (mp.mirrorName().equals(mirrorName)) {
                records.add(CoordinatorRecord.tombstone(new MirrorPartitionStateKey()
                    .setMirrorName(mirrorName).setTopicId(mp.topicId()).setPartition(mp.partition())));
                records.add(CoordinatorRecord.tombstone(new LastMirrorEpochsKey()
                    .setMirrorName(mirrorName).setTopicId(mp.topicId()).setPartition(mp.partition())));
            }
        });
        return new CoordinatorResult<>(records, null);
    }

    private void maybeUpdateLeaderEpochMap(MirrorPartition mp, int leaderEpoch) {
        if (leaderEpoch == -1) return;
        leaderEpochMap.putIfAbsent(mp, leaderEpoch);
        if (leaderEpochMap.get(mp) < leaderEpoch) {
            leaderEpochMap.put(mp, leaderEpoch);
        }
    }

    private void maybeUpdateStateEpochMap(MirrorPartition mp, int stateEpoch) {
        stateEpochMap.putIfAbsent(mp, stateEpoch);
        if (stateEpochMap.get(mp) < stateEpoch) {
            stateEpochMap.put(mp, stateEpoch);
        }
    }

    private record PendingPartitionState(MirrorPartitionMetadata metadata, int leaderEpoch, int stateEpoch) { }
    public record PartitionWriteResult(Errors error, int stateEpoch) { }
}
