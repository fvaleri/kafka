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
package org.apache.kafka.coordinator.mirror.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-shard metrics for the cluster mirror coordinator. Tracks the number of
 * mirror partitions in each {@link MirrorPartitionState} using timeline-backed
 * counters. On commit, the timeline values are copied to atomic longs so that
 * metrics only reflect committed state.
 */
public class ClusterMirrorCoordinatorMetricsShard implements CoordinatorMetricsShard {
    private final TopicPartition topicPartition;
    private final Map<MirrorPartitionState, TimelineGaugeCounter> partitionStateCounters;

    public ClusterMirrorCoordinatorMetricsShard(
        SnapshotRegistry snapshotRegistry,
        TopicPartition topicPartition
    ) {
        Objects.requireNonNull(snapshotRegistry);
        this.topicPartition = Objects.requireNonNull(topicPartition);
        this.partitionStateCounters = new EnumMap<>(MirrorPartitionState.class);
        for (MirrorPartitionState state : MirrorPartitionState.values()) {
            if (state != MirrorPartitionState.UNKNOWN) {
                partitionStateCounters.put(state,
                        new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0)));
            }
        }
    }

    public void incrementPartitionStateCount(MirrorPartitionState state) {
        TimelineGaugeCounter counter = partitionStateCounters.get(state);
        if (counter != null) {
            synchronized (counter.timelineLong) {
                counter.timelineLong.increment();
            }
        }
    }

    public void decrementPartitionStateCount(MirrorPartitionState state) {
        TimelineGaugeCounter counter = partitionStateCounters.get(state);
        if (counter != null) {
            synchronized (counter.timelineLong) {
                counter.timelineLong.decrement();
            }
        }
    }

    public long numPartitionsInState(MirrorPartitionState state) {
        TimelineGaugeCounter counter = partitionStateCounters.get(state);
        return counter != null ? counter.atomicLong.get() : 0;
    }

    @Override
    public void record(String sensorName) { }

    @Override
    public void record(String sensorName, double val) { }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public void commitUpTo(long offset) {
        for (var entry : partitionStateCounters.entrySet()) {
            TimelineGaugeCounter counter = entry.getValue();
            synchronized (counter.timelineLong) {
                long value = counter.timelineLong.get(offset);
                counter.atomicLong.set(value);
            }
        }
    }

    private record TimelineGaugeCounter(TimelineLong timelineLong, AtomicLong atomicLong) { }
}
