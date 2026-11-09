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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Aggregates per-shard mirror partition state counts into Yammer gauges.
 * Each active shard contributes its committed counts; the global gauge
 * sums across all shards.
 */
public class ClusterMirrorCoordinatorMetrics extends CoordinatorMetrics {
    private final Map<TopicPartition, ClusterMirrorCoordinatorMetricsShard> shards = new ConcurrentHashMap<>();
    private final MetricsRegistry registry;

    public ClusterMirrorCoordinatorMetrics() {
        this.registry = KafkaYammerMetrics.defaultRegistry();
        registerGauges();
    }

    @Override
    public ClusterMirrorCoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp) {
        return new ClusterMirrorCoordinatorMetricsShard(snapshotRegistry, tp);
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) {
        if (!(shard instanceof ClusterMirrorCoordinatorMetricsShard)) {
            throw new IllegalArgumentException("Expected ClusterMirrorCoordinatorMetricsShard");
        }
        shards.put(shard.topicPartition(), (ClusterMirrorCoordinatorMetricsShard) shard);
    }

    @Override
    public void deactivateMetricsShard(CoordinatorMetricsShard shard) {
        shards.remove(shard.topicPartition());
    }

    @Override
    public MetricsRegistry registry() {
        return registry;
    }

    @Override
    public void onUpdateLastCommittedOffset(TopicPartition tp, long offset) {
        CoordinatorMetricsShard shard = shards.get(tp);
        if (shard != null) {
            shard.commitUpTo(offset);
        }
    }

    private long numPartitionsInState(MirrorPartitionState state) {
        return shards.values().stream()
                .mapToLong(shard -> shard.numPartitionsInState(state))
                .sum();
    }

    private void registerGauges() {
        for (MirrorPartitionState state : MirrorPartitionState.values()) {
            if (state == MirrorPartitionState.UNKNOWN) continue;
            String gaugeName = toCamelCase(state.name()) + "PartitionState";
            registry.newGauge(metricName(gaugeName), new Gauge<Long>() {
                @Override
                public Long value() {
                    return numPartitionsInState(state);
                }
            });
        }
    }

    private static String toCamelCase(String name) {
        StringBuilder sb = new StringBuilder();
        for (String part : name.split("_")) {
            sb.append(part.charAt(0)).append(part.substring(1).toLowerCase(Locale.ROOT));
        }
        return sb.toString();
    }

    private static MetricName metricName(String name) {
        return getMetricName("kafka.server.mirror", "MirrorMetadataManager", name);
    }
}
