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

import org.apache.kafka.common.LastMirror;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Thread-safe cache for the data leader's working state of mirror partitions.
 *
 * This cache sits on the data leader and holds partition metadata populated from coordinator
 * read/write completions. The authoritative state lives in the coordinator shard's timeline-backed
 * data structures, which provide read-committed semantics and automatic rollback on write failures.
 * This cache is a best-effort local projection of that state, optimised for fast reads on the
 * data-path without round-tripping to the coordinator.
 *
 * All updates use stateEpoch-based optimistic locking: a stale update (lower stateEpoch) is
 * silently rejected so that async callback races never overwrite fresher state.
 */
public class MirrorLeaderCache {
    private static final Logger LOG = LoggerFactory.getLogger(MirrorLeaderCache.class);

    // Data leader partition metadata, populated from coordinator read/write completions.
    // Uses stateEpoch-based optimistic locking to reject stale updates from async callbacks.
    private final Map<MirrorPartition, MirrorPartitionMetadata> mirrorPartitions = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, SourceClusterLeader>> sourceLeaders = new ConcurrentHashMap<>();

    private final MetadataCache metadataCache;
    private volatile MetadataImage metadataImage;

    public static MirrorLeaderCache empty(MetadataCache metadataCache) {
        return new MirrorLeaderCache(metadataCache, MetadataImage.EMPTY);
    }

    private MirrorLeaderCache(MetadataCache metadataCache, MetadataImage metadataImage) {
        this.metadataCache = metadataCache;
        this.metadataImage = metadataImage;
    }

    public void updateMetadataImage(MetadataImage newImage) {
        this.metadataImage = newImage;
    }

    public void clear() {
        mirrorPartitions.clear();
        sourceLeaders.clear();
    }

    // ======== MIRROR PARTITIONS ======================================================================================

    /**
     * Replays a full metadata snapshot from the coordinator log (shard load or read response).
     * Rejects snapshots with a stale stateEpoch; when the epoch matches, only advances the
     * last mirror epoch forward, never regressing it.
     */
    public void updatePartitionMetadata(MirrorPartition mp, MirrorPartitionMetadata metadata) {
        mirrorPartitions.compute(mp, (key, existing) -> {
            if (existing != null && existing.stateEpoch() >= metadata.stateEpoch()) {
                if (existing.stateEpoch() > metadata.stateEpoch()) {
                    LOG.debug("Skipping stale metadata update for {}: existing stateEpoch {} > incoming {}",
                            mp, existing.stateEpoch(), metadata.stateEpoch());
                }
                return existing;
            }
            return metadata;
        });
    }

    /**
     * Updates lifecycle state during a local transition (e.g. MIRRORING to STOPPING).
     * Leaves the last mirror epoch untouched; position updates travel through the coordinator.
     * Returns true if the transition was applied, false if a newer stateEpoch already superseded it.
     */
    public boolean updatePartitionState(
            MirrorPartition mp, MirrorPartitionState state, int stateEpoch,
            String errorMessage, boolean nonRetryable, short maxRetryAttempts
    ) {
        MirrorPartitionMetadata updated = mirrorPartitions.compute(mp, (key, current) -> {
            MirrorPartitionMetadata existing = MirrorPartitionMetadata.orEmpty(current);
            if (existing.stateEpoch() > stateEpoch) {
                LOG.debug("Skipping stale state update for {}: existing stateEpoch {} > incoming {}",
                        mp, existing.stateEpoch(), stateEpoch);
                return current;
            }
            // A read may already have installed this transition; do not increment retries twice
            if (existing.stateEpoch() == stateEpoch && current != null) {
                return current;
            }
            return new MirrorPartitionMetadata.Builder(existing)
                    .withTransitionErrorFields(state, errorMessage, nonRetryable, maxRetryAttempts)
                    .withState(state)
                    .withStateEpoch(stateEpoch)
                    .build();
        });
        return updated != null && updated.stateEpoch() == stateEpoch && updated.state() == state;
    }

    /**
     * Advances the cached last mirror epoch after the coordinator confirms persistence.
     * Each LME write bumps the coordinator stateEpoch, so a higher stateEpoch always
     * carries a newer position and stale completions are rejected.
     */
    public void updateLastMirrorEpoch(MirrorPartition mp, int stateEpoch, LastMirror position) {
        mirrorPartitions.computeIfPresent(mp, (key, existing) -> {
            if (existing.stateEpoch() > stateEpoch) {
                LOG.debug("Skipping stale LastMirrorEpoch update for {}: existing stateEpoch {} > incoming {}",
                        mp, existing.stateEpoch(), stateEpoch);
                return existing;
            }
            return new MirrorPartitionMetadata.Builder(existing)
                    .withLastMirrorEpoch(position)
                    .withStateEpoch(stateEpoch)
                    .build();
        });
    }

    public Set<MirrorPartition> getMirrorPartitions() {
        return mirrorPartitions.keySet();
    }

    public void clearFailedState(MirrorPartition mp, int stateEpoch) {
        mirrorPartitions.computeIfPresent(mp, (key, existing) -> {
            if (existing.stateEpoch() != stateEpoch || existing.state() != MirrorPartitionState.MIRRORING) {
                return existing;
            }
            return new MirrorPartitionMetadata.Builder(existing)
                    .withErrorMessage(null)
                    .withRetryAttempt((short) 0)
                    .withPrevState(null)
                    .build();
        });
    }

    public MirrorPartitionMetadata getPartitionMetadata(MirrorPartition mp) {
        return MirrorPartitionMetadata.orEmpty(mirrorPartitions.get(mp));
    }

    public void removePartitionMetadata(MirrorPartition mp) {
        mirrorPartitions.remove(mp);
    }

    public void removePartitionMeatadata(String mirrorName) {
        mirrorPartitions.keySet().removeIf(key -> key.mirrorName().equals(mirrorName));
    }

    public Map<TopicPartition, MirrorPartitionState> getPartitionStates(String mirrorName) {
        Map<TopicPartition, MirrorPartitionState> result = new HashMap<>();
        mirrorPartitions.keySet().forEach(key -> {
            if (key.mirrorName().equals(mirrorName)) {
                MirrorPartitionMetadata entry = getPartitionMetadata(key);
                if (entry != null && entry.state() != null) {
                    metadataCache.getTopicName(key.topicId()).ifPresent(topicName ->
                            result.put(new TopicPartition(topicName, key.partition()), entry.state()));
                }
            }
        });
        return result;
    }

    // ======== SOURCE LEADERS =========================================================================================

    public Optional<SourceClusterLeader> getSourceClusterLeader(String mirrorName, TopicPartition tp) {
        var partitionLeaders = sourceLeaders.get(mirrorName);
        if (partitionLeaders != null) {
            SourceClusterLeader leader = partitionLeaders.get(tp);
            if (leader != null) {
                return Optional.of(leader);
            }
        }
        return Optional.empty();
    }

    public void updateSourceClusterLeader(String mirrorName, TopicPartition tp, SourceClusterLeader leader) {
        sourceLeaders.computeIfAbsent(mirrorName, k -> new ConcurrentHashMap<>()).put(tp, leader);
    }

    public void removeSourceClusterLeaders(String mirrorName) {
        sourceLeaders.remove(mirrorName);
    }

    public record SourceClusterLeader(Optional<Node> node, int leaderEpoch) { }

    // ======== CLUSTER METADATA =======================================================================================

    public String getSourceClusterId(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return (String) props.get("source.cluster.id");
    }

    public String getSourceClusterBootstrap(String mirrorName) {
        Properties props = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName));
        return Optional.ofNullable(props.get("bootstrap.servers"))
                .map(Object::toString)
                .orElse(null);
    }

    public boolean clusterHasTopic(String topicName) {
        return metadataCache.contains(topicName);
    }

    public Properties getResourceConfig(ConfigResource resource) {
        return metadataCache.config(resource);
    }

    public Optional<Node> getAliveBrokerNode(int nodeId, ListenerName listenerName) {
        return metadataCache.getAliveBrokerNode(nodeId, listenerName);
    }

    public Uuid getTopicId(String topicName) {
        return metadataCache.getTopicId(topicName);
    }

    public Optional<String> getTopicName(Uuid topicId) {
        return metadataCache.getTopicName(topicId);
    }

    public Properties getTopicConfig(String topicName) {
        return metadataCache.topicConfig(topicName);
    }

    public List<MetadataResponseTopic> getTopicMetadata(
            Set<String> topics, ListenerName listenerName, boolean allowAutoTopicCreation, boolean supportsAutoTopicCreation) {
        return metadataCache.getTopicMetadata(topics, listenerName, allowAutoTopicCreation, supportsAutoTopicCreation);
    }

    public Optional<Node> getLeaderEndpoint(String topic, int partition, ListenerName listenerName) {
        return metadataCache.getPartitionLeaderEndpoint(topic, partition, listenerName);
    }

    public int getLeaderEpoch(TopicPartition tp) {
        TopicImage topicImage = metadataImage.topics().getTopic(tp.topic());
        if (topicImage == null) {
            return -1;
        }
        var partitionReg = topicImage.partitions().get(tp.partition());
        if (partitionReg == null) {
            return -1;
        }
        return partitionReg.leaderEpoch;
    }

    public Map<String, Set<Integer>> getTopicPartitionsMapping(String mirrorName) {
        Map<String, Set<Integer>> result = new HashMap<>();
        metadataImage.topics().topicsById().values().forEach(topicInfo -> {
            if (mirrorName.equals(topicInfo.mirrorName())) {
                Set<Integer> parts = new HashSet<>();
                for (int i = 0; i < topicInfo.partitions().size(); i++) {
                    parts.add(i);
                }
                result.put(topicInfo.name(), parts);
            }
        });
        return result;
    }

    public Set<String> getConfiguredMirrorNames() {
        return metadataImage.configs().resourceData().keySet().stream()
                .filter(resource -> resource.type() == ConfigResource.Type.CLUSTER_MIRROR)
                .map(ConfigResource::name)
                .collect(Collectors.toSet());
    }

    public Set<String> getMirrorTopics(String mirrorName, Set<MirrorPartitionState> includeStates) {
        return metadataImage.topics().topicsById().values().stream()
                .filter(topicInfo -> {
                    String topicMirrorName = topicInfo.mirrorName();
                    if (topicMirrorName == null || topicMirrorName.isBlank()) return false;
                    if (!mirrorName.equals(topicMirrorName)) return false;
                    return includeStates.contains(MirrorPartitionState.fromValue(topicInfo.desiredMirrorState()));
                })
                .map(TopicImage::name)
                .collect(Collectors.toSet());
    }
}
