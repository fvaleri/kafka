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
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Lifecycle interface for the cluster mirror coordinator.
 * Used by BrokerMetadataPublisher for leader election,
 * resignation, and metadata image updates.
 */
public interface ClusterMirrorCoordinator {
    /**
     * Activates the coordinator and initializes its dependencies.
     * This must be called before any other operations on the coordinator.
     * Startup is idempotent and can be called multiple times safely.
     */
    void startup();

    /**
     * Shuts down the coordinator and releases all resources.
     * After shutdown, the coordinator cannot be reused; a new instance must be created.
     * Shutdown is idempotent and can be called multiple times safely.
     */
    void shutdown();

    /**
     * Called when this broker is elected as the leader for a coordinator partition.
     * The coordinator will begin processing and writing state records to this partition.
     *
     * @param partitionIndex the coordinator partition index in the __mirror_state topic
     * @param partitionLeaderEpoch the epoch of the leadership
     */
    void onElection(int partitionIndex, int partitionLeaderEpoch);

    /**
     * Called when this broker loses leadership of a coordinator partition.
     * The coordinator will stop processing state records for this partition.
     *
     * @param partitionIndex the coordinator partition index in the __mirror_state topic
     * @param partitionLeaderEpoch the epoch at which leadership was lost, or empty if unknown
     */
    void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch);

    /**
     * Invoked when new cluster metadata becomes available.
     * Allows the coordinator to update its internal state based on metadata changes,
     * such as cluster mirror configuration updates or topic metadata changes.
     *
     * @param newImage the new metadata image
     * @param delta the incremental changes from the previous image
     */
    void onNewMetadataImage(
        CoordinatorMetadataImage newImage,
        CoordinatorMetadataDelta delta);

    /**
     * Determines the coordinator partition index for a given mirror partition key.
     * Uses composite hashing on (mirror name, topic id, partition) to distribute
     * partition-level coordination across brokers, ensuring load balance across
     * multiple mirrors with many partitions.
     *
     * @param key the mirror partition key (mirror name, topic id, partition)
     * @return the coordinator partition index in the __mirror_state topic
     * @throws IllegalStateException if the coordinator service is not active
     */
    int partitionFor(MirrorPartition key);

    /**
     * Returns the topic configuration properties for the internal mirror state topic (__mirror_state).
     * These configurations are used by brokers and the controller to create and configure the
     * mirror state topic, ensuring consistent settings across the cluster.
     *
     * The returned Properties typically include:
     * <ul>
     *   <li>cleanup.policy=compact (for log compaction of mirror state)</li>
     *   <li>compression.type=producer (inherited from producer)</li>
     *   <li>segment.bytes (from mirror.state.topic.segment.bytes config)</li>
     *   <li>min.insync.replicas (from mirror.state.topic.min.isr config)</li>
     *   <li>retention.ms=-1 (infinite retention)</li>
     * </ul>
     *
     * @return topic configuration properties for the mirror state topic
     */
    Properties mirrorStateTopicConfigs();

    /**
     * Reads the mirror partition states for a given cluster mirror and set of topics/partitions.
     * The request is distributed across coordinator partitions based on the mirror partition key,
     * and responses are aggregated before returning to the caller.
     *
     * @param mirrorName the name of the cluster mirror
     * @param partitions a map of topic names to partition numbers to read state for
     * @return a CompletableFuture that completes with the read mirror states response data
     */
    CompletableFuture<ReadMirrorStatesResponseData> readPartitionStates(
            String mirrorName,
            Map<String, Set<Integer>> partitions
    );

    /**
     * Writes the mirror partition states for a given cluster mirror.
     * The states are distributed across coordinator partitions based on the mirror partition key,
     * and writes are performed in parallel across all affected partitions.
     *
     * @param mirrorName the name of the cluster mirror
     * @param states a map of topic names to sets of mirror state writes for that topic
     * @return a CompletableFuture that completes with the write mirror states response data
     */
    CompletableFuture<WriteMirrorStatesResponseData> writePartitionStates(
            String mirrorName,
            Map<String, Set<MirrorStateWrite>> states
    );

    /**
     * Represents a write operation to the mirror state log.
     * Contains the state change and metadata needed to persist the state of a mirrored partition.
     *
     * @param partition the coordinator partition index in the __mirror_state topic
     * @param state the mirror partition state being written
     * @param leaderEpoch the broker leader epoch when the state was determined
     * @param stateEpoch the epoch counter for the state value
     * @param lastMirror the last mirrored epoch and offset from the source topic
     * @param errorMessage error message if the state is in error, null otherwise
     * @param retryAttempt the current retry attempt number for failed mirrors
     * @param nonRetryable true if the error is non-retriable and requires manual intervention
     */
    record MirrorStateWrite(
            int partition,
            MirrorPartitionState state,
            int leaderEpoch,
            int stateEpoch,
            LastMirror lastMirror,
            String errorMessage,
            short retryAttempt,
            boolean nonRetryable
    ) { }
}
