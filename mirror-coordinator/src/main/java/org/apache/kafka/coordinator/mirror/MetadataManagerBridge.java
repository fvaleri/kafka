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
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.server.mirror.MirrorPartition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Bridge for bidirectional coordination between the coordinator (mirror-coordinator module)
 * and metadata manager (core module). This interface enables two distinct concerns:
 * <p>
 * 1. Lifecycle coordination: The coordinator notifies the manager about lifecycle events
 *    so it can initialize/cleanup resources based on coordinator partition assignments.
 * <p>
 * 2. State persistence seam: The manager reads/writes partition state transitions through
 *   the CoordinatorReader/CoordinatorWriter callbacks.
 */
public interface MetadataManagerBridge {
    void onCoordStartup(
        Function<MirrorPartition, Integer> coordPartFinder,
        CoordinatorReader coordinatorReader,
        CoordinatorWriter coordinatorWriter
    );

    void onShardLoaded(int coordPartition);

    void onShardUnloaded(int coordPartition, int coordPartitionCount);

    void onCoordShutdown();

    // Delegating the read through the runtime ensures that a shard still loading
    // surfaces as {@code COORDINATOR_LOAD_IN_PROGRESS} instead of returning
    // possibly-incomplete cached state.
    @FunctionalInterface
    interface CoordinatorReader {
        CompletableFuture<ReadMirrorStatesResponseData> readPartitionStates(
                String mirrorName,
                Map<String, Set<Integer>> partitions
        );
    }

    interface CoordinatorWriter {
        CompletableFuture<WriteMirrorStatesResponseData> writePartitionStates(
            String mirrorName,
            Map<String, Set<ClusterMirrorCoordinatorService.MirrorStateWrite>> states
        );

        CompletableFuture<Void> writeLastMirrorPositions(
            String mirrorName,
            Map<TopicPartition, EpochOffset> positions
        );

        CompletableFuture<Void> writeMirrorTombstones(String mirrorName);
    }
}
