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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ReadMirrorOffsetsRequestData;
import org.apache.kafka.common.message.ReadMirrorOffsetsResponseData;
import org.apache.kafka.common.message.ReadMirrorStatesRequestData;
import org.apache.kafka.common.message.ReadMirrorStatesResponseData;
import org.apache.kafka.common.message.WriteMirrorStatesRequestData;
import org.apache.kafka.common.message.WriteMirrorStatesResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ReadMirrorOffsetsRequest;
import org.apache.kafka.common.requests.ReadMirrorOffsetsResponse;
import org.apache.kafka.common.requests.ReadMirrorStatesRequest;
import org.apache.kafka.common.requests.ReadMirrorStatesResponse;
import org.apache.kafka.common.requests.WriteMirrorStatesRequest;
import org.apache.kafka.common.requests.WriteMirrorStatesResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.mirror.ClusterMirrorCoordinator.MirrorStateWrite;
import org.apache.kafka.coordinator.mirror.MetadataManagerBridge.CoordinatorReader;
import org.apache.kafka.coordinator.mirror.MetadataManagerBridge.CoordinatorWriter;
import org.apache.kafka.coordinator.mirror.MirrorMetadataCache;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionMetadata;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;

/**
 * Routes read and write requests to the local or remote coordinator.
 * Created by MirrorMetadataManager.
 */
@SuppressWarnings("ClassFanOutComplexity")
class MirrorCoordinatorSender {
    private final Logger log;
    private final KafkaConfig brokerConfig;
    private final int nodeId;
    private final Supplier<MetadataImage> metadataImageSupplier;
    private final MirrorMetadataCache mirrorCache;
    private final Metrics metrics;
    private final Time time;

    private Optional<Function<MirrorPartition, Integer>> coordPartFinder;
    private Optional<CoordinatorReader> coordinatorReader;
    private Optional<CoordinatorWriter> coordinatorWriter;
    private volatile MirrorInterBrokerSender interBrokerSender;

    MirrorCoordinatorSender(KafkaConfig brokerConfig,
                            Supplier<MetadataImage> metadataImageSupplier,
                            MirrorMetadataCache mirrorCache,
                            Function<MirrorPartition, Integer> coordPartFinder,
                            CoordinatorReader coordinatorReader,
                            CoordinatorWriter coordinatorWriter,
                            Metrics metrics,
                            Time time) {
        this.brokerConfig = brokerConfig;
        this.nodeId = brokerConfig.nodeId();
        String name = "[" + MirrorCoordinatorSender.class.getSimpleName() + " brokerId=" + nodeId + "] ";
        this.log = new LogContext(name).logger(MirrorCoordinatorSender.class);
        this.metadataImageSupplier = metadataImageSupplier;
        this.mirrorCache = mirrorCache;
        this.coordPartFinder = Optional.of(coordPartFinder);
        this.coordinatorReader = Optional.of(coordinatorReader);
        this.coordinatorWriter = Optional.of(coordinatorWriter);
        this.metrics = metrics;
        this.time = time;
    }

    void startup() {
        String name = MirrorInterBrokerSender.class.getSimpleName()  + " brokerId=" + nodeId;
        this.interBrokerSender = new MirrorInterBrokerSender(name,
                NetworkUtils.buildNetworkClient(MirrorCoordinatorSender.class.getSimpleName(),
                        brokerConfig, metrics, time, new LogContext(name)),
                brokerConfig.requestTimeoutMs(), Time.SYSTEM);
        interBrokerSender.start();
    }

    void shutdown() throws InterruptedException {
        if (interBrokerSender != null) {
            interBrokerSender.shutdown();
        }
    }

    boolean isInitialized() {
        return coordPartFinder.isPresent();
    }

    Optional<Function<MirrorPartition, Integer>> coordPartFinder() {
        return coordPartFinder;
    }

    boolean isLocalCoordinatorFor(String mirrorName, Uuid topicId, int partition) {
        MetadataImage image = metadataImageSupplier.get();
        if (image.topics().getTopic(MIRROR_STATE_TOPIC_NAME) != null && coordPartFinder.isPresent()) {
            int coordId = image.topics().getTopic(MIRROR_STATE_TOPIC_NAME)
                    .partitions().get(coordPartFinder.get().apply(
                            MirrorPartition.of(mirrorName, topicId, partition))).leader;
            return coordId == nodeId;
        }
        return false;
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

    // ===== READ OPERATIONS ==========================================================================================

    /**
     * Reads mirror partition states from the local coordinator via {@link CoordinatorReader}.
     * If a shard is still loading, the coordinator responds with {@link CoordinatorLoadInProgressException}
     * and the read returns empty data.
     */
    CompletableFuture<ReadMirrorStatesResponse> readStateFromLocalCoordinator(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        return coordinatorReader.map(reader -> reader.readPartitionStates(mirrorName, partitions)
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
                        if (cause instanceof CoordinatorLoadInProgressException) {
                            log.debug("Failed to read local state for partitions {} (shard loading).", partitions);
                        } else {
                            log.warn("Failed to read local state for partitions {}. {}", partitions, cause.getMessage());
                        }
                        Errors error = Errors.forException(cause);
                        return new ReadMirrorStatesResponse(new ReadMirrorStatesResponseData().setErrorCode(error.code()).setErrorMessage(error.message()));
                    }

                    // update the data leader cache
                    res.topics().forEach(topic ->
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
                                                .build(), false);
                            }));
                    return new ReadMirrorStatesResponse(res);
                })).orElseGet(() -> CompletableFuture.failedFuture(new IllegalStateException("No local coordinator reader available")));
    }

    /**
     * Read mirror partition states from remote coordinators, batching requests per coordinator node.
     * Updates the local {@link MirrorMetadataCache} with each response, then returns a merged response
     * after all nodes have replied.
     */
    CompletableFuture<ReadMirrorStatesResponse> readStateFromRemoteCoordinator(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        log.debug("Reading states from remote coordinator: {} {}", mirrorName, partitions);

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

        ReadMirrorStatesResponseData merged = new ReadMirrorStatesResponseData();
        AtomicInteger remaining = new AtomicInteger(nodeToTopicPartitions.size());
        CompletableFuture<ReadMirrorStatesResponse> future = new CompletableFuture<>();

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
                                    // update the data leader cache
                                    mirrorCache.updatePartitionMetadata(mp,
                                            new MirrorPartitionMetadata.Builder()
                                                    .withState(MirrorPartitionState.fromValue(partition.state()))
                                                    .withStateEpoch(partition.stateEpoch())
                                                    .withLastPosition(new EpochOffset(partition.lastMirrorEpoch(),
                                                            partition.lastMirrorOffset()))
                                                    .withErrorMessage(partition.errorMessage())
                                                    .withRetryAttempt(partition.retryAttempt())
                                                    .withPrevState(MirrorPartitionState.fromValue(partition.previousState()))
                                                    .build(), false);
                                }));

                            synchronized (merged) {
                                merged.topics().addAll(readMirrorStatesResponse.data().topics());
                                if (readMirrorStatesResponse.data().errorCode() != Errors.NONE.code()) {
                                    log.error("Failed to read states from remote coordinator: {}-{}",
                                            readMirrorStatesResponse.data().errorCode(), readMirrorStatesResponse.data().errorMessage());
                                    merged.setErrorCode(readMirrorStatesResponse.data().errorCode());
                                    merged.setErrorMessage(readMirrorStatesResponse.data().errorMessage());
                                }
                            }
                        } else {
                            log.warn("Unexpected response type from coordinator {}: {}. disconnected={}, timedOut={}, versionMismatch={}, authException={}",
                                    node, response.responseBody(), response.wasDisconnected(), response.wasTimedOut(),
                                    response.versionMismatch(), response.authenticationException());
                            future.completeExceptionally(new RuntimeException(
                                    "Read states from remote coordinator " + node + " failed:"
                                    + " disconnected=" + response.wasDisconnected()
                                    + " timedOut=" + response.wasTimedOut()));
                            return;
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
    CompletableFuture<ReadMirrorOffsetsResponse> readOffsetsFromRemoteLeaders(
            String mirrorName, Map<String, Set<Integer>> partitions) {
        log.debug("Reading offsets from remote leaders: {} {}", mirrorName, partitions);

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
        ReadMirrorOffsetsResponseData merged = new ReadMirrorOffsetsResponseData();
        AtomicInteger remaining = new AtomicInteger(nodeToTopicPartitions.size());

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
                            log.debug("Read offsets from remote leader node {} completed. {}", node, response.responseBody());

                            synchronized (merged) {
                                merged.topics().addAll(readOffsetsResponse.data().topics());
                                if (readOffsetsResponse.data().errorCode() != Errors.NONE.code()) {
                                    log.error("Failed to read offsets from remote leader node {}. {}: {}",
                                            node, readOffsetsResponse.data().errorCode(), readOffsetsResponse.data().errorMessage());
                                    merged.setErrorCode(readOffsetsResponse.data().errorCode());
                                    merged.setErrorMessage(readOffsetsResponse.data().errorMessage());
                                }
                            }
                        } else {
                            log.warn("Unexpected response type from node {}: {}. Disconnected={}, timedOut={}, versionMismatch={}, authException={}",
                                    node, response.responseBody(), response.wasDisconnected(), response.wasTimedOut(),
                                    response.versionMismatch(), response.authenticationException());
                            resultFuture.completeExceptionally(new RuntimeException(
                                    "Read offsets from remote leader " + node + " failed:"
                                    + " disconnected=" + response.wasDisconnected()
                                    + " timedOut=" + response.wasTimedOut()));
                            return;
                        }

                        if (remaining.decrementAndGet() == 0) {
                            resultFuture.complete(new ReadMirrorOffsetsResponse(merged));
                        }
                    }
            ));
        });

        return resultFuture;
    }

    // ===== WRITE OPERATIONS =========================================================================================

    /** Writes mirror partition states to local coordinator, batching all writes. */
    CompletableFuture<WriteMirrorStatesResponseData> writeStateToLocalCoordinator(
            String mirrorName, Map<String, Set<MirrorStateWrite>> stateWrites) {
        log.debug("Writing states to local coordinator for mirror {}", mirrorName);

        if (coordinatorWriter.isEmpty()) {
            return CompletableFuture.completedFuture(new WriteMirrorStatesResponseData());
        }

        return coordinatorWriter.get().writePartitionStates(mirrorName, stateWrites);
    }

    /** Writes mirror partition states to remote coordinators, batching requests per coordinator node. */
    CompletableFuture<WriteMirrorStatesResponse> writeStateToRemoteCoordinator(
            String mirrorName, Map<String, Set<MirrorStateWrite>> topicMetadata) {
        log.debug("Writing states to remote coordinators for mirror {}. Topic metadata: {}.",
                mirrorName, topicMetadata);

        CompletableFuture<WriteMirrorStatesResponse> resultFuture = new CompletableFuture<>();

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
                partitionData.setRetryAttempt(m.retryAttempt());

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

        WriteMirrorStatesResponseData merged = new WriteMirrorStatesResponseData();
        AtomicInteger remaining = new AtomicInteger(nodeToTopicPartitions.size());

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
                        synchronized (merged) {
                            merged.topics().addAll(writeMirrorStatesResponse.data().topics());
                            if (writeMirrorStatesResponse.data().errorCode() != Errors.NONE.code()) {
                                log.error("Failed to write states to remote coordinator: {}-{}",
                                        writeMirrorStatesResponse.data().errorCode(), writeMirrorStatesResponse.data().errorMessage());
                                merged.setErrorCode(writeMirrorStatesResponse.data().errorCode());
                                merged.setErrorMessage(writeMirrorStatesResponse.data().errorMessage());
                            }
                        }
                    } else {
                        log.warn("Unexpected response type from coordinator {}: {}. disconnected={}, timedOut={}, versionMismatch={}, authException={}",
                                node, response.responseBody(), response.wasDisconnected(), response.wasTimedOut(),
                                response.versionMismatch(), response.authenticationException());
                        resultFuture.completeExceptionally(new RuntimeException(
                                "Write states to remote coordinator " + node + " failed:"
                                        + " disconnected=" + response.wasDisconnected()
                                        + " timedOut=" + response.wasTimedOut()));
                    }

                    if (remaining.decrementAndGet() == 0) {
                        resultFuture.complete(new WriteMirrorStatesResponse(merged));
                    }
                }
            ));
        });

        return resultFuture;
    }

    CompletableFuture<Void> writeLastMirrorPositions(String mirrorName, Map<TopicPartition, EpochOffset> positions) {
        if (coordinatorWriter.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return coordinatorWriter.get().writeLastMirrorPositions(mirrorName, positions);
    }

    CompletableFuture<Void> writeMirrorTombstones(String mirrorName, Set<TopicPartition> partitions) {
        if (coordinatorWriter.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return coordinatorWriter.get().writeMirrorTombstones(mirrorName, partitions);
    }

    /**
     * A queue-based sender that handles asynchronous inter-broker
     * RPC requests not supported by the Admin API.
     */
    private static class MirrorInterBrokerSender extends InterBrokerSendThread {
        private final ConcurrentLinkedQueue<RequestAndCompletionHandler> queue = new ConcurrentLinkedQueue<>();

        MirrorInterBrokerSender(String name, KafkaClient networkClient, int requestTimeoutMs, Time time) {
            super(name, networkClient, requestTimeoutMs, time);
        }

        public void enqueue(RequestAndCompletionHandler requestAndCompletionHandler) {
            queue.offer(requestAndCompletionHandler);
            wakeup();
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            List<RequestAndCompletionHandler> requests = new ArrayList<>();
            RequestAndCompletionHandler request;
            while ((request = queue.poll()) != null) {
                requests.add(request);
            }
            return requests;
        }
    }
}
