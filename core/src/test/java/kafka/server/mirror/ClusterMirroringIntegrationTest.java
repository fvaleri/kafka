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

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateClusterMirrorOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteClusterMirrorOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsResult;
import org.apache.kafka.clients.admin.ListClusterMirrorsOptions;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RecoverMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StopMirrorTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.EpochOffset;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidMirrorStateException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig;
import org.apache.kafka.coordinator.mirror.MirrorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.mirror.MirrorPartitionState;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.mirror.MirrorPartition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.kafka.common.config.TopicConfig.MIRROR_SUPPORT_UNCLEAN_LEADER_ELECTION_CONFIG;
import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.server.config.ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.mirror.MirrorPartitionState.FAILED;
import static org.apache.kafka.server.mirror.MirrorPartitionState.MIRRORING;
import static org.apache.kafka.server.mirror.MirrorPartitionState.STOPPED;
import static org.apache.kafka.server.mirror.MirrorPartitionState.ULE_RECOVERY;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Cluster Mirroring feature.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
public class ClusterMirroringIntegrationTest {
    private static final long METADATA_REFRESH_INTERVAL_MS = 5_000;
    private static final int MAX_RETRY_ATTEMPTS = 5;

    private KafkaClusterTestKit srcCluster;
    private KafkaClusterTestKit dstCluster;

    private String srcBootstrapServer;
    private String dstBootstrapServer;

    private Admin srcAdmin;
    private Admin dstAdmin;

    @BeforeEach
    void beforeEach() throws Exception {
        srcCluster = buildCluster(2, Map.of());
        srcBootstrapServer = srcCluster.bootstrapServers().split(",")[0];

        dstCluster = buildCluster(2, Map.of());
        dstBootstrapServer = dstCluster.bootstrapServers().split(",")[0];

        srcAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers()
        ));
        dstAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers()
        ));
    }

    @AfterEach
    void afterEach() {
        closeQuietly(srcAdmin);
        closeQuietly(dstAdmin);
        closeQuietly(srcCluster);
        closeQuietly(dstCluster);
    }

    @Test
    void testFailoverFailback() throws Exception {
        String topic = "failback-topic";
        String forwardMirror = "a-to-b";
        String reverseMirror = "b-to-a";

        CreateTopicsResult createTopicsResult =
                srcAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        createTopicsResult.all().get(10, TimeUnit.SECONDS);
        produceRecords(srcCluster, topic, 0, 10);

        // Forward: src -> dst
        createAndStartMirror(dstAdmin, forwardMirror, srcCluster.bootstrapServers(), topic);

        // Failover: stop forward mirror, produce on dst
        dstAdmin.stopMirrorTopics(forwardMirror, List.of(topic), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, forwardMirror, STOPPED, topic);
        produceRecords(dstCluster, topic, 10, 5);

        // LME lookup via Topics filter and clusterId
        String srcClusterId = srcCluster.controllers().values().stream().findFirst().get().clusterId();
        Map<String, List<Integer>> topicPartitions = Map.of(topic, List.of(0));
        DescribeClusterMirrorsResult describeClusterMirrors = dstAdmin.describeClusterMirrors(
                null, topicPartitions,
                new DescribeClusterMirrorsOptions().clusterId(srcClusterId).includeMirrorState(true));
        Map<TopicPartition, EpochOffset> lastMirrorPositions =
                describeClusterMirrors.lastMirrorPositions().get(10, TimeUnit.SECONDS);
        TopicPartition tp0 = new TopicPartition(topic, 0);
        assertEquals(1, lastMirrorPositions.size(), "Should have one lookup result");
        assertTrue(lastMirrorPositions.containsKey(tp0), "Should have partition 0");
        assertTrue(lastMirrorPositions.get(tp0).epoch() >= 0, "Should have LME >= 0");

        // Failback: src mirrors from dst under a different name
        createAndStartMirror(srcAdmin, reverseMirror, dstCluster.bootstrapServers(), topic);

        // All 15 records should be consumable on src (10 original + 5 from dst)
        consumeRecords(srcCluster, topic, 15);
    }

    @Test
    void testStoppedMirrorDoesNotOverwriteOffsets() throws Exception {
        String topic = "drain-test";
        String groupId = "drain-group";
        int recordCount = 50;

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, recordCount);

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topic);

        // Stop the mirror topic
        dstAdmin.stopMirrorTopics("my-mirror", List.of(topic), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, topic);

        // Consume all records from the destination with a stable consumer group
        consumeRecords(dstCluster, topic, recordCount, groupId);

        // Poll across metadata refresh cycles to verify the offset stays stable
        // Log end is recordCount + 1 due to PID-reset control record written on mirror stop
        TopicPartition tp = new TopicPartition(topic, 0);
        assertStableOffset(dstAdmin, groupId, tp, recordCount + 1);
    }

    @Test
    void testMirrorWithPreCreatedTopic() throws Exception {
        // Create topic on source
        srcAdmin.createTopics(List.of(
                new NewTopic("my-topic", 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Get source topic description (TopicId)
        var topicDesc = describeTopics(srcAdmin, List.of("my-topic"));
        String sourceTopicId = topicDesc.get("my-topic").topicId().toString();

        // Produce data to source
        produceRecords(srcCluster, "my-topic", 0, 50);

        // Pre-create topic on destination with source's TopicId (like ClusterMirrorCommand does)
        dstAdmin.createTopics(List.of(
                new NewTopic("my-topic", Optional.of(1), Optional.empty(), Optional.of(sourceTopicId))
        )).all().get(10, TimeUnit.SECONDS);

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, "my-topic");

        // Verify all records were replicated to destination
        consumeRecords(dstCluster, "my-topic", 50);
    }

    @Test
    void testTopicsIncludeAndExclude() throws Exception {
        String ordersUsTopic = "orders-us";
        String ordersEuTopic = "orders-eu";
        String eventsTopic = "events-click";

        // Create source topics and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(ordersUsTopic, 1, (short) 1),
                new NewTopic(ordersEuTopic, 1, (short) 1),
                new NewTopic(eventsTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, ordersUsTopic, 0, 30);
        produceRecords(srcCluster, ordersEuTopic, 0, 30);
        produceRecords(srcCluster, eventsTopic, 0, 20);

        // Create mirror with topics.include=orders-.*
        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, "my-mirror", ".*");

        // Auto-discovery should find orders-us and orders-eu and start replication
        consumeRecords(dstCluster, ordersUsTopic, 30);
        consumeRecords(dstCluster, ordersEuTopic, 30);

        // Append events-.* to topics.include via incrementalAlterConfigs
        appendToTopicsInclude("my-mirror", "events-.*");

        // Auto-discovery should now find events-click and start replication
        waitForMirrorLagZero(dstAdmin, "my-mirror", eventsTopic);
        consumeRecords(dstCluster, eventsTopic, 20);

        // Update topics.exclude to exclude orders-eu via incrementalAlterConfigs
        alterMirrorConfig("my-mirror", ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, "orders-eu");

        // orders-eu should be stopped by enforceExcludePatterns
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, ordersEuTopic);

        // Produce additional records to orders-us and orders-eu on source
        produceRecords(srcCluster, ordersUsTopic, 30, 20);
        produceRecords(srcCluster, ordersEuTopic, 30, 20);

        // orders-us should continue replicating
        consumeRecords(dstCluster, ordersUsTopic, 50);

        // orders-eu should not receive new records (stays at 30)
        assertStableRecordCount(dstCluster, ordersEuTopic, 30,
                "Excluded topic should not receive new mirror records");
    }

    @Test
    void testTopicsExcludeInternalTopics() throws Exception {
        String userTopic = "user-events";
        String internalTopic = "__test-internal";

        // Create source topics and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(userTopic, 1, (short) 1),
                new NewTopic(internalTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, userTopic, 0, 25);
        produceRecords(srcCluster, internalTopic, 0, 10);

        // Create mirror with include=.* and no explicit exclude (default __.* applies)
        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, ".*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, "my-mirror", userTopic);

        // user-events should be discovered and replicated
        consumeRecords(dstCluster, userTopic, 25);

        // __test-internal should NOT be mirrored due to default exclude
        assertStableRecordCount(dstCluster, internalTopic, 0,
                "Internal topic starting with __ should not be mirrored by default");
    }

    @Test
    void testTopicsIncludeLiteral() throws Exception {
        String topic = "payments";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 40);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "payments"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, "my-mirror", topic);

        consumeRecords(dstCluster, topic, 40);
    }

    @Test
    void testTopicsIncludeLiteralAndRegex() throws Exception {
        String literalTopic = "payments";
        String regexMatchedTopic = "orders-eu";
        String unmatchedTopic = "logs-app";

        srcAdmin.createTopics(List.of(
                new NewTopic(literalTopic, 1, (short) 1),
                new NewTopic(regexMatchedTopic, 1, (short) 1),
                new NewTopic(unmatchedTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, literalTopic, 0, 20);
        produceRecords(srcCluster, regexMatchedTopic, 0, 15);
        produceRecords(srcCluster, unmatchedTopic, 0, 10);

        // Include literal "payments" and regex "orders-.*"
        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "payments,orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, "my-mirror", literalTopic, regexMatchedTopic);

        consumeRecords(dstCluster, literalTopic, 20);
        consumeRecords(dstCluster, regexMatchedTopic, 15);

        // logs-app should NOT be mirrored
        assertStableRecordCount(dstCluster, unmatchedTopic, 0,
                "Unmatched topic should not be mirrored");
    }

    @Test
    void testStopTopicPreventsRediscovery() throws Exception {
        String topicA = "orders-us";
        String topicB = "orders-eu";

        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topicA, 0, 20);
        produceRecords(srcCluster, topicB, 0, 20);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, "my-mirror", topicA, topicB);

        consumeRecords(dstCluster, topicA, 20);
        consumeRecords(dstCluster, topicB, 20);

        // Stop orders-eu to prevent auto-discovery from restarting it
        dstAdmin.stopMirrorTopics("my-mirror", List.of(topicB), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, topicB);

        // Produce more data to both topics (only orders-us should receive new records)
        produceRecords(srcCluster, topicA, 20, 20);
        produceRecords(srcCluster, topicB, 20, 20);

        consumeRecords(dstCluster, topicA, 40);

        // Verify orders-eu did NOT receive new data (still at 20, not 40)
        assertStableRecordCount(dstCluster, topicB, 20,
                "Stopped topic should not have received new mirror records");
    }

    @Test
    void testUleRecoveryProcess() throws Exception {
        String topic = "test-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 2))).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 30);

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topic);

        var topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        int dstLeader = dstCluster.brokers().get(0).metadataCache()
                .getLeaderAndIsr(topic, 0).get().leader();

        // Shut down the follower in the destination cluster
        dstCluster.brokers().values().forEach(broker -> {
            if (broker.config().nodeId() != dstLeader) {
                broker.shutdown();
            }
        });

        int srcLeader = srcCluster.brokers().get(0).metadataCache()
                .getLeaderAndIsr(topic, 0).get().leader();

        // Simulate unclean leader election with log truncation on source
        srcCluster.brokers().get(srcLeader).replicaManager()
                .getLog(new TopicPartition(topic, 0)).get().truncateTo(20);

        // Verify it stays in MIRRORING because mirror.support.unclean.leader.election is disabled
        waitForMirrorLagZero(dstAdmin, "my-mirror", topic);

        // Enable mirror.support.unclean.leader.election
        dstAdmin.incrementalAlterConfigs(Map.of(topicResource, List.of(
                new AlterConfigOp(
                        new ConfigEntry(MIRROR_SUPPORT_UNCLEAN_LEADER_ELECTION_CONFIG, "true"),
                        AlterConfigOp.OpType.SET)))).all().get();

        // Simulate another unclean leader election with log truncation on source
        srcCluster.brokers().get(srcLeader).replicaManager()
                .getLog(new TopicPartition(topic, 0)).get().truncateTo(10);

        // Partition should enter ULE_RECOVERY and stay there because one replica is not caught up
        waitForMirrorState(dstAdmin, "my-mirror", ULE_RECOVERY, topic);

        // Start the follower in the destination cluster so all replicas rejoin ISR
        dstCluster.brokers().values().forEach(broker -> {
            if (broker.config().nodeId() != dstLeader) {
                broker.startup();
            }
        });

        // Verify it returns to MIRRORING state with zero lag
        waitForMirrorLagZero(dstAdmin, "my-mirror", topic);
    }

    @Test
    void testDeleteClusterMirror() throws Exception {
        String topic = "delete-mirror-topic";

        var result = srcAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        result.all().get(10, TimeUnit.SECONDS);

        Uuid topicId = result.topicId(topic).get();

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topic);

        var listConfigResult = dstAdmin.listConfigResources(Set.of(ConfigResource.Type.CLUSTER_MIRROR),
                new ListConfigResourcesOptions()).all().get(10, TimeUnit.SECONDS);
        assertEquals(1, listConfigResult.size());

        // Verify mirror is listed before deletion
        var listingsBefore = dstAdmin.listClusterMirrors().all().get(10, TimeUnit.SECONDS);
        assertTrue(listingsBefore.stream().anyMatch(l -> "my-mirror".equals(l.mirrorName())),
                "Mirror should be listed before deletion");

        // Stop all topics (required precondition for deletion)
        dstAdmin.stopMirrorTopics("my-mirror", List.of(topic), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, topic);

        dstAdmin.deleteClusterMirror("my-mirror", new DeleteClusterMirrorOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForListMirrorEmpty(15_000);

        // Verify the mirror is no longer listed after deletion
        listConfigResult = dstAdmin.listConfigResources(Set.of(ConfigResource.Type.CLUSTER_MIRROR),
                new ListConfigResourcesOptions()).all().get(10, TimeUnit.SECONDS);
        assertTrue(listConfigResult.isEmpty());

        // Verify that the __mirror_state topic contains the tombstone records for both
        // MirrorPartitionStateKey and LastMirrorEpochsKey types
        // 1. Get the partition index hosting the metadata for the mirror topic partition
        int partId = dstCluster.brokers().get(0).clusterMirrorCoordinator()
                .partitionFor(new MirrorPartition("my-mirror", topicId, 0));
        // 2. Get the partition leader
        int leaderMirrorStatePartition = dstCluster.brokers().get(0).metadataCache()
                .getLeaderAndIsr(MIRROR_STATE_TOPIC_NAME, partId).get().leader();
        // 3. Poll until the last batch contains the expected tombstone records.
        //    Tombstones are written asynchronously by the coordinator runtime.
        TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, partId);
        MirrorRecordSerde serde = new MirrorRecordSerde();
        waitForCondition(() -> {
            var batch = dstCluster.brokers().get(leaderMirrorStatePartition).replicaManager()
                    .getLog(mirrorStateTp).get().activeSegment().log().lastBatch().get();
            List<CoordinatorRecord> recs = new ArrayList<>();
            batch.forEach(r -> recs.add(serde.deserialize(r.key(), r.value())));
            return recs.size() == 2
                    && recs.get(0).key().apiKey() == new MirrorPartitionStateKey().apiKey() && recs.get(0).value() == null
                    && recs.get(1).key().apiKey() == new LastMirrorEpochsKey().apiKey() && recs.get(1).value() == null;
        }, DEFAULT_MAX_WAIT_MS, "Tombstone records not found in last batch of " + mirrorStateTp);
    }

    @Test
    void testMirrorLoopDetection() throws Exception {
        // Create topic on source
        srcAdmin.createTopics(List.of(
                new NewTopic("my-topic", 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Create forward mirror (src -> dst) and start mirroring
        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, "my-topic");

        // Create reverse mirror (src <- dst) to trigger loop detection
        srcAdmin.createClusterMirror("new-mirror", Map.of(
                "bootstrap.servers", dstBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        srcAdmin.startMirrorTopics("new-mirror", List.of("my-topic"), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);

        // Verify the forward mirror (src -> dst) is still MIRRORING
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, "my-topic");
        // Verify the reverse mirror (src <- dst) failed due to loop detection
        waitForMirrorState(srcAdmin, "new-mirror", FAILED,
                Optional.of("Detected mirror loop for mirror"), 30_000, "my-topic");
    }

    @Test
    void testDeletedSourceTopicMovesToNonRetryable() throws Exception {
        String topic = "non-retryable-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 20);

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topic);

        // Delete source topic to trigger non-retryable failure
        srcAdmin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);

        waitForFailedState("non-retryable failed", -1, topic);

        // Verify partitions stay in FAILED across multiple refresh cycles
        long deadline = System.currentTimeMillis() + 3 * METADATA_REFRESH_INTERVAL_MS;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(allPartitionsSatisfy(dstAdmin, "my-mirror",
                            s -> FAILED.name().equals(s.state()) && s.retryAttempt() == -1, topic),
                    "Non-retryable partitions must not be restarted by metadata refresh");
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }

    @Test
    void testAutoRecovery() throws Exception {
        String topic = "auto-recovery-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 20);

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topic);

        // Shut down source to trigger FAILED state
        srcCluster.brokers().values().forEach(KafkaBroker::shutdown);
        waitForMirrorState(dstAdmin, "my-mirror", FAILED, topic);

        // Restart source so scheduled retry can automatically recover
        srcCluster.brokers().values().forEach(b -> {
            try {
                b.startup();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        waitForMirrorLagZero(dstAdmin, "my-mirror", topic);

        // Verify data still flows after automatic recovery
        produceRecords(srcCluster, topic, 20, 20);
        waitForMirrorLagZero(dstAdmin, "my-mirror", topic);
        consumeRecords(dstCluster, topic, 40);
    }

    @Test
    void testManualRecovery() throws Exception {
        String topic = "manual-recovery-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 20);

        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topic);

        // Shut down source to trigger FAILED state
        srcCluster.brokers().values().forEach(KafkaBroker::shutdown);
        waitForFailedState("retries exhausted", MAX_RETRY_ATTEMPTS, topic);

        // Restart source so recovery can succeed
        srcCluster.brokers().values().forEach(b -> {
            try {
                b.startup();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Recover the failed partitions
        dstAdmin.recoverMirrorTopics("my-mirror", List.of(topic), new RecoverMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);

        waitForMirrorLagZero(dstAdmin, "my-mirror", topic);
    }

    @Test
    void testCreatePartAllowedOnNonMirrorTopic() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";

        // Source topic that will be actively mirrored to the destination
        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // A regular destination topic that is not part of any mirror
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics("my-mirror", List.of(mirrorTopic), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, mirrorTopic);

        // With Cluster Mirroring feature enabled the broker intercepts CreatePartitions, but a
        // topic that belongs to no mirror must be forwarded and created without any mirror check.
        dstAdmin.createPartitions(Map.of(nonMirrorTopic, NewPartitions.increaseTo(2)))
                .all().get(10, TimeUnit.SECONDS);
        waitForCondition(() -> describeTopics(dstAdmin, List.of(nonMirrorTopic)).get(nonMirrorTopic).partitions().size() == 2,
                "Non mirror topic count not increased");
    }

    @Test
    void testCreatePartWithMixedMirrorAndNonMirrorTopics() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics("my-mirror", List.of(mirrorTopic), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, mirrorTopic);

        CreatePartitionsResult result = dstAdmin.createPartitions(Map.of(
                mirrorTopic, NewPartitions.increaseTo(2),
                nonMirrorTopic, NewPartitions.increaseTo(2)
        ));

        result.values().get(nonMirrorTopic).get(10, TimeUnit.SECONDS);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> result.values().get(mirrorTopic).get(10, TimeUnit.SECONDS));
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());

        waitForCondition(() -> {
            Map<String, TopicDescription> descriptionMap = describeTopics(dstAdmin, List.of(mirrorTopic, nonMirrorTopic));
            return descriptionMap.get(nonMirrorTopic).partitions().size() == 2
                    && descriptionMap.get(mirrorTopic).partitions().size() == 1;
        }, "Non mirror topic count not increased");
    }

    @Test
    void testCreatePartitionsWithMultipleTopics() throws Exception {
        String topicA = "topic-a";
        String topicB = "topic-b";

        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics("my-mirror", List.of(topicA, topicB), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, topicA, topicB);

        // Both mirror topics in a single request are rejected while mirroring
        CreatePartitionsResult result = dstAdmin.createPartitions(Map.of(
                topicA, NewPartitions.increaseTo(2),
                topicB, NewPartitions.increaseTo(2)
        ));
        for (String topic : List.of(topicA, topicB)) {
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> result.values().get(topic).get(10, TimeUnit.SECONDS));
            assertEquals(InvalidMirrorStateException.class, e.getCause().getClass(), "for topic " + topic);
        }

        // After stopping both, the same request succeeds
        dstAdmin.stopMirrorTopics("my-mirror", List.of(topicA, topicB), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, topicA, topicB);

        dstAdmin.createPartitions(Map.of(
                topicA, NewPartitions.increaseTo(2),
                topicB, NewPartitions.increaseTo(2)
        )).all().get(10, TimeUnit.SECONDS);
        waitForCondition(() -> {
            Map<String, TopicDescription> descriptionMap = describeTopics(dstAdmin, List.of(topicA, topicB));
            return descriptionMap.get(topicA).partitions().size() == 2 && descriptionMap.get(topicB).partitions().size() == 2;
        }, "Partition count not increased after stopping");
    }

    @Test
    void testDeleteWithMirrorAndNonMirrorTopics() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics("my-mirror", List.of(mirrorTopic), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, mirrorTopic);

        DeleteTopicsResult result = dstAdmin.deleteTopics(Set.of(nonMirrorTopic, mirrorTopic));

        result.topicNameValues().get(nonMirrorTopic).get(10, TimeUnit.SECONDS);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> result.topicNameValues().get(mirrorTopic).get(10, TimeUnit.SECONDS));
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());
    }

    @Test
    void testDeleteTopicsDisallowedOnNonStoppedTopic() throws Exception {
        // Create topic and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic("my-topic", 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Create mirror and start topic
        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics("my-mirror", List.of("my-topic"), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, "my-topic");

        // Attempt to delete topic
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> dstAdmin.deleteTopics(Set.of("my-topic")).all().get(10, TimeUnit.SECONDS));
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());

        // Stop mirroring and retry
        dstAdmin.stopMirrorTopics("my-mirror", List.of("my-topic"),
                new StopMirrorTopicsOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, "my-topic");

        dstAdmin.deleteTopics(Set.of("my-topic")).all().get();
    }

    @Test
    void testDeleteRecordsDisallowedOnNonStoppedTopic() throws Exception {
        // Create topic and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic("my-topic", 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        produceRecords(srcCluster, "my-topic", 0, 10);

        // Create mirror and start topic
        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, "my-topic");

        // Attempt to delete records while the topic is actively mirroring
        TopicPartition tp = new TopicPartition("my-topic", 0);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> dstAdmin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5))).all().get());
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());

        // Stop mirroring and retry
        dstAdmin.stopMirrorTopics("my-mirror", List.of("my-topic"),
                new StopMirrorTopicsOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", STOPPED, "my-topic");

        waitForCondition(() -> {
            DeleteRecordsResult result = dstAdmin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5)));
            return result.lowWatermarks().get(tp).get(10, TimeUnit.SECONDS).lowWatermark() == 5;
        }, "Records not deleted");
    }

    @Test
    void testDeleteRecordsAllowedOnNonMirrorTopic() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";

        // Source topic that will be actively mirrored to the destination
        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // A regular destination topic that is not part of any mirror
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        produceRecords(dstCluster, nonMirrorTopic, 0, 10);

        dstAdmin.createClusterMirror("my-mirror", Map.of(
                "bootstrap.servers", srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics("my-mirror", List.of(mirrorTopic), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "my-mirror", MIRRORING, mirrorTopic);

        // With Cluster Mirroring feature enabled the broker intercepts DeleteRecords, but a
        // topic that belongs to no mirror must be handled without any mirror check.
        TopicPartition tp = new TopicPartition(nonMirrorTopic, 0);
        DeleteRecordsResult result = dstAdmin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5)));
        assertEquals(5, result.lowWatermarks().get(tp).get(10, TimeUnit.SECONDS).lowWatermark());
    }

    @Test
    void testTieredStoragePartitionFails() throws Exception {
        String topic = "tiered-topic";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        KafkaClusterTestKit tieredDst = buildCluster(1, Map.of(
                ClusterMirrorConfig.MIRROR_NUM_REPLICA_FETCHERS_CONFIG, "1",
                ClusterMirrorConfig.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1",
                GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1",
                DEFAULT_REPLICATION_FACTOR_CONFIG, "1",
                ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, "3",
                ClusterMirrorConfig.MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG, "1000",
                RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true",
                RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                NoOpRemoteStorageManager.class.getName(),
                RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                NoOpRemoteLogMetadataManager.class.getName()
        ));

        try {
            try (Admin tieredAdmin = Admin.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, tieredDst.bootstrapServers()))) {

                tieredAdmin.createClusterMirror("my-mirror", Map.of(
                        "bootstrap.servers", srcBootstrapServer
                ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
                tieredAdmin.startMirrorTopics("my-mirror",
                                List.of(topic), new StartMirrorTopicsOptions())
                        .all().get(10, TimeUnit.SECONDS);
                waitForMirrorState(tieredAdmin, "my-mirror", MIRRORING, topic);

                // Enable tiered storage while mirroring is active
                ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                tieredAdmin.incrementalAlterConfigs(Map.of(topicResource, List.of(
                        new AlterConfigOp(
                                new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                                AlterConfigOp.OpType.SET)
                ))).all().get(10, TimeUnit.SECONDS);

                waitForMirrorState(tieredAdmin, "my-mirror", FAILED,
                        Optional.of("tiered storage"), 30_000, topic);
            }
        } finally {
            closeQuietly(tieredDst);
        }
    }

    @Test
    void testListClusterMirrorsFilters() throws Exception {
        String topicA = "list-filter-a";
        String topicB = "list-filter-b";
        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Mirror 1: topicA in MIRRORING state
        createAndStartMirror(dstAdmin, "my-mirror", srcBootstrapServer, topicA);

        // Mirror 2: topicB started then stopped
        createAndStartMirror(dstAdmin, "new-mirror", srcBootstrapServer, topicB);
        dstAdmin.stopMirrorTopics("new-mirror", List.of(topicB), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, "new-mirror", STOPPED, topicB);

        // No filter: both mirrors returned, default includes MIRRORING and PAUSED
        var all = listMirrors(new ListClusterMirrorsOptions());
        assertEquals(2, all.size());

        // Get source cluster ID for later assertions
        String sourceClusterId = all.iterator().next().sourceClusterId();

        // Filter by mirror name
        var byName = listMirrors(new ListClusterMirrorsOptions()
                .mirrorNameFilter(List.of("my-mirror")));
        assertEquals(1, byName.size());
        assertEquals("my-mirror", byName.iterator().next().mirrorName());

        var byNameMiss = listMirrors(new ListClusterMirrorsOptions()
                .mirrorNameFilter(List.of("nonexistent")));
        assertTrue(byNameMiss.isEmpty());

        // Filter by source cluster ID
        var bySource = listMirrors(new ListClusterMirrorsOptions()
                .sourceClusterIdFilter(List.of(sourceClusterId)));
        assertEquals(2, bySource.size());

        var bySourceMiss = listMirrors(new ListClusterMirrorsOptions()
                .sourceClusterIdFilter(List.of("unknown-cluster-id")));
        assertTrue(bySourceMiss.isEmpty());

        // Desired state filter: MIRRORING returns topicA on mirror 1, empty on mirror 2
        var byMirroring = listMirrors(new ListClusterMirrorsOptions()
                .desiredStateFilter(List.of(MIRRORING.name())));
        assertEquals(2, byMirroring.size());
        var mirroringByName = byMirroring.stream()
                .collect(java.util.stream.Collectors.toMap(ClusterMirrorListing::mirrorName, l -> l));
        assertEquals(List.of(topicA), mirroringByName.get("my-mirror").topicNames());
        assertTrue(mirroringByName.get("new-mirror").topicNames().isEmpty());

        // Desired state filter: STOPPED returns topicB on mirror 2, empty on mirror 1
        var byStopped = listMirrors(new ListClusterMirrorsOptions()
                .desiredStateFilter(List.of(STOPPED.name())));
        assertEquals(2, byStopped.size());
        var stoppedByName = byStopped.stream()
                .collect(java.util.stream.Collectors.toMap(ClusterMirrorListing::mirrorName, l -> l));
        assertTrue(stoppedByName.get("my-mirror").topicNames().isEmpty());
        assertEquals(List.of(topicB), stoppedByName.get("new-mirror").topicNames());
    }

    private KafkaClusterTestKit buildCluster(int numBrokers,
                                             Map<String, String> configOverrides) throws Exception {
        var builder = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(numBrokers)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ClusterMirrorConfig.MIRROR_NUM_REPLICA_FETCHERS_CONFIG, "2")
                .setConfigProp(ClusterMirrorConfig.MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG,
                        String.valueOf(METADATA_REFRESH_INTERVAL_MS))
                .setConfigProp(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false")
                .setConfigProp(ClusterMirrorConfig.MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG, "3")
                .setConfigProp(ClusterMirrorConfig.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "2")
                .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "2")
                .setConfigProp(DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
                .setConfigProp(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
                .setConfigProp(ClusterMirrorConfig.SOCKET_TIMEOUT_MS_CONFIG, "5000")
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, MAX_RETRY_ATTEMPTS)
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG, "100")
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG, "5000")
                .setConfigProp(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
        configOverrides.forEach(builder::setConfigProp);
        var cluster = builder.build();
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        } catch (Exception e) {
            closeQuietly(cluster);
            throw e;
        }
        return cluster;
    }

    private void createAndStartMirror(Admin admin, String mirrorName,
                                      String bootstrapServer, String... topics) throws Exception {
        admin.createClusterMirror(mirrorName, Map.of(
                "bootstrap.servers", bootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        admin.startMirrorTopics(mirrorName, List.of(topics), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(admin, mirrorName, topics);
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    private void produceRecords(KafkaClusterTestKit cluster, String topic,
                                int startIndex, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = startIndex; i < startIndex + count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }

    private List<ConsumerRecord<String, String>> consumeRecords(
            KafkaClusterTestKit cluster, String topic, int expectedRecords) throws Exception {
        return consumeRecords(cluster, topic, expectedRecords, null);
    }

    private List<ConsumerRecord<String, String>> consumeRecords(
            KafkaClusterTestKit cluster, String topic, int expectedRecords,
            String groupId) throws Exception {
        boolean commitOffsets = groupId != null;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, commitOffsets ? groupId : "test-" + System.currentTimeMillis());
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            // When expectedRecords == 0, poll briefly to verify no records exist
            long deadline = System.currentTimeMillis() + (expectedRecords > 0 ? 30_000 : 5_000);
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(allRecords::add);
                if (expectedRecords > 0 && allRecords.size() >= expectedRecords) break;
            }
            if (commitOffsets && allRecords.size() >= expectedRecords) {
                consumer.commitSync();
            }
        }
        assertEquals(expectedRecords, allRecords.size(),
                "Expected to consume " + expectedRecords + " records from " + topic);
        return allRecords;
    }

    private Map<String, TopicDescription> describeTopics(
            Admin admin, List<String> topics) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try {
                return admin.describeTopics(topics).allTopicNames().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
    }

    private Collection<ClusterMirrorListing> listMirrors(ListClusterMirrorsOptions options) throws Exception {
        return dstAdmin.listClusterMirrors(options).all().get(10, TimeUnit.SECONDS);
    }

    private void alterMirrorConfig(String mirrorName, String key, String value) throws Exception {
        ConfigResource mirrorResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName);
        dstAdmin.incrementalAlterConfigs(Map.of(mirrorResource, List.of(
                new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)
        ))).all().get(10, TimeUnit.SECONDS);
    }

    private void appendToTopicsInclude(String mirrorName, String pattern) throws Exception {
        ConfigResource mirrorResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName);
        var configResult = dstAdmin.describeConfigs(List.of(mirrorResource)).all().get(10, TimeUnit.SECONDS);
        var mirrorConfigEntries = configResult.get(mirrorResource);

        String existingValue = "";
        ConfigEntry existingEntry = mirrorConfigEntries.get(ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG);
        if (existingEntry != null && existingEntry.value() != null && !existingEntry.value().isEmpty()) {
            existingValue = existingEntry.value();
        }
        String newValue = existingValue.isEmpty() ? pattern : existingValue + "," + pattern;

        alterMirrorConfig(mirrorName, ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, newValue);
    }

    private long getCommittedOffset(Admin admin, String groupId, TopicPartition tp, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                var offsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                OffsetAndMetadata oam = offsets.get(tp);
                if (oam != null) return oam.offset();
            } catch (Exception e) {
                // Group may not be visible yet, retry
            }
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
        throw new AssertionError("No committed offset for " + tp + " in group " + groupId + " within timeout");
    }

    private boolean allPartitionsSatisfy(Admin admin, String mirrorName,
                                         Predicate<ClusterMirrorDescription.LeaderStateDescription> condition,
                                         String... topicPatterns) throws Exception {
        var result = admin.describeClusterMirrors(
                List.of(mirrorName), null,
                new DescribeClusterMirrorsOptions().includeMirrorState(true).includeMirrorOffset(true));
        var descriptions = result.allDescriptions().get(5, TimeUnit.SECONDS);
        ClusterMirrorDescription desc = descriptions.get(mirrorName);
        if (desc == null) return false;
        var patterns = Arrays.stream(topicPatterns)
                .map(java.util.regex.Pattern::compile)
                .toList();
        var matched = desc.leaderStates().entrySet().stream()
                .filter(e -> patterns.stream().anyMatch(p -> p.matcher(e.getKey()).matches()))
                .toList();
        return !matched.isEmpty()
                && matched.stream().allMatch(e -> e.getValue().stream().allMatch(condition));
    }

    private void waitForMirrorState(Admin admin, String mirrorName, MirrorPartitionState state, String... topicPatterns) throws Exception {
        waitForMirrorState(admin, mirrorName, state, Optional.empty(), 30_000, topicPatterns);
    }

    private void waitForMirrorState(Admin admin, String mirrorName, MirrorPartitionState state,
                                    Optional<String> errorMsg, long timeoutMs,
                                    String... topicPatterns) throws Exception {
        waitForCondition(
                () -> allPartitionsSatisfy(admin, mirrorName,
                        s -> state.name().equals(s.state())
                                && (errorMsg.isEmpty()
                                || s.errorMessage() != null && s.errorMessage().contains(errorMsg.get())),
                        topicPatterns),
                timeoutMs,
                "Mirror partitions for " + List.of(topicPatterns) + " did not reach state " + state + " within timeout");
    }

    private void waitForFailedState(String description, int expectedRetryAttempt,
                                    String... topicPatterns) throws Exception {
        waitForFailedState(description, expectedRetryAttempt, 60_000, topicPatterns);
    }

    private void waitForFailedState(String description, int expectedRetryAttempt,
                                    long timeoutMs, String... topicPatterns) throws Exception {
        waitForCondition(
                () -> allPartitionsSatisfy(dstAdmin, "my-mirror",
                        s -> FAILED.name().equals(s.state()) && s.retryAttempt() >= expectedRetryAttempt,
                        topicPatterns),
                timeoutMs,
                "Mirror partitions for " + List.of(topicPatterns) + " did not reach " + description + " state within timeout");
    }

    private void waitForMirrorLagZero(Admin admin, String mirrorName, String... topicPatterns) throws Exception {
        waitForMirrorLagZero(admin, mirrorName, 30_000, topicPatterns);
    }

    private void waitForMirrorLagZero(Admin admin, String mirrorName, long timeoutMs,
                                      String... topicPatterns) throws Exception {
        waitForCondition(
                () -> allPartitionsSatisfy(admin, mirrorName,
                        s -> s.lag() == 0 && MIRRORING.name().equals(s.state()),
                        topicPatterns),
                timeoutMs,
                "Mirror " + mirrorName + " lag did not reach zero for " + List.of(topicPatterns));
    }

    private void waitForListMirrorEmpty(long timeoutMs) throws Exception {
        waitForCondition(
                () -> dstAdmin.listClusterMirrors().all().get(10, TimeUnit.SECONDS).isEmpty(),
                timeoutMs,
                "Cluster mirror is not deleted successfully");
    }

    private void assertStableOffset(Admin admin, String groupId,
                                    TopicPartition tp, long expectedOffset) throws Exception {
        assertStableOffset(admin, groupId, tp, expectedOffset, 3 * METADATA_REFRESH_INTERVAL_MS);
    }

    private void assertStableOffset(Admin admin, String groupId,
                                    TopicPartition tp, long expectedOffset, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long current = getCommittedOffset(admin, groupId, tp, 10_000);
            assertEquals(expectedOffset, current,
                    "Consumer group offset must not be overwritten after mirror stop");
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }

    private void assertStableRecordCount(KafkaClusterTestKit cluster, String topic,
                                         int expectedCount, String message) throws Exception {
        assertStableRecordCount(cluster, topic, expectedCount, message, 3 * METADATA_REFRESH_INTERVAL_MS);
    }

    private void assertStableRecordCount(KafkaClusterTestKit cluster, String topic,
                                         int expectedCount, String message, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var records = consumeRecords(cluster, topic, expectedCount);
            assertEquals(expectedCount, records.size(), message);
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }
}
