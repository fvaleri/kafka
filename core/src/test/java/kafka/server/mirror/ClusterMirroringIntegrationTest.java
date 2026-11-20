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

import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;
import kafka.server.metadata.KRaftMetadataCache;

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
import org.apache.kafka.coordinator.mirror.MirrorCoordinatorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.mirror.MirrorPartition;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.server.mirror.MirrorPartitionMetadata.NON_RETRYABLE_ERROR;
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
 * Integration tests for Cluster Mirroring.
 */
public class ClusterMirroringIntegrationTest {
    private static final long METADATA_REFRESH_INTERVAL_MS = 5_000;

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

        // Forward mirror: A -> B
        dstAdmin.createClusterMirror(forwardMirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers()
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, forwardMirror, topic);

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
        Map<TopicPartition, EpochOffset> lastMirrorEpochs =
                describeClusterMirrors.lastMirrorEpochs().get(10, TimeUnit.SECONDS);
        TopicPartition tp0 = new TopicPartition(topic, 0);
        assertEquals(1, lastMirrorEpochs.size(), "Should have one lookup result");
        assertTrue(lastMirrorEpochs.containsKey(tp0), "Should have partition 0");
        assertTrue(lastMirrorEpochs.get(tp0).epoch() >= 0, "Should have LME >= 0");

        // Reverse mirror: B -> A (failback)
        srcAdmin.createClusterMirror(reverseMirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers()
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(srcAdmin, reverseMirror, topic);

        // All 15 records should be consumable on src (10 original + 5 from dst)
        consumeRecords(srcCluster, topic, 15);
    }

    @Test
    void testStoppedMirrorDoesNotOverwriteOffsets() throws Exception {
        String topic = "drain-test";
        String groupId = "drain-group";
        int numRecords = 50;
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, numRecords);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Stop the mirror topic
        dstAdmin.stopMirrorTopics(mirror, List.of(topic), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, topic);

        // Consume all records from the destination with a stable consumer group
        consumeRecords(dstCluster, topic, numRecords, groupId);

        // Poll across metadata refresh cycles to verify the offset stays stable
        // Log end is numRecords + 1 due to PID-reset control record written on mirror stop
        TopicPartition tp = new TopicPartition(topic, 0);
        assertStableOffset(dstAdmin, groupId, tp, numRecords + 1);
    }

    @Test
    void testMirrorWithPreCreatedTopic() throws Exception {
        String topic = "my-topic";
        String mirror = "my-mirror";

        // Create topic on source
        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Get source topic description (TopicId)
        var topicDesc = describeTopics(srcAdmin, List.of(topic));
        String sourceTopicId = topicDesc.get(topic).topicId().toString();

        // Produce data to source
        produceRecords(srcCluster, topic, 0, 50);

        // Pre-create topic on destination with source's TopicId (like ClusterMirrorCommand does)
        dstAdmin.createTopics(List.of(
                new NewTopic(topic, Optional.of(1), Optional.empty(), Optional.of(sourceTopicId))
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Verify all records were replicated to destination
        consumeRecords(dstCluster, topic, 50);
    }

    @Test
    void testTopicsIncludeAndExclude() throws Exception {
        String ordersUsTopic = "orders-us";
        String ordersEuTopic = "orders-eu";
        String eventsTopic = "events-click";
        String mirror = "my-mirror";

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
        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, ".*");

        // Auto-discovery should find orders-us and orders-eu and start replication
        consumeRecords(dstCluster, ordersUsTopic, 30);
        consumeRecords(dstCluster, ordersEuTopic, 30);

        // Append events-.* to topics.include via incrementalAlterConfigs
        appendToTopicsInclude(mirror, "events-.*");

        // Auto-discovery should now find events-click and start replication
        waitForMirrorLagZero(dstAdmin, mirror, eventsTopic);
        consumeRecords(dstCluster, eventsTopic, 20);

        // Update topics.exclude to exclude orders-eu via incrementalAlterConfigs
        alterMirrorConfig(mirror, ClusterMirrorConfig.TOPICS_EXCLUDE_CONFIG, ordersEuTopic);

        // orders-eu should be stopped by enforceExcludePatterns
        waitForMirrorState(dstAdmin, mirror, STOPPED, ordersEuTopic);

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
        String mirror = "my-mirror";

        // Create source topics and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(userTopic, 1, (short) 1),
                new NewTopic(internalTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, userTopic, 0, 25);
        produceRecords(srcCluster, internalTopic, 0, 10);

        // Create mirror with include=.* and no explicit exclude (default __.* applies)
        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, ".*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, userTopic);

        // user-events should be discovered and replicated
        consumeRecords(dstCluster, userTopic, 25);

        // __test-internal should NOT be mirrored due to default exclude
        assertStableRecordCount(dstCluster, internalTopic, 0,
                "Internal topic starting with __ should not be mirrored by default");
    }

    @Test
    void testTopicsIncludeLiteral() throws Exception {
        String topic = "payments";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 40);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, topic
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        consumeRecords(dstCluster, topic, 40);
    }

    @Test
    void testTopicsIncludeLiteralAndRegex() throws Exception {
        String literalTopic = "payments";
        String regexMatchedTopic = "orders-eu";
        String unmatchedTopic = "logs-app";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(literalTopic, 1, (short) 1),
                new NewTopic(regexMatchedTopic, 1, (short) 1),
                new NewTopic(unmatchedTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, literalTopic, 0, 20);
        produceRecords(srcCluster, regexMatchedTopic, 0, 15);
        produceRecords(srcCluster, unmatchedTopic, 0, 10);

        // Include literal "payments" and regex "orders-.*"
        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "payments,orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, List.of(literalTopic, regexMatchedTopic));

        consumeRecords(dstCluster, literalTopic, 20);
        consumeRecords(dstCluster, regexMatchedTopic, 15);

        // logs-app should NOT be mirrored
        assertStableRecordCount(dstCluster, unmatchedTopic, 0,
                "Unmatched topic should not be mirrored");
    }

    @Test
    void testGroupsIncludeAndExclude() throws Exception {
        String topic = "group-filter-topic";
        String mirror = "my-mirror";
        String includedGroup = "app-group1";
        String excludedGroup = "app-group2";
        String unmatchedGroup = "internal-group";
        int numRecords = 20;

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, numRecords);

        // Consume on source with three different consumer groups to commit offsets
        consumeRecords(srcCluster, topic, numRecords, includedGroup);
        consumeRecords(srcCluster, topic, numRecords, excludedGroup);
        consumeRecords(srcCluster, topic, numRecords, unmatchedGroup);

        // Create mirror with groups.include=app-.* and groups.exclude=app-group2
        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.GROUPS_INCLUDE_CONFIG, "app-.*",
                ClusterMirrorConfig.GROUPS_EXCLUDE_CONFIG, excludedGroup
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Wait for group offset sync (happens on metadata refresh)
        TopicPartition tp = new TopicPartition(topic, 0);
        waitForCondition(
                () -> {
                    try {
                        getCommittedOffset(dstAdmin, includedGroup, tp);
                        return true;
                    } catch (AssertionError e) {
                        return false;
                    }
                },
                3 * METADATA_REFRESH_INTERVAL_MS,
                "Included group offset was not synced to destination");

        // app-group1 should have its offset synced
        long offset = getCommittedOffset(dstAdmin, includedGroup, tp);
        assertEquals(numRecords, offset,
                "Included group should have offset synced");

        // app-group2 (excluded) and internal-group (not matching include) should not be synced
        long deadline = System.currentTimeMillis() + 2 * METADATA_REFRESH_INTERVAL_MS;
        while (System.currentTimeMillis() < deadline) {
            for (String group : List.of(excludedGroup, unmatchedGroup)) {
                var offsets = dstAdmin.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                assertTrue(offsets.isEmpty() || offsets.get(tp) == null,
                        "Group " + group + " should not be synced to destination");
            }
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }

    @Test
    void testStartMirrorTopics() throws Exception {
        String topic = "start-topic";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 30);

        // Create mirror with no topics included
        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, ""
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);

        // No topic should be mirrored yet
        assertStableRecordCount(dstCluster, topic, 0,
                "No topic should be mirrored with empty topics.include");

        // Explicitly start the topic
        dstAdmin.startMirrorTopics(mirror, List.of(topic), new StartMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        consumeRecords(dstCluster, topic, 30);
    }

    @Test
    void testStopTopicPreventsRediscovery() throws Exception {
        String topicA = "orders-us";
        String topicB = "orders-eu";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topicA, 0, 20);
        produceRecords(srcCluster, topicB, 0, 20);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, List.of(topicA, topicB));

        consumeRecords(dstCluster, topicA, 20);
        consumeRecords(dstCluster, topicB, 20);

        // Stop orders-eu to prevent auto-discovery from restarting it
        dstAdmin.stopMirrorTopics(mirror, List.of(topicB), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, topicB);

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
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 2)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 30);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

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

        // Verify it stays in MIRRORING because ULE support is disabled
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Enable ULE support
        dstAdmin.incrementalAlterConfigs(Map.of(topicResource, List.of(
                new AlterConfigOp(
                        new ConfigEntry(TopicConfig.MIRROR_UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET)))).all().get();

        // Simulate another unclean leader election with log truncation on source
        srcCluster.brokers().get(srcLeader).replicaManager()
                .getLog(new TopicPartition(topic, 0)).get().truncateTo(10);

        // Partition should enter ULE_RECOVERY and stay there because one replica is not caught up
        waitForMirrorState(dstAdmin, mirror, ULE_RECOVERY, topic);

        // Start the follower in the destination cluster so all replicas rejoin ISR
        dstCluster.brokers().values().forEach(broker -> {
            if (broker.config().nodeId() != dstLeader) {
                broker.startup();
            }
        });

        // Verify it returns to MIRRORING state with zero lag
        waitForMirrorLagZero(dstAdmin, mirror, topic);
    }

    @Test
    void testDeleteClusterMirror() throws Exception {
        String topic = "delete-mirror-topic";
        String mirror = "my-mirror";

        var result = srcAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        result.all().get(10, TimeUnit.SECONDS);

        Uuid topicId = result.topicId(topic).get();

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        var listConfigResult = dstAdmin.listConfigResources(Set.of(ConfigResource.Type.CLUSTER_MIRROR),
                new ListConfigResourcesOptions()).all().get(10, TimeUnit.SECONDS);
        assertEquals(1, listConfigResult.size());

        // Verify mirror is listed before deletion
        var listingsBefore = dstAdmin.listClusterMirrors().all().get(10, TimeUnit.SECONDS);
        assertTrue(listingsBefore.stream().anyMatch(l -> mirror.equals(l.mirrorName())),
                "Mirror should be listed before deletion");

        // Stop all topics (required precondition for deletion)
        dstAdmin.stopMirrorTopics(mirror, List.of(topic), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, topic);

        dstAdmin.deleteClusterMirror(mirror, new DeleteClusterMirrorOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForListMirrorEmpty(mirror);

        // Verify the mirror is no longer listed after deletion
        listConfigResult = dstAdmin.listConfigResources(Set.of(ConfigResource.Type.CLUSTER_MIRROR),
                new ListConfigResourcesOptions()).all().get(10, TimeUnit.SECONDS);
        assertTrue(listConfigResult.isEmpty());

        // Verify that the __mirror_state topic contains the tombstone records for both
        // MirrorPartitionStateKey and LastMirrorEpochsKey types
        // 1. Get the partition index hosting the metadata for the mirror topic partition
        int partId = dstCluster.brokers().get(0).mirrorCoordinator()
                .partitionFor(new MirrorPartition(mirror, topicId, 0));
        // 2. Get the partition leader
        int leaderMirrorStatePartition = dstCluster.brokers().get(0).metadataCache()
                .getLeaderAndIsr(MIRROR_STATE_TOPIC_NAME, partId).get().leader();
        // 3. Poll until the last batch contains the expected tombstone records.
        //    Tombstones are written asynchronously by the coordinator runtime.
        TopicPartition mirrorStateTp = new TopicPartition(MIRROR_STATE_TOPIC_NAME, partId);
        MirrorCoordinatorRecordSerde serde = new MirrorCoordinatorRecordSerde();
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
    void testDeleteClusterMirrorDisassociationSurvivesControllerRestart() throws Exception {
        String topic = "disassociate-topic";
        String mirror = "my-mirror";

        // Rebuild the destination cluster so that every metadata batch triggers a snapshot
        closeQuietly(dstAdmin);
        closeQuietly(dstCluster);
        dstCluster = buildCluster(2, Map.of(
                MetadataLogConfig.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, "10"));
        dstAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers()
        ));

        srcAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        produceRecords(srcCluster, topic, 0, 10);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        dstAdmin.stopMirrorTopics(mirror, List.of(topic), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, topic);

        dstAdmin.deleteClusterMirror(mirror, new DeleteClusterMirrorOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForListMirrorEmpty(mirror);

        // The controller replayed the disassociation record, so the topic is a normal topic
        dstAdmin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2)))
                .all().get(10, TimeUnit.SECONDS);
        waitForCondition(() -> describeTopics(dstAdmin, List.of(topic)).get(topic).partitions().size() == 2,
                "Partition count of " + topic + " not increased to 2");

        // Wait for the controller to snapshot past the disassociation
        long lastOffset = ((KRaftMetadataCache) dstCluster.brokers().get(0).metadataCache())
                .currentImage().provenance().lastContainedOffset();
        ControllerServer controller = dstCluster.controllers().values().iterator().next();
        waitForCondition(() -> controller.sharedServer().raftManager().replicatedLog().latestSnapshotId()
                        .map(snapshotId -> snapshotId.offset() > lastOffset).orElse(false),
                DEFAULT_MAX_WAIT_MS, "Controller snapshot past offset " + lastOffset + " not generated");

        // Restart the controller so that it rebuilds its state from the snapshot
        controller.shutdown();
        controller.startup();
        // Only 1 controller so this is fine
        waitForCondition(() -> controller.controller().isActive(), "Controller not active after restart");

        // The topic must still be a normal topic. If the snapshot re-associated it with the deleted
        // mirror, the controller rejects this with InvalidMirrorStateException.
        dstAdmin.createPartitions(Map.of(topic, NewPartitions.increaseTo(3)))
                .all().get(30, TimeUnit.SECONDS);
        waitForCondition(() -> describeTopics(dstAdmin, List.of(topic)).get(topic).partitions().size() == 3,
                "Partition count of " + topic + " not increased to 3");
    }

    @Test
    void testMirrorLoopDetection() throws Exception {
        String topic = "my-topic";
        String myMirror = "a-to-b";
        String newMirror = "b-to-a";

        // Create topic on source
        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Create forward mirror (A -> B) and start mirroring
        dstAdmin.createClusterMirror(myMirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, myMirror, topic);

        // Create reverse mirror (A <- B) to trigger loop detection
        srcAdmin.createClusterMirror(newMirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, dstBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);

        // Verify the forward mirror is still MIRRORING
        waitForMirrorState(dstAdmin, myMirror, MIRRORING, topic);
        // Verify the reverse mirror failed due to loop detection
        waitForFailedState(srcAdmin, newMirror, NON_RETRYABLE_ERROR,
                Optional.of("Detected mirror loop for mirror"), topic);
    }

    @Test
    void testDeletedSourceTopicMovesToNonRetryable() throws Exception {
        String topic = "non-retryable-topic";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 20);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Delete source topic to trigger non-retryable failure
        srcAdmin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
        waitForFailedState(dstAdmin, mirror, NON_RETRYABLE_ERROR, Optional.empty(), topic);

        // Verify partitions stay in FAILED across multiple refresh cycles
        long deadline = System.currentTimeMillis() + 3 * METADATA_REFRESH_INTERVAL_MS;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(allPartitionsSatisfy(dstAdmin, mirror,
                            s -> FAILED.name().equals(s.state())
                                    && s.retryAttempt() == NON_RETRYABLE_ERROR, List.of(topic)),
                    "Non-retryable partitions must not be restarted by metadata refresh");
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }

    @Test
    void testAutoRecovery() throws Exception {
        String topic = "auto-recovery-topic";
        String mirror = "my-mirror";

        // Create topic, send some data, start mirroring
        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 3, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        produceRecords(srcCluster, topic, 0, 20);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Shut down source to trigger FAILED state
        srcCluster.brokers().values().forEach(KafkaBroker::shutdown);
        waitForMirrorState(dstAdmin, mirror, FAILED, topic);

        // Restart source so scheduled retry can automatically recover
        srcCluster.brokers().values().forEach(b -> {
            try {
                b.startup();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        waitForMirrorState(dstAdmin, mirror, MIRRORING, topic);

        // Verify data still flows after automatic recovery
        produceRecords(srcCluster, topic, 20, 20);
        waitForMirrorLagZero(dstAdmin, mirror, topic);
        consumeRecords(dstCluster, topic, 40);
    }

    @Test
    void testManualRecovery() throws Exception {
        String topic = "manual-recovery-topic";
        int maxRetries = 3;
        String mirror = "my-mirror";

        // Restart dest cluster to set mirror.failed.retry.max.attempts
        closeQuietly(dstAdmin);
        closeQuietly(dstCluster);
        dstCluster = buildCluster(2, Map.of(
                ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, String.valueOf(maxRetries)
        ));
        dstAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers()
        ));

        // Create topic, send some data, start mirroring
        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 3, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        produceRecords(srcCluster, topic, 0, 20);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Shut down source to trigger FAILED state
        srcCluster.brokers().values().forEach(KafkaBroker::shutdown);
        waitForFailedState(dstAdmin, mirror, maxRetries, Optional.empty(), topic);

        // Restart source so recovery can succeed
        srcCluster.brokers().values().forEach(b -> {
            try {
                b.startup();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Recover the failed partitions
        dstAdmin.recoverMirrorTopics(mirror, List.of(topic), new RecoverMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, MIRRORING, topic);

        // Verify data still flows after manual recovery
        produceRecords(srcCluster, topic, 20, 20);
        waitForMirrorLagZero(dstAdmin, mirror, topic);
        consumeRecords(dstCluster, topic, 40);
    }

    @Test
    void testCreatePartAllowedOnNonMirrorTopic() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";
        String mirror = "my-mirror";

        // Source topic that will be actively mirrored to the destination
        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // A regular destination topic that is not part of any mirror
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, mirrorTopic);

        // With Cluster Mirroring feature enabled the broker intercepts CreatePartitions, but a
        // topic that belongs to no mirror must be forwarded and created without any mirror check.
        dstAdmin.createPartitions(Map.of(nonMirrorTopic, NewPartitions.increaseTo(2)))
                .all().get(10, TimeUnit.SECONDS);
        waitForCondition(() -> describeTopics(dstAdmin,
                        List.of(nonMirrorTopic)).get(nonMirrorTopic).partitions().size() == 2,
                "Non mirror topic count not increased");
    }

    @Test
    void testCreatePartWithMixedMirrorAndNonMirrorTopics() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, mirrorTopic);

        CreatePartitionsResult result = dstAdmin.createPartitions(Map.of(
                mirrorTopic, NewPartitions.increaseTo(2),
                nonMirrorTopic, NewPartitions.increaseTo(2)
        ));

        result.values().get(nonMirrorTopic).get(10, TimeUnit.SECONDS);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> result.values().get(mirrorTopic).get(10, TimeUnit.SECONDS));
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());

        waitForCondition(() -> {
            Map<String, TopicDescription> descriptionMap =
                    describeTopics(dstAdmin, List.of(mirrorTopic, nonMirrorTopic));
            return descriptionMap.get(nonMirrorTopic).partitions().size() == 2
                    && descriptionMap.get(mirrorTopic).partitions().size() == 1;
        }, "Non mirror topic count not increased");
    }

    @Test
    void testCreatePartitionsWithMultipleTopics() throws Exception {
        String topicA = "topic-a";
        String topicB = "topic-b";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, List.of(topicA, topicB));

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
        dstAdmin.stopMirrorTopics(mirror, List.of(topicA, topicB), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, List.of(topicA, topicB));

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
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, mirrorTopic);

        DeleteTopicsResult result = dstAdmin.deleteTopics(Set.of(nonMirrorTopic, mirrorTopic));

        result.topicNameValues().get(nonMirrorTopic).get(10, TimeUnit.SECONDS);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> result.topicNameValues().get(mirrorTopic).get(10, TimeUnit.SECONDS));
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());
    }

    @Test
    void testDeleteTopicsDisallowedOnNonStoppedTopic() throws Exception {
        String topic = "my-topic";
        String mirror = "my-mirror";

        // Create topic and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Attempt to delete topic
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> dstAdmin.deleteTopics(Set.of(topic)).all().get(10, TimeUnit.SECONDS));
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());

        // Stop mirroring and retry
        dstAdmin.stopMirrorTopics(mirror, List.of(topic),
                new StopMirrorTopicsOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, topic);

        dstAdmin.deleteTopics(Set.of(topic)).all().get();
    }

    @Test
    void testDeleteRecordsDisallowedOnNonStoppedTopic() throws Exception {
        String topic = "my-topic";
        String mirror = "my-mirror";

        // Create topic and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        produceRecords(srcCluster, topic, 0, 10);

        // Create mirror and start topic
        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, topic);

        // Attempt to delete records while the topic is actively mirroring
        TopicPartition tp = new TopicPartition(topic, 0);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> dstAdmin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5))).all().get());
        assertEquals(InvalidMirrorStateException.class, e.getCause().getClass());

        // Stop mirroring and retry
        dstAdmin.stopMirrorTopics(mirror, List.of(topic),
                new StopMirrorTopicsOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, mirror, STOPPED, topic);

        waitForCondition(() -> {
            DeleteRecordsResult result = dstAdmin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5)));
            return result.lowWatermarks().get(tp).get(10, TimeUnit.SECONDS).lowWatermark() == 5;
        }, "Records not deleted");
    }

    @Test
    void testDeleteRecordsAllowedOnNonMirrorTopic() throws Exception {
        String mirrorTopic = "mirror-topic";
        String nonMirrorTopic = "non-mirror-topic";
        String mirror = "my-mirror";

        // Source topic that will be actively mirrored to the destination
        srcAdmin.createTopics(List.of(
                new NewTopic(mirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // A regular destination topic that is not part of any mirror
        dstAdmin.createTopics(List.of(
                new NewTopic(nonMirrorTopic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);
        produceRecords(dstCluster, nonMirrorTopic, 0, 10);

        dstAdmin.createClusterMirror(mirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, mirror, mirrorTopic);

        // With Cluster Mirroring feature enabled the broker intercepts DeleteRecords, but a
        // topic that belongs to no mirror must be handled without any mirror check.
        TopicPartition tp = new TopicPartition(nonMirrorTopic, 0);
        DeleteRecordsResult result = dstAdmin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5)));
        assertEquals(5, result.lowWatermarks().get(tp).get(10, TimeUnit.SECONDS).lowWatermark());
    }

    @Test
    void testTieredStoragePartitionFails() throws Exception {
        String topic = "tiered-topic";
        String mirror = "my-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        KafkaClusterTestKit tieredDst = buildCluster(1, Map.of(
                ClusterMirrorConfig.MIRROR_NUM_REPLICA_FETCHERS_CONFIG, "1",
                ClusterMirrorConfig.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1",
                ClusterMirrorConfig.MIRROR_STATE_TOPIC_MIN_ISR_CONFIG, "1",
                GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1",
                ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1",
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
                tieredAdmin.createClusterMirror(mirror, Map.of(
                        ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer
                ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
                waitForMirrorLagZero(tieredAdmin, mirror, topic);

                // Enable tiered storage while mirroring is active
                ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                tieredAdmin.incrementalAlterConfigs(Map.of(topicResource, List.of(
                        new AlterConfigOp(
                                new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                                AlterConfigOp.OpType.SET)
                ))).all().get(10, TimeUnit.SECONDS);
                waitForFailedState(tieredAdmin, mirror, NON_RETRYABLE_ERROR,
                        Optional.of("tiered storage"), topic);
            }
        } finally {
            closeQuietly(tieredDst);
        }
    }

    @Test
    void testListClusterMirrorsFilters() throws Exception {
        String topicA = "list-filter-a";
        String topicB = "list-filter-b";
        String myMirror = "my-mirror";
        String newMirror = "new-mirror";

        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(10, TimeUnit.SECONDS);

        // Mirror 1: topicA in MIRRORING state
        dstAdmin.createClusterMirror(myMirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, topicA
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, myMirror, topicA);

        // Mirror 2: topicB started then stopped
        dstAdmin.createClusterMirror(newMirror, Map.of(
                ClusterMirrorConfig.BOOTSTRAP_SERVERS_CONFIG, srcBootstrapServer,
                ClusterMirrorConfig.TOPICS_INCLUDE_CONFIG, topicB
        ), new CreateClusterMirrorOptions()).all().get(10, TimeUnit.SECONDS);
        waitForMirrorLagZero(dstAdmin, newMirror, topicB);
        dstAdmin.stopMirrorTopics(newMirror, List.of(topicB), new StopMirrorTopicsOptions())
                .all().get(10, TimeUnit.SECONDS);
        waitForMirrorState(dstAdmin, newMirror, STOPPED, topicB);

        // No filter: both mirrors returned
        var all = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions()
        ).all().get(10, TimeUnit.SECONDS);
        assertEquals(2, all.size());

        // Get source cluster ID for later assertions
        String sourceClusterId = all.iterator().next().sourceClusterId();

        // Filter by mirror name
        var byName = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions().mirrorNameFilter(List.of(myMirror))
        ).all().get(10, TimeUnit.SECONDS);
        assertEquals(1, byName.size());
        assertEquals(myMirror, byName.iterator().next().mirrorName());

        var byNameMiss = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions().mirrorNameFilter(List.of("nonexistent"))
        ).all().get(10, TimeUnit.SECONDS);
        assertTrue(byNameMiss.isEmpty());

        // Filter by source cluster ID
        var bySourceCid = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions().sourceClusterIdFilter(List.of(sourceClusterId))
        ).all().get(10, TimeUnit.SECONDS);
        assertEquals(2, bySourceCid.size());

        var bySourceCidMiss = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions().sourceClusterIdFilter(List.of("unknown-cluster-id"))
        ).all().get(10, TimeUnit.SECONDS);
        assertTrue(bySourceCidMiss.isEmpty());

        // Desired state filter: MIRRORING returns topicA on mirror 1, empty on mirror 2
        var byMirroring = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions().desiredStateFilter(List.of(MIRRORING.name()))
        ).all().get(10, TimeUnit.SECONDS);
        assertEquals(2, byMirroring.size());
        var mirroringByName = byMirroring.stream()
                .collect(java.util.stream.Collectors.toMap(ClusterMirrorListing::mirrorName, l -> l));
        assertEquals(List.of(topicA), mirroringByName.get(myMirror).topicNames());
        assertTrue(mirroringByName.get(newMirror).topicNames().isEmpty());

        // Desired state filter: STOPPED returns topicB on mirror 2, empty on mirror 1
        var byStopped = dstAdmin.listClusterMirrors(
                new ListClusterMirrorsOptions().desiredStateFilter(List.of(STOPPED.name()))
        ).all().get(10, TimeUnit.SECONDS);
        assertEquals(2, byStopped.size());
        var stoppedByName = byStopped.stream()
                .collect(java.util.stream.Collectors.toMap(ClusterMirrorListing::mirrorName, l -> l));
        assertTrue(stoppedByName.get(myMirror).topicNames().isEmpty());
        assertEquals(List.of(topicB), stoppedByName.get(newMirror).topicNames());
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
                .setConfigProp(ClusterMirrorConfig.MIRROR_STATE_TOPIC_MIN_ISR_CONFIG, "1")
                .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
                .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "2")
                .setConfigProp(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
                .setConfigProp(ClusterMirrorConfig.SOCKET_TIMEOUT_MS_CONFIG, "5000")
                .setConfigProp(ClusterMirrorConfig.MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, "10")
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
            KafkaClusterTestKit cluster, String topic, int expectedRecords) {
        return consumeRecords(cluster, topic, expectedRecords, null);
    }

    private List<ConsumerRecord<String, String>> consumeRecords(
            KafkaClusterTestKit cluster, String topic, int expectedRecords, String groupId) {
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

    private long getCommittedOffset(Admin admin, String groupId, TopicPartition tp) throws Exception {
        long timeoutMs = 10_000;
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
        throw new AssertionError("No committed offset for " + tp + " in group " + groupId);
    }

    private boolean allPartitionsSatisfy(Admin admin, String mirrorName,
                                         Predicate<ClusterMirrorDescription.LeaderStateDescription> condition,
                                         List<String> topicPatterns) throws Exception {
        var result = admin.describeClusterMirrors(
                List.of(mirrorName), null,
                new DescribeClusterMirrorsOptions().includeMirrorState(true).includeMirrorOffset(true));
        var descriptions = result.allDescriptions().get(5, TimeUnit.SECONDS);
        ClusterMirrorDescription desc = descriptions.get(mirrorName);
        if (desc == null) return false;
        var patterns = topicPatterns.stream()
                .map(java.util.regex.Pattern::compile)
                .toList();
        var matched = desc.leaderStates().entrySet().stream()
                .filter(e -> patterns.stream().anyMatch(p -> p.matcher(e.getKey()).matches()))
                .toList();
        return !matched.isEmpty()
                && matched.stream().allMatch(e -> e.getValue().stream().allMatch(condition));
    }

    private void waitForMirrorState(Admin admin, String mirrorName, MirrorPartitionState state,
                                    String topicPattern) throws Exception {
        waitForMirrorState(admin, mirrorName, state, List.of(topicPattern));
    }

    private void waitForMirrorState(Admin admin, String mirrorName, MirrorPartitionState state,
                                    List<String> topicPatterns) throws Exception {
        waitForCondition(
                () -> allPartitionsSatisfy(admin, mirrorName,
                        s -> state.name().equals(s.state()),
                        topicPatterns),
                180_000,
                "Mirror partitions for " + topicPatterns + " did not reach " + state + " state");
    }

    private void waitForFailedState(Admin admin, String mirrorName, int maxRetries,
                                    Optional<String> errorMsg, String topicPattern) throws Exception {
        waitForCondition(
                () -> allPartitionsSatisfy(admin, mirrorName,
                        s -> FAILED.name().equals(s.state())
                                && (s.retryAttempt() == NON_RETRYABLE_ERROR || s.retryAttempt() >= maxRetries)
                                && (errorMsg.isEmpty() || s.errorMessage() != null
                                    && s.errorMessage().contains(errorMsg.get())),
                        List.of(topicPattern)),
                180_000,
                "Mirror partitions for " + topicPattern + " did not reach FAILED state, " +
                        "or error message did not match");
    }

    private void waitForMirrorLagZero(Admin admin, String mirrorName, String topicPattern) throws Exception {
        waitForMirrorLagZero(admin, mirrorName, List.of(topicPattern));
    }

    private void waitForMirrorLagZero(Admin admin, String mirrorName, List<String> topicPatterns) throws Exception {
        waitForCondition(
                () -> allPartitionsSatisfy(admin, mirrorName,
                        s -> s.lag() == 0 && MIRRORING.name().equals(s.state()),
                        topicPatterns),
                180_000,
                "Mirror partitions for " + topicPatterns + "did not reach zero lag");
    }

    private void waitForListMirrorEmpty(String mirrorName) throws Exception {
        waitForCondition(
                () -> dstAdmin.listClusterMirrors(
                        new ListClusterMirrorsOptions().mirrorNameFilter(List.of(mirrorName))
                ).all().get(5, TimeUnit.SECONDS).isEmpty(),
                180_000,
                "Cluster mirror is not deleted successfully");
    }

    private void assertStableOffset(Admin admin, String groupId,
                                    TopicPartition tp, long expectedOffset) throws Exception {
        long timeoutMs = 3 * METADATA_REFRESH_INTERVAL_MS;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long current = getCommittedOffset(admin, groupId, tp);
            assertEquals(expectedOffset, current,
                    "Consumer group offset must not be overwritten after mirror stop");
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }

    private void assertStableRecordCount(KafkaClusterTestKit cluster, String topic,
                                         int expectedCount, String message) throws Exception {
        long timeoutMs = 3 * METADATA_REFRESH_INTERVAL_MS;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var records = consumeRecords(cluster, topic, expectedCount);
            assertEquals(expectedCount, records.size(), message);
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }
}
