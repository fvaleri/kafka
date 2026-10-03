# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark.resource import cluster
from ducktape.mark import defaults
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.tests.core.cluster_mirroring_common import ClientService, MirrorConfig, MirrorUtils
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
)


class ClusterMirroringTest(MirrorUtils, Test):
    """Tests for Cluster Mirroring on latest version."""

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClusterMirroringTest, self).__init__(test_context)
        # Single client node shared by both clusters (same DEV_BRANCH version).
        self.client = ClientService(test_context)
        server_props = [
            ["auto.create.topics.enable", "false"],
            ["default.replication.factor", "2"],
            ["min.insync.replicas", "1"],
            ["offsets.topic.replication.factor", "2"],
            ["transaction.state.log.replication.factor", "2"],
            ["transaction.state.log.min.isr", "1"],
            ["share.coordinator.state.topic.replication.factor", "2"],
            ["share.coordinator.state.topic.min.isr", "1"],
            ["mirror.state.topic.replication.factor", "2"],
            ["mirror.failed.retry.max.attempts", "10"],
            ["mirror.metadata.refresh.interval.ms", "5000"],
            ["mirror.num.replica.fetchers", "2"],
            ["mirror.failed.retry.max.backoff.ms", "5000"],
        ]
        self.source_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )
        self.dest_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )

    def setup(self):
        self.client.start()
        self.client_node = self.client.nodes[0]
        self.source_kafka.start()
        self.dest_kafka.start()

        for kafka in [self.source_kafka, self.dest_kafka]:
            self.logger.info(
                "Changing metadata.version on %s to %s", kafka, CLUSTER_MIRRORING_METADATA_VERSION
            )
            kafka.upgrade_metadata_version(CLUSTER_MIRRORING_METADATA_VERSION)
            self.logger.info(
                "Changing mirror.version on %s to %s", kafka, CLUSTER_MIRRORING_VERSION
            )
            kafka.run_features_command(
                "upgrade", "mirror.version", CLUSTER_MIRRORING_VERSION
            )

    def teardown(self):
        self.client.stop()
        for kafka in [self.dest_kafka, self.source_kafka]:
            try:
                kafka.stop()
            except Exception:
                self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                for node in kafka.nodes:
                    kafka.stop_node(node, clean_shutdown=False)

    def run_background_producer(self, kafka, client_node, topic, num_messages,
                                throughput=10):
        """Start a background producer."""
        cmd = "%s --topic %s --num-records %d --record-size 1 --throughput %d" \
              " --producer-props bootstrap.servers=%s" % (
                  kafka.path.script("kafka-producer-perf-test.sh", client_node),
                  topic, num_messages, throughput,
                  kafka.bootstrap_servers(kafka.security_protocol))
        client_node.account.ssh("nohup %s >/dev/null 2>&1 &" % cmd, allow_fail=True)

    def stop_background_process(self, client_node, pattern, signal="SIGTERM"):
        """Stop background processes matching the given pattern."""
        client_node.account.ssh(
            "pkill -%s -f '%s' || true" % (signal, pattern), allow_fail=True)

    def consume_share_messages(self, kafka, client_node, topic, group,
                               max_messages=None, expected_count=None,
                               timeout_sec=120):
        cmd = "%s --bootstrap-server %s --topic %s --group %s --timeout-ms 5000" % (
            kafka.path.script("kafka-console-share-consumer.sh", client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, group)
        if max_messages is not None:
            cmd += " --max-messages %d" % max_messages
        cmd += " 2>/dev/null"

        count = [0]
        def try_consume():
            count[0] = 0
            for line in client_node.account.ssh_capture(cmd, allow_fail=True):
                if line.strip():
                    count[0] += 1
            self.logger.debug("Share-consumed %d messages from %s so far (expected %s)",
                              count[0], topic, expected_count)
            return expected_count is None or count[0] >= expected_count

        if expected_count is not None:
            deadline = time.time() + timeout_sec
            try_consume()
            while count[0] < expected_count:
                if time.time() >= deadline:
                    raise AssertionError(
                        "Expected %d messages on %s, got %d" % (expected_count, topic, count[0]))
                time.sleep(5)
                try_consume()
        else:
            try_consume()
        return count[0]

    def get_end_offsets(self, kafka, client_node, topic, time=-1):
        """Run kafka-get-offsets and return a {partition: offset} dict."""
        cmd = "%s --bootstrap-server %s --topic %s --time %s" % (
            kafka.path.script("kafka-get-offsets.sh", client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, time)
        offsets = {}
        for line in client_node.account.ssh_capture(cmd, allow_fail=True):
            parts = line.strip().split(":")
            if len(parts) == 3:
                offsets[int(parts[1])] = int(parts[2])
        return offsets

    def parse_topic_configs(self, desc):
        """Extract topic configs from kafka-topics describe output."""
        for line in desc.splitlines():
            if "Configs:" in line:
                configs_str = line.split("Configs:")[1].strip()
                if not configs_str:
                    return {}
                result = {}
                for pair in configs_str.split(","):
                    k, v = pair.strip().split("=", 1)
                    result[k] = v
                return result
        return {}

    def trigger_ule(self, source_kafka, client_node, node, topic):
        """Trigger unclean leader election on the given node."""
        cmd = "%s --bootstrap-server %s --topic %s --partition 0 --election-type UNCLEAN" % (
            source_kafka.path.script("kafka-leader-election.sh", client_node),
            self.broker_bootstrap(node), topic)
        try:
            client_node.account.ssh(cmd, allow_fail=False)
            return True
        except RemoteCommandError:
            return False

    def log_hashes(self, source_kafka, dest_kafka, topic, label):
        """Log MD5 hashes of partition log segments for all brokers across both clusters."""
        self.logger.info("#### %s-0 %s ####", topic, label)
        for name, kafka in [("source", source_kafka), ("dest", dest_kafka)]:
            for node in kafka.nodes:
                cmd = "md5sum %s*/%s-0/*.log 2>/dev/null" % (
                    KafkaService.DATA_LOG_DIR_PREFIX, topic)
                lines = list(node.account.ssh_capture(cmd, allow_fail=True))
                if lines:
                    for line in lines:
                        self.logger.info("%s %s: %s", name, node.name, line.strip())
                else:
                    self.logger.info("%s %s: n/a", name, node.name)

    def run_txn_producer(self, client_node, kafka, topic, transactional_id=None, mode="commit",
                         num_messages=1, waiting_ms=0, background=False):
        """Run TransactionalTestProducer."""
        cmd = kafka.path.script("kafka-run-class.sh", client_node)
        cmd += " org.apache.kafka.tools.TransactionalTestProducer"
        cmd += " --bootstrap-server %s" % kafka.bootstrap_servers(kafka.security_protocol)
        cmd += " --topic %s" % topic
        if transactional_id is not None:
            cmd += " --transactional-id %s" % transactional_id
            cmd += " --mode %s" % mode
            if waiting_ms > 0:
                cmd += " --waiting-ms %s" % str(waiting_ms)
        cmd += " --num-records %s" % str(num_messages)
        if background:
            cmd += " &"
        client_node.account.ssh(cmd, allow_fail=False)

    def count_ongoing_txns(self, client_node, kafka, topic, topics):
        """Count ongoing transactions across all partitions of a topic."""
        count = 0
        for partition in range(topics[topic]["partitions"]):
            cmd = kafka.path.script("kafka-transactions.sh", client_node)
            cmd += " --bootstrap-server %s" % kafka.bootstrap_servers(kafka.security_protocol)
            cmd += " describe-producers --topic %s --partition %d" % (topic, partition)
            for line in client_node.account.ssh_capture(cmd, allow_fail=True):
                fields = line.strip().split()
                if fields and fields[-1] != "None" and fields[-1] != "CurrentTransactionStartOffset":
                    count += 1
        return count

    def parse_group_offset(self, desc, topic):
        """Parse describe consumer/share group output and return the offset for a topic."""
        for line in desc.strip().splitlines():
            fields = line.split()
            if len(fields) >= 4 and fields[1] == topic:
                return int(fields[3])
        return None

    def wait_mirror_retries_exhausted(self, kafka, client_node, mirror_name,
                                      topics, max_attempts, err_msg=None):
        """Wait until all mirror partitions are FAILED with retryAttempt >= max_attempts."""
        def check():
            return self.all_partitions_satisfy(
                kafka, client_node, mirror_name,
                lambda p: p["state"] == "FAILED" and p["retry_attempt"] >= max_attempts, topics)
        if err_msg is None:
            err_msg = "Mirror did not exhaust %d retries" % max_attempts
        wait_until(check, timeout_sec=240, backoff_sec=2, err_msg=err_msg)


    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_failover_failback(self, metadata_quorum):
        """Verify failover makes destination writable and failback leverages LME lookup for incremental mirroring."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})

        self.logger.info("Send 3 messages to source")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)

        self.logger.info("Rolling restart source and produce to bump leader epoch")
        for node in self.source_kafka.nodes:
            self.source_kafka.restart_node(node)
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)

        self.logger.info("Start cluster mirror a-to-b on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "a-to-b", mirror_cfg, "my-topic")

        self.logger.info("Consume from destination (expect 6 messages)")
        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                      max_messages=6, expected_count=6)
        assert count >= 6, "Expected 6 messages on my-topic, got %d" % count

        self.logger.info("Failover: stop mirroring so destination topic becomes writable")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, "a-to-b", "my-topic")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "a-to-b", ["my-topic"], "STOPPED")

        self.logger.info("Send 1 record to destination (should work now)")
        self.produce_messages(self.dest_kafka, self.client_node, "my-topic", 1)

        self.logger.info("Send 2 more messages to source (not mirrored)")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 2)

        self.logger.info("Consume from source (expect 8 messages)")
        count = self.consume_messages(self.source_kafka, self.client_node, "my-topic",
                                      max_messages=8, expected_count=8)
        assert count >= 8, "Expected 8 messages on my-topic, got %d" % count

        self.logger.info("Failback: source mirrors from destination via b-to-a")
        mirror_cfg = MirrorConfig(self.dest_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.source_kafka, self.client_node, "b-to-a", mirror_cfg, "my-topic")

        self.logger.info("Verify LME lookup found a valid epoch (direct failback)")
        log_path = "%s/info/server.log" % self.source_kafka.OPERATIONAL_LOG_DIR
        pattern = "Received LME lookup response for mirror=b-to-a"
        found = False
        for node in self.source_kafka.nodes:
            for line in node.account.ssh_capture(
                    "grep '%s' %s 2>/dev/null || true" % (pattern, log_path),
                    allow_fail=True):
                line = line.strip()
                if line:
                    self.logger.info("LME lookup log on %s: %s", node.name, line)
                    value = line.split("my-topic-0=")[1].split("}")[0]
                    epoch = int(value.split("epoch=")[1].split(",")[0])
                    offset = int(value.split("offset=")[1].split(")")[0])
                    assert epoch >= 1, \
                        "Expected LME epoch >= 1, got %d in: %s" % (epoch, line)
                    assert offset == 6, \
                        "Expected LME offset == 6, got %s in: %s" % (offset, line)
                    found = True
        assert found, "No LME lookup log found on any source broker"

        self.logger.info("Consume from source (non-mirrored data should be truncated)")
        count = self.consume_messages(self.source_kafka, self.client_node, "my-topic",
                                      max_messages=8, expected_count=7)
        assert count >= 7, "Expected 7 messages on my-topic, got %d" % count

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_pause_resume(self, metadata_quorum):
        """Verify that pausing stops replication and resuming catches up."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})

        self.logger.info("Send 3 messages to source")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Pause mirroring and send 1 more record to source")
        self.dest_kafka.pause_cluster_mirror_topics(self.client_node, "my-mirror", "my-topic")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 1)
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "PAUSED")

        self.logger.info("Consume from destination while paused (expect 3, the 4th is not mirrored yet)")
        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                      max_messages=4, expected_count=3)
        assert count == 3, "Expected 3 messages on destination while paused, got %d" % count

        self.logger.info("Produce to destination while paused (should fail)")
        self.produce_messages(self.dest_kafka, self.client_node, "my-topic", 1)

        self.logger.info("Resume mirroring and wait for it to catch up")
        self.dest_kafka.resume_cluster_mirror_topics(self.client_node, "my-mirror", "my-topic")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "MIRRORING")
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"])

        self.logger.info("Consume from destination (expect 4 messages after resume)")
        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                      max_messages=4, expected_count=4)
        assert count == 4, "Expected 4 messages on my-topic, got %d" % count

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_config_update(self, metadata_quorum):
        """Verify that updating mirror config triggers reconnection and mirroring continues."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 1})

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Alter config with invalid config (should fail)")
        self.dest_kafka.alter_mirror_config(self.client_node, "my-mirror", "invalid.config=foo")

        self.logger.info("Alter config with valid bootstrap.servers pointing to source broker 1 (triggers reconnection)")
        new_bootstrap = "%s:9092" % self.source_kafka.nodes[1].account.hostname
        self.dest_kafka.alter_mirror_config(self.client_node, "my-mirror", "bootstrap.servers=%s" % new_bootstrap)

        self.logger.info("Verify mirroring still works after config change")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "MIRRORING")

        self.logger.info("Send messages to source and verify they arrive at destination")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"])
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                      max_messages=3, expected_count=3)
        assert count >= 3, "Expected 3 messages on my-topic, got %d" % count

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_mirror_deletion(self, metadata_quorum):
        """Verify that a mirror cannot be deleted while not empty, but can after stopping all topics."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 1})

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Delete mirror while not empty (should fail)")
        self.dest_kafka.delete_cluster_mirror(self.client_node, "my-mirror")
        output = self.dest_kafka.list_cluster_mirror(self.client_node)
        assert "my-mirror" in output, "Mirror should still exist after failed deletion"

        self.logger.info("Stop mirror topics and wait for STOPPED state")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, "my-mirror", "my-topic")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "STOPPED")

        self.logger.info("Delete mirror (should work now)")
        self.dest_kafka.delete_cluster_mirror(self.client_node, "my-mirror")
        output = self.dest_kafka.list_cluster_mirror(self.client_node)
        assert "my-mirror" not in output, "Mirror should be gone after deletion"

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_topics_include_and_exclude(self, metadata_quorum):
        """Verify topic include and exclude filtering via mirror config, stopped topics not re-discovered, and auto-discovery."""
        self.logger.info("Using %s", metadata_quorum)
        self.logger.info("Create 4 topics on source")
        topics = {
            "orders-us": {"partitions": 1, "replication-factor": 2},
            "orders-eu": {"partitions": 1, "replication-factor": 2},
            "orders-internal": {"partitions": 1, "replication-factor": 2},
            "payments": {"partitions": 1, "replication-factor": 2},
        }
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Send messages to source topics")
        self.produce_messages(self.source_kafka, self.client_node, "orders-us", 3)
        self.produce_messages(self.source_kafka, self.client_node, "orders-eu", 3)
        self.produce_messages(self.source_kafka, self.client_node, "orders-internal", 2)
        self.produce_messages(self.source_kafka, self.client_node, "payments", 2)

        self.logger.info("Start mirror with regex include and exclude via config")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, "my-mirror", mirror_cfg),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        self.dest_kafka.alter_mirror_config(
            self.client_node, "my-mirror", "topics.include=orders-.*,topics.exclude=orders-internal")
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        self.logger.info("Only orders-us and orders-eu should be mirrored")
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror",
                                  ["orders-us", "orders-eu"])

        count = self.consume_messages(self.dest_kafka, self.client_node, "orders-us",
                                      max_messages=3, expected_count=3)
        assert count >= 3, "Expected 3 messages on orders-us, got %d" % count
        count = self.consume_messages(self.dest_kafka, self.client_node, "orders-eu",
                                      max_messages=3, expected_count=3)
        assert count >= 3, "Expected 3 messages on orders-eu, got %d" % count

        self.logger.info("Verify excluded and non-matching topics don't exist on destination")
        dest_topics = list(self.dest_kafka.list_topics(self.client_node))
        assert "orders-internal" not in dest_topics, \
            "Expected orders-internal to not exist on destination (excluded)"
        assert "payments" not in dest_topics, \
            "Expected payments to not exist on destination (not included)"

        self.logger.info("Stop orders-eu")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, "my-mirror", "orders-eu")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["orders-eu"], "STOPPED")

        self.logger.info("Send 3 more messages to orders-eu on source (should not be mirrored)")
        self.produce_messages(self.source_kafka, self.client_node, "orders-eu", 3)
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        count_eu = self.consume_messages(self.dest_kafka, self.client_node, "orders-eu")
        assert count_eu == 3, \
            "Expected 3 messages for orders-eu after stop (not 6), got %d" % count_eu

        self.logger.info("Verify orders-eu is not re-discovered after two more metadata refresh cycles")
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        count_eu = self.consume_messages(self.dest_kafka, self.client_node, "orders-eu")
        assert count_eu == 3, \
            "Expected orders-eu to remain at 3 messages (not re-discovered), got %d" % count_eu

        self.logger.info("Create new topic on source that matches the include pattern")
        self.source_kafka.create_topic({"topic": "orders-jp", "partitions": 1, "replication-factor": 2})
        self.produce_messages(self.source_kafka, self.client_node, "orders-jp", 3)

        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["orders-jp"], "MIRRORING")
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["orders-jp"])

        count = self.consume_messages(self.dest_kafka, self.client_node, "orders-jp",
                                      max_messages=3, expected_count=3)
        assert count >= 3, "Expected 3 messages on orders-jp, got %d" % count

        self.logger.info("Add payments to include pattern via alter config")
        self.dest_kafka.alter_mirror_config(
            self.client_node, "my-mirror", "topics.include=[orders-.*,payments]")

        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["payments"], "MIRRORING")
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["payments"])

        count = self.consume_messages(self.dest_kafka, self.client_node, "payments",
                                      max_messages=2, expected_count=2)
        assert count >= 2, "Expected 2 messages on payments, got %d" % count

        self.logger.info("Verify orders-internal still doesn't exist on destination after all operations")
        dest_topics = list(self.dest_kafka.list_topics(self.client_node))
        assert "orders-internal" not in dest_topics, \
            "Expected orders-internal to still not exist on destination (excluded)"

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_props_exclude(self, metadata_quorum):
        """Verify that excluded topic properties are not synced to destination."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 1})

        self.logger.info("Start cluster mirror with properties exclude")
        mirror_cfg = MirrorConfig(
            self.source_kafka.bootstrap_servers(),
            mirror_topic_properties_exclude="retention.bytes,min.insync.replicas")
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Alter topic config on source")
        self.source_kafka.alter_topic_config(
            "my-topic", "retention.ms=100002,retention.bytes=10000000002,min.insync.replicas=2",
            node=self.client_node)

        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        self.logger.info("Verify excluded properties were not synced to destination")
        dest_configs = self.parse_topic_configs(self.dest_kafka.describe_topic("my-topic", node=self.client_node))
        self.logger.info("Destination topic configs: %s", dest_configs)

        assert dest_configs.get("retention.ms") == "100002", \
            "Expected dest retention.ms=100002 (synced), got %s" % dest_configs.get("retention.ms")
        assert dest_configs.get("retention.bytes") != "10000000002", \
            "Expected dest retention.bytes to not be synced (excluded), got %s" % dest_configs.get("retention.bytes")
        assert dest_configs.get("min.insync.replicas") != "2", \
            "Expected dest min.insync.replicas to not be synced (excluded), got %s" % dest_configs.get("min.insync.replicas")

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_groups_filtering(self, metadata_quorum):
        """Verify consumer group offset mirroring with include/exclude filtering."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})

        self.logger.info("Produce messages to source")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)

        self.logger.info("Create consumer groups on source by consuming all messages")
        for group in ["app-group1", "app-group2", "internal-group1"]:
            self.consume_messages(self.source_kafka, self.client_node, "my-topic", group, max_messages=3)

        self.logger.info("Start cluster mirror with groups include (app-.*) and exclude (app-group2)")
        mirror_cfg = MirrorConfig(
            self.source_kafka.bootstrap_servers(),
            mirror_groups_include="app-.*",
            mirror_groups_exclude="app-group2",
        )
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        self.logger.info("Verify only app-group1 is synced on destination")
        groups_output = self.dest_kafka.list_consumer_groups(self.client_node)
        assert "app-group1" in groups_output, \
            "Expected app-group1 to be synced on destination"
        assert "app-group2" not in groups_output, \
            "Expected app-group2 to be excluded from destination"
        assert "internal-group1" not in groups_output, \
            "Expected internal-group1 to not be included on destination"

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_log_convergence(self, metadata_quorum):
        """Verify that mirror log segments are byte identical to source after bouncing both clusters."""
        self.logger.info("Using %s", metadata_quorum)
        self.logger.info("Create source topics and produce initial data")
        topics = {
            "my-topic-a": {"partitions": 3, "replication-factor": 2},
            "my-topic-b": {"partitions": 2, "replication-factor": 2},
            "new-topic": {"partitions": 1, "replication-factor": 2},
        }
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        for topic in topics:
            self.produce_messages(self.source_kafka, self.client_node, topic, 100)

        self.logger.info("Bounce source cluster to trigger leader elections before mirroring")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Create and start cluster mirrors")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        # mirrors map mirror name to (topics_regex, topic_list)
        mirrors = {
            "my-mirror": ("my-topic.*", ["my-topic-a", "my-topic-b"]),
            "new-mirror": ("new-topic", ["new-topic"]),
        }

        for mirror_name in mirrors:
            self.logger.info("Creating mirror %s", mirror_name)
            wait_until(
                lambda mn=mirror_name: self.dest_kafka.create_cluster_mirror(
                    self.client_node, mn, mirror_cfg
                ),
                timeout_sec=60,
                backoff_sec=2,
                err_msg="Failed to create cluster mirror %s" % mirror_name,
            )
        for mirror_name, (topics_regex, _) in mirrors.items():
            wait_until(
                lambda mn=mirror_name, tr=topics_regex: (
                        "Started"
                        in self.dest_kafka.start_cluster_mirror_topics(self.client_node, mn, tr)
                ),
                timeout_sec=60,
                backoff_sec=2,
                err_msg="Failed to start mirror topics for %s" % mirror_name,
            )
        for mirror_name, (_, topic_list) in mirrors.items():
            self.wait_mirror_state(
                self.dest_kafka, self.client_node, mirror_name, topic_list, "MIRRORING",
                err_msg="Failed to start mirror %s" % mirror_name,
            )

        self.logger.info("Start background producers on source topics")
        for t in topics:
            self.run_background_producer(self.source_kafka, self.client_node, t, 100)

        self.logger.info("Bounce source and dest brokers in interleaved order to trigger leader elections")
        src0, src1 = self.source_kafka.nodes[0], self.source_kafka.nodes[1]
        dst0, dst1 = self.dest_kafka.nodes[0], self.dest_kafka.nodes[1]
        for kafka, node in [
            (self.dest_kafka, dst0), (self.dest_kafka, dst1),
            (self.source_kafka, src1), (self.dest_kafka, dst0),
            (self.source_kafka, src0), (self.dest_kafka, dst1),
        ]:
            kafka.restart_node(node)

        self.logger.info("Kill background producers")
        self.stop_background_process(
            self.client_node, "ProducerPerformance", signal="SIGKILL")

        for mirror_name, (_, topic_list) in mirrors.items():
            self.wait_mirror_lag_zero(
                self.dest_kafka, self.client_node, mirror_name, topics=topic_list,
                err_msg="Mirror %s did not catch up" % mirror_name)

        self.logger.info("Poll until source and destination log segment hashes converge")
        self.wait_for_log_convergence(self.source_kafka, self.dest_kafka, topics)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_log_convergence_ule(self, metadata_quorum):
        """Verify log convergence after unclean leader elections and failover/failback."""
        self.logger.info("Using %s", metadata_quorum)
        self.logger.info("Create source topic with ULE support enabled")
        topics = {"my-topic": {"partitions": 1, "replication-factor": 2}}

        self.source_kafka.create_topic({
            "topic": "my-topic", **topics["my-topic"],
            "configs": {"mirror.support.unclean.leader.election": "true"},
        })

        src_broker0 = self.source_kafka.nodes[0]
        src_broker1 = self.source_kafka.nodes[1]

        self.logger.info("Bounce source brokers to trigger leader elections")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Send 1 message via source broker 0")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 1,
                              bootstrap_servers=self.broker_bootstrap(src_broker0))

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "new-mirror", mirror_cfg, "my-topic")

        dest_broker0 = self.dest_kafka.nodes[0]
        enable_ule_support_cmd = "%s --entity-type topics --entity-name %s --alter --add-config mirror.support.unclean.leader.election=true" % \
                                 (self.dest_kafka.kafka_configs_cmd_with_optional_security_settings(dest_broker0, force_use_zk_connection=False), "my-topic")
        dest_broker0.account.ssh(enable_ule_support_cmd)

        self.logger.info("Stop source broker 0 (broker 0 becomes stale)")
        self.source_kafka.stop_node(src_broker0)

        self.logger.info("Send 1 message via source broker 1")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 1,
                              bootstrap_servers=self.broker_bootstrap(src_broker1))
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node,
                                  "new-mirror", ["my-topic"], err_msg="Mirror did not catch up after broker 0 stopped")
        self.log_hashes(
            self.source_kafka, self.dest_kafka, "my-topic",
            "After source broker 0 stopped (broker 0 should be out of sync)")

        self.logger.info("ULE 1: stop broker 1, start broker 0 (stale), elect it as leader")
        self.source_kafka.stop_node(src_broker1)
        self.source_kafka.start_node(src_broker0)
        wait_until(
            lambda: self.trigger_ule(
                self.source_kafka, self.client_node, src_broker0, "my-topic"),
            timeout_sec=30, backoff_sec=2,
            err_msg="Failed to trigger unclean leader election on %s" % src_broker0.name)

        self.logger.info("Send 2 messages via source broker 0")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 2,
                              bootstrap_servers=self.broker_bootstrap(src_broker0))
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node,
                                  "new-mirror", ["my-topic"], err_msg="Mirror did not catch up after ULE 1")
        self.log_hashes(
            self.source_kafka, self.dest_kafka, "my-topic",
            "After ULE 1 (broker 1 should be out of sync)")

        self.logger.info("Failover: stop mirror so destination topic becomes writable")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, "new-mirror", "my-topic")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "new-mirror", ["my-topic"], "STOPPED")

        self.logger.info("Send 2 messages via destination broker 0")
        self.produce_messages(self.dest_kafka, self.client_node, "my-topic", 2)

        self.logger.info("ULE 2: stop broker 0, start broker 1 (stale), elect it as leader")
        self.source_kafka.stop_node(src_broker0)
        self.source_kafka.start_node(src_broker1)
        wait_until(
            lambda: self.trigger_ule(
                self.source_kafka, self.client_node, src_broker1, "my-topic"),
            timeout_sec=30, backoff_sec=2,
            err_msg="Failed to trigger unclean leader election on %s" % src_broker1.name)

        self.logger.info("Send 6 messages via source broker 1")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 6,
                              bootstrap_servers=self.broker_bootstrap(src_broker1))
        self.log_hashes(
            self.source_kafka, self.dest_kafka, "my-topic",
            "After ULE 2 (broker 1 should have the most up to date data)")

        self.logger.info("Failback: source now mirrors from destination")
        mirror_cfg = MirrorConfig(self.dest_kafka.bootstrap_servers())
        wait_until(
            lambda: self.source_kafka.create_cluster_mirror(
                self.client_node, "new-mirror", mirror_cfg),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to create reverse cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.source_kafka.start_cluster_mirror_topics(
                self.client_node, "new-mirror", "my-topic"),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to start reverse mirror topics",
        )
        # Mirror stays in LOG_ALIGNMENT until all source replicas rejoin ISR for LME truncation
        self.wait_mirror_state(self.source_kafka, self.client_node,
                               "new-mirror", ["my-topic"], "LOG_ALIGNMENT")

        self.logger.info("Start the stopped source broker so all replicas rejoin ISR for LME truncation")
        self.source_kafka.start_node(src_broker0)
        self.wait_mirror_state(self.source_kafka, self.client_node,
                               "new-mirror", ["my-topic"], "MIRRORING", err_msg="Reverse mirror did not reach MIRRORING state")

        self.logger.info("Wait for reverse mirror to catch up")
        self.wait_mirror_lag_zero(self.source_kafka, self.client_node,
                                  "new-mirror", ["my-topic"], err_msg="Reverse mirror did not catch up")

        self.logger.info("Poll until log segment hashes converge (dest is source of truth after failback)")
        self.wait_for_log_convergence(self.dest_kafka, self.source_kafka, topics)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_stop_with_txns(self, metadata_quorum):
        """Verify that stop operation aborts pending transactions and allows committed reads."""
        self.logger.info("Using %s", metadata_quorum)
        self.logger.info("Create source topic")
        topics = {"my-topic": {"partitions": 1, "replication-factor": 2}}
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Run initial committed transaction and bounce source to bump leader epoch")
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic", "my-topic-a", "commit")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Send interleaved transactional and non-transactional messages, leaving 2 txns pending")
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic", "my-topic-a", "commit", waiting_ms=5000, background=True)
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic")
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic", "my-topic-b", "pending")
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic", "my-topic-d", "pending")
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic", "my-topic-c", "abort", waiting_ms=5000, background=True)
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic", "my-topic-d", "pending")
        self.run_txn_producer(self.client_node, self.source_kafka, "my-topic")

        ongoing = self.count_ongoing_txns(self.client_node, self.source_kafka, "my-topic", topics)
        assert ongoing == 2, "Expected 2 ongoing transactions, got %d" % ongoing

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Stop cluster mirror (aborts pending transactions and writes PID reset barriers)")
        self.dest_kafka.stop_cluster_mirror_topics(
            self.client_node, "my-mirror", "my-topic"
        )
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "STOPPED")

        self.logger.info("Verify extra messages on destination from abort markers and PID reset barriers")
        source_offsets = self.get_end_offsets(self.source_kafka, self.client_node, "my-topic")
        dest_offsets = self.get_end_offsets(self.dest_kafka, self.client_node, "my-topic")
        extra = sum(dest_offsets.values()) - sum(source_offsets.values())
        # 2 abort markers (one per pending txn) + 1 PID reset barrier per partition
        num_partitions = topics["my-topic"]["partitions"]
        expected_extra = 2 + num_partitions
        assert extra == expected_extra, \
            "Expected %d extra messages (2 abort + %d PID reset) on destination, got %d. " \
            "source=%s, dest=%s" % (expected_extra, num_partitions, extra, source_offsets, dest_offsets)

        self.logger.info("Commit pending transactions on destination and verify consumer counts")
        self.run_txn_producer(self.client_node, self.dest_kafka, "my-topic", "my-topic-b", "commit")
        self.run_txn_producer(self.client_node, self.dest_kafka, "my-topic", "my-topic-d", "commit")

        self.logger.info("Checking raw number of messages")
        source_count = self.consume_messages(self.source_kafka, self.client_node, "my-topic")
        dest_count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                           expected_count=source_count + 2)
        assert dest_count >= source_count + 2, \
            "Expected %d messages on my-topic, got %d" % (source_count + 2, dest_count)

        self.logger.info("Checking read_committed consumer can make progress")
        # 4 aborted messages are filtered out: txn-b, txn-c, txn-d fenced, txn-d pending
        committed_count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                                isolation_level="read_committed",
                                                expected_count=dest_count - 4)
        assert committed_count >= dest_count - 4, \
            "Expected %d read_committed messages on my-topic, got %d" % (dest_count - 4, committed_count)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_consumer_groups_commit(self, metadata_quorum):
        """Verify consumer group offset sync for mirror topics."""
        self.logger.info("Using %s", metadata_quorum)

        # Phase 1: Offset syncs while group is idle.
        self.logger.info("Create two topics on source and send 1 message")
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})
        self.source_kafka.create_topic({"topic": "other-topic", "partitions": 1, "replication-factor": 2})
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 1)
        self.produce_messages(self.source_kafka, self.client_node, "other-topic", 1)

        self.logger.info("Start cluster mirror with only my-topic (not other-topic)")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Consume from both topics using a named consumer group")
        self.consume_messages(self.source_kafka, self.client_node, "my-topic", "my-group", max_messages=1)
        self.consume_messages(self.source_kafka, self.client_node, "other-topic", "my-group", max_messages=1)
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        self.logger.info("Verify my-group on dest has my-topic but not other-topic")
        dest_desc = self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node)
        dest_offset = self.parse_group_offset(dest_desc, "my-topic")
        assert dest_offset == 1, \
            "Expected offset 1 on destination, got %s" % dest_offset
        assert "other-topic" not in dest_desc, \
            "Expected other-topic offset to not appear on destination"

        # Phase 2: Offset sync skipped while group is active.
        # Create dummy-topic on destination so the consumer can join my-group without committing offsets for my-topic.
        # We need to create dummy-topic because a consumer group can't be subscribed to a non-existent topic.
        # The group coordinator rejects offset commits for groups with active members.
        self.logger.info("Start background consumers to make my-group active on destination")
        self.dest_kafka.create_topic({"topic": "dummy-topic", "partitions": 1, "replication-factor": 1})
        dest_consumer_cmd = "%s --bootstrap-server %s --topic dummy-topic --group my-group --consumer-property session.timeout.ms=10000" % (
            self.dest_kafka.path.script("kafka-console-consumer.sh", self.client_node),
            self.dest_kafka.bootstrap_servers(self.dest_kafka.security_protocol))
        self.client_node.account.ssh("nohup %s >/dev/null 2>&1 &" % dest_consumer_cmd, allow_fail=True)

        # Background consumer on source to advance committed offset
        src_consumer_cmd = "%s --bootstrap-server %s --topic my-topic --group my-group --from-beginning --consumer-property session.timeout.ms=10000" % (
            self.source_kafka.path.script("kafka-console-consumer.sh", self.client_node),
            self.source_kafka.bootstrap_servers(self.source_kafka.security_protocol))
        self.client_node.account.ssh("nohup %s >/dev/null 2>&1 &" % src_consumer_cmd, allow_fail=True)

        self.logger.info("Produce on source and wait for source offset to advance")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 1)
        wait_until(
            lambda: self.parse_group_offset(
                self.describe_consumer_group(self.source_kafka, "my-group", self.client_node),
                "my-topic") == 2,
            timeout_sec=30, backoff_sec=2,
            err_msg="Source consumer group offset did not reach 2")
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"])
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        self.logger.info("Verify offset did NOT advance on destination")
        dest_offset = self.parse_group_offset(
            self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node), "my-topic")
        assert dest_offset is not None and dest_offset < 2, \
            "Expected offset sync to be skipped while group is active, but offset is %s" % dest_offset

        self.logger.info("Stop background consumers and verify offset syncs after group becomes idle")
        self.stop_background_process(self.client_node, "ConsoleConsumer")
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")
        wait_until(
            lambda: self.parse_group_offset(
                self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node),
                "my-topic") == 2,
            timeout_sec=30, backoff_sec=2,
            err_msg="Consumer group offset did not reach 2 on destination after group became idle")

        # Phase 3: No offset sync after mirror stop.
        self.logger.info("Stop mirror")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, "my-mirror", "my-topic")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "STOPPED")

        self.logger.info("Verify no offset sync after mirror stop")
        self.consume_messages(self.source_kafka, self.client_node, "my-topic", "my-group",
                              max_messages=1, from_beginning=False)
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")
        dest_offset = self.parse_group_offset(
            self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node), "my-topic")
        assert dest_offset == 2, \
            "Expected offset to stay at 2 after mirror stop, but got %s" % dest_offset

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_share_groups_commit(self, metadata_quorum):
        """Verify share group offset sync for mirror topics."""
        self.logger.info("Using %s", metadata_quorum)

        # Phase 1: Offset syncs while group is idle.
        self.logger.info("Create two topics on source and send some messages")
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})
        self.source_kafka.create_topic({"topic": "other-topic", "partitions": 1, "replication-factor": 2})
        # With more messages pending in the partition, consuming 1 at a time should trigger SPSO archival reliably.
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 10)
        self.produce_messages(self.source_kafka, self.client_node, "other-topic", 10)

        self.logger.info("Start cluster mirror with only my-topic")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        # Without initial reset no previously produced record can be consumed (there is no --from-beginning flag)
        self.source_kafka.reset_share_group_offsets(self.client_node, "my-share-group", "my-topic")

        self.logger.info("Consume from both topics using a named share group")
        self.consume_share_messages(
            self.source_kafka, self.client_node, "my-topic", "my-share-group", max_messages=1)
        self.consume_share_messages(
            self.source_kafka, self.client_node, "other-topic", "my-share-group", max_messages=1)
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")

        self.logger.info("Verify my-share-group on dest has my-topic but not other-topic")
        dest_desc = self.dest_kafka.describe_share_group("my-share-group", self.client_node)
        dest_spso = self.parse_group_offset(dest_desc, "my-topic")
        assert dest_spso == 1, \
            "Expected SPSO 1 on destination, got %s" % dest_spso
        assert "other-topic" not in dest_desc, \
            "Expected other-topic offset to not appear on destination"

        # Phase 2: Offset sync skipped while group is active.
        # Subscribe to dummy-topic so the share consumer joins my-share-group without consuming from my-topic.
        # Unlike consumer groups, share groups can subscribe to non-existent topics, so no pre-creation is needed.
        # The group coordinator rejects alterShareGroupOffsets for non-empty groups.
        self.logger.info("Start background share consumer to make my-share-group active on destination")
        dest_share_cmd = "%s --bootstrap-server %s --topic dummy-topic --group my-share-group 2>/dev/null" % (
            self.dest_kafka.path.script("kafka-console-share-consumer.sh", self.client_node),
            self.dest_kafka.bootstrap_servers(self.dest_kafka.security_protocol))
        self.client_node.account.ssh("nohup %s >/dev/null 2>&1 &" % dest_share_cmd, allow_fail=True)

        self.logger.info("Advance source SPSO while destination share group is active")
        self.consume_share_messages(
            self.source_kafka, self.client_node, "my-topic", "my-share-group", max_messages=1)
        wait_until(
            lambda: self.parse_group_offset(
                self.source_kafka.describe_share_group("my-share-group", self.client_node),
                "my-topic") == 2,
            timeout_sec=120, backoff_sec=2,
            err_msg="Source share group offset did not reach 2")

        self.logger.info("Verify SPSO did NOT advance on destination")
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")
        dest_spso = self.parse_group_offset(
            self.dest_kafka.describe_share_group("my-share-group", self.client_node), "my-topic")
        assert dest_spso is not None and dest_spso < 2, \
            "Expected offset sync to be skipped while share group is active, but offset is %s" % dest_spso

        self.logger.info("Stop background share consumers and verify offset syncs after group becomes idle")
        self.stop_background_process(self.client_node, "ConsoleShareConsumer")
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")
        wait_until(
            lambda: self.parse_group_offset(
                self.dest_kafka.describe_share_group("my-share-group", self.client_node),
                "my-topic") == 2,
            timeout_sec=30, backoff_sec=2,
            err_msg="Share group offset did not reach 2 on destination after group became idle")

        # Phase 3: No offset sync after mirror stop.
        self.logger.info("Stop mirror and verify SPSO synced to destination")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, "my-mirror", "my-topic")
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "STOPPED")

        self.logger.info("Verify no SPSO sync after mirror stop")
        self.consume_share_messages(
            self.source_kafka, self.client_node, "my-topic", "my-share-group",
            max_messages=2, expected_count=2)
        self.wait_for_metadata_refresh(self.dest_kafka, self.client_node, "my-mirror")
        dest_spso = self.parse_group_offset(
            self.dest_kafka.describe_share_group("my-share-group", self.client_node), "my-topic")
        assert dest_spso == 2, \
            "Expected SPSO to stay at 2 after mirror stop, but got %s" % dest_spso

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_epoch_bump(self, metadata_quorum):
        """Verify that consumers on mirror topics with source leader epochs don't hang on restart."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 1})

        self.logger.info("Produce 3 messages and bounce source brokers to bump leader epoch")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)
        for node in self.source_kafka.nodes:
            self.source_kafka.restart_node(node)

        self.logger.info("Produce 2 more messages after leader elections")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 2)

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Consume 2 messages from destination with a consumer group (commits offsets with source LE)")
        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic", "my-group",
                                      max_messages=2, expected_count=2)
        assert count >= 2, "Expected 2 messages on my-topic, got %d" % count

        self.logger.info("Restart consumer (triggers OffsetFetch and refreshCommittedOffsets)")
        # Without the epoch bump fix, this would hang because source LE > local LE
        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic", "my-group",
                                      max_messages=2, expected_count=2, from_beginning=False)
        assert count >= 2, "Expected 2 messages on my-topic after restart, got %d" % count

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_auto_recovery(self, metadata_quorum):
        """Verify that a FAILED partition can be automatically recovered after source broker restart."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 3, "replication-factor": 1})

        self.logger.info("Produce initial messages")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Stop all source brokers to trigger FAILED state")
        for node in self.source_kafka.nodes:
            self.source_kafka.stop_node(node)
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "FAILED",
                               err_msg="Mirror did not reach FAILED state after source shutdown")

        self.logger.info("Restart source brokers so scheduled retry can recover")
        for node in self.source_kafka.nodes:
            self.source_kafka.start_node(node)
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "MIRRORING",
                               err_msg="Mirror did not recover to MIRRORING after source restart")

        self.logger.info("Verify data still flows after recovery")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"])

        count = self.consume_messages(self.dest_kafka, self.client_node, "my-topic",
                                      max_messages=6, expected_count=6)
        assert count >= 6, "Expected 6 messages on my-topic, got %d" % count

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_manual_recovery(self, metadata_quorum):
        """Verify that a FAILED partition can be manually recovered after source broker restart."""
        self.logger.info("Using %s", metadata_quorum)
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 3, "replication-factor": 1})

        self.logger.info("Produce initial messages")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)

        self.logger.info("Start cluster mirror on destination cluster")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.create_and_start_mirror(
            self.dest_kafka, self.client_node, "my-mirror", mirror_cfg, "my-topic")

        self.logger.info("Stop all source brokers to trigger FAILED state with retries exhausted")
        for node in self.source_kafka.nodes:
            self.source_kafka.stop_node(node)
        self.wait_mirror_retries_exhausted(self.dest_kafka, self.client_node, "my-mirror",
                                           ["my-topic"], max_attempts=10,
                                           err_msg="Mirror did not exhaust retries after source shutdown")

        self.logger.info("Restart source brokers")
        for node in self.source_kafka.nodes:
            self.source_kafka.start_node(node)

        self.logger.info("Recover failed mirror partitions")
        wait_until(
            lambda: "Recovered" in self.dest_kafka.recover_cluster_mirror_topics(
                self.client_node, "my-mirror", "my-topic"),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to recover mirror topics",
        )
        self.wait_mirror_state(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"], "MIRRORING",
                               err_msg="Mirror did not recover to MIRRORING after recover command")

        self.logger.info("Verify data still flows after recovery")
        self.produce_messages(self.source_kafka, self.client_node, "my-topic", 3)
        self.wait_mirror_lag_zero(self.dest_kafka, self.client_node, "my-mirror", ["my-topic"])
