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

import itertools
import json
import os
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import KafkaService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH


class ClientService(KafkaPathResolverMixin, Service):
    """Provides a dedicated node for running Kafka CLI commands off broker nodes."""

    def __init__(self, context, version=DEV_BRANCH):
        super().__init__(context, 1)
        for node in self.nodes:
            node.version = version

    def setup_security(self, security_config):
        security_config.setup_node(self.nodes[0])

    def start_node(self, node):
        pass

    def stop_node(self, node):
        # Kill any hanging or background client
        node.account.ssh("pkill -SIGKILL -f 'kafka\\.tools\\.' || true", allow_fail=True)

    def clean_node(self, node):
        pass


class MirrorConfig:
    """Configuration for a cluster mirror connection properties file."""
    def __init__(
            self,
            bootstrap_servers: str,
            mirror_topic_properties_exclude: str = None,
            mirror_groups_include: str = None,
            mirror_groups_exclude: str = None,
            mirror_acl_include: str = None,
            security_config: SecurityConfig = None,
    ):
        self.properties = {
            "bootstrap.servers": bootstrap_servers,
        }
        if mirror_topic_properties_exclude is not None:
            self.properties["topic.properties.exclude"] = (
                mirror_topic_properties_exclude
            )
        if mirror_groups_include is not None:
            self.properties["groups.include"] = mirror_groups_include
        if mirror_groups_exclude is not None:
            self.properties["groups.exclude"] = mirror_groups_exclude
        if mirror_acl_include is not None:
            self.properties["acls.include"] = mirror_acl_include
        self.properties["socket.timeout.ms"] = "5000"

        if (
                security_config is not None
                and security_config.security_protocol != SecurityConfig.PLAINTEXT
        ):
            self.properties |= security_config.properties

    def props(self, prefix=""):
        config_lines = (
            prefix + key + "=" + value for key, value in self.properties.items()
        )
        return "\n".join(itertools.chain([""], config_lines, [""]))

    def __str__(self):
        """
        Return properties as a string with line separators.
        """
        return self.props()


class MirrorUtils:
    """Shared helpers for Cluster Mirroring tests."""

    def broker_bootstrap(self, node):
        """Return bootstrap server address for a single broker node."""
        return "%s:9092" % node.account.hostname

    def produce_messages(self, kafka, client_node, topic, num_messages,
                         bootstrap_servers=None):
        """Produce messages on a client node using kafka-producer-perf-test."""
        if bootstrap_servers is None:
            bootstrap_servers = kafka.bootstrap_servers(kafka.security_protocol)
        env_prefix, cmd_suffix = kafka._cmd_security_opts(client_node)
        cmd = "%s%s --topic %s --num-records %d --record-size 1 --throughput -1" \
              " --producer-props bootstrap.servers=%s%s" % (
                  env_prefix,
                  kafka.path.script("kafka-producer-perf-test.sh", client_node),
                  topic, num_messages, bootstrap_servers,
                  cmd_suffix.replace("--command-config", "--producer.config")
                  if cmd_suffix else "")
        output = ""
        for line in client_node.account.ssh_capture(cmd, allow_fail=True):
            output += line
        self.logger.debug("Producer output for %s:\n%s", topic, output.strip())

    def consume_messages(self, kafka, client_node, topic, group=None,
                         max_messages=None, expected_count=None,
                         timeout_sec=120, isolation_level=None,
                         from_beginning=True):
        env_prefix, cmd_suffix = kafka._cmd_security_opts(client_node)
        cmd = "%s%s --bootstrap-server %s --topic %s --timeout-ms 5000" % (
            env_prefix,
            kafka.path.script("kafka-console-consumer.sh", client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic)
        if from_beginning:
            cmd += " --from-beginning"
        if max_messages is not None:
            cmd += " --max-messages %d" % max_messages
        if isolation_level is not None:
            cmd += " --isolation-level %s" % isolation_level
        if group is not None:
            cmd += " --group %s" % group
        if cmd_suffix:
            cmd += cmd_suffix.replace("--command-config", "--consumer.config")
        cmd += " 2>/dev/null"

        count = [0]
        def try_consume():
            count[0] = 0
            for line in client_node.account.ssh_capture(cmd, allow_fail=True):
                if line.strip():
                    count[0] += 1
            self.logger.info("Consumed %d messages from %s so far (expected %s)",
                             count[0], topic, expected_count)
            return expected_count is None or count[0] >= expected_count

        # When expected_count is set, retry consumption because the high watermark on
        # destination replicas may not have advanced yet when mirror lag reaches zero.
        if expected_count is not None:
            deadline = time.time() + timeout_sec
            try_consume()
            while count[0] < expected_count and time.time() < deadline:
                time.sleep(5)
                try_consume()
        else:
            try_consume()
        return count[0]

    def create_and_start_mirror(self, kafka, client_node, mirror_name,
                                mirror_cfg, topic):
        """Create a cluster mirror, start a topic, and wait for zero lag."""
        wait_until(
            lambda: kafka.create_cluster_mirror(client_node, mirror_name, mirror_cfg),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in kafka.start_cluster_mirror_topics(
                client_node, mirror_name, topic),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(kafka, client_node, mirror_name, [topic])

    def all_partitions_satisfy(self, kafka, client_node, mirror_name, per_partition_condition, topics):
        """Check that all partitions of the given mirror topics satisfy the condition."""
        output = kafka.describe_cluster_mirror(client_node)
        if output == "":
            return False

        try:
            mirrors = kafka.parse_describe_cluster_mirror(output)
        except (json.JSONDecodeError, KeyError):
            return False

        if mirror_name not in mirrors:
            return False
        mirror = mirrors[mirror_name]

        for topic in topics:
            if topic not in mirror:
                return False

            for partition in mirror[topic].values():
                if not per_partition_condition(partition):
                    return False
        return True

    def wait_mirror_state(self, kafka, client_node, mirror_name, topics,
                          state, err_msg=None):
        """Wait until all mirror partitions reach the given state."""
        def check():
            return self.all_partitions_satisfy(
                kafka, client_node, mirror_name,
                lambda p: p["state"] == state, topics)
        if err_msg is None:
            err_msg = "Mirror did not reach %s state" % state
        wait_until(check, timeout_sec=120, backoff_sec=2, err_msg=err_msg)

    def wait_mirror_lag_zero(self, kafka, client_node, mirror_name,
                             topics, err_msg="Mirror did not catch up"):
        """Wait until all mirror partitions reach MIRRORING state with zero lag."""
        def check():
            return self.all_partitions_satisfy(
                kafka, client_node, mirror_name,
                lambda p: p["lag"] == 0 and p["state"] == "MIRRORING", topics)
        wait_until(check, timeout_sec=120, backoff_sec=2, err_msg=err_msg)

    def wait_for_metadata_refresh(self, kafka, client_node, mirror_name):
        """Wait for metadata sync by sleeping based on the configured refresh interval."""
        kafka.describe_mirror_config(client_node, mirror_name)
        interval_ms = 30000
        for prop in kafka.server_prop_overrides:
            if prop[0] == "mirror.metadata.refresh.interval.ms":
                interval_ms = int(prop[1])
                break
        sleep_s = interval_ms // 1000
        self.logger.info("Waiting %ds for metadata sync (interval=%dms)", sleep_s, interval_ms)
        time.sleep(sleep_s)

    def describe_consumer_group(self, kafka, group, client_node):
        """Describe a consumer group on a client node with security support."""
        env_prefix, cmd_suffix = kafka._cmd_security_opts(client_node)
        cmd = "%s%s --bootstrap-server %s --group %s --describe%s" % (
            env_prefix,
            kafka.path.script("kafka-consumer-groups.sh", client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            group, cmd_suffix)
        output = ""
        for line in client_node.account.ssh_capture(cmd, allow_fail=True):
            if not (line.startswith("SLF4J") or line.startswith("GROUP")
                    or line.startswith("Could not fetch offset")):
                output += line
        return output

    def wait_for_log_convergence(self, source_kafka, dest_kafka, topics):
        """Poll until source leader and all dest replica log segment hashes match."""
        def log_segment_hashes(node, topic, partition):
            cmd = "md5sum %s*/%s-%d/*.log 2>/dev/null" % (
                KafkaService.DATA_LOG_DIR_PREFIX, topic, partition)
            hashes = {}
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                parts = line.strip().split()
                if len(parts) == 2:
                    hashes[os.path.basename(parts[1])] = parts[0]
            return hashes

        def check():
            for topic, cfg in topics.items():
                for partition in range(cfg["partitions"]):
                    source = log_segment_hashes(
                        source_kafka.leader(topic, partition), topic, partition)
                    for node in dest_kafka.replicas(topic, partition):
                        dest = log_segment_hashes(node, topic, partition)
                        if source.keys() != dest.keys() or any(
                                source[seg] != dest[seg] for seg in source):
                            return False
                        self.logger.info("Hashes match for %s-%d dest %s: %d segments verified",
                                         topic, partition, node.name, len(source))
            return True

        wait_until(
            check,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Log segments did not converge between source and destination",
        )
