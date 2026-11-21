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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.LastMirror;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.Map;

/**
 * The result of the {@link Admin#describeClusterMirrors} call.
 */
@InterfaceStability.Evolving
public class DescribeClusterMirrorsResult {
    private final KafkaFuture<Map<String, ClusterMirrorDescription>> future;
    private final KafkaFuture<Map<TopicPartition, LastMirror>> lastMirrorFuture;

    DescribeClusterMirrorsResult(KafkaFuture<Map<String, ClusterMirrorDescription>> future) {
        this(future, KafkaFuture.completedFuture(Collections.emptyMap()));
    }

    DescribeClusterMirrorsResult(KafkaFuture<Map<String, ClusterMirrorDescription>> future,
                                 KafkaFuture<Map<TopicPartition, LastMirror>> lastMirrorFuture) {
        this.future = future;
        this.lastMirrorFuture = lastMirrorFuture;
    }

    /**
     * Return a future which succeeds only if all the mirror descriptions succeed.
     */
    public KafkaFuture<Map<String, ClusterMirrorDescription>> allDescriptions() {
        return future;
    }

    /**
     * Return a future containing last mirror epoch and offset lookup results.
     * Keyed by {@link TopicPartition} to {@link LastMirror}.
     */
    public KafkaFuture<Map<TopicPartition, LastMirror>> lastMirrorEpochs() {
        return lastMirrorFuture;
    }
}
