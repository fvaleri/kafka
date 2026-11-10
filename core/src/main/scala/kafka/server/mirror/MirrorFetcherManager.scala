/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.mirror

import kafka.server._
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState}
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfig
import org.apache.kafka.server.network.BrokerEndPoint

import scala.collection.{Map, mutable}
import scala.collection.concurrent.TrieMap
import scala.jdk.OptionConverters._

/**
 * Manages mirror fetcher threads that replicate partitions across cluster mirrors.
 * Partitions from different mirrors are assigned to separate threads to isolate
 * authentication, configuration, and load balancing concerns.
 */
class MirrorFetcherManager(brokerConfig: KafkaConfig,
                           protected val replicaManager: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quotaManager: ReplicationQuotaManager,
                           brokerEpochSupplier: () => Long,
                           metadataCache: MetadataCache,
                           mirrorCache: Option[MirrorLeaderCache] = None)
    extends AbstractFetcherManager[MirrorFetcherThread](
      name = "MirrorFetcherManager brokerId=" + brokerConfig.brokerId,
      clientId = "MirrorReplica",
      numFetchers = brokerConfig.mirrorConfig.numReplicaFetchers) {
  private lazy val mirrorFetcherThreadMap = new mutable.HashMap[MirrorFetcherKey, MirrorFetcherThread]
  private val mirrorOffsetInfoMap = new TrieMap[MirrorLagKey, MirrorOffsetInfo]

  override def deadThreadCount: Int = lock synchronized { mirrorFetcherThreadMap.values.count(_.isThreadFailed) }
  override def minFetchRate: Double = {
    // Current min fetch rate across all fetchers/topics/partitions
    val headRate = mirrorFetcherThreadMap.values.headOption.map(_.fetcherStats.requestRate.oneMinuteRate).getOrElse(0.0)
    mirrorFetcherThreadMap.values.foldLeft(headRate)((curMinAll, fetcherThread) =>
      math.min(curMinAll, fetcherThread.fetcherStats.requestRate.oneMinuteRate))
  }
  override def maxLag: Long = {
    // Current max lag across all fetchers/topics/partitions
    mirrorFetcherThreadMap.values.foldLeft(0L) { (curMaxLagAll, fetcherThread) =>
      val maxLagThread = fetcherThread.fetcherLagStats.stats.values.stream().mapToLong(v => v.lag).max().orElse(0L)
      math.max(curMaxLagAll, maxLagThread)
    }
  }

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): MirrorFetcherThread = {
    throw new UnsupportedOperationException("Use the overload method with mirrorName")
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    if (isClosed) {
      return
    }

    logger.debug("Adding fetchers for partitions; existing fetchers: {}", mirrorFetcherThreadMap.keys)
    // Ensures partitions with different cluster mirrors get separate fetcher threads.
    // This is crucial because different cluster mirrors may require different authentication credentials.
    val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
      MirrorFetcherKey(
        getFetcherId(topicPartition),
        brokerAndInitialFetchOffset.leader,
        brokerAndInitialFetchOffset.mirrorName
      )
    }

    this.synchronized {
      if (isClosed) {
        return
      }

      def addAndStartFetcherThread(fetcherKey: MirrorFetcherKey): MirrorFetcherThread = {
        val fetcherThread = createFetcherThread(fetcherKey.fetcherId, fetcherKey.sourceBroker, fetcherKey.mirrorName)
        mirrorFetcherThreadMap.put(fetcherKey, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((mirrorFetcherKey, initialFetchOffsets) <- partitionsPerFetcher) {
        val fetcherThread = mirrorFetcherThreadMap.get(mirrorFetcherKey) match {
          case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == mirrorFetcherKey.sourceBroker =>
            logger.debug("Reusing fetcher thread for {}", mirrorFetcherKey)
            currentFetcherThread
          case Some(f) =>
            logger.debug("Recreating fetcher thread for {}", mirrorFetcherKey)
            f.shutdown()
            addAndStartFetcherThread(mirrorFetcherKey)
          case None =>
            logger.debug("Creating fetcher thread for {}", mirrorFetcherKey)
            addAndStartFetcherThread(mirrorFetcherKey)
        }
        // Failed partitions are removed when added partitions to thread
        addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)

        // Initialize lag information for newly added partitions
        initialFetchOffsets.foreach { case (topicPartition, _) =>
          val key = MirrorLagKey(mirrorFetcherKey.mirrorName, topicPartition)
          if (!mirrorOffsetInfoMap.contains(key)) {
            // Initialize with current high watermark (or 0) if no cached offset info, until first fetch updates it
            val destinationOffset = replicaManager.getPartition(topicPartition) match {
              case HostedPartition.Online(partition) =>
                partition.log.map(_.highWatermark).getOrElse(0L)
              case _ => 0L
            }
            mirrorOffsetInfoMap.put(key, MirrorOffsetInfo(destinationOffset, destinationOffset, time.milliseconds()))
          }
        }
      }
    }
  }

  private def createFetcherThread(fetcherId: Int, srcEndpoint: BrokerEndPoint, mirrorName: String): MirrorFetcherThread = {
    if (mirrorName.isEmpty) {
      throw new IllegalArgumentException("Mirror name must be provided for remote fetchers")
    }

    val threadName = s"mirror-fetcher-$fetcherId-$mirrorName"
    val logContext = new LogContext(s"[MirrorFetcherThread fetcherId=$fetcherId, srcEndpoint=$srcEndpoint, mirrorName=$mirrorName] ")

    info(s"Creating $threadName")
    val mirrorProperties = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName))
    val mirrorConfig = ClusterMirrorConfig.fromProperties(mirrorProperties, true)
    val sender = new MirrorBlockingSender(srcEndpoint, mirrorConfig, metrics, time, srcEndpoint.id, threadName, logContext)
    val fetchSessionHandler = new FetchSessionHandler(logContext, srcEndpoint.id)
    val endpoint: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, sender, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, () => metadataCache.metadataVersion(), brokerEpochSupplier, isClusterMirror = true,
      mirrorConfig = Some(mirrorConfig))
    val mirrorFetchBackoffMs = mirrorConfig.fetchBackoffMs().toInt
    new MirrorFetcherThread(threadName, endpoint, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, mirrorName, mirrorFetchBackoffMs, mirrorCache)
  }

  override def removeFetcherForPartitions(partitions: scala.collection.Set[TopicPartition]): scala.collection.Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    this.synchronized {
      for ((fetcherKey, fetcher) <- mirrorFetcherThreadMap) {
        val removed = fetcher.removePartitions(partitions)
        fetchStates ++= removed
        // Remove lag cache entries for partitions that were actually removed
        for (partition <- removed.keys) {
          val lagKey = MirrorLagKey(fetcherKey.mirrorName, partition)
          val lisr = metadataCache.getLeaderAndIsr(partition.topic(), partition.partition())
          if (lisr.isPresent) {
            // Only remove the mirror offset info if the leadership has changed.
            if (lisr.get().leader() != brokerConfig.nodeId) {
              mirrorOffsetInfoMap.remove(lagKey)
            }
          } else {
            mirrorOffsetInfoMap.remove(lagKey)
          }
        }
      }
      failedPartitions.removeAll(partitions)
    }
    if (fetchStates.nonEmpty)
      logger.info("Removed fetcher threads for partitions: {}", fetchStates.keySet)
    fetchStates
  }

  // Collect idle fetchers under lock, shut down outside to avoid deadlock
  override def shutdownIdleFetcherThreads(): Unit = {
    val idleFetchers = this.synchronized {
      val keysToBeRemoved = new mutable.HashSet[MirrorFetcherKey]
      val fetchersToShutdown = new mutable.ArrayBuffer[MirrorFetcherThread]
      for ((key, fetcher) <- mirrorFetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetchersToShutdown += fetcher
          keysToBeRemoved += key
        }
      }
      mirrorFetcherThreadMap --= keysToBeRemoved
      fetchersToShutdown
    }
    idleFetchers.foreach(_.shutdown())
  }

  override def resizeThreadPool(newSize: Int): Unit = {
    val excessThreads = new mutable.ArrayBuffer[MirrorFetcherThread]()
    this.synchronized {
      if (isClosed) return
      val currentSize = updateNumFetchers(newSize)
      if (newSize == currentSize) return
      logger.info("Resizing fetcher thread pool from {} to {}", currentSize, newSize)
      val allPartitions = mutable.Map[TopicPartition, InitialFetchState]()
      for ((key, thread) <- mirrorFetcherThreadMap) {
        val partitionStates = thread.removeAllPartitions()
        if (key.fetcherId >= newSize) {
          thread.initiateShutdown()
          excessThreads += thread
        }
        partitionStates.foreachEntry { (topicPartition, state) =>
          allPartitions += topicPartition -> InitialFetchState(state.topicId.toScala,
            thread.leader.brokerEndPoint(),
            currentLeaderEpoch = state.currentLeaderEpoch,
            initOffset = state.fetchOffset,
            mirrorName = state.mirrorName())
        }
      }
      mirrorFetcherThreadMap.filterInPlace((key, _) => key.fetcherId < newSize)
      addFetcherForPartitions(allPartitions)
    }
    shutdownIdleFetcherThreads()
    excessThreads.foreach(_.shutdown())
  }

  override def closeAllFetchers(): Unit = {
    val fetchers = this.synchronized {
      isClosed = true
      val all = mirrorFetcherThreadMap.values.toSeq
      all.foreach(_.initiateShutdown())
      mirrorFetcherThreadMap.clear()
      all
    }
    fetchers.foreach(_.shutdown())
  }

  def updateOffsetInfo(mirrorName: String, topicPartition: TopicPartition, sourceOffset: Long, destinationOffset: Long): Unit = {
    val key = MirrorLagKey(mirrorName, topicPartition)
    mirrorOffsetInfoMap.put(key, MirrorOffsetInfo(sourceOffset, destinationOffset, time.milliseconds()))
  }

  def getOffsetInfo(mirrorName: String): Map[TopicPartition, MirrorOffsetInfo] = {
    mirrorOffsetInfoMap.collect {
      case (key, info) if key.mirrorName == mirrorName => key.topicPartition -> info
    }.toMap
  }

  def removeFetchersForMirror(mirrorName: String): Unit = {
    this.synchronized {
      val affectedPartitions = mirrorFetcherThreadMap
        .filter(_._1.mirrorName == mirrorName)
        .values
        .flatMap(_.partitions)
        .toSet
      if (affectedPartitions.nonEmpty) {
        logger.info("Removing fetcher threads for mirror {}: {} affected partitions", mirrorName, affectedPartitions.size)
        removeFetcherForPartitions(affectedPartitions)
      }
    }
  }

  def shutdown(): Unit = {
    logger.info("Shutting down")
    closeAllFetchers()
    mirrorOffsetInfoMap.clear()
    logger.info("Shutdown completed")
  }
}

/**
 * Three-dimensional key for grouping mirror fetcher threads.
 * <p>
 * Multiple partitions share the same fetcher thread when they
 * have identical keys (Fetcher ID, Source Broker, Mirror Name).
 * <p>
 * Example with num.mirror.replica.fetchers = 2:
 * <pre>
 * | Partition  | Fetcher ID | Source Broker | Mirror Name | Thread |
 * |------------|------------|---------------|-------------|--------|
 * | topic1-p0  | 0          | broker-1      | a-to-b      | New    |
 * | topic1-p1  | 1          | broker-1      | a-to-b      | New    |
 * | topic2-p0  | 0          | broker-1      | a-to-b      | Reuse  |
 * | topic2-p1  | 1          | broker-1      | a-to-b      | Reuse  |
 * | topic3-p0  | 0          | broker-2      | a-to-b      | New    |
 * | topic4-p0  | 0          | broker-1      | a-to-c      | New    |
 * </pre>
 */
case class MirrorFetcherKey(fetcherId: Int, sourceBroker: BrokerEndPoint, mirrorName: String)
case class MirrorLagKey(mirrorName: String, topicPartition: TopicPartition)
case class MirrorOffsetInfo(sourceOffset: Long, destinationOffset: Long, lastUpdateMs: Long)
