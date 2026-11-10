/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.mirror

import kafka.cluster.Partition
import kafka.server._
import kafka.server.mirror.MirrorMetadataManager.LEADER_EPOCH_BUMP_THRESHOLD
import org.apache.kafka.common.errors.{MirrorLeaderEpochExceededException, MirrorPartitionStaleMetadataException}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.{Node, TopicPartition}
import kafka.server.mirror.MirrorLeaderCache.SourceClusterLeader
import org.apache.kafka.server.mirror.MirrorPartitionState
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState}
import org.apache.kafka.storage.internals.log.{LogAppendInfo, LogStartOffsetIncrementReason}

import java.util.Optional
import scala.collection.{Map, Set}
import scala.jdk.CollectionConverters.SetHasAsJava

/**
 * Fetcher thread for cross-cluster replication.
 * Extends AbstractFetcherThread with mirror-specific handling.
 */
class MirrorFetcherThread(name: String,
                          leader: LeaderEndPoint,
                          failedPartitions: FailedPartitions,
                          replicaMgr: ReplicaManager,
                          quota: ReplicaQuota,
                          logPrefix: String,
                          mirrorName: String,
                          mirrorFetchBackoffMs: Int,
                          mirrorCache: Option[MirrorLeaderCache] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false),
                                fetchBackOffMs = mirrorFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats,
                                mirrorName) {
  this.logIdent = logPrefix

  // Processes fetched data
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionLeaderEpoch: Int,
                                    partitionData: FetchResponseData.PartitionData): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))

    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    if (logTrace)
      trace(s"Appending records for partition $topicPartition: log end offset=${log.logEndOffset}, " +
        s"record bytes=${records.sizeInBytes}, leader high watermark=${partitionData.highWatermark}")

    validateLeaderEpoch(topicPartition, partition, records, partitionLeaderEpoch)

    // Append batches from the source cluster to the destination partition's log.
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, partitionLeaderEpoch)

    if (logTrace)
      trace(s"Appended records for partition $topicPartition: log end offset=${log.logEndOffset}, record bytes=${records.sizeInBytes}")

    val leaderLogStartOffset = partitionData.logStartOffset

    // This works as producer write with acks=1. The leader node will append data into log without HW incremented.
    // The leader's HW will be incremented only when all ISR (at least minISR) are caught up.
    if (!partition.maybeIncrementLeaderHWWithLock(log)) {
      trace(s"Could not update replica high watermark for partition $topicPartition (leader high watermark=${partitionData.highWatermark})")
    }

    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)

    // Update mirroring lag
    replicaMgr.updateMirrorOffsetInfo(mirrorName, topicPartition, partitionData.highWatermark, log.highWatermark)

    // Account for replication quota
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  // Validates source leader epoch against destination leader epoch
  private def validateLeaderEpoch(topicPartition: TopicPartition,
                                  partitionState: Partition,
                                  sourceRecords: Records,
                                  fetchLeaderEpoch: Int): Unit = {
    // Three leader epoch values to validate:
    // - destinationLeaderEpoch: current destination epoch
    // - sourceLeaderEpochFromLastBatch: epoch of the most recent batch in fetched records
    // - fetchLeaderEpoch: fetch state epoch (source epoch known to destination)
    val destinationLeaderEpoch = partitionState.getLeaderEpoch
    val sourceLeaderEpochFromLastBatch = if (sourceRecords.lastBatch().isPresent)
      sourceRecords.lastBatch().get().partitionLeaderEpoch() else -1
    log.trace(s"Validating leader epoch for partition $topicPartition. " +
      s"Source leader epoch=$sourceLeaderEpochFromLastBatch, " +
      s"destination leader epoch=$destinationLeaderEpoch, " +
      s"fetch leader epoch=$fetchLeaderEpoch")

    // If records are from a future epoch relative to destination, fence the partition.
    // This prevents the destination from accepting records that would violate leader epoch invariant (DLE >= SLE).
    if (sourceLeaderEpochFromLastBatch > destinationLeaderEpoch) {
      throw new MirrorLeaderEpochExceededException(s"Source leader epoch $sourceLeaderEpochFromLastBatch " +
        s"exceeds destination leader epoch $destinationLeaderEpoch for partition $topicPartition")
    } else {
      replicaMgr.mirrorManager.foreach { mmm =>
        mmm.maybeClearFailedStateAndPersist(mirrorName, topicPartition)

        // Proactively bump destination's epoch if batches are approaching the current epoch.
        // This avoids fencing events from natural leadership changes on source.
        if (sourceLeaderEpochFromLastBatch > destinationLeaderEpoch - LEADER_EPOCH_BUMP_THRESHOLD) {
          mmm.scheduleBumpLeaderEpoch(partitionState.getMirrorName().get(), topicPartition)
            .whenComplete { (_, ex) =>
              if (ex != null) log.warn(s"Failed to bump leader epoch for partition $topicPartition", ex)
            }
        }
      }
    }

    if (sourceLeaderEpochFromLastBatch > fetchLeaderEpoch) {
      // In older Kafka versions, the fetch response does not include the current source leader epoch.
      // After a source leadership change, the response can therefore contain batches from a newer epoch than the one
      // known to the fetcher from source metadata.
      //
      // KAFKA-18723 changed replication to reject batches whose leader epoch is newer than the epoch in the fetch request.
      // Without refreshing source metadata, the fetcher may repeatedly receive and reject these batches.
      // Treat this as stale source metadata signal and refresh it to discover the new source leader epoch.
      throw new MirrorPartitionStaleMetadataException(s"Batch epoch $sourceLeaderEpochFromLastBatch exceeds partition " +
        s"leader epoch $fetchLeaderEpoch for partition $topicPartition; refreshing source metadata")
    }
  }

  override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateTo(truncationState.offset, isFuture = false)
  }

  override def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  override def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.latestEpoch
  }

  override def latestEpochFromLog(topicPartition: TopicPartition): Optional[Integer] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.latestEpochFromLog()
  }

  override def logStartOffset(topicPartition: TopicPartition): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logStartOffset
  }

  override def logEndOffset(topicPartition: TopicPartition): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logEndOffset
  }

  override def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.endOffsetForEpoch(epoch)
  }

  override protected def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    replicaMgr.mirrorFetcherManager.removeFetcherForPartitions(partitions)
  }

  override protected def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    mirrorCache.foreach { cache =>
      partitionAndOffsets.foreach { case (tp, state) =>
        cache.updateSourceClusterLeader(mirrorName, tp,
          new SourceClusterLeader(Optional.of(new Node(state.leader.id(),
            state.leader.host(), state.leader.port())), state.currentLeaderEpoch))
      }
    }
    replicaMgr.mirrorFetcherManager.addFetcherForPartitions(partitionAndOffsets)
  }

  override def updateMirrorSourceLeader(mirrorName: String,
                                        partition: TopicPartition,
                                        leaderNode: Optional[Node],
                                        leaderEpoch: Int): Unit = {
    mirrorCache.foreach(cache => {
      val currentLeader = cache.getSourceClusterLeader(mirrorName, partition)
      // The leader node might be empty when election is in process
      val node: Optional[Node] = if (leaderNode.isPresent)
        leaderNode
      else if (currentLeader.isPresent && currentLeader.get().node().isPresent)
        currentLeader.get().node()
      else
        Optional.empty()

      // Use the highest leader epoch known
      val epoch = if (currentLeader.isPresent && currentLeader.get().leaderEpoch() > leaderEpoch)
        currentLeader.get().leaderEpoch()
      else
        leaderEpoch

      cache.updateSourceClusterLeader(mirrorName, partition, new SourceClusterLeader(node, epoch))
    })
  }

  override protected def refreshMirrorSourceMetadata(mirrorPartitions: Set[TopicPartition], reason: String): Unit = {
    replicaMgr.mirrorManager.foreach(_.scheduleOnceSourceTopicMetadataSync(mirrorName))
    replicaMgr.mirrorManager.foreach(_.transitionTo(mirrorName, mirrorPartitions.asJava,
      MirrorPartitionState.FAILED, reason, false))
  }

  override protected def maybeWaitForMirrorConvergence(mirrorPartitions: Set[TopicPartition]): Unit = {
    removeFetcherForPartitions(mirrorPartitions)
    val uleEnabledPartitions = mirrorPartitions.filter(tp =>
      replicaMgr.getLog(tp).get.config().mirrorUncleanLeaderElectionEnable).toSet
    val uleDisabledPartitions = mirrorPartitions.filter(tp =>
      !replicaMgr.getLog(tp).get.config().mirrorUncleanLeaderElectionEnable).toSet
    if (uleEnabledPartitions.nonEmpty) {
      replicaMgr.mirrorManager.foreach(_.transitionTo(mirrorName, uleEnabledPartitions.asJava,
        MirrorPartitionState.ULE_RECOVERY, null, false))
    }
    if (uleDisabledPartitions.nonEmpty) {
      // Move the state to terminal FAILED state.
      replicaMgr.mirrorManager.foreach(_.transitionTo(mirrorName, uleDisabledPartitions.asJava,
        MirrorPartitionState.FAILED, "Detected log truncation during mirroring. This implies unclean leader election " +
          "in source cluster, but mirror.unclean.leader.election.enable is disabled. Moving to FAILED state.", true))
    }
  }

  override protected def handleMirrorPartitionFailed(topicPartition: TopicPartition, reason: String): Unit = {
    replicaMgr.mirrorManager.foreach(_.transitionTo(mirrorName, java.util.Set.of(topicPartition),
      MirrorPartitionState.FAILED, reason, false))
  }

  // Source leader epoch exceeds local epoch: transition to EPOCH_FENCING to bump the
  // local epoch before allowing further appends. If the bump fails, the coordinator
  // transitions to FAILED and the exponential backoff retry takes over.
  override protected def handleMirrorLeaderEpochExceeded(mirrorName: String, topicPartition: TopicPartition): Unit = {
    replicaMgr.mirrorManager.foreach(_.transitionTo(mirrorName, java.util.Set.of(topicPartition),
      MirrorPartitionState.EPOCH_FENCING, null, false))
  }

  override def mirrorLeaderEpochFromSource(tp: TopicPartition): Option[Int] = {
    mirrorCache.flatMap(cache => {
      val sourceLeader = cache.getSourceClusterLeader(mirrorName, tp)
      if (sourceLeader.isPresent) Some(sourceLeader.get().leaderEpoch())
      else None
    })
  }

  // Returns the mirror partition lag computed from cached source/destination offsets
  override def partitionLag(topicPartition: TopicPartition, leaderHW: Long, nextOffset: Long, mirrorName: String): Long = {
    replicaMgr.mirrorFetcherManager.getOffsetInfo(mirrorName).get(topicPartition).map { info =>
      Math.max(0, info.sourceOffset - info.destinationOffset)
    }.getOrElse(0L)
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      try {
        leader.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Error initiating close of leader endpoint for fetcher thread $name", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    try {
      leader.close()
    } catch {
      case t: Throwable =>
        error(s"Error closing leader endpoint for fetcher thread $name", t)
    }
  }
}
