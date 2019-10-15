// Copyright (C) 2018  Australian Bureau of Statistics
//
// Author: Neil Marchant
//
// This file is part of dblink.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package com.github.cleanzr.dblink.partitioning

import LPTScheduler._

import scala.Numeric.Implicits._
import scala.Ordering.Implicits._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Implementation of the longest processing time (LPT) algorithm for
  * partitioning jobs across multiple processors
  * @param jobs
  * @param numPartitions
  * @tparam J job index
  * @tparam T job runtime
  */
class LPTScheduler[J, T : ClassTag : Numeric](jobs: IndexedSeq[(J,T)],
                                val numPartitions: Int) extends Serializable {

  val numJobs: Int = jobs.size
  private val partitions: Array[Partition[J, T]] = partitionJobs[J, T](jobs, numPartitions)
  val partitionSizes: Array[T] = partitions.iterator.map[T](_.size).toArray[T]
  private val partitionsIndex = indexPartitions(partitions)

  def getJobs(partitionId: Int): Seq[J] = partitions(partitionId).jobs

  def getSize(partitionId: Int): T = partitionSizes(partitionId)

  def getPartitionId(jobId: J): Int = partitionsIndex(jobId)
  // TODO: deal with non-existent jobId
}

object LPTScheduler {
  case class Partition[J, T : ClassTag : Numeric](var size: T, jobs: ArrayBuffer[J])

  private def partitionJobs[J, T : ClassTag : Numeric](jobs: IndexedSeq[(J, T)],
                                          numPartitions: Int): Array[Partition[J, T]] = {
    val sortedJobs = jobs.sortBy(-_._2)

    // Initialise array of Partitions
    val partitions = Array.fill[Partition[J, T]](numPartitions) {
      Partition(implicitly[Numeric[T]].zero, ArrayBuffer[J]())
    }

    // Loop through jobs, putting each in the partition with the smallest size so far
    sortedJobs.foreach {j =>
      val (_, smallestId) = partitions.zipWithIndex.foldLeft ((partitions.head, 0) ) {
        (xO, xN) => if (xO._1.size > xN._1.size) xN else xO
      }
      partitions(smallestId).size += j._2
      partitions(smallestId).jobs += j._1
    }

    partitions
  }

  private def indexPartitions[J, T : Numeric](partitions: Array[Partition[J,T]]): Map[J, Int]  = {
    val bIndex = Map.newBuilder[J, Int]
    partitions.zipWithIndex.foreach { case (partition, idx) =>
      partition.jobs.foreach(item => bIndex += (item -> idx))
    }
    bIndex.result()
  }
}