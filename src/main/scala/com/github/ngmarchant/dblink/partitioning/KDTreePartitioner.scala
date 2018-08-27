// Copyright (C) 2018  Neil Marchant
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

package com.github.ngmarchant.dblink.partitioning

import com.github.ngmarchant.dblink.accumulators.MapDoubleAccumulator
import com.github.ngmarchant.dblink.partitioning.KDTreePartitioner.getNewSplits
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.github.ngmarchant.dblink.Logging

class KDTreePartitioner[T : Ordering](numLevels: Int,
                                      attributeIds: Traversable[Int]) extends PartitionFunction[T] {
  require(numLevels >= 0, "`numLevels` must be non-negative.")
  if (numLevels > 0) require(attributeIds.nonEmpty, "`attributeIds` must be non-empty if `numLevels` > 0")

  override def numPartitions: Int = tree.numLeaves

  private val tree = new MutableBST[T]

  override def fit(records: RDD[Array[T]]): Unit = {
    if (numLevels > 0) require(attributeIds.nonEmpty, "non-empty list of attributes is required to build the tree.")

    val sc = records.sparkContext

    var level = 0
    var itAttributeIds = attributeIds.toIterator
    while (level < numLevels) {
      /** Go back to the beginning of `attributeIds` if we reach the end */
      val attrId = if (itAttributeIds.hasNext) itAttributeIds.next() else {
        itAttributeIds = attributeIds.toIterator // reset
        itAttributeIds.next()
      }
      KDTreePartitioner.info(s"Splitting on attribute $attrId at level $level.")
      val bcTree = sc.broadcast(tree)
      val newLevel = getNewSplits(records, bcTree, attrId)
      newLevel.foreach { case (nodeId, splitter) =>
        if (splitter.splitQuality <= 0.9)
          KDTreePartitioner.warn(s"Poor quality split (${splitter.splitQuality*100}%) at node $nodeId.")
        tree.splitNode(nodeId, attrId, splitter)
      }
      level += 1
    }
  }

  override def getPartitionId(attributes: Array[T]): Int = tree.getLeafNumber(attributes)

  override def mkString: String = {
    if (numLevels == 0) s"KDTreePartitioner(numLevels=0)"
    else s"KDTreePartitioner(numLevels=$numLevels, attributeIds=${attributeIds.mkString("[",",","]")})"
  }
}

object KDTreePartitioner extends Logging {

  def apply[T : Ordering](numLevels: Int, attributeIds: Traversable[Int]): KDTreePartitioner[T] = {
    new KDTreePartitioner(numLevels, attributeIds)
  }

  def apply[T : Ordering](): KDTreePartitioner[T] = {
    new KDTreePartitioner(0, Traversable())
  }

  private def getNewSplits[T : Ordering](records: RDD[Array[T]],
                                         bcTree: Broadcast[MutableBST[T]],
                                         attributeId: Int): Map[Int, DomainSplitter[T]] = {
    val sc = records.sparkContext

    val acc = new MapDoubleAccumulator[(Int, T)]
    sc.register(acc, s"tree level builder")

    /** Iterate over the records counting the number of occurrences of each
      * attribute value per node */
    records.foreachPartition { partition =>
      val tree = bcTree.value
      partition.foreach { attributes =>
        val attributeValue = attributes(attributeId)
        val nodeId = tree.getLeafNodeId(attributes)
        acc.add((nodeId, attributeValue), 1L)
      }
    }

    /** Group by node and compute the splitters for each node */
    acc.value.toArray
      .groupBy(_._1._1)
      .mapValues { x =>
        DomainSplitter[T](x.map { case ((_, value), weight) => (value, weight) })
      }
  }
}