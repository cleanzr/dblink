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

import SimplePartitioner._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
  * Block on the values of a single field, then combine to get roughly
  * equal-size partitions (based on empirical distribution)
  *
  * @param attributeId
  * @param numPartitions
  */
class SimplePartitioner[T : ClassTag : Ordering](val attributeId: Int,
                                                 override val numPartitions: Int)
  extends PartitionFunction[T] {

  private var _lpt: LPTScheduler[T, Double] = _

  override def fit(records: RDD[Array[T]]): Unit = {
    val weights = records.map(values => (values(attributeId), 1.0))
      .reduceByKey(_ + _)
      .collect()
    _lpt = generateLPTScheduler(weights, numPartitions)
  }

  override def getPartitionId(values: Array[T]): Int = {
    if (_lpt == null) -1
    else _lpt.getPartitionId(values(attributeId))
  }

  override def mkString: String = s"SimplePartitioner(attributeId=$attributeId, numPartitions=$numPartitions)"
}

object SimplePartitioner {
  private def generateLPTScheduler[T](weights: Array[(T, Double)],
                                      numPartitions: Int): LPTScheduler[T, Double] = {
    new LPTScheduler[T, Double](weights, numPartitions)
  }

  def apply[T : ClassTag : Ordering](attributeId: Int,
                                     numPartitions: Int): SimplePartitioner[T] = {
    new SimplePartitioner(attributeId, numPartitions)
  }
}