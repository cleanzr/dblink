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

package com.github.cleanzr.dblink.analysis

import com.github.cleanzr.dblink.{Cluster, RecordId}
import org.apache.spark.rdd.RDD

object baselines {
  def exactMatchClusters(records: RDD[(RecordId, Seq[String])]): RDD[Cluster] = {
    records.map(row => (row._2.mkString, row._1)) // (values, id)
     .aggregateByKey(Set.empty[RecordId]) (
      seqOp = (recIds, recId) => recIds + recId,
      combOp = (recIdsA, recIdsB) => recIdsA ++ recIdsB
    ).map(_._2)
  }

  /** Generates (overlapping) clusters that are near matches based on attribute agreements/disagreements
    *
    * @param records
    * @param numDisagree 
    * @return
    */
  def nearClusters(records: RDD[(RecordId, Seq[String])], numDisagree: Int): RDD[Cluster] = {
    require(numDisagree >= 0, "`numAgree` must be non-negative")

    records.flatMap { row =>
      val numAttr = row._2.length
      val attrIds = 0 until numAttr
      attrIds.combinations(numDisagree).map { delIds =>
        val partialValues = row._2.zipWithIndex.collect { case (value, attrId) if !delIds.contains(attrId) => value }
        (partialValues.mkString, row._1)
      }
    }.aggregateByKey(Set.empty[RecordId])(
      seqOp = (recIds, recId) => recIds + recId,
      combOp = (recIdsA, recIdsB) => recIdsA ++ recIdsB
    ).map(_._2)
  }
}
