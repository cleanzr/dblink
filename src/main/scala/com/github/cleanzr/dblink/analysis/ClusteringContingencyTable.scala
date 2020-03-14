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

package com.github.cleanzr.dblink.analysis

import ClusteringContingencyTable.ContingencyTableRow
import com.github.cleanzr.dblink.Cluster
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

case class ClusteringContingencyTable(table: Dataset[ContingencyTableRow],
                                      size: Long) {
  def persist(newLevel: StorageLevel) {
    table.persist(newLevel)
  }

  def unpersist() {
    table.unpersist()
  }
}

object ClusteringContingencyTable {
  case class ContingencyTableRow(PredictedUID: Long, TrueUID: Long, NumCommonElements: Int)

  // Note: this is a sparse representation of the contingency table. Cluster
  // pairs which are not present in the table have no common elements.
  def apply(predictedClusters: Dataset[Cluster], trueClusters: Dataset[Cluster]): ClusteringContingencyTable = {
    val spark = predictedClusters.sparkSession
    import spark.implicits._
    // Convert to membership representation
    val predictedMembership = predictedClusters.toMembership
    val trueMembership = trueClusters.toMembership

    val predictedSize = predictedMembership.count()
    val trueSize = trueMembership.count()

    // Ensure that clusterings partition the same set of elements (continue checking after join...)
    if (predictedSize != trueSize) throw new Exception("Clusterings do not partition the same set of elements.")

    val joined = predictedMembership.rdd.join(trueMembership.rdd).persist()
    val joinedSize = joined.count()

    // Continued checking...
    if (trueSize != joinedSize) throw new Exception("Clusterings do not partition the same set of elements.")

    val table = joined.map{case (_, (predictedUID, trueUID)) => ((predictedUID, trueUID), 1)}
      .reduceByKey(_ + _)
      .map{case ((predictedUID, trueUID), count) => ContingencyTableRow(predictedUID, trueUID, count)}
      .toDS()

    joined.unpersist()

    ClusteringContingencyTable(table, trueSize)
  }
}