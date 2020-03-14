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

import com.github.cleanzr.dblink.Cluster
import org.apache.commons.math3.util.CombinatoricsUtils.binomialCoefficient
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

case class ClusteringMetrics(adjRandIndex: Double) {
  def mkString: String = {
    "=====================================\n" +
    "          Cluster metrics            \n" +
    "-------------------------------------\n" +
    s" Adj. Rand index: $adjRandIndex\n" +
    "=====================================\n"
  }

  def print(): Unit = {
    Console.print(mkString)
  }
}

object ClusteringMetrics {
  private def comb2(x: Int): Long = if (x >= 2) binomialCoefficient(x,2) else 0

  def AdjustedRandIndex(contingencyTable: ClusteringContingencyTable): Double = {
    // Compute sum_{PredictedUID} comb2(sum_{TrueUID} NumCommonElements(PredictedUID, TrueUID))
    val predCombSum = contingencyTable.table.rdd
      .map(row => (row.PredictedUID, row.NumCommonElements))
      .reduceByKey(_ + _)     // sum over True UID
      .aggregate(0L)(
        seqOp = (sum, x) => sum + comb2(x._2),
        combOp = _ + _        // apply comb2(.) and sum over PredictedUID
      )

    // Compute sum_{TrueUID} comb2(sum_{PredictedUID} NumCommonElements(PredictedUID, TrueUID))
    val trueCombSum = contingencyTable.table.rdd
      .map(row => (row.TrueUID, row.NumCommonElements))
      .reduceByKey(_ + _)     // sum over Pred UID
      .aggregate(0L)(
        seqOp = (sum, x) => sum + comb2(x._2),
        combOp = _ + _        // apply comb2(.) and sum over True UID
      )

    // Compute sum_{TrueUID} sum_{PredictedUID} comb2(NumCommonElements(PredictedUID, TrueUID))
    val totalCombSum = contingencyTable.table.rdd
      .aggregate(0L)(
        seqOp = (sum, x) => sum + comb2(x.NumCommonElements),
        combOp = _ + _          // apply comb2(.) and sum over PredictedUID & TrueUID
      )

    // Return adjusted Rand index
    val expectedIndex = predCombSum.toDouble * trueCombSum / comb2(contingencyTable.size.toInt)
    val maxIndex = (predCombSum.toDouble + trueCombSum) / 2.0
    (totalCombSum - expectedIndex)/(maxIndex - expectedIndex)
  }

  def apply(predictedClusters: Dataset[Cluster],
            trueClusters: Dataset[Cluster]): ClusteringMetrics = {
    val contingencyTable = ClusteringContingencyTable(predictedClusters, trueClusters)
    contingencyTable.persist(StorageLevel.MEMORY_ONLY)
    val adjRandIndex = AdjustedRandIndex(contingencyTable)
    contingencyTable.unpersist()
    ClusteringMetrics(adjRandIndex)
  }
}
