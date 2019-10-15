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

import org.apache.spark.rdd.RDD

case class BinaryConfusionMatrix(numTruePositives: Long,
                                 numFalsePositives: Long,
                                 numFalseNegatives: Long) {
  val numPositives: Long = numTruePositives + numFalseNegatives
  val numPredictedPositives: Long = numTruePositives + numFalsePositives
}

object BinaryConfusionMatrix {
  def apply(predictionsAndLabels: Seq[(Boolean, Boolean)]): BinaryConfusionMatrix = {
    var TP = 0L
    var FP = 0L
    var FN = 0L
    predictionsAndLabels.foreach {case (prediction, label) =>
      if (prediction & label) TP += 1L
      if (prediction & !label) FP += 1L
      if (!prediction & label) FN += 1L
    }
    BinaryConfusionMatrix(TP, FP, FN)
  }

  def apply(predictionsAndLabels: RDD[(Boolean, Boolean)]): BinaryConfusionMatrix = {
    val sc = predictionsAndLabels.sparkContext
    val accTP = sc.longAccumulator("TP")
    val accFP = sc.longAccumulator("FP")
    val accFN = sc.longAccumulator("FN")
    predictionsAndLabels.foreach {case (prediction, label) =>
      if (prediction & label) accTP.add(1L)
      if (prediction & !label) accFP.add(1L)
      if (!prediction & label) accFN.add(1L)
    }
    BinaryConfusionMatrix(accTP.value, accFP.value, accFN.value)
  }
}