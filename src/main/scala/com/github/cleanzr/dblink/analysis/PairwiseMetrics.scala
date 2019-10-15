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

case class PairwiseMetrics(precision: Double,
                           recall: Double,
                           f1score: Double) {
  def mkString: String = {
    "=====================================\n" +
    "          Pairwise metrics           \n" +
    "-------------------------------------\n" +
    s" Precision:       $precision\n" +
    s" Recall:          $recall\n" +
    s" F1-score:        $f1score\n" +
    "=====================================\n"
  }

  def print(): Unit = {
    Console.print(mkString)
  }
}

object PairwiseMetrics {
  def LinksConfusionMatrix(predictedLinks: PairwiseLinks,
                           trueLinks: PairwiseLinks): BinaryConfusionMatrix = {
    // Create PairRDDs so we can use the fullOuterJoin function
    val predictedLinks2 = predictedLinks.rdd.map(pair => (pair, true))
    val trueLinks2 = trueLinks.rdd.map(pair => (pair, true))
    val joinedLinks = predictedLinks2.fullOuterJoin(trueLinks2)
      .map { case (_, link) => (link._1.isDefined, link._2.isDefined)}
    BinaryConfusionMatrix(joinedLinks)
  }

  def apply(predictedLinks: PairwiseLinks,
            trueLinks: PairwiseLinks): PairwiseMetrics = {
    val confusionMatrix = LinksConfusionMatrix(predictedLinks, trueLinks)

    val precision = BinaryClassificationMetrics.precision(confusionMatrix)
    val recall = BinaryClassificationMetrics.recall(confusionMatrix)
    val f1score = BinaryClassificationMetrics.fMeasure(confusionMatrix, beta = 1.0)

    PairwiseMetrics(precision, recall, f1score)
  }
}
