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

package com.github.ngmarchant.dblink.analysis

object BinaryClassificationMetrics {
  def precision(binaryConfusionMatrix: BinaryConfusionMatrix): Double = {
    binaryConfusionMatrix.numTruePositives.toDouble / binaryConfusionMatrix.numPredictedPositives
  }

  def recall(binaryConfusionMatrix: BinaryConfusionMatrix): Double = {
    binaryConfusionMatrix.numTruePositives.toDouble / binaryConfusionMatrix.numPositives
  }

  def fMeasure(binaryConfusionMatrix: BinaryConfusionMatrix,
               beta: Double = 1.0): Double = {
    val betaSq = beta * beta
    val pr = precision(binaryConfusionMatrix)
    val re = recall(binaryConfusionMatrix)
    (1 + betaSq) * pr * re / (betaSq * pr + re)
  }
}
