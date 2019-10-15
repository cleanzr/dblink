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

package com.github.cleanzr.dblink

import com.github.cleanzr.dblink.accumulators.MapLongAccumulator
import com.github.cleanzr.dblink.accumulators.MapLongAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

/** Collection of accumulators used for computing `SummaryVars`
  *
  * @param logLikelihood double accumulator for the (un-normalised) log-likelihood
  * @param numIsolates long accumulator for the number of isolated latent entities
  * @param aggDistortions map accumulator for the number of distortions per
  *                       file and attribute.
  * @param recDistortions
  */
case class SummaryAccumulators(logLikelihood: DoubleAccumulator,
                               numIsolates: LongAccumulator,
                               aggDistortions: MapLongAccumulator[(AttributeId, FileId)],
                               recDistortions: MapLongAccumulator[Int]) {
  /** Reset all accumulators */
  def reset(): Unit = {
    logLikelihood.reset()
    numIsolates.reset()
    aggDistortions.reset()
    recDistortions.reset()
  }
}

object SummaryAccumulators {
  /** Create a `SummaryAccumulators` object using a given SparkContext.
    *
    * @param sparkContext a SparkContext.
    * @return a `SummaryAccumulators` object.
    */
  def apply(sparkContext: SparkContext): SummaryAccumulators = {
    val logLikelihood = new DoubleAccumulator
    val numIsolates = new LongAccumulator
    val aggDistortions = new MapLongAccumulator[(AttributeId, FileId)]
    val recDistortions = new MapLongAccumulator[Int]
    sparkContext.register(logLikelihood, "log-likelihood (un-normalised)")
    sparkContext.register(numIsolates, "number of isolates")
    sparkContext.register(aggDistortions, "aggregate number of distortions per attribute/file")
    sparkContext.register(recDistortions, "frequency distribution of total record distortion")
    new SummaryAccumulators(logLikelihood, numIsolates, aggDistortions, recDistortions)
  }
}