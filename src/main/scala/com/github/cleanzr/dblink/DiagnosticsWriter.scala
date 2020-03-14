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

package com.github.cleanzr.dblink

import com.github.cleanzr.dblink.util.BufferedFileWriter
import org.apache.spark.SparkContext

/** Buffered writer for diagnostics along the Markov chain
  *
  * Output is in CSV format
  *
  * @param path path to save diagnostics (can be a HDFS or local path)
  * @param continueChain whether to append to existing diagnostics (if present)
  */
class DiagnosticsWriter(path: String, continueChain: Boolean)
                       (implicit sparkContext: SparkContext) extends Logging {
  private val writer = BufferedFileWriter(path, continueChain, sparkContext)
  private var firstWrite = true
  info(s"Writing diagnostics along chain to $path.")

  /** Write header for CSV */
  private def writeHeader(state: State): Unit = {
    val recordsCache = state.bcRecordsCache.value
    val aggDistortionsHeaders = recordsCache.indexedAttributes.map(x => s"aggDist-${x.name}").mkString(",")
    val recDistortionsHeaders = (0 to recordsCache.numAttributes).map(k => s"recDistortion-$k").mkString(",")
    writer.write(s"iteration,systemTime-ms,numObservedEntities,logLikelihood,$aggDistortionsHeaders,$recDistortionsHeaders\n")
  }

  /** Write a row of diagnostics for the given state */
  def writeRow(state: State): Unit = {
    if (firstWrite && !continueChain) writeHeader(state); firstWrite = false

    // Get number of attributes
    val numAttributes = state.bcRecordsCache.value.numAttributes

    // Aggregate number of distortions for each attribute (sum over fileId)
    val aggAttrDistortions = state.summaryVars.aggDistortions
      .groupBy(_._1._1) // group by attrId
      .mapValues(_.values.sum) // sum over fileId

    // Convenience variable
    val recDistortions = state.summaryVars.recDistortions

    // Build row of string values matching header
    val row: Iterator[String] = Iterator(
        state.iteration.toString,                                                          // iteration
        System.currentTimeMillis().toString,                                               // systemTime-ms
        (state.bcParameters.value.populationSize - state.summaryVars.numIsolates).toString,   // numObservedEntities
        f"${state.summaryVars.logLikelihood}%.9e") ++                                      // logLikelihood
        (0 until numAttributes).map(aggAttrDistortions.getOrElse(_, 0L).toString) ++       // aggDist-*
        (0 to numAttributes).map(recDistortions.getOrElse(_, 0L).toString)                 // recDistortion-*

    writer.write(row.mkString(",") + "\n")
  }

  def close(): Unit = writer.close()

  def flush(): Unit = writer.flush()

  /** Need to call this occasionally to keep the writer alive */
  def progress(): Unit = writer.progress()
}