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

package com.github.ngmarchant.dblink

import com.github.ngmarchant.dblink.util.BufferedFileWriter
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

    /** Aggregate number of distortions over the files */
    val aggDistortions = state.summaryVars.aggDistortions
      .groupBy(_._1._1) // group by attrId
      .mapValues(_.values.sum) // sum over fileId
      .toArray
      .sortBy(_._1)
      .map(_._2.toString)

    val numAttributes = aggDistortions.length
    val recDistortions = (0 to numAttributes).map(state.summaryVars.recDistortions.getOrElse(_, 0L).toString)

    /** Build row of string values matching header*/
    val row: Iterator[String] = Iterator(state.iteration.toString,
      System.currentTimeMillis().toString,
      (state.bcParameters.value.numEntities - state.summaryVars.numIsolates).toString,
      f"${state.summaryVars.logLikelihood}%.9e") ++ aggDistortions ++
        recDistortions

    writer.write(row.mkString(",") + "\n")
  }

  def close(): Unit = writer.close()

  def flush(): Unit = writer.flush()

  /** Need to call this occasionally to keep the writer alive */
  def progress(): Unit = writer.progress()
}