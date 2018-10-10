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

package com.github.ngmarchant.dblink

import com.github.ngmarchant.dblink.util.PeriodicRDDCheckpointer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Sampler extends Logging {

  /** Generates posterior samples by successively applying the Markov
    * transition operator starting from a given initial state. The samples are
    * written to disk on the executors.
    *
    * @param initialState initial state.
    * @param sampleSize number of samples to keep (after burn-in and thinning).
    * @param burninInterval burn-in interval.
    * @param thinningInterval thinning interval.
    * @param checkpointInterval
    * @param writeBufferSize
    * @param savePath
    * @return the final state of the Markov chain.
    */
  def sample(initialState: State,
             sampleSize: Int,
             savePath: String,
             burninInterval: Int = 0,
             thinningInterval: Int = 1,
             checkpointInterval: Int = 50,
             writeBufferSize: Int = 10,
             collapsedEntityIds: Boolean = false,
             collapsedEntityValues: Boolean = true): State = {
    require(burninInterval >= 0, "`burninInterval` must be non-negative.")
    require(thinningInterval > 0, "`thinningInterval` must be positive.")
    require(checkpointInterval >= 0, "`checkpointInterval` must be non-negative.")
    require(sampleSize > 0, "`sampleSize` must be positive.")
    require(writeBufferSize > 0, "`writeBufferSize` must be positive.")

    var sampleCtr = 0
    var state = initialState
    val startIteration = initialState.iteration

    // TODO: ensure that savePath is a directory

    val continueChain = startIteration != 0
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext

    val linkagePath = savePath + "linkage-chain.parquet"
    var linkageWriter = new LinkageStructureWriter(linkagePath, continueChain, writeBufferSize)
    val diagnosticsPath = savePath + "diagnostics.csv"
    val diagnosticsWriter = new DiagnosticsWriter(diagnosticsPath, continueChain)

    if (!continueChain && burninInterval == 0) {
      /** Need to record initial state */
      linkageWriter.write(state)
      diagnosticsWriter.writeRow(state)
    }

    val cp = new PeriodicRDDCheckpointer[(PartitionId, EntRecPair)](checkpointInterval, sc)

    if (burninInterval > 0) info(s"Running burn-in for $burninInterval iterations.")
    while (sampleCtr < sampleSize) {
      val completedIterations = state.iteration - startIteration

      state = state.nextState(checkpointer = cp, collapsedEntityIds, collapsedEntityValues)
      //newState.partitions.persist(StorageLevel.MEMORY_ONLY_SER)
      //state.partitions.unpersist()
      //state = newState

      if (completedIterations == burninInterval) {
        if (burninInterval > 0) info("Burn-in complete.")
        info(s"Generating $sampleSize sample(s) with thinningInterval=$thinningInterval.")
      }

      if (completedIterations >= burninInterval) {
        if ((completedIterations - burninInterval)%thinningInterval == 0) {
          linkageWriter.write(state)
          diagnosticsWriter.writeRow(state)
          sampleCtr += 1
        }
      }

      diagnosticsWriter.progress() // ensure that diagnosticsWriter is kept alive
    }

    info("Sampling complete. Writing final state and remaining samples to disk.")
    linkageWriter.close()
    diagnosticsWriter.close()
    state.save(savePath)
    info(s"Finished writing to disk at $savePath")
    state
  }
}
