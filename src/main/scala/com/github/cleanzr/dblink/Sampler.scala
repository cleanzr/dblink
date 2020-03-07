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

import com.github.cleanzr.dblink.util.{BufferedRDDWriter, PeriodicRDDCheckpointer}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Sampler extends Logging {

  /** Generates posterior samples by successively applying the Markov transition operator starting from a given
    * initial state. The samples are written to the path provided.
    *
    * @param initialState The initial state of the Markov chain.
    * @param sampleSize A positive integer specifying the desired number of samples (after burn-in and thinning)
    * @param outputPath A string specifying the path to save output (includes samples and diagnostics). HDFS and
    *                   local filesystems are supported.
    * @param burninInterval A non-negative integer specifying the number of initial samples to discard as burn-in.
    *                       The default is 0, which means no burn-in is applied.
    * @param thinningInterval A positive integer specifying the period for saving samples to disk. The default value is
    *                         1, which means no thinning is applied.
    * @param checkpointInterval A non-negative integer specifying the period for checkpointing. This prevents the
    *                           lineage of the RDD (internal to state) from becoming too long. Smaller values require
    *                           more frequent writing to disk, larger values require more CPU/memory. The default
    *                           value of 20, is a reasonable trade-off.
    * @param writeBufferSize A positive integer specifying the number of samples to queue in memory before writing to
    *                        disk.
    * @param collapsedEntityIds A Boolean specifying whether to collapse the distortions when updating the entity ids.
    *                           Defaults to false.
    * @param collapsedEntityValues A Boolean specifying whether to collapse the distotions when updating the entity
    *                              values. Defaults to true.
    * @return The final state of the Markov chain.
    */
  def sample(initialState: State,
             sampleSize: Int,
             outputPath: String,
             burninInterval: Int = 0,
             thinningInterval: Int = 1,
             checkpointInterval: Int = 20,
             writeBufferSize: Int = 10,
             collapsedEntityIds: Boolean = false,
             collapsedEntityValues: Boolean = true,
             sequential: Boolean = false): State = {
    require(sampleSize > 0, "`sampleSize` must be positive.")
    require(burninInterval >= 0, "`burninInterval` must be non-negative.")
    require(thinningInterval > 0, "`thinningInterval` must be positive.")
    require(checkpointInterval >= 0, "`checkpointInterval` must be non-negative.")
    require(writeBufferSize > 0, "`writeBufferSize` must be positive.")
    // TODO: ensure that savePath is a valid directory

    var sampleCtr = 0                             // counter for number of samples produced (excludes burn-in/thinning)
    var state = initialState                      // current state
    val initialIteration = initialState.iteration // initial iteration (need not be zero)
    val continueChain = initialIteration != 0     // whether we're continuing a previous chain

    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    /** Set-up writers */
    val linkagePath = outputPath + "linkage-chain.parquet"
    var linkageWriter = BufferedRDDWriter[LinkageState](writeBufferSize, linkagePath, continueChain)
    val diagnosticsPath = outputPath + "diagnostics.csv"
    val diagnosticsWriter = new DiagnosticsWriter(diagnosticsPath, continueChain)
    val checkpointer = new PeriodicRDDCheckpointer[(PartitionId, EntRecPair)](checkpointInterval, sc)

    if (!continueChain && burninInterval == 0) {
      /** Need to record initial state */
      checkpointer.update(state.partitions)
      linkageWriter = linkageWriter.append(state.getLinkageStructure())
      diagnosticsWriter.writeRow(state)
    }

    if (burninInterval > 0) info(s"Running burn-in for $burninInterval iterations.")
    while (sampleCtr < sampleSize) {
      state = state.nextState(checkpointer = checkpointer, collapsedEntityIds, collapsedEntityValues, sequential)

      //newState.partitions.persist(StorageLevel.MEMORY_ONLY_SER)
      //state = newState
      val completedIterations = state.iteration - initialIteration

      if (completedIterations - 1 == burninInterval) {
        if (burninInterval > 0) info("Burn-in complete.")
        info(s"Generating $sampleSize sample(s) with thinningInterval=$thinningInterval.")
      }

      if (completedIterations >= burninInterval) {
        /** Finished burn-in, so start writing samples to disk (accounting for thinning) */
        if ((completedIterations - burninInterval)%thinningInterval == 0) {
          linkageWriter = linkageWriter.append(state.getLinkageStructure())
          diagnosticsWriter.writeRow(state)
          sampleCtr += 1
        }
      }

      /** Ensure writer is kept alive (may die if burninInterval/thinningInterval is large) */
      diagnosticsWriter.progress()
    }

    info("Sampling complete. Writing final state and remaining samples to disk.")
    linkageWriter = linkageWriter.flush()
    diagnosticsWriter.close()
    state.save(outputPath)
    info(s"Finished writing to disk at $outputPath")
    state
  }
}
