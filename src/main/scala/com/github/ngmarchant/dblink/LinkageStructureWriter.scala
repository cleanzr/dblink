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

import com.github.ngmarchant.dblink.util.BufferedRDDWriter
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** BufferedWriter for saving samples of the linkage structure to disk.
  *
  * @param path path to save samples (can be a HDFS or local path)
  * @param continueChain whether to append to existing samples (if present)
  * @param bufferSize number of samples to store in the buffer before flushing to disk
  */
class LinkageStructureWriter(path: String, continueChain: Boolean, bufferSize: Int)
                            (implicit sparkSession: SparkSession) extends Logging {
  import sparkSession.implicits._
  private var writer = BufferedRDDWriter[LinkageState](bufferSize, path, continueChain)

  info(s"Writing linkage structure along chain to $path.")

  /** Write state to disk (extracts the linkage structure)
    *
    * @param state state of Markov chain
    */
  def write(state: State): Unit = {
    /** Extract the linkage structure from the state */
    val linkageStructure = state.partitions.mapPartitions(partition => {
      val entRecClusters = mutable.LongMap.empty[ArrayBuffer[RecordId]]
      var partitionId = -1
      partition.foreach { case (pId: Int, EntRecPair(entity, record)) =>
        partitionId = pId
        val cluster = entRecClusters.getOrElseUpdate(entity.id, ArrayBuffer.empty[RecordId])
        if (record.isDefined) {
          cluster += record.get.id
        }
      }
      if (entRecClusters.isEmpty) {
        Iterator()
      } else {
        Iterator(LinkageState(state.iteration, partitionId, entRecClusters.toMap))
      }
    }, preservesPartitioning = true)

    writer = BufferedRDDWriter.append(writer, linkageStructure)
  }

  def flush(): Unit = BufferedRDDWriter.flush(writer)

  def close(): Unit = BufferedRDDWriter.flush(writer)
}