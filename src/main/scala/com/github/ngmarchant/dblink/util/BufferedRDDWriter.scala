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

package com.github.ngmarchant.dblink.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.storage.StorageLevel
import com.github.ngmarchant.dblink.Logging
import scala.reflect.ClassTag

case class BufferedRDDWriter[T : ClassTag : Encoder](path: String,
                                                     capacity: Int,
                                                     append: Boolean,
                                                     rdds: Seq[RDD[T]],
                                                     firstFlush: Boolean)

object BufferedRDDWriter extends Logging {
  def append[T : ClassTag : Encoder](writerState: BufferedRDDWriter[T], rows: RDD[T]): BufferedRDDWriter[T] = {
    val currentState = if (writerState.capacity - writerState.rdds.size == 0) flush(writerState) else writerState
    rows.persist(StorageLevel.MEMORY_ONLY_SER)
    rows.count() // to force evaluation
    val newRdds = currentState.rdds :+ rows
    BufferedRDDWriter(currentState.path, currentState.capacity, currentState.append, newRdds, currentState.firstFlush)
  }

  def flush[T : ClassTag : Encoder](writerState: BufferedRDDWriter[T]): BufferedRDDWriter[T] = {
    if (writerState.rdds.nonEmpty) {
      val sc = writerState.rdds.head.sparkContext

      val spark = SparkSession.builder().getOrCreate()

      val unionedRdds = sc.union(writerState.rdds)
      val unionedDS = spark.createDataset(unionedRdds)
      if (writerState.firstFlush) {
        // Special case for first write to disk: may need to overwrite a
        // pre-existing file at the given path
        if (writerState.append) {
          unionedDS.write.partitionBy("partitionId").format("parquet").mode("append").save(writerState.path)
        } else {
          unionedDS.write.partitionBy("partitionId").format("parquet").mode("overwrite").save(writerState.path)
        }
      } else {
        unionedDS.write.partitionBy("partitionId").format("parquet").mode("append").save(writerState.path)
      }
      debug(s"Flushed to disk at ${writerState.path}")
      writerState.rdds.foreach(_.unpersist())
      val newRdds = Seq.empty[RDD[T]]
      BufferedRDDWriter(writerState.path, writerState.capacity, writerState.append, newRdds, firstFlush = false)
    } else {
      writerState
    }
  }

  def apply[T : ClassTag : Encoder](capacity: Int, path: String, append: Boolean): BufferedRDDWriter[T] = {
    val rdds = Seq.empty[RDD[T]]
    BufferedRDDWriter[T](path, capacity, append, rdds, firstFlush = true)
  }
}