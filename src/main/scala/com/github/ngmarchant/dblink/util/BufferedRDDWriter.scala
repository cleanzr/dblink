// Copyright (C) 2018  Australian Bureau of Statistics
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
                                                     firstFlush: Boolean) extends Logging {

  def append(rows: RDD[T]): BufferedRDDWriter[T] = {
    val writer = if (capacity - rdds.size == 0) this.flush() else this
    rows.persist(StorageLevel.MEMORY_ONLY)
    rows.count() // force evaluation
    val newRdds = writer.rdds :+ rows
    this.copy(rdds = newRdds)
  }

  private def write(unionedRdds: RDD[T], overwrite: Boolean): Unit = {
    /** Write to disk in Parquet format relying on Dataset/Dataframe API */
    val spark = SparkSession.builder().getOrCreate()
    val unionedDS = spark.createDataset(unionedRdds)
    val saveMode = if (overwrite) "overwrite" else "append"
    unionedDS.write.partitionBy("partitionId").format("parquet").mode(saveMode).save(path)
  }

  def flush(): BufferedRDDWriter[T] = {
    if (rdds.isEmpty) return this

    /** Combine RDDs in the buffer using a union operation */
    val sc = rdds.head.sparkContext
    val unionedRdds = sc.union(rdds)

    /** Write to disk. Note: overwrite if not appending and this is the first write to disk. */
    write(unionedRdds, firstFlush && !append)
    debug(s"Flushed to disk at ${path}")

    /** Unpersist RDDs in buffer as they're no longer required */
    rdds.foreach(_.unpersist(blocking = false))

    this.copy(rdds = Seq.empty[RDD[T]], firstFlush = false)
  }
}

object BufferedRDDWriter extends Logging {
  def apply[T : ClassTag : Encoder](capacity: Int, path: String, append: Boolean): BufferedRDDWriter[T] = {
    val rdds = Seq.empty[RDD[T]]
    BufferedRDDWriter[T](path, capacity, append, rdds, firstFlush = true)
  }
}