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

import com.github.ngmarchant.dblink.{Cluster, EntityId, RecordId}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class Clusters(val rdd: RDD[Cluster]) {

  def saveCsv(path: String, overwrite: Boolean = true): Unit = {
    val sc = rdd.sparkContext
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val file = new Path(path)
    if (hdfs.exists(file)) {
      if (!overwrite) return else hdfs.delete(file, true)
    }
    rdd.map(cluster => cluster.mkString(", "))
      .saveAsTextFile(path)
  }

  def toPairwiseLinks: RDD[(RecordId, RecordId)] =
    rdd.flatMap(_.toSeq.combinations(2).map(x => (x(0), x(1))))

  def toMembership: RDD[(RecordId, EntityId)] =
    rdd.zipWithUniqueId().flatMap { case (recIds, entityId) =>
      recIds.iterator.map(recId => (recId, entityId))
    }
}

object Clusters {
  def readCsv(path: String): RDD[Cluster] = {
    val sc = SparkContext.getOrCreate()
    sc.textFile(path)
      .map(line => line.split(",").map(_.trim).toSet)
  }

  // Input: RDD containing record ids and their cluster memberships
  def apply[T : ClassTag](membership: RDD[(RecordId, T)]): RDD[Cluster] = {
    membership
      .map(_.swap)
      .aggregateByKey(Set.empty[RecordId])(
        seqOp = (recIds, recId) => recIds + recId,
        combOp = (partA, partB) => partA union partB
      )
      .map(_._2) // discard the entity identifiers
  }
}
