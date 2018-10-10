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

import com.github.ngmarchant.dblink.util.BufferedFileWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import com.github.ngmarchant.dblink.LinkageChain._

import scala.collection.mutable

class LinkageChain(val rdd: RDD[LinkageState]) extends Logging {

  lazy val numSamples: Long = rdd.map(_.iteration).distinct().count()

  /** Computes the most probable clustering for each record
    *
    * @return A PairRDD containing the most probable cluster for each record.
    *         The key is the recId and the value contains the most probable
    *         cluster (as a set of recIds) and the relative frequency of
    *         occurence (along the chain).
    */
  lazy val mostProbableClusters: RDD[(RecordId, (Cluster, Double))] = _mostProbableClusters(rdd, numSamples)

  /** Computes the shared most probable clusters
    * TODO: description
    * @return
    */
  def sharedMostProbableClusters: RDD[Cluster] = _sharedMostProbableClusters(mostProbableClusters)

  /** Computes the cluster size frequency distribution along the linkage chain
    * and saves the result to `clusterSizeDistribution.csv` in the working directory.
    *
    * @param path path to working directory
    */
  def saveClusterSizeDistribution(path: String): Unit = {
    val sc = rdd.sparkContext

    /** Compute dist separately for each partition, then combine the results */
    val distAlongChainRDD = rdd
      .map(partitionState => {
        val linkageStructure = partitionState.linkageStructure
        val iteration = partitionState.iteration
        val clustSizes = mutable.HashMap.empty[Int, Long]
        linkageStructure.foreach { case (_, recIds) =>
          val k = recIds.size
          clustSizes.update(k, clustSizes.getOrElse(k, 0L) + 1L)
        }
        (iteration, clustSizes)
      })
      .reduceByKey((a, b) => {
        val combined = mutable.HashMap.empty[Int, Long]
        (a.keySet ++ b.keySet).foreach(k => combined(k) = a.getOrElse(k, 0L) + b.getOrElse(k, 0L))
        combined // combine maps
      })
      .persist(StorageLevel.MEMORY_ONLY_SER)

    /** Get the size of the largest cluster in the samples */
    val maxClustSize = distAlongChainRDD.aggregate(0)(
      seqOp = (currentMax, x) => math.max(currentMax, x._2.keySet.max),
      combOp = (a, b) => math.max(a, b)
    )

    /** Collect and sort by iteration */
    val distAlongChain = distAlongChainRDD.collect().sortBy(_._1)
    distAlongChainRDD.unpersist()

    /** Output file can be created from Hadoop file system. */
    val fullPath = path + "clusterSizeDistribution.csv"
    info(s"Writing cluster size frequency distribution along the chain to $fullPath")
    val writer = BufferedFileWriter(fullPath, append = false, sc)

    /** Write CSV header */
    writer.write("iteration" + "," + (0 to maxClustSize).mkString(",") + "\n")
    /** Write rows (one for each iteration) */
    distAlongChain.foreach { case (iteration, kToCounts) =>
      val countsArray = (0 to maxClustSize).map(k => kToCounts.getOrElse(k, 0L))
      writer.write(iteration.toString + "," + countsArray.mkString(",") + "\n")
    }
    writer.close()
  }

  /** Computes the sizes of the partitions along the chain and saves the result to
    * `partitionSizes.csv` in the working directory.
    *
    * @param path path to working directory
    */
  def savePartitionSizes(path: String): Unit = _partitionSizes(rdd, path)
}

object LinkageChain {
  /** Read samples of the linkage structure
    *
    * @param path path to the working directory.
    * @return a linkage chain: an RDD containing samples of the linkage
    *         structure (by partition) along the Markov chain.
    */
  def read(path: String): RDD[LinkageState] = {
    // TODO: check path
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    spark.read.format("parquet")
      .load(path + "linkageChain.parquet")
      .as[LinkageState]
      .rdd
  }

  private def _mostProbableClusters(rdd: RDD[LinkageState], numSamples: Long): RDD[(RecordId, (Cluster, Double))] = {
    rdd
      .flatMap(_.linkageStructure.iterator.collect {case (_, recIds) if recIds.nonEmpty => (recIds.toSet, 1.0/numSamples)})
      .reduceByKey(_ + _)
      // key = recIds (observed as a cluster in samples) | value = freq in samples
      .flatMap {case (recIds, freq) => recIds.iterator.map(recId => (recId, (recIds, freq)))}
      // key = recId | value = (recIds in the same cluster, freq in samples)
      .reduceByKey((x, y) => if (x._2 >= y._2) x else y)
    // for each recId keep only the row with the highest freq (most probable cluster)
  }

  private def _sharedMostProbableClusters(mpClusters: RDD[(RecordId, (Cluster, Double))]): RDD[Cluster] = {
    mpClusters.map { case (recId, (mpCluster, _)) => (mpCluster, recId) }
      // key = most probable cluster | value = recId
      .aggregateByKey(zeroValue = Set.empty[RecordId])(
      seqOp = (recIds, recId) => recIds + recId,
      combOp = (recIdsA, recIdsB) => recIdsA union recIdsB
    )
      // key = most probable cluster | value = aggregated recIds
      .map(_._2)
    //      .flatMap[Set[RecordIdType]] {
    //        case (cluster, recIds) if cluster == recIds => Iterator(recIds)
    //          // cluster is shared most probable for all records it contains
    //        case (cluster, recIds) if cluster != recIds => recIds.iterator.map(Set(_))
    //          // cluster isn't shared most probable -- output each record as a
    //          // separate cluster
    //      }
  }

  private def _partitionSizes(rdd: RDD[LinkageState], path: String): Unit = {
    val sc = rdd.sparkContext
    val partSizesAlongChain = rdd.map(x => (x.iteration, (x.partitionId, x.linkageStructure.keySet.size)))
      .collect()
      .groupBy(_._1) // iteration
      .mapValues(_.map(_._2).toMap) // partId -> size map
      .toArray
      .sortBy(_._1)

    val partIds = partSizesAlongChain.map(_._2.keySet).reduce(_ ++ _).toArray.sorted

    /** Output file can be created from Hadoop file system. */
    val fullPath = path + "partitionSizes.csv"
    val writer = BufferedFileWriter(fullPath, append = false, sc)

    /** Write CSV header */
    writer.write("iteration" + "," + partIds.mkString(",") + "\n")
    /** Write rows (one for each iteration) */
    partSizesAlongChain.foreach { case (iteration, m) =>
      val sizes = partIds.map(partId => m.getOrElse(partId, 0))
      writer.write(iteration.toString + "," + sizes.mkString(",") + "\n")
    }
    writer.close()
  }
}