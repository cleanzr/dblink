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

import com.github.cleanzr.dblink.util.BufferedFileWriter
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object LinkageChain extends Logging {

  /** Read samples of the linkage structure
    *
    * @param path path to the output directory.
    * @return a linkage chain: an RDD containing samples of the linkage
    *         structure (by partition) along the Markov chain.
    */
  def readLinkageChain(path: String): Dataset[LinkageState] = {
    // TODO: check path
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    spark.read.format("parquet")
      .load(path + "linkage-chain.parquet")
      .as[LinkageState]
  }


  /**
    * Computes the most probable clustering for each record
    *
    * @param linkageChain A Dataset representing the linkage structure across iterations and partitions.
    * @return A Dataset containing the most probable cluster for each record.
    */
  def mostProbableClusters(linkageChain: Dataset[LinkageState]): Dataset[MostProbableCluster] = {
    val spark = linkageChain.sparkSession
    import spark.implicits._

    val numSamples = linkageChain.map(_.iteration).distinct().count()
    linkageChain.rdd
      .flatMap(_.linkageStructure.iterator.collect {case (_, recIds) if recIds.nonEmpty => (recIds.toSet, 1.0/numSamples)})
      .reduceByKey(_ + _)
      .flatMap {case (recIds, freq) => recIds.iterator.map(recId => (recId, (recIds, freq)))}
      .reduceByKey((x, y) => if (x._2 >= y._2) x else y)
      .map(x => MostProbableCluster(x._1, x._2._1, x._2._2))
      .toDS()
  }


  /**
    * Computes a point estimate of the most likely clustering that obeys transitivity constraints. The method was
    * introduced by Steorts et al. (2016), where it is referred to as the method of shared most probable maximal
    * matching sets.
    *
    * @param mostProbableClusters A Dataset containing the most probable cluster for each record.
    * @return A Dataset of record clusters
    */
  def sharedMostProbableClusters(mostProbableClusters: Dataset[MostProbableCluster]): Dataset[Cluster] = {
    val spark = mostProbableClusters.sparkSession
    import spark.implicits._

    mostProbableClusters.rdd
      .map(x => (x.cluster, x.recordId))
      .aggregateByKey(zeroValue = Set.empty[RecordId])(
        seqOp = (recordIds, recordId) => recordIds + recordId,
        combOp = (recordIdsA, recordIdsB) => recordIdsA union recordIdsB
      )
      // key = most probable cluster | value = aggregated recIds
      .map(_._2)
      .toDS()
    //      .flatMap[Set[RecordIdType]] {
    //        case (cluster, recIds) if cluster == recIds => Iterator(recIds)
    //          // cluster is shared most probable for all records it contains
    //        case (cluster, recIds) if cluster != recIds => recIds.iterator.map(Set(_))
    //          // cluster isn't shared most probable -- output each record as a
    //          // separate cluster
    //      }
  }


  /**
    * Computes a point estimate of the most likely clustering that obeys transitivity constraints. The method was
    * introduced by Steorts et al. (2016), where it is referred to as the method of shared most probable maximal
    * matching sets.
    *
    * @param linkageChain A Dataset representing the linkage structure across iterations and partitions.
    * @return A Dataset of record clusters
    */
  def sharedMostProbableClusters(linkageChain: Dataset[LinkageState])(implicit i1: DummyImplicit): Dataset[Cluster] = {
    val mpc = mostProbableClusters(linkageChain)
    sharedMostProbableClusters(mpc)
  }


  /** Computes the partition sizes along the linkage chain
    *
    * @param linkageChain A Dataset representing the linkage structure across iterations and partitions.
    * @return A Dataset containing the partition sizes at each iteration. The key is the iteration and the value is a
    *         map from partition ids to their corresponding sizes.
    */
  def partitionSizes(linkageChain: Dataset[LinkageState]): Dataset[(Long, Map[PartitionId, Int])] = {
    val spark = linkageChain.sparkSession
    import spark.implicits._
    linkageChain.rdd
      .map(x => (x.iteration, (x.partitionId, x.linkageStructure.keySet.size)))
      .aggregateByKey(Map.empty[PartitionId, Int])(
        seqOp = (m, v) => m + v,
        combOp = (m1, m2) => m1 ++ m2
      )
      .toDS()
  }


  /** Computes the cluster size frequency distribution along the linkage chain
    *
    * @param linkageChain A Dataset representing the linkage structure across iterations and partitions.
    * @return A Dataset containing the cluster size frequency distribution at each iteration. The key is the iteration
    *         and the value is a map from cluster sizes to their corresponding frequencies.
    */
  def clusterSizeDistribution(linkageChain: Dataset[LinkageState]): Dataset[(Long, mutable.Map[Int, Long])] = {
    // Compute distribution separately for each partition, then combine the results
    val spark = linkageChain.sparkSession
    import spark.implicits._

    linkageChain.rdd
      .map(x => {
        val clustSizes = mutable.Map[Int, Long]().withDefaultValue(0L)
        x.linkageStructure.foreach { case (_, recIds) =>
          val k = recIds.size
          clustSizes(k) += 1L
        }
        (x.iteration, clustSizes)
      })
      .reduceByKey((a, b) => {
        val combined = mutable.Map[Int, Long]().withDefaultValue(0L)
        (a.keySet ++ b.keySet).foreach(k => combined(k) = a(k) + b(k))
        combined // combine maps
      })
      .toDS()
  }


  /** Computes the cluster size frequency distribution along the linkage chain
    * and saves the result to `cluster-size-distribution.csv` in the output directory.
    *
    * @param path path to working directory
    */
  def saveClusterSizeDistribution(clusterSizeDistribution: Dataset[(Long, mutable.Map[Int, Long])], path: String): Unit = {
    val sc = clusterSizeDistribution.sparkSession.sparkContext

    val distAlongChain = clusterSizeDistribution.collect().sortBy(_._1) // collect on driver and sort by iteration

    // Get the size of the largest cluster in the samples
    val maxClustSize = distAlongChain.aggregate(0)(
      seqop = (currentMax, x) => math.max(currentMax, x._2.keySet.max),
      combop = (a, b) => math.max(a, b)
    )
    // Output file can be created from Hadoop file system.
    val fullPath = path + "cluster-size-distribution.csv"
    info(s"Writing cluster size frequency distribution along the chain to $fullPath")
    val writer = BufferedFileWriter(fullPath, append = false, sc)

    // Write CSV header
    writer.write("iteration" + "," + (0 to maxClustSize).mkString(",") + "\n")
    // Write rows (one for each iteration)
    distAlongChain.foreach { case (iteration, kToCounts) =>
      val countsArray = (0 to maxClustSize).map(k => kToCounts.getOrElse(k, 0L))
      writer.write(iteration.toString + "," + countsArray.mkString(",") + "\n")
    }
    writer.close()
  }


  /** Computes the sizes of the partitions along the chain and saves the result to
    * `partition-sizes.csv` in the output directory.
    *
    * @param path path to output directory
    */
  def savePartitionSizes(partitionSizes: Dataset[(Long, Map[PartitionId, Int])], path: String): Unit = {
    val sc = partitionSizes.sparkSession.sparkContext
    val partSizesAlongChain = partitionSizes.collect().sortBy(_._1)

    val partIds = partSizesAlongChain.map(_._2.keySet).reduce(_ ++ _).toArray.sorted

    // Output file can be created from Hadoop file system.
    val fullPath = path + "partition-sizes.csv"
    val writer = BufferedFileWriter(fullPath, append = false, sc)

    // Write CSV header
    writer.write("iteration" + "," + partIds.mkString(",") + "\n")
    // Write rows (one for each iteration)
    partSizesAlongChain.foreach { case (iteration, m) =>
      val sizes = partIds.map(partId => m.getOrElse(partId, 0))
      writer.write(iteration.toString + "," + sizes.mkString(",") + "\n")
    }
    writer.close()
  }
}