package com.github.cleanzr.dblink

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

package object analysis {

  /**
    * TODO
    * @param links
    * @return
    */
  def canonicalizePairwiseLinks(links: Dataset[RecordPair]): Dataset[RecordPair] = {
    val spark = links.sparkSession
    import spark.implicits._

    links.map { recIds =>
      // Ensure pairs are sorted
      recIds._1.compareTo(recIds._2) match {
        case x if x < 0 => (recIds._1, recIds._2)
        case x if x > 0 => (recIds._2, recIds._1)
        case 0 => throw new Exception(s"Invalid link: ${recIds._1} <-> ${recIds._2}.")
      }
    }.distinct()
  }



  /**
    * TODO
    * @param path
    * @return
    */
  def readClustersCSV(path: String): Dataset[Cluster] = {
    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.textFile(path)
      .map(line => line.split(",").map(_.trim).toSet).toDS()
  }



  /**
    * TODO
    * @param membership
    * @tparam T
    * @return
    */
  def membershipToClusters[T : ClassTag](membership: Dataset[(RecordId, T)]): Dataset[Cluster] = {
    val spark = membership.sparkSession
    import spark.implicits._
    membership.rdd
      .map(_.swap)
      .aggregateByKey(Set.empty[RecordId])(
        seqOp = (recIds, recId) => recIds + recId,
        combOp = (partA, partB) => partA union partB
      )
      .map(_._2)
      .toDS()
  }



  /*
   These private methods must appear outside the Clusters implicit value class due to a limitation of the compiler
   */
  private def _toPairwiseLinks(clusters: Dataset[Cluster]): Dataset[RecordPair] = {
    val spark = clusters.sparkSession
    import spark.implicits._
    val links = clusters.flatMap(_.toSeq.combinations(2).map(x => (x(0), x(1))))
    canonicalizePairwiseLinks(links)
  }

  private def _toMembership(clusters: Dataset[Cluster]): Dataset[(RecordId, EntityId)] = {
    val spark = clusters.sparkSession
    import spark.implicits._
    clusters.rdd
      .zipWithUniqueId()
      .flatMap { case (recIds, entityId) => recIds.iterator.map(recId => (recId, entityId.toInt)) }
      .toDS()
  }

  /**
    * Represents a clustering of records as sets of record ids. Provides methods for converting to pairwise and
    * membership representations.
    *
    * @param ds A Dataset of record clusters
    */
  implicit class Clusters(val ds: Dataset[Cluster]) extends AnyVal {
    /**
      * Save to a CSV file
      *
      * @param path Path to the file. May be a path on the local filesystem or HDFS.
      * @param overwrite Whether to overwrite an existing file or not.
      */
    def saveCsv(path: String, overwrite: Boolean = true): Unit = {
      val sc = ds.sparkSession.sparkContext
      val hdfs = FileSystem.get(sc.hadoopConfiguration)
      val file = new Path(path)
      if (hdfs.exists(file)) {
        if (!overwrite) return else hdfs.delete(file, true)
      }
      ds.rdd.map(cluster => cluster.mkString(", "))
        .saveAsTextFile(path)
    }

    /**
      * Convert to a pairwise representation
      */
    def toPairwiseLinks: Dataset[RecordPair] = _toPairwiseLinks(ds)

    /**
      * Convert to a membership vector representation
      */
    def toMembership: Dataset[(RecordId, EntityId)] = _toMembership(ds)
  }
}
