package com.github.ngmarchant.dblink.analysis

import com.github.ngmarchant.dblink.{Cluster, LinkageChain, LinkageState, RecordId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object implicits {
  implicit def toLinkageChain(rdd: RDD[LinkageState]): LinkageChain = new LinkageChain(rdd)

  implicit def toMostProbableClusters(rdd: RDD[(RecordId, (Cluster, Double))]): MostProbableClusters =
    new MostProbableClusters(rdd)

  implicit def toClusters(rdd: RDD[Cluster]): Clusters = new Clusters(rdd)

  implicit def toPairwiseLinks(rdd: RDD[(RecordId, RecordId)]): PairwiseLinks = new PairwiseLinks(rdd)

  implicit def toClusters2[T : ClassTag](rdd: RDD[(RecordId, T)]): Clusters = Clusters(rdd)
}
