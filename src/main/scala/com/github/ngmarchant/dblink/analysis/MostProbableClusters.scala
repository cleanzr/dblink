package com.github.ngmarchant.dblink.analysis

import com.github.ngmarchant.dblink.{Cluster, RecordId}
import org.apache.spark.rdd.RDD

class MostProbableClusters(val rdd: RDD[(RecordId, (Cluster, Double))]) {
  /** Converts to a `Clusters` object. Strips record identifier and probability info.
    *
    * TODO: title
    *
    * @return
    */
  def toClusters: RDD[Cluster] = rdd.map(_._2._1)
}