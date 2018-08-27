import com.github.ngmarchant.dblink.{Cluster, RecordId}
import org.apache.spark.rdd.RDD

object baselines {
  def exactMatchClusters(records: RDD[(String, Seq[String])]): RDD[Cluster] = {
    records.map(row => (row._2.mkString, row._1)) // (values, id)
      .aggregateByKey(Set.empty[RecordId])(
      seqOp = (recIds, recId) => recIds + recId,
      combOp = (recIdsA, recIdsB) => recIdsA ++ recIdsB
    )
      .map(_._2)
  }

  def nearClusters(records: RDD[(String, Seq[String])], numAgree: Int): RDD[Cluster] = {
    require(numAgree >= 0, "`numAgree` must be non-negative")
    records.flatMap { row =>
      val numFields = row._2.length
      val fieldIds = 0 until numFields
      fieldIds.combinations(numAgree).map { delIds =>
        val partialValues = row._2.zipWithIndex.collect { case (fieldVal, fieldId) if !delIds.contains(fieldId) => fieldVal }
        (partialValues.mkString, row._1)
      }
    }.aggregateByKey(Set.empty[RecordId])(
      seqOp = (recIds, recId) => recIds + recId,
      combOp = (recIdsA, recIdsB) => recIdsA ++ recIdsB
    )
      .map(_._2)
  }
}
