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

import com.github.ngmarchant.dblink.accumulators.MapLongAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Container to store statistics/metadata for a collection of records and
  * facilitate sampling from the attribute domains.
  *
  * This container is broadcast to each executor.
  *
  * @param indexedAttributes indexes for the attributes
  * @param fileSizes number of records in each file
  */
class RecordsCache(val indexedAttributes: IndexedSeq[IndexedAttribute],
                   val fileSizes: Map[FileId, Long],
                   val missingCounts: Option[Map[(FileId, AttributeId), Long]] = None) extends Serializable {

  def distortionPrior: Iterator[BetaShapeParameters] = indexedAttributes.iterator.map(_.distortionPrior)

  /** Number of records across all files */
  val numRecords: Long = fileSizes.values.sum

  /** Number of attributes used for matching */
  def numAttributes: Int = indexedAttributes.length

  /** Transform to value ids
    *
    * @param records an RDD of records in raw form (string attribute values)
    * @return an RDD of records where the string attribute values are replaced
    *         by integer value ids
    */
  def transformRecords(records: RDD[Record[String]]): RDD[Record[ValueId]] =
    RecordsCache._transformRecords(records, indexedAttributes)
}

object RecordsCache extends Logging {

  /** Build RecordsCache
    *
    * @param records an RDD of records in raw form (string attribute values)
    * @param attributeSpecs specifications for each record attribute. Must match
    *                       the order of attributes in `records`.
    * @param expectedMaxClusterSize largest expected record cluster size. Used
    *                               as a hint when precaching distributions
    *                               over the non-constant attribute domain.
    * @return a RecordsCache
    */
  def apply(records: RDD[Record[String]],
            attributeSpecs: IndexedSeq[Attribute],
            expectedMaxClusterSize: Int): RecordsCache = {
    val firstRecord = records.take(1).head
    require(firstRecord.values.length == attributeSpecs.length, "attribute specifications do not match the records")

    /** Use accumulators to gather record stats in one pass */
    val accFileSizes = new MapLongAccumulator[FileId]
    implicit val sc: SparkContext = records.sparkContext
    sc.register(accFileSizes, "number of records per file")

    val accMissingCounts = new MapLongAccumulator[(FileId, AttributeId)]
    sc.register(accMissingCounts, s"missing counts per file and attribute")

    val accValueCounts = attributeSpecs.map{ attribute =>
      val acc = new MapLongAccumulator[String]
      sc.register(acc, s"value counts for attribute ${attribute.name}")
      acc
    }

    info("Gathering statistics from source data files.")
    /** Get file and value counts in a single foreach action */
    records.foreach { case Record(_, fileId, values) =>
      accFileSizes.add((fileId, 1L))
      values.zipWithIndex.foreach { case (value, attrId) =>
        if (value != null) accValueCounts(attrId).add(value, 1L)
        else accMissingCounts.add(((fileId, attrId), 1L))
      }
    }

    val missingCounts = accMissingCounts.value
    val fileSizes = accFileSizes.value
    val totalRecords = fileSizes.values.sum
    val percentageMissing = 100.0 * missingCounts.values.sum / (totalRecords * attributeSpecs.size)
    if (percentageMissing >= 0.0) {
      info(f"Finished gathering statistics from $totalRecords records across ${fileSizes.size} file(s). $percentageMissing%.3f%% of the record attribute values are missing.")
    } else {
      info(s"Finished gathering statistics from $totalRecords records across ${fileSizes.size} file(s). There are no missing record attribute values.")
    }

    /** Build an index for each attribute (generates a mapping from strings -> integers) */
    val indexedAttributes = attributeSpecs.zipWithIndex.map { case (attribute, attrId) =>
      val valuesWeights = accValueCounts(attrId).value.mapValues(_.toDouble)
      info(s"Indexing attribute '${attribute.name}' with ${valuesWeights.size} unique values.")
      val index = AttributeIndex(valuesWeights, attribute.similarityFn,
        Some(1 to expectedMaxClusterSize))
      IndexedAttribute(attribute.name, attribute.similarityFn, attribute.distortionPrior, index)
    }

    new RecordsCache(indexedAttributes, fileSizes, Some(missingCounts))
  }

  private def _transformRecords(records: RDD[Record[String]],
                                indexedAttributes: IndexedSeq[IndexedAttribute]): RDD[Record[ValueId]] = {
    val firstRecord = records.take(1).head
    require(firstRecord.values.length == indexedAttributes.length, "attribute specifications do not match the records")

    records.mapPartitions { partition =>
      partition.map { record =>
        val mappedValues = record.values.zipWithIndex.map { case (stringValue, attrId) =>
          if (stringValue != null) indexedAttributes(attrId).index.valueIdxOf(stringValue)
          else -1
        }
        Record[ValueId](record.id, record.fileId, mappedValues)
      }
    }
  }
}