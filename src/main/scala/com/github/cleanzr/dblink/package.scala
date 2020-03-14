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

package com.github.cleanzr

import com.github.cleanzr.dblink.SimilarityFn.ConstantSimilarityFn

/** Types/case classes used throughout */
package object dblink {
  import org.apache.spark.rdd.RDD

  type FileId = String
  type RecordId = String
  type PartitionId = Int
  type EntityId = Int
  type ValueId = Int
  type AttributeId = Int
  type Partitions = RDD[(PartitionId, EntRecCluster)]
  type AggDistortions = Map[(AttributeId, FileId), Long]
  type Cluster = Set[RecordId]
  type RecordPair = (RecordId, RecordId)

  case class MostProbableCluster(recordId: RecordId, cluster: Set[RecordId], frequency: Double)

  /** Record (row) in the input data
    *
    * @param id a unique identifier for the record (must be unique across
    *           _all_ files)
    * @param fileId file identifier for the record
    * @param values attribute values for the record
    * @tparam T value type
    */
  case class Record[T](id: RecordId,
                       fileId: FileId,
                       values: Array[T]) {
    def mkString: String = s"Record(id=$id, fileId=$fileId, values=${values.mkString(",")})"
  }


  /** Latent entity
    *
    * @param values attribute values for the entity.
    */
  case class Entity(values: Array[ValueId]) {
    def mkString: String = s"Entity(${values.mkString(",")})"
  }


  /** Attribute value subject to distortion
    *
    * @param value the attribute value
    * @param distorted whether the value is distorted
    */
  case class DistortedValue(value: ValueId, distorted: Boolean) {
    def mkString: String = s"DistortedValue(value=$value, distorted=$distorted"
  }


  /**
    * Entity-record cluster
    * @param entity a latent entity
    * @param records records linked to the entity
    */
  case class EntRecCluster(entity: Entity,
                           records: Option[Array[Record[DistortedValue]]]) {
    def mkString: String = {
      records match {
        case Some(r) => s"${entity.mkString}\t->\t${r.mkString(", ")}"
        case None => entity.mkString
      }
    }
  }

  /** Linkage state
    *
    * Represents the linkage structure within a partition for a single iteration.
    */
  case class LinkageState(iteration: Long,
                          partitionId: PartitionId,
                          linkageStructure: Seq[Seq[RecordId]])


  /** Partition-entity-record cluster triple
    *
    * This class is used in the Dataset representation of the partitions---not
    * the RDD representation.
    *
    * @param partitionId identifier for the partition
    * @param entRecCluster entity cluster of records
    */
  case class PartEntRecCluster(partitionId: PartitionId, entRecCluster: EntRecCluster)

  /** Container for the summary variables
    *
    * @param numIsolates
    * @param logLikelihood
    * @param aggDistortions
    * @param recDistortions
    */
  case class SummaryVars(numIsolates: Long,
                         logLikelihood: Double,
                         aggDistortions: AggDistortions,
                         recDistortions: Map[Int, Long])


  /** Specifications for an attribute
    *
    * @param name column name of the attribute (should match original DataFrame)
    * @param similarityFn an attribute similarity function
    * @param distortionPrior prior for the distortion
    */
  case class Attribute(name: String,
                       similarityFn: SimilarityFn,
                       distortionPrior: BetaShapeParameters) {
    /** Whether the similarity function for this attribute is constant or not*/
    def isConstant: Boolean = {
      similarityFn match {
        case ConstantSimilarityFn => true
        case _ => false
      }
    }
  }

  /** Specifications for an indexed attribute
    *
    * @param name column name of the attribute (should match original DataFrame)
    * @param similarityFn an attribute similarity function
    * @param distortionPrior prior for the distortion
    * @param index index for the attribute
    */
  case class IndexedAttribute(name: String,
                              similarityFn: SimilarityFn,
                              distortionPrior: BetaShapeParameters,
                              index: AttributeIndex) {
    /** Whether the similarity function for this attribute is constant or not*/
    def isConstant: Boolean = {
      similarityFn match {
        case ConstantSimilarityFn => true
        case _ => false
      }
    }
  }


  /** Container for shape parameters of the Beta distribution
    *
    * @param alpha "alpha" shape parameter
    * @param beta "beta" shape parameter
    */
  case class BetaShapeParameters(alpha: Double, beta: Double) {
    require(alpha > 0 && beta > 0, "shape parameters must be positive")

    def mkString: String = s"BetaShapeParameters(alpha=$alpha, beta=$beta)"
  }
}