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

package com.github.ngmarchant

import com.github.ngmarchant.dblink.SimilarityFn.ConstantSimilarityFn

/** Types/case classes used throughout */
package object dblink {
  import org.apache.spark.rdd.RDD

  type FileId = String
  type RecordId = String
  type PartitionId = Int
  type EntityId = Long
  type ValueId = Int
  type AttributeId = Int
  type Partitions = RDD[(PartitionId, EntRecPair)]
  type AggDistortions = Map[(AttributeId, FileId), Long]
  type Cluster = Set[RecordId]


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
                       values: Array[T])


  /** Latent entity
    *
    * @param id a unique identifier for the entity.
    * @param values attribute values for the entity.
    */
  case class Entity(id: EntityId,
                    values: Array[ValueId])


  /** Attribute value subject to distortion
    *
    * @param value the attribute value
    * @param distorted whether the value is distorted
    */
  case class DistortedValue(value: ValueId, distorted: Boolean)


  /** Linked entity-record pair
    *
    * @param entity a latent entity
    * @param record a record
    */
  case class EntRecPair(entity: Entity,
                        record: Option[Record[DistortedValue]]) {
    def mkString: String = {
      record match {
        case Some(r) => s"entId: ${entity.id}\tvalues: ${entity.values.mkString}\t\trecId: ${r.id}\tvalues: ${r.values.mkString}"
        case None => s"entId: ${entity.id}\tvalues: ${entity.values.mkString}"
      }
    }
  }


  /** Linkage state
    *
    * Represents the linkage structure within a partition for a single iteration.
    */
  case class LinkageState(iteration: Long,
                          partitionId: PartitionId,
                          linkageStructure: Map[EntityId, Seq[RecordId]])


  /** Linked partition-entity-record triple
    *
    * This class is used in the Dataset representation of the partitions---not
    * the RDD representation.
    *
    * @param partitionId identifier for the partition
    * @param entRecPair linked entity-record pair
    */
  case class PartEntRecTriple(partitionId: PartitionId, entRecPair: EntRecPair)


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