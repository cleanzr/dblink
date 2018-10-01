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

import com.github.ngmarchant.dblink.random.DiscreteDist
import com.github.ngmarchant.dblink.partitioning.PartitionFunction
import com.github.ngmarchant.dblink.util._
import org.apache.commons.math3.distribution.BetaDistribution
import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.log

object GibbsUpdates {

  /** Lightweight inverted index for the entity attribute values.
    * Only supports insertion and querying.
    * Updating is not required since the index is rebuilt from scratch at the
    * beginning of each iteration.
    */
  class EntityInvertedIndex extends Serializable {
    private val attrValueToEntIds =
      mutable.HashMap.empty[(AttributeId, ValueId), mutable.Set[EntityId]]
    
    /** Method to add a single (attribute value -> entId) combination to
      * the index */
    private def add_attribute(attrId: AttributeId, valueId: ValueId,
                              entId: EntityId): Unit = {
      attrValueToEntIds.get((attrId, valueId)) match {
        case Some(currentEntities) => currentEntities.add(entId)
        case None => attrValueToEntIds.update((attrId, valueId), mutable.Set(entId))
      }
    }
    
    /** Add entity to the inverted index */
    def add(entity: Entity): Unit = {
      var attrId = 0
      while (attrId < entity.values.length) {
        this.add_attribute(attrId, entity.values(attrId), entity.id)
        attrId += 1
      }
    }

    /** Get matching entities
      * 
      * @param attrId query attribute id
      * @param valueId query value id (corresponding to the attribute id)
      * @return the set of entity ids that match the queried attribute value
      */
    def getEntityIds(attrId: AttributeId, valueId: ValueId): scala.collection.Set[EntityId] = {
      attrValueToEntIds.getOrElse((attrId, valueId), mutable.Set.empty[EntityId])
    }
  }



  // ------------------------------------------------------------------------ //
  /** An index from entity ids to row ids
    * Rows corresponding to isolated entities (i.e. not records) are omitted
    */
  class LinksIndex(allEntityIds: Iterator[EntityId],
                   numRows: Int) extends Serializable {
    private val entIdToRowIds =
      allEntityIds.foldLeft(mutable.LongMap.empty[mutable.ArrayBuffer[Int]]) {
        (m, entId) => m += (entId -> mutable.ArrayBuffer.empty[Int])
      }

    private val rowIdToEntId = Array.fill[EntityId](numRows)(-1L)

    // IMPORTANT: only add (entId, rowId) pairs that correspond to records
    // Must filter out isolated entities
    def addLink(entId: EntityId, rowId: Int): Unit = {
      rowIdToEntId(rowId) = entId
      entIdToRowIds(entId) += rowId
      // TODO: handle gracefully if entId doesn't exist in the map
      // (but something's wrong if it isn't there)
    }

    def toIterator: Iterator[(EntityId, Traversable[Int])] = entIdToRowIds.toIterator

    def isolatedEntityIds: Iterator[EntityId] = {
      entIdToRowIds.iterator.collect {case (entId, rowIds) if rowIds.isEmpty => entId}
    }

    def getLinkedRows(entId: EntityId): Traversable[Int] = {
      entIdToRowIds.apply(entId)
      // TODO: handle gracefully if entId doesn't exist in the map
      // (but something's wrong if it isn't there)
    }

    def getLinkedEntity(rowId: Int): EntityId = rowIdToEntId(rowId)
  }

  // ------------------------------------------------------------------------ //
  def updatePartitions(iteration: Long,
                       partitions: Partitions,
                       bcDistProbs: Broadcast[DistortionProbs],
                       partitioner: HardPartitioner,
                       randomSeed: Long,
                       bcPartitionFunction: Broadcast[PartitionFunction[ValueId]],
                       bcRecordsCache: Broadcast[RecordsCache],
                       collapseDistortions: Boolean): Partitions = {
    partitions.mapPartitionsWithIndex { (index, partition) =>
      /** Convenience variables */
      val recordsCache = bcRecordsCache.value

      /** Ensure we get different pseudo-random numbers on each partition and
        * for each iteration */
      implicit val rand: RandomGenerator = new MersenneTwister()
      val newSeed = iteration + index + randomSeed + 1L
      rand.setSeed(newSeed.longValue())

      /** Ensure attribute distributions reference the same RandomGenerator (for
        * this partition and iteration) */
      recordsCache.setRand(rand)

      updatePartition(partition, bcDistProbs.value, bcPartitionFunction.value, recordsCache, collapseDistortions)
    }.partitionBy(partitioner) // Move entity clusters to newly-assigned partitions
     //.persist(StorageLevel.MEMORY_ONLY_SER)
  }



  // ------------------------------------------------------------------------ //
  def updatePartition(itPartition: Iterator[(PartitionId, EntRecPair)],
                      distProbs: DistortionProbs,
                      partitionFunction: PartitionFunction[ValueId],
                      recordsCache: RecordsCache,
                      collapseDistortions: Boolean)
                     (implicit rand: RandomGenerator): Iterator[(PartitionId, EntRecPair)] = {
    /** Convenience variables */
    val indexedAttributes = recordsCache.indexedAttributes

    /** Read data from `itPartition` into memory.
      *
      * Put record-related data into an ArrayBuffer
      * Put entity-related data into an index that supports queries:
      *   attribute value -> entIds and entId -> attribute values
      */
    val bRecords = ArrayBuffer.empty[Record[DistortedValue]]
    val entities = mutable.LongMap.empty[Entity]
    val entityInvertedIndex = new EntityInvertedIndex
    itPartition.foreach { case (_, EntRecPair(entity, record)) =>
      if (record.isDefined) bRecords += record.get
      entities.put(entity.id, entity) match {
        case Some(_) =>
          // Already seen this entity before. Nothing to do.
        case None =>
          // Haven't seen this entity before. Add it to the inverted index
          entityInvertedIndex.add(entity)
      }
    }
    val records = bRecords.toArray

    /** Update the links from records to entities
      *
      * Build an index that supports queries:
      *   entId -> corresponding row ids in `records` ArrayBuffer
      */
    val linksIndex = new LinksIndex(entities.keysIterator, records.length)
    records.iterator.zipWithIndex.foreach { case (record, rowId) =>
      val entId = drawEntityId(records(rowId), entities, entityInvertedIndex, recordsCache)
      //val entId = updateEntityIdCollapsed(records(rowId), entities, entityInvertedIndex, recordsCache, distProbs)
      linksIndex.addLink(entId, rowId)
    }

    /** Update entity attribute values and store in a map (entId -> Entity) */
    val newEntities = updateEntities(records, linksIndex, distProbs, recordsCache, collapseDistortions)

    /** Build output iterator over entity-record pairs (separately for the
      * entity-record pairs and isolated entities) */

    /** Records: insert updated entity attribute values and partitionIds, and update distortions */
    val itRecords = records.iterator.zipWithIndex.map { case (record, rowId) =>
      val entId = linksIndex.getLinkedEntity(rowId)
      val entity = newEntities(entId)
      val newPartitionId = partitionFunction.getPartitionId(entity.values) // TODO: delegate to allEntityValues
      val newRecord = updateDistortions(entity, record, distProbs, indexedAttributes)
      (newPartitionId, EntRecPair(entity, Some(newRecord)))
    }

    /** Isolated entities: insert updated entity attribute values and partitionIds */
    val itIsolatedEntities = linksIndex.isolatedEntityIds.map { entId =>
      val entity = newEntities(entId)
      val newPartitionId = partitionFunction.getPartitionId(entity.values) // TODO: delegate to allEntityValues
      (newPartitionId, EntRecPair(entity, None))
    }

    itRecords ++ itIsolatedEntities
  }


  // ------------------------------------------------------------------------ //
  // Modifies accumulators in-place (doesn't matter because we don't use them
  // anywhere else)
  def updateSummaryVariables(partitions: Partitions,
                             accumulators: SummaryAccumulators,
                             bcDistProbs: Broadcast[DistortionProbs],
                             bcRecordsCache: Broadcast[RecordsCache]): SummaryVars = {
    accumulators.reset()

    partitions.foreachPartition { partition =>
      /** Convenience variables */
      val indexedAttributes = bcRecordsCache.value.indexedAttributes

      val seenEntities = mutable.HashSet.empty[EntityId]

      partition.foreach {
        case (_, EntRecPair(entity, Some(record))) =>
          /** Row is an entity-record pair */

          /** Keep track of whether the entity id for the current pair has been seen before */
          val thisEntityUnseen = seenEntities.add(entity.id)
          /** Count number of distorted attributes for this record */
          var recDistortion = 0

          record.values.iterator.zipWithIndex.foreach { case (DistortedValue(recValue, distorted), attrId) =>
            if (distorted) {
              recDistortion += 1
              accumulators.aggDistortions.add(((attrId, record.fileId), 1L))
              val attribute = indexedAttributes(attrId)
              val prob = if (recValue >= 0) {
                if (attribute.isConstant) {
                  attribute.index.probabilityOf(recValue)
                } else {
                  val entValue = entity.values(attrId)
                  attribute.index.probabilityOf(recValue) *
                    attribute.index.simNormalizationOf(entValue) *
                    attribute.index.expSimOf(recValue, entValue)
                }
              } else 1.0
              accumulators.logLikelihood.add(log(prob))
            }
            if (thisEntityUnseen) {
              val entValue = entity.values(attrId)
              val prob = indexedAttributes(attrId).index.probabilityOf(entValue)
              accumulators.logLikelihood.add(log(prob))
            }
            /** NOTE: assume we don't enter a state where the distortion indicator is false
              * and the attributes disagree. Then the likelihood would be zero.
              */
          }
          accumulators.recDistortions.add((recDistortion, 1L))
        case (_, EntRecPair(entity, None)) =>
          /** Row is an isolated entity (not linked to any records) */
          accumulators.numIsolates.add(1L)

          /** Keep track of whether the entity id for the current pair has been seen before */
          val thisEntityUnseen = seenEntities.add(entity.id)

          if (thisEntityUnseen) {
            (entity.values, indexedAttributes).zipped.foreach { case (entValue, attribute) =>
              val prob = attribute.index.probabilityOf(entValue)
              accumulators.logLikelihood.add(log(prob))
            }
          }
      }
    }

    /** Convenience variables */
    val distProbs = bcDistProbs.value
    val fileSizes = bcRecordsCache.value.fileSizes
    val distortionPrior = bcRecordsCache.value.distortionPrior
    val aggDistortions = accumulators.aggDistortions.value

    /** Add distortion contribution to the log-likelihood on the driver
      * since it depends on the total number of distortions across all
      * partitions.
      */
    distortionPrior.zipWithIndex.foreach { case (BetaShapeParameters(alpha, beta), attrId) =>
      fileSizes.foreach { case (fileId, numRecords) =>
        val distProb = distProbs(attrId, fileId)
        val numDist = aggDistortions.getOrElse((attrId, fileId), 0L)
        accumulators.logLikelihood.add((alpha + numDist - 1.0) * log(distProb) +
          (beta + numRecords - numDist - 1.0) * log(1.0 - distProb))
      }
    }

    SummaryVars(
      accumulators.numIsolates.value.longValue(),
      accumulators.logLikelihood.value.doubleValue(),
      aggDistortions,
      accumulators.recDistortions.value
    )
  }



  // ------------------------------------------------------------------------ //
  // Warning: updates rand in-place
  def drawDistProbs(summaryVars: SummaryVars,
                    recordsCache: RecordsCache)
                   (implicit rand: RandomGenerator): DistortionProbs = {

    val probs = recordsCache.distortionPrior.zipWithIndex.flatMap { case (BetaShapeParameters(alpha, beta), attrId) =>
      recordsCache.fileSizes.map { case (fileId, numRecords) =>
        val numDist = summaryVars.aggDistortions.getOrElse((attrId, fileId), 0L)
        val effNumDist = numDist.toDouble + alpha
        val effNumNonDist = numRecords.toDouble - numDist.toDouble + beta
        val thisProb = new BetaDistribution(rand, effNumDist, effNumNonDist)
        ((attrId, fileId), thisProb.sample())
      }
    }.toMap

    DistortionProbs(probs)
  }



  // ------------------------------------------------------------------------ //
  // Warning: updates rand in-place
  def updateDistortions(entity: Entity,
                        record: Record[DistortedValue],
                        distProbs: DistortionProbs,
                        indexedAttributes: IndexedSeq[IndexedAttribute])
                       (implicit rand: RandomGenerator): Record[DistortedValue] = {
    val newValues = Array.tabulate(record.values.length) { attrId =>
      val distRecValue = record.values(attrId)
      if (distRecValue.value < 0) {
        // Record attribute is unobserved
        val distProb = distProbs(attrId, record.fileId)
        distRecValue.copy(distorted = rand.nextDouble() < distProb)
      } else {
        // Record attribute is observed
        if (distRecValue.value == entity.values(attrId)) {
          // Record and entity attribute values agree, so draw distortion indicator randomly
          val indexedAttribute = indexedAttributes(attrId)
          val recValue = distRecValue.value
          val distProb = distProbs(attrId, record.fileId)
          val pr1 = if (indexedAttribute.isConstant) {
            distProb * indexedAttribute.index.probabilityOf(recValue)
          } else {
            distProb * indexedAttribute.index.probabilityOf(recValue) *
              indexedAttribute.index.simNormalizationOf(recValue) *
              indexedAttribute.index.expSimOf(recValue, recValue)
          }
          val pr0 = 1.0 - distProb
          val p = if (pr1 + pr0 != 0.0) pr1 / (pr1 + pr0) else 0.0
          distRecValue.copy(distorted = rand.nextDouble() < p) // bernoulli draw
        } else {
          // Record and entity attribute values disagree, so attribute is distorted with certainty
          distRecValue.copy(distorted = true)
        }
      }
    }
    record.copy(values = newValues)
  }

  def drawEntityIdCollapsed(record: Record[DistortedValue],
                            entities: mutable.LongMap[Entity],
                            entityInvertedIndex: EntityInvertedIndex,
                            recordsCache: RecordsCache,
                            distProbs: DistortionProbs)
                           (implicit rand: RandomGenerator): EntityId = {
    val indexedAttributes = recordsCache.indexedAttributes
    val entIds = entities.keys.toArray
    val valuesAndWeights = entities.mapValues { entity =>
      entity.values.iterator.zipWithIndex.foldLeft(1.0) { case (weight, (entValue, attrId)) =>
        val constAttr = indexedAttributes(attrId).isConstant
        val attributeIndex = indexedAttributes(attrId).index
        val recValue = record.values(attrId).value
        val recValueProb = attributeIndex.probabilityOf(recValue)
        val distProb = distProbs(attrId, record.fileId)
        if (constAttr) {
          weight * ((if (recValue == entValue) 1.0 - distProb else 0.0) +
            distProb * recValueProb)
        } else {
          weight * ((if (recValue == entValue) 1.0 - distProb else 0.0) +
            distProb * recValueProb * attributeIndex.simNormalizationOf(entValue) *
              attributeIndex.expSimOf(recValue, entValue))
        }
      }
    }
    DiscreteDist(valuesAndWeights).sample()
  }

  def drawEntityId(record: Record[DistortedValue],
                   entities: mutable.LongMap[Entity],
                   entityInvertedIndex: EntityInvertedIndex,
                   recordsCache: RecordsCache)
                  (implicit rand: RandomGenerator): EntityId = {

    val (possibleEntityIds, obsDistNonConstAttrIds) = getPossibleEntities(record, entities.keysIterator,
      entityInvertedIndex, recordsCache.indexedAttributes)

    if (obsDistNonConstAttrIds.isEmpty) {
      /** No observed, distorted, non-constant record attributes implies distribution over possible entity
        * ids is uniform */
      val uniformIdx = rand.nextInt(possibleEntityIds.length)
      possibleEntityIds(uniformIdx)
    } else {
      /** Some observed, distorted, non-constant record attributes implies distribution over possible entity
        * ids is non-uniform */
      val weights = possibleEntityIds.map { entId =>
        obsDistNonConstAttrIds.foldLeft(1.0) { (weight, attrId) =>
          val entValue = entities(entId).values(attrId)
          val distRecValue = record.values(attrId)
          val attributeIndex = recordsCache.indexedAttributes(attrId).index
          weight * attributeIndex.simNormalizationOf(entValue) * attributeIndex.expSimOf(distRecValue.value, entValue)
        }
      }
      val randIdx = DiscreteDist(weights).sample()
      possibleEntityIds(randIdx)
    }
  }

  def getPossibleEntities(record: Record[DistortedValue],
                          allEntityIds: Iterator[EntityId],
                          entityInvertedIndex: EntityInvertedIndex,
                          indexedAttributes: IndexedSeq[IndexedAttribute]):
      (IndexedSeq[EntityId], Seq[AttributeId]) = {

    /** Keep track of any observed, distorted record attributes with a non-constant similarity fn */
    val obsDistNonConstAttrIds = mutable.ArrayBuffer.empty[AttributeId]

    /** Build an array of sets of entity ids (one set for each observed, non-distorted record attribute) */
    val bSets = Array.newBuilder[scala.collection.Set[EntityId]]
    var attrId = 0
    while (attrId < indexedAttributes.length) {
      val distRecValue = record.values(attrId)
      if (distRecValue.value >= 0) { // Record attribute is observed
        if (!distRecValue.distorted) { // Record attribute is not distorted
          bSets += entityInvertedIndex.getEntityIds(attrId, distRecValue.value)
        } else if (!indexedAttributes(attrId).isConstant) { // Record attribute is distorted and non-constant
          obsDistNonConstAttrIds += attrId
        }
      }
      attrId += 1
    }

    /** Sort sets in increasing order of size to improve the efficiency of the multiple set
      * intersection algorithm */
    val sets = bSets.result().sortBy(x => x.size)

    /** Now compute the multiple set intersection, but first handle special cases */
    if (sets.isEmpty) {
      /** All of the record attributes are distorted or unobserved, so return all entity ids as possibilities */
      (allEntityIds.toIndexedSeq, obsDistNonConstAttrIds)
    } else if (sets.length == 1) {
      /** No need to compute intersection for a single set */
      (sets.head.toIndexedSeq, obsDistNonConstAttrIds)
    } else {
      /** ArrayBuffer to store result of the multiple set intersection */
      var result = mutable.ArrayBuffer.empty[EntityId]

      /** Compute the intersection of the first and second sets and store in `result` */
      val firstSet = sets(0)
      val secondSet = sets(1)
      firstSet.foreach { entId => if (secondSet.contains(entId)) result += entId }

      /** Update `result` after intersecting with each of the remaining sets */
      var i = 2
      while (i < sets.length) {
        val temp = mutable.ArrayBuffer.empty[EntityId]

        val newSet = sets(i)
        result.foreach { entId => if (newSet.contains(entId)) temp += entId}
        result = temp

        i += 1
      }

      (result, obsDistNonConstAttrIds)
    }
  }

  def perturbedDistYCollapsed(attrId: AttributeId,
                              constAttr: Boolean,
                              attributeIndex: AttributeIndex,
                              records: Array[Record[DistortedValue]],
                              observedLinkedRowIds: Iterator[Int],
                              distProbs: DistortionProbs,
                              baseDistribution: DiscreteDist[ValueId])
                             (implicit rand: RandomGenerator): DiscreteDist[ValueId] = {

    val valuesWeights = mutable.HashMap.empty[ValueId, Double]

    while (observedLinkedRowIds.hasNext) {
      val rowId = observedLinkedRowIds.next()
      val record = records(rowId)
      val distProb = distProbs(attrId, record.fileId)
      val distRecValue = record.values(attrId)
      val recValueProb = attributeIndex.probabilityOf(distRecValue.value)

      if (constAttr) {
        val weight = 1.0 + (1.0 / distProb - 1.0) / recValueProb
        /** If key already exists, do a multiplicative update, otherwise
          * add a new key with value `weight` */
        valuesWeights.update(distRecValue.value, weight * valuesWeights.getOrElse(distRecValue.value, 1.0))
      } else {
        val recValueNorm = attributeIndex.simNormalizationOf(distRecValue.value)
        /** Iterate over values similar to record value */
        attributeIndex.simValuesOf(distRecValue.value).foreach { case (simValue, expSim) =>
          val weight = if (distRecValue.value == simValue) expSim + (1.0 / distProb - 1.0) / (recValueProb * recValueNorm) else expSim
          /** If key already exists, do a multiplicative update, otherwise
            * add a new key with value `weight` */
          valuesWeights.update(simValue, weight * valuesWeights.getOrElse(simValue, 1.0))
        }
      }
    }

    valuesWeights.transform((valueId, weight) =>  baseDistribution.probabilityOf(valueId) * (weight - 1.0))

    DiscreteDist(valuesWeights)
  }

  def drawEntityValueCollapsed(attrId: AttributeId,
                               indexedAttribute: IndexedAttribute,
                               records: Array[Record[DistortedValue]],
                               linkedRowIds: Traversable[Int],
                               distProbs: DistortionProbs)
                              (implicit rand: RandomGenerator): ValueId = {
    val observedLinkedRowIds = linkedRowIds.filter(rowId => records(rowId).values(attrId).value >= 0)
    val constAttribute = indexedAttribute.isConstant
    val baseDistribution = if (!constAttribute && observedLinkedRowIds.nonEmpty) {
      indexedAttribute.index.getSimNormDist(observedLinkedRowIds.size)
    } else indexedAttribute.index.distribution

    if (observedLinkedRowIds.isEmpty) {
      baseDistribution.sample()
    } else {
      val perturbDistribution = perturbedDistYCollapsed(attrId, constAttribute,
        indexedAttribute.index, records, observedLinkedRowIds.toIterator, distProbs, baseDistribution)
      if (rand.nextDouble() < 1.0/(1.0 + perturbDistribution.totalWeight)) {
        baseDistribution.sample()
      } else {
        perturbDistribution.sample()
      }
    }
  }

  def drawEntityValue(attrId: AttributeId,
                      indexedAttribute: IndexedAttribute,
                      records: Array[Record[DistortedValue]],
                      linkedRowIds: Traversable[Int])
                     (implicit rand: RandomGenerator): ValueId = {
    val constAttribute = indexedAttribute.isConstant
    val baseDistribution = if (!constAttribute && linkedRowIds.nonEmpty) {
      indexedAttribute.index.getSimNormDist(linkedRowIds.size)
    } else indexedAttribute.index.distribution

    if (linkedRowIds.isEmpty) {
      baseDistribution.sample()
    } else {
      /** Search for an observed, non-distorted value */
      var nonDistortedValue: ValueId = -1
      val itLinkedRowIds = linkedRowIds.toIterator
      while (itLinkedRowIds.hasNext && nonDistortedValue < 0) {
        val rowId = itLinkedRowIds.next()
        val distRecValue = records(rowId).values(attrId)
        if (!distRecValue.distorted && distRecValue.value >= 0) nonDistortedValue = distRecValue.value
      }

      if (nonDistortedValue >= 0) {
        /** Observed, non-distorted value exists, so the new value is determined */
        nonDistortedValue
      } else {
        /** All linked record values are distorted or unobserved for this attribute. */
        if (constAttribute) {
          baseDistribution.sample()
        } else {
          val perturbDistribution = perturbedDistY(attrId, indexedAttribute.index,
            records, linkedRowIds.toIterator, baseDistribution)
          if (rand.nextDouble() < 1.0/(1.0 + perturbDistribution.totalWeight)) {
            baseDistribution.sample()
          } else {
            perturbDistribution.sample()
          }
        }
      }
    }
  }

  def perturbedDistY(attrId: AttributeId,
                     attributeIndex: AttributeIndex,
                     records: Array[Record[DistortedValue]],
                     linkedRowIds: Iterator[Int],
                     baseDistribution: DiscreteDist[ValueId])
                    (implicit rand: RandomGenerator): DiscreteDist[ValueId] = {

    val valuesWeights = mutable.HashMap.empty[ValueId, Double]

    while (linkedRowIds.hasNext) {
      val rowId = linkedRowIds.next()
      val distRecValue = records(rowId).values(attrId)

      if (distRecValue.value >= 0) { // Record value is observed
        /** Iterate over values similar to record value */
        attributeIndex.simValuesOf(distRecValue.value).foreach { case (simValue, expSim) =>
          /** If key already exists, do a multiplicative update, otherwise
            * add a new key with value `expSim` */
          valuesWeights.update(simValue, expSim * valuesWeights.getOrElse(simValue, 1.0))
        }
      }
    }

    valuesWeights.transform((k,v) => baseDistribution.probabilityOf(k) * (v - 1.0))

    DiscreteDist(valuesWeights)
  }

  // ------------------------------------------------------------------------ //
  // Warning: updates valuesDist in-place
  def updateEntities(records: Array[Record[DistortedValue]],
                     linksIndex: LinksIndex,
                     distProbs: DistortionProbs,
                     recordsCache: RecordsCache,
                     collapseDistortions: Boolean)
                    (implicit rand: RandomGenerator): mutable.LongMap[Entity] = {
    val newEntities = mutable.LongMap.empty[Entity] // TODO: use builder?
    linksIndex.toIterator.foreach { case (entId, linkedRowIds) =>
      val entityValues = Array.tabulate(recordsCache.numAttributes) { attrId =>
        val indexedAttribute = recordsCache.indexedAttributes(attrId)
        if (collapseDistortions) {
          drawEntityValueCollapsed(attrId, indexedAttribute, records, linkedRowIds, distProbs)
        } else {
          drawEntityValue(attrId, indexedAttribute, records, linkedRowIds)
        }
      }
      newEntities += (entId -> Entity(entId, entityValues))
    }
    newEntities
  }
}


