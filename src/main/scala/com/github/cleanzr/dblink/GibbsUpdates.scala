// Copyright (C) 2018  Australian Bureau of Statistics
// Copyright (C) 2018  Neil Marchant
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

import com.github.cleanzr.dblink.util.HardPartitioner
import com.github.cleanzr.dblink.random.DiscreteDist
import com.github.cleanzr.dblink.partitioning.PartitionFunction
import org.apache.commons.math3.distribution.BetaDistribution
import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.log

object GibbsUpdates {

  /** Lightweight inverted index for the entity attribute values.
    *
    * Only supports insertion and querying. Updating is not required since the index is rebuilt from scratch at the
    * beginning of each iteration.
    */
  class EntityInvertedIndex extends Serializable {
    private val attrValueToEntIds =
      mutable.HashMap.empty[(AttributeId, ValueId), mutable.Set[EntityId]]
    
    /** Method to add a single (attribute value -> entId) combination to the index */
    private def add_attribute(attrId: AttributeId, valueId: ValueId,
                              entId: EntityId): Unit = {
      attrValueToEntIds.get((attrId, valueId)) match {
        case Some(currentEntities) => currentEntities.add(entId)
        case None => attrValueToEntIds.update((attrId, valueId), mutable.Set(entId))
      }
    }
    
    /** Add an entity to the inverted index
      *
      * @param entId unique id for the entity (only needs to be unique within the partition)
      * @param entity entity attribute values
      */
    def add(entId: EntityId, entity: Entity): Unit = {
      var attrId = 0
      while (attrId < entity.values.length) {
        this.add_attribute(attrId, entity.values(attrId), entId)
        attrId += 1
      }
    }

    /** Query entities which match on an attribute value
      * 
      * @param attrId query attribute id
      * @param valueId query value id (corresponding to the attribute id)
      * @return set of entity ids that match the queried attribute value
      */
    def getEntityIds(attrId: AttributeId, valueId: ValueId): scala.collection.Set[EntityId] = {
      attrValueToEntIds.getOrElse((attrId, valueId), mutable.Set.empty[EntityId])
    }
  }



  /** An index from entity ids to record array ids
    *
    * Rows corresponding to isolated entities (i.e. not records) are omitted
    */
  class LinksIndex(numEntities: Int, numRecords: Int) extends Serializable {
    private val entIdToRecIds = Array.fill(numEntities)(ArrayBuffer.empty[Int])

    private val recIdToEntId = Array.fill[EntityId](numRecords)(-1)

    /** Add a link between an entity and a record
      *
      * @param entId id for the entity (at the partition-level)
      * @param recId id of the record (at the partition-level)
      */
    def addLink(entId: EntityId, recId: Int): Unit = {
      recIdToEntId(recId) = entId
      entIdToRecIds(entId) += recId
      // TODO: handle gracefully if entId doesn't exist in the map
      // (but something's wrong if it isn't there)
    }

    /** Get ids of entitys which are non-isolated (i.e. are linked to records) */
    def nonIsolatedEntityIds: Iterator[EntityId] = {
      entIdToRecIds.iterator.zipWithIndex.collect {case (recIds, entId) if recIds.nonEmpty => entId}
    }

    /** Get ids of records that are linked to a particular entity
      *
      * @param entId id for the entity (at the partition-level)
      * @return linked record ids (at the partition-level)
      */
    def getLinkedRows(entId: EntityId): Traversable[Int] = entIdToRecIds(entId)

    /** Get linked entity id for a particular record
      *
      * @param recId id of the record (at the partition-level)
      * @return id of linked entity (at the partition-level)
      */
    def getLinkedEntity(recId: Int): EntityId = recIdToEntId(recId)
  }



  /** Updates all partitions */
  def updatePartitions(partitions: Partitions,
                       populationSize: Int,
                       bcDistProbs: Broadcast[DistortionProbs],
                       partitioner: HardPartitioner,
                       currentRandomSeed: Long,
                       bcPartitionFunction: Broadcast[PartitionFunction[ValueId]],
                       bcRecordsCache: Broadcast[RecordsCache],
                       collapsedEntityId: Boolean,
                       collapsedEntityValues: Boolean,
                       sequential: Boolean): (Partitions, Long) = {

    // Step 1: Update the linkage structure, entity values and distortion indicators. The isolated entities are
    // updated in this step if the the population size is fixed.
    val step1 = partitions.mapPartitionsWithIndex((index, partition) => {
      // Ensure we get different pseudo-random numbers on each partition and for each iteration
      val newSeed = index + currentRandomSeed
      implicit val rand: RandomGenerator = new MersenneTwister(newSeed.longValue())

      updatePartition(partition, bcDistProbs.value, bcPartitionFunction.value, bcRecordsCache.value, collapsedEntityId,
        collapsedEntityValues, sequential)
    }).partitionBy(partitioner)

    // Increment the random seed by 1 for each partition, per mapPartitions operation
    val newRandomSeed = currentRandomSeed + partitioner.numPartitions

    // Step 3: Move entity clusters to newly-assigned partitions
    val step2 = step1.partitionBy(partitioner)

    (step2, newRandomSeed)
  }

  /** Updates a single partition */
  def updatePartition(itPartition: Iterator[(PartitionId, EntRecCluster)],
                      distProbs: DistortionProbs,
                      partitionFunction: PartitionFunction[ValueId],
                      recordsCache: RecordsCache,
                      collapsedEntityId: Boolean,
                      collapsedEntityValues: Boolean,
                      sequential: Boolean)
                     (implicit rand: RandomGenerator): Iterator[(PartitionId, EntRecCluster)] = {
    // Convenience variables
    val indexedAttributes = recordsCache.indexedAttributes

    /*
    Read data from iterator into memory

    Record-related data goes into an ArrayBuffer (need fast random access)
    Entity-related data goes into forward/reverse indices to supports queries:
      attribute value -> entIds with that value, and
      entId -> entity's attribute values
    */
    val records = ArrayBuffer.empty[Record[DistortedValue]]
    val entityInvertedIndex = new EntityInvertedIndex

    val entities = itPartition.zipWithIndex.map { case ((_, EntRecCluster(ent, recs)), entId) =>
      if (recs.isDefined) records ++= recs.get
      if (!sequential) {
        entityInvertedIndex.add(entId, ent)
      }
      ent
    }.toArray

    /*
    Update the links from records to entities

    Build an index that supports queries:
      entId -> corresponding row ids in `records` ArrayBuffer
    */
    val linksIndex = new LinksIndex(entities.length, records.length)
    records.iterator.zipWithIndex.foreach { case (record, rowId) =>
      val entId = if (sequential) updateEntityIdSeq(record, entities, recordsCache)
      else if (collapsedEntityId) updateEntityIdCollapsed(record, entities, entityInvertedIndex, recordsCache, distProbs)
      else updateEntityId(record, entities, entityInvertedIndex, recordsCache)
      linksIndex.addLink(entId, rowId)
    }

    // Update entity attribute values in-place
    updateEntityValues(records, entities, linksIndex, distProbs, recordsCache, collapsedEntityValues, sequential)

    // Return an iterator over the updated clusters, while updating the partition assignments and update distortion
    // indicators
    entities.iterator.zipWithIndex.map { case (entity, entId) =>
      val newPartitionId = partitionFunction.getPartitionId(entity.values)
      val linkedRecords = linksIndex.getLinkedRows(entId)
        .map(recId => updateDistortions(entity, records(recId), distProbs, indexedAttributes)).toArray
      (newPartitionId, EntRecCluster(entity, if (linkedRecords.isEmpty) None else Some(linkedRecords)))
    }
  }


  // Modifies accumulators in-place (doesn't matter because we don't use them
  // anywhere else)
  /** Updates summary variables (aggregate distortion per file/attribute, log-likelihood, number of
    * isolated entities) for a given state
    */
  def updateSummaryVariables(partitions: Partitions,
                             accumulators: SummaryAccumulators,
                             distProbs: DistortionProbs,
                             bcRecordsCache: Broadcast[RecordsCache]): SummaryVars = {
    accumulators.reset()

    partitions.foreachPartition { partition =>
      // Convenience variables
      val indexedAttributes = bcRecordsCache.value.indexedAttributes

      partition.foreach {
        case (_, EntRecCluster(entity, Some(records))) =>
          // Entity cluster is non-isolated (contains linked some records)

          // Contribution from the entity attribute values
          (entity.values, indexedAttributes).zipped.foreach { case (entValue, attribute) =>
            val prob = attribute.index.probabilityOf(entValue)
            accumulators.logLikelihood.add(log(prob))
          }

          // Contribution from the linked records
          records.foreach { record =>
            // Count number of distorted attributes for this record
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


              // NOTE: assume we don't enter a state where the distortion indicator is false and the attributes
              // disagree. Then the likelihood would be zero.
            }
            accumulators.recDistortions.add((recDistortion, 1L))
          }
        case (_, EntRecCluster(entity, None)) =>
          // Entity is isolated (not linked to any records)
          accumulators.numIsolates.add(1L)

          (entity.values, indexedAttributes).zipped.foreach { case (entValue, attribute) =>
            val prob = attribute.index.probabilityOf(entValue)
            accumulators.logLikelihood.add(log(prob))
          }
      }
    }

    // Convenience variables
    val fileSizes = bcRecordsCache.value.fileSizes
    val distortionPrior = bcRecordsCache.value.distortionPrior
    val aggDistortions = accumulators.aggDistortions.value

    // Add remaining contributions to the log-likelihood on the driver

    // Add distortion contribution, depends on the aggregate distortion across all records
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


  /** Updates distortion probabilities (on driver) */
  def updateDistProbs(summaryVars: SummaryVars,
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


  /** Updates distortions for a given record */
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


  /** Updates assigned entity for a given record, while collapsing the distortions for the record */
  def updateEntityIdCollapsed(record: Record[DistortedValue],
                              entities: Array[Entity],
                              entityInvertedIndex: EntityInvertedIndex,
                              recordsCache: RecordsCache,
                              distProbs: DistortionProbs)
                             (implicit rand: RandomGenerator): EntityId = {
    val indexedAttributes = recordsCache.indexedAttributes
    val weights = entities.map { entity =>
      entity.values.iterator.zipWithIndex.foldLeft(1.0) { case (weight, (entValue, attrId)) =>
        val recValue = record.values(attrId).value
        if (recValue < 0) {
          // Record attribute is missing: weight is unchanged
          weight
        } else {
          // Record attribute is observed: need to update weight
          val constAttr = indexedAttributes(attrId).isConstant
          val attributeIndex = indexedAttributes(attrId).index

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
    }
    DiscreteDist(weights).sample()
  }


  /** Updates assigned entity for a given record using an ordinary Gibbs update */
  def updateEntityId(record: Record[DistortedValue],
                     entities: Array[Entity],
                     entityInvertedIndex: EntityInvertedIndex,
                     recordsCache: RecordsCache)
                    (implicit rand: RandomGenerator): EntityId = {

    val (possibleEntityIds, obsDistAttrIds) = getPossibleEntities(record, entities.indices.iterator,
      entityInvertedIndex, recordsCache.indexedAttributes)

    if (obsDistAttrIds.isEmpty) {
      // No observed, distorted record attributes implies distribution over possible entity ids is uniform
      val uniformIdx = rand.nextInt(possibleEntityIds.length)
      possibleEntityIds(uniformIdx)
    } else {
      // Some observed, distorted record attributes implies distribution over possible entity ids is non-uniform
      val weights = possibleEntityIds.map { entId =>
        obsDistAttrIds.foldLeft(1.0) { (weight, attrId) =>
          val entValue = entities(entId).values(attrId)
          val distRecValue = record.values(attrId)
          val indexedAttribute = recordsCache.indexedAttributes(attrId)
          val attributeIndex = indexedAttribute.index
          if (indexedAttribute.isConstant) {
            weight * attributeIndex.probabilityOf(distRecValue.value)
          } else {
            weight * attributeIndex.simNormalizationOf(entValue) * attributeIndex.expSimOf(distRecValue.value, entValue) * attributeIndex.probabilityOf(distRecValue.value)
          }
        }
      }
      val randIdx = DiscreteDist(weights).sample()
      possibleEntityIds(randIdx)
    }
  }


  /** Updates assigned entity for a given record using an ordinary Gibbs update (without using an inverted index)  */
  def updateEntityIdSeq(record: Record[DistortedValue],
                        entities: Array[Entity],
                        recordsCache: RecordsCache)
                       (implicit rand: RandomGenerator): EntityId = {

    val weights = entities.toTraversable.map { entity =>
      var weight = 1.0
      var attrId = 0
      val itRecordValues = record.values.iterator
      val entValues = entity.values
      while (itRecordValues.hasNext && weight > 0) {
        val distRecValue = itRecordValues.next()
        if (distRecValue.value >= 0) {
          // Record attribute is observed
          val entValue = entValues(attrId)
          if (!distRecValue.distorted) {
            if (distRecValue.value != entValue) weight = 0.0
          } else {
            val indexedAttribute = recordsCache.indexedAttributes(attrId)
            val index = indexedAttribute.index
            if (indexedAttribute.isConstant) {
              weight *= index.probabilityOf(distRecValue.value)
            } else {
              weight *= index.simNormalizationOf(entValue) * index.expSimOf(distRecValue.value, entValue) * index.probabilityOf(distRecValue.value)
            }
          }
        }
        attrId += 1
      }
      weight
    }
    DiscreteDist(weights).sample()
  }


  /** Computes a set of candidate entities for a given record, which are compatible with the attributes/distortions.
    * For example, if the first record attribute has a non-distorted value of "10", we can immediately eliminate
    * any entities whose first attribute value is not "10".
    */
  def getPossibleEntities(record: Record[DistortedValue],
                          allEntityIds: Iterator[EntityId],
                          entityInvertedIndex: EntityInvertedIndex,
                          indexedAttributes: IndexedSeq[IndexedAttribute]):
      (IndexedSeq[EntityId], Seq[AttributeId]) = {

    // Keep track of any observed, distorted record attributes
    val obsDistAttrIds = mutable.ArrayBuffer.empty[AttributeId]

    // Build an array of sets of entity ids (one set for each observed, non-distorted record attribute)
    val sets = mutable.ArrayBuffer.empty[scala.collection.Set[EntityId]]
    var attrId = 0
    while (attrId < indexedAttributes.length) {
      val distRecValue = record.values(attrId)
      if (distRecValue.value >= 0) { // Record attribute is observed
        if (!distRecValue.distorted) { // Record attribute is not distorted
          sets += entityInvertedIndex.getEntityIds(attrId, distRecValue.value)
        } else { // Record attribute is distorted
          obsDistAttrIds += attrId
        }
      }
      attrId += 1
    }

    // Sort sets in increasing order of size to improve the efficiency of the multiple set intersection algorithm
    val sortedSets = sets.sortBy(x => x.size)

    // Now compute the multiple set intersection, but first handle special cases
    if (sortedSets.isEmpty) {
      // All of the record attributes are distorted or unobserved, so return all entity ids as possibilities
      (allEntityIds.toIndexedSeq, obsDistAttrIds)
    } else if (sortedSets.length == 1) {
      // No need to compute intersection for a single set
      (sortedSets.head.toIndexedSeq, obsDistAttrIds)
    } else {
      // ArrayBuffer to store result of the multiple set intersection
      var result = mutable.ArrayBuffer.empty[EntityId]

      // Compute the intersection of the first and second sets and store in `result`
      val firstSet = sortedSets(0)
      val secondSet = sortedSets(1)
      firstSet.foreach { entId => if (secondSet.contains(entId)) result += entId }

      // Update `result` after intersecting with each of the remaining sets
      var i = 2
      while (i < sortedSets.length) {
        val temp = mutable.ArrayBuffer.empty[EntityId]

        val newSet = sortedSets(i)
        result.foreach { entId => if (newSet.contains(entId)) temp += entId}
        result = temp

        i += 1
      }

      (result, obsDistAttrIds)
    }
  }


  /** Computes the perturbation distribution corresponding to updateEntityValueCollapsed */
  def perturbedDistYCollapsed(attrId: AttributeId,
                              constAttr: Boolean,
                              attributeIndex: AttributeIndex,
                              records: IndexedSeq[Record[DistortedValue]],
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
        // If key already exists, do a multiplicative update, otherwise add a new key with value `weight`
        valuesWeights.update(distRecValue.value, weight * valuesWeights.getOrElse(distRecValue.value, 1.0))
      } else {
        val recValueNorm = attributeIndex.simNormalizationOf(distRecValue.value)
        // Iterate over values similar to record value
        attributeIndex.simValuesOf(distRecValue.value).foreach { case (simValue, expSim) =>
          val weight = if (distRecValue.value == simValue) expSim + (1.0 / distProb - 1.0) / (recValueProb * recValueNorm) else expSim
          // If key already exists, do a multiplicative update, otherwise add a new key with value `weight`
          valuesWeights.update(simValue, weight * valuesWeights.getOrElse(simValue, 1.0))
        }
      }
    }

    valuesWeights.transform((valueId, weight) =>  baseDistribution.probabilityOf(valueId) * (weight - 1.0))

    DiscreteDist(valuesWeights)
  }


  /** Updates an attribute value for a given entity (represented by a set of linked records).
    * Collapses the distortion indicators and uses perturbation sampling.
    */
  def updateEntityValueCollapsed(attrId: AttributeId,
                                 indexedAttribute: IndexedAttribute,
                                 records: IndexedSeq[Record[DistortedValue]],
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


  /** Updates an attribute value for a given entity (represented by a set of linked records).
    * Uses a Gibbs update with perturbation sampling.
    */
  def updateEntityValue(attrId: AttributeId,
                        indexedAttribute: IndexedAttribute,
                        records: IndexedSeq[Record[DistortedValue]],
                        linkedRowIds: Traversable[Int])
                       (implicit rand: RandomGenerator): ValueId = {
    val observedLinkedRowIds = linkedRowIds.filter(rowId => records(rowId).values(attrId).value >= 0)
    val constAttribute = indexedAttribute.isConstant
    val baseDistribution = if (!constAttribute && observedLinkedRowIds.nonEmpty) {
      indexedAttribute.index.getSimNormDist(observedLinkedRowIds.size)
    } else indexedAttribute.index.distribution

    if (observedLinkedRowIds.isEmpty) {
      baseDistribution.sample()
    } else {
      // Search for an observed, non-distorted value
      var nonDistortedValue: ValueId = -1
      val itLinkedRowIds = observedLinkedRowIds.toIterator
      while (itLinkedRowIds.hasNext && nonDistortedValue < 0) {
        val rowId = itLinkedRowIds.next()
        val distRecValue = records(rowId).values(attrId)
        if (!distRecValue.distorted) nonDistortedValue = distRecValue.value
      }

      if (nonDistortedValue >= 0) {
        // Observed, non-distorted value exists, so the new value is determined
        nonDistortedValue
      } else {
        // All observed linked record values are distorted for this attribute.
        if (constAttribute) {
          baseDistribution.sample()
        } else {
          val perturbDistribution = perturbedDistY(attrId, indexedAttribute.index,
            records, observedLinkedRowIds.toIterator, baseDistribution)
          if (rand.nextDouble() < 1.0/(1.0 + perturbDistribution.totalWeight)) {
            baseDistribution.sample()
          } else {
            perturbDistribution.sample()
          }
        }
      }
    }
  }


  /** Updates an attribute value for a given entity (represented by a set of linked records).
    * Uses a Gibbs update without perturbation sampling.
    */
  def updateEntityValueSeq(attrId: AttributeId,
                           indexedAttribute: IndexedAttribute,
                           records: IndexedSeq[Record[DistortedValue]],
                           linkedRowIds: Traversable[Int])
                          (implicit rand: RandomGenerator): ValueId = {
    val observedLinkedRowIds = linkedRowIds.filter(rowId => records(rowId).values(attrId).value >= 0)
    val index = indexedAttribute.index
    val constAttribute = indexedAttribute.isConstant

    if (observedLinkedRowIds.isEmpty) {
      index.draw()
    } else {
      var nonDistortedValue: ValueId = -1
      val itLinkedRowIds = observedLinkedRowIds.toIterator
      while (itLinkedRowIds.hasNext && nonDistortedValue < 0) {
        val rowId = itLinkedRowIds.next()
        val distRecValue = records(rowId).values(attrId)
        if (!distRecValue.distorted) nonDistortedValue = distRecValue.value
      }

      if (nonDistortedValue >= 0) {
        // Observed, non-distorted value exists, so the new value is determined
        nonDistortedValue
      } else {
        // All observed linked record values are distorted for this attribute
        if (constAttribute) {
          index.draw()
        } else {
          val valuesAndWeights = index.distribution.toIterator.map { case (valId, prob) =>
            var weight = prob
            val itLinkedRowIds = observedLinkedRowIds.toIterator
            while (itLinkedRowIds.hasNext && weight > 0) {
              val rowId = itLinkedRowIds.next()
              val distRecValue = records(rowId).values(attrId)
              if (!distRecValue.distorted) {
                if (distRecValue.value != valId) weight = 0.0
              } else {
                weight *= index.expSimOf(distRecValue.value, valId) * index.simNormalizationOf(valId) * index.probabilityOf(distRecValue.value)
              }
            }
            (valId, weight)
          }.toMap
          DiscreteDist(valuesAndWeights).sample()
        }
      }
    }
  }


  /** Computes the perturbation distribution corresponding to updateEntityValue */
  def perturbedDistY(attrId: AttributeId,
                     attributeIndex: AttributeIndex,
                     records: IndexedSeq[Record[DistortedValue]],
                     observedLinkedRowIds: Iterator[Int],
                     baseDistribution: DiscreteDist[ValueId])
                    (implicit rand: RandomGenerator): DiscreteDist[ValueId] = {

    val valuesWeights = mutable.HashMap.empty[ValueId, Double]

    while (observedLinkedRowIds.hasNext) {
      val rowId = observedLinkedRowIds.next()
      val distRecValue = records(rowId).values(attrId)

      if (distRecValue.value >= 0) { // Record value is observed
        // Iterate over values similar to record value
        attributeIndex.simValuesOf(distRecValue.value).foreach { case (simValue, expSim) =>
          // If key already exists, do a multiplicative update, otherwise add a new key with value `expSim`
          valuesWeights.update(simValue, expSim * valuesWeights.getOrElse(simValue, 1.0))
        }
      }
    }

    valuesWeights.transform((k,v) => baseDistribution.probabilityOf(k) * (v - 1.0))

    DiscreteDist(valuesWeights)
  }


  /** Updates the attribute values for all entities in-place */
  def updateEntityValues(records: IndexedSeq[Record[DistortedValue]],
                         entities: Array[Entity],
                         linksIndex: LinksIndex,
                         distProbs: DistortionProbs,
                         recordsCache: RecordsCache,
                         collapsedEntityValues: Boolean,
                         sequential: Boolean)
                        (implicit rand: RandomGenerator): Unit = {
    for (entId <- entities.indices) {
      val linkedRowIds = linksIndex.getLinkedRows(entId)
      val entityValues = Array.tabulate(recordsCache.numAttributes) { attrId =>
        val indexedAttribute = recordsCache.indexedAttributes(attrId)
        if (sequential) {
          updateEntityValueSeq(attrId, indexedAttribute, records, linkedRowIds)
        } else {
          if (collapsedEntityValues) {
            updateEntityValueCollapsed(attrId, indexedAttribute, records, linkedRowIds, distProbs)
          } else {
            updateEntityValue(attrId, indexedAttribute, records, linkedRowIds)
          }
        }
      }
      entities(entId) = Entity(entityValues)
    }
  }
}


