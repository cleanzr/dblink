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

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.github.cleanzr.dblink.partitioning.PartitionFunction
import com.github.cleanzr.dblink.util.{HardPartitioner, PeriodicRDDCheckpointer}
import com.github.cleanzr.dblink.GibbsUpdates.{updateDistProbs, updatePartitions, updateSummaryVariables}
import com.github.cleanzr.dblink.partitioning.PartitionFunction
import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** State of the Markov chain
  *
  * @param iteration iteration counter.
  * @param partitions RDD containing Partition-Entity-Record triples
  *                   and their associated parameters (entity attribute values,
  *                   record distortion indicators and record attribute values).
  * @param distProbs map containing the distortion probabilities for each
  *                  fileId and attributeId.
  * @param summaryVars collection of quantities that summarise the state. Each
  *                    quantity is a function of `partitions` and `distProbs`.
  * @param accumulators accumulators used for calculating `summaryVars`.
  * @param partitioner used to repartition `partitions` after it is updated.
  * @param randomSeed
  * @param bcParameters
  * @param bcPartitionFunction
  * @param bcRecordsCache broadcast variable that contains objects required on each
  *                node.
  * @param rand random number generator used for updating the distortion
  *             probabilities on the driver.
  */
case class State(iteration: Long,
                 partitions: Partitions,
                 distProbs: DistortionProbs,
                 summaryVars: SummaryVars,
                 accumulators: SummaryAccumulators,
                 partitioner: HardPartitioner,
                 randomSeed: Long,
                 bcParameters: Broadcast[Parameters],
                 bcPartitionFunction: Broadcast[PartitionFunction[ValueId]],
                 bcRecordsCache: Broadcast[RecordsCache])
                (implicit val rand: RandomGenerator) {

  /** Applies a Markov transition operator to the given state
    *
    * @param checkpointer whether to checkpoint the RDD.
    * @param collapsedEntityIds whether to do a partially-collapsed update for the entity ids (default: false)
    * @param collapsedEntityValues whether to do a partially-collapsed update for the entity values (default: true)
    * @param sequential
    * @return new State after applying the transition operator.
    */
  def nextState(checkpointer: PeriodicRDDCheckpointer[(PartitionId, EntRecPair)],
                collapsedEntityIds: Boolean = false,
                collapsedEntityValues: Boolean = true,
                sequential: Boolean = false): State = {
    /** Update distortion probabilities and broadcast */
    val newDistProbs = updateDistProbs(summaryVars, bcRecordsCache.value)
    val bcDistProbs = partitions.sparkContext.broadcast(newDistProbs)

    val newPartitions = updatePartitions(iteration, partitions, bcDistProbs,
      partitioner, randomSeed, bcPartitionFunction, bcRecordsCache, collapsedEntityIds, collapsedEntityValues, sequential)
    /** If handling persistence here, may want to set this:
      * .persist(StorageLevel.MEMORY_ONLY_SER) */
    checkpointer.update(newPartitions)
    //if (checkpointer) newPartitions.checkpoint()

    val newSummaryVariables = updateSummaryVariables(newPartitions, accumulators,
      bcDistProbs, bcRecordsCache)
    bcDistProbs.destroy()

    this.copy(iteration = iteration + 1, partitions = newPartitions, distProbs = newDistProbs,
      summaryVars = newSummaryVariables)
  }

  def getLinkageStructure(): RDD[LinkageState] = {
    partitions.mapPartitions(partition => {
      val entRecClusters = mutable.LongMap.empty[ArrayBuffer[RecordId]]
      var partitionId = -1
      partition.foreach { case (pId: Int, EntRecPair(entity, record)) =>
        partitionId = pId
        val cluster = entRecClusters.getOrElseUpdate(entity.id, ArrayBuffer.empty[RecordId])
        if (record.isDefined) {
          cluster += record.get.id
        }
      }
      if (entRecClusters.isEmpty) {
        Iterator()
      } else {
        Iterator(LinkageState(iteration, partitionId, entRecClusters.toMap))
      }
    }, preservesPartitioning = true)
  }

  /** Save state to disk.
    *
    * The variables on the driver are serialized and stored in a single file
    * called "driverState". The `partitions` RDD is converted to a Dataset,
    * and stored in Parquet file called "partitions-state.parquet".
    *
    * @param path path to write the files (must be a directory).
    */
  def save(path: String): Unit = {
    // TODO: check path is valid (a directory, maybe nothing to overwrite?)
    val sc = partitions.sparkContext
    /** Save state of driver */
    val driverStatePath = new Path(path + "driver-state")
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val os = hdfs.create(driverStatePath)
    val oos = new ObjectOutputStream(os)

    oos.writeLong(this.iteration)
    oos.writeObject(this.distProbs)
    oos.writeObject(this.summaryVars)
    oos.writeObject(this.partitioner)
    oos.writeLong(this.randomSeed)
    oos.writeObject(this.bcParameters.value)
    oos.writeObject(this.bcPartitionFunction.value)
    oos.writeObject(this.bcRecordsCache.value)
    oos.writeObject(this.rand)
    oos.close()

    /** Save state on workers (partitions) in Parquet format */
    val partitionsPath = path + "partitions-state.parquet"
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    partitions.map(r => PartEntRecTriple(r._1, r._2)).toDS() // convert to Dataset[PartEntRecTriple]
      .write.format("parquet").mode("overwrite").save(partitionsPath)
  }
}

object State {

  /** Read state from disk.
    *
    * @param path path containing the files (must be a directory).
    * @return a `State` object.
    */
  def read(path: String): State = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val driverStatePath = new Path(path + "driver-state")
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val is = hdfs.open(driverStatePath)
    val ois = new ObjectInputStream(is)

    val iteration = ois.readLong()
    val distProbs = ois.readObject().asInstanceOf[DistortionProbs]
    val summaryVars = ois.readObject().asInstanceOf[SummaryVars]
    val partitioner = ois.readObject().asInstanceOf[HardPartitioner]
    val randomSeed = ois.readLong()
    val parameters = ois.readObject().asInstanceOf[Parameters]
    val partitionFunction = ois.readObject().asInstanceOf[PartitionFunction[ValueId]] // TODO: what if a partition function is a different class?
    val recordsCache = ois.readObject().asInstanceOf[RecordsCache]
    implicit val rand: RandomGenerator = ois.readObject().asInstanceOf[RandomGenerator]

    val partitionsPath = path + "partitions-state.parquet"
    val partitions = spark.read.format("parquet")
      .load(partitionsPath).as[PartEntRecTriple].rdd
      .map(r => (r.partitionId, r.entRecPair))
    val bcParameters = sc.broadcast(parameters)
    val bcPartitionFunction = sc.broadcast(partitionFunction)
    val bcRecordsCache = sc.broadcast(recordsCache)
    val accumulators = SummaryAccumulators(sc)

    State(iteration, partitions, distProbs, summaryVars, accumulators, partitioner, randomSeed,
      bcParameters, bcPartitionFunction, bcRecordsCache)
  }

  /** Initialise a new State object based on a simple deterministic method.
    *
    * @param records RDD containing records to link
    * @param attributeSpecs A sequence of `Attribute` instances, each of which specifies the model parameters
    *                       associated with a matching attribute.
    * @param parameters other model parameters
    * @param partitionFunction A `PartitionFunction` which specifies how the space of entities is partitioned
    * @param randomSeed A long random seed
    * @return a `State` object.
    */
  def deterministic(records: RDD[Record[String]],
                    attributeSpecs: IndexedSeq[Attribute],
                    parameters: Parameters,
                    partitionFunction: PartitionFunction[ValueId],
                    randomSeed: Long): State = {
    /** Parse records and build the cache */
    val recordsCache = RecordsCache(records, attributeSpecs, parameters.maxClusterSize)

    /** Broadcast to executors */
    val sc = records.sparkContext
    val bcRecordsCache = sc.broadcast(recordsCache)
    val bcParameters = sc.broadcast(parameters)

    /** Divide the entities among the partitions using a heuristic bin-packing method
      *
      * Starting point: set the number of entities equal to the number of records in the partition.
      * Then, iteratively refine by adding/subtracting entities from partitions until the target number is reached.
      */
    val numRecsPartition = records.mapPartitions(x => Array(x.size).iterator).collect()
    assert(parameters.populationSize > numRecsPartition.size, "Too few entities. Need at least one entity per partition")
    val numEntsPartition = {
      var numAdditionalEnts = parameters.populationSize - recordsCache.numRecords
      val temp = numRecsPartition.clone()
      var i = 0
      while (numAdditionalEnts != 0) {
        if (i >= temp.length) i = 0 // reset iterator if we reach the end
        if (numAdditionalEnts > 0) {
          temp(i) += 1
          numAdditionalEnts -= 1
        } else {
          // try to subtract from current
          if (temp(i) > 1) {
            temp(i) -= 1
            numAdditionalEnts += 1
          }
        }
        i += 1
      }
      temp
    }

    /** Initialize the partitions of entity-record pairs.
      * - links: each record is preferentially linked to a unique entity
      * - distortion: prefer no distortion
      * - entity values: copied directly from the records (missing generated randomly)
      */
    val entRecPairs = recordsCache.transformRecords(records) // map string attribute values to integer ids
      .mapPartitionsWithIndex((partId, partition) => {       // generate latent variables
        /** Ensure we get different pseudo-random numbers on each partition */
        implicit val rand: RandomGenerator = new MersenneTwister((partId + randomSeed).longValue())

        /** Convenience variable */
        val indexedAttributes = bcRecordsCache.value.indexedAttributes

        val numEntities = numEntsPartition(partId) // number of entities in this partition
        val firstEntId = numEntsPartition.take(partId).sum // ensures unique entity ids across all partitions

        /** Convert from an iterator to an array, as we need support for indexing */
        val records = partition.toArray

        val recordsIt = records.zipWithIndex.iterator.map { case (Record(id, fileId, values), i) =>
          /** Initialize entity values using record values. Prefer to use linked record values, but allow for
            * the case where numEntities < numRecords by taking the modulus */
          val linkRecId = i % numEntities
          val entId = firstEntId + linkRecId
          val pickedRecValues = records(linkRecId).values
          /** Replace any missing values by randomly-generated values */
          val entValues = (pickedRecValues, indexedAttributes).zipped.map {
            case (valueId, _) if valueId >= 0 => valueId
            case (_, IndexedAttribute(_, _, _, index)) =>  index.draw()
          }
          /** Initialize the distortion indicators. Prefer no distortion unless values disagree */
          val distValues = (values, entValues).zipped.map {
            case (recValue, entValue) => DistortedValue(recValue, distorted = recValue != entValue)
          }
          val entity = Entity(entId, entValues)
          val newRecord = Record[DistortedValue](id, fileId, distValues)
          EntRecPair(entity, Some(newRecord))
        }

        /** Deal with the case numEntities > numRecords, where there are isolated entities */
        val numIsolates = if (numEntities > records.length) numEntities - records.length else 0
        val isolatesIt = Iterator.tabulate(numIsolates) { i =>
          val entId = firstEntId + records.size + i
          val entValues = indexedAttributes.toArray.map { case IndexedAttribute(_, _, _, index) => index.draw() }
          val entity = Entity(entId, entValues)
          EntRecPair(entity, None)
        }

        recordsIt ++ isolatesIt
      }, true)
    entRecPairs.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** Initialize partitioner */
    val entityValues = entRecPairs.map(_.entity.values)
    partitionFunction.fit(entityValues)
    val partitioner = new HardPartitioner(partitionFunction.numPartitions)
    val bcPartitionFunction = sc.broadcast(partitionFunction)

    /** Apply partitioner to entity-record pairs */
    val partitions = entRecPairs.keyBy(pair => bcPartitionFunction.value.getPartitionId(pair.entity.values))
      .partitionBy(partitioner)
    entRecPairs.unpersist()

    /** Initialize the distortion probabilities (based on the prior hyperparameters) */
    val distProbs = DistortionProbs(recordsCache.fileSizes.keys, recordsCache.distortionPrior)
    val bcDistProbs = sc.broadcast(distProbs)

    /** Initialize accumulators and use them to update the summary variables */
    val accumulators = SummaryAccumulators(sc)
    val summaryVars = updateSummaryVariables(partitions, accumulators, bcDistProbs,
      bcRecordsCache)
    bcDistProbs.destroy()

    /** Initialize random number generator for the distortion probabilities */
    implicit val rand: RandomGenerator = new MersenneTwister()
    rand.setSeed(randomSeed.longValue())

    State(0l, partitions, distProbs, summaryVars, accumulators, partitioner, randomSeed,
      bcParameters, bcPartitionFunction, bcRecordsCache)
  }

  /** Initialise a new State object based on a simple deterministic method.
    *
    * @param records A DataFrame containing the records to link
    * @param recIdColname Name of column in `records` that contains record ids
    * @param fileIdColname  Name of column in `records` that contains file/source ids (optional)
    * @param attributeSpecs A sequence of `Attribute` instances, each of which specifies the model parameters
    *                       associated with a matching attribute.
    * @param parameters Other model parameters
    * @param partitionFunction A `PartitionFunction` which specifies how the space of entities is partitioned
    * @param randomSeed A long random seed
    * @return a `State` object.
    */
  def deterministic(records: DataFrame,
                    recIdColname: String,
                    fileIdColname: Option[String],
                    attributeSpecs: IndexedSeq[Attribute],
                    parameters: Parameters,
                    partitionFunction: PartitionFunction[ValueId],
                    randomSeed: Long): State = {
    val spark = records.sparkSession
    import spark.implicits._

    val rdd: RDD[Record[String]] = fileIdColname match {
      case Some(fIdCol) =>
        records.select(
          col(recIdColname),
          col(fIdCol),
          array(attributeSpecs.map(_.name) map col: _*)
        ).map(r =>
          Record(r.getString(0), r.getString(1), r.getSeq[String](2).toArray)
        ).rdd
      case None =>
        records.select(
          col(recIdColname),
          array(attributeSpecs.map(_.name) map col: _*)
        ).map(r =>
          Record(r.getString(0), "0", r.getSeq[String](1).toArray)
        ).rdd
    }
    deterministic(rdd, attributeSpecs, parameters, partitionFunction, randomSeed)
  }
}