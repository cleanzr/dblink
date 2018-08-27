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

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.github.ngmarchant.dblink.util.HardPartitioner
import com.github.ngmarchant.dblink.GibbsUpdates.{drawDistProbs, updatePartitions, updateSummaryVariables}
import com.github.ngmarchant.dblink.partitioning.PartitionFunction
import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

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
  * @param rand random number generator used for updating the distortion
  *             probabilities on the driver.
  * @param partitioner used to repartition `partitions` after it is updated.
  * @param bcRecordsCache broadcast variable that contains objects required on each
  *                node.
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
    * @param checkpoint whether to checkpoint the RDD.
    * @param collapseDistortions whether to collapse the distortion indicator
    *                            variables for the latent attribute value update.
    * @return new State after applying the transition operator.
    */
  def nextState(checkpoint: Boolean,
                collapseDistortions: Boolean = true): State = {
    /** Update distortion probabilities and broadcast */
    val newDistProbs = drawDistProbs(summaryVars, bcRecordsCache.value)
    val bcDistProbs = partitions.sparkContext.broadcast(newDistProbs)

    val newPartitions = updatePartitions(iteration, partitions, bcDistProbs,
      partitioner, randomSeed, bcPartitionFunction, bcRecordsCache, collapseDistortions)
    /** If handling persistence here, may want to set this:
      * .persist(StorageLevel.MEMORY_ONLY_SER) */
    if (checkpoint) newPartitions.checkpoint()
    val newSummaryVariables = updateSummaryVariables(newPartitions, accumulators,
      bcDistProbs, bcRecordsCache)
    bcDistProbs.destroy()

    this.copy(iteration = iteration + 1, partitions = newPartitions, distProbs = newDistProbs,
      summaryVars = newSummaryVariables)
  }

  /** Save state to disk.
    *
    * The variables on the driver are serialized and stored in a single file
    * called "driverState". The `partitions` RDD is converted to a Dataset,
    * and stored in Parquet file called "partitionsState.parquet".
    *
    * @param path path to write the files (must be a directory).
    */
  def save(path: String): Unit = {
    // TODO: check path is valid (a directory, maybe nothing to overwrite?)
    /** Driver variables */
    val driverStatePath = path + "driverState"
    val oos = new ObjectOutputStream(new FileOutputStream(driverStatePath))
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

    /** Save `partitions` in Parquet format */
    val partitionsPath = path + "partitionsState.parquet"
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
    val driverStatePath = path + "driverState"
    val ois = new ObjectInputStream(new FileInputStream(driverStatePath))
    val iteration = ois.readLong()
    val distProbs = ois.readObject().asInstanceOf[DistortionProbs]
    val summaryVars = ois.readObject().asInstanceOf[SummaryVars]
    val partitioner = ois.readObject().asInstanceOf[HardPartitioner]
    val randomSeed = ois.readLong()
    val parameters = ois.readObject().asInstanceOf[Parameters]
    val partitionFunction = ois.readObject().asInstanceOf[PartitionFunction[ValueId]] // TODO: what if a partition function is a different class?
    val recordsCache = ois.readObject().asInstanceOf[RecordsCache]
    implicit val rand: RandomGenerator = ois.readObject().asInstanceOf[RandomGenerator]

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val partitionsPath = path + "partitionsState.parquet"
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
    * @param records Dataset containing records to link.
    * @param parameters eber parameters.
    * @param partitionFunction
    * @param randomSeed
    * @return a `State` object.
    */
  def deterministic(records: RDD[Record[String]],
                    attributeSpecs: IndexedSeq[Attribute],
                    parameters: Parameters,
                    partitionFunction: PartitionFunction[ValueId],
                    randomSeed: Long): State = {
    /** Parse records and build the cache */
    val recordsCache = RecordsCache(records, attributeSpecs, parameters.maxClusterSize)
    val transformedRecords = recordsCache.transformRecords(records)
    transformedRecords.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** Initialize partitioner */
    val recordValues = transformedRecords.map(_.values)
    partitionFunction.fit(recordValues)
    val partitioner = new HardPartitioner(partitionFunction.numPartitions)

    /** Broadcast to executors */
    val sc = records.sparkContext
    val bcRecordsCache = sc.broadcast(recordsCache)
    val bcPartitionFunction = sc.broadcast(partitionFunction)
    val bcParameters = sc.broadcast(parameters)

    /** Initialize the partitions of entity-record pairs.
      * - links: each record is linked to a separate entity
      * - distortion: no distortion
      * - entity values: copied directly from the records
      */
    val partitions = transformedRecords
      .zipWithUniqueId()
      .map {case (Record(id, fileId, values), entId: EntityId) =>
        val distValues = values.map(DistortedValue(_, distorted = false))
        val entity = Entity(entId, values)
        val newRecord = Record[DistortedValue](id, fileId, distValues)
        EntRecPair(entity, Some(newRecord))
      }
      .keyBy(pair => bcPartitionFunction.value.getPartitionId(pair.entity.values))
      .partitionBy(partitioner)
    transformedRecords.unpersist()

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
}