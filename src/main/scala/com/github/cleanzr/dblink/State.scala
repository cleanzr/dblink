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

import com.github.cleanzr.dblink.util.{HardPartitioner, PeriodicRDDCheckpointer}
import com.github.cleanzr.dblink.GibbsUpdates.{updateDistProbs, updatePartitions, updateSummaryVariables}
import com.github.cleanzr.dblink.partitioning.PartitionFunction
import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/** State of the Markov chain
  *
  * @param iteration Iteration counter.
  * @param partitions RDD containing Partition-Entity clusters and their associated parameters (entity
  *                   attribute values, record distortion indicators and record attribute values).
  * @param distProbs Map containing the distortion probabilities for each fileId and attributeId.
  * @param summaryVars Collection of quantities that summarise the state. Each quantity is a function of `partitions`
  *                    and `distProbs`.
  * @param accumulators Accumulators used for calculating `summaryVars`.
  * @param partitioner Partitioner used to repartition `partitions` after it is updated.
  * @param startRandomSeed Starting random seed.
  * @param currentRandomSeed Current random seed. To ensure unique pseudo-random numbers in a distributed setting,
  *                          we increment the random seed by 1 when initializing the pseudo-RNG for each iteration
  *                          and partition.
  * @param bcParameters Other model parameters as a Spark broadcast variable.
  * @param bcPartitionFunction Partition function as a Spark broadcast variable.
  * @param bcRecordsCache Records cache (includes summary statistics and attribute-level parameters) as a Spark
  *                       broadcast variable.
  * @param rand Random number generator used for updating the distortion probabilities on the driver.
  */
case class State(iteration: Long,
                 partitions: Partitions,
                 distProbs: DistortionProbs,
                 populationSize: Int,
                 summaryVars: SummaryVars,
                 accumulators: SummaryAccumulators,
                 partitioner: HardPartitioner,
                 startRandomSeed: Long,
                 currentRandomSeed: Long,
                 bcParameters: Broadcast[Parameters],
                 bcPartitionFunction: Broadcast[PartitionFunction[ValueId]],
                 bcRecordsCache: Broadcast[RecordsCache])
                (implicit val rand: RandomGenerator) {

  /** Applies a Markov transition operator to the given state
    *
    * @param checkpointer Whether to checkpoint the RDD.
    * @param collapsedEntityIds Whether to do a partially-collapsed update for the entity ids (default: false)
    * @param collapsedEntityValues Whether to do a partially-collapsed update for the entity values (default: true)
    * @param sequential Whether to perform the update without using an inverted index.
    * @return new State after applying the transition operator.
    */
  def nextState(checkpointer: PeriodicRDDCheckpointer[(PartitionId, EntRecCluster)],
                collapsedEntityIds: Boolean = false,
                collapsedEntityValues: Boolean = true,
                sequential: Boolean = false): State = {
    // Update distortion probabilities and broadcast
    val newDistProbs = updateDistProbs(summaryVars, bcRecordsCache.value)
    val bcDistProbs = partitions.sparkContext.broadcast(newDistProbs)

    // Update parameters on the partitions and the population size
    val (newPartitions, newRandomSeed) = updatePartitions(partitions,
      populationSize, bcDistProbs, partitioner, currentRandomSeed, bcPartitionFunction, bcRecordsCache,
      collapsedEntityIds, collapsedEntityValues, sequential)

    checkpointer.update(newPartitions)

    // Compute summary statistics
    val newSummaryVariables = updateSummaryVariables(newPartitions, accumulators, bcDistProbs.value, bcRecordsCache)
    bcDistProbs.unpersist()

    this.copy(iteration = iteration + 1, partitions = newPartitions, distProbs = newDistProbs,
      summaryVars = newSummaryVariables, currentRandomSeed = newRandomSeed)
  }

  /** Get the linkage structure (i.e. a clustering of records according to their linked entities) */
  def getLinkageStructure(): RDD[LinkageState] = {
    partitions.aggregateByKey(zeroValue = Seq.empty[Seq[RecordId]])(
      seqOp = (clusters, cluster) => cluster.records match {
        case Some(r) => clusters :+ r.map(_.id).toSeq
        case None => clusters
      },
      combOp = _ ++ _
    ).mapPartitions(partition => {
      partition.map { case (partitionId, clusters) => LinkageState(iteration, partitionId, clusters)}
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
    oos.writeObject(this.populationSize)
    oos.writeObject(this.summaryVars)
    oos.writeObject(this.partitioner)
    oos.writeLong(this.startRandomSeed)
    oos.writeLong(this.currentRandomSeed)
    oos.writeObject(this.bcParameters.value)
    oos.writeObject(this.bcPartitionFunction.value)
    oos.writeObject(this.bcRecordsCache.value)
    oos.writeObject(this.rand)
    oos.close()

    /** Save state on workers (partitions) in Parquet format */
    val partitionsPath = path + "partitions-state.parquet"
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    partitions.map(r => PartEntRecCluster(r._1, r._2)).toDS() // convert to Dataset[PartEntRecTriple]
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
    val populationSize = ois.readInt()
    val summaryVars = ois.readObject().asInstanceOf[SummaryVars]
    val partitioner = ois.readObject().asInstanceOf[HardPartitioner]
    val startRandomSeed = ois.readLong()
    val currentRandomSeed = ois.readLong()
    val parameters = ois.readObject().asInstanceOf[Parameters]
    val partitionFunction = ois.readObject().asInstanceOf[PartitionFunction[ValueId]] // TODO: what if a partition function is a different class?
    val recordsCache = ois.readObject().asInstanceOf[RecordsCache]
    implicit val rand: RandomGenerator = ois.readObject().asInstanceOf[RandomGenerator]

    val partitionsPath = path + "partitions-state.parquet"
    val partitions = spark.read.format("parquet")
      .load(partitionsPath).as[PartEntRecCluster].rdd
      .map(r => (r.partitionId, r.entRecCluster))
    val bcParameters = sc.broadcast(parameters)
    val bcPartitionFunction = sc.broadcast(partitionFunction)
    val bcRecordsCache = sc.broadcast(recordsCache)
    val accumulators = SummaryAccumulators(sc)

    State(iteration, partitions, distProbs, populationSize, summaryVars, accumulators, partitioner, startRandomSeed,
      currentRandomSeed, bcParameters, bcPartitionFunction, bcRecordsCache)
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
                    populationSize: Option[Int],
                    parameters: Parameters,
                    partitionFunction: PartitionFunction[ValueId],
                    randomSeed: Long): State = {
    /** Parse records and build the cache */
    val recordsCache = RecordsCache(records, attributeSpecs, parameters.maxClusterSize)

    /** Broadcast to executors */
    val sc = records.sparkContext
    val bcRecordsCache = sc.broadcast(recordsCache)
    val bcParameters = sc.broadcast(parameters)

    val numRecsPartition = records.mapPartitionsWithIndex( (index, partition) => Array((index, partition.size)).iterator).collectAsMap()

    val popSize = populationSize.getOrElse(numRecsPartition.values.sum)

    assert(popSize >= numRecsPartition.size, "Too few entities. Need at least one entity per partition")

    /** Divide the entities among the partitions using a heuristic bin-packing method
      *
      * Starting point: set the number of entities equal to the number of records in the partition.
      * Then, iteratively refine by adding/subtracting entities from partitions until the target number is reached.
      */
    val numEntsPartition = {
      var numAdditionalEnts = popSize - recordsCache.numRecords
      val temp = mutable.Map(numRecsPartition.toSeq: _*)

      var it = temp.keys.toIterator
      while (numAdditionalEnts != 0) {
        if (!it.hasNext) it = temp.keys.toIterator // reset iterator if we reach the end
        val key = it.next()
        if (numAdditionalEnts > 0) {
          temp(key) += 1
          numAdditionalEnts -= 1
        } else {
          // try to subtract from current
          if (temp(key) > 1) {
            temp(key) -= 1
            numAdditionalEnts += 1
          }
        }
      }
      temp
    }

    /** Initialize the partitions of entity-record pairs.
      * - links: each record is preferentially linked to a unique entity
      * - distortion: prefer no distortion
      * - entity values: copied directly from the records (missing generated randomly)
      */
    val entRecClusters = recordsCache.transformRecords(records) // map string attribute values to integer ids
      .mapPartitionsWithIndex((index, partition) => {       // generate latent variables
        /** Ensure we get different pseudo-random numbers on each partition */
        val seed = index + randomSeed
        implicit val rand: RandomGenerator = new MersenneTwister(seed.longValue())

        /** Convenience variable */
        val indexedAttributes = bcRecordsCache.value.indexedAttributes

        val numEntities = numEntsPartition(index) // number of entities in this partition
        val result = mutable.Map.empty[EntityId, EntRecCluster]

        partition.zipWithIndex.foreach { case (Record(id, fileId, recValues), i) =>
          /** Initialize entity values using record values. Prefer to use linked record values, but allow for
            * the case where numEntities < numRecords by taking the modulus */
          val entId = i % numEntities

          val thisClust = result.getOrElse(entId, {
            /** Replace any missing values by randomly-generated values */
            val entValues = (recValues, indexedAttributes).zipped.map {
              case (valueId, _) if valueId >= 0 => valueId
              case (_, IndexedAttribute(_, _, _, index)) => index.draw()
            }
            EntRecCluster(Entity(entValues), None)
          })

          /** Initialize the distortion indicators. Prefer no distortion unless values disagree */
          val distValues = (recValues, thisClust.entity.values).zipped.map {
            case (recValue, entValue) => DistortedValue(recValue, distorted = (recValue >= 0) & (recValue != entValue))
          }
          val newRecord = Record[DistortedValue](id, fileId, distValues)

          thisClust match {
            case EntRecCluster(_, Some(r)) => result.update(entId, thisClust.copy(records = Some(r :+ newRecord)))
            case EntRecCluster(_, None) => result.update(entId, thisClust.copy(records = Some(Array(newRecord))))
          }
        }

        /** Deal with the case numEntities > numRecords, where there are isolated entities */
        val numIsolates = numEntities - result.size
        val isolatesIt = Iterator.tabulate(numIsolates) { _ =>
          val entValues = indexedAttributes.toArray.map { case IndexedAttribute(_, _, _, index) => index.draw() }
          val entity = Entity(entValues)
          EntRecCluster(entity, None)
        }

        result.valuesIterator ++ isolatesIt
      }, preservesPartitioning = true)
    entRecClusters.persist()
    val newRandomSeed = randomSeed + entRecClusters.getNumPartitions

    /** Initialize partitioner */
    val entityValues = entRecClusters.map(_.entity.values)
    partitionFunction.fit(entityValues)
    val partitioner = new HardPartitioner(partitionFunction.numPartitions)
    val bcPartitionFunction = sc.broadcast(partitionFunction)

    /** Apply partitioner to entity-record pairs */
    val partitions = entRecClusters.keyBy(cluster => bcPartitionFunction.value.getPartitionId(cluster.entity.values))
      .partitionBy(partitioner)
    entRecClusters.unpersist()

    /** Initialize the distortion probabilities (based on the prior hyperparameters) */
    val distProbs = DistortionProbs(recordsCache.fileSizes.keys, recordsCache.distortionPrior)
    val bcDistProbs = sc.broadcast(distProbs)

    /** Initialize accumulators and use them to update the summary variables */
    val accumulators = SummaryAccumulators(sc)
    val summaryVars = updateSummaryVariables(partitions, accumulators, bcDistProbs.value, bcRecordsCache)
    bcDistProbs.unpersist()

    /** Initialize random number generator for the distortion probabilities */
    implicit val rand: RandomGenerator = new MersenneTwister()
    rand.setSeed(randomSeed.longValue())

    State(0L, partitions, distProbs, popSize, summaryVars, accumulators, partitioner, randomSeed,
      newRandomSeed, bcParameters, bcPartitionFunction, bcRecordsCache)
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
                    populationSize: Option[Int],
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
    deterministic(rdd, attributeSpecs, populationSize, parameters, partitionFunction, randomSeed)
  }
}