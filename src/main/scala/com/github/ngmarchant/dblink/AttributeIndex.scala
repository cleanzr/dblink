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

package com.github.ngmarchant.dblink

import com.github.ngmarchant.dblink.random.DiscreteDist
import org.apache.commons.math3.random.RandomGenerator
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.math.{exp, pow}

/** An index for an attribute domain.
  * It includes:
  *  - an index from raw strings in the domain to integer value ids
  *  - the empirical distribution over the domain
  *  - an index that accepts a query value id and returns a set of
  *    "similar" value ids (whose truncated similarity is non-zero)
  *  - an index over pairs of value ids that returns exponentiated
  *    truncated similarity scores
  */
trait AttributeIndex extends Serializable {

  /** Number of distinct attribute values */
  def numValues: Int

  /** Object for empirical distribution */
  def distribution: DiscreteDist[Int]

  /** Get the probability mass associated with a value id
    *
    * @param valueId value id.
    * @return probability mass. Returns 0.0 if the value id does not exist.
    */
  def probabilityOf(valueId: ValueId): Double


  /** Draw a value id according to the empirical distribution
    *
    * @return a value id.
    */
  def draw(): ValueId


  /** Get the value id for a given string value
    *
    * @param value original string value
    * @return integer value id. Returns `-1` if value does not exist in the
    *         index.
    */
  def valueIdxOf(value: String): ValueId


  /** Set the random number generator
    *
    * @param rand a RandomGenerator
    */
  def setRand(rand: RandomGenerator)


  /** Get the similarity normalization corresponding to the value id.
    *
    * @param valueId integer value id
    * @return sum_{w}(probabilities(w) * exp(sim(w,value))
    */
  def simNormalizationOf(valueId: ValueId): Double


  /** Get the value ids that are "similar" (above the similarity threshold)
    * to the given value id, along with the exponentiated similarity scores.
    *
    * @param valueId original value string
    * @return
    */
  def simValuesOf(valueId: ValueId): scala.collection.Map[ValueId, Double]


  /** Get the exponentiated similarity score for a pair of value ids
    *
    * @param valueId1 integer value id 1
    * @param valueId2 integer value id 2
    * @return exp(sim(valueId1, valueId2))
    */
  def expSimOf(valueId1: ValueId, valueId2: ValueId): Double


  /** Get distribution of the form:
    *    p(v) \propto probabilityOf(v) * pow(simNormalizationOf(v), power)
    *
    * @param power the similarity normalization is raised to this power
    * @return a distribution
    */
  def getSimNormDist(power: Int): DiscreteDist[Int]
}

object AttributeIndex {
  def apply(valuesWeights: Map[String, Double],
            similarityFn: SimilarityFn,
            precachePowers: Option[Traversable[Int]] = None)
           (implicit rand: RandomGenerator, sc: SparkContext): AttributeIndex = {
    require(valuesWeights.nonEmpty, "index cannot be empty")

    val valuesWeights_sorted = valuesWeights.toArray.sortBy(_._1)
    val totalWeight = valuesWeights_sorted.foldLeft(0.0){ case (sum, (_, weight)) => sum + weight }
    val probs = valuesWeights_sorted.map(_._2/totalWeight)
    val stringToId = valuesWeights_sorted.iterator.zipWithIndex.map(x => (x._1._1, x._2)).toMap

    similarityFn match {
      case SimilarityFn.ConstantSimilarityFn => new ConstantAttributeIndex(stringToId, probs)
      case simFn =>
        val simValueIndex = computeSimValueIndex(stringToId, simFn)
        val simNormalizations = computeSimNormalizations(simValueIndex, probs)
        new GenericAttributeIndex(stringToId, probs, simValueIndex, simNormalizations, precachePowers)
    }
  }

  /** Implementation for attributes with constant similarity functions */
  private class ConstantAttributeIndex(protected val stringToId: Map[String, ValueId],
                                       protected val probs: Array[Double])
                                      (implicit rand: RandomGenerator) extends AttributeIndex {

    /** Empirical distribution over the field values */
    val distribution: DiscreteDist[Int] = DiscreteDist(probs)

    val numValues: Int = stringToId.size

    override def probabilityOf(valueId: ValueId): Double = {
      require(valueId >= 0 && valueId < numValues, "valueId is not in the index")
      distribution.probabilityOf(valueId)
    }

    override def draw(): ValueId = distribution.sample()

    override def valueIdxOf(value: String): ValueId = stringToId(value)

    override def setRand(rand: RandomGenerator): Unit = distribution.setRand(rand)

    /** Assuming exp(sim(v1, v2)) = 1.0 for all v1, v2 */
    override def simNormalizationOf(valueId: ValueId): Double = {
      require(valueId >= 0 && valueId < numValues, "valueId is not in the index")
      1.0
    }

    /** Assuming exp(sim(v1, v2)) = 1.0 for all v1, v2 */
    override def simValuesOf(valueId: ValueId): scala.collection.Map[ValueId, Double] = {
      require(valueId >= 0 && valueId < numValues, "valueId is not in the index")
      Map.empty[ValueId, Double]
    }

    /** Assuming exp(sim(v1, v2)) = 1.0 for all v1, v2 */
    override def expSimOf(valueId1: ValueId, valueId2: ValueId): Double = {
      require(valueId1 >= 0 && valueId1 < numValues, "valueId1 is not in the index")
      require(valueId2 >= 0 && valueId2 < numValues, "valueId2 is not in the index")
      1.0
    }

    /** For a constant similarity function, this is the same as the empirical distribution */
    override def getSimNormDist(power: Int): DiscreteDist[Int] = {
      require(power > 0, "power must be a positive integer")
      distribution
    }
  }

  /** Implementation for attributes with non-constant attribute similarity functions */
  private class GenericAttributeIndex(stringToId: Map[String, ValueId],
                                      probs: Array[Double],
                                      private val simValueIndex: Array[Map[ValueId, Double]],
                                      private val simNormalizations: Array[Double],
                                      precachePowers: Option[Traversable[Int]])
                                     (implicit rand: RandomGenerator)
    extends ConstantAttributeIndex(stringToId, probs)(rand) {

    private var rng = rand

    override def simNormalizationOf(valueId: ValueId): Double = simNormalizations(valueId)

    override def simValuesOf(valueId: ValueId): scala.collection.Map[ValueId, Double] = simValueIndex(valueId)

    override def expSimOf(valueId1: ValueId, valueId2: ValueId): Double = {
      require(valueId2 >= 0 && valueId2 < numValues, "valueId2 is not in the index")
      simValuesOf(valueId1).getOrElse(valueId2, 1.0)
    }

    override def setRand(rand: RandomGenerator): Unit = {
      this.rng = rand
      distribution.setRand(rand)
      cachedSimNormDist.foreach(_._2.setRand(rand))
    }

    private val cachedSimNormDist = precachePowers match {
      case Some(powers) => powers.foldLeft(mutable.Map.empty[Int, DiscreteDist[Int]]) {
        case (m, power) if power > 0 =>
          m + (power -> computeSimNormDist(probs, simNormalizations, power)(rng))
        case (m, _) => m
      }
      case None => mutable.Map.empty[Int, DiscreteDist[Int]]
    }

    override def getSimNormDist(power: Int): DiscreteDist[Int] = {
      require(power > 0, "power must be a positive integer")
      cachedSimNormDist.get(power) match {
        case Some(dist) => dist
        case None =>
          val dist = computeSimNormDist(probs, simNormalizations, power)(rng)
          cachedSimNormDist.update(power, dist)
          dist
      }
    }
  }


  private def computeSimNormDist(probs: Array[Double],
                                 simNormalizations: Array[Double],
                                 power: Int)
                                (implicit rand: RandomGenerator): DiscreteDist[Int] = {
    val simNormWeights = Array.tabulate(probs.length) { valueId =>
      probs(valueId) * pow(simNormalizations(valueId), power)
    }
    DiscreteDist(simNormWeights)
  }


  private def computeSimValueIndex(stringToId: Map[String, ValueId],
                                   similarityFn: SimilarityFn)
                                  (implicit sc: SparkContext): Array[Map[ValueId, Double]] = {
    val valuesRDD = sc.parallelize(stringToId.toSeq) // TODO: how to set numSlices? Take into account number of executors.
    val valuePairsRDD = valuesRDD.cartesian(valuesRDD)

    val simValues = valuePairsRDD.map { case ((a, a_id), (b, b_id)) => (a_id, (b_id, exp(similarityFn.getSimilarity(a, b)))) }
      .filter(_._2._2 > 1.0) // non-zero truncated similarity
      .aggregateByKey(Map.empty[Int, Double])(seqOp = _ + _, combOp = _ ++ _)
      .collect()

    simValues.sortBy(_._1).map(_._2)
  }


  private def computeSimNormalizations(simValueIndex: Array[Map[ValueId, Double]],
                                       probs: Array[Double]): Array[Double] = {
    simValueIndex.map { simValues =>
      var valueId = 0
      var norm = 0.0
      while (valueId < probs.length) {
        norm += probs(valueId) * simValues.getOrElse(valueId, 1.0)
        valueId += 1
      }
      1.0/norm
    }
  }
}