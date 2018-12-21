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

package com.github.ngmarchant.dblink.random

import org.apache.commons.math3.random.RandomGenerator

import scala.collection.Map
import scala.reflect.ClassTag

/** A non-uniform distribution over a discrete set of values
  *
  * @param valuesWeights map from values to weights (need not be normalised)
  * @param rand pseudo-random number generator
  * @param ev
  * @tparam T type of the values
  */
class NonUniformDiscreteDist[T](valuesWeights: Map[T, Double])
                               (implicit ev: ClassTag[T]) extends DiscreteDist[T] {
  require(valuesWeights.nonEmpty, "`valuesWeights` must be non-empty")

  private val (_valuesArray, _probsArray, _totalWeight) = NonUniformDiscreteDist.processValuesWeights(valuesWeights)

  override def totalWeight: Double = _totalWeight

  override def values: Traversable[T] = _valuesArray.toTraversable

  override val numValues: Int = valuesWeights.size

  /** AliasSampler to efficiently sample from the distribution */
  private val sampler = AliasSampler(_probsArray, checkWeights = false, normalized = true)

  //    /** Inverse CDF */
  //    private def sampleNaive(): T = {
  //      var prob = rand.nextDouble() * totalWeight
  //      val it = valuesWeights.iterator
  //      var vw = it.next()
  //      while (prob > 0.0 && it.hasNext) {
  //        vw = it.next()
  //        prob -= vw._2
  //      }
  //      vw._1
  //    }

  override def sample()(implicit rand: RandomGenerator): T = _valuesArray(sampler.sample())

  override def probabilityOf(value: T): Double =
    valuesWeights.getOrElse(value, 0.0)/totalWeight

  override def toIterator: Iterator[(T, Double)] = {
    (_valuesArray,_probsArray).zipped.toIterator
  }
}

object NonUniformDiscreteDist {
  def apply[T](valuesAndWeights: Map[T, Double])
              (implicit ev: ClassTag[T]): NonUniformDiscreteDist[T] = {
    new NonUniformDiscreteDist[T](valuesAndWeights)
  }

  private def processValuesWeights[T : ClassTag](valuesWeights: Map[T, Double]): (Array[T], Array[Double], Double) = {
    val values = Array.ofDim[T](valuesWeights.size)
    val probs = Array.ofDim[Double](valuesWeights.size)
    val it = valuesWeights.iterator
    var totalWeight: Double = 0.0
    var i = 0
    valuesWeights.foreach { pair =>
      val weight = pair._2
      values(i) = pair._1
      probs(i) = weight
      if (weight < 0 || weight.isInfinity || weight.isNaN) {
        throw new IllegalArgumentException("invalid weight encountered")
      }
      totalWeight += weight
      i += 1
    }
    if (totalWeight.isInfinity) throw new IllegalArgumentException("total weight is not finite")
    if (totalWeight == 0.0) throw new IllegalArgumentException("zero probability mass")
    i = 0
    while (i < probs.length) {
      probs(i) = probs(i)/totalWeight
      i += 1
    }
    (values, probs, totalWeight)
  }
}
