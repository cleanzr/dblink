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

package com.github.cleanzr.dblink.random

import org.apache.commons.math3.random.RandomGenerator

class IndexNonUniformDiscreteDist(weights: Traversable[Double]) extends DiscreteDist[Int] {
  require(weights.nonEmpty, "`weights` must be non-empty")

  private val (_probsArray, _totalWeight) = IndexNonUniformDiscreteDist.processWeights(weights)

  override def totalWeight: Double = _totalWeight

  override def numValues: Int = _probsArray.length

  override def values: Traversable[Int] = 0 until numValues

  /** AliasSampler to efficiently sample from the distribution */
  private val sampler = AliasSampler(_probsArray, checkWeights = false, normalized = true)

  //    /** Inverse CDF */
  //    private def sampleNaive(): Int = {
  //      var prob = rand.nextDouble() * totalWeight
  //      var idx = 0
  //      while (prob > 0.0 && idx < numValues) {
  //        prob -= weights(idx)
  //        idx += 1
  //      }
  //      if (prob > 0.0) idx else idx - 1
  //    }

  override def sample()(implicit rand: RandomGenerator): Int = sampler.sample()

  override def probabilityOf(idx: Int): Double = {
    if (idx < 0 || idx >= numValues) 0.0
    else _probsArray(idx)
  }

  override def toIterator: Iterator[(Int, Double)] = {
    (0 until numValues).zip(_probsArray).toIterator
  }
}

object IndexNonUniformDiscreteDist {
  def apply(weights: Traversable[Double]): IndexNonUniformDiscreteDist = {
    new IndexNonUniformDiscreteDist(weights)
  }

  private def processWeights(weights: Traversable[Double]): (Array[Double], Double) = {
    val probs = Array.ofDim[Double](weights.size)
    var totalWeight: Double = 0.0
    var i = 0
    weights.foreach { weight =>
      if (weight < 0 || weight.isInfinity || weight.isNaN) {
        throw new IllegalArgumentException("invalid weight encountered")
      }
      probs(i) = weight
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
    (probs, totalWeight)
  }
}