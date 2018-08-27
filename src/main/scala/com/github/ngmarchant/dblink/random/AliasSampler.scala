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

package com.github.ngmarchant.dblink.random

import com.github.ngmarchant.dblink.random.AliasSampler._
import org.apache.commons.math3.random.RandomGenerator

class AliasSampler(weights: Traversable[Double], checkWeights: Boolean, normalized: Boolean)
                  (implicit rand: RandomGenerator) extends Serializable {

  private var rng = rand

  def size: Int = weights.size

  private val (_probabilities, _aliasTable) = computeTables(weights, checkWeights, normalized)

  def probabilities: IndexedSeq[Double] = _probabilities

  def sample(): Int = {
    val U = rng.nextDouble() * size
    val i = U.toInt
    if (U < _probabilities(i)) i else _aliasTable(i)
  }

  def sample(sampleSize: Int): Array[Int] = {
    Array.tabulate(sampleSize)(_ => this.sample())
  }

  // TODO: this should probably return a new instance rather than modifying in-place
  def setRand(rand: RandomGenerator): Unit = {this.rng = rand}
}

object AliasSampler {
  def apply(weights: Traversable[Double], checkWeights: Boolean = true, normalized: Boolean = false)
           (implicit rand: RandomGenerator): AliasSampler = {
    new AliasSampler(weights, checkWeights, normalized)
  }

  private def computeTables(weights: Traversable[Double],
                            checkWeights: Boolean,
                            normalized: Boolean): (Array[Double], Array[Int]) = {
    val size = weights.size

    val totalWeight = if (checkWeights && !normalized) {
      weights.foldLeft(0.0) { (sum, weight) =>
        if (weight < 0 || weight.isInfinity || weight.isNaN) {
          throw new IllegalArgumentException("invalid weight encountered")
        }
        sum + weight
      }
    } else if (checkWeights && normalized) {
      weights.foreach { weight =>
        if (weight < 0 || weight.isInfinity || weight.isNaN) {
          throw new IllegalArgumentException("invalid weight encountered")
        }
      }
      1.0
    } else if (!checkWeights && !normalized) {
      weights.sum
    } else 1.0

    require(totalWeight > 0.0, "zero probability mass")

    val probabilities = weights.map( weight => weight * size / totalWeight).toArray

    val aliasTable = Array.ofDim[Int](size)

    /** Store small and large worklists in a single array.
      * "Small" elements are stored on the left side, and "large" elements are
      * stored on the right side */
    val worklist = Array.ofDim[Int](size)
    var posSmall = 0
    var posLarge = size

    /** Fill worklists */
    var i = 0
    while(i < size) {
      if (probabilities(i) < 1.0) {
        worklist(posSmall) = i // add index i to the small worklist
        posSmall += 1
      } else {
        posLarge -= 1 // add index i to the large worklist
        worklist(posLarge) = i
      }
      i += 1
    }

    /** Remove elements from worklists */
    if (posSmall > 0) { // both small and large worklists contain elements
      posSmall = 0
      while (posSmall < size && posLarge < size) {
        val l = worklist(posSmall)
        posSmall += 1 // "remove" element from small worklist
        val g = worklist(posLarge)
        aliasTable(l) = g
        probabilities(g) = (probabilities(g) + probabilities(l)) - 1.0
        if (probabilities(g) < 1.0) posLarge += 1 // "move" element g to small worklist
      }
    }

    /** Since the uniform on [0,1] in the `sample` method is shifted by i */
    i = 0
    while (i < size) {
      probabilities(i) += i
      i += 1
    }
    
    (probabilities, aliasTable)
  }
}