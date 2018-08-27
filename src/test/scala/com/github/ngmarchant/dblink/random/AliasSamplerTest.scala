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

import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.scalatest.{FlatSpec, Matchers}

class AliasSamplerTest extends FlatSpec with Matchers {
  implicit val rand: RandomGenerator = new MersenneTwister(1)

  def regularProbs = IndexedSeq(0.1, 0.2, 0.7)
  def extremeProbs = IndexedSeq(0.000000001, 0.000000001, 0.999999999)

  def empiricalDistributionIsConsistent(inputProbs: IndexedSeq[Double],
                                        sampler: AliasSampler,
                                        numSamples: Int = 100000000,
                                        tolerance: Double = 1e-4): Boolean = {
    val empiricalProbs = Array.fill(sampler.size)(0.0)
    var i = 0
    while (i < numSamples) {
      empiricalProbs(sampler.sample()) += 1.0/numSamples
      i += 1
    }
    (empiricalProbs, inputProbs).zipped.forall( (pE, pT) => pT === (pT +- tolerance))
  }

  behavior of "An Alias sampler"

  it should "complain when initialized with a negative weight" in {
    assertThrows[IllegalArgumentException]  {
      AliasSampler(Seq(-1.0, 1.0))
    }
  }

  it should "complain when initialized with a NaN weight" in {
    assertThrows[IllegalArgumentException]  {
      AliasSampler(Seq(0.0/0.0, 1.0))
    }
  }

  it should "complain when initialized with an infinite weight" in {
    assertThrows[IllegalArgumentException]  {
      AliasSampler(Seq(1.0/0.0, 1.0))
    }
  }

  it should "produce an asymptotic empirical distribution that is consistent with the input distribution [0.1, 0.2, 0.7]" in {
    val sampler = AliasSampler(regularProbs)
    assert(empiricalDistributionIsConsistent(regularProbs, sampler))
  }

  it should "produce an asymptotic empirical distribution that is consistent with the input distribution [0.000000001, 0.000000001, 0.999999999]" in {
    val sampler = AliasSampler(extremeProbs)
    assert(empiricalDistributionIsConsistent(extremeProbs, sampler))
  }
}
