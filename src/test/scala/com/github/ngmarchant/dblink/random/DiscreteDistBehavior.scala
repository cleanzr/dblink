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

import org.scalatest.{FlatSpec, Matchers}

trait DiscreteDistBehavior extends Matchers { this: FlatSpec =>

  def genericDiscreteDist[T](dist: DiscreteDist[T], valueOutsideSupport: T): Unit = {
    it should "be normalised" in {
      assert(dist.values.foldLeft(0.0) { case (sum, v) => sum + dist.probabilityOf(v) } === (1.0 +- 1e-9) )
    }

    it should "return valid probabilities for all values in the support" in {
      assert(dist.values.forall {value =>
        val prob = dist.probabilityOf(value)
        prob >= 0 && !prob.isInfinity && !prob.isNaN && prob <= 1})
    }

    it should "return a probability of 0.0 for a value outside the support" in {
      assert(dist.probabilityOf(valueOutsideSupport) === 0.0)
    }

    it should "return `numValues` equal to the size of `values`" in {
      assert(dist.numValues === dist.values.size)
    }

    it should "return a valid `totalWeight`" in {
      assert(dist.totalWeight > 0)
      assert(!dist.totalWeight.isNaN && !dist.totalWeight.isInfinity)
    }

    it should "not return sample values that have probability 0.0" in {
      assert((1 to 1000).map(_ => dist.sample()).forall(v => dist.probabilityOf(v) > 0.0))
    }
  }


}
