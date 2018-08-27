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
import org.scalatest.FlatSpec

class DiscreteDistTest extends FlatSpec with DiscreteDistBehavior {

  implicit val rand: RandomGenerator = new MersenneTwister

  def valuesWeights = Map("A" -> 100.0, "B" -> 200.0, "C" -> 700.0)
  val indexDist = DiscreteDist(valuesWeights.values)
  val dist = DiscreteDist(valuesWeights)

  "A discrete distribution (without values given)" should behave like genericDiscreteDist(indexDist, 5)

  "A discrete distribution (with values given)" should behave like genericDiscreteDist(dist, "D")
}
