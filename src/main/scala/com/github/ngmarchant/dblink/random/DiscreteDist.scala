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

/** A distribution over a discrete set of values
  *
  * @tparam T type of the values
  */
trait DiscreteDist[T] extends Serializable {

  /** Values in the support set */
  def values: Traversable[T]

  /** Number of values in the support set */
  def numValues: Int

  /** Total weight before normalization */
  def totalWeight: Double

  /** Draw a value according to the distribution
    *
    * @return a value from the support set
    */
  def sample(): T

  /** Get the probability mass associated with a value
    *
    * @param value a value from the support set
    * @return probability. Returns 0.0 if the value is not in the support set.
    */
  def probabilityOf(value: T): Double

  /** Change the internal random generator
    *
    * @param rng a RandomGenerator
    */
  def setRand(rng: RandomGenerator): Unit
}

object DiscreteDist {
  def apply[T](valuesAndWeights: Map[T, Double])
              (implicit rand: RandomGenerator, ev: ClassTag[T]): NonUniformDiscreteDist[T] = {
    NonUniformDiscreteDist[T](valuesAndWeights)
  }

  def apply(weights: Traversable[Double])
           (implicit rand: RandomGenerator): IndexNonUniformDiscreteDist = {
    IndexNonUniformDiscreteDist(weights)
  }
}