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

package com.github.cleanzr.dblink.partitioning

import scala.collection.mutable
import scala.math.abs

/** Splits a domain of discrete weighted values into two partitions of roughly
  * equal weight.
  *
  * @tparam T value type
  */
abstract class DomainSplitter[T] {
  /** Quality of the split (1.0 is best, 0.0 is worst) */
  val splitQuality: Double

  /** Returns whether value x is in the left (false) or right (true) set
    *
    * @param x value
    * @return
    */
  def apply(x: T): Boolean
}

object DomainSplitter {
  def apply[T : Ordering](domain: Array[(T, Double)]): DomainSplitter[T] = {
    /** Use the set splitter for small domains */
    if (domain.length <= 30) new LPTDomainSplitter[T](domain)
    else new RanDomainSplitter[T](domain)
  }

  /** Range splitter.
    *
    * Splits the domain by sorting the values, then splitting at the (weighted)
    * median.
    *
    * @param domain value-weight pairs for the domain
    * @tparam T type of the values in the domain
    */
  class RanDomainSplitter[T : Ordering](domain: Array[(T, Double)]) extends DomainSplitter[T] with Serializable {
    private val halfWeight = domain.iterator.map(_._2).sum / 2.0

    private val (splitWeight, splitValue) = {
      val numCandidates = domain.length
      val ordered = domain.sortBy(_._1)
      var cumWeight = 0.0
      var i = 0
      while (cumWeight <= halfWeight && i < numCandidates - 1) {
        cumWeight += ordered(i)._2
        i += 1
      }
      (cumWeight, ordered(i)._1)
    }

    override val splitQuality: Double = 1.0 - abs(splitWeight - halfWeight)/halfWeight

    override def apply(x: T): Boolean = Ordering[T].gt(x, splitValue)
  }

  /** LPT splitter.
    *
    * Splits the domain using the longest processing time (LPT) algorithm with
    * only two "processors" (buckets). This is not space efficient for large
    * domains, roughly half of the domain values must be stored internally.
    *
    * @param domain value-weight pairs for the domain
    * @tparam T type of the values in the domain
    */
  class LPTDomainSplitter[T](domain: Array[(T, Double)]) extends DomainSplitter[T] with Serializable {
    private val halfWeight = domain.iterator.map(_._2).sum / 2.0

    private val rightSet = mutable.Set.empty[T]

    private val splitWeight = {
      val ordered = domain.sortBy(-_._2) // decreasing order of weight
      var leftWeight = 0.0
      var rightWeight = 0.0
      ordered.foreach { case (value, weight) =>
        if (leftWeight >= rightWeight) {
          rightSet.add(value)
          rightWeight += weight
        } else {
          // effectively putting value in the left set
          leftWeight += weight
        }
      }
      leftWeight
    }

    override val splitQuality: Double = 1.0 - abs(splitWeight - halfWeight)/halfWeight

    override def apply(x: T): Boolean = rightSet.contains(x)
  }
}