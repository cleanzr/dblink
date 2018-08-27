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

import org.apache.commons.lang3.StringUtils.getLevenshteinDistance

/** Represents a truncated attribute similarity function */
sealed trait SimilarityFn extends Serializable {
  /** Compute the truncated similarity
    *
    * @param a an attribute value
    * @param b an attribute value
    * @return truncated similarity for `a` and `b`
    */
  def getSimilarity(a: String, b: String): Double

  /** Maximum possible raw/truncated similarity score */
  def maxSimilarity: Double

  /** Minimum possible raw similarity */
  def minSimilarity: Double

  /** Truncation threshold.
    * All raw similarities below this threshold are truncated to zero. */
  def threshold: Double

  def mkString: String
}

object SimilarityFn {
  /** Constant attribute similarity function. */
  case object ConstantSimilarityFn extends SimilarityFn {
    override def getSimilarity(a: String, b: String): Double = 0.0

    override def maxSimilarity: Double = 0.0

    override def minSimilarity: Double = 0.0

    override def threshold: Double = 0.0

    override def mkString: String = "ConstantSimilarityFn"
  }

  abstract class NonConstantSimilarityFn(override val threshold: Double,
                                         override val maxSimilarity: Double) extends SimilarityFn  {
    require(maxSimilarity > 0.0, "`maxSimilarity` must be positive")
    require(threshold >= minSimilarity && threshold < maxSimilarity,
      s"`threshold` must be in the interval [$minSimilarity, $maxSimilarity)")

    protected val transFactor: Double = maxSimilarity / (maxSimilarity - threshold)

    override def getSimilarity(a: String, b: String): Double = {
      val transSim = transFactor * (maxSimilarity * unitSimilarity(a, b) - threshold)
      if (transSim > 0.0) transSim else 0.0
    }

    override def minSimilarity: Double = 0.0

    /** Similarity function that provides scores on the unit interval */
    protected def unitSimilarity(a: String, b: String): Double
  }


  /* Levenshtein attribute similarity function. */
  class LevenshteinSimilarityFn(threshold: Double, maxSimilarity: Double)
    extends NonConstantSimilarityFn(threshold, maxSimilarity) {

    /** Similarity measure based on the normalized Levenshtein distance metric.
      *
      * See the following reference:
      * L. Yujian and L. Bo, “A Normalized Levenshtein Distance Metric,”
      * IEEE Transactions on Pattern Analysis and Machine Intelligence,
      * vol. 29, no. 6, pp. 1091–1095, Jun. 2007.
      */
    override protected def unitSimilarity(a: String, b: String): Double = {
      val totalLength = a.length + b.length
      if (totalLength > 0) {
        val dist = getLevenshteinDistance(a, b).toDouble
        1.0 - 2.0 * dist / (totalLength + dist)
      } else 1.0
    }

    override def mkString: String = s"LevenshteinSimilarityFn(threshold=$threshold, maxSimilarity=$maxSimilarity)"
  }

  object LevenshteinSimilarityFn {
    def apply(threshold: Double = 7.0, maxSimilarity: Double = 10.0) =
      new LevenshteinSimilarityFn(threshold, maxSimilarity)
  }
}





