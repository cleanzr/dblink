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

import com.github.ngmarchant.dblink.SimilarityFn._
import org.scalatest.FlatSpec

class SimilarityFnTest extends FlatSpec {
  behavior of "A constant similarity function"

  it should "return the same (constant) value for the max, min, threshold similarities" in {
    assert(ConstantSimilarityFn.maxSimilarity === ConstantSimilarityFn.minSimilarity)
    assert(ConstantSimilarityFn.maxSimilarity === ConstantSimilarityFn.threshold)
  }

  it should "return the max (constant) similarity for identical values" in {
    assert(ConstantSimilarityFn.getSimilarity("TestValue", "TestValue") === ConstantSimilarityFn.maxSimilarity)
  }

  it should "return the max (constant) similarity for distinct values" in {
    assert(ConstantSimilarityFn.getSimilarity("TestValue1", "TestValue2") === ConstantSimilarityFn.maxSimilarity)
  }

  def thresSimFn: LevenshteinSimilarityFn = LevenshteinSimilarityFn(5.0, 10.0)
  def noThresSimFn: LevenshteinSimilarityFn = LevenshteinSimilarityFn(0.0, 10.0)

  behavior of "A Levenshtein similarity function (maxSimilarity=10.0 and threshold=5.0)"

  it should "return the max similarity for an identical pair of non-empty strings" in {
    assert(thresSimFn.getSimilarity("John Smith", "John Smith") === thresSimFn.maxSimilarity)
  }

  it should "return the max similarity for a pair of empty strings" in {
    assert(thresSimFn.getSimilarity("", "") === thresSimFn.maxSimilarity)
  }

  it should "return the min similarity for an empty and non-empty string" in {
    assert(thresSimFn.getSimilarity("", "John Smith") === thresSimFn.minSimilarity)
  }

  it should "be symmetric in its arguments" in {
    assert(thresSimFn.getSimilarity("Jane Smith", "John Smith") === thresSimFn.getSimilarity("John Smith", "Jane Smith"))
  }

  it should "return a similarity of 2.0 for the strings 'AB' and 'BB'" in {
    assert(thresSimFn.getSimilarity("AB", "BB") === 2.0)
  }

  behavior of "A Levenshtein similarity function with no threshold (maxSimilarity=10.0)"

  it should "return the same value for the min, threshold similarities" in {
    assert(noThresSimFn.threshold === noThresSimFn.minSimilarity)
  }

  it should "return a similarity of 6.0 for the strings 'AB' and 'BB'" in {
    assert(noThresSimFn.getSimilarity("AB", "BB") === 6.0)
  }
}
