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

import org.apache.commons.math3.random.{MersenneTwister, RandomGenerator}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import com.github.ngmarchant.dblink.SimilarityFn._

class AttributeIndexTest extends FlatSpec with AttributeIndexBehaviors with Matchers {

  implicit val rand: RandomGenerator = new MersenneTwister(0)
  implicit val sc: SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("d-blink test")
    SparkContext.getOrCreate(conf)
  }
  sc.setLogLevel("WARN")

  lazy val stateWeights = Map("Australian Capital Territory" -> 0.410,
    "New South Wales" -> 7.86, "Northern Territory" -> 0.246, "Queensland" -> 4.92,
    "South Australia" -> 1.72, "Tasmania" -> 0.520, "Victoria" -> 6.32,
    "Western Australia" -> 2.58)

  lazy val constantIndex = AttributeIndex(stateWeights, ConstantSimilarityFn)

  lazy val nonConstantIndex = AttributeIndex(stateWeights, LevenshteinSimilarityFn(5.0, 10.0))

  /** The following results are for an index with LevenshteinSimilarityFn(5.0, 10.0) */
  def stateSimNormalizations = Map("Australian Capital Territory" -> 0.0027140755302269004,
    "New South Wales" -> 1.4193905286944585E-4,
    "Northern Territory" -> 0.00451528932619675,
    "Queensland" -> 2.2673706056780077E-4,
    "South Australia" -> 6.465919296781136E-4,
    "Tasmania" -> 0.00214117348291189,
    "Victoria" -> 1.7651936247903708E-4,
    "Western Australia" -> 4.317863538883541E-4)

  def simValuesSA = Map(7 -> 39.813678188084864, 4 -> 22026.465794806718)

  val expSimSAWA = 39.813678188084864
  val expSimVICTAS = 1.0

  "An attribute index (with a constant similarity function)" should behave like genericAttributeIndex(constantIndex, stateWeights)

  it should "return a similarity normalization constant of 1.0 for all values" in {
    assert((0 until constantIndex.numValues).forall(valueId => constantIndex.simNormalizationOf(valueId) == 1.0))
  }

  it should "return no similar values for all values" in {
    assert((0 until constantIndex.numValues).forall(valueId => constantIndex.simValuesOf(valueId).isEmpty))
  }

  it should "return an exponentiated similarity score of 1.0 for all value pairs" in {
    val allValueIds = 0 until constantIndex.numValues
    val allValueIdPairs = allValueIds.flatMap(valueId1 => allValueIds.map(valueId2 => (valueId1, valueId2)))
    assert(allValueIdPairs.forall { case (valueId1, valueId2) => constantIndex.expSimOf(valueId1, valueId2) == 1.0 })
  }

  "An attribute index (with a non-constant similarity function)" should behave like genericAttributeIndex(nonConstantIndex, stateWeights)

  it should "return the correct similarity normalization constants for all values" in {
    assert(stateSimNormalizations.forall { case (stringValue, trueSimNorm) =>
      nonConstantIndex.simNormalizationOf(nonConstantIndex.valueIdxOf(stringValue)) === (trueSimNorm +- 1e-4)
    })
  }

  it should "return the correct similar values for a query value" in {
    val testSimValues = nonConstantIndex.simValuesOf(nonConstantIndex.valueIdxOf("South Australia"))
    assert(simValuesSA.keySet === testSimValues.keySet)
    assert(simValuesSA.forall { case (valueId, trueExpSim) => testSimValues(valueId) === (trueExpSim +- 1e-4) })
  }

  it should "return the correct exponeniated similarity score for a query value pair" in {
    val valueIdSA = nonConstantIndex.valueIdxOf("South Australia")
    val valueIdWA = nonConstantIndex.valueIdxOf("Western Australia")
    assert(nonConstantIndex.expSimOf(valueIdSA, valueIdWA) === (expSimSAWA +- 1e-4))
    val valueIdVIC = nonConstantIndex.valueIdxOf("Victoria")
    val valueIdTAS = nonConstantIndex.valueIdxOf("Tasmania")
    assert(nonConstantIndex.expSimOf(valueIdVIC, valueIdTAS) === (expSimVICTAS +- 1e-4))
  }
}
