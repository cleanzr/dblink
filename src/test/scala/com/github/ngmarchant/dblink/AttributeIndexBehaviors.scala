package com.github.ngmarchant.dblink

import org.scalatest.{FlatSpec, Matchers}

trait AttributeIndexBehaviors extends Matchers { this: FlatSpec =>

  def genericAttributeIndex(index: AttributeIndex, valuesWeights: Map[String, Double]) {
    it should "have the correct number of values" in {
      assert(index.numValues === valuesWeights.size)
    }

    it should "assign unique value ids in the set {0, ..., numValues - 1}" in {
      assert(valuesWeights.keysIterator.map(stringValue => index.valueIdxOf(stringValue)).toSet ===
        (0 until valuesWeights.size).toSet)
    }

    it should "have the correct probability distribution" in {
      val totalWeight = valuesWeights.foldLeft(0.0)((total, x) => total + x._2)
      assert(valuesWeights.mapValues(_/totalWeight).forall { case (stringValue, correctProb) =>
        index.probabilityOf(index.valueIdxOf(stringValue)) === (correctProb +- 1e-4)
      })
    }

    it should "complain when a probability is requested for a non-indexed value" in {
      assertThrows[RuntimeException] {
        index.probabilityOf(index.numValues + 1)
      }
    }

    it should "complain when a similarity normalization is requested for a non-indexed value" in {
      assertThrows[RuntimeException] {
        index.simNormalizationOf(index.numValues + 1)
      }
    }

    it should "complain when similar values are requested for a non-indexed value" in {
      assertThrows[RuntimeException] {
        index.simValuesOf(index.numValues + 1)
      }
    }

    it should "complain when an exponentiated similarity score is requested for a non-indexed value" in {
      assertThrows[RuntimeException] {
        index.expSimOf(index.numValues + 1, 0)
      }
      assertThrows[RuntimeException] {
        index.expSimOf(0, index.numValues + 1)
      }
    }
  }
}
