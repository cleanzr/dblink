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

import org.scalatest.FlatSpec

class DistortionProbsTest extends FlatSpec {

  val twoFilesOneAttribute = DistortionProbs(Seq("A", "B"), Iterator(BetaShapeParameters(3.0, 3.0)))

  behavior of "Distortion probabilities for two files and one attribute (with identical shape parameters)"

  it should "return probability 0.5 for both files" in {
    assert(twoFilesOneAttribute(0, "A") === 0.5)
    assert(twoFilesOneAttribute(0, "B") === 0.5)
  }

  it should "complain when a probability is requested for a third file" in {
    assertThrows[NoSuchElementException] {
      twoFilesOneAttribute(0, "C")
    }
  }

  it should "complain when a probability is requested for a second attribute" in {
    assertThrows[NoSuchElementException] {
      twoFilesOneAttribute(1, "A")
    }
  }
}