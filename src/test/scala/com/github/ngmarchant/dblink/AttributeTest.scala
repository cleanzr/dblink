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
import com.github.ngmarchant.dblink.SimilarityFn._

class AttributeTest extends FlatSpec {
  behavior of "An attribute with a constant similarity function"

  it should "be constant" in {
    assert(Attribute("name", ConstantSimilarityFn, BetaShapeParameters(1.0, 1.0)).isConstant === true)
  }

  behavior of "An attribute with a non-constant similarity function"

  it should "not be constant" in {
    assert(Attribute("name", LevenshteinSimilarityFn(), BetaShapeParameters(1.0, 1.0)).isConstant === false)
  }
}
