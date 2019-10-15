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

package com.github.cleanzr.dblink

/** Container for the distortion probabilities (which vary for each file
  * and attribute).
  *
  * @param probs map used internally to store the probabilities
  */
case class DistortionProbs(probs: Map[(AttributeId, FileId), Double]) {
  def apply(attrId: AttributeId, fileId: FileId): Double = probs.apply((attrId, fileId))
}

object DistortionProbs {
  /** Generate distortion probabilities based on the prior mean */
  def apply(fileIds: Traversable[FileId],
            distortionPrior: Iterator[BetaShapeParameters]): DistortionProbs = {
    require(fileIds.nonEmpty, "`fileIds` cannot be empty")
    require(distortionPrior.nonEmpty, "`distortionPrior` cannot be empty")

    val probs = distortionPrior.zipWithIndex.flatMap { case (BetaShapeParameters(alpha, beta), attrId) =>
      fileIds.map { fileId => ((attrId, fileId), alpha / (alpha + beta)) }
    }.toMap

    DistortionProbs(probs)
  }
}