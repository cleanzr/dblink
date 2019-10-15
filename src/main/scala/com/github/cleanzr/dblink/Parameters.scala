// Copyright (C) 2018  Australian Bureau of Statistics
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

case class Parameters(populationSize: Long,
                      maxClusterSize: Int) {
  require(populationSize > 0, "`populationSize` must be a positive integer.")
  require(maxClusterSize > 0, "`maxClusterSize` must be a positive integer.")

  def mkString: String = {
    "populationSize: " + populationSize + "\n" +
    "maxClusterSize: " + maxClusterSize
  }
}