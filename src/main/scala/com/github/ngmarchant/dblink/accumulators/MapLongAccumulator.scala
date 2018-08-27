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

package com.github.ngmarchant.dblink.accumulators

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * Accumulates counts corresponding to keys.
  * e.g. if we add K1 to the accumulator and (K1 -> 10L) is the current
  * key-value pair, the resulting key value pair will be (K1 -> 11L).
  *
  * @tparam K key type
  */
class MapLongAccumulator[K] extends AccumulatorV2[(K, Long), Map[K, Long]] {
  private val _map = mutable.HashMap.empty[K, Long]

  override def reset(): Unit = _map.clear()

  override def add(kv: (K, Long)): Unit = {
    _map.update(kv._1, _map.getOrElse(kv._1, 0l) + kv._2)
  }

  override def value: Map[K, Long] = _map.toMap

  override def isZero: Boolean = _map.isEmpty

  override def copy(): MapLongAccumulator[K] = {
    val newAcc = new MapLongAccumulator[K]
    newAcc._map ++= _map
    newAcc
  }

  def toIterator: Iterator[(K, Long)] = _map.iterator

  override def merge(other: AccumulatorV2[(K, Long), Map[K, Long]]): Unit = other match {
    case o: MapLongAccumulator[K] => o.toIterator.foreach { x => this.add(x) }
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }
}