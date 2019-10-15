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

package com.github.cleanzr.dblink.accumulators

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Accumulates arrays of values corresponding to keys.
  * e.g. if we add (K1 -> "C") to the accumulator and (K1 -> ArrayBuffer("A","B"))
  * is the current key-value pair, the resulting key value pair will be
  * (K1 -> ArrayBuffer("A", B", "C")).
  *
  * @tparam K key type
  * @tparam V value type (for elements of set)
  */
class MapArrayAccumulator[K,V] extends AccumulatorV2[(K,V), Map[K, mutable.ArrayBuffer[V]]] {
  private val _map = mutable.HashMap.empty[K, mutable.ArrayBuffer[V]]

  override def reset(): Unit = _map.clear()

  override def add(x: (K,V)): Unit = {
    val _array = _map.getOrElseUpdate(x._1, mutable.ArrayBuffer.empty[V])
    _array += x._2
  }

  private def add_ks(x: (K, Traversable[V])): Unit = {
    val _array = _map.getOrElseUpdate(x._1, mutable.ArrayBuffer.empty[V])
    x._2.foreach { v => _array += v }
  }

  override def value: Map[K, mutable.ArrayBuffer[V]] = _map.toMap

  override def isZero: Boolean = _map.isEmpty

  def toIterator: Iterator[(K, mutable.ArrayBuffer[V])] = _map.iterator

  override def copy(): MapArrayAccumulator[K, V] = {
    val newAcc = new MapArrayAccumulator[K, V]
    _map.foreach( x => newAcc._map.update(x._1, x._2.clone()))
    newAcc
  }

  override def merge(other: AccumulatorV2[(K, V), Map[K, mutable.ArrayBuffer[V]]]): Unit =
    other match {
      case o: MapArrayAccumulator[K, V] => o.toIterator.foreach {x => this.add_ks(x)}
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
}