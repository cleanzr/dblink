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

package com.github.cleanzr.dblink.partitioning

import scala.collection.mutable.ArrayBuffer
import scala.math.pow

/** ArrayBuffer-based binary search tree.
  * Not very generic. Consider combining with KDTreePartitioner.
  * Fast to search, but not space efficient for imbalanced trees (not
  * a problem for our application where the trees are small and ideally
  * perfectly balanced.)
  *
  * @tparam A type of field values
  */
class MutableBST[A : Ordering] extends Serializable {
  case class Node(var attributeId: Int, var splitter: DomainSplitter[A], var value: Int)

  private val _nodes = ArrayBuffer[Node](Node(-1, null, 0))
  private var _numLevels: Int = 0
  private var _numLeaves: Int = 1

  def numLeaves: Int = _numLeaves
  def numLevels: Int = _numLevels
  def isEmpty: Boolean = _nodes.head.splitter == null
  def nonEmpty: Boolean = _nodes.head.splitter != null

  /** Search the tree for the leaf node corresponding to the given set of attribute
    * values, and return the "number" of the leaf.
    *
    * @param values a set of attribute values
    * @return leaf number (in 0, ..., numLeaves)
    */
  def getLeafNumber(values: IndexedSeq[A]): Int = {
    val nodeId = getLeafNodeId(values)
    _nodes(nodeId).value
  }

  /** Search the tree for the leaf node corresponding to the given set of attribute
    * values.
    *
    * @param attributes a set of attribute values
    * @return id of the leaf node
    */
  def getLeafNodeId(attributes: IndexedSeq[A]): Int = {
    // TODO: check whether point has too many or too few dimensions
    var found = false
    var nodeId = 0
    while (!found && nodeId < _nodes.length) {
      val node = _nodes(nodeId)
      if (node == null) println("ERROR!!!!")
      if (node.splitter != null) {
        val pointVal = attributes(node.attributeId)
        // descend to next level
        if (node.splitter(pointVal)) nodeId = 2*nodeId + 2 // go right
        else nodeId = 2*nodeId + 1 // go left
      } else {
        found = true // at a leaf node
      }
    }
    nodeId
  }

  /** Split an existing node of the tree into two leaf nodes
    *
    * @param nodeId id of node to split
    * @param attributeId attribute associated with split
    * @param splitter splitter (specifies whether to go left or right)
    */
  def splitNode(nodeId: Int, attributeId: Int,
                splitter: DomainSplitter[A]): Unit = {
    /** Ensure that the node already exists as a leaf node. */
    val node = if (nodeId < _nodes.length) _nodes(nodeId) else null
    require(node != null, "node does not exist")
    require(node.splitter == null, "node is already split")

    /** Update the node */
    node.attributeId = attributeId
    node.splitter = splitter

    /** Insert children for the node */
    val leftChildId = 2*nodeId + 1
    val rightChildId = 2*nodeId + 2
    /** If `node` was a leaf at the lowest level, need to add space for its children */
    if (leftChildId >= _nodes.length) {
      // grow array for storing nodes
      _numLevels += 1
      val newLevelSize = pow(2.0, _numLevels).toInt
      _nodes ++= Iterator.fill(newLevelSize)(null)
    }
    _nodes(leftChildId) = Node(-1, null, node.value)
    _nodes(rightChildId) = Node(-1, null, _numLeaves)
    _numLeaves += 1
  }
}
