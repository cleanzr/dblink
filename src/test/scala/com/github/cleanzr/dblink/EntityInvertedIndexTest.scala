package com.github.cleanzr.dblink

import com.github.cleanzr.dblink.GibbsUpdates.EntityInvertedIndex
import org.scalatest._

class EntityInvertedIndexTest extends FlatSpec {

  def entityIndex = new EntityInvertedIndex

  val allEntityIds = Set(1L, 2L, 3L, 4L)

  lazy val entitiesWithOneDup = Seq(
    Entity(1L, Array(1, 0, 0)),
    Entity(2L, Array(2, 0, 1)),
    Entity(3L, Array(3, 0, 1)),
    Entity(4L, Array(4, 1, 0)),
    Entity(4L, Array(4, 1, 0)) // same entity twice
  )

  behavior of "An entity inverted index (containing one entity)"

  it should "return singleton sets for the entity's attribute values" in {
    val index = new EntityInvertedIndex
    index.add(entitiesWithOneDup.head)
    assert(entitiesWithOneDup.head.values.zipWithIndex.forall {case (valueId, attrId) => index.getEntityIds(attrId, valueId).size === 1})
  }

  behavior of "An entity inverted index (containing multiple entities)"

  it should "return the correct responses to queries" in {
    val index = new EntityInvertedIndex
    entitiesWithOneDup.foreach(entity => index.add(entity))
    assert(index.getEntityIds(0, 1) === Set(1L))
    assert(index.getEntityIds(0, 4) === Set(4L))
    assert(index.getEntityIds(1, 0) === Set(1L, 2L, 3L))
  }
}
