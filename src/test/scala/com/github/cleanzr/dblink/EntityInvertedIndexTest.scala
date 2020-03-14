package com.github.cleanzr.dblink

import com.github.cleanzr.dblink.GibbsUpdates.EntityInvertedIndex
import org.scalatest._

class EntityInvertedIndexTest extends FlatSpec {

  def entityIndex = new EntityInvertedIndex

  val allEntityIds = Set(1, 2, 3, 4)

  lazy val entitiesWithOneDup = Seq(
    (1, Entity(Array(1, 0, 0))),
    (2, Entity(Array(2, 0, 1))),
    (3, Entity(Array(3, 0, 1))),
    (4, Entity(Array(4, 1, 0))),
    (4, Entity(Array(4, 1, 0))) // same entity twice
  )

  behavior of "An entity inverted index (containing one entity)"

  it should "return singleton sets for the entity's attribute values" in {
    val index = new EntityInvertedIndex
    index.add(entitiesWithOneDup.head._1, entitiesWithOneDup.head._2)
    assert(entitiesWithOneDup.head._2.values.zipWithIndex.forall {case (valueId, attrId) => index.getEntityIds(attrId, valueId).size === 1})
  }

  behavior of "An entity inverted index (containing multiple entities)"

  it should "return the correct responses to queries" in {
    val index = new EntityInvertedIndex
    entitiesWithOneDup.foreach { case (entId, entity) => index.add(entId, entity) }
    assert(index.getEntityIds(0, 1) === Set(1))
    assert(index.getEntityIds(0, 4) === Set(4))
    assert(index.getEntityIds(1, 0) === Set(1, 2, 3))
  }
}
