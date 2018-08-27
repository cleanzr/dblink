import com.github.ngmarchant.dblink.Entity
import com.github.ngmarchant.dblink.GibbsUpdates.EntityInvertedIndex
import com.github.ngmarchant.dblink.GibbsUpdates.LinksIndex
import com.github.ngmarchant.dblink.GibbsUpdates.getPossibleEntities

val entityIndex = new EntityInvertedIndex

val allEntityIds = Set(1L, 2L, 3L, 4L)

val entities = Seq(
  Entity(1L, Array(1, 0, 0)),
  Entity(2L, Array(2, 0, 1)),
  Entity(3L, Array(3, 0, 1)),
  Entity(4L, Array(4, 1, 0)),
  Entity(4L, Array(4, 1, 0)) // same entity twice
)

entities.foreach{ entity => entityIndex.add(entity)}

entityIndex.getEntityIds(1, 1) == Set(4L)

entityIndex.getEntityIds(1, 0) == Set(1L,2L,3L)

//val records = Seq(2L, 3L, 4L, 3L, 2L, 3L)

//val entityRowIndex = new LinksIndex(allEntityIds.iterator, records.length)
//
//entityRowIndex.isolatedEntityIds.toSet == allEntityIds
//
//records.zipWithIndex.foreach {x => entityRowIndex.addLink(x._1, x._2)}
//
//entityRowIndex.isolatedEntityIds.toSet == Set(1L)
//
//entityRowIndex.getLinkedRows(2L) == Set(0, 4)
//
//val fieldsOrdered = Seq((CategoricalField, 0), (CategoricalField, 1), (CategoricalField, 2))
//
//val queryRecord = Record("000001", "FileA",
//  Attributes(Array((1, true), (0, false), (1, false)), Array.empty[(ValueId, Boolean)])
//)
//
//getPossibleEntities(queryRecord, allEntityIds.iterator, entityIndex, fieldsOrdered.iterator)