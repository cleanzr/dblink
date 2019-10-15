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

import com.esotericsoftware.kryo.Kryo
import com.github.cleanzr.dblink.GibbsUpdates.{EntityInvertedIndex, LinksIndex}
import com.github.cleanzr.dblink.partitioning.{DomainSplitter, KDTreePartitioner, LPTScheduler, MutableBST, PartitionFunction, SimplePartitioner}
import com.github.cleanzr.dblink.random.{AliasSampler, DiscreteDist, IndexNonUniformDiscreteDist, NonUniformDiscreteDist}
import com.github.cleanzr.dblink.GibbsUpdates.{EntityInvertedIndex, LinksIndex}
import com.github.cleanzr.dblink.partitioning._
import com.github.cleanzr.dblink.random.{AliasSampler, DiscreteDist, IndexNonUniformDiscreteDist, NonUniformDiscreteDist}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.serializer.KryoRegistrator

class CustomKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[EntRecPair])
    kryo.register(classOf[PartEntRecTriple])
    kryo.register(classOf[Record[_]])
    kryo.register(classOf[Entity])
    kryo.register(classOf[DistortedValue])
    kryo.register(classOf[SummaryVars])
    kryo.register(classOf[Attribute])
    kryo.register(classOf[SimilarityFn])
    kryo.register(classOf[IndexedAttribute])
    kryo.register(classOf[BetaShapeParameters])
    kryo.register(classOf[EntityInvertedIndex])
    kryo.register(classOf[LinksIndex])
    kryo.register(classOf[DomainSplitter[_]])
    kryo.register(classOf[KDTreePartitioner[_]])
    kryo.register(classOf[LPTScheduler[_,_]])
    kryo.register(classOf[LPTScheduler.Partition[_,_]])
    kryo.register(classOf[MutableBST[_]])
    kryo.register(classOf[PartitionFunction[_]])
    kryo.register(classOf[SimplePartitioner[_]])
    kryo.register(classOf[AliasSampler])
    kryo.register(classOf[DiscreteDist[_]])
    kryo.register(classOf[IndexNonUniformDiscreteDist])
    kryo.register(classOf[NonUniformDiscreteDist[_]])
    kryo.register(Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))
    kryo.register(Class.forName("[[B"))
    kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"))
    kryo.register(Class.forName("org.apache.spark.unsafe.types.UTF8String"))
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[Array[EntRecPair]])
    kryo.register(classOf[RecordsCache])
    kryo.register(classOf[Parameters])
    kryo.register(classOf[AliasSampler])
    kryo.register(classOf[MersenneTwister])
    kryo.register(classOf[DistortionProbs])
  }
}
//
//class AugmentedRecordSerializer extends Serializer[EntRecPair] {
//  override def write(kryo: Kryo, output: Output, `object`: EntRecPair): Unit = {
//    output.writeLong(`object`.latId, true)
//    output.writeString(`object`.recId)
//    output.writeString(`object`.fileId)
//    val numLatFields = `object`.latFieldValues.length
//    output.writeInt(numLatFields)
//    `object`.recFieldValues.foreach(v => output.writeString(v))
//    `object`.distortions.foreach(v => output.writeBoolean(v))
//    `object`.latFieldValues.foreach(v => output.writeString(v))
//  }
//
//  override def read(kryo: Kryo, input: Input, `type`: Class[EntRecPair]): EntRecPair = {
//    val latId = input.readLong(true)
//    val recId = input.readString()
//    val fileId = input.readString()
//    val numLatFields = input.readInt()
//    val numRecFields = if (recId == null) 0 else numLatFields
//    val fieldValues = Array.fill(numRecFields)(input.readString())
//    val distortions = Array.fill(numRecFields)(input.readBoolean())
//    val latFieldValues = Array.fill(numLatFields)(input.readString())
//    LatRecPair(latId, latFieldValues, recId, fileId, fieldValues, distortions)
//  }
//}
