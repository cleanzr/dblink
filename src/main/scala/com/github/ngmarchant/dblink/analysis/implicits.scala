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

package com.github.ngmarchant.dblink.analysis

import com.github.ngmarchant.dblink.{Cluster, LinkageChain, LinkageState, RecordId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object implicits {
  implicit def toLinkageChain(rdd: RDD[LinkageState]): LinkageChain = new LinkageChain(rdd)

  implicit def toMostProbableClusters(rdd: RDD[(RecordId, (Cluster, Double))]): MostProbableClusters =
    new MostProbableClusters(rdd)

  implicit def toClusters(rdd: RDD[Cluster]): Clusters = new Clusters(rdd)

  implicit def toPairwiseLinks(rdd: RDD[(RecordId, RecordId)]): PairwiseLinks = new PairwiseLinks(rdd)

  implicit def toClusters2[T : ClassTag](rdd: RDD[(RecordId, T)]): Clusters = Clusters(rdd)
}
