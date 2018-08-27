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

package com.github.ngmarchant.dblink

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.ConfigFactory

object Rundblink extends App with Logging {

  //val config = ConfigFactory.parseFile(new File(args.head))
  val config = ConfigFactory.parseFile(new File("/home/nmarchant/Dropbox/Employment/AMSIIntern ABS/dblink/RLdata10000.conf")).resolve()

  // Create SparkContext *before* processing project config, as it's needed to
  // verify aspects of the config
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("dblink")

  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("WARN")

  val project = Project(config)
  println(project.mkString)

  val actions = ProjectActions(config, project)
  println("\n" + actions.mkString)

  sc.setCheckpointDir(project.checkpointPath)

  actions.execute()

//  def evaluate(projectPath: String,
//               membership: RDD[(RecordId, EntityId)],
//               linkageChain: RDD[LinkageState],
//               lowerIterationCutoff: Long = 0L): Unit = {
//    import com.github.ngmarchant.dblink.analysis.implicits._
//
//    val trueClusters = Clusters(membership)
//    info("Writing true clusters to disk.")
//    trueClusters.saveCsv(projectPath + "trueClusters.csv")
//
//    info("Writing cluster size distribution along the chain to disk.")
//    linkageChain.clusterSizeDistribution(projectPath)
//
//    val mpc = linkageChain.filter(_.iteration >= lowerIterationCutoff).mostProbableClusters.persist()
//    info("Evaluating most probable clusters against ground truth.")
//    val mpcClusters = mpc.toClusters
//    info(PairwiseMetrics(mpcClusters.toPairwiseLinks, trueClusters.toPairwiseLinks).mkString)
//
//    val sMPC = LinkageChain._sharedMostProbableClusters(mpc).persist()
//    info("Writing shared most probable clusters to disk.")
//    sMPC.saveCsv(projectPath + "sMPC.csv")
//    mpc.unpersist()
//
//    info("Evaluating shared most probable clusters against ground truth.")
//    info(s"True number of entities:        ${trueClusters.count()}")
//    info(s"Predicted number of entities:   ${sMPC.count()}")
//    info(PairwiseMetrics(sMPC.toPairwiseLinks, trueClusters.toPairwiseLinks).mkString)
//    info(ClusteringMetrics(sMPC, trueClusters).mkString)
//    sMPC.unpersist()
//  }
}