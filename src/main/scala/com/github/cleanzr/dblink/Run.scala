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

package com.github.cleanzr.dblink

import com.github.cleanzr.dblink.util.{BufferedFileWriter, PathToFileConverter}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path

object Run extends App with Logging {

  val spark = SparkSession.builder().appName("dblink").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val configFile = PathToFileConverter.fileToPath(new Path(args.head), sc.hadoopConfiguration)

  val config = ConfigFactory.parseFile(configFile).resolve()

  val project = Project(config)
  val writer = BufferedFileWriter(project.outputPath + "run.txt", append = false, sparkContext = project.sparkContext)
  writer.write(project.mkString)

  val steps = ProjectSteps(config, project)
  writer.write("\n" + steps.mkString)
  writer.close()

  sc.setCheckpointDir(project.checkpointPath)

  steps.execute()

  sc.stop()
}
