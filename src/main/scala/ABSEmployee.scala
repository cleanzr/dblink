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

import com.github.ngmarchant.dblink.SimilarityFn._
import com.github.ngmarchant.dblink._
import com.github.ngmarchant.dblink.partitioning.KDTreePartitioner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ABSEmployee extends Logging {

  /**
    * @param local whether running on laptop (true) or MUPPET (false)
    * @param full whether to use the full dataset (true) or a single block (false)
    */
  def build(local: Boolean, full: Boolean): Project = {

    // File URI for the source data files
    val dataPath: String =
      if (local) "/home/nmarchant/Dropbox/Employment/AMSIIntern ABS/experiments/synthetic-data/EDM Simulation v2.FILE*.mac.csv"
      else "/scratch/neil/EDM Simulation v2.FILE*.mac.csv"

    // Directory used for saving samples + state
    val projectPath: String =
      if (local) "/home/nmarchant/ABSEmployee_6levels_med-prior_collapsed/"
      else "/data/neil/ABSEmployee_6levels_med-prior_collapsed/"

    // Directory used for saving Spark checkpoints
    val checkpointPath: String =
      if (local) "/home/nmarchant/SparkCheckpoint/"
      else "/scratch/neil/SparkCheckpoint/"

    // Maps a file path to a unique file identifier
    val pathToFileId: String => String = (path: String) => {
      path.split("/").last.substring(26, 27)
    }

    /** Loads the source data files into a Spark DataFrame using the recordsSchema.
      * No transformations are applied.
      */
    val dataFrame: DataFrame = {
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      spark.udf.register("get_file_name", pathToFileId)

      val df = if (full) {
        spark.read.format("csv")
          .option("header", "true")
          .option("mode", "DROPMALFORMED")
          .option("nullValue", ".")
          .load(dataPath)
          .na.drop() // drop rows with nulls (since EBER doesn't handle missing values yet)
      } else {
        spark.read.format("csv")
          .option("header", "true")
          .option("mode", "DROPMALFORMED")
          .option("nullValue", ".")
          .load(dataPath)
          .na.drop() // drop rows with nulls (since EBER doesn't handle missing values yet)
          .filter(r => r.getString(1) == "10001")
      }

      df.withColumnRenamed("recid", "ent_id")
        .withColumn("file_id", callUDF("get_file_name", input_file_name))
        .withColumn("rec_id", concat($"file_id", $"ent_id"))
    }

    val numRecords = dataFrame.count()

    //val prior = BetaShapeParameters(1.0, 99.0)
    val prior = BetaShapeParameters(numRecords * 0.1 * 0.01, numRecords * 0.1)

    Project(
      dataPath = dataPath,
      projectPath = projectPath,
      checkpointPath = checkpointPath,
      recIdAttribute = "rec_id",
      fileIdAttribute = Some("file_id"),
      entIdAttribute = Some("ent_id"),
      matchingAttributes = IndexedSeq(
        Attribute("mb", ConstantSimilarityFn, prior),
        Attribute("bday", ConstantSimilarityFn, prior),
        Attribute("byear", ConstantSimilarityFn, prior),
        Attribute("sex", ConstantSimilarityFn, prior)
      ),
      partitionFunction = KDTreePartitioner[ValueId](),
      randomSeed = 319158L,
      expectedMaxClusterSize = 10,
      dataFrame = dataFrame
    )
  }
//  // Schema for the source data files (used to read as DataFrame)
//  val recordsSchema = StructType(
//    StructField("recid", StringType, nullable = false) ::
//      StructField("sa1", StringType, nullable = false) ::
//      StructField("mb", StringType, nullable = false) ::
//      StructField("bday", StringType, nullable = false) ::
//      StructField("byear", StringType, nullable = false) ::
//      StructField("sex", StringType, nullable = false) ::
//      StructField("industry", StringType, nullable = false) ::
//      StructField("casual", StringType, nullable = false) ::
//      StructField("fulltime", StringType, nullable = false) ::
//      StructField("hours", StringType, nullable = false) ::
//      StructField("payrate", StringType, nullable = false) ::
//      StructField("awe", StringType, nullable = false) :: Nil
//  )
//lazy val parameters = Parameters(
//  distortionPrior = ,
//  numEntities = numRecords,
//  randomSeed = ,
//  maxClusterSize = 2 + 8
//)
}