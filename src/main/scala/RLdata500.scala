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
import org.apache.spark.sql.{DataFrame, SparkSession}

object RLdata500 extends Logging {
  /**
    * @param local whether running on laptop (true) or MUPPET (false)
    */
  def build(local: Boolean): Project = {

    // File URI for the source data files
    val dataPath: String =
      if (local) "/home/nmarchant/Dropbox/Employment/AMSIIntern ABS/experiments/RLdata/RLdata500.csv"
      else "/data/neil/RLdata500.csv"

    // Directory used for saving samples + state
    val projectPath: String =
      if (local) "/home/nmarchant/RLdata500_med-prior_collapsed_0.1temp/"
      else "/data/neil/RLdata500_med-prior_collapsed_0.1temp/"

    // Directory used for saving Spark checkpoints
    val checkpointPath: String =
      if (local) "/home/nmarchant/SparkCheckpoint/"
      else "/scratch/neil/SparkCheckpoint/"

    /** Loads the source data files into a Spark DataFrame using the recordsSchema.
      * No transformations are applied.
      */
    val dataFrame: DataFrame = {
      val spark = SparkSession.builder().getOrCreate()
      spark.read.format("csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("nullValue", "NA")
        .load(dataPath)
    }

    val numRecords = dataFrame.count()

    val prior = BetaShapeParameters(1.0, 99.0)
    //val prior = BetaShapeParameters(numRecords * 0.1 * 0.01, numRecords * 0.1)

    Project(
      dataPath = dataPath,
      projectPath = projectPath,
      checkpointPath = checkpointPath,
      recIdAttribute = "rec_id",
      fileIdAttribute = None,
      entIdAttribute = Some("ent_id"),
      matchingAttributes = Array(
        Attribute("by", ConstantSimilarityFn, prior),
        Attribute("bm", ConstantSimilarityFn, prior),
        Attribute("bd", ConstantSimilarityFn, prior),
        Attribute("fname_c1", LevenshteinSimilarityFn(), prior),
        Attribute("lname_c1", LevenshteinSimilarityFn(), prior)
      ),
      partitionFunction = KDTreePartitioner[ValueId](),
      randomSeed = 319158L,
      expectedMaxClusterSize = 10,
      dataFrame = dataFrame
    )
  }

//  // Schema for the source data files (used to read as DataFrame)
//  val recordsSchema = StructType(
//    StructField("fname_c1", StringType, nullable=false) ::
//      StructField("fname_c2", StringType, nullable=false) ::
//      StructField("lname_c1", StringType, nullable=false) ::
//      StructField("lname_c2", StringType, nullable=false) ::
//      StructField("by", StringType, nullable=false) ::
//      StructField("bm", StringType, nullable=false) ::
//      StructField("bd", StringType, nullable=false) ::
//      StructField("rec_id", StringType, nullable=false) ::
//      StructField("ent_id", StringType, nullable=false) :: Nil
//  )
}