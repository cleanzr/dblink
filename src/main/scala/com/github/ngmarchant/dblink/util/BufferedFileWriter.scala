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

package com.github.ngmarchant.dblink.util

import java.io.{BufferedWriter, OutputStreamWriter}

import com.github.ngmarchant.dblink.util.BufferedFileWriter._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkContext

case class BufferedFileWriter(path: String,
                              append: Boolean,
                              sparkContext: SparkContext) {
  private val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
  private val file = new Path(path)
  private val _progress = new WriterProgress
  private var partsDir: Path = _ // temp working dir for appending

  private val outStream = if (hdfs.exists(file) && append) {
    /** Hadoop doesn't support append on a ChecksumFilesystem.
      * Get around this limitation by writing to a temporary new file and
      * merging with the old file on .close() */
    partsDir = new Path(path + "-PARTS")
    hdfs.mkdirs(partsDir) // dir for new and old parts
    hdfs.rename(file, new Path(partsDir.toString + Path.SEPARATOR + "PART0.csv")) // move old part into dir
    hdfs.create(new Path(partsDir.toString + Path.SEPARATOR + "PART1.csv"), true, 4*1024, _progress)
  } else {
    hdfs.create(file, true, 4*1024, _progress)
  }

  private val writer = new BufferedWriter(new OutputStreamWriter(outStream, "UTF-8"))

  def close(): Unit = {
    writer.close()
    if (partsDir != null) {
      /** Need to merge new and old parts */
      FileUtil.copyMerge(hdfs, partsDir, hdfs, file,
        true, sparkContext.hadoopConfiguration, null)
      hdfs.delete(partsDir, true)
    }
    //hdfs.close()
  }

  def flush(): Unit = writer.flush()

  def newLine(): Unit = writer.newLine()

  def write(str: String): Unit = writer.write(str)

  def progress(): Unit = _progress.progress()
}

object BufferedFileWriter {
  class WriterProgress extends Progressable {
    override def progress(): Unit = {}
  }
}