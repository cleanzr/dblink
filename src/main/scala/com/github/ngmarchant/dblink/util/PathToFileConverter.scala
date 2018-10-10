package com.github.ngmarchant.dblink.util

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

object PathToFileConverter {

  def fileToPath(path: Path, conf: Configuration): File = {
    val fs = FileSystem.get(path.toUri, conf)
    val tempFile = File.createTempFile(path.getName, "")
    tempFile.deleteOnExit()
    fs.copyToLocalFile(path, new Path(tempFile.getAbsolutePath))
    tempFile
  }
}
