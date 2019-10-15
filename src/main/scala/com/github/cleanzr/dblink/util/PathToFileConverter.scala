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

package com.github.cleanzr.dblink.util

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
