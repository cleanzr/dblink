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

import com.github.ngmarchant.dblink.Run
import org.apache.spark.{SparkConf, SparkContext}

object Launch {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("dblink")
    val sc = SparkContext.getOrCreate(conf)
    Run.main(args)
  }
}