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

package com.github.cleanzr.dblink.analysis

import java.io._

import com.github.cleanzr.dblink.RecordId
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class PairwiseLinks(private var links: RDD[(RecordId, RecordId)]) {
  /** Ensure links are ordered and distinct */
  links = links.map { recIds =>
    // Ensure pairs are sorted
    recIds._1.compareTo(recIds._2) match {
      case x if x < 0 => (recIds._1, recIds._2)
      case x if x > 0 => (recIds._2, recIds._1)
      case 0 => throw new Exception(s"Invalid link: ${recIds._1} <-> ${recIds._2}.")
    }
  }.distinct()

  def rdd: RDD[(RecordId, RecordId)] = links

  def saveCsv(path: String): Unit = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    rdd.collect().foreach(pair => bw.write(s"${pair._1}, ${pair._2}\n"))
    bw.close()
  }
}

object PairwiseLinks {
  def readCSV(path: String): PairwiseLinks = {
    val file = new File(path)
    val br = new BufferedReader(new FileReader(file))

    val itLines = br.lines().iterator.asScala

    val pairs = itLines.map { line =>
      val x = line.split(",")
      (x(0), x(1))
    }.toSeq

    val sc = SparkContext.getOrCreate()
    new PairwiseLinks(sc.makeRDD(pairs))
  }
}