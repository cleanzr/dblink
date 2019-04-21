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

import ProjectStep.{CopyFilesStep, EvaluateStep, SampleStep, SummarizeStep}
import com.typesafe.config.{Config, ConfigException}
import Project.toConfigTraversable

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class ProjectSteps(config: Config, project: Project) {

  val steps: Traversable[ProjectStep] = ProjectSteps.parseSteps(config, project)

  def execute(): Unit = {
    steps.foreach(_.execute())
  }

  def mkString: String = {
    val lines = mutable.ArrayBuffer.empty[String]
    lines += "Scheduled steps"
    lines += "---------------"
    lines ++= steps.map(step => "  * " + step.mkString)

    lines.mkString("\n")
  }
}

object ProjectSteps {
  def apply(config: Config, project: Project): ProjectSteps = {
    new ProjectSteps(config, project)
  }

  private def parseSteps(config: Config, project: Project): Traversable[ProjectStep] = {
    config.getObjectList("dblink.steps").toTraversable.map { step =>
      step.getString("name") match {
        case "sample" =>
          new SampleStep(project,
            sampleSize = step.getInt("parameters.sampleSize"),
            burninInterval = Try {step.getInt("parameters.burninInterval")} getOrElse 0,
            thinningInterval = Try {step.getInt("parameters.thinningInterval")} getOrElse 1,
            resume = Try {step.getBoolean("parameters.resume")} getOrElse true,
            sampler = Try {step.getString("parameters.sampler")} getOrElse "PCG-I")
        case "evaluate" =>
          new EvaluateStep(project,
            lowerIterationCutoff = Try {step.getInt("parameters.lowerIterationCutoff")} getOrElse 0,
            metrics = step.getStringList("parameters.metrics").asScala,
            useExistingSMPC = Try {step.getBoolean("parameters.useExistingSMPC")} getOrElse false
          )
        case "summarize" =>
          new SummarizeStep(project,
            lowerIterationCutoff = Try {step.getInt("parameters.lowerIterationCutoff")} getOrElse 0,
            quantities = step.getStringList("parameters.quantities").asScala
          )
        case "copy-files" =>
          new CopyFilesStep(project,
            fileNames = step.getStringList("parameters.fileNames").asScala,
            destinationPath = step.getString("parameters.destinationPath"),
            overwrite = Try {step.getBoolean("parameters.overwrite")} getOrElse false,
            deleteSource = Try {step.getBoolean("parameters.deleteSource")} getOrElse false
          )
        case _ => throw new ConfigException.BadValue(config.origin(), "name", "unsupported step")
      }
    }
  }
}