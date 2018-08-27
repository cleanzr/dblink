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

import ProjectAction.{EvaluateAction, SampleAction, SummarizeAction}
import com.typesafe.config.{Config, ConfigException}
import Project.toConfigTraversable

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class ProjectActions(config: Config, project: Project) {

  val actions: Traversable[ProjectAction] = ProjectActions.parseActions(config, project)

  def execute(): Unit = {
    actions.foreach(_.execute())
  }

  def mkString: String = {
    val lines = mutable.ArrayBuffer.empty[String]
    lines += "Scheduled actions"
    lines += "-----------------"
    lines ++= actions.map(action => "  * " +action.mkString)

    lines.mkString("\n")
  }
}

object ProjectActions {
  def apply(config: Config, project: Project): ProjectActions = {
    new ProjectActions(config, project)
  }

  private def parseActions(config: Config, project: Project): Traversable[ProjectAction] = {
    config.getObjectList("dblink.actions").toTraversable.map { action =>
      action.getString("name") match {
        case "sample" =>
          new SampleAction(project,
            sampleSize = action.getInt("parameters.sampleSize"),
            burninInterval = action.getInt("parameters.burninInterval"),
            thinningInterval = action.getInt("parameters.thinningInterval"),
            resume = action.getBoolean("parameters.resume"))
        case "evaluate" =>
          new EvaluateAction(project,
            lowerIterationCutoff = Try {action.getInt("parameters.lowerIterationCutoff")} getOrElse 0,
            metrics = action.getStringList("parameters.metrics").asScala,
            useExistingSMPC = Try {action.getBoolean("parameters.useExistingSMPC")} getOrElse false
          )
        case "summarize" =>
          new SummarizeAction(project,
            lowerIterationCutoff = Try {action.getInt("parameters.lowerIterationCutoff")} getOrElse 0,
            quantities = action.getStringList("parameters.quantities").asScala
          )
        case _ => throw new ConfigException.BadValue(config.origin(), "name", "unsupported action")
      }
    }
  }
}