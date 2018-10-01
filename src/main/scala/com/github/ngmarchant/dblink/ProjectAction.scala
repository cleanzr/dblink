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

import com.github.ngmarchant.dblink.analysis.{ClusteringMetrics, PairwiseMetrics}

trait ProjectAction {
  def execute(): Unit

  def mkString: String
}

object ProjectAction {
  private val supportedSamplers = Set("PCG-I", "PCG-II", "Gibbs")
  private val supportedEvaluationMetrics = Set("pairwise", "cluster")

  class SampleAction(project: Project, sampleSize: Int, burninInterval: Int,
                     thinningInterval: Int, resume: Boolean, sampler: String) extends ProjectAction {
    require(sampleSize > 0, "sampleSize must be positive")
    require(burninInterval >= 0, "burninInterval must be non-negative")
    require(thinningInterval >= 0, "thinningInterval must be non-negative")
    require(supportedSamplers.contains(sampler), s"sampler must be one of ${supportedSamplers.mkString("", ",", "")}.")

    override def execute(): Unit = {
      val initialState = if (resume) {
        project.getSavedState.getOrElse(project.generateInitialState)
      } else {
        project.generateInitialState
      }
      sampler match {
        case "PCG-I" => Sampler.sample(initialState, sampleSize, project.projectPath, burninInterval=burninInterval, thinningInterval=thinningInterval, collapsedEntityIds = false, collapsedEntityValues = true)
        case "PCG-II" => Sampler.sample(initialState, sampleSize, project.projectPath, burninInterval=burninInterval, thinningInterval=thinningInterval, collapsedEntityIds = true, collapsedEntityValues = true)
        case "Gibbs" => Sampler.sample(initialState, sampleSize, project.projectPath, burninInterval=burninInterval, thinningInterval=thinningInterval, collapsedEntityIds = false, collapsedEntityValues = false)
      }
    }

    override def mkString: String = {
      if (resume) s"SampleAction: Evolving the chain from saved state with sampleSize=$sampleSize, burninInterval=$burninInterval, thinningInterval=$thinningInterval and sampler=$sampler"
      else s"SampleAction: Evolving the chain from new initial state with sampleSize=$sampleSize, burninInterval=$burninInterval, thinningInterval=$thinningInterval and sampler=$sampler"
    }
  }

  class EvaluateAction(project: Project, lowerIterationCutoff: Int, metrics: Traversable[String],
                       useExistingSMPC: Boolean) extends ProjectAction {
    require(project.entIdAttribute.isDefined, "Ground truth entity ids are required for evaluation")
    require(lowerIterationCutoff >=0, "lowerIterationCutoff must be non-negative")
    require(metrics.forall(m => supportedEvaluationMetrics.contains(m)), s"metrics must be one of ${supportedEvaluationMetrics.mkString("", ",", "")}.")

    override def execute(): Unit = {
      import com.github.ngmarchant.dblink.analysis.implicits._

      // Get ground truth clustering
      val trueClusters = project.getTrueClusters match {
        case Some(clusters) => clusters.persist()
        case None =>
          sys.error("Cannot complete evaluation as ground truth clusters are unavailable")
          return
      }

      // Get predicted clustering (using sMPC method)
      val sMPC = if (useExistingSMPC && project.sharedMostProbableClustersOnDisk) {
        // Read saved sMPC from disk
        project.getSavedSharedMostProbableClusters
      } else {
        // Try to compute sMPC using saved linkage chain (and save to disk)
        project.getSavedLinkageChain(lowerIterationCutoff) match {
          case Some(chain) =>
            val sMPC = chain.sharedMostProbableClusters.persist()
            sMPC.saveCsv(project.projectPath + "sharedMostProbableClusters.csv")
            chain.unpersist()
            Some(sMPC)
          case None =>
            sys.error("No linkage chain")
            None
        }
      }

      sMPC match {
        case Some(predictedClusters) =>
          metrics.foreach {
            case metric if metric == "pairwise" =>
              println(PairwiseMetrics(predictedClusters.toPairwiseLinks, trueClusters.toPairwiseLinks).mkString)
            case metric if metric == "cluster" =>
              println(ClusteringMetrics(predictedClusters, trueClusters).mkString)
            case metric => sys.error(s"Skipping unknown metric '$metric'")
          }
        case None => sys.error("Cannot complete evaluation as predicted clusters are unavailable")
      }
    }

    override def mkString: String = {
      if (useExistingSMPC) s"EvaluateAction: Evaluating saved sMPC clusters using ${metrics.map("'" + _ + "'").mkString("[", ",", "]")} metrics"
      else s"EvaluateAction: Evaluating sMPC clusters (computed from the chain for iterations >= $lowerIterationCutoff) using ${metrics.map("'" + _ + "'").mkString("[", ",", "]")} metrics"
    }
  }

  class SummarizeAction(project: Project, lowerIterationCutoff: Int,
                        quantities: Traversable[String]) extends ProjectAction {
    require(lowerIterationCutoff >= 0, "lowerIterationCutoff must be non-negative")

    override def execute(): Unit = {
      import com.github.ngmarchant.dblink.analysis.implicits._
      project.getSavedLinkageChain(lowerIterationCutoff) match {
        case Some(chain) =>
          chain.persist()
          quantities.foreach {
            case quantity if quantity == "cluster-size-distribution" => chain.clusterSizeDistribution(project.projectPath)
            case quantity => sys.error(s"skipping unknown quantity '$quantity'")
          }
          chain.unpersist()
        case None => sys.error("no linkage chain")
      }
    }

    override def mkString: String = {
      s"SummarizeAction: Calculating summary quantities ${quantities.map("'" + _ + "'").mkString("[", ",", "]")} along the chain for iterations >= $lowerIterationCutoff"
    }
  }
}