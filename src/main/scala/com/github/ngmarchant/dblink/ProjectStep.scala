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
import com.github.ngmarchant.dblink.util.BufferedFileWriter
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

trait ProjectStep {
  def execute(): Unit

  def mkString: String
}

object ProjectStep {
  private val supportedSamplers = Set("PCG-I", "PCG-II", "Gibbs")
  private val supportedEvaluationMetrics = Set("pairwise", "cluster")
  private val supportedSummaryQuantities = Set("cluster-size-distribution", "partition-sizes")

  class SampleStep(project: Project, sampleSize: Int, burninInterval: Int,
                     thinningInterval: Int, resume: Boolean, sampler: String) extends ProjectStep with Logging {
    require(sampleSize > 0, "sampleSize must be positive")
    require(burninInterval >= 0, "burninInterval must be non-negative")
    require(thinningInterval >= 0, "thinningInterval must be non-negative")
    require(supportedSamplers.contains(sampler), s"sampler must be one of ${supportedSamplers.mkString("", ", ", "")}.")

    override def execute(): Unit = {
      info(mkString)
      val initialState = if (resume) {
        project.getSavedState.getOrElse(project.generateInitialState)
      } else {
        project.generateInitialState
      }
      sampler match {
        case "PCG-I" => Sampler.sample(initialState, sampleSize, project.outputPath, burninInterval=burninInterval, thinningInterval=thinningInterval, collapsedEntityIds = false, collapsedEntityValues = true)
        case "PCG-II" => Sampler.sample(initialState, sampleSize, project.outputPath, burninInterval=burninInterval, thinningInterval=thinningInterval, collapsedEntityIds = true, collapsedEntityValues = true)
        case "Gibbs" => Sampler.sample(initialState, sampleSize, project.outputPath, burninInterval=burninInterval, thinningInterval=thinningInterval, collapsedEntityIds = false, collapsedEntityValues = false)
      }
    }

    override def mkString: String = {
      if (resume) s"SampleStep: Evolving the chain from saved state with sampleSize=$sampleSize, burninInterval=$burninInterval, thinningInterval=$thinningInterval and sampler=$sampler"
      else s"SampleStep: Evolving the chain from new initial state with sampleSize=$sampleSize, burninInterval=$burninInterval, thinningInterval=$thinningInterval and sampler=$sampler"
    }
  }

  class EvaluateStep(project: Project, lowerIterationCutoff: Int, metrics: Traversable[String],
                     useExistingSMPC: Boolean) extends ProjectStep with Logging {
    require(project.entIdAttribute.isDefined, "Ground truth entity ids are required for evaluation")
    require(lowerIterationCutoff >=0, "lowerIterationCutoff must be non-negative")
    require(metrics.nonEmpty, "metrics must be non-empty")
    require(metrics.forall(m => supportedEvaluationMetrics.contains(m)), s"metrics must be one of ${supportedEvaluationMetrics.mkString("{", ", ", "}")}.")

    override def execute(): Unit = {
      import com.github.ngmarchant.dblink.analysis.implicits._
      info(mkString)

      // Get ground truth clustering
      val trueClusters = project.getTrueClusters match {
        case Some(clusters) => clusters.persist()
        case None =>
          error("Ground truth clusters are unavailable")
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
            sMPC.saveCsv(project.outputPath + "shared-most-probable-clusters.csv")
            chain.unpersist()
            Some(sMPC)
          case None =>
            error("No linkage chain")
            None
        }
      }

      sMPC match {
        case Some(predictedClusters) =>
          val results = metrics.map {
            case metric if metric == "pairwise" =>
              PairwiseMetrics(predictedClusters.toPairwiseLinks, trueClusters.toPairwiseLinks).mkString
            case metric if metric == "cluster" =>
              ClusteringMetrics(predictedClusters, trueClusters).mkString
          }
          val writer = BufferedFileWriter(project.outputPath + "evaluation-results.txt", append = false, project.sparkContext)
          writer.write(results.mkString("", "\n", "\n"))
          writer.close()
        case None => error("Predicted clusters are unavailable")
      }
    }

    override def mkString: String = {
      if (useExistingSMPC) s"EvaluateStep: Evaluating saved sMPC clusters using ${metrics.map("'" + _ + "'").mkString("{", ", ", "}")} metrics"
      else s"EvaluateStep: Evaluating sMPC clusters (computed from the chain for iterations >= $lowerIterationCutoff) using ${metrics.map("'" + _ + "'").mkString("{", ", ", "}")} metrics"
    }
  }

  class SummarizeStep(project: Project, lowerIterationCutoff: Int,
                      quantities: Traversable[String]) extends ProjectStep with Logging {
    require(lowerIterationCutoff >= 0, "lowerIterationCutoff must be non-negative")
    require(quantities.nonEmpty, "quantities must be non-empty")
    require(quantities.forall(q => supportedSummaryQuantities.contains(q)), s"quantities must be one of ${supportedSummaryQuantities.mkString("{", ", ", "}")}.")

    override def execute(): Unit = {
      info(mkString)

      import com.github.ngmarchant.dblink.analysis.implicits._
      project.getSavedLinkageChain(lowerIterationCutoff) match {
        case Some(chain) =>
          chain.persist()
          quantities.foreach {
            case "cluster-size-distribution" => chain.saveClusterSizeDistribution(project.outputPath)
            case "partition-sizes" => chain.savePartitionSizes(project.outputPath)
          }
          chain.unpersist()
        case None => error("No linkage chain")
      }
    }

    override def mkString: String = {
      s"SummarizeStep: Calculating summary quantities ${quantities.map("'" + _ + "'").mkString("{", ", ", "}")} along the chain for iterations >= $lowerIterationCutoff"
    }
  }

  class CopyFilesStep(project: Project, fileNames: Traversable[String], destinationPath: String,
                      overwrite: Boolean, deleteSource: Boolean) extends ProjectStep with Logging {

    override def execute(): Unit = {
      info(mkString)

      val conf = project.sparkContext.hadoopConfiguration
      val srcParent = new Path(project.outputPath)
      val srcFs = FileSystem.get(srcParent.toUri, conf)
      val dstParent = new Path(destinationPath)
      val dstFs = FileSystem.get(dstParent.toUri, conf)
      fileNames.map(fName => new Path(srcParent.toString + Path.SEPARATOR + fName))
        .filter(srcFs.exists)
        .foreach { src =>
          val dst = new Path(dstParent.toString + Path.SEPARATOR + src.getName)
          FileUtil.copy(srcFs, src, dstFs, dst, deleteSource, overwrite, conf)
        }
    }

    override def mkString: String = {
      s"CopyFilesStep: Copying ${fileNames.mkString("{",", ","}")} to destination $destinationPath"
    }
  }
}