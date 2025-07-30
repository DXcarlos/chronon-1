package ai.chronon.api.planner

import ai.chronon.api.thrift.TBase
import ai.chronon.api.{Constants, GroupBy, Join, PartitionSpec, StagingQuery, ThriftJsonCodec}
import ai.chronon.planner.ConfPlan

import java.io.File
import scala.reflect.ClassTag

object LocalRunner {

  private def listFiles(dir: String = "."): Seq[String] = {
    val baseDir = new File(dir)
    Option(baseDir.listFiles).getOrElse(Array()).flatMap { file =>
      if (file.isDirectory) listFiles(file.getPath)
      else Seq(file.getPath.replaceFirst("^\\./", ""))
    }
  }

  private def isIgnorableFile(path: String): Boolean = {
    val file = new File(path)
    Constants.extensionsToIgnore.exists(file.getName.endsWith) ||
    Constants.foldersToIgnore.exists(file.getPath.split("/").contains(_))
  }

  def parseConfs[T <: TBase[_, _]: Manifest: ClassTag](confSubfolder: String): Seq[T] = listFiles(confSubfolder)
    .filterNot(isIgnorableFile)
    .map(ThriftJsonCodec.fromJsonFile(_, check = true))

  def processConfigurations(confSubfolder: String, confType: String)(implicit
      partitionSpec: PartitionSpec): Seq[ConfPlan] = {
    confType match {
      case "joins" => {
        val confs = parseConfs[Join](confSubfolder)
        confs.map((c) => MonolithJoinPlanner(c)).map(_.buildPlan)
      }
      case "staging_queries" => {
        val confs = parseConfs[StagingQuery](confSubfolder)
        confs.map((c) => new StagingQueryPlanner(c)).map(_.buildPlan)
      }
      case "groupbys" => {
        val confs = parseConfs[GroupBy](confSubfolder)
        confs.map((c) => new GroupByPlanner(c)).map(_.buildPlan)
      }
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported conf type: $confType. Supported types are: joins, staging_queries, groupbys."
        )
    }
  }

  /** To run:
    * bazel build //api:planner_deploy.jar
    * bazel run -- //api:planner <path-to-confs> <conf_type>
    * @param args
    */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: LocalRunner <path_to_conf_subfolder> <conf_type>")
      System.exit(1)
    }

    val confSubfolder = args(0)
    val confType = args(1)
    implicit val testPartitionSpec = PartitionSpec.daily

    println("Parsed configurations:")
    val plans = processConfigurations(confSubfolder, confType)
    plans.foreach(println)
  }

}
