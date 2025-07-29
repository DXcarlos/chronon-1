package ai.chronon.api.runner

import ai.chronon.api.{Constants, GroupBy, Join, PartitionSpec, StagingQuery, ThriftJsonCodec}
import ai.chronon.api.planner.{GroupByPlanner, MonolithJoinPlanner, StagingQueryPlanner}
import ai.chronon.api.thrift.TBase
import ai.chronon.orchestration.ConfType

import java.io.File
import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag

class Builder {

  private def listFiles(dir: String = "."): ParArray[String] = {
    val baseDir = new File(dir)
    Option(baseDir.listFiles).getOrElse(Array()).flatMap { file =>
      if (file.isDirectory) listFiles(file.getPath)
      else Seq(file.getPath.replaceFirst("^\\./", ""))
    }.filterNot(isIgnorableFile).par
  }

  private def isIgnorableFile(path: String): Boolean = {
    val file = new File(path)
    Constants.extensionsToIgnore.exists(file.getName.endsWith) ||
      Constants.foldersToIgnore.exists(file.getPath.split("/").contains(_))
  }


  def buildIndex(compiledDir: String)(implicit partitionSpec: PartitionSpec): Index = {

    val joinEntries = listFiles(compiledDir + "/joins").map { file =>
      ConfEntry[Join](file, ConfType.JOIN, { join: Join => join.metaData }, MonolithJoinPlanner.apply)
    }

    val stagingEntries = listFiles(compiledDir + "/staging_queries").map { file =>
      ConfEntry[StagingQuery](file, ConfType.STAGING_QUERY, { sq: StagingQuery => sq.metaData }, StagingQueryPlanner.apply)
    }

    val groupByEntries = listFiles(compiledDir + "/group_bys").map { file =>
      ConfEntry[GroupBy](file, ConfType.GROUP_BY, { gb: GroupBy => gb.metaData }, GroupByPlanner.apply)
    }

    val confEntries: Array[ConfEntry] = (stagingEntries ++ groupByEntries ++ joinEntries).toArray

    val nodeEntries = confEntries
      .flatMap(_.nodes)
      .map(node => NodeEntry(node.metaData.name, ThriftJsonCodec.hexDigest(node), node))

    Index(confEntries, nodeEntries)
  }

}
