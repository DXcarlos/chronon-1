package ai.chronon.spark.local

import ai.chronon.api.Extensions.{MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api._
import py4j.GatewayServer

import java.io.File
import java.util.{List => JavaList}
import scala.collection.immutable.{Map, Set, _}
import scala.collection.mutable
import java.nio.file.{Files, Path, Paths}
import scala.collection.parallel.{ParMap, ParSeq}
import scala.jdk.CollectionConverters._

class ConfIndex(rootDir: String) {

  val tablesToJoin: ParMap[String, Join] = getTablesToJoin
  val tablesToGroupBy: ParMap[String, GroupBy] = getTablesToGroupBy
  val tablesToStagingQuery: ParMap[String, StagingQuery] = getTablesToStagingQuery

  private def listFiles(dir: String): ParSeq[String] = {
    val path = Paths.get(dir)
    Files
      .walk(path)
      .iterator()
      .asScala
      .toList
      .map(_.toAbsolutePath.toString)
      .filterNot(isIgnorableFile)
      .par
  }

  private def isIgnorableFile(path: String): Boolean = {
    val file = new File(path)

    // Skip directories - we only want regular files
    if (file.isDirectory) return true

    // Skip files that don't end with expected extensions (only process JSON-like files)
    if (!file.getName.contains(".")) return true

    val extensions = Constants.extensionsToIgnore.toSeq
    val folders = Constants.foldersToIgnore.toSeq
    val pathParts = file.getPath.split("/").toSeq

    extensions.exists(file.getName.endsWith(_)) || folders.exists(pathParts.contains(_))
  }

  private def getTablesToStagingQuery: ParMap[String, StagingQuery] = {
    listFiles(rootDir + "/staging_queries")
      .map(file => ThriftJsonCodec.fromJsonFile[StagingQuery](file, check = false))
      .map(sq => sq.metaData.outputTable -> sq)
      .toMap
  }

  private def getTablesToJoin: ParMap[String, Join] = {
    listFiles(rootDir + "/joins")
      .map(file => ThriftJsonCodec.fromJsonFile[Join](file, check = false))
      .map(join => join.metaData.outputTable -> join)
      .toMap
  }

  private def getTablesToGroupBy: ParMap[String, GroupBy] = {
    listFiles(rootDir + "/group_bys")
      .map(file => ThriftJsonCodec.fromJsonFile[GroupBy](file, check = false))
      .map(gb => gb.metaData.outputTable -> gb)
      .toMap
  }
}
