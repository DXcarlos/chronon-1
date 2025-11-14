package ai.chronon.online

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import ai.chronon.api.Constants._
import ai.chronon.api.Extensions._
import com.google.gson.Gson
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.io.FileReader
import java.nio.file.Files
import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.util.Try

class MetadataDirWalker(dirPath: String, metadataEndPointNames: List[String], maybeConfType: Option[String] = None) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private def loadJsonToConf[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[T] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = false)
      Some(configConf)
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to parse compiled Chronon config file: $file, \nerror=${e.getMessage}")
        None
    }
  }
  private def parseName(path: String): Option[String] = {
    val gson = new Gson()
    val reader = Files.newBufferedReader(Paths.get(path))
    try {
      val map = gson.fromJson(reader, classOf[java.util.Map[String, AnyRef]])
      Option(map.get("metaData"))
        .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
        .map(_.get("name"))
        .flatMap(Option(_))
        .map(_.asInstanceOf[String])
    } catch {
      case ex: Throwable =>
        logger.error(s"Failed to parse Chronon config file at $path as JSON", ex)
        ex.printStackTrace()
        None
    }
  }

  lazy val fileList: Seq[File] = {
    val configFile = new File(dirPath)
    assert(configFile.exists(), s"$configFile does not exist")
    logger.info(s"Uploading Chronon configs from $dirPath")
    MetadataDirWalker.listFiles(configFile).getValidFilesAndReport
  }

  lazy val nonEmptyFileList: Seq[File] = {
    fileList
      .filter { file =>
        val name = parseName(file.getPath)
        if (name.isEmpty) logger.info(s"Skipping invalid file ${file.getPath}")
        name.isDefined
      }
  }

  /** Iterate over the list of files and extract the key value pairs for each file
    * @return Map of endpoint -> (Map of key -> List of values)
    *         e.g. (
    *            CHRONON_METADATA_BY_TEAM -> (team -> List("join1", "join2")),
    *            CHRONON_METADATA -> (teams/joins/join1 -> config1)
    *         )
    */
  def run: Map[String, Map[String, List[String]]] = {
    val fileList = nonEmptyFileList

    fileList.foldLeft(Map.empty[String, Map[String, List[String]]]) { (acc, file) =>
      // For each end point we apply the extractFn to the file path to extract the key value pair
      val filePath = file.getPath
      require(filePath.startsWith(dirPath),
              s"Returned file path $filePath doesn't belong to metadata directory $dirPath")
      val relativePath = filePath.drop(dirPath.length)

      val (optConf, confKeyName) =
        try {
          relativePath match {
            case value if value.contains(s"$JoinFolder/") || maybeConfType.contains(JoinFolder) =>
              val conf = loadJsonToConf[api.Join](filePath)
              (conf, conf.map(_.keyNameForKvStore))
            case value if value.contains(s"$GroupByFolder/") || maybeConfType.contains(GroupByFolder) =>
              val conf = loadJsonToConf[api.GroupBy](filePath)
              (conf, conf.map(a => a.keyNameForKvStore))
            case value if value.contains(s"$StagingQueryFolder/") || maybeConfType.contains(StagingQueryFolder) =>
              val conf = loadJsonToConf[api.StagingQuery](filePath)
              (conf, conf.map(_.keyNameForKvStore))
            case value if value.contains(s"$ModelFolder/") || maybeConfType.contains(ModelFolder) =>
              val conf = loadJsonToConf[api.ModelTransforms](filePath)
              (conf, conf.map(_.keyNameForKvStore))
          }
        } catch {
          case e: Throwable =>
            logger.error(s"Failed to parse compiled team from file path: $filePath, \nerror=${e.getMessage}")
            (None, None)
        }

      if (optConf.isDefined && confKeyName.isDefined) {
        val kvPairToEndPoint: List[(String, (String, String))] = metadataEndPointNames
          .map { endPointName =>
            val conf = optConf.get

            val kVPair = filePath match {
              case value if value.contains(s"$JoinFolder/") || maybeConfType.contains(JoinFolder) =>
                MetadataEndPoint
                  .getEndPoint[api.Join](endPointName)
                  .extractFn(confKeyName.get, conf.asInstanceOf[api.Join])

              case value if value.contains(s"$GroupByFolder/") || maybeConfType.contains(GroupByFolder) =>
                MetadataEndPoint
                  .getEndPoint[api.GroupBy](endPointName)
                  .extractFn(confKeyName.get, conf.asInstanceOf[api.GroupBy])

              case value if value.contains(s"$StagingQueryFolder/") || maybeConfType.contains(StagingQueryFolder) =>
                MetadataEndPoint
                  .getEndPoint[api.StagingQuery](endPointName)
                  .extractFn(confKeyName.get, conf.asInstanceOf[api.StagingQuery])

              case value if value.contains(s"$ModelFolder/") || maybeConfType.contains(ModelFolder) =>
                MetadataEndPoint
                  .getEndPoint[api.ModelTransforms](endPointName)
                  .extractFn(confKeyName.get, conf.asInstanceOf[api.ModelTransforms])
            }

            (endPointName, kVPair)
          }

        kvPairToEndPoint
          .map(kvPair => {
            val endPoint = kvPair._1
            val (key, value) = kvPair._2
            val map = acc.getOrElse(endPoint, Map.empty[String, List[String]])
            val list = map.getOrElse(key, List.empty[String]) ++ List(value)
            (endPoint, map.updated(key, list))
          })
          .toMap
      } else {
        logger.info(s"Skipping invalid file ${file.getPath}")
        acc
      }
    }
  }
}

object MetadataDirWalker {
  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  case class FileList(fileList: Seq[File] = Seq.empty, ignored: Seq[File] = Seq.empty) {
    def ++(other: FileList): FileList = FileList(fileList ++ other.fileList, ignored ++ other.ignored)

    def getValidFilesAndReport: Seq[File] = {
      if (ignored.nonEmpty)
        logger.debug(
          s"Skipping invalid files with invalid extensions. Skipping..:\n  ${ignored.map(relativePath).mkString("\n  ")}")

      fileList
    }
  }

  def relativePath(file: File): String = {
    val currentDir = Paths.get("")
    currentDir.toAbsolutePath.relativize(file.toPath).toString
  }

  def listFiles(base: File, recursive: Boolean = true): FileList = {

    if (base.isFile) return FileList(Array(base))

    val (folders, files) = base.listFiles.partition(_.isDirectory)

    val (invalidPaths, remainingFiles) = files.partition { file =>
      Constants.extensionsToIgnore.exists(file.getName.endsWith) ||
      Constants.foldersToIgnore.exists(file.getPath.split("/").contains(_))
    }

    val (validFiles, unParseableFiles) = remainingFiles.partition { parseMetadataName(_).isSuccess }

    val filesHere = FileList(validFiles, invalidPaths ++ unParseableFiles)

    val nestedFiles: FileList =
      if (recursive)
        folders.map(listFiles(_, recursive)).reduceOption(_ ++ _).getOrElse(FileList())
      else
        FileList()

    filesHere ++ nestedFiles

  }

  private def parseMetadataName(file: File): Try[String] =
    Try {
      val gson = new Gson()
      val reader = new FileReader(file)
      val map = gson.fromJson(reader, classOf[java.util.Map[String, AnyRef]])
      val result = map
        .get("metaData")
        .asInstanceOf[java.util.Map[String, AnyRef]]
        .get("name")
        .asInstanceOf[String]

      reader.close()
      result
    }

  def parse[T <: TBase[_, _]: Manifest: ClassTag](file: File): Try[T] =
    Try {
      ThriftJsonCodec.fromJsonFile[T](file, check = false)
    }
}
