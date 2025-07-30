package ai.chronon.api
import scala.util.parsing.combinator._

abstract class DataPointer {
  def tableOrPath: String
  def readFormat: Option[String]
  def writeFormat: Option[String]

  def readOptions: Map[String, String]
  def writeOptions: Map[String, String]

}

case class URIDataPointer(
    override val tableOrPath: String,
    override val readFormat: Option[String],
    override val writeFormat: Option[String],
    options: Map[String, String]
) extends DataPointer {

  override val readOptions: Map[String, String] = options
  override val writeOptions: Map[String, String] = options
}

// parses string representations of data pointers
// ex: namespace.table
// ex: s3://bucket/path/to/data.parquet
// ex: bigquery(option1=value1,option2=value2)://project-id.dataset.table
// ex: kafka://my-topic
// The code below is based on parser combinators - one can think of those as composable a regex parser.
object DataPointer extends RegexParsers {
  def apply(str: String): DataPointer = {
    parse(dataPointer, str) match {
      case Success(result, _) => result
      case Failure(msg, next) =>
        throw new IllegalArgumentException(s"Invalid DataPointer string at position ${next.pos}: $str. Error: $msg")
      case Error(msg, next) =>
        throw new IllegalArgumentException(s"Invalid DataPointer string at position ${next.pos}: $str. Error: $msg")
    }
  }

  private def dataPointer: Parser[DataPointer] =
    opt(catalogWithOptionalFormat ~ opt(options) ~ "://") ~ tableOrPath ^^ {
      // format is specified in the prefix s3+parquet://bucket/path/to/data/*/*/
      // note that if you have s3+parquet://bucket/path/to/data.csv, format is still parquet
      case Some((ctl, Some(fmt)) ~ opts ~ sep) ~ path =>
        URIDataPointer(ctl + sep + path, Some(fmt), Some(fmt), opts.getOrElse(Map.empty))

      // format is extracted from the path for relevant sources
      // ex: s3://bucket/path/to/data.parquet
      // ex: file://path/to/data.csv
      // ex: hdfs://path/to/data.with.dots.parquet
      // for other sources like bigquery, snowflake, format is None
      case Some((ctl, None) ~ opts ~ sep) ~ path =>
        val (_, fmt) = extractFormatFromPath(path, ctl)

        fmt match {
          // Retain the full uri if it's a path.
          case Some(ft) => URIDataPointer(ctl + sep + path, Some(ft), Some(ft), opts.getOrElse(Map.empty))
          case None     => URIDataPointer(path, Some(ctl), Some(ctl), opts.getOrElse(Map.empty))
        }

      case None ~ path =>
        // No prefix case (direct table reference)
        URIDataPointer(path, None, None, Map.empty)
    }

  private def catalogWithOptionalFormat: Parser[(String, Option[String])] =
    """[a-zA-Z0-9]+""".r ~ opt("+" ~> """[a-zA-Z0-9]+""".r) ^^ { case catalog ~ format =>
      (catalog, format)
    }

  private def options: Parser[Map[String, String]] = "(" ~> repsep(option, ",") <~ ")" ^^ (_.toMap)

  private def option: Parser[(String, String)] =
    ("""[^=,]+""".r <~ "=") ~ """[^,)]+""".r ^^ { case key ~ value =>
      (key.trim, value.trim)
    }

  private def tableOrPath: Parser[String] = """[^:]+""".r

  private def extractFormatFromPath(
      path: String,
      catalog: String,
      fileCatalogs: Seq[String] = Seq("s3", "gcs", "hdfs", "file")): (String, Option[String]) = {
    catalog.toLowerCase match {
      // direct file case - extract string after the last dot as format
      case ctl if fileCatalogs.contains(ctl) =>
        val parts = path.split("\\.")
        if (parts.length > 1 && !parts.last.contains("/")) {
          (parts.init.mkString("."), Some(parts.last))
        } else {
          (path, None)
        }

      // catalog table case - no format
      case _ => (path, None)
    }
  }
}
