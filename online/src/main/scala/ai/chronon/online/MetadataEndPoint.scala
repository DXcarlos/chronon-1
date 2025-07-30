package ai.chronon.online

import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

case class MetadataEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](
    extractFn: (String, Conf) => (String, String),
    name: String
)
object MetadataEndPoint {
  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val ConfByKeyEndPointName = "CHRONON_METADATA"

  // key: entity path, e.g. joins/team/team.example_join.v1
  // value: entity config in json format
  private def confByKeyEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (metadataName, conf) => (metadataName, ThriftJsonCodec.toJsonStr(conf)),
      name = ConfByKeyEndPointName
    )

  def getEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](endPointName: String): MetadataEndPoint[Conf] = {
    endPointName match {
      case ConfByKeyEndPointName => confByKeyEndPoint[Conf]
      case _ =>
        logger.error(s"Failed to find endpoint for $endPointName")
        throw new Exception(s"Failed to find endpoint for $endPointName")
    }
  }
}
