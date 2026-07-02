package ai.chronon.api.submission

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

case class KubernetesToleration(key: String, operator: String, value: Option[String] = None, effect: String)

case class KubernetesPlacement(team: String,
                               mode: String,
                               nodePool: String,
                               nodeSelector: Map[String, String],
                               tolerations: Seq[KubernetesToleration]) {
  def labels: Map[String, String] =
    Map(
      KubernetesPlacement.TeamLabel -> team,
      KubernetesPlacement.ModeLabel -> mode,
      KubernetesPlacement.NodePoolLabel -> nodePool
    )

  def submissionProperties: Map[String, String] =
    Map(
      JobSubmitterConstants.ZiplineTeam -> team,
      JobSubmitterConstants.RunMode -> mode,
      JobSubmitterConstants.KubernetesNodeSelector -> KubernetesPlacement.encodeNodeSelector(nodeSelector),
      JobSubmitterConstants.KubernetesTolerations -> KubernetesPlacement.encodeTolerations(tolerations),
      JobSubmitterConstants.EksNodeSelector -> KubernetesPlacement.encodeNodeSelector(nodeSelector),
      JobSubmitterConstants.EksTolerations -> KubernetesPlacement.encodeTolerations(tolerations)
    )
}

object KubernetesPlacement {
  val TeamLabel = "zipline.ai/team"
  val ModeLabel = "zipline.ai/mode"
  val NodePoolLabel = "zipline.ai/node-pool"
  val DefaultTeam = "default"

  def forTeamMode(team: String, mode: String): KubernetesPlacement = {
    val normalizedTeam = normalizeLabelValue(Option(team).getOrElse(DefaultTeam))
    val normalizedMode = normalizeLabelValue(Option(mode).getOrElse("backfill"))
    val nodePool = normalizeLabelValue(s"$normalizedTeam-$normalizedMode")
    KubernetesPlacement(
      team = normalizedTeam,
      mode = normalizedMode,
      nodePool = nodePool,
      nodeSelector = Map(NodePoolLabel -> nodePool),
      tolerations = Seq(KubernetesToleration(NodePoolLabel, "Equal", Some(nodePool), "NoSchedule"))
    )
  }

  def encodeNodeSelector(selector: Map[String, String]): String =
    selector.toSeq
      .sortBy(_._1)
      .map { case (key, value) => s"$key=$value" }
      .mkString(",")

  def decodeNodeSelector(raw: String): Map[String, String] =
    Option(raw).map(_.trim).filter(_.nonEmpty).fold(Map.empty[String, String]) { value =>
      value
        .split(",")
        .iterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .map { pair =>
          val splitAt = pair.indexOf('=')
          if (splitAt <= 0) {
            throw new IllegalArgumentException(s"Malformed node selector pair: '$pair' (expected key=value)")
          }
          pair.substring(0, splitAt).trim -> pair.substring(splitAt + 1).trim
        }
        .filter { case (key, _) => key.nonEmpty }
        .toMap
    }

  def encodeTolerations(tolerations: Seq[KubernetesToleration]): String =
    tolerations
      .map { toleration =>
        Seq(
          "key" -> toleration.key,
          "operator" -> toleration.operator,
          "value" -> toleration.value.getOrElse(""),
          "effect" -> toleration.effect
        ).map { case (key, value) => s"$key=$value" }.mkString(",")
      }
      .mkString("|")

  def decodeTolerations(raw: String): Seq[KubernetesToleration] =
    Option(raw).map(_.trim).filter(_.nonEmpty).toSeq.flatMap { value =>
      value.split("\\|").toSeq.map(_.trim).filter(_.nonEmpty).map { item =>
        val fields = decodeNodeSelector(item)
        KubernetesToleration(
          key = required(fields, "key"),
          operator = fields.getOrElse("operator", "Equal"),
          value = fields.get("value").filter(_.nonEmpty),
          effect = required(fields, "effect")
        )
      }
    }

  def tolerationsAsStringMaps(tolerations: Seq[KubernetesToleration]): Seq[Map[String, String]] =
    tolerations.map { toleration =>
      Map("key" -> toleration.key, "operator" -> toleration.operator, "effect" -> toleration.effect) ++
        toleration.value.filter(_.nonEmpty).map("value" -> _)
    }

  private def required(fields: Map[String, String], key: String): String =
    fields
      .get(key)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Missing toleration field: $key"))

  private def normalizeLabelValue(raw: String): String = {
    val cleaned = raw.trim.toLowerCase(Locale.ROOT)
      .replaceAll("[^a-z0-9-]", "-")
      .replaceAll("-+", "-")
      .stripPrefix("-")
      .stripSuffix("-")
    val value = if (cleaned.nonEmpty) cleaned else DefaultTeam
    if (value.length <= 63) value
    else {
      val suffix = shortHash(value)
      value.take(63 - suffix.length - 1).stripSuffix("-") + "-" + suffix
    }
  }

  private def shortHash(value: String): String =
    MessageDigest
      .getInstance("SHA-1")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .take(4)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
}
