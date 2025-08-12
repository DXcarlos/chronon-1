package ai.chronon.spark.local

import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.orchestration.{BaseEvalResult, CheckResult, GroupByEvalResult, JoinEvalResult}
import org.apache.spark.sql.types.StructType
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object RenderUtils {

  def getUnifiedJoinErrorString(joinEvalResult: JoinEvalResult): String = {
    val errors = mutable.ListBuffer[String]()

    // Check left expression errors
    Option(joinEvalResult.getLeftExpressionCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- left: ${check.getMessage}"
      }
    }

    // Check left timestamp errors
    Option(joinEvalResult.getLeftTimestampCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- left_timestamp: ${check.getMessage}"
      }
    }

    // Check join part errors
    Option(joinEvalResult.getJoinPartChecks).foreach { joinParts =>
      joinParts.toScala.foreach { joinPart =>
        val partName = Option(joinPart.getPartName).getOrElse("unknown_part")

        // Check key schema errors
        Option(joinPart.getKeySchemaCheck).foreach { check =>
          if (check.getCheckResult == CheckResult.FAILURE) {
            errors += s"- $partName: ${check.getMessage}"
          }
        }

        // Check GroupBy eval result errors
        Option(joinPart.getGbEvalResult).foreach { gbResult =>
          Option(gbResult.getSourceExpressionCheck).foreach { check =>
            if (check.getCheckResult == CheckResult.FAILURE) {
              errors += s"- ${partName}_source: ${check.getMessage}"
            }
          }

          Option(gbResult.getSourceTimestampCheck).foreach { check =>
            if (check.getCheckResult == CheckResult.FAILURE) {
              errors += s"- ${partName}_timestamp: ${check.getMessage}"
            }
          }

          Option(gbResult.getAggExpressionCheck).foreach { check =>
            if (check.getCheckResult == CheckResult.FAILURE) {
              errors += s"- ${partName}_aggregation: ${check.getMessage}"
            }
          }

          Option(gbResult.getDerivationsExpressionCheck).foreach { check =>
            if (check.getCheckResult == CheckResult.FAILURE) {
              errors += s"- ${partName}_derivations: ${check.getMessage}"
            }
          }
        }
      }
    }

    // Check derivation validity errors
    Option(joinEvalResult.getDerivationValidityCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- derivation: ${check.getMessage}"
      }
    }

    errors.mkString("\n")
  }

  def getUnifiedGroupByErrorString(groupByEvalResult: GroupByEvalResult): String = {
    val errors = mutable.ListBuffer[String]()

    // Check source expression errors
    Option(groupByEvalResult.getSourceExpressionCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- source: ${check.getMessage}"
      }
    }

    // Check source timestamp errors
    Option(groupByEvalResult.getSourceTimestampCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- source_timestamp: ${check.getMessage}"
      }
    }

    // Check aggregation expression errors
    Option(groupByEvalResult.getAggExpressionCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- aggregation: ${check.getMessage}"
      }
    }

    // Check derivations expression errors
    Option(groupByEvalResult.getDerivationsExpressionCheck).foreach { check =>
      if (check.getCheckResult == CheckResult.FAILURE) {
        errors += s"- derivations: ${check.getMessage}"
      }
    }

    errors.mkString("\n")
  }

  def structTypeToSchemaMap(structType: StructType): java.util.Map[String, String] = {
    structType.fields
      .map { field =>
        field.name -> field.dataType.toString
      }
      .toMap
      .asJava
  }

}
