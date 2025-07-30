package ai.chronon.flink.window

import ai.chronon.api.GroupBy
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.deser.ProjectedEvent
import org.apache.flink.api.java.functions.KeySelector
import org.slf4j.LoggerFactory

import java.util
import scala.collection.Seq

/** A KeySelector is what Flink uses to determine how to partition a DataStream. In a distributed environment, the
  * KeySelector guarantees that events with the same key always end up in the same machine.
  * If invoked multiple times on the same object, the returned key must be the same.
  */
object KeySelectorBuilder {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)

  /** Given a GroupBy, create a function to key the output of a SparkExprEval operator by the entities defined in the
    * GroupBy. The function returns a List of size equal to the number of keys in the GroupBy.
    *
    * For example, if a GroupBy is defined as "GroupBy(..., keys=["color", "size"], ...), the function will key the
    * Flink SparkExprEval DataStream by color and size, so all events with the same (color, size) are sent to the same
    * operator.
    */
  def build(groupBy: GroupBy): KeySelector[ProjectedEvent, util.List[Any]] = {
    // List uses MurmurHash.seqHash for its .hashCode(), which gives us hashing based on content.
    // (instead of based on the instance, which is the case for Array).
    val groupByKeys: Seq[String] = groupBy.keyColumns.toScala
    logger.info(
      f"Creating key selection function for Flink app. groupByKeys=$groupByKeys"
    )
    // Create explicit KeySelector instead of lambda
    new KeySelector[ProjectedEvent, util.List[Any]] {
      override def getKey(deSerEvent: ProjectedEvent): util.List[Any] = {
        val sparkEvalOutput = deSerEvent.fields
        val result = new util.ArrayList[Any](groupByKeys.length)
        groupByKeys.foreach(k => result.add(sparkEvalOutput.get(k).orNull))
        result
      }
    }
  }
}
