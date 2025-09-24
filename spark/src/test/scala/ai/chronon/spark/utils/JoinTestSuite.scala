package ai.chronon.spark.utils

import ai.chronon.api._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online._

case class JoinTestSuite(joinConf: Join,
                         groupBys: Seq[GroupByTestSuite],
                         fetchExpectations: (Map[String, AnyRef], Map[String, AnyRef]))
object JoinTestSuite {

  def apply(joinConf: Join, groupBys: Seq[GroupByTestSuite]): JoinTestSuite = {
    val suite = JoinTestSuite(joinConf, groupBys)
    assert(
      groupBys.map(_.groupByConf.metaData.name) ==
        joinConf.joinParts.toScala
          .map(_.groupBy.metaData.name)
    )
    suite
  }
}