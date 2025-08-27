package ai.chronon.spark.test.utils

import ai.chronon.api._
import ai.chronon.online
import ai.chronon.online._
import org.apache.spark.sql.DataFrame

case class GroupByTestSuite(
    name: String,
    groupByConf: GroupBy,
    groupByData: DataFrame
)