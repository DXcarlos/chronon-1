package ai.chronon.spark.batch

import ai.chronon.api.{Builders, DateRange}
import ai.chronon.planner.SourceWithFilterNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class SourceJobTest extends SparkTestBase with Matchers {

  private implicit val tableUtils: TableUtils = TableUtils(spark)
  private val namespace = "test_namespace_source_job"
  createDatabase(namespace)

  it should "scan custom source partition columns when explicit selects are present" in {
    import spark.implicits._

    val inputTable = s"$namespace.source_events_custom_partition"
    val outputTable = s"$namespace.source_job_custom_partition_output"
    spark.sql(s"DROP TABLE IF EXISTS $inputTable")
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")

    Seq(
      ("u0", 0L, 100L, "2026-05-19"),
      ("u1", 10L, 200L, "2026-05-20"),
      ("u2", 20L, 300L, "2026-05-21"),
      ("u3", 30L, 400L, "2026-05-22")
    ).toDF("user", "value", "ts", "event_date")
      .save(inputTable, partitionColumns = Seq("event_date"))

    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("user", "value"),
        partitionColumn = "event_date"
      ),
      table = inputTable
    )
    val node = new SourceWithFilterNode().setSource(source)
    val metaData = Builders.MetaData(name = "source_job_custom_partition_output", namespace = namespace)
    val range = new DateRange().setStartDate("2026-05-20").setEndDate("2026-05-21")

    new SourceJob(node, metaData, range).run()

    val rows = tableUtils
      .loadTable(outputTable)
      .select("user", "value", tableUtils.partitionColumn)
      .as[(String, Long, String)]
      .collect()
      .toSet

    rows should equal(
      Set(
        ("u1", 10L, "2026-05-20"),
        ("u2", 20L, "2026-05-21")
      )
    )
  }
}
