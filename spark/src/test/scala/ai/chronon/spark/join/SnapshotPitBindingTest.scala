package ai.chronon.spark.join

import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Constants, DateRange, Operation, TsUtils}
import ai.chronon.planner.JoinMergeNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.MergeJob
import org.junit.Assert._

/** Snapshot-accuracy joins must be point-in-time correct ACROSS snapshot changes: a row at
  * time T binds to the latest snapshot whose as-of boundary is <= T, regardless of which left
  * partition the row sits in. The interesting case for sub-daily outputs is a left partition
  * straddling midnight - its rows must SPLIT across two snapshot versions.
  */
class SnapshotPitBindingTest extends BaseJoinTest {

  it should "bind rows of one sub-daily left partition to different snapshot versions per row time" in {
    import spark.implicits._

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(query = Builders.Query(), table = s"$namespace.snapshot_pit_views")),
      keyColumns = Seq("item"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
      metaData = Builders.MetaData(name = "unit_test.snapshot_pit_gb", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinPart = Builders.JoinPart(groupBy = viewsGroupBy)
    val join = Builders.Join(
      left = Builders.Source.events(Builders.Query(), table = s"$namespace.snapshot_pit_left"),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = "unit_test.snapshot_pit_join", namespace = namespace)
    )

    val mergeJob = new MergeJob(
      new JoinMergeNode().setJoin(join),
      join.metaData,
      new DateRange().setStartDate("2026-06-03").setEndDate("2026-06-04"),
      Seq(joinPart)
    )(tableUtils)

    // two rows of the SAME 3h@01:00 left partition [22:00, 01:00) straddling midnight
    val leftDf = Seq(
      ("a", TsUtils.datetimeToTs("2026-06-03 23:30:00"), "2026-06-03-22-00"),
      ("a", TsUtils.datetimeToTs("2026-06-04 00:30:00"), "2026-06-03-22-00")
    ).toDF("item", "ts", "ds")
      .withTimeBasedColumn(Constants.TimePartitionColumn)

    // daily snapshots with a value CHANGE between versions: ds=06-02 is as-of midnight 06-03,
    // ds=06-03 is as-of midnight 06-04
    val rightDf = Seq(
      ("a", 100.0, "2026-06-02"),
      ("a", 200.0, "2026-06-03")
    ).toDF("item", "time_spent_ms_average", "ds")

    val joined = mergeJob.joinWithLeft(leftDf, rightDf, joinPart)
    val valueCol = joined.columns.find(_.endsWith("time_spent_ms_average")).get

    val byTs = joined
      .collect()
      .map(row => row.getAs[Long]("ts") -> row.getAs[Double](valueCol))
      .toMap

    // pre-midnight row sees the old snapshot version, post-midnight row the new one
    assertEquals(100.0, byTs(TsUtils.datetimeToTs("2026-06-03 23:30:00")), 0.0)
    assertEquals(200.0, byTs(TsUtils.datetimeToTs("2026-06-04 00:30:00")), 0.0)
  }
}
