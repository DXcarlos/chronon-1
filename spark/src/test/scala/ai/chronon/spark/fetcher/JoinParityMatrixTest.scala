package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.RunnerUtils
import ai.chronon.spark.batch.ModularMonolith
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers

import java.util.TimeZone
import java.util.concurrent.Executors

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}

/** Online/offline join-parity matrix with hand-computed golden values.
  *
  * The matrix is accuracy/cadence x data model — six cells, spread over two joins with EVENTS
  * left sources:
  *
  * {{{
  *                          EVENTS                            ENTITIES
  *   TEMPORAL               sawtooth windows over events      mutations-based as-of state
  *   SNAPSHOT daily         previous-day snapshot binding     previous-day snapshot binding
  *   SNAPSHOT 3h offset 1h  as-of binding by left row time    as-of binding by left row time
  * }}}
  *
  * Cells 1 (temporal events), 2 (snapshot daily events), 5 (snapshot 3h events) and
  * 6 (snapshot 3h entities) ride on a sub-daily join whose left/output partition grid is
  * 3h with a 1h offset (partitions ..., 22:00, 01:00, 04:00, ..., labels = interval starts).
  * Cells 3 (temporal entities / mutations) and 4 (snapshot daily entities) ride on a classic
  * daily join: the engine's MergeJob binds snapshot/mutation entity partitions through a
  * single-join-span shift, so daily entity tables can only be bound by a daily-grid join
  * (see "engine binding convention" below); the daily join is the engine-supported home for
  * those two cells, and the sub-daily join carries a 3h+1h-partitioned entity table instead.
  *
  * ==Engine snapshot binding convention (read from MergeJob / SourceJob / Extensions)==
  *
  * For an EVENTS left and a SNAPSHOT-accuracy join part:
  *  - SourceJob/Extensions.withTimeBasedColumn stamps each left row with
  *    TimePartitionColumn = floor(left.ts to the JOIN's partition grid, including the grid
  *    offset) — e.g. ts 12:07 on the 3h+1h grid floors to "2023-08-14 10:00".
  *  - JoinPartJob materializes the join part table at JOIN-grid partition labels p, where
  *    partition p holds the aggregate as-of epoch(p) + joinSpan (snapshotEvents shifts end
  *    times by one span; snapshotEntities passes entity partitions through unchanged).
  *  - MergeJob scans the part table one join span back (dayStep.shift(-1)) and relabels
  *    right ds + joinSpan as TimePartitionColumn before the equality join.
  *
  * Net effect: a left row binds the snapshot value as-of floor(left.ts, joinGrid) — e.g.
  * left ts 12:07 -> grid floor 10:00 -> bound part-table partition 07:00 (one span back) ->
  * value = aggregate over events with ts < 10:00. For RHS sources that are *coarser* than
  * the join grid (the daily-events cell under the 3h+1h join), as-of correctness relies on
  * the source declaring no timeColumn: the engine then synthesizes ts = partition end - 1ms,
  * so the binding becomes "latest source partition COMPLETE at the join-grid floor", which
  * is exactly what online serving (whole-partition batch uploads) can reproduce. A coarse
  * snapshot source with a real timeColumn would diverge from online (offline would see
  * intra-partition events past the upload boundary) — that is why the daily events fixture
  * deliberately has no ts column.
  *
  * Online semantics being mirrored: SNAPSHOT parts serve the batch-upload value as-of the
  * upload's batch-end (epoch of the groupBy-grid floor of endDs); TEMPORAL parts serve
  * batch IR as-of batch-end merged with streaming rows in [batchEnd, queryTs).
  */
class JoinParityMatrixTest extends SparkTestBase with Matchers {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  // 3h span, 1h offset: grid points 01:00, 04:00, 07:00, ..., 22:00 (UTC)
  private val subDailySpec =
    PartitionSpec("ds", "yyyy-MM-dd HH:mm", 3 * WindowUtils.Hour.millis, WindowUtils.Hour.millis)
  private val dailySpec = PartitionSpec.daily

  private val sixHours = new Window(6, TimeUnit.HOURS)
  private val oneDay = new Window(1, TimeUnit.DAYS)

  // All timestamps UTC. ts("2023-08-14 12:07") => epoch millis.
  private def ts(arg: String): Long = TsUtils.datetimeToTs(s"$arg:00")

  // ---------------------------------------------------------------------------------------------
  // Sub-daily join: query timestamps (left rows). Left partitions are on the 3h+1h grid.
  // ---------------------------------------------------------------------------------------------
  private val T_U1_OFF_GRID = ts("2023-08-14 12:07") // off-grid: floors to 10:00
  private val T_U2_POST_STREAM = ts("2023-08-14 12:30") // after the 12:20 streaming event
  private val T_U1_ON_GRID = ts("2023-08-14 13:00") // exactly on a 3h+1h grid boundary
  private val T_U3_POST_MIDNIGHT = ts("2023-08-14 00:30") // floors across midnight to 2023-08-13 22:00

  case class Golden(user: String, tsMillis: Long, leftDs: String, features: Map[String, Any])

  // ---------------------------------------------------------------------------------------------
  // Sub-daily join goldens. Derivations reference the fixture tables created in
  // generateSubDailyJoin below. Engine convention: snapshot cells bind as-of
  // floor(left.ts, 3h+1h grid); temporal cell is sawtooth-accurate at left.ts.
  // ---------------------------------------------------------------------------------------------
  private val subDailyGoldens = Seq(
    // u1 @ 12:07 (off-grid) -> 3h+1h grid floor = 10:00 -> snapshot part partition = 07:00 (one
    // span back) -> snapshot values as-of 10:00.
    Golden(
      "u1",
      T_U1_OFF_GRID,
      "2023-08-14 10:00",
      Map(
        // temporal events, SUM(txn_amount) over a 1d sawtooth window ending at 12:07:
        //   5 @ 08:15 + 11 @ 11:30 = 16. The 1000 @ 2023-08-13 11:00 fell out of the 1d window
        //   (~1h before the window tail), and 13 @ 12:20 belongs to u2.
        "tmp_user_id_txn_amount_sum_1d" -> 16L,
        // daily-events snapshot (no timeColumn => engine ts = partition end - 1ms): daily
        // partitions complete by 10:00 are ds <= 2023-08-13 -> SUM = 4. The 50 in ds=2023-08-14
        // (completes at 08-15 00:00) is excluded even though it is scanned by the join part job.
        "snapd_user_id_amount_d_sum" -> 4L,
        // 3h+1h events snapshot as-of 10:00 (real ts): 15 @ 08-13 23:40 + 20 @ 02:30 +
        // 35 @ 05:30 + 100 @ 08:30 = 170; 1000 @ 11:30 is after the 10:00 bound.
        "snap3_user_id_amount_3h_sum" -> 170L,
        // 6h window as-of 10:00 => events with ts in [04:00, 10:00): 35 @ 05:30 + 100 @ 08:30 = 135.
        "snap3_user_id_amount_3h_sum_6h" -> 135L,
        // 3h+1h entity snapshot: binds entity partition 07:00 = state as-of 10:00 -> u1 balance 15.
        "ent3_user_id_balance_3h" -> 15L
      )
    ),
    // u2 @ 12:30 -> same 10:00 grid floor; temporal cell additionally sees the 12:20 event that
    // only the streaming path can deliver online (proves TEMPORAL is actually temporal: no
    // snapshot bound at 10:00 or 13:00 contains a 12:20 event for a 12:30 query).
    Golden(
      "u2",
      T_U2_POST_STREAM,
      "2023-08-14 10:00",
      Map(
        // 7 @ 09:40 (batch side) + 13 @ 12:20 (post-batch-end, streamed) = 20
        "tmp_user_id_txn_amount_sum_1d" -> 20L,
        // daily partitions complete by 10:00: ds <= 2023-08-13 -> 9 (90 @ ds=2023-08-14 excluded)
        "snapd_user_id_amount_d_sum" -> 9L,
        // as-of 10:00: 7 @ 06:10 + 70 @ 09:00 = 77
        "snap3_user_id_amount_3h_sum" -> 77L,
        // 6h window [04:00, 10:00): both events inside -> 77
        "snap3_user_id_amount_3h_sum_6h" -> 77L,
        // entity partition 07:00 -> u2 balance 25
        "ent3_user_id_balance_3h" -> 25L
      )
    ),
    // u1 @ exactly 13:00 (on-grid) -> floor(13:00) = 13:00 -> bound part partition = 10:00 ->
    // snapshot values as-of 13:00: the [10:00, 13:00) partition IS visible at exactly 13:00
    // (boundary is inclusive of the partition that just completed, exclusive of events at >= 13:00).
    Golden(
      "u1",
      T_U1_ON_GRID,
      "2023-08-14 13:00",
      Map(
        // 1d window ending 13:00: 5 @ 08:15 + 11 @ 11:30 = 16 (nothing for u1 in (12:07, 13:00])
        "tmp_user_id_txn_amount_sum_1d" -> 16L,
        // daily partitions complete by 13:00: still only ds <= 2023-08-13 -> 4 (proves the daily
        // cell binds a different (coarser) partition than the 3h cell for the same left row)
        "snapd_user_id_amount_d_sum" -> 4L,
        // as-of 13:00: 170 + 1000 @ 11:30 = 1170 (the 11:30 event flips in vs. the 12:07 row)
        "snap3_user_id_amount_3h_sum" -> 1170L,
        // 6h window [07:00, 13:00): 100 @ 08:30 + 1000 @ 11:30 = 1100
        "snap3_user_id_amount_3h_sum_6h" -> 1100L,
        // entity partition 10:00 -> u1 balance 16 (differs from the 12:07 row's 15: different bind)
        "ent3_user_id_balance_3h" -> 16L
      )
    ),
    // u3 @ 00:30 just after midnight -> 3h+1h grid floor crosses the day boundary to
    // 2023-08-13 22:00 -> snapshot part partition = 2023-08-13 19:00 -> values as-of 22:00 (08-13).
    Golden(
      "u3",
      T_U3_POST_MIDNIGHT,
      "2023-08-13 22:00",
      Map(
        // 1d window ending 00:30: 9 @ 08-13 21:00 + 17 @ 08-14 00:10 = 26 (the 00:10 event lives
        // in source partition "2023-08-13 22:00", which straddles midnight)
        "tmp_user_id_txn_amount_sum_1d" -> 26L,
        // daily partitions complete by 2023-08-13 22:00: only ds <= 2023-08-12 -> 6. The 60 in
        // ds=2023-08-13 is excluded because that partition only completes at 08-14 00:00 > 22:00.
        "snapd_user_id_amount_d_sum" -> 6L,
        // as-of 2023-08-13 22:00: 3 @ 19:30; 40 @ 23:00 is after the bound
        "snap3_user_id_amount_3h_sum" -> 3L,
        // 6h window [16:00, 22:00) on 08-13: 3 @ 19:30
        "snap3_user_id_amount_3h_sum_6h" -> 3L,
        // entity partition 2023-08-13 19:00 -> u3 balance 31
        "ent3_user_id_balance_3h" -> 31L
      )
    )
  )

  // ---------------------------------------------------------------------------------------------
  // Daily join: query timestamps and goldens.
  // ---------------------------------------------------------------------------------------------
  private val T_B_U1_POST_MIDNIGHT = ts("2023-08-14 00:30")
  private val T_B_U1_MIDDAY = ts("2023-08-14 12:07")
  private val T_B_U2_PRE_MUTATION = ts("2023-08-14 09:00")
  private val T_B_U2_POST_MUTATION = ts("2023-08-14 12:30")

  private val dailyGoldens = Seq(
    // Temporal entities batch state as-of 2023-08-14 00:00 (snapshot partition 2023-08-13):
    //   u1: rating 4 (ts 08-12 10:00) + rating 3 (ts 08-13 09:00) = 7;  u2: rating 5 = 5.
    // 08-14 mutations: u1 insert rating 2 @ 06:00; u1 update 2 -> 9 @ 11:00; u2 insert 1 @ 10:00.
    // Daily entity snapshot: previous-day binding -> partition 2023-08-13 regardless of intra-day
    // ts -> u1 balance 102, u2 balance 202 (101/201 of 08-12 and 103/203 of 08-14 are NOT bound).
    Golden("u1",
           T_B_U1_POST_MIDNIGHT,
           "2023-08-14",
           Map(
             // no mutation_ts <= 00:30 yet -> batch state only
             "mut_user_id_rating_sum" -> 7L,
             "entd_user_id_balance_d" -> 102L
           )),
    Golden("u1",
           T_B_U1_MIDDAY,
           "2023-08-14",
           Map(
             // 7 + insert(2) @ 06:00, then update @ 11:00 reverses 2 and lands 9: 7 + 2 - 2 + 9 = 16
             "mut_user_id_rating_sum" -> 16L,
             "entd_user_id_balance_d" -> 102L
           )),
    Golden("u2",
           T_B_U2_PRE_MUTATION,
           "2023-08-14",
           Map(
             // u2's insert happens at 10:00 > 09:00 -> still batch state 5
             "mut_user_id_rating_sum" -> 5L,
             "entd_user_id_balance_d" -> 202L
           )),
    Golden("u2",
           T_B_U2_POST_MUTATION,
           "2023-08-14",
           Map(
             // 5 + insert(1) @ 10:00 = 6
             "mut_user_id_rating_sum" -> 6L,
             "entd_user_id_balance_d" -> 202L
           ))
  )

  it should "match goldens offline and online for the sub-daily matrix cells" in {
    val namespace = "join_parity_matrix_subdaily"
    val joinConf = generateSubDailyJoin(namespace, spark)
    implicit val tableUtils: TableUtils = TableUtils(spark, subDailySpec)

    // Offline backfill over the full left partition range (22:00 of 08-13 through 13:00 of 08-14;
    // the in-between grid partitions have no left rows and are skipped by the source job).
    val dateRange = new DateRange().setStartDate("2023-08-13 22:00").setEndDate("2023-08-14 13:00")
    ModularMonolith.run(joinConf, dateRange)

    val offlineRows = assertOfflineMatchesGoldens(joinConf, subDailyGoldens)

    // Online phase 1: pin batch-end at the 10:00 grid boundary (endDs partition
    // "2023-08-14 10:00"; each groupBy uploads its grid floor of that boundary: the 3h+1h
    // groupBys batch-end at 10:00, the daily groupBy at 2023-08-14 00:00). Events at/after
    // 10:00 reach the temporal cell only through the streaming injection path. Only left
    // rows inside [10:00, 13:00) are servable from this upload.
    val phase1Rows = subDailyGoldens.filter(g => g.leftDs == "2023-08-14 10:00")
    serveAndAssertOnline(joinConf, "2023-08-14 10:00", subDailySpec, namespace, "subdaily_p1", phase1Rows, offlineRows)

    // Online phase 2: re-serve with batch-end pinned at 13:00 (fresh KV store and re-uploaded
    // batch data) and fetch the row that sits exactly on the 13:00 grid boundary. A query at
    // exactly batch-end is legal (batchEndTs > queryTs is the rejection condition) and must see
    // the [10:00, 13:00) snapshot partition.
    // The u3 @ 00:30 row is offline-golden-only: a single pinned batch upload cannot represent a
    // query time before its batch-end (the temporal path rejects queryTs < batchEnd, and snapshot
    // serving cannot time-travel), so there is no meaningful online assertion for it.
    val phase2Rows = subDailyGoldens.filter(g => g.leftDs == "2023-08-14 13:00")
    serveAndAssertOnline(joinConf, "2023-08-14 13:00", subDailySpec, namespace, "subdaily_p2", phase2Rows, offlineRows)
  }

  it should "match goldens offline and online for the daily matrix cells" in {
    val namespace = "join_parity_matrix_daily"
    val joinConf = generateDailyJoin(namespace, spark)
    implicit val tableUtils: TableUtils = TableUtils(spark, dailySpec)

    val dateRange = new DateRange().setStartDate("2023-08-14").setEndDate("2023-08-14")
    ModularMonolith.run(joinConf, dateRange)

    val offlineRows = assertOfflineMatchesGoldens(joinConf, dailyGoldens)

    // Online: batch-end pinned at 2023-08-14 00:00 (upload of the 2023-08-13 snapshot
    // partitions); the 08-14 mutations are injected through the streaming path. All four query
    // timestamps are inside the [08-14 00:00, 08-15 00:00) serving window, so all are fetchable.
    serveAndAssertOnline(joinConf, "2023-08-14", dailySpec, namespace, "daily_p1", dailyGoldens, offlineRows)
  }

  // ---------------------------------------------------------------------------------------------
  // Assertion harness
  // ---------------------------------------------------------------------------------------------

  /** Asserts the offline join output equals the goldens row by row (exact values, exact left
    * partition labels, no extra rows) and returns the offline rows keyed by (user, ts).
    */
  private def assertOfflineMatchesGoldens(joinConf: Join, goldens: Seq[Golden])(implicit
      tableUtils: TableUtils): Map[(String, Long), Map[String, Any]] = {
    val outputDf = tableUtils.sql(s"SELECT * FROM ${joinConf.metaData.outputTable}")
    outputDf.show(truncate = false)
    val featureColumns = goldens.flatMap(_.features.keys).distinct

    val offlineRows: Map[(String, Long), Map[String, Any]] = outputDf
      .collect()
      .map { row =>
        val key = (row.getAs[String]("user_id"), row.getAs[Long]("ts"))
        val values: Map[String, Any] =
          (featureColumns.map(c => c -> row.getAs[Any](c)) :+ ("ds" -> row.getAs[String]("ds"))).toMap
        key -> values
      }
      .toMap

    withClue(s"offline output of ${joinConf.metaData.outputTable} should have exactly one row per golden row: ") {
      offlineRows.keySet shouldEqual goldens.map(g => (g.user, g.tsMillis)).toSet
    }

    goldens.foreach { golden =>
      val actual = offlineRows((golden.user, golden.tsMillis))
      withClue(s"offline left partition label for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)}: ") {
        actual("ds") shouldEqual golden.leftDs
      }
      golden.features.foreach { case (feature, expected) =>
        withClue(s"offline $feature for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)}: ") {
          actual(feature) shouldEqual expected
        }
      }
    }
    offlineRows
  }

  /** Uploads every joinPart groupBy (batch + streaming where applicable) into a fresh in-memory
    * KV store for the given endDs, fetches the join at the goldens' query timestamps and asserts
    * the fetched values equal both the goldens and the offline rows.
    */
  private def serveAndAssertOnline(joinConf: Join,
                                   endDs: String,
                                   joinSpec: PartitionSpec,
                                   namespace: String,
                                   phaseId: String,
                                   goldens: Seq[Golden],
                                   offlineRows: Map[(String, Long), Map[String, Any]]): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore(s"JoinParityMatrixTest_$phaseId")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    joinConf.joinParts.toScala.foreach { jp =>
      val groupBySpec = RunnerUtils.outputPartitionSpec(jp.groupBy.metaData, joinSpec)
      val groupByEndDs = joinSpec.translate(endDs, groupBySpec)
      // A previous serve phase leaves its batch partition in the upload table; bulkPut loads
      // every partition of that table, so stale uploads must be dropped before re-serving.
      spark.sql(s"DROP TABLE IF EXISTS ${jp.groupBy.metaData.uploadTable}")
      val groupByTableUtils = TableUtils(spark, groupBySpec)
      OnlineUtils.serve(groupByTableUtils, inMemoryKvStore, kvStoreFunc, namespace, groupByEndDs, jp.groupBy, dropDsOnWrite = true)
    }

    inMemoryKvStore.create(MetadataDataset)
    new MetadataStore(FetchContext(inMemoryKvStore)).putJoinConf(joinConf)

    val fetcher = mockApi.buildFetcher(debug = true)
    val requests = goldens.map { g =>
      Request(joinConf.metaData.name, Map("user_id" -> (g.user: AnyRef)), Some(g.tsMillis))
    }
    val responses = Await.result(fetcher.fetchJoin(requests), Duration(100, SECONDS))

    responses.zip(goldens).foreach { case (response, golden) =>
      val fetched = response.values.get
      golden.features.foreach { case (feature, expected) =>
        withClue(s"online[$phaseId] $feature for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)} " +
          s"(batch served at endDs=$endDs), full response: $fetched : ") {
          fetched.get(feature).orNull shouldEqual expected
          // ... and equal to the offline backfill row for the same (key, ts)
          fetched.get(feature).orNull shouldEqual offlineRows((golden.user, golden.tsMillis))(feature)
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Sub-daily join fixture
  // ---------------------------------------------------------------------------------------------
  private def generateSubDailyJoin(namespace: String, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    // Left events: deliberately awkward timestamps (see goldens above for the binding each
    // row exercises). ds labels are the 3h+1h grid floor of ts.
    val leftTable = s"$namespace.left_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", T_U1_OFF_GRID, "2023-08-14 10:00"),
          ("u2", T_U2_POST_STREAM, "2023-08-14 10:00"),
          ("u1", T_U1_ON_GRID, "2023-08-14 13:00"),
          ("u3", T_U3_POST_MIDNIGHT, "2023-08-13 22:00")
        ))
      .toDF("user_id", "ts", "ds")
      .save(leftTable)

    // TEMPORAL/EVENTS cell source, partitioned on the 3h+1h grid. The 11:30/12:20/13:40 events
    // sit at/after the phase-1 batch-end (10:00) and reach the fetcher via streaming injection.
    val txnTable = s"$namespace.txn_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", 1000L, ts("2023-08-13 11:00"), "2023-08-13 10:00"), // outside every 1d query window
          ("u3", 9L, ts("2023-08-13 21:00"), "2023-08-13 19:00"),
          ("u3", 17L, ts("2023-08-14 00:10"), "2023-08-13 22:00"), // partition label straddles midnight
          ("u1", 5L, ts("2023-08-14 08:15"), "2023-08-14 07:00"),
          ("u2", 7L, ts("2023-08-14 09:40"), "2023-08-14 07:00"),
          ("u1", 11L, ts("2023-08-14 11:30"), "2023-08-14 10:00"), // post-batch-end: streaming only
          ("u2", 13L, ts("2023-08-14 12:20"), "2023-08-14 10:00"), // post-batch-end: streaming only
          // u9 is never queried; it keeps the phase-2 streaming injection (ds >= 13:00) non-empty
          ("u9", 999L, ts("2023-08-14 13:40"), "2023-08-14 13:00")
        ))
      .toDF("user_id", "txn_amount", "ts", "ds")
      .save(txnTable)

    // SNAPSHOT-daily/EVENTS cell source: a classic daily event table WITHOUT a ts column. The
    // engine synthesizes ts = partition end - 1ms, which makes the offline binding "latest daily
    // partition complete at the left row's grid floor" — identical to online whole-partition
    // batch serving (see class doc).
    val dailyEventsTable = s"$namespace.daily_events"
    spark
      .createDataFrame(
        Seq(
          ("u3", 6L, "2023-08-12"),
          ("u1", 4L, "2023-08-13"),
          ("u2", 9L, "2023-08-13"),
          ("u3", 60L, "2023-08-13"), // excluded for u3 @ 00:30: 08-13 only completes at 08-14 00:00
          ("u1", 50L, "2023-08-14"), // proves daily vs 3h cells bind different partitions: the 3h
          ("u2", 90L, "2023-08-14") //  cell sees 08-14-morning data at 10:00, the daily cell can't
        ))
      .toDF("user_id", "amount_d", "ds")
      .save(dailyEventsTable)

    // SNAPSHOT-3h+1h/EVENTS cell source: events partitioned on the offset grid with real
    // timestamps (timeColumn = ts), so snapshot values are exact as-of the 3h+1h boundaries.
    val offsetEventsTable = s"$namespace.offset_grid_events"
    spark
      .createDataFrame(
        Seq(
          ("u3", 3L, ts("2023-08-13 19:30"), "2023-08-13 19:00"),
          ("u1", 15L, ts("2023-08-13 23:40"), "2023-08-13 22:00"),
          ("u3", 40L, ts("2023-08-13 23:00"), "2023-08-13 22:00"), // after u3's 22:00 bound
          ("u1", 20L, ts("2023-08-14 02:30"), "2023-08-14 01:00"),
          ("u1", 35L, ts("2023-08-14 05:30"), "2023-08-14 04:00"),
          ("u2", 7L, ts("2023-08-14 06:10"), "2023-08-14 04:00"),
          ("u1", 100L, ts("2023-08-14 08:30"), "2023-08-14 07:00"),
          ("u2", 70L, ts("2023-08-14 09:00"), "2023-08-14 07:00"),
          ("u1", 1000L, ts("2023-08-14 11:30"), "2023-08-14 10:00") // visible only to the 13:00 row
        ))
      .toDF("user_id", "amount_3h", "ts", "ds")
      .save(offsetEventsTable)

    // SNAPSHOT-3h+1h/ENTITIES cell source: full entity state per 3h+1h partition; partition P
    // holds the state for [P, P+3h), i.e. the state as-of P+3h once complete. Balances encode the
    // partition (u1: 1x, u2: 2x, u3: 3x) so a wrong bind is unambiguous.
    val offsetBalanceTable = s"$namespace.offset_grid_balance"
    spark
      .createDataFrame(
        Seq(
          ("u1", 11L, "2023-08-13 19:00"),
          ("u2", 21L, "2023-08-13 19:00"),
          ("u3", 31L, "2023-08-13 19:00"),
          ("u1", 12L, "2023-08-13 22:00"),
          ("u2", 22L, "2023-08-13 22:00"),
          ("u3", 32L, "2023-08-13 22:00"),
          ("u1", 13L, "2023-08-14 01:00"),
          ("u2", 23L, "2023-08-14 01:00"),
          ("u3", 33L, "2023-08-14 01:00"),
          ("u1", 14L, "2023-08-14 04:00"),
          ("u2", 24L, "2023-08-14 04:00"),
          ("u3", 34L, "2023-08-14 04:00"),
          ("u1", 15L, "2023-08-14 07:00"),
          ("u2", 25L, "2023-08-14 07:00"),
          ("u3", 35L, "2023-08-14 07:00"),
          ("u1", 16L, "2023-08-14 10:00"),
          ("u2", 26L, "2023-08-14 10:00"),
          ("u3", 36L, "2023-08-14 10:00")
        ))
      .toDF("user_id", "balance_3h", "ds")
      .save(offsetBalanceTable)

    val temporalEventsGroupBy = Builders.GroupBy(
      metaData =
        Builders.MetaData(namespace = namespace, name = "parity_txn_sum", executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "txn_amount"),
                           timeColumn = "ts",
                           startPartition = "2023-08-13 10:00"),
            subDailySpec
          ),
          table = txnTable,
          topic = "parity_txn_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations =
        Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "txn_amount", windows = Seq(oneDay))),
      accuracy = Accuracy.TEMPORAL
    )

    val dailySnapshotEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_daily_amount",
                                   executionInfo = executionInfo(dailySpec)),
      sources = Seq(
        Builders.Source.events(
          // no timeColumn: classic snapshot-style daily event table (see class doc)
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "amount_d"), startPartition = "2023-08-12"),
            dailySpec
          ),
          table = dailyEventsTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_d",
                             windows = Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.SNAPSHOT
    )

    val offsetSnapshotEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_offset_amount",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "amount_3h"),
                           timeColumn = "ts",
                           startPartition = "2023-08-13 19:00"),
            subDailySpec
          ),
          table = offsetEventsTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_3h",
                             windows = Seq(WindowUtils.Unbounded, sixHours))),
      accuracy = Accuracy.SNAPSHOT
    )

    val offsetSnapshotEntitiesGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_offset_balance",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "balance_3h"), startPartition = "2023-08-13 19:00"),
            subDailySpec
          ),
          snapshotTable = offsetBalanceTable
        )),
      keyColumns = Seq("user_id")
      // no aggregations: pass-through latest state per partition (inferred SNAPSHOT accuracy)
    )

    Builders.Join(
      left = Builders.Source.events(
        query = withPartition(
          Builders.Query(selects = Builders.Selects("user_id", "ts"), startPartition = "2023-08-13 22:00"),
          subDailySpec
        ),
        table = leftTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEventsGroupBy, prefix = "tmp").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailySnapshotEventsGroupBy, prefix = "snapd").setUseLongNames(false),
        Builders.JoinPart(groupBy = offsetSnapshotEventsGroupBy, prefix = "snap3").setUseLongNames(false),
        Builders.JoinPart(groupBy = offsetSnapshotEntitiesGroupBy, prefix = "ent3").setUseLongNames(false)
      ),
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_matrix_subdaily_join",
                                   team = "chronon",
                                   executionInfo = executionInfo(subDailySpec))
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Daily join fixture (temporal entities via mutations + daily entity snapshot)
  // ---------------------------------------------------------------------------------------------
  private def generateDailyJoin(namespace: String, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    val leftTable = s"$namespace.left_events_daily"
    spark
      .createDataFrame(
        Seq(
          ("u1", T_B_U1_POST_MIDNIGHT, "2023-08-14"),
          ("u1", T_B_U1_MIDDAY, "2023-08-14"),
          ("u2", T_B_U2_PRE_MUTATION, "2023-08-14"),
          ("u2", T_B_U2_POST_MUTATION, "2023-08-14")
        ))
      .toDF("user_id", "ts", "ds")
      .save(leftTable)

    // TEMPORAL/ENTITIES cell: snapshot partition ds holds the state as of end-of-ds; the 08-13
    // partition is the batch state at 08-14 00:00 (u1: 4 + 3 = 7, u2: 5).
    val ratingsSnapshotTable = s"$namespace.ratings_snapshot"
    spark
      .createDataFrame(
        Seq(
          ("u1", ts("2023-08-12 10:00"), 4L, "2023-08-13"),
          ("u1", ts("2023-08-13 09:00"), 3L, "2023-08-13"),
          ("u2", ts("2023-08-13 11:00"), 5L, "2023-08-13")
        ))
      .toDF("user_id", "ts", "rating", "ds")
      .save(ratingsSnapshotTable)

    // Mutations of 2023-08-14 (partitioned by mutation day):
    //  - u1 inserts rating 2 at 06:00 (single is_before=false row),
    //  - u1 updates that rating 2 -> 9 at 11:00 (is_before=true reversal + is_before=false new),
    //  - u2 inserts rating 1 at 10:00.
    val ratingsMutationsTable = s"$namespace.ratings_mutations"
    spark
      .createDataFrame(
        Seq(
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14", ts("2023-08-14 06:00"), false),
          ("u1", ts("2023-08-14 06:00"), 2L, "2023-08-14", ts("2023-08-14 11:00"), true),
          ("u1", ts("2023-08-14 06:00"), 9L, "2023-08-14", ts("2023-08-14 11:00"), false),
          ("u2", ts("2023-08-14 10:00"), 1L, "2023-08-14", ts("2023-08-14 10:00"), false)
        ))
      .toDF("user_id", "ts", "rating", "ds", "mutation_ts", "is_before")
      .save(ratingsMutationsTable)

    // SNAPSHOT-daily/ENTITIES cell: balances encode the partition; only the previous-day
    // partition (2023-08-13 -> 102/202) may be bound for 08-14 left rows.
    val dailyBalanceTable = s"$namespace.daily_balance"
    spark
      .createDataFrame(
        Seq(
          ("u1", 101L, "2023-08-12"),
          ("u2", 201L, "2023-08-12"),
          ("u1", 102L, "2023-08-13"),
          ("u2", 202L, "2023-08-13"),
          ("u1", 103L, "2023-08-14"),
          ("u2", 203L, "2023-08-14")
        ))
      .toDF("user_id", "balance_d", "ds")
      .save(dailyBalanceTable)

    val mutationsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_ratings_sum",
                                   executionInfo = executionInfo(dailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "ts" -> "ts", "rating" -> "rating"),
            startPartition = "2023-08-12",
            mutationTimeColumn = "mutation_ts",
            reversalColumn = "is_before"
          ),
          snapshotTable = ratingsSnapshotTable,
          mutationTable = ratingsMutationsTable,
          mutationTopic = "parity_mutations_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "rating", windows = Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.TEMPORAL
    )

    val dailySnapshotEntitiesGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_daily_balance",
                                   executionInfo = executionInfo(dailySpec)),
      sources = Seq(
        Builders.Source.entities(
          query = Builders.Query(selects = Builders.Selects("user_id", "balance_d"), startPartition = "2023-08-12"),
          snapshotTable = dailyBalanceTable
        )),
      keyColumns = Seq("user_id")
      // no aggregations: pass-through latest state per partition (inferred SNAPSHOT accuracy)
    )

    Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(selects = Builders.Selects("user_id", "ts"), startPartition = "2023-08-14"),
        table = leftTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = mutationsGroupBy, prefix = "mut").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailySnapshotEntitiesGroupBy, prefix = "entd").setUseLongNames(false)
      ),
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "parity_matrix_daily_join",
                                   team = "chronon",
                                   executionInfo = executionInfo(dailySpec))
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Builders helpers
  // ---------------------------------------------------------------------------------------------

  // Declares the partition grid (column/format/interval/offset) on a source query. The offset is
  // always set explicitly: an unset query offset falls back to the JOIN's offset, which would
  // silently put e.g. a daily source onto the 1h-offset grid.
  private def withPartition(query: Query, partitionSpec: PartitionSpec): Query =
    query
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(partitionSpec.intervalWindow)
      .setPartitionOffset(offsetWindow(partitionSpec))

  private def executionInfo(partitionSpec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo()
      .setOutputTableInfo(
        new TableInfo()
          .setPartitionColumn(partitionSpec.column)
          .setPartitionFormat(partitionSpec.format)
          .setPartitionInterval(partitionSpec.intervalWindow)
          .setPartitionOffset(offsetWindow(partitionSpec))
      )

  private def offsetWindow(partitionSpec: PartitionSpec): Window =
    new Window((partitionSpec.offsetMillis / WindowUtils.Hour.millis).toInt, TimeUnit.HOURS)
}
