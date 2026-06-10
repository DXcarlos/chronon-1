package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, SourceOps, WindowOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.{Accuracy, DataModel, GroupBy, Join, PartitionSpec, Source}

/** Sub-daily output specs are supported for events-left joins (temporal parts compute at the
  * output grain; snapshot parts look back to the latest covering DAILY snapshot via
  * JoinUtils.snapshotLookbackRange) and for GroupBy backfill/upload. ENTITIES-left joins stay
  * daily: the left side itself is a daily snapshot, so sub-daily output partitions would just
  * replicate each day's rows.
  */
object SubDailyValidation {

  // anything that isn't plain midnight-anchored daily gets the stricter treatment - a daily
  // span with a nonzero anchor offset has the same snapshot-boundary concerns as hourly
  def isSubDaily(spec: PartitionSpec): Boolean = !spec.isDaily

  def assertJoinSupported(join: Join)(implicit outputSpec: PartitionSpec): Unit = {
    if (!isSubDaily(outputSpec)) return

    require(
      join.left.dataModel != DataModel.ENTITIES,
      s"join ${join.metaData.name}: sub-daily output partitions are not supported for ENTITIES left sources; " +
        "entity snapshots are daily. Use daily partitions."
    )

    // the driving inputs must be able to deliver intraday data; snapshot-accuracy parts are
    // exempt because they bind to daily snapshots by design (lookback semantics)
    assertSourcesIntradayReady(Seq(join.left), join.metaData.name)
    Option(join.joinParts)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty)
      .filter(_.groupBy.inferredAccuracy == Accuracy.TEMPORAL)
      .foreach(jp => assertSourcesIntradayReady(jp.groupBy.sources.toScala.toSeq, jp.groupBy.metaData.name))
  }

  /** A sub-daily schedule promises freshness its inputs must be able to deliver: every driving
    * EVENTS source must either declare a grain at least as fine as the output interval, or be
    * timestamp-backed (timePartitioned) so its watermark moves intraday. A daily
    * catalog-partitioned events source under a sub-daily output either fires on possibly
    * incomplete partitions or waits and runs all intraday partitions as a burst after the day
    * closes - invalid either way. ENTITIES (daily snapshot semantics), cumulative
    * (latest-partition semantics) and chained JoinSources are exempt.
    */
  def assertSourcesIntradayReady(sources: Seq[Source], confName: String)(implicit outputSpec: PartitionSpec): Unit = {
    if (!isSubDaily(outputSpec)) return

    sources.foreach { source =>
      val exempt = source.dataModel != DataModel.EVENTS || source.isCumulative || source.isSetJoinSource
      if (!exempt) {
        val query = source.query
        val timePartitioned = Option(query).exists(q => q.isSetTimePartitioned && q.isTimePartitioned)
        val declaredSpan = Option(query)
          .flatMap(q => Option(q.partitionInterval))
          .map(_.millis)
          .getOrElse(24 * 60 * 60 * 1000L)
        require(
          timePartitioned || declaredSpan <= outputSpec.spanMillis,
          s"$confName: events source ${source.rawTable} cannot deliver sub-daily freshness for a " +
            s"${outputSpec.spanMillis}ms output interval. Either declare partition_interval (<= the output " +
            "interval) matching the table's actual grain, set time_partitioned=True for timestamp-backed " +
            "tables, or use a daily schedule."
        )
      }
    }
  }

  def assertGroupByUploadSupported(groupBy: GroupBy)(implicit outputSpec: PartitionSpec): Unit = {
    if (!isSubDaily(outputSpec)) return
    // uploads re-aggregate as-of the partition boundary; entity sources still snapshot daily
    require(
      groupBy.dataModel != DataModel.ENTITIES,
      s"groupBy ${groupBy.metaData.name}: sub-daily uploads are not supported for ENTITIES sources; " +
        "entity snapshots are daily."
    )
    assertSourcesIntradayReady(groupBy.sources.toScala.toSeq, groupBy.metaData.name)
  }
}
