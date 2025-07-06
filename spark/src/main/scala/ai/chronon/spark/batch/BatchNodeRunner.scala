package ai.chronon.spark.batch

import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, ThriftJsonCodec}
import ai.chronon.api.planner.NodeRunner
import ai.chronon.planner.{GroupByUploadNode, MonolithJoinNode, NodeContent}
import ai.chronon.spark.{GroupByUpload, Join}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.submission.SparkSessionBuilder
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class BatchNodeRunnerArgs(args: Array[String]) extends ScallopConf(args) {

  this: ScallopConf =>

  val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to node configuration file")
  val startDs: ScallopOption[String] = opt[String](
    required = false,
    descr = "Start date string in format yyyy-MM-dd, used for partitioning"
  )
  val endDs: ScallopOption[String] = opt[String](
    required = true,
    descr = "End date string in format yyyy-MM-dd, used for partitioning"
  )

  verify()
}

object BatchNodeRunner extends NodeRunner {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private def createTableUtils(name: String): TableUtils = {
    val spark = SparkSessionBuilder.build(s"batch-node-runner-${name}")
    TableUtils(spark)
  }

  private[batch] def run(metadata: MetaData, conf: NodeContent, range: PartitionRange, tableUtils: TableUtils): Unit = {
    conf.getSetField match {
      case NodeContent._Fields.MONOLITH_JOIN =>
        runMonolithJoin(metadata, conf.getMonolithJoin, range, tableUtils)
      case NodeContent._Fields.GROUP_BY_UPLOAD =>
        runGroupByUpload(metadata, conf.getGroupByUpload, range, tableUtils)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported NodeContent type: ${conf.getSetField}")
    }
  }

  private def runGroupByUpload(metadata: MetaData,
                               groupByUpload: GroupByUploadNode,
                               range: PartitionRange,
                               tableUtils: TableUtils): Unit = {
    require(groupByUpload.isSetGroupBy, "GroupByUploadNode must have a groupBy set")
    val groupBy = groupByUpload.groupBy
    logger.info(s"Running groupBy upload for '${metadata.name}' for day: ${range.end}")

    GroupByUpload.run(groupBy, range.end, Some(tableUtils))
    logger.info(s"Successfully completed groupBy upload for '${metadata.name}' for day: ${range.end}")
  }

  private def runMonolithJoin(metadata: MetaData,
                              monolithJoin: MonolithJoinNode,
                              range: PartitionRange,
                              tableUtils: TableUtils): Unit = {
    require(monolithJoin.isSetJoin, "MonolithJoinNode must have a join set")

    val joinConf = monolithJoin.join
    val joinName = metadata.name
    val skewFreeMode = tableUtils.sparkSession.conf
      .get("spark.chronon.join.backfill.mode.skewFree", "false")
      .toBoolean

    logger.info(s"Running join backfill for '$joinName' with skewFree mode: $skewFreeMode")
    logger.info(s"Processing range: [${range.start}, ${range.end}]")

    if (skewFreeMode) {
      UnionJoin.computeJoinAndSave(joinConf, range)(tableUtils)
      logger.info(s"Successfully wrote range: $range")
    } else {
      val join = new Join(joinConf, range.end, tableUtils)
      val df = join.computeJoin(overrideStartPartition = Option(range.start))

      df.show(numRows = 3, truncate = 0, vertical = true)
      logger.info(s"\nShowing three rows of output above.\nQuery table '$joinName' for more.\n")
    }
  }

  override def run(metadata: MetaData, conf: NodeContent, range: Option[PartitionRange]): Unit = {
    require(range.isDefined, "Partition range must be defined for batch node runner")
    run(metadata, conf, range.get, createTableUtils(metadata.name))
  }

  private[batch] def loadNodeContent(confPath: String): (MetaData, NodeContent) = {
    val nodeContent = ThriftJsonCodec.fromJsonFile[NodeContent](confPath, check = true)
    (nodeContent.getSetField match {
       case NodeContent._Fields.MONOLITH_JOIN => nodeContent.getMonolithJoin.join.metaData
       case NodeContent._Fields.STAGING_QUERY => nodeContent.getStagingQuery.stagingQuery.metaData
       case other => throw new UnsupportedOperationException(s"NodeContent type ${other} not supported")
     },
     nodeContent)
  }

  def runFromArgs(confPath: String, startDs: String, endDs: String): Try[Unit] = {
    Try {
      val range = PartitionRange(startDs, endDs)(PartitionSpec.daily)
      val (metadata, nodeContent) = loadNodeContent(confPath)

      logger.info(s"Starting batch node runner for '${metadata.name}'")
      run(metadata, nodeContent, Option(range))
      logger.info(s"Successfully completed batch node runner for '${metadata.name}'")
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val batchArgs = new BatchNodeRunnerArgs(args)

      runFromArgs(batchArgs.confPath(), batchArgs.startDs.toOption.orNull, batchArgs.endDs()) match {
        case Success(_) =>
          logger.info("Batch node runner completed successfully")
        case Failure(exception) =>
          logger.error("Batch node runner failed", exception)
          System.exit(1)
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to parse arguments or initialize runner", e)
        System.exit(1)
    }
  }
}
