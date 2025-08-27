package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api.planner.{DependencyResolver, NodeRunner}
import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, ThriftJsonCodec}
import ai.chronon.online.Api
import ai.chronon.planner._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.{GroupBy, GroupByUpload, Join}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
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

  val onlineClass = opt[String](required = true,
                                descr =
                                  "Fully qualified Online.Api based class. We expect the jar to be on the class path")

  val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API Store")

  val tablePartitionsDataset = opt[String](required = true,
                                           descr = "Name of table in kv store to use to keep track of partitions",
                                           default = Option(NodeRunner.DefaultTablePartitionsDataset))

  verify()
}

class BatchNodeRunner(node: Node, tableUtils: TableUtils) extends NodeRunner {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def checkPartitions(conf: ExternalSourceSensorNode, range: PartitionRange): Try[Unit] = {
    val tableName = Option(conf.sourceTableDependency)
      .map(_.tableInfo)
      .map(_.table)
      .getOrElse(
        throw new IllegalArgumentException("ExternalSourceSensorNode must have a sourceTableDependency defined")
      )
    val retryCount = if (conf.isSetRetryCount) conf.retryCount else 3L
    val retryIntervalMin = if (conf.isSetRetryIntervalMin) conf.retryIntervalMin else 3L

    val spec = conf.sourceTableDependency.tableInfo.partitionSpec(tableUtils.partitionSpec)

    @tailrec
    def retry(attempt: Long): Try[Unit] = {
      val result = Try {
        val partitionsInRange =
          tableUtils.partitions(tableName,
                                partitionRange = Option(range.translate(spec)),
                                tablePartitionSpec = Option(spec))
        Option(range).map(_.partitions).getOrElse(Seq.empty).diff(partitionsInRange)
      }

      result match {
        case Success(missingPartitions) if missingPartitions.isEmpty =>
          logger.info(s"Input table ${tableName} has the requested range present: ${range}.")
          Success(())
        case Success(missingPartitions) if attempt < retryCount =>
          logger.warn(
            s"Attempt ${attempt + 1} failed: Input table ${tableName} is missing partitions: ${missingPartitions
                .mkString(", ")}. Retrying in ${retryIntervalMin} minutes")
          Thread.sleep(retryIntervalMin * 60 * 1000)
          retry(attempt + 1)
        case Success(missingPartitions) =>
          Failure(new RuntimeException(
            s"Sensor timed out after ${retryIntervalMin * attempt} minutes. Input table ${tableName} is missing partitions: ${missingPartitions
                .mkString(", ")}"))
        case Failure(e) => Failure(e)
      }
    }
    retry(0)
  }

  private def runStagingQuery(metaData: MetaData, stagingQuery: StagingQueryNode, range: PartitionRange): Unit = {
    require(stagingQuery.isSetStagingQuery, "StagingQueryNode must have a stagingQuery set")
    logger.info(s"Running staging query for '${metaData.name}'")
    val stagingQueryConf = stagingQuery.stagingQuery
    val sq = StagingQuery.from(stagingQueryConf, range.end, tableUtils)
    sq.compute(
      range,
      Option(stagingQuery.stagingQuery.setups).map(_.asScala).getOrElse(Seq.empty),
      Option(true)
    )

    logger.info(s"Successfully completed staging query for '${metaData.name}'")
  }

  private def runGroupByUpload(metadata: MetaData, groupByUpload: GroupByUploadNode, range: PartitionRange): Unit = {
    require(groupByUpload.isSetGroupBy, "GroupByUploadNode must have a groupBy set")
    val groupBy = groupByUpload.groupBy
    logger.info(s"Running groupBy upload for '${metadata.name}' for day: ${range.end}")

    GroupByUpload.run(groupBy, range.end, Option(tableUtils))
    logger.info(s"Successfully completed groupBy upload for '${metadata.name}' for day: ${range.end}")
  }

  private def runMonolithJoin(metadata: MetaData, monolithJoin: MonolithJoinNode, range: PartitionRange): Unit = {
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
      val result = join.forceComputeRangeAndSave(range)

      result match {
        case Some(df) =>
          logger.info(s"\nShowing three rows of output above.\nQuery table '${metadata.outputTable}' for more.\n")
          df.show(numRows = 3, truncate = 0, vertical = true)

        case None =>
          throw new IllegalArgumentException(
            s"Join produced no results for range $range. Ensure that the input data is all present"
          )
      }
    }
  }

  override def run(metadata: MetaData, conf: NodeContent, maybeRange: Option[PartitionRange]): Unit = {
    require(maybeRange.isDefined, "Partition range must be defined for batch node runner")
    val range = maybeRange.get
    conf.getSetField match {
      case NodeContent._Fields.MONOLITH_JOIN =>
        runMonolithJoin(metadata, conf.getMonolithJoin, range)
      case NodeContent._Fields.GROUP_BY_UPLOAD =>
        runGroupByUpload(metadata, conf.getGroupByUpload, range)
      case NodeContent._Fields.GROUP_BY_BACKFILL =>
        logger.info(s"Running groupBy backfill for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        GroupBy.computeBackfill(
          conf.getGroupByBackfill.groupBy,
          range.end,
          tableUtils,
          overrideStartPartition = Option(range.start)
        )
        logger.info(s"Successfully completed groupBy backfill for '${metadata.name}'")
      case NodeContent._Fields.STAGING_QUERY =>
        runStagingQuery(metadata, conf.getStagingQuery, range)
      case NodeContent._Fields.EXTERNAL_SOURCE_SENSOR => {

        checkPartitions(conf.getExternalSourceSensor, range) match {
          case Success(_) => System.exit(0)
          case Failure(exception) =>
            logger.error(s"ExternalSourceSensor check failed.", exception)
            System.exit(1)
        }
      }
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported NodeContent type: ${conf.getSetField}")
    }
  }

  def runFromArgs(
      api: Api,
      startDs: String,
      endDs: String,
      tablePartitionsDataset: String
  ): Try[Unit] = {
    Try {
      val metadata = node.metaData
      val range = PartitionRange(startDs, endDs)(PartitionSpec.daily)
//      val kvStore = api.genKvStore

      logger.info(s"Starting batch node runner for '${metadata.name}'")
      val inputTablesToRange = Option(metadata.executionInfo.getTableDependencies)
        .map(_.asScala.toArray)
        .getOrElse(Array.empty)
        .map((td) => {
          val inputPartSpec =
            Option(td.getTableInfo).map(_.partitionSpec(tableUtils.partitionSpec)).getOrElse(tableUtils.partitionSpec)
          td.getTableInfo.table -> DependencyResolver.computeInputRange(range, td).map(_.translate(inputPartSpec))
        })
        .toMap
      val allInputTablePartitions = inputTablesToRange.map {
        case (tableName, maybePartitionRange) => {
          // The partitions returned here are going to follow the tableUtils.partitionSpec default spec.
          tableName -> tableUtils.partitions(tableName, tablePartitionSpec = maybePartitionRange.map(_.partitionSpec))
        }
      }

      val maybeMissingPartitions = inputTablesToRange.map {
        case (tableName, maybePartitionRange) => {
          tableName -> maybePartitionRange.map((requestedPR) => {
            // Need to normalize back again to the default spec before diffing against the existing partitions.
            try {
              requestedPR.translate(tableUtils.partitionSpec).partitions.diff(allInputTablePartitions(tableName))
            } catch {
              case e: Exception =>
                logger.error(s"Error computing missing partitions for table $tableName.")
                throw e
            }
          })
        }
      }
//      val kvStoreUpdates = kvStore.multiPut(allInputTablePartitions.map { case (tableName, allPartitions) =>
//        val partitionsJson = PartitionRange.collapsedPrint(allPartitions)(range.partitionSpec)
//        PutRequest(tableName.getBytes, partitionsJson.getBytes, tablePartitionsDataset)
//      }.toSeq)
//
//      Await.result(kvStoreUpdates, Duration.Inf)

      val missingPartitions = maybeMissingPartitions.collect {
        case (tableName, Some(missing)) if missing.nonEmpty =>
          tableName -> missing
      }
      if (missingPartitions.nonEmpty) {
        throw new RuntimeException(
          "The following input tables are missing partitions for the requested range:\n" +
            missingPartitions
              .map { case (tableName, missing) =>
                s"Table: $tableName, Missing Partitions: ${missing.mkString(", ")}"
              }
              .mkString("\n")
        )
      } else {
        run(metadata, node.content, Option(range))
//        val outputTablePartitionSpec = (for {
//          meta <- Option(metadata)
//          executionInfo <- Option(meta.executionInfo)
//          outputTableInfo <- Option(executionInfo.outputTableInfo)
//          definedSpec = outputTableInfo.partitionSpec(tableUtils.partitionSpec)
//        } yield definedSpec).getOrElse(tableUtils.partitionSpec)
//        val allOutputTablePartitions = tableUtils.partitions(metadata.executionInfo.outputTableInfo.table,
//                                                             tablePartitionSpec = Option(outputTablePartitionSpec))
//
//        val outputTablePartitionsJson = PartitionRange.collapsedPrint(allOutputTablePartitions)(range.partitionSpec)
//        val putRequest = PutRequest(metadata.executionInfo.outputTableInfo.table.getBytes,
//                                    outputTablePartitionsJson.getBytes,
//                                    tablePartitionsDataset)
//        val kvStoreUpdates = kvStore.put(putRequest)
//        Await.result(kvStoreUpdates, Duration.Inf)
        logger.info(s"Successfully completed batch node runner for '${metadata.name}'")
      }
    }
  }
}

object BatchNodeRunner {

  def main(args: Array[String]): Unit = {
    val batchArgs = new BatchNodeRunnerArgs(args)
    val node = ThriftJsonCodec.fromJsonFile[Node](batchArgs.confPath(), check = false)
    val tableUtils = TableUtils(SparkSessionBuilder.build(s"batch-node-runner-${node.metaData.name}"))
    val runner = new BatchNodeRunner(node, tableUtils)
    val api = instantiateApi(batchArgs.onlineClass(), batchArgs.apiProps)
    val exitCode = {
      runner.runFromArgs(api, batchArgs.startDs(), batchArgs.endDs(), batchArgs.tablePartitionsDataset()) match {
        case Success(_) =>
          println("Batch node runner succeeded")
          0
        case Failure(exception) =>
          println(s"Batch node runner failed: ${exception.traceString}")
          1
      }
    }
    tableUtils.sparkSession.stop()
    System.exit(exitCode)
  }

  def instantiateApi(onlineClass: String, props: Map[String, String]): Api = {
    val cl = Thread.currentThread().getContextClassLoader
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props)
    onlineImpl.asInstanceOf[Api]
  }
}
