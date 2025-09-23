package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.catalog.Format
import com.google.cloud.bigquery._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.{ExplainMode, FileSourceScanExec, ProjectExec, SimpleMode}
import org.apache.spark.sql.{Encoders, Row, SparkSession}

import scala.jdk.CollectionConverters._

case object BigQueryExternal extends Format {

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  lazy val bigQueryClient: BigQuery = bqOptions.getService

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)

  private[cloud_gcp] def partitions(tableName: String, partitionFilters: String, bqClient: BigQuery)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val btTableIdentifier = SparkBQUtils.toTableId(tableName)(sparkSession)
    val definition = scala
      .Option(bqClient.getTable(btTableIdentifier))
      .map((table) => table.getDefinition.asInstanceOf[ExternalTableDefinition])
      .getOrElse(throw new IllegalArgumentException(s"Table ${tableName} does not exist."))

    val formatOptions = definition.getFormatOptions.asInstanceOf[FormatOptions]

    val uri = scala
      .Option(definition.getHivePartitioningOptions)
      .map(_.getSourceUriPrefix)
      .getOrElse {
        val uris = definition.getSourceUris.asScala
        require(uris.size == 1, s"External table ${tableName} can be backed by only one URI.")
        uris.head.replaceAll("/\\*\\.parquet$", "")
      }

    /** Given:
      *  hdfs://<host>:<port>/ path/ to/ partition/ a=1/ b=hello/ c=3.14
      *  hdfs://<host>:<port>/ path/ to/ partition/ a=2/ b=world/ c=6.28
      *
      * it returns:
      * PartitionSpec(
      *  partitionColumns = StructType(
      *    StructField(name = "a", dataType = IntegerType, nullable = true),
      *    StructField(name = "b", dataType = StringType, nullable = true),
      *    StructField(name = "c", dataType = DoubleType, nullable = true)),
      *    partitions = Seq(
      *      Partition(
      *        values = Row(1, "hello", 3.14),
      *        path = "hdfs://<host>:<port>/ path/ to/ partition/ a=1/ b=hello/ c=3.14"),
      *      Partition(
      *        values = Row(2, "world", 6.28),
      *        path = "hdfs://<host>:<port>/ path/ to/ partition/ a=2/ b=world/ c=6.28")))
      */
    val df = sparkSession.read
      .format(formatOptions.getType)
      .load(uri)

    val finalDf = if (partitionFilters.isEmpty) {
      df
    } else {
      df.where(partitionFilters)
    }

    val plan = finalDf.queryExecution.sparkPlan
    val scanExecOpt = plan.find(_.isInstanceOf[FileSourceScanExec])

    scanExecOpt match {
      case Some(scanNode) =>
        extractPartitionsFromFileSourceScanExec(sparkSession, scanNode.asInstanceOf[FileSourceScanExec])
      case _ =>
        val explainString = finalDf.queryExecution.explainString(ExplainMode.fromString(SimpleMode.name))
        throw new IllegalStateException(s"Cannot extract partition columns from plan \n$explainString\n")
    }

  }

  private def extractPartitionsFromFileSourceScanExec(sparkSession: SparkSession, fse: FileSourceScanExec) = {
    val partitionSpec = fse.relation.location
      .asInstanceOf[PartitioningAwareFileIndex] // Punch through the layers!!
      .partitionSpec
    val partitionColumns = partitionSpec.partitionColumns
    val partitions = partitionSpec.partitions.map(_.values)
    val deserializer =
      try {
        Encoders.row(partitionColumns).asInstanceOf[ExpressionEncoder[Row]].resolveAndBind().createDeserializer()
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to create deserializer for partition columns: ${e.getMessage}", e)
      }

    val roundTripped = sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(partitions.map(deserializer)), partitionColumns)
      .collect

    roundTripped
      .map(part =>
        partitionColumns.fields.iterator.zipWithIndex.map { case (field, idx) =>
          val fieldName = field.name
          val fieldValue = part.get(idx)
          fieldName -> fieldValue.toString // Just going to cast this as a string.

        }.toMap)
      .toList
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    partitions(tableName, partitionFilters, bigQueryClient)
  }

  override def supportSubPartitionsFilter: Boolean = true

}
