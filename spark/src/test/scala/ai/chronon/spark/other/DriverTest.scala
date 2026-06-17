package ai.chronon.spark.other

import ai.chronon.api.{ConfigProperties, ExecutionInfo, GroupBy, MetaData}
import ai.chronon.spark.Driver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class DriverTest extends AnyFlatSpec with Matchers {

  "GroupByUploadToKVBulkLoad" should "merge common and upload config into API props" in {
    val commonConf = Map(
      "spark.chronon.table_write.upload.location" -> "s3://common/upload",
      "spark.chronon.partition.column" -> "ds",
      "common_only" -> "from_common",
      "explicit_override" -> "from_common"
    ).asJava
    val uploadConf = Map(
      "spark.chronon.table_write.upload.location" -> "s3://upload-mode/upload",
      "spark.chronon.table_writer.ion_writer.timeout_ms" -> "3600000",
      "upload_only" -> "from_upload"
    ).asJava
    val config = new ConfigProperties()
      .setCommon(commonConf)
      .setModeConfigs(Map("upload" -> uploadConf).asJava)
    val metadata = new MetaData()
      .setName("test_group_by")
      .setExecutionInfo(new ExecutionInfo().setConf(config))
    val groupBy = new GroupBy().setMetaData(metadata)

    val merged = Driver.GroupByUploadToKVBulkLoad.uploadApiProps(
      groupBy,
      Map("explicit_override" -> "from_cli", "UPLOADER" -> "bigquery")
    )

    merged("spark.chronon.table_write.upload.location") shouldBe "s3://upload-mode/upload"
    merged("spark.chronon.partition.column") shouldBe "ds"
    merged("spark.chronon.table_writer.ion_writer.timeout_ms") shouldBe "3600000"
    merged("common_only") shouldBe "from_common"
    merged("upload_only") shouldBe "from_upload"
    merged("explicit_override") shouldBe "from_cli"
    merged("UPLOADER") shouldBe "bigquery"
  }
}
