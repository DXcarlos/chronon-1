package ai.chronon.spark.test.submission

import ai.chronon.api
import ai.chronon.spark.submission.JobSubmitter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Paths

class JobSubmitterTest extends AnyFlatSpec with MockitoSugar {
  it should "test getArgValue" in {
    val args = Array("--arg1=value1", "--arg2=value2")
    val argKeyword = "--arg1"
    val result = JobSubmitter.getArgValue(args, argKeyword)
    assert(result.contains("value1"))
  }

  it should "successfully test parseConf" in {
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, "_main/spark/src/test/resources/joins/team/example_join.v1")

    JobSubmitter.parseConf[api.Join](path.toAbsolutePath.toString)
  }

  it should "test getModeConfigProperties with only common" in {

    val confPath = "_main/spark/src/test/resources/group_bys/team/purchases_only_conf_common.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${path.toAbsolutePath.toString}",
        "--conf-type=group_bys",
        "--original-mode=backfill"
      )
    )
    assert(modeMap.get == Map("spark.chronon.partition.format" -> "yyyy-MM-dd"))
  }

  it should "test getModeConfigProperties with common and modeConfigs" in {

    val confPath = "_main/spark/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${path.toAbsolutePath.toString}",
        "--conf-type=group_bys",
        "--original-mode=backfill"
      )
    )
    assert(modeMap.get == Map("spark.dummy" -> "value"))
  }

  it should "test getModeConfigProperties without common or modeConfigs" in {

    val confPath = "_main/spark/src/test/resources/group_bys/team/example_group_by.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${path.toAbsolutePath.toString}",
        "--conf-type=group_bys",
        "--original-mode=backfill"
      )
    )
    assert(modeMap.isEmpty)
  }

  it should "test getModeConfigProperties for a raw Metadata conf" in {
    val confPath = "_main/spark/src/test/resources/teams_metadata/default_team_metadata"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${path.toAbsolutePath.toString}",
        "--original-mode=metastore"
      )
    )
    assert(modeMap.isDefined)
    assert(
      modeMap.get == Map("spark.chronon.partition.format" -> "yyyy-MM-dd", "spark.chronon.partition.column" -> "_DATE")
    )
  }
}
