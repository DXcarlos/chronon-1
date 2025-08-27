package ai.chronon.spark.test.other

import ai.chronon.api
import ai.chronon.spark.submission.JobSubmitter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar


class JobSubmitterTest extends AnyFlatSpec with MockitoSugar {
  it should "test getArgValue" in {
    val args = Array("--arg1=value1", "--arg2=value2")
    val argKeyword = "--arg1"
    val result = JobSubmitter.getArgValue(args, argKeyword)
    assert(result.contains("value1"))
  }

  it should "successfully test parseConf" in {
    val resourcePath = getClass.getClassLoader.getResource("joins/team/example_join.v1").getPath
    JobSubmitter.parseConf[api.Join](resourcePath)
  }

  it should "test getModeConfigProperties with only common" in {
    val resourcePath = getClass.getClassLoader.getResource("group_bys/team/purchases_only_conf_common.v1").getPath
    
    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${resourcePath}",
        "--conf-type=group_bys",
        "--original-mode=backfill"
      ))
    assert(modeMap.get == Map("spark.chronon.partition.format" -> "yyyy-MM-dd"))
  }

  it should "test getModeConfigProperties with common and modeConfigs" in {
    val resourcePath = getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath
    
    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${resourcePath}",
        "--conf-type=group_bys",
        "--original-mode=backfill"
      ))
    assert(modeMap.get == Map("spark.dummy" -> "value"))
  }

  it should "test getModeConfigProperties without common or modeConfigs" in {
    val resourcePath = getClass.getClassLoader.getResource("group_bys/team/example_group_by.v1").getPath
    
    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${resourcePath}",
        "--conf-type=group_bys",
        "--original-mode=backfill"
      ))
    assert(modeMap.isEmpty)
  }

  it should "test getModeConfigProperties for a raw Metadata conf" in {
    val resourcePath = getClass.getClassLoader.getResource("teams_metadata/default_team_metadata").getPath
    
    val modeMap = JobSubmitter.getModeConfigProperties(
      Array(
        s"--local-conf-path=${resourcePath}",
        "--original-mode=metastore"
      ))
    assert(modeMap.isDefined)
    assert(
      modeMap.get == Map("spark.chronon.partition.format" -> "yyyy-MM-dd", "spark.chronon.partition.column" -> "_DATE"))
  }
}
