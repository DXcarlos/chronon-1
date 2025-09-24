package ai.chronon.flink_connectors.pubsub

import ai.chronon.flink.source.FlinkSourceProvider
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class PubSubFlinkSourceSpec extends AnyFlatSpec {
  it should "fail on missing gcp project id" in {
    val params = Map(
      PubSubFlinkSource.SubscriptionName -> "test-subscription"
    )
    val topicInfo = TopicInfo("test-topic", "pubsub", params)
    assertThrows[Exception] {
      FlinkSourceProvider.build(Map.empty, null, topicInfo)
    }
  }

  it should "fail on missing subscription name" in {
    val props = Map(
      PubSubFlinkSource.GcpProject -> "my-gcp-project"
    )
    val topicInfo = TopicInfo("test-topic", "pubsub", Map.empty)
    assertThrows[Exception] {
      FlinkSourceProvider.build(props, null, topicInfo)
    }
  }

  it should "choose default parallelism" in {
    val props = Map(
      PubSubFlinkSource.GcpProject -> "my-gcp-project",
    )
    val params = Map(
      PubSubFlinkSource.SubscriptionName -> "test-subscription"
    )
    val topicInfo = TopicInfo("test-topic", "pubsub", params)
    val src = FlinkSourceProvider.build(props, null, topicInfo)
    src.parallelism shouldBe PubSubFlinkSource.DefaultParallelism
  }

  it should "override default parallelism" in {
    val props = Map(
      PubSubFlinkSource.GcpProject -> "my-gcp-project"
    )
    val params = Map(
      PubSubFlinkSource.TaskParallelism -> "5",
      PubSubFlinkSource.SubscriptionName -> "test-subscription"
    )
    val topicInfo = TopicInfo("test-topic", "pubsub", params)
    val src = FlinkSourceProvider.build(props, null, topicInfo)
    src.parallelism shouldBe 5
  }
}
