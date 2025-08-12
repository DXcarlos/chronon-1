package ai.chronon.api.planner

import ai.chronon.api._
import ai.chronon.planner.Node

object LocalRunner {

  case class StepStatus(nodeHash: String,
                        rangeOpt: Option[PartitionRange],
                        jobStatus: String,
                        jobInfo: Map[String, String])

  case class NodeStatus(steps: Seq[StepStatus], partitions: Seq[String])

  trait JobExecutor {
    def run(id: String, node: Node, rangeOpt: Option[PartitionRange])

    def track()

  }

  /** To run:
    * bazel build //api:planner_deploy.jar
    * bazel run -- //api:planner <path-to-confs> <conf_type>
    * @param args
    */
  def main(args: Array[String]): Unit = {
    implicit val testPartitionSpec = PartitionSpec.daily
    // TODO invoke buildIndex
  }

}
