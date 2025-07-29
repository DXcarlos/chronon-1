package ai.chronon.api.runner

import ai.chronon.api.PartitionRange
import ai.chronon.api.planner.LocalRunner.NodeStatus

abstract class NodeManager(nodeName: String, jobExecutor: Any) {

  def accept(workflowId: String, node: NodeEntry, rangeOpt: Option[PartitionRange]): Unit = ???

  def cancel(workflowId: String): Unit = ???

  def updatePartitions(partitions: Seq[String],
                       forRange: Option[PartitionRange]): Unit = ???

  def status(rangeOpt: Option[PartitionRange]): NodeStatus = ???

  def status(workflowId: String): NodeStatus = ???

}


//class BatchNodeManager(nodeName: String, jobExecutor: Any) {
//
//
//  case class StepRun(nodeName: String = nodeName, nodeHash: String, partitionRange: PartitionRange)
//  val runningSteps: List[StepRun]
//
//  val finishedSteps:
//
//  def accept(workflowId: String, node: NodeEntry, rangeOpt: Option[PartitionRange]): Unit = ???
//
//  def cancel(workflowId: String): Unit = ???
//
//  def updatePartitions(partitions: Seq[String],
//                       forRange: Option[PartitionRange]): Unit = ???
//
//  def status(rangeOpt: Option[PartitionRange]): NodeStatus = ???
//
//  def status(workflowId: String): NodeStatus = ???
//}