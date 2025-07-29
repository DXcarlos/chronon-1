package ai.chronon.api.runner

import ai.chronon.api.PartitionRange
import ai.chronon.planner.Node

sealed trait JobStatus
object Running extends JobStatus
object Failed extends JobStatus
object Cancelled extends JobStatus
object Pending extends JobStatus

case class JobDetails(jobStatus: JobStatus,
                      clusterManagerUrl: String,
                      jobManagerUrl: String,
                      runTimeMillis: Long,
                      cost: Double)

trait JobExecutor {
  def submit(node: Node, range: Option[PartitionRange]): JobDetails

  def status(jobId: String): JobDetails

  def cancel(jobId: String): JobDetails
}
