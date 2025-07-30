package ai.chronon.flink.source

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

abstract class FlinkSource[T] extends Serializable {

  /** Parallelism to be used for the source. This is used to determine the number of tasks
    * at the source and downstream operators in the job.
    */
  implicit val parallelism: Int

  /** Return a Flink DataStream for the given topic and groupBy.
    *
    * When implementing a source, you should also make a conscious decision about your allowed lateness strategy.
    */
  def getDataStream(topic: String, groupByName: String)(
      env: StreamExecutionEnvironment,
      parallelism: Int
  ): SingleOutputStreamOperator[T]
}
