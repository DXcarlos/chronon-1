package ai.chronon.spark.batch

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, JListOps}
import ai.chronon.api.{Accuracy, DataModel, DateRange, MetaData, PartitionRange, PartitionSpec}
import ai.chronon.api.planner.{DependencyResolver, JoinPlanner}
import ai.chronon.planner.{
  JoinBootstrapNode,
  JoinDerivationNode,
  JoinMergeNode,
  JoinPartNode,
  Node,
  NodeContent,
  SourceWithFilterNode
}
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.catalog.TableUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.Seq

/** ModularMonolith orchestrates the join pipeline using discrete job classes from an api.Join object:
  * SourceJob -> JoinBootstrapJob -> JoinPartJob(s) -> MergeJob -> JoinDerivationJob
  *
  * Uses JoinPlanner to generate the node plan and executes nodes in topological order.
  */
class ModularMonolith(join: api.Join, dateRange: DateRange)(implicit tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: api.PartitionSpec = tableUtils.partitionSpec

  // Use JoinPlanner to generate the node plan
  private val planner = new JoinPlanner(join)
  private val nodes = planner.offlineNodes

  // Execute nodes in topological order (nodes are already ordered by planner)
  def run(): Unit = {
    logger.info(s"Starting ModularMonolith pipeline for join: ${join.metaData.name}")
    logger.info(s"Executing ${nodes.size} nodes in topological order")

    // DateRange is guaranteed to be in daily spec
    val queryRange = PartitionRange(dateRange)

    nodes.foreach { node =>
      runNode(node, queryRange)
    }

    logger.info(s"Completed ModularMonolith pipeline for join: ${join.metaData.name}")
  }

  private def computeNodeRange(node: Node, queryRange: PartitionRange): DateRange = {
    // Get the table dependencies from the node's metadata
    val tableDeps = Option(node.metaData.executionInfo)
      .flatMap(ei => Option(ei.tableDependencies))
      .map(_.toScala)
      .getOrElse(Seq.empty)

    if (tableDeps.isEmpty) {
      // No dependencies, use the query range directly
      return new DateRange().setStartDate(queryRange.start).setEndDate(queryRange.end)
    }

    // Compute the input range needed for each dependency
    val inputRanges = tableDeps.flatMap { dep =>
      DependencyResolver.computeInputRange(queryRange, dep)
    }

    if (inputRanges.isEmpty) {
      // Fallback to query range
      return new DateRange().setStartDate(queryRange.start).setEndDate(queryRange.end)
    }

    // Take the union of all input ranges (earliest start, latest end)
    val start = inputRanges.map(_.start).min
    val end = inputRanges.map(_.end).max

    new DateRange().setStartDate(start).setEndDate(end)
  }

  private def runNode(node: Node, queryRange: PartitionRange): Unit = {
    val metadata = node.metaData
    val nodeType = node.content.getSetField

    logger.info(s"Running node: ${metadata.name} (type: $nodeType)")

    // Compute the appropriate date range for this node based on its dependencies
    val nodeRange = computeNodeRange(node, queryRange)
    logger.info(s"Node ${metadata.name} will run for range: [${nodeRange.startDate}, ${nodeRange.endDate}]")

    node.content.getSetField match {
      case NodeContent._Fields.SOURCE_WITH_FILTER =>
        runSourceJob(node.content.getSourceWithFilter, metadata, nodeRange)

      case NodeContent._Fields.JOIN_BOOTSTRAP =>
        runBootstrapJob(node.content.getJoinBootstrap, metadata, nodeRange)

      case NodeContent._Fields.JOIN_PART =>
        runJoinPartJob(node.content.getJoinPart, metadata, nodeRange)

      case NodeContent._Fields.JOIN_MERGE =>
        runMergeJob(node.content.getJoinMerge, metadata, nodeRange)

      case NodeContent._Fields.JOIN_DERIVATION =>
        runDerivationJob(node.content.getJoinDerivation, metadata, nodeRange)

      case NodeContent._Fields.GROUP_BY_BACKFILL =>
        logger.info(s"Skipping GroupBy backfill node (will be run separately): ${metadata.name}")

      case unsupported =>
        logger.warn(s"Skipping unsupported node type ${unsupported}: ${metadata.name}")
    }
  }

  private def runSourceJob(sourceNode: SourceWithFilterNode, metaData: MetaData, nodeRange: DateRange): Unit = {
    StepRunner(nodeRange, metaData) { stepRange =>
      val sourceJob = new SourceJob(sourceNode, metaData, stepRange)
      sourceJob.run()
    }
    logger.info(s"SourceJob completed, output table: ${metaData.outputTable}")
  }

  private def runBootstrapJob(bootstrapNode: JoinBootstrapNode, metaData: MetaData, nodeRange: DateRange): Unit = {
    StepRunner(nodeRange, metaData) { stepRange =>
      val bootstrapJob = new JoinBootstrapJob(bootstrapNode, metaData, stepRange)
      bootstrapJob.run()
    }
    logger.info(s"JoinBootstrapJob completed, output table: ${metaData.outputTable}")
  }

  private def runJoinPartJob(joinPartNode: JoinPartNode, metaData: MetaData, nodeRange: DateRange): Unit = {
    StepRunner(nodeRange, metaData) { stepRange =>
      val joinPartJob = new JoinPartJob(joinPartNode, metaData, stepRange, alignOutput = true)
      joinPartJob.run(None)
    }
    logger.info(s"JoinPartJob completed, output table: ${metaData.outputTable}")
  }

  private def runMergeJob(mergeNode: JoinMergeNode, metaData: MetaData, nodeRange: DateRange): Unit = {
    val joinParts = Option(mergeNode.join.joinParts).map(_.asScala.toSeq).getOrElse(Seq.empty)

    StepRunner(nodeRange, metaData) { stepRange =>
      val mergeJob = new MergeJob(mergeNode, metaData, stepRange, joinParts)
      mergeJob.run()
    }
    logger.info(s"MergeJob completed, output table: ${metaData.outputTable}")
  }

  private def runDerivationJob(derivationNode: JoinDerivationNode, metaData: MetaData, nodeRange: DateRange): Unit = {
    StepRunner(nodeRange, metaData) { stepRange =>
      val derivationJob = new JoinDerivationJob(derivationNode, metaData, stepRange)
      derivationJob.run()
    }
    logger.info(s"JoinDerivationJob completed, output table: ${metaData.outputTable}")
  }
}

object ModularMonolith {

  def run(join: api.Join, dateRange: DateRange)(implicit tableUtils: TableUtils): Unit = {
    new ModularMonolith(join, dateRange).run()
  }
}
