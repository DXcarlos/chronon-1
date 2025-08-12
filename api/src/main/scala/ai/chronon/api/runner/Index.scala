package ai.chronon.api.runner

import ai.chronon.api.Extensions.{MetadataOps, TableInfoOps}
import ai.chronon.api.ScalaJavaConversions.{IteratorOps, JMapOps}
import ai.chronon.api.ThriftJsonCodec.fromJson
import ai.chronon.planner.{ConfPlan, ExternalSourceSensorNode, Mode, Node, NodeContent}
import ai.chronon.api.planner.{ConfPlanner, DependencyResolver, MetaDataUtils}
import ai.chronon.api.runner.Index.{NodeGraph, NodeWithRange}
import ai.chronon.api.thrift.TBase
import ai.chronon.api.{HashUtils, MetaData, PartitionRange, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.orchestration.ConfType

import java.util
import scala.collection.mutable
import scala.io.Source.fromFile
import scala.reflect.ClassTag

case class Index(confs: Array[ConfEntry], nodes: Array[NodeEntry]) {

  private val confNameToEntry: Map[String, ConfEntry] = confs.map { conf =>
    conf.confName -> conf
  }.toMap

  private val outputTableToNode: Map[String, NodeEntry] = {
    nodes.map(node => node.outputTable -> node).toMap
  }

  private val nameToNode: Map[String, NodeEntry] = {
    nodes.map(node => node.nodeName -> node).toMap
  }

  def buildNodeGraph(confName: String, mode: Mode, rangeOpt: Option[PartitionRange]): NodeGraph = {

    val confEntry = confNameToEntry(confName)
    val confPlan = confEntry.confPlan
    val terminalNodeName = confPlan.terminalNodeNames.get(mode)

    val nodeWithRange = NodeWithRange(terminalNodeName, rangeOpt)
    val nodePlan = NodeGraph(mutable.HashMap.empty, Seq(nodeWithRange))

    traverseParentsAndUpdate(nodeWithRange, nodePlan)
    nodePlan
  }

  private val sensorJobConf: Map[String, String] = Map(
    "spark.driver.memory" -> "1G",
    "spark.executor.memory" -> "1G",
    "spark.executor.cores" -> "1",
    "spark.dynamicAllocation.maxExecutors" -> "2",
    "spark.dynamicAllocation.enabled" -> "true"
  )

  // if a node produces a table, we will simply return that
  // otherwise, it must be an external sensor
  // we will borrow the conf from child, change the cluster params to be tiny: 1G driver + 1G executor, 1 core
  private def findParentNodeOrCreateSensor(tableInfo: TableInfo, child: NodeEntry): NodeEntry = {

    // if it is computed from a node, return it
    val tableName = tableInfo.table
    val existingNode = outputTableToNode.get(tableName)
    if (existingNode.isDefined) return existingNode.get

    // otherwise create a sensor node that points to the metadata cluster
    implicit val partitionSpec: PartitionSpec = tableInfo.partitionSpec(PartitionSpec.daily)

    // remove any preset cluster creation configs - we don't want to create a cluster
    // it should go into a pre-existing cluster
    val childMetadata = child.node.metaData.deepCopy()
    childMetadata.executionInfo.unsetClusterConf()

    val backfillConf = childMetadata.executionInfo.conf.modeConfigs
      .putIfAbsent("backfill", new util.HashMap[String, String]())

    // sensor job should be small and cheap and pointed to a long-running cluster
    backfillConf.putAll(sensorJobConf.toJava)

    val metadata = MetaDataUtils.layer(
      baseMetadata = child.node.metaData,
      modeName = "backfill",
      nodeName = tableName + "__sensor",
      tableDependencies = Seq(),
      stepDays = Some(1),
      outputTableOverride = Some(tableName)
    )

    require(
      metadata.outputTable == tableName,
      s"Metadata construction logic issue. " +
        s"Output table name doesn't match the sensor name ${metadata.outputTable} != $tableName"
    )

    val externalNode = new ExternalSourceSensorNode()
    externalNode.setSourceName(tableName)

    val externalContent = new NodeContent()
    externalContent.setExternalSourceSensor(externalNode)

    val node: Node = new Node().setContent(externalContent).setMetaData(metadata)

    NodeEntry(tableName, node.hashCode().toHexString, node)
  }

  private def traverseParentsAndUpdate(nodeWithRange: NodeWithRange, plan: NodeGraph): Unit = {

    val name = nodeWithRange.nodeName
    val nodeEntry = nameToNode(name)
    val tableDeps = nodeEntry.dependencies
    val rangeOpt = nodeWithRange.range

    tableDeps.foreach { dep =>
      val tableInfo = dep.getTableInfo

      val producingNode = findParentNodeOrCreateSensor(tableInfo, nodeEntry)

      val inputRangeOpt = rangeOpt.flatMap(range => DependencyResolver.computeInputRange(range, dep))
      val parentNodeWithRange = NodeWithRange(producingNode.nodeName, inputRangeOpt)

      val parentsBuffer = plan.nodeToParent.getOrElseUpdate(name, mutable.Buffer.empty)
      parentsBuffer.append(parentNodeWithRange)

      // recurse up
      traverseParentsAndUpdate(parentNodeWithRange, plan)
    }
  }
}

object Index {

  case class NodeWithRange(nodeName: String, range: Option[PartitionRange])

  case class NodeGraph(nodeToParent: mutable.Map[String, mutable.Buffer[NodeWithRange]],
                       terminalNodeRanges: Seq[NodeWithRange])

  case class ConfEntry(confName: String,
                       confHash: String,
                       confType: ConfType,
                       confPlan: ConfPlan,
                       metadata: MetaData,
                       content: String) {
    def nodes: Array[Node] = confPlan.getNodes.iterator().toScala.toArray
  }

  object ConfEntry {
    def apply[T <: TBase[_, _]: Manifest: ClassTag](path: String,
                                                    confType: ConfType,
                                                    metadataFunc: T => MetaData,
                                                    planner: T => ConfPlanner[T]): ConfEntry = {
      val src = fromFile(path)
      val content =
        try src.mkString
        finally src.close()
      val conf = fromJson[T](content, check = false)
      val confPlan = planner(conf).buildPlan
      val confHash = HashUtils.md5Hex(content)
      val metadata = metadataFunc(conf)
      ConfEntry(metadata.name, confHash, confType, confPlan, metadata, content)
    }
  }

  case class NodeEntry(nodeName: String, nodeHash: String, node: Node) {
    def outputTable: String = node.metaData.outputTable
    def dependencies: Array[TableDependency] =
      node.metaData.executionInfo.getTableDependencies.iterator().toScala.toArray
  }
}
