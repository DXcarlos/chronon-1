package ai.chronon.api.planner

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.api.ThriftJsonCodec.fromJson
import ai.chronon.api._
import ai.chronon.api.thrift.TBase
import ai.chronon.orchestration.ConfType
import ai.chronon.planner.{ConfPlan, ExternalSourceSensorNode, Node, NodeContent}

import java.io.File
import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray
import scala.io.Source.fromFile
import scala.reflect.ClassTag

object LocalRunner {

  private def listFiles(dir: String = "."): ParArray[String] = {
    val baseDir = new File(dir)
    Option(baseDir.listFiles).getOrElse(Array()).flatMap { file =>
      if (file.isDirectory) listFiles(file.getPath)
      else Seq(file.getPath.replaceFirst("^\\./", ""))
    }.filterNot(isIgnorableFile).par
  }

  private def isIgnorableFile(path: String): Boolean = {
    val file = new File(path)
    Constants.extensionsToIgnore.exists(file.getName.endsWith) ||
    Constants.foldersToIgnore.exists(file.getPath.split("/").contains(_))
  }

  def parseConfs[T <: TBase[_, _]: Manifest: ClassTag](confSubfolder: String): Seq[T] = listFiles(confSubfolder)
    .filterNot(isIgnorableFile)
    .map(ThriftJsonCodec.fromJsonFile(_, check = true))
    .seq


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
    def dependencies: Array[TableDependency] = node.metaData.executionInfo.getTableDependencies.iterator().toScala.toArray
  }

  case class NodeWithRange(nodeName: String, range: Option[PartitionRange])

  case class NodeGraph(nodeToParent: mutable.Map[String, mutable.Buffer[NodeWithRange]], terminalNodeRanges: Seq[NodeWithRange])


  def buildIndex(compiledDir: String)(implicit partitionSpec: PartitionSpec): Index = {

    val joinEntries = listFiles(compiledDir + "/joins").map { file =>
      ConfEntry[Join](file, ConfType.JOIN, {join: Join => join.metaData}, MonolithJoinPlanner)
    }

    val stagingEntries = listFiles(compiledDir + "/staging_queries").map { file =>
      ConfEntry[StagingQuery](file, ConfType.STAGING_QUERY, {sq: StagingQuery => sq.metaData}, StagingQueryPlanner)
    }

    val groupByEntries = listFiles(compiledDir + "/group_bys").map { file =>
      ConfEntry[GroupBy](file, ConfType.GROUP_BY, {gb: GroupBy => gb.metaData}, GroupByPlanner.apply)
    }

    val confEntries: Array[ConfEntry] = (stagingEntries ++ groupByEntries ++ joinEntries).toArray

    val nodeEntries = confEntries
      .flatMap(_.nodes)
      .map(node => NodeEntry(node.metaData.name, ThriftJsonCodec.hexDigest(node), node))

    Index(confEntries, nodeEntries)
  }

  case class Index(confs: Array[ConfEntry], baseNodes: Array[NodeEntry]) {

    private val confNameToEntry: Map[String, ConfEntry] = confs.map{
        conf => conf.confName -> conf
      }.toMap

    val allNodes: Array[NodeEntry] = baseNodes ++ externalNodes

    private val outputTableToNode: Map[String, NodeEntry] = {
      allNodes.map(node => node.outputTable -> node).toMap
    }

    private val nameToNode: Map[String, NodeEntry] = {
      allNodes.map(node => node.nodeName -> node).toMap
    }

    def buildNodeGraph(confName: String, mode: Mode, rangeOpt: Option[PartitionRange]): NodeGraph = {

      val confEntry = confNameToEntry(confName)
      val confPlan = confEntry.confPlan
      val terminalNodeName = confPlan.terminalNodeNames.get(mode)

      val nodeWithRange = NodeWithRange(terminalNodeName, rangeOpt)
      val nodePlan = NodeGraph(mutable.HashMap.empty, Seq(nodeWithRange))

      traverseAndUpdate(nodeWithRange, nodePlan)
      nodePlan
    }

    private def traverseAndUpdate(nodeWithRange: NodeWithRange, plan: NodeGraph): Unit = {

      val name  = nodeWithRange.nodeName
      val nodeEntry = nameToNode(name)
      val tableDeps = nodeEntry.dependencies
      val rangeOpt = nodeWithRange.range

      tableDeps.foreach { dep =>

        val tableInfo = dep.getTableInfo
        val tableName = tableInfo.getTable

        val producingNode = outputTableToNode(tableName)

        val inputRangeOpt = rangeOpt.flatMap(range => DependencyResolver.computeInputRange(range, dep))
        val parentNodeWithRange = NodeWithRange(producingNode.nodeName, inputRangeOpt)

        val parentsBuffer = plan.nodeToParent.getOrElseUpdate(name, mutable.Buffer.empty)
        parentsBuffer.append(parentNodeWithRange)

        // recurse up
        traverseAndUpdate(parentNodeWithRange, plan)
      }
    }

    private def externalTables: Set[String] = {
      val outputTableToNode: Map[String, NodeEntry] = {
        baseNodes.map(node => node.outputTable -> node).toMap
      }
      val outputTables = outputTableToNode.keySet
      val inputTables = baseNodes.flatMap(_.dependencies).map(_.getTableInfo.getTable).toSet
      inputTables -- outputTables
    }

    private def externalNodes: Set[NodeEntry] = externalTables.map{ table =>

      val parts = table.split('.')
      val tableName = parts.last
      val namespace = parts.init.mkString(".")

      val metadata = new MetaData().setName(tableName).setOutputNamespace(namespace)

      require(
        metadata.outputTable == table,
        s"Metadata construction logic issue. " +
          s"Output table name doesn't match the sensor name ${metadata.outputTable} != $table"
      )

      val externalNode = new ExternalSourceSensorNode()
      externalNode.setSourceName(table)


      val externalContent = new NodeContent()
      externalContent.setExternalSourceSensor(externalNode)

      val node: Node = new Node().setContent(externalContent).setMetaData(metadata)
      NodeEntry(table, node.hashCode().toHexString, node)
    }
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
