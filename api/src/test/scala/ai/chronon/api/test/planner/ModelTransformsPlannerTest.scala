package ai.chronon.api.test.planner

import ai.chronon.api.{Builders => B, _}
import ai.chronon.api.planner.ModelTransformsPlanner
import ai.chronon.planner.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ModelTransformsPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def buildModelTransforms(name: String, source: Source): ModelTransforms = {
    val testModel = B.Model(
      metaData = B.MetaData(name = s"${name}_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    B.ModelTransforms(
      metaData = B.MetaData(
        name = name,
        namespace = "test_namespace"
      ),
      sources = Seq(source),
      models = Seq(testModel)
    )
  }

  "ModelTransformsPlanner" should "create an upload node" in {
    val source = B.Source.events(
      query = B.Query(),
      table = "test_namespace.test_table"
    )

    val modelTransforms = buildModelTransforms("test_model_transforms", source)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    // Should create plan successfully with expected number of nodes
    plan.nodes.asScala should have size 1

    // Find the upload node
    val uploadNode = plan.nodes.asScala.find(_.content.isSetModelTransformsUpload)

    uploadNode should be(defined)

    // Upload node should have content
    uploadNode.get.content should not be null
    uploadNode.get.content.getModelTransformsUpload should not be null
    uploadNode.get.content.getModelTransformsUpload.modelTransforms should not be null

    // Verify metadata
    uploadNode.get.metaData.name should equal(s"${modelTransforms.metaData.name}__upload")

    // Verify no table dependencies for non-JoinSource sources
    val deps = uploadNode.get.metaData.executionInfo.tableDependencies.asScala
    deps should be(empty)

    // Verify terminal nodes
    plan.terminalNodeNames.asScala.size shouldBe 1
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.get(Mode.DEPLOY) shouldBe uploadNode.get.metaData.name
  }

  "ModelTransformsPlanner" should "handle model transforms with join source" in {
    val join = B.Join(
      left = B.Source.events(
        query = B.Query(),
        table = "test_namespace.events_table"
      ),
      joinParts = Seq.empty,
      metaData = B.MetaData(
        name = "test_join",
        namespace = "test_namespace"
      )
    )

    val joinSource = B.Source.joinSource(
      join = join,
      query = B.Query()
    )

    val modelTransforms = buildModelTransforms("test_model_transforms_with_join", joinSource)
    val planner = new ModelTransformsPlanner(modelTransforms)
    val plan = planner.buildPlan

    // Should create plan successfully
    plan.nodes.asScala should have size 1

    val uploadNode = plan.nodes.asScala.find(_.content.isSetModelTransformsUpload)
    uploadNode should be(defined)

    // Verify the node has dependencies (should include the upstream join metadata upload)
    val deps = uploadNode.get.metaData.executionInfo.tableDependencies.asScala
    deps should not be empty
  }
}
