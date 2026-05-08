package ai.chronon.online.fetcher

import ai.chronon.api
import ai.chronon.api.{DataType, Model, StructType}
import ai.chronon.api.Extensions.{MetadataOps, ThrowableOps}
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.metrics.Metrics
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class InferenceFetcher(modelPlatformProvider: ModelPlatformProvider, debug: Boolean = false)(implicit
    executionContext: ExecutionContext) {

  require(modelPlatformProvider != null, "ModelPlatformProvider is required for InferenceFetcher")

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def fetchJoinSourceInference(requests: Seq[Request],
                                     inference: api.Inference,
                                     joinResponses: Seq[Response]): Future[Seq[Response]] = {
    // We tack on indices to help collate final responses in the original order
    val requestResponseWithIndex = requests.zipWithIndex.zip(joinResponses).map { case ((request, index), response) =>
      (request, response, index)
    }
    val (failedJoinResponses, successfulJoinResponses) = requestResponseWithIndex.partition(_._2.values.isFailure)

    if (debug) {
      logger.info(
        s"Kicking off inference for ${successfulJoinResponses.size} successful join responses and " +
          s"${failedJoinResponses.size} failed join responses out of total ${requests.size} requests")
    }

    // Make bulk inference call for successful join calls
    val successfulJoinModelLookupRequests = successfulJoinResponses.map { case (originalRequest, joinResponse, _) =>
      val joinFeatures = joinResponse.values.get
      // merge original request keys with join features to enable passthrough step later
      val mergedKeys = originalRequest.keys ++ joinFeatures
      originalRequest.copy(keys = mergedKeys)
    }

    val inferenceFuture = fetchInference(successfulJoinModelLookupRequests, inference)

    inferenceFuture.map { inferenceResponses =>
      val finalResponses = Array.ofDim[Response](requests.length)

      // Fill in failure responses directly by index
      failedJoinResponses.foreach { case (originalRequest, joinResponse, index) =>
        finalResponses(index) = Response(originalRequest, joinResponse.values)
      }

      // Fill in success responses also by index
      inferenceResponses.zip(successfulJoinResponses).foreach {
        case (modelResponse, (originalRequest, _, originalIndex)) =>
          // Preserve original request keys in the final response, but keep the model response values
          finalResponses(originalIndex) = Response(originalRequest, modelResponse.values)
      }

      finalResponses.toSeq
    }
  }

  def fetchInference(requests: scala.Seq[Request],
                           inference: api.Inference): Future[scala.Seq[Response]] = {
    if (requests.isEmpty) {
      return Future.successful(Seq.empty)
    }

    val models = Option(inference.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    if (models.isEmpty) {
      if (debug) {
        logger.info(
          s"No models defined in inference: ${inference.metaData.name}, returning passthrough only")
      }
      return Future.successful(requests.map(createPassthroughResponse(_, inference)))
    }

    val ts = System.currentTimeMillis()
    val ctx =
      Metrics.Context(Metrics.Environment.InferenceFetching, inference = inference.metaData.name)

    processBulkInference(requests, models, inference, ctx, ts)
  }

  private def createPassthroughResponse(request: Request, inference: api.Inference): Response = {
    val passthroughData = extractPassthroughFields(request.keys, inference.passthrough)
    Response(request, Success(passthroughData))
  }

  private def processBulkInference(requests: Seq[Request],
                                         models: Seq[api.Model],
                                         inference: api.Inference,
                                         ctx: Metrics.Context,
                                         ts: Long): Future[Seq[Response]] = {
    // Extract keySchema once for all requests
    val keySchema = Option(inference.keySchema).map(api.DataType.fromTDataType)
    if (debug && keySchema.isDefined) {
      logger.info(s"Derived ${inference.metaData.name}'s keySchema = ${keySchema.get}")
    }

    val modelResultFutures = models.map { model =>
      processBulkModelInfer(requests, model, keySchema)
        .recover { case exception =>
          ctx.incrementException(exception)
          logger.error(s"Model ${model.metaData.name} failed, returning error features", exception)
          // Return error features for this model instead of failing entire request
          requests.map { request =>
            val errorFeatures =
              Map(s"${model.metaData.name}${FetcherUtil.FeatureExceptionSuffix}" -> exception.traceString)
            request -> errorFeatures
          }
        }
    }

    Future.sequence(modelResultFutures).map { allModelResults =>
      // We have a Seq[(Request, Map[String, Any])] per model (so a Seq[Seq[...]] )
      // We grab iterators for each of the model result sequences and zip through them in lockstep to compute our
      // merged results per request
      val modelResultIterators = allModelResults.map(_.iterator)
      val mergedResults = requests.map { request =>
        val mergedModelOutputs = modelResultIterators
          .map { iterator =>
            val (iterRequest, modelOutput) = iterator.next()
            assert(iterRequest == request, s"Request mismatch: expected $request, got $iterRequest")
            modelOutput
          }
          .foldLeft(Map.empty[String, AnyRef])(_ ++ _)

        val passthroughData = extractPassthroughFields(request.keys, inference.passthrough)
        val combinedResults = mergedModelOutputs ++ passthroughData

        logFeatureMetrics(Response(request, Success(combinedResults)), ctx, ts)
      }
      mergedResults
    }
  }

  private def processBulkModelInfer(requests: Seq[Request],
                                    model: api.Model,
                                    keySchema: Option[api.DataType]): Future[Seq[(Request, Map[String, AnyRef])]] = {

    val ctx = Metrics.Context(Metrics.Environment.ModelInfer, model = model.metaData.name)
    val modelPreprocessStartTime = System.currentTimeMillis()

    try {
      // Step 1: Apply input mapping to all requests and collect transformed inputs
      val transformedRequestsWithInputs = requests.map { request =>
        val inputData = request.keys
        val transformedInput =
          applyMapping(model.inputs, inputData, keySchema, s"input_mapping_${model.metaData.name}")
        if (debug) {
          logger.info(
            s"Model ${model.metaData.name} input mapping: original = $inputData, transformed = $transformedInput")
        }
        (request, transformedInput)
      }

      // Step 2: Dedupe by transformed input - group requests by their transformed input
      val inputToRequests: Seq[(Map[String, AnyRef], Seq[Request])] = transformedRequestsWithInputs
        .groupBy(_._2) // Group by transformedInput
        .map { case (transformedInput, requestInputPairs) =>
          (transformedInput, requestInputPairs.map(_._1)) // Extract just the requests
        }
        .toSeq

      // Step 3: Time to call model inference
      val runtime = model.runtime
      if (runtime == null) {
        throw new IllegalArgumentException(s"Model ${model.metaData.name} missing inference specification")
      }

      val modelPlatform = modelPlatformProvider.getPlatform(
        runtime.backend,
        Option(runtime.params).map(_.asScala.toMap).getOrElse(Map.empty)
      )

      // Create bulk inference request with all unique inputs
      val allUniqueInputs = inputToRequests.map(_._1)
      val bulkInferRequest = InferRequest(model, allUniqueInputs)

      val modelStartTime = System.currentTimeMillis()
      val inferFuture = modelPlatform.infer(bulkInferRequest)

      // Step 4: Apply output mapping and map results back to original requests
      inferFuture.map { inferResponse =>
        ctx.distribution("model_preprocess.latency.millis", modelStartTime - modelPreprocessStartTime)
        ctx.distribution("model_inference.latency.millis", System.currentTimeMillis() - modelStartTime)
        ctx.count("model_inference.bulk_requests.count", allUniqueInputs.size)

        val inputToResult = inferResponse.outputs match {
          case Success(modelOutputs) =>
            if (modelOutputs.size != allUniqueInputs.size) {
              throw new RuntimeException(
                s"Model ${model.metaData.name} returned ${modelOutputs.size} outputs but expected ${allUniqueInputs.size}")
            }

            val inputToOutput = allUniqueInputs.zip(modelOutputs).toMap

            // Create map from transformed input to result for quick lookup
            inputToRequests.map { case (transformedInput, _) =>
              val modelOutput = inputToOutput(transformedInput)

              val postProcessStartTime = System.currentTimeMillis()

              // Prefix raw model output keys with model name to match InferenceJob behavior
              val modelName = model.metaData.cleanName
              val prefixedModelOutput = modelOutput.map { case (k, v) => s"${modelName}__$k" -> v }

              val prefixedValueSchema = computePrefixedValueSchema(model, modelName)
              val mappedResults = applyMapping(model.outputs,
                                               prefixedModelOutput,
                                               prefixedValueSchema,
                                               s"output_mapping_${model.metaData.name}")

              // Prefix the output mapping result keys as well
              // Only prefix if there was an output mapping, otherwise the keys are already prefixed
              val hasOutputMapping = Option(model.outputs).exists(!_.isEmpty)
              val modelResults = if (hasOutputMapping) {
                mappedResults.map { case (k, v) => s"${modelName}__$k" -> v }
              } else {
                mappedResults
              }

              if (debug) {
                logger.info(
                  s"Model ${model.metaData.name} output mapping. Value schema: $prefixedValueSchema\n model_output = $modelOutput\n " +
                    s"prefixed_output = $prefixedModelOutput\n mapped_output = $mappedResults\n final_output = $modelResults")
              }
              ctx.distribution("model_postprocess.latency.millis", System.currentTimeMillis() - postProcessStartTime)

              transformedInput -> modelResults
            }.toMap

          case Failure(exception) =>
            ctx.incrementException(exception)
            throw new RuntimeException(s"Model inference failed for ${model.metaData.name}", exception)
        }

        // Map results back to original requests in their original order
        transformedRequestsWithInputs.map { case (request, transformedInput) =>
          request -> inputToResult(transformedInput)
        }
      }
    } catch {
      case exception: Throwable =>
        ctx.incrementException(exception)
        Future.failed(exception)
    }
  }

  // We prefix value schema fields with the sanitized model name to match the InferenceJob behavior
  // e.g. if model name is "my model", and value schema has field "score", we prefix to "my_model__score"
  // This is done to ensure no collisions in the final output when multiple models are used
  private def computePrefixedValueSchema(model: Model, modelName: String) = {
    val valueSchema = Option(model.outputSchema).map { schema =>
      DataType.fromTDataType(schema) match {
        case structType: StructType =>
          val prefixedFields = structType.fields.map { field =>
            field.copy(name = s"${modelName}__${field.name}")
          }
          api.StructType(s"${structType.name}_prefixed", prefixedFields)
        case other => other
      }
    }
    valueSchema
  }

  private def applyMapping(inputs: util.Map[String, String],
                           data: Map[String, AnyRef],
                           schema: Option[DataType],
                           context: String): Map[String, AnyRef] = {

    val mapping = Option(inputs).map(_.asScala.toMap).getOrElse(Map.empty)
    if (mapping.isEmpty) {
      data
    } else {
      applySqlMapping(mapping, data, context, schema)
    }
  }

  private def applySqlMapping(mapping: Map[String, String],
                              data: Map[String, AnyRef],
                              context: String,
                              schema: Option[api.DataType] = None): Map[String, AnyRef] = {
    val inputSchema = schema match {
      case Some(schema: api.StructType) => schema
      case Some(_) =>
        throw new IllegalArgumentException(s"schema must be a StructType for SQL mapping in $context")
      case None =>
        throw new IllegalArgumentException(s"schema is required for SQL mapping in $context")
    }

    val pooledCatalyst = new PooledCatalystUtil(mapping.toSeq, inputSchema)
    val results = pooledCatalyst.performSql(data.asInstanceOf[Map[String, Any]])

    if (results.nonEmpty) {
      results.head.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }
    } else {
      Map.empty[String, AnyRef]
    }
  }

  private def extractPassthroughFields(baseData: Map[String, AnyRef],
                                       passthroughFields: java.util.List[String]): Map[String, AnyRef] = {
    if (passthroughFields != null && !passthroughFields.isEmpty) {
      val fieldsToPassthrough = passthroughFields.asScala.toSet
      baseData.filter { case (k, _) => fieldsToPassthrough.contains(k) }
    } else {
      Map.empty[String, AnyRef]
    }
  }

  private def logFeatureMetrics(resp: Response, ctx: Metrics.Context, ts: Long): Response = {
    ctx.distribution("model_transform.overall.latency.millis", System.currentTimeMillis() - ts)

    resp.values match {
      case Success(responseMap) =>
        Fetcher.logFeatureNullRates(responseMap, ctx)
        ctx.distribution("model_transform.feature_count", responseMap.size)
      case Failure(exception) =>
        ctx.incrementException(exception)
    }

    resp
  }
}
