package ai.chronon.service.handlers;

import ai.chronon.api.Constants;
import ai.chronon.online.Api;
import ai.chronon.online.JavaStatsResponse;
import ai.chronon.online.JavaStatsService;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

/**
 * Handler for fetching enhanced statistics from KV Store.
 *
 * Endpoint: GET /v1/stats/:tableName
 * Query parameters:
 *   - startTime: Start time in milliseconds (required)
 *   - endTime: End time in milliseconds (required)
 *   - dataset: Dataset name (optional, defaults to ENHANCED_STATS)
 */
public class StatsHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(StatsHandler.class);
    private final Api api;
    private final ExecutionContext ec = ExecutionContext$.MODULE$.global();

    public StatsHandler(Api api) {
        this.api = api;
    }

    @Override
    public void handle(RoutingContext ctx) {
        // Extract path parameter
        String tableName = ctx.pathParam("tableName");
        if (tableName == null || tableName.trim().isEmpty()) {
            sendError(ctx, 400, "Missing required path parameter: tableName");
            return;
        }

        // Extract query parameters
        String startTimeStr = ctx.request().getParam("startTime");
        String endTimeStr = ctx.request().getParam("endTime");
        String datasetName = ctx.request().getParam("dataset");

        // Validate required parameters
        if (startTimeStr == null || endTimeStr == null) {
            sendError(ctx, 400, "Missing required query parameters: startTime and endTime");
            return;
        }

        long startTimeMillis;
        long endTimeMillis;
        try {
            startTimeMillis = Long.parseLong(startTimeStr);
            endTimeMillis = Long.parseLong(endTimeStr);
        } catch (NumberFormatException e) {
            sendError(ctx, 400, "Invalid time parameters: startTime and endTime must be valid long values");
            return;
        }

        // Validate time range
        if (startTimeMillis > endTimeMillis) {
            sendError(ctx, 400, "Invalid time range: startTime must be less than or equal to endTime");
            return;
        }

        // Use default dataset if not provided
        if (datasetName == null || datasetName.trim().isEmpty()) {
            datasetName = Constants.EnhancedStatsDataset();
        }

        logger.info("Fetching stats for table: {}, timeRange: [{}, {}], dataset: {}",
                tableName, startTimeMillis, endTimeMillis, datasetName);

        // Create stats service and fetch stats
        // Use default table name "ENHANCED_STATS" for BigTable storage
        JavaStatsService statsService = new JavaStatsService(api, "ENHANCED_STATS", datasetName, ec);
        CompletableFuture<JavaStatsResponse> statsResponseFuture =
                statsService.fetchStats(tableName, startTimeMillis, endTimeMillis);

        // Convert Java future to Vert.x Future
        Future<JavaStatsResponse> vertxFuture = Future.fromCompletionStage(statsResponseFuture);

        vertxFuture.onSuccess(statsResponse -> {
            if (statsResponse.isSuccess()) {
                JsonObject responseJson = new JsonObject()
                        .put("success", true)
                        .put("tableName", statsResponse.getTableName())
                        .put("tilesCount", statsResponse.getTilesCount())
                        .put("statistics", new JsonObject(statsResponse.getStatistics()))
                        .put("timeRange", new JsonObject()
                                .put("startTime", startTimeMillis)
                                .put("endTime", endTimeMillis));

                ctx.response()
                        .setStatusCode(200)
                        .putHeader("content-type", "application/json")
                        .end(responseJson.encode());

                logger.info("Successfully fetched stats for table: {}, tiles: {}",
                        tableName, statsResponse.getTilesCount());
            } else {
                sendError(ctx, 500, statsResponse.getErrorMessage());
            }
        });

        vertxFuture.onFailure(err -> {
            logger.error("Failed to fetch stats for table: " + tableName, err);
            sendError(ctx, 500, "Failed to fetch statistics: " + err.getMessage());
        });
    }

    private void sendError(RoutingContext ctx, int statusCode, String message) {
        List<String> errors = Collections.singletonList(message);
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("success", false).put("errors", errors).encode());
    }
}
