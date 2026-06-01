package ai.chronon.service.handlers;

import ai.chronon.online.JavaFetcher;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GroupByListHandler implements Handler<RoutingContext> {

    private final JavaFetcher fetcher;

    public GroupByListHandler(JavaFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void handle(RoutingContext ctx) {
        CompletableFuture<List<String>> resultsJavaFuture = fetcher.listGroupBys(true);
        // wrap the Java future we get in a Vert.x Future to not block the worker thread
        Future<List<String>> maybeFeatureResponses = Future.fromCompletionStage(resultsJavaFuture);

        maybeFeatureResponses.onSuccess(
                resultList -> {
                    ctx.response()
                            .setStatusCode(200)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("groupByNames", resultList).encode());
                });

        maybeFeatureResponses.onFailure(
                err -> {

                    List<String> failureMessages = Collections.singletonList(err.getMessage());

                    ctx.response()
                            .setStatusCode(500)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("errors", failureMessages).encode());
                });
    }
}
