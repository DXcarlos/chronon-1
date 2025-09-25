package ai.chronon.service.handlers;

import ai.chronon.online.*;
import ai.chronon.service.model.GetFeaturesResponse;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

// Configures the routes for our get features endpoints
// We support bulkGets of groupBys and bulkGets of joins
public class FetchRouterV2 {

    public static class JoinFetcherAvroStringFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchJoinV2WithAvroString(requests);
        }
    }

    public static class AvroStringOnSuccessFunction implements BiFunction<List<GetFeaturesResponse.Result>, RoutingContext, Void> {
        @Override
        public Void apply(List<GetFeaturesResponse.Result> resultList, RoutingContext ctx) {
            GetFeaturesResponse.Builder responseBuilder = GetFeaturesResponse.builder();
            responseBuilder.results(resultList);

            ctx.response()
                    .setStatusCode(200)
                    .putHeader("content-type", "application/json")
                    .end(JsonObject.mapFrom(responseBuilder.build()).encode());
            return null;
        }
    }

    public static Router createFetchRoutes(Vertx vertx, JavaFetcher fetcher) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/join/avrostring/:name").handler(new FetchHandlerV2(fetcher, new JoinFetcherAvroStringFunction(), new AvroStringOnSuccessFunction()));

        return router;
    }
}