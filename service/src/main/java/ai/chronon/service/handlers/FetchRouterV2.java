package ai.chronon.service.handlers;

import ai.chronon.online.*;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
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

    public static class JoinFetcherAvroBytesFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchJoinV2WithAvroBytes(requests);
        }
    }

    public static Router createFetchRoutes(Vertx vertx, JavaFetcher fetcher) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/join/avrostring/:name").handler(new FetchHandlerV2(fetcher, new JoinFetcherAvroStringFunction()));
        router.post("/join/avrobytes/:name").handler(new FetchHandlerV2(fetcher, new JoinFetcherAvroBytesFunction()));

        return router;
    }
}
