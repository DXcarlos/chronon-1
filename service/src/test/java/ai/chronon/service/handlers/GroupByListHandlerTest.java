package ai.chronon.service.handlers;

import ai.chronon.online.JavaFetcher;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class GroupByListHandlerTest {
    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse response;

    private GroupByListHandler handler;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();

        handler = new GroupByListHandler(mockFetcher);

        // Set up common routing context behavior
        when(routingContext.response()).thenReturn(response);
        when(response.putHeader(anyString(), anyString())).thenReturn(response);
        when(response.setStatusCode(anyInt())).thenReturn(response);
    }

    @Test
    public void testSuccessfulRequest(TestContext context) {
        Async async = context.async();

        List<String> groupBys = List.of("my_group_bys.gb_a.v1", "my_group_bys.gb_a.v2", "my_group_bys.gb_b.v1");
        // Set up mocks
        CompletableFuture<List<String>> futureListResponse =
                CompletableFuture.completedFuture(groupBys);

        when(mockFetcher.listGroupBys(anyBoolean())).thenReturn(futureListResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(200);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            JsonObject actualResponse = new JsonObject(responseCaptor.getValue());
            JsonArray groupByNames = actualResponse.getJsonArray("groupByNames");
            context.assertEquals(groupByNames.size(), groupBys.size());
            for (int i = 0; i < groupByNames.size(); i++) {
                context.assertEquals(groupBys.get(i), groupByNames.getString(i));
            }
            async.complete();
        });
    }

    @Test
    public void testFailedFutureRequest(TestContext context) {
        Async async = context.async();

        // Set up mocks
        CompletableFuture<List<String>> futureResponse = new CompletableFuture<>();
        futureResponse.completeExceptionally(new RuntimeException("Error in KV store lookup"));

        when(mockFetcher.listGroupBys(anyBoolean())).thenReturn(futureResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(500);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            validateFailureResponse(responseCaptor.getValue(), context);
            async.complete();
        });
    }

    private void validateFailureResponse(String jsonResponse, TestContext context) {
        JsonObject actualResponse = new JsonObject(jsonResponse);
        context.assertTrue(actualResponse.containsKey("errors"));

        String failureString = actualResponse.getJsonArray("errors").getString(0);
        context.assertNotNull(failureString);
    }
}
