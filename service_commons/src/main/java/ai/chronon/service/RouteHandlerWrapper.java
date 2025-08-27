package ai.chronon.service;

import ai.chronon.api.thrift.*;
import ai.chronon.api.thrift.protocol.TBinaryProtocol;
import ai.chronon.api.thrift.protocol.TSimpleJSONProtocol;
import ai.chronon.api.thrift.transport.TTransportException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Wrapper class for creating Route handlers that map parameters to an Input
 * object and transform it to Output The wrapped handler produces a JSON
 * response. TODO: Add support for Thrift BinaryProtocol based serialization
 * based on a special request query param.
 */
public class RouteHandlerWrapper {

    public static String RESPONSE_CONTENT_TYPE_HEADER = "response-content-type";
    public static String TBINARY_B64_TYPE_VALUE = "application/tbinary-b64";
    public static String JSON_TYPE_VALUE = "application/json";

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteHandlerWrapper.class.getName());

    private static final ThreadLocal<TSerializer> binarySerializer = ThreadLocal.withInitial(() -> {
        try {
            return new TSerializer(new TBinaryProtocol.Factory());
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<TDeserializer> binaryDeSerializer = ThreadLocal.withInitial(() -> {
        try {
            return new TDeserializer(new TBinaryProtocol.Factory());
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<Base64.Encoder> base64Encoder = ThreadLocal.withInitial(Base64::getEncoder);
    private static final ThreadLocal<Base64.Decoder> base64Decoder = ThreadLocal.withInitial(Base64::getDecoder);

    public static <T extends TBase> T deserializeTBinaryBase64(String base64Data, Class<? extends TBase> clazz)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException,
            TException {
        byte[] binaryData = base64Decoder.get().decode(base64Data);
        T tb = (T) clazz.getDeclaredConstructor().newInstance();
        binaryDeSerializer.get().deserialize(tb, binaryData);
        return tb;
    }

    /**
     * Combines path parameters, query parameters, and JSON body into a single JSON
     * object. Returns the JSON object as a string.
     */
    public static String combinedParamJson(RoutingContext ctx) {
        JsonObject params = ctx.body().asJsonObject();
        if (params == null) {
            params = new JsonObject();
        }

        // Add path parameters
        for (Map.Entry<String, String> entry : ctx.pathParams().entrySet()) {
            params.put(entry.getKey(), entry.getValue());
        }

        // Add query parameters
        Map<String, List<String>> queryParamGroups = new HashMap<>();

        for (Map.Entry<String, String> entry : ctx.queryParams().entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            queryParamGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }

        for (Map.Entry<String, List<String>> entry : queryParamGroups.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();

            if (values.size() == 1) {
                String singleValue = values.get(0);
                // Handle single key with array-like (string) syntax (ex. `?statuses=[SUCCEEDED,FAILED]`)
                if (singleValue.startsWith("[") && singleValue.endsWith("]")) {
                    String arrayContent = singleValue.substring(1, singleValue.length() - 1).trim();
                    if (!arrayContent.isEmpty()) {
                        String[] items = arrayContent.split(",");
                        List<String> arrayItems = new ArrayList<>();
                        for (String item : items) {
                            arrayItems.add(item.trim());
                        }
                        params.put(key, arrayItems);
                    } else {
                        // Empty array
                        params.put(key, new ArrayList<>());
                    }
                } else {
                    params.put(key, singleValue);
                }
            } else {
                // Multiple query parameters with same name (ex. `?statuses=SUCCEEDED&statuses=FAILED`)
                params.put(key, values);
            }
        }

        return params.encodePrettily();
    }

    private static <I> I parseInput(RoutingContext ctx, Class<I> inputClass) throws Exception {
        String encodedParams = combinedParamJson(ctx);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(encodedParams, inputClass);
    }

    private static <O> void sendSuccessResponse(RoutingContext ctx, O output) throws Exception {
        String responseFormat = ctx.request().getHeader(RESPONSE_CONTENT_TYPE_HEADER);
        if (responseFormat == null || responseFormat.equals("application/json")) {
            String outputJson = outputToJson(ctx, output);
            ctx.response()
                    .setStatusCode(200)
                    .putHeader("content-type", JSON_TYPE_VALUE)
                    .end(outputJson);
        } else {
            String responseBase64 = convertToTBinaryB64(responseFormat, output);
            ctx.response()
                    .setStatusCode(200)
                    .putHeader("content-type", TBINARY_B64_TYPE_VALUE)
                    .end(responseBase64);
        }
    }

    private static void handleException(RoutingContext ctx, Throwable ex, String errorMessage) {
        if (ex instanceof IllegalArgumentException) {
            LOGGER.error(errorMessage, ex);
            ctx.response()
                .setStatusCode(400)
                .putHeader("content-type", "application/json")
                .end(toErrorPayload(ex));
        } else {
            LOGGER.error(errorMessage, ex);
            ctx.response()
                .setStatusCode(500)
                .putHeader("content-type", "application/json")
                .end(toErrorPayload(ex));
        }
    }

    /**
     * Creates a RoutingContext handler that maps parameters to an Input object and
     * transforms it to Output
     *
     * @param transformer Function to convert from Input to Output
     * @param inputClass  Class object for the Input type
     * @param <I>         Input type with setter methods
     * @param <O>         Output type
     * @return Handler for RoutingContext that produces Output TODO: To use
     *         consistent helper wrappers for the response.
     */
    public static <I, O> Handler<RoutingContext> createHandler(Function<I, O> transformer, Class<I> inputClass) {
        return ctx -> {
            try {
                I input = parseInput(ctx, inputClass);
                O output = transformer.apply(input);
                sendSuccessResponse(ctx, output);
            } catch (Exception ex) {
                handleException(ctx, ex, "Error in handler");
            }
        };
    }

    /**
     * Creates a RoutingContext handler that maps parameters to an Input object and
     * transforms it to a CompletableFuture of Output This method is specifically
     * designed for asynchronous handlers that return CompletableFuture.
     *
     * @param asyncTransformer Function to convert from Input to CompletableFuture of Output
     * @param inputClass       Class object for the Input type
     * @param <I>              Input type with setter methods
     * @param <O>              Output type
     * @return Handler for RoutingContext that handles the CompletableFuture response
     */
    public static <I, O> Handler<RoutingContext> createAsyncHandler(Function<I, CompletableFuture<O>> asyncTransformer,
            Class<I> inputClass) {
        return ctx -> {
            try {
                I input = parseInput(ctx, inputClass);

                // Apply the async transformer to get the CompletableFuture
                CompletableFuture<O> futureOutput = asyncTransformer.apply(input);

                // Handle the future's completion
                futureOutput.whenComplete((output, throwable) -> {
                    if (throwable != null) {
                        // Handle errors from the CompletableFuture
                        LOGGER.error("Error in async operation", throwable);
                        Throwable rootCause = throwable;
                        while (rootCause.getCause() != null) {
                            rootCause = rootCause.getCause();
                        }

                        int statusCode = 500; // Default to internal server error
                        if (rootCause instanceof IllegalArgumentException) {
                            statusCode = 400; // Bad request for validation errors
                        }

                        ctx.response()
                                .setStatusCode(statusCode)
                                .putHeader("content-type", "application/json")
                                .end(toErrorPayload(rootCause));
                    } else {
                        try {
                            sendSuccessResponse(ctx, output);
                        } catch (Exception ex) {
                            handleException(ctx, ex, "Error processing successful async result");
                        }
                    }
                });
            } catch (Exception ex) {
                handleException(ctx, ex, "Error preparing async handler");
            }
        };
    }

    private static <O> String convertToTBinaryB64(String responseFormat, O output) throws TException {
        if (!responseFormat.equals(TBINARY_B64_TYPE_VALUE)) {
            throw new IllegalArgumentException(
                    String.format("Unsupported response-content-type: %s. Supported values are: %s and %s",
                            responseFormat, JSON_TYPE_VALUE, TBINARY_B64_TYPE_VALUE));
        }

        // Verify output is a Thrift object before casting
        if (!(output instanceof TBase)) {
            throw new IllegalArgumentException("Output must be a Thrift object for binary serialization");
        }
        TBase<?, TFieldIdEnum> tb = (TBase<?, TFieldIdEnum>) output;
        // Serialize output to Thrift BinaryProtocol
        byte[] serializedOutput = binarySerializer.get().serialize(tb);
        String responseBase64 = base64Encoder.get().encodeToString(serializedOutput);
        return responseBase64;
    }

    private static <O> String outputToJson(RoutingContext ctx, O output) {
        try {
            String jsonString;
            if (output instanceof TBase) {
                // For Thrift objects, use TSerializer
                TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
                jsonString = serializer.toString((TBase) output);
            } else {
                // For regular Java objects, use Vertx's JSON support
                JsonObject jsonObject = new JsonObject(Json.encode(output));
                jsonString = jsonObject.encode();
            }
            return jsonString;
        } catch (TException e) {
            LOGGER.error("Failed to serialize response", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error during serialization", e);
            throw new RuntimeException(e);
        }
    }

    public static String toErrorPayload(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return new JsonObject().put("error", sw.getBuffer().toString()).encode();
    }
}
