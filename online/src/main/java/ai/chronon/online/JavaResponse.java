/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online;

import ai.chronon.api.ScalaJavaConversions;
import ai.chronon.online.fetcher.Fetcher;
import ai.chronon.online.fetcher.ResponseType;
import scala.Enumeration;
import scala.util.Try;

import java.util.Map;


public class JavaResponse {
    public JavaRequest request;
    public JTry<Map<String, Object>> values;
    public JTry<byte[]> valuesAvroBytes;
    public JTry<String> valuesAvroString;
    public JTry<Map<String, Object>> errorsV2;
    public Enumeration.Value valueType;

    public JavaResponse(JavaRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
        this.valueType = ResponseType.Map(); // since values is a Map
    }

    // Original constructor for backward compatibility
    public JavaResponse(Fetcher.Response scalaResponse) {
        this.request = new JavaRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(v -> {
                    if (v != null)
                        return ScalaJavaConversions.toJava(v);
                    else
                        return null;
                });
        this.valueType = ResponseType.Map(); // since values is a Map
    }

    // New generic constructor that handles both Response and ResponseV2
    public JavaResponse(Fetcher.BaseResponse scalaResponse) {
        if (scalaResponse instanceof Fetcher.Response) {
            // Handle Response (original behavior)
            Fetcher.Response response = (Fetcher.Response) scalaResponse;
            this.request = new JavaRequest(response.request());
            this.values = JTry
                    .fromScala(response.values())
                    .map(v -> {
                        if (v != null)
                            return ScalaJavaConversions.toJava(v);
                        else
                            return null;
                    });
            this.valueType = ResponseType.Map(); // since values is a Map
        } else if (scalaResponse instanceof Fetcher.ResponseV2) {
            Fetcher.ResponseV2 responseV2 = (Fetcher.ResponseV2) scalaResponse;
            this.request = new JavaRequest(responseV2.request());
            Enumeration.Value valueType = responseV2.getResponseValueType();
            this.valueType = valueType;
            this.errorsV2 = JTry.fromScala(responseV2.errors())
                    .map(v -> {
                        if (v != null)
                            return ScalaJavaConversions.toJava(v);
                        else
                            return null;
                    });
            if (valueType == ResponseType.WithAvroBytes()) {
                this.valuesAvroBytes = JTry.fromScala(responseV2.valuesAvroBytes());
            } else if (valueType == ResponseType.WithAvroString()) {
                this.valuesAvroString = JTry.fromScala(responseV2.valuesAvroString());
            } else {
                throw new IllegalArgumentException("Unknown response type: " + valueType);
            }
        } else {
            throw new IllegalArgumentException("Unsupported response type: " + scalaResponse.getClass().getName());
        }
    }

    // Factory methods for cleaner API
    public static JavaResponse fromResponse(Fetcher.Response scalaResponse) {
        return new JavaResponse(scalaResponse);
    }

    public static JavaResponse fromResponseV2(Fetcher.ResponseV2 scalaResponseV2) {
        return new JavaResponse(scalaResponseV2);
    }

    public static JavaResponse fromBaseResponse(Fetcher.BaseResponse scalaResponse) {
        return new JavaResponse(scalaResponse);
    }

    public Fetcher.Response toScala() {
        return new Fetcher.Response(
                request.toScalaRequest(),
                values.map(ScalaJavaConversions::toScala).toScala());
    }

    // New method to convert to ResponseV2 if needed
    public Fetcher.ResponseV2 toScalaV2() {
        // This would need to be implemented based on how you want to handle the conversion
        // For now, throwing an exception as this requires more context about your use case
        throw new UnsupportedOperationException("Conversion to ResponseV2 not yet implemented");
    }
}
