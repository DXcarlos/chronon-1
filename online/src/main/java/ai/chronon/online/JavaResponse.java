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
import ai.chronon.online.fetcher.Fetcher.ResponseValue;
import ai.chronon.online.fetcher.ResponseType;
import scala.Enumeration;

import java.util.Map;


public class JavaResponse {

    public JavaRequest request;
    public JTry<Map<String, Object>> values;
    public JTry<byte[]> valuesAvroBytes;
    public JTry<String> valuesAvroString;
    public Enumeration.Value valueType;

    public JavaResponse(JavaRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
        this.valueType = ResponseType.Map(); // since values is a Map
    }

    public JavaResponse(Fetcher.Response scalaResponse) {
        this.request = new JavaRequest(scalaResponse.request());

        Enumeration.Value valueType = scalaResponse.getResponseValueType();

        if (valueType == ResponseType.Map()) {
            this.values = JTry
                    .fromScala(scalaResponse.valuesMap())
                    .map(v -> {
                        if (v != null)
                            return ScalaJavaConversions.toJava(v);
                        else
                            return null;
                    });
            this.valueType = valueType;
        } else if (valueType == ResponseType.WithAvroBytes()) {
            this.valuesAvroBytes = JTry.fromScala(scalaResponse.valuesAvroBytes());
            this.valueType = valueType;
        } else if (valueType == ResponseType.WithAvroString()) {
            this.valuesAvroString = JTry.fromScala(scalaResponse.valuesAvroString());
            this.valueType = valueType;
        } else {
            throw new IllegalArgumentException("Unknown response type: " + valueType);
        }
    }

    public Fetcher.Response toScala() {
        if (this.valueType == ResponseType.Map()) {
            return new Fetcher.Response(
                    request.toScalaRequest(),
                    Fetcher.ResponseValue$.MODULE$.createMap(values.map(ScalaJavaConversions::toScala).toScala())
            );
        } else if (this.valueType == ResponseType.WithAvroBytes()) {
            return new Fetcher.Response(
                    request.toScalaRequest(),
                    Fetcher.ResponseValue$.MODULE$.createAvroBytes(valuesAvroBytes.map(v -> v).toScala())
            );
        } else if (this.valueType == ResponseType.WithAvroString()) {
            return new Fetcher.Response(
                    request.toScalaRequest(),
                    Fetcher.ResponseValue$.MODULE$.createAvroString(valuesAvroString.map(v -> v).toScala())
            );
        } else {
            throw new IllegalArgumentException("Unknown response type: " + this.valueType);
        }
    }
}
