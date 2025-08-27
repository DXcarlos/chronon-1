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

package ai.chronon.api

import ai.chronon.api.Extensions.StringsOps
import ai.chronon.api.HashUtils.md5Bytes
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.thrift.TBase
import ai.chronon.api.thrift.TDeserializer
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.thrift.protocol.TCompactProtocol
import ai.chronon.api.thrift.protocol.TSimpleJSONProtocol
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import java.util.Base64
import scala.io.BufferedSource
import scala.io.Source._
import scala.reflect.ClassTag

object ThriftJsonCodec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient
  private lazy val serializerThreaded: ThreadLocal[TSerializer] = new ThreadLocal[TSerializer] {
    override def initialValue(): TSerializer = new TSerializer(new TSimpleJSONProtocol.Factory())
  }

  private val mapper = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }

  def serializer: TSerializer = serializerThreaded.get()

  def toJsonStr[T <: TBase[_, _]: Manifest](obj: T): String = {
    new String(serializer.serialize(obj), Constants.UTF8)
  }

  def toJsonList[T <: TBase[_, _]: Manifest](obj: util.List[T]): String = {
    if (obj == null) return ""
    obj.toScala
      .map(o => new String(serializer.serialize(o)))
      .prettyInline
  }

  def toCompactBase64[T <: TBase[_, _]: Manifest](obj: T): String = {
    val compactSerializer = new TSerializer(new TCompactProtocol.Factory())
    Base64.getEncoder.encodeToString(compactSerializer.serialize(obj))
  }

  def md5Digest[T <: TBase[_, _]: Manifest](obj: T): String = {
    HashUtils.md5Base64(ThriftJsonCodec.toJsonStr(obj).getBytes(Constants.UTF8))
  }

  def hexDigest[T <: TBase[_, _]: Manifest](obj: T, length: Int = 6): String = {
    // Get the MD5 hash bytes
    md5Bytes(serializer.serialize(obj)).map("%02x".format(_)).mkString.take(length)
  }

  def md5Digest[T <: TBase[_, _]: Manifest](obj: util.List[T]): String = {
    HashUtils.md5Base64(ThriftJsonCodec.toJsonList(obj).getBytes(Constants.UTF8))
  }

  def fromCompactBase64[T <: TBase[_, _]: Manifest](base: T, base64: String): T = {
    val compactDeserializer = new TDeserializer(new TCompactProtocol.Factory())
    val bytes = Base64.getDecoder.decode(base64)
    try {
      compactDeserializer.deserialize(base, bytes)
      base
    } catch {
      case _: Exception => {
        logger.error("Failed to deserialize using compact protocol, trying Json.")
        fromJsonStr(new String(bytes), check = false, base.getClass)
      }
    }
  }

  def fromJsonStr[T <: TBase[_, _]: Manifest](jsonStr: String, check: Boolean = true, clazz: Class[_ <: T]): T = {
    val obj: T = mapper.readValue(jsonStr, clazz)
    if (check) {
      val inputNode: JsonNode = mapper.readTree(jsonStr)
      val reSerializedInput: JsonNode = mapper.readTree(toJsonStr(obj))
      require(
        inputNode.equals(reSerializedInput),
        message = s"""
               |Parsed Json object isn't reversible.
               |Original JSON String:
               |
               |$jsonStr
               |-----------------------------------
               |JSON produced by serializing object:
               |
               |$reSerializedInput
               |------------------------------------
               |""".stripMargin
      )
    }
    obj
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](fileName: String, check: Boolean): T = {
    fromJsonFile(fromFile(fileName), check)
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](file: File, check: Boolean): T = {
    fromJsonFile(fromFile(file), check)
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](src: BufferedSource, check: Boolean): T = {
    val jsonStr =
      try src.mkString
      finally src.close()
    fromJson[T](jsonStr, check)
  }

  def fromJson[T <: TBase[_, _]: Manifest: ClassTag](jsonStr: String, check: Boolean): T = {
    val obj: T = fromJsonStr[T](jsonStr, check, clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
    obj
  }
}
