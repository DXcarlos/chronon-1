package ai.chronon.api

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/** Gzip + Base64 codec for compact serialization over CLI args, env vars, etc. */
object GzipCodec {

  /** Encode: UTF-8 bytes → gzip → Base64 string. */
  def encode(s: String): String = {
    val baos = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(baos)
    gzip.write(s.getBytes("UTF-8"))
    gzip.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  /** Decode: Base64 string → gunzip → UTF-8 string. */
  def decode(encoded: String): String = {
    val compressed = Base64.getDecoder.decode(encoded)
    val gzipIn = new GZIPInputStream(new ByteArrayInputStream(compressed))
    val bytes = new ByteArrayOutputStream()
    val buf = new Array[Byte](4096)
    var n = gzipIn.read(buf)
    while (n != -1) {
      bytes.write(buf, 0, n)
      n = gzipIn.read(buf)
    }
    gzipIn.close()
    bytes.toString("UTF-8")
  }
}
