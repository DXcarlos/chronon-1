package ai.chronon.spark.submission

import ai.chronon.api.ThriftJsonCodec
import ai.chronon.planner.Node

/** Reads Node config from local files or cloud storage URIs (gs://, s3://, abfss://).
  *
  * Cloud URIs are read using Hadoop FileSystem with minimal configuration.
  * The GCS/S3 connectors must be on the classpath (typically via the Spark image's
  * /opt/spark/jars/ directory).
  */
object NodeConfReader {

  def read(confPath: String): Node = {
    if (isCloudUri(confPath)) {
      readFromCloudStorage(confPath)
    } else {
      ThriftJsonCodec.fromJsonFile[Node](confPath, check = false)
    }
  }

  private def isCloudUri(path: String): Boolean =
    path.startsWith("gs://") || path.startsWith("s3://") ||
      path.startsWith("s3a://") || path.startsWith("abfss://")

  private def readFromCloudStorage(confPath: String): Node = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoopConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    val path = new org.apache.hadoop.fs.Path(confPath)
    val fs = org.apache.hadoop.fs.FileSystem.get(path.toUri, hadoopConf)
    val stream = fs.open(path)
    try {
      val json = scala.io.Source.fromInputStream(stream).mkString
      ThriftJsonCodec.fromJson[Node](json, check = false)
    } finally {
      stream.close()
    }
  }
}
