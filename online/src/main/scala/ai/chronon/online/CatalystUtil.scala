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

package ai.chronon.online

import ai.chronon.api.{DataType, StructType}
import ai.chronon.online.CatalystUtil.{PoolKey, poolMap}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.serde._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionAlreadyExistsException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, types}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}
import java.util.function
import scala.collection.Seq

object CatalystUtil {
  import scala.util.{Failure, Success, Try}

  lazy val session: SparkSession = {
    def getJavaMajorVersion: Int = {
      val version = System.getProperty("java.version")
      Try {
        if (version.startsWith("1.")) {
          // Java 8 and earlier: "1.8.0_xxx" -> 8
          version.substring(2, 3).toInt
        } else {
          // Java 9+: "11.0.1" -> 11, "17.0.2" -> 17
          version.split("\\.")(0).toInt
        }
      }.getOrElse {
        // Fallback - assume Java 8 if parsing fails
        println(s"Warning: Could not parse Java version '$version', assuming Java 8")
        8
      }
    }

    // Check if we need module system workarounds
    def needsModuleWorkarounds: Boolean = {
      val majorVersion = getJavaMajorVersion
      val needs = majorVersion >= 9
      println(s"Java version: $majorVersion, needs module workarounds: $needs")
      needs
    }

    // Build the configuration
    val builder = SparkSession
      .builder()
      .appName(s"catalyst_test_${Thread.currentThread().toString}")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.parquet.columnarReaderBatchSize", "16")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
      .config(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, false)

    // Add module workarounds if needed
    val finalBuilder = if (needsModuleWorkarounds) {
      val moduleOptions = Seq(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
      ).mkString(" ")

      builder
        .config("spark.driver.extraJavaOptions", moduleOptions)
        .config("spark.executor.extraJavaOptions", moduleOptions)
    } else {
      builder
    }

    val spark = finalBuilder
      .enableHiveSupport()
      .getOrCreate()

    assert(spark.sessionState.conf.wholeStageEnabled)
    spark
  }

  case class PoolKey(expressions: Seq[(String, String)], inputSchema: StructType)
  val poolMap: PoolMap[PoolKey, CatalystUtil] = new PoolMap[PoolKey, CatalystUtil](pi =>
    new CatalystUtil(pi.inputSchema, pi.expressions))
}

class PoolMap[Key, Value](createFunc: Key => Value, maxSize: Int = 100, initialSize: Int = 2) {
  val map: ConcurrentHashMap[Key, ArrayBlockingQueue[Value]] = new ConcurrentHashMap[Key, ArrayBlockingQueue[Value]]()
  def getPool(input: Key): ArrayBlockingQueue[Value] =
    map.computeIfAbsent(
      input,
      new function.Function[Key, ArrayBlockingQueue[Value]] {
        override def apply(t: Key): ArrayBlockingQueue[Value] = {
          val result = new ArrayBlockingQueue[Value](maxSize)
          var i = 0
          while (i < initialSize) {
            result.add(createFunc(t))
            i += 1
          }
          result
        }
      }
    )

  def performWithValue[Output](key: Key, pool: ArrayBlockingQueue[Value])(func: Value => Output): Output = {
    var value = pool.poll()
    if (value == null) {
      value = createFunc(key)
    }
    try {
      func(value)
    } catch {
      case e: Exception => throw e
    } finally {
      pool.offer(value)
    }
  }
}

class PooledCatalystUtil(expressions: Seq[(String, String)], inputSchema: StructType) {
  private val poolKey = PoolKey(expressions, inputSchema)
  private val cuPool = poolMap.getPool(PoolKey(expressions, inputSchema))
  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] =
    poolMap.performWithValue(poolKey, cuPool) { _.performSql(values) }
  def outputChrononSchema: Array[(String, DataType)] =
    poolMap.performWithValue(poolKey, cuPool) { _.outputChrononSchema }
}

class CatalystUtil(inputSchema: StructType,
                   selects: Seq[(String, String)],
                   wheres: Seq[String] = Seq.empty,
                   setups: Seq[String] = Seq.empty) {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  val selectClauses: Seq[String] = selects.map { case (name, expr) => s"$expr as $name" }
  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  val whereClauseOpt: Option[String] = Option(wheres)
    .filter(_.nonEmpty)
    .map { w =>
      // wrap each clause in parens
      w.map(c => s"( $c )").mkString(" AND ")
    }

  @transient lazy val inputSparkSchema: types.StructType = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = SparkInternalRowConversions.to(inputSparkSchema)
  val inputArrEncoder: Any => Any = SparkInternalRowConversions.to(inputSparkSchema, false)

  private val (transformFunc: (InternalRow => Seq[InternalRow]), outputSparkSchema: types.StructType) = initialize()

  private lazy val outputArrDecoder = SparkInternalRowConversions.from(outputSparkSchema, false)
  @transient lazy val outputChrononSchema: Array[(String, DataType)] =
    SparkConversions.toChrononSchema(outputSparkSchema)
  private val outputDecoder = SparkInternalRowConversions.from(outputSparkSchema)

  def performSql(values: Array[Any]): Seq[Array[Any]] = {
    val internalRow = inputArrEncoder(values).asInstanceOf[InternalRow]
    val resultRowSeq = transformFunc(internalRow)
    val outputVal = resultRowSeq.map(resultRow => outputArrDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Array[Any]])
  }

  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] = {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    performSql(internalRow)
  }

  def performSql(row: InternalRow): Seq[Map[String, Any]] = {
    val resultRowMaybe = transformFunc(row)
    val outputVal = resultRowMaybe.map(resultRow => outputDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Map[String, Any]])
  }

  def getOutputSparkSchema: types.StructType = outputSparkSchema

  private def initialize(): (InternalRow => Seq[InternalRow], types.StructType) = {
    val session = CatalystUtil.session

    // run through and execute the setup statements
    setups.foreach { statement =>
      try {
        session.sql(statement)
        logger.info(s"Executed setup statement: $statement")
      } catch {
        case _: FunctionAlreadyExistsException =>
        // ignore - this crops up in unit tests on occasion
        case e: Exception =>
          logger.warn(s"Failed to execute setup statement: $statement", e)
          throw new RuntimeException(s"Error executing setup statement: $statement", e)
      }
    }

    // create dummy df with sql query and schema
    val emptyRowRdd = session.emptyDataFrame.rdd
    val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val df = session.sqlContext.table(sessionTable).selectExpr(selectClauses.toSeq: _*)
    val filteredDf = whereClauseOpt.map(df.where(_)).getOrElse(df)

    // extract transform function from the df spark plan
    val execPlan = filteredDf.queryExecution.executedPlan
    logger.info(s"Catalyst Execution Plan - ${execPlan}")

    // Use the new recursive approach to build a transformation chain
    val transformer = CatalystTransformBuilder.buildTransformChain(execPlan)

    (transformer, df.schema)
  }
}
