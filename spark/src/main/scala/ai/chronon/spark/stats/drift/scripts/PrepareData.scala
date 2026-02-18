package ai.chronon.spark.stats.drift.scripts

import ai.chronon.api
import ai.chronon.api.Builders
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.Operation
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.observability.DriftMetric
import ai.chronon.observability.DriftSpec
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row => SRow}

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class PrepareData(namespace: String)(implicit tableUtils: TableUtils) {

  val dimMerchant = "dim_merchant"
  val dimUser = "dim_user"
  val txnByMerchant = "txn_by_merchant"
  val txnByUser = "txn_by_user"

  // generate data for a hypothetical fraud model with anomalies
  // ported from: https://github.com/zipline-ai/chronon/pull/30/
  // jsondiff-ed it to make sure they are semantically equivalent
  def generateAnomalousFraudJoin: api.Join = {

    val merchant_source = Builders.Source.entities(
      query = Builders.Query(
        selects = Seq("merchant_id",
                      "account_age",
                      "zipcode",
                      "is_big_merchant",
                      "country",
                      "account_type",
                      "preferred_language").map(s => s -> s).toMap
      ),
      snapshotTable = "data.merchants"
    )
    val merchant_group_by = Builders.GroupBy(
      metaData = Builders.MetaData(name = dimMerchant, namespace = namespace),
      sources = Seq(merchant_source),
      keyColumns = Seq("merchant_id")
    )

    def createTransactionSource(key: String): api.Source = {
      Builders.Source.events(
        query = Builders.Query(
          selects = Seq(key, "transaction_amount", "transaction_type").map(s => s -> s).toMap,
          timeColumn = "transaction_time"
        ),
        table = "data.txn_events"
      )
    }

    def createTxnGroupBy(source: api.Source, key: String, name: String): api.GroupBy = {
      val windowSizes = Seq(
        new Window(1, TimeUnit.HOURS),
        new Window(1, TimeUnit.DAYS),
        new Window(7, TimeUnit.DAYS),
        new Window(30, TimeUnit.DAYS),
        new Window(365, TimeUnit.DAYS)
      )
      val avg = Builders.Aggregation(
        inputColumn = "transaction_amount",
        operation = Operation.AVERAGE
      )
      val countAgg = Builders.Aggregation(
        inputColumn = "transaction_amount",
        operation = Operation.COUNT,
        windows = windowSizes
      )
      val sumAgg = Builders.Aggregation(
        inputColumn = "transaction_amount",
        operation = Operation.SUM,
        windows = Seq(new Window(1, TimeUnit.HOURS))
      )
      Builders.GroupBy(
        metaData = Builders.MetaData(name = name, namespace = namespace),
        sources = Seq(source),
        keyColumns = Seq(key),
        aggregations = Seq(avg, countAgg, sumAgg)
      )
    }

    val source_user_transactions = createTransactionSource("user_id")
    val txn_group_by_user = createTxnGroupBy(source_user_transactions, "user_id", txnByUser)

    val source_merchant_transactions = createTransactionSource("merchant_id")
    val txn_group_by_merchant = createTxnGroupBy(source_merchant_transactions, "merchant_id", txnByMerchant)

    val userSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Seq("user_id",
                      "account_age",
                      "account_balance",
                      "credit_score",
                      "number_of_devices",
                      "country",
                      "account_type",
                      "preferred_language").map(s => s -> s).toMap
      ),
      snapshotTable = "data.users"
    )
    val userGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = dimUser, namespace = namespace),
      sources = Seq(userSource),
      keyColumns = Seq("user_id")
    )

    // TODO: this is inconsistent with the defn of userSource above - but to maintain portability - we will keep it as is
    val joinUserSource = Builders.Source.events(
      query = Builders.Query(
        selects = Seq("user_id", "ts").map(s => s -> s).toMap,
        timeColumn = "ts"
      ),
      table = "data.users"
    )

    val driftSpec = new DriftSpec()
    driftSpec.setTileSize(new Window(30, TimeUnit.MINUTES))
    driftSpec.setDriftMetric(DriftMetric.JENSEN_SHANNON)
    val windows = new java.util.ArrayList[Window]()
    windows.add(new Window(30, TimeUnit.MINUTES))
    driftSpec.setLookbackWindows(windows)

    Builders.Join(
      left = joinUserSource,
      joinParts = Seq(
        Builders.JoinPart(groupBy = txn_group_by_user),
        Builders.JoinPart(groupBy = txn_group_by_merchant),
        Builders.JoinPart(groupBy = userGroupBy),
        Builders.JoinPart(groupBy = merchant_group_by)
      ),
      metaData = Builders.MetaData(
        name = "risk.user_transactions.txn_join",
        namespace = namespace,
        driftSpec = driftSpec
      )
    )
  }

  def timeToValue(t: LocalTime,
                  baseValue: Double,
                  amplitude: Double,
                  noiseLevel: Double,
                  scale: Double = 1.0): java.lang.Double = {
    if (scale == 0) null
    else {
      val hours = t.getHour + t.getMinute / 60.0 + t.getSecond / 3600.0
      val x = hours / 24.0 * 2 * math.Pi
      val y = (math.sin(x) + math.sin(2 * x)) / 2
      val value = baseValue + amplitude * y + Random.nextGaussian() * noiseLevel
      math.max(0, value * scale)
    }
  }

  object RandomUtils {
    def between(start: Int, end: Int): Int = {
      require(start < end, "Start must be less than end")
      start + Random.nextInt(end - start)
    }
  }

  def generateNonOverlappingWindows(startDate: LocalDate,
                                    endDate: LocalDate,
                                    numWindows: Int): List[(LocalDate, LocalDate)] = {
    val totalDays = ChronoUnit.DAYS.between(startDate, endDate).toInt
    val windowLengths = List.fill(numWindows)(RandomUtils.between(3, 8))
    val maxGap = totalDays - windowLengths.sum
    val gapDays = RandomUtils.between(2, maxGap)

    val windows = new ListBuffer[(LocalDate, LocalDate)]()
    var currentStart = startDate.plusDays(RandomUtils.between(0, totalDays - windowLengths.sum - gapDays + 1))

    for (length <- windowLengths) {
      val windowEnd = currentStart.plusDays(length)
      if (windowEnd.isAfter(endDate)) {
        return windows.toList
      }
      windows += ((currentStart, windowEnd))
      currentStart = windowEnd.plusDays(gapDays)
      if (!currentStart.isBefore(endDate)) {
        return windows.toList
      }
    }

    windows.toList
  }

  case class DataWithTime(ts: LocalDateTime, value: java.lang.Double)
  case class TimeSeriesWithAnomalies(dataWithTime: Array[DataWithTime],
                                     nullWindow: (LocalDate, LocalDate),
                                     spikeWindow: (LocalDate, LocalDate))

  def generateTimeseriesWithAnomalies(numSamples: Int = 1000,
                                      baseValue: Double = 100,
                                      amplitude: Double = 50,
                                      noiseLevel: Double = 10): TimeSeriesWithAnomalies = {

    val startDate = LocalDate.of(2023, 1, 1)
    val endDate = LocalDate.of(2023, 12, 31)

    val anomalyWindows = generateNonOverlappingWindows(startDate, endDate, 2)
    val (nullWindow, spikeWindow) = (anomalyWindows.head, anomalyWindows(1))

    val timeDelta = ChronoUnit.SECONDS.between(startDate.atStartOfDay(), endDate.atTime(23, 59, 59)) / numSamples

    val data = (0 until numSamples).map { i =>
      val transactionTime = startDate.atStartOfDay().plusSeconds(i * timeDelta)

      val scale =
        if (isWithinWindow(transactionTime.toLocalDate, nullWindow)) None
        else if (isWithinWindow(transactionTime.toLocalDate, spikeWindow)) Some(5.0)
        else Some(1.0)

      val value = timeToValue(
        transactionTime.toLocalTime,
        baseValue = baseValue,
        amplitude = amplitude,
        noiseLevel = noiseLevel,
        scale = scale.getOrElse(0.0)
      )

      DataWithTime(transactionTime, value)
    }.toArray

    TimeSeriesWithAnomalies(data, nullWindow = nullWindow, spikeWindow = spikeWindow)
  }

  implicit class StructFieldOps(sf: StructField) {
    def prefix(p: String): StructField = {
      val name = s"${p.sanitize}_${sf.name}"
      sf.copy(name = name)
    }
  }

  private val fraudFields = Array(
    // join.source - txn_events
    StructField("user_id", IntegerType, nullable = true),
    StructField("merchant_id", IntegerType, nullable = true),
    // Contextual - 3
    StructField("transaction_amount", DoubleType, nullable = true),
    StructField("transaction_time", LongType, nullable = true),
    StructField("transaction_type", StringType, nullable = true),
    // Transactions agg'd by user - 5 (txn_events)
    StructField("transaction_amount_average", DoubleType, nullable = true).prefix(txnByUser),
    StructField("transaction_amount_count_1h", IntegerType, nullable = true).prefix(txnByUser),
    StructField("transaction_amount_count_1d", IntegerType, nullable = true).prefix(txnByUser),
    StructField("transaction_amount_count_7d", IntegerType, nullable = true).prefix(txnByUser),
    StructField("transaction_amount_count_30d", IntegerType, nullable = true).prefix(txnByUser),
    StructField("transaction_amount_count_365d", IntegerType, nullable = true).prefix(txnByUser),
    StructField("transaction_amount_sum_1h", DoubleType, nullable = true).prefix(txnByUser),
    // Transactions agg'd by merchant - 7 (txn_events)
    StructField("transaction_amount_average", DoubleType, nullable = true).prefix(txnByMerchant),
    StructField("transaction_amount_count_1h", IntegerType, nullable = true).prefix(txnByMerchant),
    StructField("transaction_amount_count_1d", IntegerType, nullable = true).prefix(txnByMerchant),
    StructField("transaction_amount_count_7d", IntegerType, nullable = true).prefix(txnByMerchant),
    StructField("transaction_amount_count_30d", IntegerType, nullable = true).prefix(txnByMerchant),
    StructField("transaction_amount_count_365d", IntegerType, nullable = true).prefix(txnByMerchant),
    StructField("transaction_amount_sum_1h", DoubleType, nullable = true).prefix(txnByMerchant),
    // User features (dim_user) – 7
    StructField("account_age", IntegerType, nullable = true).prefix(dimUser),
    StructField("account_balance", DoubleType, nullable = true).prefix(dimUser),
    StructField("credit_score", IntegerType, nullable = true).prefix(dimUser),
    StructField("number_of_devices", IntegerType, nullable = true).prefix(dimUser),
    StructField("country", StringType, nullable = true).prefix(dimUser),
    StructField("account_type", IntegerType, nullable = true).prefix(dimUser),
    StructField("preferred_language", StringType, nullable = true).prefix(dimUser),
    // merchant features (dim_merchant) – 4
    StructField("account_age", IntegerType, nullable = true).prefix(dimMerchant),
    StructField("zipcode", IntegerType, nullable = true).prefix(dimMerchant),
    // set to true for 100 merchant_ids
    StructField("is_big_merchant", BooleanType, nullable = true).prefix(dimMerchant),
    StructField("country", StringType, nullable = true).prefix(dimMerchant),
    StructField("account_type", IntegerType, nullable = true).prefix(dimMerchant),
    StructField("preferred_language", StringType, nullable = true).prefix(dimMerchant),
    // derived features - transactions_last_year / account_age - 1
    StructField("transaction_frequency_last_year", DoubleType, nullable = true)
  )

  private val fraudJoin: api.Join = generateAnomalousFraudJoin
  private val fraudSchema: StructType = {
    val schema = StructType(fraudFields)
    val expected = fraudJoin.outputColumnsByGroup.values.flatten.toSet
    val actual = schema.fieldNames.toSet

    val matching = expected.intersect(actual)
    val missing = expected.diff(actual)
    val extra = actual.diff(expected)

    require(matching == expected, s"Schema mismatch: expected $expected, got $actual. Missing: $missing, Extra: $extra")
    schema
  }

  def generateFraudSampleData(numSamples: Int = 10000,
                              startDateStr: String,
                              endDateStr: String,
                              outputTable: String): DataFrame = {

    def toLocalTime(s: String): LocalDateTime = {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      LocalDate.parse(s, formatter).atStartOfDay()
    }
    val startDate = toLocalTime(startDateStr)
    val endDate = toLocalTime(endDateStr)

    val timeDelta = Duration.between(startDate, endDate).dividedBy(numSamples)

    val anomalyWindows = generateNonOverlappingWindows(startDate.toLocalDate, endDate.toLocalDate, 2)

    // Generate base values
    val transactionAmount = generateTimeseriesWithAnomalies(numSamples, 100, 50, 10)
    val accountBalance = generateTimeseriesWithAnomalies(numSamples, 5000, 2000, 500)
    val userAverageTransactionAmount = generateTimeseriesWithAnomalies(numSamples, 80, 30, 5)
    val merchantAverageTransactionAmount = generateTimeseriesWithAnomalies(numSamples, 80, 30, 5)
    val userLastHourList = generateTimeseriesWithAnomalies(numSamples, 5, 3, 1)
    val merchantLastHourList = generateTimeseriesWithAnomalies(numSamples, 5, 3, 1)

    println("generating anomalous data for fraud")
    val data: Seq[SRow] = (0 until numSamples).map { i =>
      val transactionTime = startDate.plus(timeDelta.multipliedBy(i))
      val merchantId = Random.nextInt(250) + 1

      if (i % 100000 == 0) {
        println(s"Generated $i/$numSamples rows of data.")
      }

      val isFastDrift = transactionTime.isAfter(anomalyWindows.head._1.atStartOfDay) &&
        transactionTime.isBefore(anomalyWindows.head._2.atTime(23, 59))
      val isSlowDrift = transactionTime.isAfter(anomalyWindows(1)._1.atStartOfDay) &&
        transactionTime.isBefore(anomalyWindows(1)._2.atTime(23, 59))

      val driftFactor = if (isFastDrift) 10 else if (isSlowDrift) 1.05 else 1.0

      def genTuple(lastHour: java.lang.Double): (Integer, Integer, Integer, Integer, Integer) = {
        lastHour match {
          case x if x == null => (null, null, null, null, null)
          case x =>
            val lastHour = x.toInt

            @inline
            def build(lh: Int, offset: Int): java.lang.Integer = {
              Integer.valueOf(((Random.nextInt(offset - lh + 1) + lh) * driftFactor).toInt)
            }

            (
              Integer.valueOf(lastHour),
              build(lastHour, 100),
              build(lastHour, 500),
              build(lastHour, 1000),
              build(lastHour, 10000)
            )
        }
      }

      val userAccountAge = Random.nextInt(3650) + 1

      val (adjustedUserLastHour,
           adjustedUserLastDay,
           adjustedUserLastWeek,
           adjustedUserLastMonth,
           adjustedUserLastYear) = genTuple(userLastHourList.dataWithTime(i).value)

      val (adjustedMerchantLastHour,
           adjustedMerchantLastDay,
           adjustedMerchantLastWeek,
           adjustedMerchantLastMonth,
           adjustedMerchantLastYear) = genTuple(merchantLastHourList.dataWithTime(i).value)

      val arr = Array(
        Random.nextInt(100) + 1,
        merchantId,
        transactionAmount.dataWithTime(i).value,
        transactionTime.toEpochSecond(ZoneOffset.UTC) * 1000,
        Seq("purchase", "withdrawal", "transfer")(Random.nextInt(3)),
        userAverageTransactionAmount.dataWithTime(i).value,
        adjustedUserLastHour,
        adjustedUserLastDay,
        adjustedUserLastWeek,
        adjustedUserLastMonth,
        adjustedUserLastYear,
        Random.nextDouble() * 100,
        merchantAverageTransactionAmount.dataWithTime(i).value,
        adjustedMerchantLastHour,
        adjustedMerchantLastDay,
        adjustedMerchantLastWeek,
        adjustedMerchantLastMonth,
        adjustedMerchantLastYear,
        Random.nextDouble() * 1000,
        userAccountAge,
        accountBalance.dataWithTime(i).value,
        Random.nextInt(551) + 300,
        Random.nextInt(5) + 1,
        if (!isFastDrift) Seq("US", "UK", "CA", "AU", "DE", "FR")(Random.nextInt(6))
        else Seq("US", "UK", "CA", "BR", "ET", "GE")(Random.nextInt(6)),
        Random.nextInt(101),
        Seq("en-US", "es-ES", "fr-FR", "de-DE", "zh-CN")(Random.nextInt(5)),
        Random.nextInt(3650) + 1,
        Random.nextInt(90000) + 10000,
        merchantId < 100,
        if (!isFastDrift) Seq("US", "UK", "CA", "AU", "DE", "FR")(Random.nextInt(6))
        else Seq("US", "UK", "CA", "BR", "ET", "GE")(Random.nextInt(6)),
        Random.nextInt(101),
        Seq("en-US", "es-ES", "fr-FR", "de-DE", "zh-CN")(Random.nextInt(5)),
        if (adjustedUserLastYear == null) null else adjustedUserLastYear.toDouble / userAccountAge
      )

      val row = new GenericRow(arr)
      row
    }

    val spark = tableUtils.sparkSession
    val df = spark.createDataFrame(java.util.Arrays.asList(data: _*), fraudSchema)
    val dfWithTimeConvention = df
      .withColumn(Constants.TimeColumn, col("transaction_time"))
      .withColumn(tableUtils.partitionColumn,
                  date_format(from_unixtime(col(Constants.TimeColumn) / 1000), tableUtils.partitionSpec.format))

    dfWithTimeConvention.save(outputTable)
    println(s"Successfully wrote fraud data to table. ${outputTable.yellow}")

    dfWithTimeConvention
  }

  def isWithinWindow(date: LocalDate, window: (LocalDate, LocalDate)): Boolean = {
    !date.isBefore(window._1) && !date.isAfter(window._2)
  }

  // dummy code below to write to spark
  def expandTilde(path: String): String = {
    if (path.startsWith("~" + java.io.File.separator)) {
      System.getProperty("user.home") + path.substring(1)
    } else {
      path
    }
  }

  def saveToFile(content: String, filePath: String): Unit = {
    try {
      val expandedPath = expandTilde(filePath)
      val path = Paths.get(expandedPath)
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      println(s"Content successfully written to: $expandedPath")
    } catch {
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    }
  }

  def prettifyJson(str: String): String = {
    val jsonObject = JsonParser.parseString(str).getAsJsonObject
    val gson = new GsonBuilder().setPrettyPrinting().create
    val prettyJsonString = gson.toJson(jsonObject)
    prettyJsonString
  }
}
