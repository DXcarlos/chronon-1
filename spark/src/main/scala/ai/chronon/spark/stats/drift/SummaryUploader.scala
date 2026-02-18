package ai.chronon.spark.stats.drift
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.online.Api
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success

class SummaryUploader(summaryDF: DataFrame,
                      api: Api,
                      putsPerRequest: Int = 100,
                      datasetName: String = Constants.TiledSummaryDataset,
                      requestsPerSecondPerExecutor: Int = 1000 // TODO: implement rate limiting
)(implicit tu: TableUtils)
    extends Serializable {

  def run(): Unit = {
    // Validate schema
    val requiredColumns = Seq("keyBytes", "valueBytes", "timestamp")
    val missingColumns = requiredColumns.filterNot(summaryDF.columns.contains)
    require(missingColumns.isEmpty, s"Missing required columns: ${missingColumns.mkString(", ")}")

    // create dataset if missing
    try {
      api.genKvStore.create(datasetName)
    } catch {
      // swallows all exceptions right now
      // TODO: swallow only already existing exception - move this into the kvstore.createIfNotExists
      case e: Exception => e.printStackTrace()
    }

    val keyIndex = summaryDF.schema.fieldIndex("keyBytes")
    val valueIndex = summaryDF.schema.fieldIndex("valueBytes")
    val timestampIndex = summaryDF.schema.fieldIndex("timestamp")

    require(summaryDF.schema(keyIndex).dataType == types.BinaryType, "keyBytes must be BinaryType")
    require(summaryDF.schema(valueIndex).dataType == types.BinaryType, "valueBytes must be BinaryType")
    require(summaryDF.schema(timestampIndex).dataType == types.LongType, "timestamp must be LongType")

    summaryDF.foreachPartition((rows: Iterator[Row]) => {
      val kvStore: KVStore = api.genKvStore

      def toPutRequest(row: Row): PutRequest = {
        if (row.isNullAt(keyIndex)) return null
        val keyBytes = row.getAs[Array[Byte]](keyIndex)
        val valueBytes = if (row.isNullAt(valueIndex)) Array.empty[Byte] else row.getAs[Array[Byte]](valueIndex)
        val timestamp =
          if (timestampIndex < 0 || row.isNullAt(timestampIndex)) None else Some(row.getAs[Long](timestampIndex))
        PutRequest(keyBytes, valueBytes, datasetName, timestamp)
      }

      // TODO implement rate limiting
      val putResponses: Iterator[Future[Seq[Boolean]]] = rows
        .grouped(putsPerRequest)
        .map { _.map(toPutRequest).filter(_ != null).toArray }
        .map { requests => kvStore.multiPut(requests) }

      val aggregatedFuture = Future.sequence(putResponses.toSeq).map(_.flatten)
      aggregatedFuture.onComplete {
        case Success(s) =>
          val failures = s.filter(_ == false)
          if (failures.nonEmpty) println(s"Hit some multiput failures. ${failures.size} / ${s.size}".red)
        case Failure(e) =>
          throw new RuntimeException(s"Failed to upload summary statistics: ${e.getMessage}", e)
      }
      // wait for futures to wrap up
      Await.result(aggregatedFuture, Duration(10L, TimeUnit.SECONDS))
      ()
    })
  }
}
