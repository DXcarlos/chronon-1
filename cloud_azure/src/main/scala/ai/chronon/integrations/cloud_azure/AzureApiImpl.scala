package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.redis.RedisKVStoreFactory
import ai.chronon.online._
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, SerDe}

import java.util.concurrent.atomic.AtomicReference

/** Implementation of Chronon's API interface for Azure using Redis as the KV store.
  *
  * Configuration is loaded from environment variables or the provided conf map:
  *   - REDIS_CLUSTER_NODES: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379") [required]
  *   - REDIS_PASSWORD: Redis password (optional)
  *   - REDIS_MAX_CONNECTIONS: Maximum pool connections (default: 50)
  *   - See RedisKVStoreFactory for additional configuration options.
  */
class AzureApiImpl(conf: Map[String, String]) extends Api(conf) {

  import AzureApiImpl._

  override def genKvStore: KVStore = {
    try {
      Option(sharedKvStore.get()) match {
        case Some(existingStore) =>
          existingStore
        case None =>
          kvStoreLock.synchronized {
            Option(sharedKvStore.get()) match {
              case Some(existingStore) => existingStore
              case None =>
                val newStore = RedisKVStoreFactory.create(conf)
                sharedKvStore.set(newStore)
                newStore
            }
          }
      }
    } catch {
      case _: IllegalArgumentException => null
    }
  }

  override def genMetricsKvStore(tableBaseName: String): KVStore = null

  override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = null

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe =
    new AvroSerDe(AvroConversions.fromChrononSchema(groupByServingInfoParsed.streamChrononSchema))

  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  override def logResponse(resp: LoggableResponse): Unit = {
    logger.debug(s"LogResponse called for join ${resp.joinName} at ts ${resp.tsMillis}")
  }
}

object AzureApiImpl {
  private val sharedKvStore = new AtomicReference[KVStore]()
  private val kvStoreLock = new Object()

  private val sharedMetricsKvStore = new AtomicReference[KVStore]()
  private val metricsKvStoreLock = new Object()

  private val sharedEnhancedStatsKvStore = new AtomicReference[KVStore]()
  private val enhancedStatsKvStoreLock = new Object()
}
