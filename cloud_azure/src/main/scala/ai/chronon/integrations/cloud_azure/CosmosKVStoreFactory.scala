package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.cloud_azure.CosmosKVStoreConstants._
import com.azure.cosmos.{ConsistencyLevel, CosmosAsyncClient, CosmosClientBuilder}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

object CosmosKVStoreFactory {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  // Singleton client cache (CosmosAsyncClient is expensive to create and thread-safe)
  private val clientCache = new AtomicReference[CosmosAsyncClient]()
  private val clientLock = new Object()

  def create(conf: Map[String, String]): CosmosKVStoreImpl = {
    val endpoint = getOrElseThrow(PropCosmosEndpoint, EnvCosmosEndpoint, conf)
    val key = getOrElseThrow(PropCosmosKey, EnvCosmosKey, conf)
    val databaseName = conf.getOrElse(
      PropCosmosDatabase,
      sys.env.getOrElse(EnvCosmosDatabase, DefaultDatabaseName)
    )

    val client = Option(clientCache.get()) match {
      case Some(existingClient) => existingClient
      case None =>
        clientLock.synchronized {
          Option(clientCache.get()) match {
            case Some(existingClient) => existingClient
            case None =>
              logger.info(s"Creating Cosmos client for endpoint: $endpoint")
              val newClient = createCosmosClient(endpoint, databaseName, key, conf)
              clientCache.set(newClient)
              newClient
          }
        }
    }

    val database = client.getDatabase(databaseName)
    new CosmosKVStoreImpl(database, conf)
  }

  private def createCosmosClient(
      endpoint: String,
      databaseName: String,
      key: String,
      conf: Map[String, String]
  ): CosmosAsyncClient = {

    val clientBuilder = new CosmosClientBuilder()
      .endpoint(endpoint)
      .key(key)
      .consistencyLevel(ConsistencyLevel.SESSION)

    // Use Gateway mode to avoid Netty version conflicts with Spark's bundled Netty.
    // Direct mode uses a custom TCP protocol (RNTBD) on reactor-netty, which requires
    // Netty 4.1.118+ but Spark 3.5.3 bundles ~4.1.96. Gateway mode uses the standard
    // azure-core HTTP pipeline instead.
    clientBuilder.gatewayMode()

    val isEmulator = conf.get(PropEmulatorMode).exists(_.toBoolean) || CosmosKVStoreConstants.isEmulator(endpoint)
    if (isEmulator) {
      logger.info("Detected emulator endpoint, using Gateway mode")
    }

    val preferredRegions = parsePreferredRegions(conf)
    if (!preferredRegions.isEmpty) {
      clientBuilder.preferredRegions(preferredRegions)
    } else {
      logger.info("Preferred regions not configured")
    }

    clientBuilder.buildAsyncClient()
  }

  private def parsePreferredRegions(conf: Map[String, String]): java.util.List[String] = {
    conf
      .get(PropCosmosPreferredRegions)
      .orElse(sys.env.get(EnvCosmosPreferredRegions))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList.asJava)
      .getOrElse(java.util.Collections.emptyList[String]())
  }

  private def getOrElseThrow(
      propKey: String,
      envKey: String,
      conf: Map[String, String]
  ): String = {
    conf.getOrElse(
      propKey,
      sys.env.getOrElse(
        envKey,
        throw new IllegalArgumentException(s"$propKey or $envKey required but not found")
      )
    )
  }

  // Cleanup on shutdown
  sys.addShutdownHook {
    Option(clientCache.get()).foreach { client =>
      logger.info("Closing Cosmos client")
      client.close()
    }
  }
}
