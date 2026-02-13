package ai.chronon.integrations.cloud_azure

object CosmosKVStoreConstants {
  // TTL configuration (5 days default)
  val DefaultDataTTLSeconds: Int = 5 * 24 * 60 * 60 // 432000 seconds

  // Throughput defaults
  val DefaultAutoscaleMaxRU: Int = 1000

  // Query configuration
  val DefaultMaxItemsPerPage: Int = 100

  // Environment variables
  val EnvCosmosEndpoint: String = "COSMOS_ENDPOINT"
  val EnvCosmosKey: String = "COSMOS_KEY"
  val EnvCosmosDatabase: String = "COSMOS_DATABASE"
  val EnvCosmosPreferredRegions: String = "COSMOS_PREFERRED_REGIONS"

  // Container names for shared containers
  val GroupByBatchContainer: String = "groupby_batch"
  val GroupByStreamingContainer: String = "groupby_streaming"

  // Configuration property keys
  val PropTTLSeconds: String = "ttl-seconds"
  val PropThroughput: String = "throughput"
  val PropAutoscale: String = "autoscale"
  val PropCosmosEndpoint: String = "cosmos.endpoint"
  val PropCosmosKey: String = "cosmos.key"
  val PropCosmosDatabase: String = "cosmos.database"
  val PropCosmosPreferredRegions: String = "cosmos.preferred.regions"
  val PropEmulatorMode: String = "emulator-mode"

  // Default database name
  val DefaultDatabaseName: String = "chronon"

  // Emulator detection
  def isEmulator(endpoint: String): Boolean = {
    endpoint.contains("localhost") || endpoint.contains("127.0.0.1")
  }
}
