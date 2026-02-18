package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.catalog.Format
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.net.URI

/** Snowflake Format implementation for reading tables and partition metadata via Spark Snowflake connector.
  *
  * Configuration is read from environment variables (set via teams.py executionInfo.env):
  *   - SNOWFLAKE_JDBC_URL: JDBC URL (e.g., jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w)
  *
  * Private key authentication (required, uses tiered lookup):
  *   1. SNOWFLAKE_PRIVATE_KEY: PEM-encoded private key content (PKCS#8 format) directly in the environment variable
  *   2. SNOWFLAKE_VAULT_URI: Azure Key Vault secret URI (e.g., https://<vault-name>.vault.azure.net/secrets/<secret-name>)
  *      - If SNOWFLAKE_PRIVATE_KEY is not set, retrieves the private key from Azure Key Vault
  *   3. If neither is set, an exception is thrown with instructions
  *
  * The partition column is determined by spark.chronon.partition.column (default: ds)
  */
case object Snowflake extends Format {

  @transient private lazy val snowflakeLogger = LoggerFactory.getLogger(getClass)

  override def table(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {
    throw new UnsupportedOperationException(
      "Direct table reads are not supported for Snowflake format. Use a stagingQuery with EngineType.SNOWFLAKE to export the data first.")
  }

  override def createTable(tableName: String,
                           schema: StructType,
                           partitionColumns: List[String],
                           tableProperties: Map[String, String],
                           semanticHash: Option[String])(implicit sparkSession: SparkSession): Unit = {
    throw new UnsupportedOperationException("Table creation is not supported for Snowflake format.")
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val defaultPartitionColumn = sparkSession.conf.get("spark.chronon.partition.column", "ds")
    val partitionFormat = sparkSession.conf.get("spark.chronon.partition.format", "yyyy-MM-dd")

    // Parse table name to extract database and schema if provided
    val (database, schema, table) = parseTableName(tableName)

    // Discover the partition column from the table's clustering key or fall back to default
    val partitionColumn = getPartitionColumn(database, schema, table).getOrElse(defaultPartitionColumn)
    snowflakeLogger.info(s"Using partition column '$partitionColumn' for table $tableName")

    queryDistinctPartitions(tableName, partitionColumn, partitionFilters, partitionFormat)
  }

  /** Parse a potentially qualified table name into (database, schema, table) components.
    * Uses the JDBC URL defaults for database and schema if not specified in the table name.
    */
  private def parseTableName(tableName: String): (String, String, String) = {
    val jdbcParams = {
      val jdbcUrl = getJdbcUrl
      val queryString = new URI(jdbcUrl.replace("jdbc:snowflake://", "http://")).getQuery
      if (queryString != null && queryString.nonEmpty) {
        queryString.split("&").map(_.split("=", 2)).collect { case Array(k, v) => k -> v }.toMap
      } else {
        Map.empty[String, String]
      }
    }
    val defaultDb = jdbcParams.getOrElse("db", "")
    val defaultSchema = jdbcParams.getOrElse("schema", "")

    val parts = tableName.split("\\.")
    parts.length match {
      case 3 => (parts(0), parts(1), parts(2))
      case 2 => (defaultDb, parts(0), parts(1))
      case 1 => (defaultDb, defaultSchema, parts(0))
      case _ => throw new IllegalArgumentException(s"Invalid table name format: $tableName")
    }
  }

  /** Discovers the partition column for a Snowflake table by checking:
    * 1. The table's clustering key (first column if exists)
    * 2. Falls back to None if no clustering key is found
    */
  private def getPartitionColumn(database: String, schema: String, table: String)(implicit
      sparkSession: SparkSession): Option[String] = {
    val sfOptions = getSnowflakeOptions

    // Query INFORMATION_SCHEMA.TABLES to get the clustering key
    val clusteringKeyQuery =
      s"""
         |SELECT CLUSTERING_KEY
         |FROM ${database.toUpperCase}.INFORMATION_SCHEMA.TABLES
         |WHERE TABLE_SCHEMA = '${schema.toUpperCase}'
         |  AND TABLE_NAME = '${table.toUpperCase}'
         |""".stripMargin

    snowflakeLogger.info(s"Querying clustering key: $clusteringKeyQuery")

    try {
      val df = sparkSession.read
        .format("net.snowflake.spark.snowflake")
        .options(sfOptions)
        .option("query", clusteringKeyQuery)
        .load()

      val clusteringKey = df.collect().headOption.flatMap { row =>
        Option(row.getAs[String](0)).filter(_.nonEmpty)
      }

      clusteringKey.flatMap { key =>
        // Clustering key format is like "LINEAR(col1, col2)" or just "col1, col2"
        // Extract the first column name
        val cleanKey = key
          .replaceAll("(?i)LINEAR\\s*\\(", "")
          .replaceAll("\\)", "")
          .trim
        cleanKey.split(",").headOption.map(_.trim.toUpperCase)
      }
    } catch {
      case e: Exception =>
        snowflakeLogger.warn(s"Failed to query clustering key for $database.$schema.$table: ${e.getMessage}")
        None
    }
  }

  override def supportSubPartitionsFilter: Boolean = false

  private def getJdbcUrl: String = {
    val jdbcUrl = sys.env.getOrElse(
      "SNOWFLAKE_JDBC_URL",
      throw new IllegalStateException(
        "SNOWFLAKE_JDBC_URL not found in environment. " +
          "Expected format: jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w")
    )
    if (!jdbcUrl.startsWith("jdbc:snowflake://")) {
      throw new IllegalStateException(s"SNOWFLAKE_JDBC_URL must start with 'jdbc:snowflake://'. Got: $jdbcUrl")
    }
    jdbcUrl
  }

  /** Fetches the PEM-encoded private key content using tiered lookup:
    * 1. SNOWFLAKE_PRIVATE_KEY environment variable
    * 2. SNOWFLAKE_VAULT_URI (Azure Key Vault)
    * 3. Throws exception with helpful message if neither is found
    *
    * @return The PEM-encoded private key string
    */
  private def getPrivateKeyPem(): String = {
    sys.env.get("SNOWFLAKE_PRIVATE_KEY") match {
      case Some(privateKey) =>
        snowflakeLogger.info("Using private key from SNOWFLAKE_PRIVATE_KEY environment variable")
        privateKey
      case None =>
        sys.env.get("SNOWFLAKE_VAULT_URI") match {
          case Some(vaultUri) =>
            snowflakeLogger.info(s"Using private key from Azure Key Vault: $vaultUri")
            val (vaultUrl, secretName) = AzureKeyVaultHelper.parseSecretUri(vaultUri)
            AzureKeyVaultHelper.getSecret(vaultUrl, secretName)
          case None =>
            throw new IllegalStateException(
              "Snowflake private key not found. Please provide one of the following:\n" +
                "  1. SNOWFLAKE_PRIVATE_KEY environment variable with PEM-encoded private key content, or\n" +
                "  2. SNOWFLAKE_VAULT_URI environment variable with Azure Key Vault URI " +
                "(e.g., https://<vault-name>.vault.azure.net/secrets/<secret-name>)"
            )
        }
    }
  }

  /** Builds the Spark Snowflake connector options from the JDBC URL and private key.
    */
  private def getSnowflakeOptions: Map[String, String] = {
    val jdbcUrl = getJdbcUrl

    // Parse JDBC URL to extract parameters
    val jdbcParams: Map[String, String] = {
      val queryString = new URI(jdbcUrl.replace("jdbc:snowflake://", "http://")).getQuery
      if (queryString != null && queryString.nonEmpty) {
        queryString.split("&").map(_.split("=", 2)).collect { case Array(k, v) => k -> v }.toMap
      } else {
        Map.empty
      }
    }

    // Get the PEM Base64 content for Snowflake connector (without headers)
    val pemContent = getPrivateKeyPem()
    val pemBase64 = pemContent
      .replaceAll("-----BEGIN.*-----", "")
      .replaceAll("-----END.*-----", "")
      .replaceAll("\\s", "")

    Map(
      "sfURL" -> jdbcUrl.split("\\?").head.replace("jdbc:snowflake://", ""),
      "pem_private_key" -> pemBase64,
      "sfUser" -> jdbcParams.getOrElse("user", throw new IllegalStateException("User missing in SNOWFLAKE_JDBC_URL")),
      "sfDatabase" -> jdbcParams.getOrElse("db", throw new IllegalStateException("DB missing in SNOWFLAKE_JDBC_URL")),
      "sfSchema" -> jdbcParams.getOrElse("schema",
                                         throw new IllegalStateException("Schema missing in SNOWFLAKE_JDBC_URL")),
      "sfWarehouse" -> jdbcParams.getOrElse("warehouse",
                                            throw new IllegalStateException("Warehouse missing in SNOWFLAKE_JDBC_URL"))
    )
  }

  private def queryDistinctPartitions(tableName: String,
                                      partitionColumn: String,
                                      partitionFilters: String,
                                      partitionFormat: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    snowflakeLogger.info(s"Querying partitions for table: $tableName using Spark Snowflake connector")

    val query = buildPartitionQuery(tableName, partitionColumn, partitionFilters, partitionFormat)
    snowflakeLogger.info(s"Executing partition query: $query")

    val sfOptions = getSnowflakeOptions

    val snowflakeDF = sparkSession.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", query)
      .load()

    val df = snowflakeDF.toDF(snowflakeDF.columns.map(_.toLowerCase): _*)

    // Convert DataFrame to list of partition maps
    // Snowflake returns column names in uppercase, so we need to handle that
    val columnName = df.columns.find(_.equalsIgnoreCase(partitionColumn)).getOrElse(partitionColumn)

    val partitions = df
      .select(columnName)
      .distinct()
      .collect()
      .flatMap { row =>
        val value = row.get(0)
        if (value != null) Some(Map(columnName -> value.toString))
        else None
      }
      .toList
      .sortBy(_(columnName))

    snowflakeLogger.info(s"Found ${partitions.size} distinct partitions for table $tableName")
    partitions
  }

  private def buildPartitionQuery(tableName: String,
                                  partitionColumn: String,
                                  partitionFilters: String,
                                  partitionFormat: String): String = {
    // Convert Java DateTimeFormatter pattern to Snowflake format pattern
    // Common mappings: yyyy->YYYY, MM->MM, dd->DD
    val snowflakeFormat = partitionFormat
      .replace("yyyy", "YYYY")
      .replace("dd", "DD")
    // Cast to DATE first (handles timestamps, dates, and date strings), then format as string
    val formattedColumn = s"TO_VARCHAR($partitionColumn::DATE, '$snowflakeFormat') AS $partitionColumn"
    val baseQuery = s"SELECT DISTINCT $formattedColumn FROM $tableName"
    if (partitionFilters.nonEmpty) {
      s"$baseQuery WHERE $partitionFilters"
    } else {
      baseQuery
    }
  }
}
