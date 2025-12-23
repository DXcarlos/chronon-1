# Chronon Data Seeder

The Chronon Data Seeder module generates test data for Chronon tables and writes them to cloud storage. This is useful for creating test datasets when running Chronon on cloud infrastructure like AWS Glue with Iceberg.

## Overview

The seeder module allows you to:
- Define table schemas programmatically using Scala
- Generate realistic test data with configurable cardinalities and row counts
- Write data to cloud storage using Chronon's FormatProvider (supports Iceberg, Hudi, Delta, Parquet)
- Seed data for both event tables and entity tables

## Module Structure

### Source Files

- **TableSchema.scala**: Core schema definition classes
  - `TableType` (Events/Entities) for different table types
  - `ColumnDef` for defining column schemas with cardinality
  - `TableSchema` for complete table definitions
  - `SeederConfig` for grouping multiple tables

- **SchemaLoader.scala**: Schema loading and validation
  - `SeederSchema` trait for users to extend
  - `SchemaLoader` object for loading schema definitions from compiled classes
  - Validation logic for seeder configs

- **DataSeeder.scala**: Main entry point
  - CLI interface for running the seeder
  - Uses TableUtils for cloud-agnostic table writing
  - Uses DataFrameGen to generate test data based on schemas

- **CanarySeeder.scala**: Example seeder implementation
  - Defines schemas for AWS canary demo join tables
  - Demonstrates how to create seeder configurations

## Dependencies

The seeder module depends on:
- **cloud_aws**: For AWS-specific integrations
- **spark**: For Spark SQL operations
- **spark.test**: For DataFrameGen utility (test data generation)
- **aggregator.test**: For Column type definitions

## Building

### Compile the module:
```bash
./mill seeder.compile
```

### Build assembly JAR:
```bash
./mill seeder.assembly
```

The assembly JAR will be created at: `out/seeder/2.12.18/assembly.dest/out.jar`

## Usage

### Creating a Seeder Schema

Create a Scala object extending `SeederSchema` in your project:

```scala
package com.example

import ai.chronon.api._
import ai.chronon.seeder._

object MySeeder extends SeederSchema {

  val userEvents = TableSchema(
    tableName = "demo.user_events",
    columns = Seq(
      ColumnDef("user_id", StringType, cardinality = 1000),
      ColumnDef("event_type", StringType, cardinality = 10),
      ColumnDef("value", LongType, cardinality = 10000)
    ),
    rowCount = 50000,
    partitionCount = 30,
    tableType = Events
  )

  val userProfiles = TableSchema(
    tableName = "demo.user_profiles",
    columns = Seq(
      ColumnDef("user_id", StringType, cardinality = 1000),
      ColumnDef("country", StringType, cardinality = 50),
      ColumnDef("age", LongType, cardinality = 100)
    ),
    rowCount = 5000,
    partitionCount = 7,
    tableType = Entities
  )

  override def config: SeederConfig = SeederConfig(
    Seq(userEvents, userProfiles)
  )
}
```

### Running the Seeder

#### Basic Usage:
```bash
spark-submit --class ai.chronon.seeder.DataSeeder \
  out/seeder/2.12.18/assembly.dest/out.jar \
  --seeder-class com.example.MySeeder \
  --file-format PARQUET
```

#### Dry Run (preview without writing):
```bash
spark-submit --class ai.chronon.seeder.DataSeeder \
  out/seeder/2.12.18/assembly.dest/out.jar \
  --seeder-class com.example.MySeeder \
  --file-format PARQUET \
  --dry-run
```

#### Available Options:
- `--seeder-class`: Fully qualified class name of your seeder schema (required)
- `--file-format`: Storage format (default: PARQUET). Options: PARQUET, ORC, ICEBERG, HUDI, DELTA
- `--dry-run`: Generate and show data without writing (default: false)

### Using with AWS Glue / Iceberg

The seeder automatically uses Chronon's FormatProvider, which respects your Spark configuration. For AWS Glue with Iceberg:

```bash
spark-submit \
  --class ai.chronon.seeder.DataSeeder \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.spark_catalog.warehouse=s3://your-bucket/data/tables/ \
  --conf spark.chronon.table_write.format=iceberg \
  out/seeder/2.12.18/assembly.dest/out.jar \
  --seeder-class ai.chronon.seeder.CanarySeeder
```

## Table Types

### Events
Event tables have timestamps and are typically fact tables. The seeder generates:
- Time-series data with timestamps
- Partition column (defaults to 'ds' with format 'yyyy-MM-dd')

### Entities
Entity tables are dimension/snapshot tables. The seeder generates:
- Static attributes
- Partition column for snapshot versioning

## CanarySeeder Example

The module includes `CanarySeeder` which generates test data for the AWS canary demo join:

**Tables created:**
- `demo.user_activities` - Event data with user activity events
- `demo.dim_listings` - Listing dimension attributes
- `demo.dim_merchants` - Merchant dimension attributes

**To run:**
```bash
./mill seeder.assembly

spark-submit --class ai.chronon.seeder.DataSeeder \
  out/seeder/2.12.18/assembly.dest/out.jar \
  --seeder-class ai.chronon.seeder.CanarySeeder \
  --file-format PARQUET
```

## Schema Definition Reference

### ColumnDef
```scala
case class ColumnDef(
  name: String,           // Column name
  dataType: DataType,     // Chronon DataType (StringType, LongType, etc.)
  cardinality: Int        // Number of unique values to generate
)
```

### TableSchema
```scala
case class TableSchema(
  tableName: String,                      // Full table name (database.table)
  columns: Seq[ColumnDef],                // Column definitions
  rowCount: Int = 1000,                   // Total rows to generate
  partitionCount: Int = 10,               // Number of partitions
  tableType: TableType = Events,          // Events or Entities
  partitionColumn: Option[String] = None, // Override partition column name
  partitionFormat: Option[String] = None  // Override partition format
)
```

### SeederConfig
```scala
case class SeederConfig(
  tables: Seq[TableSchema]  // List of tables to seed
)
```

## How It Works

1. **Schema Definition**: Define your table schemas using `TableSchema` objects
2. **Class Loading**: The seeder loads your schema class at runtime using reflection
3. **Data Generation**: Uses `DataFrameGen` from spark.test to generate realistic data based on cardinalities
4. **Table Writing**: Uses `TableUtils` and FormatProvider to write data in the configured format
5. **Cloud Integration**: Automatically integrates with your cloud provider (AWS Glue, GCP BigQuery, etc.)

## Testing

Run tests for the seeder module:
```bash
./mill seeder.test
```

## Integration with Chronon Workflows

The seeder is designed to work seamlessly with Chronon:
- Generates data that can be consumed by Chronon GroupBys and Joins
- Respects partition columns and formats expected by Chronon
- Uses the same TableUtils infrastructure as Chronon backfills
- Compatible with all FormatProviders (AWS, GCP, local)

## Troubleshooting

### Compilation Issues
If you encounter compilation issues, ensure dependencies are built:
```bash
./mill spark.compile
./mill spark.test.compile
./mill aggregator.test.compile
./mill seeder.compile
```

### ClassNotFoundException at Runtime
Make sure you're using the assembly JAR which includes all dependencies:
```bash
./mill seeder.assembly
```

### Table Already Exists
The seeder uses `SaveMode.Overwrite` by default. If you encounter issues:
- Check table permissions in your cloud provider
- Verify the table location is writable
- Ensure the format provider is correctly configured

## Contributing

When adding new features to the seeder:
1. Update this README with usage examples
2. Add tests to validate functionality
3. Ensure compatibility with all supported FormatProviders
4. Follow the existing code patterns in TableSchema.scala

## License

Licensed under the Apache License, Version 2.0. See the LICENSE.txt file in the repository root.
