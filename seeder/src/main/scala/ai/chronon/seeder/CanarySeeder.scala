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

package ai.chronon.seeder

import ai.chronon.api._

/** Canary seeder schema for the AWS demo join: python/test/canary/compiled/joins/aws/demo.v1__1
  *
  * This generates test data for the three raw demo tables required by the canary:
  * - demo.user_activities (event data with event_time partition)
  * - demo.dim_listings (listing snapshots with ds partition)
  * - demo.dim_merchants (merchant snapshots with ds partition)
  *
  * These raw tables feed into the export staging queries which then feed the join.
  *
  * To use this:
  * ./mill seeder.compile
  * ./mill seeder.assembly
  *
  * spark-submit --class ai.chronon.seeder.DataSeeder \
  *   out/seeder/2.12.18/assembly.dest/out.jar \
  *   --seeder-class ai.chronon.seeder.CanarySeeder \
  *   --file-format PARQUET
  */
object CanarySeeder extends SeederSchema {

  // User activities event table - source for event data
  // This will have event_time as partition which gets converted to ds by the export
  val userActivities = TableSchema(
    tableName = "demo.user_activities",
    columns = Seq(
      ColumnDef("event_id", StringType, cardinality = 10000),
      ColumnDef("user_id", StringType, cardinality = 100),
      ColumnDef("listing_id", StringType, cardinality = 500),
      ColumnDef("event_type", StringType, cardinality = 5), // view, click, purchase, favorite, add_to_cart
      ColumnDef("device_type", StringType, cardinality = 3), // mobile, desktop, tablet
      ColumnDef("event_time_ms", LongType, cardinality = 10000)
    ),
    rowCount = 2000,
    partitionCount = 30,
    tableType = Events,
    partitionColumn = Some("event_time"),
    partitionFormat = Some("yyyy-MM-dd HH:mm:ss")
  )

  // Listing dimension table - snapshot data
  val dimListings = TableSchema(
    tableName = "demo.dim_listings",
    columns = Seq(
      ColumnDef("listing_id", StringType, cardinality = 500),
      ColumnDef("merchant_id", StringType, cardinality = 100),
      ColumnDef("headline", StringType, cardinality = 500),
      ColumnDef("brief_description", StringType, cardinality = 500),
      ColumnDef("long_description", StringType, cardinality = 500),
      ColumnDef("primary_category", StringType, cardinality = 20),
      ColumnDef("price_cents", LongType, cardinality = 1000),
      ColumnDef("currency", StringType, cardinality = 5), // USD, EUR, GBP, etc.
      ColumnDef("inventory_count", LongType, cardinality = 1000),
      ColumnDef("is_active", LongType, cardinality = 2), // 0 or 1
      ColumnDef("weight_grams", LongType, cardinality = 500),
      ColumnDef("main_image_path", StringType, cardinality = 500),
      ColumnDef("secondary_image_paths", StringType, cardinality = 500),
      ColumnDef("tags", StringType, cardinality = 10)
    ),
    rowCount = 100,
    partitionCount = 30,
    tableType = Entities
  )

  // Merchant dimension table - snapshot data
  val dimMerchants = TableSchema(
    tableName = "demo.dim_merchants",
    columns = Seq(
      ColumnDef("merchant_id", StringType, cardinality = 100),
      ColumnDef("primary_category", StringType, cardinality = 20)
    ),
    rowCount = 50,
    partitionCount = 30,
    tableType = Entities
  )

  // Return all tables to seed
  override def config: SeederConfig = SeederConfig(
    Seq(userActivities, dimListings, dimMerchants)
  )
}
