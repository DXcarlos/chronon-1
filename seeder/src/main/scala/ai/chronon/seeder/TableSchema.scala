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

import ai.chronon.aggregator.test.Column
import ai.chronon.api.DataType

/** Defines the data type for generated tables
  */
sealed trait TableType
case object Events extends TableType // Event tables with timestamps
case object Entities extends TableType // Entity tables with snapshots

/** Configuration for a column in a table schema
  *
  * @param name Column name
  * @param dataType Chronon data type (e.g., LongType, StringType, etc.)
  * @param cardinality Number of unique values to generate
  */
case class ColumnDef(
    name: String,
    dataType: DataType,
    cardinality: Int
) {
  def toColumn: Column = Column(name, dataType, cardinality)
}

/** Schema definition for a table to seed
  *
  * @param tableName Name of the table to create
  * @param columns Column definitions
  * @param rowCount Number of rows to generate
  * @param partitionCount Number of partitions to generate
  * @param tableType Type of table (Events or Entities)
  * @param partitionColumn Optional partition column name (defaults to standard)
  * @param partitionFormat Optional partition format (defaults to standard)
  */
case class TableSchema(
    tableName: String,
    columns: Seq[ColumnDef],
    rowCount: Int = 1000,
    partitionCount: Int = 10,
    tableType: TableType = Events,
    partitionColumn: Option[String] = None,
    partitionFormat: Option[String] = None
)

/** Collection of table schemas to seed
  *
  * @param tables Sequence of table schemas to generate
  */
case class SeederConfig(
    tables: Seq[TableSchema]
)
