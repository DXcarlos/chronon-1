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

import scala.reflect.runtime.universe

/** Base trait for seeder schema files.
  * Users create objects extending this trait to define their table schemas.
  *
  * Example:
  * {{{
  * object MySeeder extends SeederSchema {
  *   import ai.chronon.api._
  *
  *   val userEvents = TableSchema(
  *     tableName = "db.user_events",
  *     columns = Seq(
  *       ColumnDef("user_id", StringType, cardinality = 1000),
  *       ColumnDef("event_type", StringType, cardinality = 10),
  *       ColumnDef("value", LongType, cardinality = 10000)
  *     ),
  *     rowCount = 50000,
  *     partitionCount = 30,
  *     tableType = Events
  *   )
  *
  *   override def config = SeederConfig(Seq(userEvents))
  * }
  * }}}
  */
trait SeederSchema {
  def config: SeederConfig
}

/** Loads seeder schema definitions from compiled Scala classes
  */
object SchemaLoader {

  /** Load a seeder schema from a fully qualified class name
    *
    * @param className Fully qualified class name (e.g., "com.example.MySeeder")
    * @return SeederConfig loaded from the class
    */
  def load(className: String): SeederConfig = {
    try {
      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule(className)
      val obj = runtimeMirror.reflectModule(module)
      val instance = obj.instance

      instance match {
        case schema: SeederSchema => schema.config
        case _ =>
          throw new IllegalArgumentException(
            s"Class $className does not extend SeederSchema trait"
          )
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to load seeder schema from class $className", e)
    }
  }

  /** Validate a seeder config
    */
  def validate(config: SeederConfig): Unit = {
    require(config.tables.nonEmpty, "SeederConfig must contain at least one table")

    config.tables.foreach { table =>
      require(table.tableName.nonEmpty, "Table name cannot be empty")
      require(table.columns.nonEmpty, "Table must have at least one column")
      require(table.rowCount > 0, "Row count must be positive")
      require(table.partitionCount > 0, "Partition count must be positive")

      table.columns.foreach { col =>
        require(col.name.nonEmpty, "Column name cannot be empty")
        require(col.cardinality > 0, "Column cardinality must be positive")
      }
    }
  }
}
