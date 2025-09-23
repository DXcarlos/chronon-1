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

package ai.chronon.spark.other

import ai.chronon.spark.Driver.LocalExportTableAbility
import ai.chronon.spark.Driver.OfflineSubcommand
import ai.chronon.spark.LocalTableExporter
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.rogach.scallop.ScallopConf
import org.scalatest.flatspec.AnyFlatSpec

class LocalExportTableAbilityTest extends AnyFlatSpec {
  class TestArgs(args: Array[String], localTableExporter: LocalTableExporter)
      extends ScallopConf(args)
      with OfflineSubcommand
      with LocalExportTableAbility {
    verify()

    override def subcommandName: String = "test"

    override def buildSparkSession(): SparkSession = SparkSessionBuilder.build(subcommandName, local = true)

    protected override def buildLocalTableExporter(tableUtils: TableUtils): LocalTableExporter = localTableExporter
  }

  it should "local table exporter is not used when not in local mode" in {
    val argList = Seq("--conf-path", "joins/team/example_join.v1", "--end-date", "2023-03-03")
    val args = new TestArgs(argList.toArray, mock(classOf[LocalTableExporter]))
    assertFalse(args.shouldExport())
  }

  it should "local table exporter is not used when not export path is not specified" in {
    val argList =
      Seq("--conf-path", "joins/team/example_join.v1", "--end-date", "2023-03-03", "--local-data-path", "somewhere")
    val args = new TestArgs(argList.toArray, mock(classOf[LocalTableExporter]))
    assertFalse(args.shouldExport())
  }

  it should "local table exporter is used when necessary" in {
    val targetOutputPath = "path/to/somewhere"
    val targetFormat = "parquet"
    val prefix = "test_prefix"
    val argList = Seq(
      "--conf-path",
      "joins/team/example_join.v1",
      "--end-date",
      "2023-03-03",
      "--local-data-path",
      "somewhere",
      "--local-table-export-path",
      targetOutputPath,
      "--local-table-export-format",
      targetFormat,
      "--local-table-export-prefix",
      prefix
    )
    val exporter = mock(classOf[LocalTableExporter])
    val tableUtils = mock(classOf[TableUtils])
    doNothing().when(exporter).exportTable(any())
    val args = new TestArgs(argList.toArray, exporter)

    assertEquals(targetOutputPath, args.localTableExportPath())
    assertEquals(targetFormat, args.localTableExportFormat())
    assertEquals(prefix, args.localTableExportPrefix())

    assertTrue(args.shouldExport())

    args.exportTableToLocal("test.test_table", tableUtils)
    verify(exporter, times(1)).exportTable(any())
  }
}
