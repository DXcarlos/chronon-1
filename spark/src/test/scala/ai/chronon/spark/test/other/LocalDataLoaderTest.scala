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

package ai.chronon.spark.test.other

import ai.chronon.spark.submission.SparkSessionBuilder
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.junit.AfterClass

import java.io.File

object LocalDataLoaderTest {

  val tmpDir: File = Files.createTempDir()

  val spark: SparkSession =
    SparkSessionBuilder.build("LocalDataLoaderTest", local = true, localWarehouseLocation = Some(tmpDir.getPath))

  @AfterClass
  def teardown(): Unit = {
    FileUtils.deleteDirectory(tmpDir)
  }
}

// Not needed since we are doing this via interactive.py

//class LocalDataLoaderTest extends AnyFlatSpec {
//
//  it should "load data file as table should be correct" in {
//    val resourceURL = Option(getClass.getResource("/local_data_csv/test_table_1_data.csv"))
//      .getOrElse(throw new IllegalStateException("Required test resource not found"))
//    val file = new File(resourceURL.getFile)
//    val nameSpaceAndTable = "test.table"
//    LocalDataLoader.loadDataFileAsTable(file, spark, nameSpaceAndTable)
//
//    val loadedDataDf = spark.sql(s"SELECT * FROM $nameSpaceAndTable")
//    val expectedColumns = Set("id_listing_view_event", "id_product", "dim_product_type", "ds")
//
//    loadedDataDf.columns.foreach(column => expectedColumns.contains(column))
//    assertEquals(3, loadedDataDf.count())
//  }
//
//  it should "load data recursively should be correct" in {
//    val resourceURI = getClass.getResource("/local_data_csv")
//    val path = new File(resourceURI.getFile)
//    LocalDataLoader.loadDataRecursively(path, spark)
//
//    val loadedDataDf = spark.sql("SELECT * FROM local_data_csv.test_table_1_data")
//    val expectedColumns = Set("id_listing_view_event", "id_product", "dim_product_type", "ds")
//
//    loadedDataDf.columns.foreach(column => expectedColumns.contains(column))
//    assertEquals(3, loadedDataDf.count())
//  }
//}
