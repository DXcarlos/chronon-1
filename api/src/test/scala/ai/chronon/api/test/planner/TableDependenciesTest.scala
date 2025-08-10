package ai.chronon.api.test.planner

import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.planner.TableDependencies
import ai.chronon.api.{Builders, TimeUnit, Window}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TableDependenciesTest extends AnyFlatSpec with Matchers {

  "TableDependencies.fromTable" should "handle left Source with null query (default behavior)" in {
    val table = "test.events_table"
    val shift = Some(WindowUtils.Day)

    val result = TableDependencies.fromTable(table, query = null, shift)

    result should not be null
    result.getTableInfo should not be null
    result.getTableInfo.getTable should equal(table)

    // When query is null, partition info should not be set
    result.getTableInfo.getPartitionColumn should be(null)
    result.getTableInfo.getPartitionFormat should be(null)
    result.getTableInfo.getPartitionInterval should be(null)

    // Offsets should use the shift parameter (WindowUtils.Day)
    result.getStartOffset should equal(shift.get)
    result.getEndOffset should equal(shift.get)

    // Cut-offs should be null
    result.getStartCutOff should be(null)
    result.getEndCutOff should be(null)
  }

  it should "handle left Source with null query and no shift (default behavior)" in {
    val table = "test.events_table"

    val result = TableDependencies.fromTable(table, query = null, shift = None)

    result should not be null
    result.getTableInfo should not be null
    result.getTableInfo.getTable should equal(table)

    // When query is null, partition info should not be set
    result.getTableInfo.getPartitionColumn should be(null)
    result.getTableInfo.getPartitionFormat should be(null)
    result.getTableInfo.getPartitionInterval should be(null)

    // Offsets should use zero when no shift is provided
    result.getStartOffset should equal(WindowUtils.zero())
    result.getEndOffset should equal(WindowUtils.zero())

    // Cut-offs should be null
    result.getStartCutOff should be(null)
    result.getEndCutOff should be(null)
  }

  it should "handle left Source with non-null query (validate partitionLag, start/end partitions)" in {
    val table = "test.events_table"
    val partitionLag = new Window(2, TimeUnit.DAYS)
    val startPartition = "2023-01-01"
    val endPartition = "2023-12-31"
    val partitionColumn = "ds"
    val partitionFormat = "yyyy-MM-dd"
    val partitionInterval = WindowUtils.Day

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = partitionColumn
    )
    query.setPartitionFormat(partitionFormat)
    query.setPartitionInterval(partitionInterval)
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query)

    result should not be null
    result.getTableInfo should not be null
    result.getTableInfo.getTable should equal(table)

    // Partition information should be preserved from query
    result.getTableInfo.getPartitionColumn should equal(partitionColumn)
    result.getTableInfo.getPartitionFormat should equal(partitionFormat)
    result.getTableInfo.getPartitionInterval should equal(partitionInterval)

    // Offsets should use partitionLag from query
    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)

    // Cut-offs should use start/end partitions from query
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "handle left Source with non-null query but no partitionLag" in {
    val table = "test.events_table"
    val startPartition = "2023-01-01"
    val endPartition = "2023-12-31"
    val partitionColumn = "ds"

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = partitionColumn
    )

    val result = TableDependencies.fromTable(table, query)

    result should not be null

    // When no partitionLag, should default to zero
    result.getStartOffset should equal(WindowUtils.zero())
    result.getEndOffset should equal(WindowUtils.zero())

    // Cut-offs should still be set
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "handle left Source with non-null query and shift parameter" in {
    val table = "test.events_table"
    val partitionLag = new Window(1, TimeUnit.DAYS)
    val shift = Some(new Window(3, TimeUnit.DAYS))
    val startPartition = "2023-01-01"
    val endPartition = "2023-12-31"

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = "ds"
    )
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query, shift)

    result should not be null

    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)

    // Cut-offs should still use query values
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "prioritize partitionLag over shiftwhen both are provided" in {
    val table = "test.events_table"
    val partitionLag = new Window(1, TimeUnit.DAYS)
    val shift = Some(new Window(5, TimeUnit.HOURS))

    val query = Builders.Query()
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query, shift)

    // shift should take precedence over partitionLag
    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)
  }

  it should "handle complete query with all partition settings" in {
    val table = "test.detailed_events"
    val partitionLag = new Window(1, TimeUnit.HOURS)
    val startPartition = "2023-06-01"
    val endPartition = "2023-06-30"
    val partitionColumn = "partition_date"
    val partitionFormat = "yyyy-MM-dd"
    val partitionInterval = new Window(1, TimeUnit.DAYS)

    val query = Builders.Query(
      startPartition = startPartition,
      endPartition = endPartition,
      partitionColumn = partitionColumn
    )
    query.setPartitionFormat(partitionFormat)
    query.setPartitionInterval(partitionInterval)
    query.setPartitionLag(partitionLag)

    val result = TableDependencies.fromTable(table, query)

    result should not be null
    val tableInfo = result.getTableInfo

    // Validate all table info fields
    tableInfo.getTable should equal(table)
    tableInfo.getPartitionColumn should equal(partitionColumn)
    tableInfo.getPartitionFormat should equal(partitionFormat)
    tableInfo.getPartitionInterval should equal(partitionInterval)

    // Validate dependency timing
    result.getStartOffset should equal(partitionLag)
    result.getEndOffset should equal(partitionLag)
    result.getStartCutOff should equal(startPartition)
    result.getEndCutOff should equal(endPartition)
  }

  it should "handle minimal query with only required fields" in {
    val table = "test.minimal_table"
    val partitionColumn = "dt"

    val query = Builders.Query(partitionColumn = partitionColumn)

    val result = TableDependencies.fromTable(table, query)

    result should not be null
    val tableInfo = result.getTableInfo

    tableInfo.getTable should equal(table)
    tableInfo.getPartitionColumn should equal(partitionColumn)

    // Optional fields should be null when not provided
    tableInfo.getPartitionFormat should be(null)
    tableInfo.getPartitionInterval should be(null)

    // Should default to zero offsets when no lag specified
    result.getStartOffset should equal(WindowUtils.zero())
    result.getEndOffset should equal(WindowUtils.zero())
    result.getStartCutOff should be(null)
    result.getEndCutOff should be(null)
  }
}
