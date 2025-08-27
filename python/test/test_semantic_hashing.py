#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from gen_thrift.api.ttypes import (
    Aggregation,
    Derivation,
    EventSource,
    GroupBy,
    Join,
    JoinPart,
    MetaData,
    Operation,
    Query,
    Source,
)
from ai.chronon.cli.compile.column_hashing import (
    _extract_source_semantic_info,
    compute_group_by_columns_hashes,
    compute_join_column_hashes,
)


class TestSemanticHashing:
    """Test suite for semantic hashing functionality."""

    def test_identical_group_bys_have_same_hashes(self):
        """Test that two identical GroupBy objects produce identical semantic hashes if minor versions differ."""
        # Create first GroupBy
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="price",
                    operation=Operation.SUM,
                )
            ],
            metaData=MetaData(name="test_gb1__0", version="0") # Minor version 0
        )

        # Create second GroupBy (identical to first)
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="price",
                    operation=Operation.SUM,
                )
            ],
            metaData=MetaData(name="test_gb1__1", version="1") # Minor version 1
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # Should have same hashes for all columns
        assert hashes1 == hashes2
        assert len(hashes1) == len(hashes2)

    def test_identical_group_bys_major_version_hash_mismatch(self):
        """Test that two identical GroupBy objects produce different semantic hashes if major versions differ."""
        # Create first GroupBy
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="price",
                    operation=Operation.SUM,
                )
            ],
            metaData=MetaData(name="test_gb1__0", version="0") # Major version gb1
        )

        # Create second GroupBy (identical to first)
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="price",
                    operation=Operation.SUM,
                )
            ],
            metaData=MetaData(name="test_gb2__0", version="0") # Major version gb2
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # Should have same hashes for all columns
        assert hashes1 != hashes2
        assert len(hashes1) == len(hashes2)

    def test_different_select_expressions_produce_different_hashes(self):
        """Test that GroupBys with different select expressions produce different hashes."""
        # Create first GroupBy with simple select
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={
                                "user_id": "user_id",
                                "price": "price",
                                "quantity": "quantity",
                            },
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM),
                Aggregation(inputColumn="quantity", operation=Operation.COUNT),
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Create second GroupBy with different select expression for price
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={
                                "user_id": "user_id",
                                "price": "price * 1.1",  # Different expression
                                "quantity": "quantity",
                            },
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM),
                Aggregation(inputColumn="quantity", operation=Operation.COUNT),
            ],
            metaData=MetaData(name="test_gb1__1", version="1"),
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # The price_sum column should have different hashes
        assert hashes1["price_sum"] != hashes2["price_sum"]
        
        # The quantity_count column should have same hash (same expression)
        assert hashes1["quantity_count"] == hashes2["quantity_count"]
        
        # The user_id key should have same hash (same expression)
        assert hashes1["user_id"] == hashes2["user_id"]

    def test_different_tables_produce_different_hashes(self):
        """Test that GroupBys with different source tables produce different hashes."""
        # Create first GroupBy with table1
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="table1",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Create second GroupBy with table2
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="table2",  # Different table
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # All columns should have different hashes
        assert hashes1["price_sum"] != hashes2["price_sum"]
        assert hashes1["user_id"] != hashes2["user_id"]

    def test_different_filters_produce_different_hashes(self):
        """Test that GroupBys with different filter conditions produce different hashes."""
        # Create first GroupBy with one filter
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Create second GroupBy with different filter
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 10"],  # Different filter
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # All columns should have different hashes
        assert hashes1["price_sum"] != hashes2["price_sum"]
        assert hashes1["user_id"] != hashes2["user_id"]


    def test_time_column_expressions_produce_different_hashes(self):
        """Test that different time column expressions produce different hashes."""
        # Create first GroupBy with simple time column
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Create second GroupBy with different time column expression
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="CAST(timestamp AS DOUBLE)",  # Different time column expression
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # All columns should have different hashes due to different time column
        assert hashes1["price_sum"] != hashes2["price_sum"]
        assert hashes1["user_id"] != hashes2["user_id"]


    def test_derivations_base_col_change_produces_different_hashes(self):
        """Test that GroupBys with different base col semantics produce different hashes in derivations."""
        # Create first GroupBy
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            derivations=[
                Derivation(name="*", expression="*"),  # Include all columns
                Derivation(name="price_sum_doubled", expression="price_sum * 2"),
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Create second GroupBy with derivations
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price * 10"}, # Note, price expression is different
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            derivations=[
                Derivation(name="*", expression="*"),  # Include all columns
                Derivation(name="price_sum_doubled", expression="price_sum * 2"),
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # GB2 should have an additional derived column
        assert "price_sum_doubled" in hashes2
        assert "price_sum_doubled" in hashes1

        # The base price_sum column should have differing hashes
        assert hashes1["price_sum"] != hashes2["price_sum"]

    def test_derivations_time_col_change_produces_different_hashes(self):
        """Test that GroupBys with different derivations produce different hashes."""
        # Create first GroupBy
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            derivations=[
                Derivation(name="*", expression="*"),  # Include all columns
                Derivation(name="price_sum_doubled", expression="price_sum * 2"),
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Create second GroupBy with derivations
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp * 10",  # Note, price expression is different
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            derivations=[
                Derivation(name="*", expression="*"),  # Include all columns
                Derivation(name="price_sum_doubled", expression="price_sum * 2"),
            ],
            metaData=MetaData(name="test_gb1__0", version="0"),
        )

        # Get column hashes
        hashes1 = compute_group_by_columns_hashes(gb1)
        hashes2 = compute_group_by_columns_hashes(gb2)

        # GB2 should have an additional derived column
        assert "price_sum_doubled" in hashes2
        assert "price_sum_doubled" in hashes1

        # The base price_sum column should have differing hashes
        assert hashes1["price_sum"] != hashes2["price_sum"]


    def test_join_semantic_hashing(self):
        """Test semantic hashing for Join objects."""
        # Create a simple join
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds="user_id",
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="right_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="test_group_by"),
                    ),
                    prefix="purchase",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False
        )

        # Get column hashes
        hashes = compute_join_column_hashes(join)

        # Should have hashes for left columns and join part columns
        assert "user_id" in hashes
        assert "timestamp" in hashes
        assert "purchase_user_id_price_sum" in hashes


    def test_join_semantic_hashing_long_names(self):
        """Test semantic hashing for Join objects."""
        # Create a simple join
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds=["user_id"],
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="right_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="test_group_by"),
                    ),
                    prefix="purchase",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=True
        )

        # Get column hashes
        hashes = compute_join_column_hashes(join)

        # Should have hashes for left columns and join part columns
        assert "user_id" in hashes
        assert "timestamp" in hashes
        assert "purchase_test_group_by_price_sum" in hashes


    def test_extract_source_semantic_info(self):
        """Test the _extract_source_semantic_info function."""
        source = Source(
            events=EventSource(
                table="test_table",
                query=Query(
                    selects={"user_id": "user_id", "price": "price"},
                    wheres=["price > 0"],
                    timeColumn="timestamp",
                ),
            )
        )

        components = _extract_source_semantic_info(source, ["user_id"])

        # Should include table, filters, time column, and key column expressions
        expected = [
            'cumulative:',
            "filters:['price > 0']",
            'mutation_table:',
            'select:user_id=user_id',
            'table:test_table',
            'time_column:timestamp=timestamp'
        ]

        assert components == expected, f"Mismatch:\nExpected: {expected}\nGot: {components}"


    def test_join_groupby_base_column_change_affects_derivation_semantics(self):
        """Test that GroupBy base column semantic change affects Join derivation semantic change."""
        # Create base GroupBy
        base_group_by = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="right_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_group_by__0", version="0"),
        )

        # Create first join with base GroupBy
        join1 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds="timestamp",
            joinParts=[
                JoinPart(
                    groupBy=base_group_by,
                    prefix="purchase",
                )
            ],
            derivations=[
                Derivation(name="*", expression="*"),  # Include all columns
                Derivation(name="price_doubled", expression="purchase_user_id_price_sum * 2"),
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Create modified GroupBy with different base column semantics
        modified_group_by = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="right_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price * 1.1"},  # Different price expression
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_group_by__0", version="0"),
        )

        # Create second join with modified GroupBy
        join2 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds=["user_id", "timestamp"],
            joinParts=[
                JoinPart(
                    groupBy=modified_group_by,
                    prefix="purchase",
                )
            ],
            derivations=[
                Derivation(name="*", expression="*"),  # Include all columns
                Derivation(name="price_doubled", expression="purchase_user_id_price_sum * 2"),
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Get column hashes
        hashes1 = compute_join_column_hashes(join1)
        hashes2 = compute_join_column_hashes(join2)

        # The base column should have different hashes
        assert hashes1["purchase_user_id_price_sum"] != hashes2["purchase_user_id_price_sum"]
        
        # The derivation should also have different hashes since it depends on the base column
        assert hashes1["price_doubled"] != hashes2["price_doubled"]

        # Left side columns should be the same
        assert hashes1["user_id"] == hashes2["user_id"]
        assert hashes1["timestamp"] == hashes2["timestamp"]

    def test_join_timestamp_change_affects_semantics(self):
        """Test that changing join timestamp changes semantics."""
        # Create first join with simple timestamp
        join1 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds="user_id",
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="right_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="test_group_by"),
                    ),
                    prefix="purchase",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Create second join with different timestamp expression
        join2 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="CAST(timestamp AS DOUBLE)",  # Different time column expression
                    ),
                )
            ),
            rowIds="timestamp",
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="right_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="test_group_by"),
                    ),
                    prefix="purchase",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Get column hashes
        hashes1 = compute_join_column_hashes(join1)
        hashes2 = compute_join_column_hashes(join2)

        # All columns should have different hashes due to different left timestamp
        assert hashes1["timestamp"] != hashes2["timestamp"]
        assert hashes1["purchase_user_id_price_sum"] != hashes2["purchase_user_id_price_sum"]

    def test_join_part_keymapping_changes_semantics(self):
        """Test that changing join_part keyMapping changes semantics."""
        # Create base GroupBy
        base_group_by = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="right_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_group_by"),
        )

        # Create first join without keyMapping
        join1 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "user": "user", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds="user",
            joinParts=[
                JoinPart(
                    groupBy=base_group_by,
                    prefix="purchase",
                    # No keyMapping
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Create second join with keyMapping
        join2 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "user": "user", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds=["user_id"],
            joinParts=[
                JoinPart(
                    groupBy=base_group_by,
                    prefix="purchase",
                    keyMapping={"user_id": "user"},  # Explicit keyMapping
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Get column hashes
        hashes1 = compute_join_column_hashes(join1)
        hashes2 = compute_join_column_hashes(join2)

        # The join part columns should have different hashes due to different keyMapping
        assert hashes1["purchase_user_id_price_sum"] != hashes2["purchase_user_id_price_sum"]

        # Left side columns should be the same
        assert hashes1["user_id"] == hashes2["user_id"]
        assert hashes1["timestamp"] == hashes2["timestamp"]

    def test_join_key_column_expression_affects_only_relevant_joinparts(self):
        """Test that changing join key column expression affects only joinparts that use that key."""
        # Create first join
        join1 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "product_id": "product_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds=["user_id", "product_id"],
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="user_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="user_group_by"),
                    ),
                    prefix="user",
                ),
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="product_table",
                                    query=Query(
                                        selects={"product_id": "product_id", "rating": "rating"},
                                        wheres=["rating > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["product_id"],
                        aggregations=[
                            Aggregation(inputColumn="rating", operation=Operation.AVERAGE)
                        ],
                        metaData=MetaData(name="product_group_by"),
                    ),
                    prefix="product",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Create second join with different user_id expression
        join2 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "UPPER(user_id)", "product_id": "product_id", "timestamp": "timestamp"},  # Different user_id expression
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds="product_id",
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="user_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="user_group_by"),
                    ),
                    prefix="user",
                ),
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="product_table",
                                    query=Query(
                                        selects={"product_id": "product_id", "rating": "rating"},
                                        wheres=["rating > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["product_id"],
                        aggregations=[
                            Aggregation(inputColumn="rating", operation=Operation.AVERAGE)
                        ],
                        metaData=MetaData(name="product_group_by"),
                    ),
                    prefix="product",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Get column hashes
        hashes1 = compute_join_column_hashes(join1)
        hashes2 = compute_join_column_hashes(join2)

        print("\n\nhashes1:")
        print(hashes1)
        print("\n\nhashes2:")
        print(hashes2)

        # Left side user_id should be different (expression changed)
        assert hashes1["user_id"] != hashes2["user_id"]

        # Left side product_id should be the same (expression unchanged)
        assert hashes1["product_id"] == hashes2["product_id"]

        # Left side timestamp should be the same (expression unchanged)
        assert hashes1["timestamp"] == hashes2["timestamp"]

        # User join part should be affected (uses user_id key that changed)
        assert hashes1["user_user_id_price_sum"] != hashes2["user_user_id_price_sum"]

        # Product join part should NOT be affected (uses product_id key that didn't change)
        assert hashes1["product_product_id_rating_average"] == hashes2["product_product_id_rating_average"]

    def test_exclude_keys_parameter(self):
        """Test the exclude_keys parameter in get_group_by_output_columns."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="test_table",
                        query=Query(
                            selects={"user_id": "user_id", "price": "price"},
                            wheres=["price > 0"],
                            timeColumn="timestamp",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="price", operation=Operation.SUM)
            ],
            metaData=MetaData(name="test_gb"),
        )

        # Get hashes with keys included
        hashes_with_keys = compute_group_by_columns_hashes(gb, exclude_keys=False)
        
        # Get hashes with keys excluded
        hashes_without_keys = compute_group_by_columns_hashes(gb, exclude_keys=True)

        # Should have user_id in the first result but not the second
        assert "user_id" in hashes_with_keys
        assert "user_id" not in hashes_without_keys

        # Both should have the aggregated column
        assert "price_sum" in hashes_with_keys
        assert "price_sum" in hashes_without_keys

        # The hash for price_sum should be the same in both cases
        assert hashes_with_keys["price_sum"] == hashes_without_keys["price_sum"]

    def test_none_values_handling(self):
        """Test that semantic hashing handles None values gracefully."""
        # Create GroupBy with minimal fields to test None handling
        gb = GroupBy(
            sources=None,  # This would normally cause issues
            keyColumns=None,
            aggregations=None,
            metaData=MetaData(name="test_gb"),
        )

        # This should not raise an exception
        try:
            hashes = compute_group_by_columns_hashes(gb)
            # If we get here, the function handled None values gracefully
            assert isinstance(hashes, dict)
        except (TypeError, AttributeError):
            # Expected for incomplete objects
            pass

    def test_adding_left_columns_does_not_change_existing_semantics(self):
        """Test that adding columns to the left side does not change semantic hashes of existing columns."""
        # Create first join with basic left columns
        join1 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={"user_id": "user_id", "timestamp": "timestamp"},
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds="user_id",
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="right_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="test_group_by"),
                    ),
                    prefix="purchase",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Create second join with additional left columns
        join2 = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(
                        selects={
                            "user_id": "user_id", 
                            "timestamp": "timestamp",
                            "session_id": "session_id",  # Additional column
                            "device_type": "device_type",  # Additional column
                        },
                        timeColumn="timestamp",
                    ),
                )
            ),
            rowIds=["session_id"],
            joinParts=[
                JoinPart(
                    groupBy=GroupBy(
                        sources=[
                            Source(
                                events=EventSource(
                                    table="right_table",
                                    query=Query(
                                        selects={"user_id": "user_id", "price": "price"},
                                        wheres=["price > 0"],
                                        timeColumn="timestamp",
                                    ),
                                )
                            )
                        ],
                        keyColumns=["user_id"],
                        aggregations=[
                            Aggregation(inputColumn="price", operation=Operation.SUM)
                        ],
                        metaData=MetaData(name="test_group_by"),
                    ),
                    prefix="purchase",
                )
            ],
            metaData=MetaData(name="test_join"),
            useLongNames=False,
        )

        # Get column hashes
        hashes1 = compute_join_column_hashes(join1)
        hashes2 = compute_join_column_hashes(join2)

        # Existing columns should have the same hashes
        assert hashes1["user_id"] == hashes2["user_id"]
        assert hashes1["timestamp"] == hashes2["timestamp"]
        assert hashes1["purchase_user_id_price_sum"] == hashes2["purchase_user_id_price_sum"]

        # New columns should be present in join2 but not in join1
        assert "session_id" not in hashes1
        assert "device_type" not in hashes1
        assert "session_id" in hashes2
        assert "device_type" in hashes2
