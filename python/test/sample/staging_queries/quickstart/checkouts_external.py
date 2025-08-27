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

from ai.chronon.staging_query import StagingQuery, TableDependency

query = """
    SELECT
        purchases.ds,
        purchases.ts as purchase_ts,
        purchases.user_id,
        purchases.purchase_price,
        checkouts.return_id,
        checkouts.refund_amt,
        checkouts.product_id,
        checkouts.ts as checkout_ts
    FROM data.purchases AS purchases 
    LEFT OUTER JOIN data.checkouts_external AS checkouts
    USING (user_id)
    WHERE purchases.ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

checkouts_query = StagingQuery(
    query=query,
    output_namespace="data",
    dependencies=[
        TableDependency(table="data.purchases", partition_column="ds"),
        TableDependency(table="data.checkouts_external", partition_column="ds")
    ],
    version=0
)
