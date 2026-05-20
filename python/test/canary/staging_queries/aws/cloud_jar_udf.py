"""
Config that uses a remote file in spark

Copy of transform in repo://scripts/canary_resources/transform.py
"""
from ai.chronon.types import StagingQuery


v1 = StagingQuery(
    setups=[
        f"ADD FILE 's3://zipline-spark-libs/canary/cloud-jar-udf/transform.py'"
    ],
    query="""
SELECT
    TRANSFORM (user, score, {{ end_date }}) USING 'python3 transform.py' AS (formatted_name STRING, multiplied_score INT, ds STRING)
FROM (
  SELECT 'user' as user, 1 as score
) sq
""",
    output_namespace="data",
    dependencies=[],
    version=0,
    step_days=30,
)
