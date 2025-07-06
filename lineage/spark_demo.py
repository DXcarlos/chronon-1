#!/usr/bin/env python3
"""
PySpark demo with complex SQL query featuring CTEs, joins, groupBys, and list functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType
from pyspark.sql.functions import col, array, lit, to_timestamp
from datetime import datetime, timedelta
import json


def create_spark_session():
    """Create Spark session with appropriate configuration."""
    return SparkSession.builder \
        .appName("ColumnLineageDemo") \
        .master("local[1]") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.driver.host", "localhost") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()


def create_sample_data(spark):
    """Create sample tables with test data."""
    
    # Users table
    users_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", TimestampType(), True),
        StructField("preferences", ArrayType(StringType()), True)
    ])
    
    users_data = [
        ("u1", "alice", "alice@example.com", "US", datetime(2023, 1, 15), ["email", "push"]),
        ("u2", "bob", "bob@example.com", "UK", datetime(2023, 2, 20), ["sms", "email"]),
        ("u3", "charlie", "charlie@example.com", "CA", datetime(2023, 3, 10), ["push"]),
        ("u4", "diana", "diana@example.com", "US", datetime(2023, 1, 25), ["email", "push", "sms"]),
        ("u5", "eve", "eve@example.com", "DE", datetime(2023, 4, 5), ["email"])
    ]
    
    users_df = spark.createDataFrame(users_data, users_schema)
    users_df.createOrReplaceTempView("users")
    
    # Events table
    events_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("properties", ArrayType(StringType()), True),
        StructField("session_id", StringType(), True)
    ])
    
    base_time = datetime(2023, 5, 1)
    events_data = [
        ("e1", "u1", "page_view", base_time, ["home"], "s1"),
        ("e2", "u1", "click", base_time + timedelta(minutes=1), ["button", "cta"], "s1"),
        ("e3", "u1", "purchase", base_time + timedelta(minutes=5), ["product_123"], "s1"),
        ("e4", "u2", "page_view", base_time + timedelta(hours=1), ["product"], "s2"),
        ("e5", "u2", "click", base_time + timedelta(hours=1, minutes=2), ["add_to_cart"], "s2"),
        ("e6", "u3", "page_view", base_time + timedelta(hours=2), ["home"], "s3"),
        ("e7", "u3", "click", base_time + timedelta(hours=2, minutes=1), ["menu"], "s3"),
        ("e8", "u4", "page_view", base_time + timedelta(hours=3), ["product"], "s4"),
        ("e9", "u4", "click", base_time + timedelta(hours=3, minutes=1), ["button"], "s4"),
        ("e10", "u4", "purchase", base_time + timedelta(hours=3, minutes=10), ["product_456"], "s4"),
        ("e11", "u5", "page_view", base_time + timedelta(hours=4), ["about"], "s5")
    ]
    
    events_df = spark.createDataFrame(events_data, events_schema)
    events_df.createOrReplaceTempView("events")
    
    # Products table
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("tags", ArrayType(StringType()), True)
    ])
    
    products_data = [
        ("product_123", "Wireless Headphones", "Electronics", 99, ["audio", "wireless", "premium"]),
        ("product_456", "Coffee Maker", "Appliances", 149, ["kitchen", "coffee", "automatic"]),
        ("product_789", "Running Shoes", "Sports", 129, ["shoes", "running", "comfort"])
    ]
    
    products_df = spark.createDataFrame(products_data, products_schema)
    products_df.createOrReplaceTempView("products")
    
    # Sessions table
    sessions_schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("device_type", StringType(), True),
        StructField("channels", ArrayType(StringType()), True)
    ])
    
    sessions_data = [
        ("s1", "u1", base_time, base_time + timedelta(minutes=10), "desktop", ["organic", "direct"]),
        ("s2", "u2", base_time + timedelta(hours=1), base_time + timedelta(hours=1, minutes=15), "mobile", ["social", "facebook"]),
        ("s3", "u3", base_time + timedelta(hours=2), base_time + timedelta(hours=2, minutes=5), "tablet", ["search", "google"]),
        ("s4", "u4", base_time + timedelta(hours=3), base_time + timedelta(hours=3, minutes=20), "desktop", ["email", "newsletter"]),
        ("s5", "u5", base_time + timedelta(hours=4), base_time + timedelta(hours=4, minutes=8), "mobile", ["direct"])
    ]
    
    sessions_df = spark.createDataFrame(sessions_data, sessions_schema)
    sessions_df.createOrReplaceTempView("sessions")
    
    print("✅ Created sample tables: users, events, products, sessions")


def create_complex_query():
    """Create a complex SQL query with CTEs, joins, groupBys, and list functions."""
    return """
    WITH user_segments AS (
        SELECT 
            user_id,
            username,
            country,
            CASE 
                WHEN exists(preferences, x -> x = 'push') THEN 'push_enabled'
                WHEN exists(preferences, x -> x = 'email') THEN 'email_only'
                ELSE 'minimal'
            END AS preference_segment,
            preferences
        FROM users
        WHERE country IN ('US', 'UK', 'CA')
    ),
    
    session_events AS (
        SELECT 
            s.session_id,
            s.user_id,
            s.device_type,
            s.channels,
            collect_list(e.event_type) AS event_types,
            collect_list(e.properties) AS all_properties,
            count(*) AS event_count,
            max(e.event_timestamp) AS last_event_time
        FROM sessions s
        LEFT JOIN events e ON s.session_id = e.session_id
        GROUP BY s.session_id, s.user_id, s.device_type, s.channels
    ),
    
    engagement_metrics AS (
        SELECT 
            se.user_id,
            se.device_type,
            count(DISTINCT se.session_id) AS session_count,
            sum(se.event_count) AS total_events,
            avg(se.event_count) AS avg_events_per_session,
            collect_set(flatten(se.all_properties)) AS unique_properties,
            CASE 
                WHEN exists(flatten(collect_list(se.event_types)), x -> x = 'purchase') THEN 'converter'
                WHEN exists(flatten(collect_list(se.event_types)), x -> x = 'click') THEN 'engaged'
                ELSE 'browser'
            END AS user_type,
            array_distinct(flatten(collect_list(se.channels))) AS all_channels
        FROM session_events se
        GROUP BY se.user_id, se.device_type
    ),
    
    final_enriched AS (
        SELECT 
            us.user_id,
            us.username,
            us.country,
            us.preference_segment,
            em.device_type,
            em.session_count,
            em.total_events,
            em.avg_events_per_session,
            em.user_type,
            em.all_channels,
            CASE 
                WHEN em.user_type = 'converter' AND exists(us.preferences, x -> x = 'push') THEN 'high_value'
                WHEN em.user_type = 'engaged' AND em.avg_events_per_session > 2 THEN 'medium_value'
                ELSE 'low_value'
            END AS value_segment,
            filter(em.unique_properties, x -> x != 'home') AS filtered_properties,
            transform(em.all_channels, x -> upper(x)) AS normalized_channels
        FROM user_segments us
        JOIN engagement_metrics em ON us.user_id = em.user_id
        WHERE em.total_events > 0
    )
    
    SELECT 
        country,
        preference_segment,
        device_type,
        value_segment,
        count(*) AS user_count,
        avg(total_events) AS avg_total_events,
        array_distinct(flatten(collect_list(normalized_channels))) AS all_channel_types,
        collect_set(user_type) AS user_types_in_segment,
        CASE 
            WHEN exists(collect_list(value_segment), x -> x = 'high_value') THEN 'contains_high_value'
            ELSE 'no_high_value'
        END AS segment_quality,
        size(array_distinct(flatten(collect_list(filtered_properties)))) AS unique_property_count
    FROM final_enriched
    GROUP BY country, preference_segment, device_type, value_segment
    HAVING count(*) > 0
    ORDER BY country, preference_segment, device_type, value_segment
    """


def main():
    """Main execution function."""
    print("🚀 Starting PySpark Column Lineage Demo")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create sample data
        create_sample_data(spark)
        
        # Create and execute complex query
        complex_query = create_complex_query()
        
        print("\n📝 Executing complex query with:")
        print("   - CTEs (user_segments, session_events, engagement_metrics, final_enriched)")
        print("   - JOINs (sessions + events, user_segments + engagement_metrics)")
        print("   - GROUP BYs (multiple levels of aggregation)")
        print("   - List functions (exists, collect_list, filter, transform, flatten)")
        print("   - Inline closures (x -> x = 'click', x -> upper(x), etc.)")
        
        # Save query to file
        with open("complex_query.sql", "w") as f:
            f.write(complex_query)
        print("\n💾 Saved query to complex_query.sql")
        
        # Execute query
        result_df = spark.sql(complex_query)
        
        print("\n📊 Query Results:")
        result_df.show(truncate=False)
        
        print(f"\n✅ Query executed successfully! Found {result_df.count()} result rows")
        
        # Show schema
        print("\n📋 Result Schema:")
        result_df.printSchema()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()