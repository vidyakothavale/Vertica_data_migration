# Databricks notebook source
from pyspark.sql.functions import col, lit, when, year, month, day, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql import DataFrame
from functools import reduce
from datetime import datetime
import concurrent.futures
import pandas as pd
import gc

# ------------------ CONFIG ------------------
VERTICA = {
    "url": "jdbc:vertica://vertica.ops.snapdeal.io:5433/snapdealdwh",
    "user": dbutils.secrets.get(scope="sd-prd-veritca", key="vertica_user"),
    "password": dbutils.secrets.get(scope="sd-prd-veritca", key="vertica_password"),
    "driver": "com.vertica.jdbc.Driver"
}
OUTPUT_TABLE = "sd_digivate_stage_catalog.vertica_migration.overall_count" 
MAX_WORKERS = 20
APPEND_BATCH_SIZE = 50
DATE_CANDIDATES = ["date", "created", "updated", "createdAt", "updatedAt"]
START_DATE = "2025-01-01"

RESULT_SCHEMA = StructType([
    StructField("target_table_name", StringType()),
    StructField("dpYear", IntegerType(), True),
    StructField("dpMonth", IntegerType(), True),
    StructField("dpDay", IntegerType(), True),
    StructField("source_count", LongType()),
    StructField("target_count", LongType()),
    StructField("validation_status", StringType()),
    StructField("date_column_used", StringType(), True)
])

# ------------------ LOAD INPUT TABLES ------------------
try:
    pdf = pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/vertica_schema_data.csv")
    csv_df = spark.createDataFrame(pdf).select("schema_name", "table_name")
    csv_df = csv_df.select(col("schema_name"), col("table_name")).distinct()
except Exception as e:
    print(f"Error loading or processing CSV file: {e}")
    raise

# ------------------ FETCH ROW COUNT ESTIMATES (for prioritization) ------------------
table_names_list = [row.asDict() for row in csv_df.collect()]
if not table_names_list:
    print("No tables found in the input CSV. Exiting.")
    dbutils.notebook.exit("No tables found.")

table_schema_pairs = [(t['schema_name'], t['table_name']) for t in table_names_list]
table_filter_conditions = ' OR '.join([f"(p.anchor_table_name = '{table}' AND sc.schema_name = '{schema}')" for schema, table in table_schema_pairs])

if table_filter_conditions:
    query_estimates = f"""
    WITH num_rows AS (
        SELECT sc.schema_name, p.anchor_table_name AS table_name,
               SUM(sc.total_row_count - sc.deleted_row_count) AS rows
        FROM v_monitor.storage_containers sc
        JOIN v_catalog.projections p
          ON sc.projection_id = p.projection_id
         AND p.is_super_projection = true
        WHERE {table_filter_conditions}
        GROUP BY sc.schema_name, p.anchor_table_name, sc.projection_id
    )
    SELECT schema_name, table_name, MAX(rows) AS estimated_row_count
    FROM num_rows
    GROUP BY schema_name, table_name
    """

    try:
        row_count_df = spark.read.format("jdbc") \
            .option("url", VERTICA["url"]) \
            .option("query", query_estimates) \
            .option("user", VERTICA["user"]) \
            .option("password", VERTICA["password"]) \
            .option("driver", VERTICA["driver"]) \
            .load()
    except Exception as e:
        print(f"Error fetching estimated row counts from Vertica: {e}")
        row_count_df = spark.createDataFrame([], StructType([
             StructField("schema_name", StringType()),
             StructField("table_name", StringType()),
             StructField("estimated_row_count", LongType())
         ]))
else:
     row_count_df = spark.createDataFrame([], StructType([
             StructField("schema_name", StringType()),
             StructField("table_name", StringType()),
             StructField("estimated_row_count", LongType())
         ]))


table_df = csv_df.join(row_count_df, on=["schema_name", "table_name"], how="left").fillna(0)
table_list = table_df.orderBy(col("estimated_row_count").desc()).collect()

print(f"Fetched list of {len(table_list)} tables to process.")

# COMMAND ----------

query_unity_catalog = """
SELECT schema_name, table_name, SUM(row_count) AS row_count
FROM information_schema.tables
GROUP BY schema_name, table_name
"""

try:
    unity_catalog_df = spark.sql(query_unity_catalog)
    display(unity_catalog_df)
    row_count_df = row_count_df.withColumnRenamed("estimated_row_count", "vertica_count")
    unity_catalog_df = unity_catalog_df.withColumnRenamed("row_count", "uc_count")

    joined_df = row_count_df.join(unity_catalog_df, on=["schema_name", "table_name"], how="inner")

    result_df = joined_df.withColumn("is_matching", col("vertica_count") == col("uc_count"))

    display(result_df)
except Exception as e:
    print(f"Error fetching row counts from Unity Catalog: {e}")