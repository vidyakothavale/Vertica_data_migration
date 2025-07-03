# Databricks notebook source
# MAGIC %pip install vertica_python
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import uuid
import os
import vertica_python
import boto3
import pandas as pd
import os
import tempfile
import datetime
import vertica_python
import time
import logging
from datetime import datetime
import concurrent.futures
from pyspark.sql.functions import year, month, dayofmonth, col,lit
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random 
import string

# COMMAND ----------

from datetime import datetime
start_time = datetime.now()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

from pyspark.sql.types import *

AUDIT_CATALOG = "sd_digivate_stage_catalog"
AUDIT_SCHEMA = "vertica_migration"
AUDIT_LOG_TABLE = "vertica_to_s3_migration_prod_log"

audit_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("s3_path", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("partition_columns", StringType(), False),
    StructField("managed_table", StringType(), False),
    StructField("rows_processed", IntegerType(), True),
    StructField("error_message", StringType(), True)
])

# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_CATALOG}.{AUDIT_SCHEMA}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_CATALOG}.{AUDIT_SCHEMA}")
# spark.sql(f"DROP TABLE IF EXISTS {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}")

# empty_df = spark.createDataFrame([], audit_schema)
# empty_df.write.format("delta").mode("overwrite").saveAsTable(f"{AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}")

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *

AUDIT_CATALOG = "sd_digivate_stage_catalog"
AUDIT_SCHEMA = "vertica_migration"
AUDIT_LOG_TABLE = "vertica_to_s3_migration_prod_log"


def escape(value):
    return value.replace("'", "''") if value else ""


def create_audit_log(original_table, s3_location, status, external_table, final_table):

    audit_schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("s3_path", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("partition_columns", StringType(), False),
        StructField("managed_table", StringType(), False),
        StructField("rows_processed", IntegerType(), True),
        StructField("error_message", StringType(), True)
    ])

    row = Row(
        table_name=f"{original_table}",
        s3_path=f"{s3_location}",
        start_time=datetime.now(),
        end_time=None,
        status="IN_PROGRESS",
        partition_columns="no_partition_columns",
        managed_table=f"{final_table}",
        rows_processed=0,
        error_message=None
    )
    row_df = spark.createDataFrame([row], audit_schema)
    row_df.write.format("delta").mode("append").saveAsTable(f"{AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}")


def update_audit_log(original_table, status, partition_columns, rows_processed=0, error_message=None):

    end_time=datetime.now()

    spark.sql(f"""
        UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
        SET end_time = "{end_time}", status = "{status}", partition_columns="{partition_columns}", rows_processed = "{rows_processed}", error_message = "{error_message}"
        WHERE table_name = "{original_table}"
    """)
    
def get_audit_log(original_table):
    try:
        return spark.sql(f"""
            SELECT status FROM {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
            WHERE table_name = "{original_table}"
            ORDER BY start_time DESC
            LIMIT 1
        """).collect()[0][0]

        print(table_running_status_df)
    except:
        return None

# COMMAND ----------

from datetime import datetime
start_time = datetime.now()

hostname = "vertica.ops.snapdeal.io"
username = dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_user") 
password = dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_password") 
database_port = 5433
database_name = "snapdealdwh"

access=dbutils.secrets.get(scope='sd-prd-dataplatform', key='dataplatform_prd_digivate_s3_bucket_access_key')
secret=dbutils.secrets.get(scope='sd-prd-dataplatform', key='dataplatform_prd_digivate_s3_bucket_secret_key')


spark.conf.set("spark.databricks.delta.preview.enabled", "true")

vertica_connection_info = {
    "host" : hostname,
    "port" : database_port,
    "database" : database_name,
    "user" : username,
    "password" : password
}



# COMMAND ----------

cursor = None
try:
    conn = vertica_python.connect(**vertica_connection_info)
    cursor = conn.cursor()
except Exception as exc:
    raise Exception("Unable to connect to Vertica") 

# COMMAND ----------

# MAGIC %md
# MAGIC need to mention specific col name its not automatically fetch col type that logic not added

# COMMAND ----------

from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import lit
def export_vertica_to_s3_parquet(spark, vertica_connection_info, table_name, s3_bucket, s3_prefix, timestamp_col="rejected_timestamp", chunk_days=30, max_workers=6):
    try:
        audit_status = get_audit_log(table_name)
        if audit_status == "SUCCESS":
            print(f"Table {table_name} already processed successfully")
            return

        s3_path = f"s3a://{s3_bucket}/{s3_prefix}"
        schema, tbl = table_name.split('.') if '.' in table_name else ('public', table_name)
        full_table_name = f"{schema}.{tbl}"
        print(f"Starting export for: {full_table_name}")

        create_audit_log(full_table_name, s3_path, "IN_PROGRESS", "timestamp_partitioning", full_table_name)

        # Step 1: Get min/max timestamps
        bounds_query = f"(SELECT MIN({timestamp_col}) AS min_ts, MAX({timestamp_col}) AS max_ts FROM {full_table_name}) AS bounds"
        bounds_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:vertica://{vertica_connection_info['host']}:{vertica_connection_info['port']}/{vertica_connection_info['database']}") \
            .option("user", vertica_connection_info["user"]) \
            .option("password", vertica_connection_info["password"]) \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", bounds_query) \
            .load()
        
        row = bounds_df.first()
        min_ts, max_ts = row["min_ts"], row["max_ts"]
        if not min_ts or not max_ts:
            print(f"Skipping table {full_table_name}, no data in timestamp column.")
            update_audit_log(full_table_name, "SUCCESS", [], rows_processed=0, error_message="Empty timestamp range")
            return

        # Step 2: Calculate chunk ranges
        total_days = (max_ts - min_ts).days + 1
        chunk_ranges = [
            (min_ts + timedelta(days=i), min(min_ts + timedelta(days=i + chunk_days), max_ts + timedelta(days=1)))
            for i in range(0, total_days, chunk_days)
        ]
        print(f"Generated {len(chunk_ranges)} timestamp chunks")

        def process_chunk(idx, start_ts, end_ts):
            query = f"(SELECT * FROM {full_table_name} WHERE {timestamp_col} >= TIMESTAMP '{start_ts}' AND {timestamp_col} < TIMESTAMP '{end_ts}') AS chunk"
            print(f"[Chunk {idx+1}] {start_ts} → {end_ts}")
            df = spark.read.format("jdbc") \
                .option("url", f"jdbc:vertica://{vertica_connection_info['host']}:{vertica_connection_info['port']}/{vertica_connection_info['database']}") \
                .option("dbtable", query) \
                .option("user", vertica_connection_info["user"]) \
                .option("password", vertica_connection_info["password"]) \
                .option("driver", "com.vertica.jdbc.Driver") \
                .option("fetchsize", "1000000") \
                .load()
            
            row_count = df.count()
            print(f"[Chunk {idx+1}] Rows: {row_count}")
            chunk_path = f"{s3_path}/chunk_{idx}"
            df.write.mode("overwrite").format("parquet").save(chunk_path)
            return {"index": idx, "start": start_ts, "end": end_ts, "count": row_count, "path": chunk_path}

        # Step 3: Run chunk exports in parallel
        chunk_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_chunk, idx, start_ts, end_ts)
                for idx, (start_ts, end_ts) in enumerate(chunk_ranges)
            ]
            for f in as_completed(futures):
                chunk_results.append(f.result())

        total_rows = sum(r["count"] for r in chunk_results)
        paths = [r["path"] for r in sorted(chunk_results, key=lambda x: x["index"])]

        print(f"Combining {len(paths)} chunks with total {total_rows:,} rows")
        final_df = spark.read.parquet(*paths)
        final_count = final_df.count()
        if abs(final_count - total_rows) > 0:
            raise Exception(f"Row count mismatch: Expected {total_rows}, Got {final_count}")
        
        final_df.write.mode("overwrite").format("parquet").save(f"{s3_path}/final")
        for path in paths:
            dbutils.fs.rm(path, recurse=True)

        update_audit_log(full_table_name, "SUCCESS", [], rows_processed=final_count, error_message="No Error")
        print(f"Export successful: {final_count:,} rows written to {s3_path}/final")

    except Exception as e:
        error_msg = str(e)
        print(f"Error during export of {table_name}: {error_msg}")
        try:
            update_audit_log(table_name, "ERROR", "timestamp_partitioning", rows_processed=0, error_message=error_msg)
        except Exception as log_err:
            print(f"Failed to update audit log: {log_err}")


# COMMAND ----------

#public	resource_rejection_details
from pyspark.sql.functions import col, lit
meta_df = spark.table("sd_digivate_stage_catalog.default.vertica_to_s3_290_tables_final")
# meta_df = meta_df.filter((col("schema_name") == 'analytics_logistics') & (col("table_name") == "pl_warehouse_item_master_final"))
meta_df = meta_df.filter((col("schema_name") == 'public') & (col("table_name") == "resource_rejection_details"))
tables_to_process = meta_df.select("schema_name", "table_name").collect()
total_tables = len(tables_to_process)
print(f"Total tables to process: {total_tables}")

# Loop tables
for row in tables_to_process:
    schema = row["schema_name"]
    table_name = row["table_name"]
    full_table_name = f"{schema}.{table_name}"
    print(f"\nProcessing table: {full_table_name}")
    #clean_schema_name = schema.replace("_dwh", "")
    s3_prefix = f"digivate/vertica_production_data/{schema}/{table_name}"
    
    try:
        # Validate table first
        try:
            cursor.execute(f"SELECT 1 FROM {full_table_name} LIMIT 1")
        except Exception as table_exc:
            print(f"Error validating table {full_table_name}: {str(table_exc)}")
            update_audit_log(
                full_table_name, 
                "ERROR", 
                "no_partition_columns", 
                rows_processed=0, 
                error_message=f"Table validation failed: {str(table_exc)}"
            )
            continue
            
        export_vertica_to_s3_parquet(
            spark=spark,
            vertica_connection_info=vertica_connection_info,
            table_name=full_table_name,
            s3_bucket="sd-preprod-databricks-data",
            s3_prefix=s3_prefix
        )
    except Exception as exc:
        error_msg = str(exc)
        if "PARSE_SYNTAX_ERROR" in error_msg:
            print(f"Syntax error in table {full_table_name}, skipping: {error_msg}")
        else:
            print(f"Error processing {full_table_name}: {error_msg}")
            
        try:
            safe_error = error_msg.replace("'", "''")
            update_audit_log(
                full_table_name,
                "ERROR", 
                "no_partition_columns",
                rows_processed=0,
                error_message=safe_error
            )
        except Exception as log_exc:
            print(f"Failed to update audit log: {str(log_exc)}")
        continue
print("\nProcessing complete!")