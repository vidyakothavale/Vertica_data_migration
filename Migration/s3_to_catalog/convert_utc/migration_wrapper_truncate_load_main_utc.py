# Databricks notebook source
# MAGIC %md
# MAGIC ## Running Multiple Migrations 

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *

AUDIT_CATALOG = "sd_digivate_stage_catalog"
AUDIT_SCHEMA = "vertica_migration"
AUDIT_LOG_TABLE = "vertica_migration_truncate_utc"

def create_audit_log(original_table, s3_location, status, external_table, final_table):

    audit_schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("s3_path", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("external_table", StringType(), False),
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
        external_table=f"{external_table}",
        managed_table=f"{final_table}",
        rows_processed=0,
        error_message=None
    )
    row_df = spark.createDataFrame([row], audit_schema)
    row_df.write.format("delta").mode("append").saveAsTable(f"{AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}")


def update_audit_log(original_table, status, rows_processed=0, error_message=None):

    end_time=datetime.now()

    spark.sql(f"""
        UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
        SET end_time = "{end_time}", status = "{status}", rows_processed = "{rows_processed}", error_message = "{error_message}"
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

# MAGIC %md
# MAGIC ### Memory Config

# COMMAND ----------

spark.conf.set("spark.databricks.delta.clone.enableOptimizedMetadataOnly", "true")
spark.conf.set("spark.sql.shuffle.partitions", "32")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
spark.conf.set("spark.sql.broadcastTimeout", "1800")
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") 
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728") 
spark.conf.set("spark.sql.shuffle.spill.compress", "true")


# COMMAND ----------

# MAGIC %md
# MAGIC ###S3 Metadata Information

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import lower, col
df = pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/truncate_load_UTC.csv")
meta_data_df = spark.createDataFrame(df)
meta_data_df.createOrReplaceTempView("vertica_to_vertica_uc_metadata")
display(meta_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### External Table and Final Table Details
# MAGIC

# COMMAND ----------

external_catalog = "sd_digivate_stage_catalog"
external_schema = "migration_approach"
final_catalog = "sd_dwh_prd"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Migrate

# COMMAND ----------


import threading
import concurrent.futures
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit


def run_small_tables(original_schema, s3_location, external_table, original_table, final_table):

    table_running_stats = get_audit_log(original_table)
    if table_running_stats in ("IN_PROGRESS", "SUCCESS", "VALIDATION_SUCCESS", "VALIDATION_FAILED"):
        print(f"Skipping {original_table} as it is already processed")
        return
    
    create_audit_log(original_table, s3_location, "IN PROGRESS", external_table, final_table)
    error_message = None

    try:
        spark.catalog.clearCache()

        run_result = dbutils.notebook.run(
            "./vertica_migrate_new_trucncate_load_utc",
            timeout_seconds=36000,
            arguments={
                "original_schema" : original_schema,
                "external_table": external_table,
                "s3_location": s3_location,
                "original_table": original_table,
                "final_table": final_table
            }
        )
        if run_result != "SUCCESS":
            update_audit_log(original_table, "ERROR", 0, run_result)
    except Exception as err:
        error_message = str(err)[0]
        update_audit_log(original_table, "ERROR", 0, error_message) 


def process_row(row):
    original_schema = f"""{row["schema_name"]}"""
    original_table = f"""{row["schema_name"]}.{row["table_name"]}"""
    s3_location = row["s3_path"]
    external_table = f"""{external_catalog}.{external_schema}.{row["table_name"]}"""
    final_table = f"""{final_catalog}.{row["schema_name"]}.{row["table_name"]}"""
    
    try:
        run_small_tables(original_schema, s3_location, external_table, original_table, final_table)
        return f"Processed {original_table} successfully"
    except Exception as e:
        return f"Error processing {original_table}: {str(e)}"

def process_in_batches(small_meta_data_df, batch_size=4):
    rows = small_meta_data_df.collect()
    total_rows = len(rows)
    results = []
    next_index = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = {}
        for i in range(min(batch_size, total_rows)):
            future = executor.submit(process_row, rows[next_index])
            futures[future] = rows[next_index]
            next_index += 1
        
        # Process results and submit new tasks as current ones complete
        while futures:
            # Wait for the next task to complete
            done, _ = concurrent.futures.wait(
                futures.keys(), 
                return_when=concurrent.futures.FIRST_COMPLETED
            )
            
            for future in done:
                row = futures.pop(future)
                try:
                    result = future.result()
                    results.append(result)
                    print(result)
                except Exception as e:
                    print(f"Error processing row: {str(e)}")
                
                # Submit a new task if there are more rows to process
                if next_index < total_rows:
                    new_future = executor.submit(process_row, rows[next_index])
                    futures[new_future] = rows[next_index]
                    next_index += 1
    return results

results = process_in_batches(meta_data_df, batch_size=4)
print(f"Processed {len(results)} tables")

meta_data_df.unpersist()


# COMMAND ----------

for row in meta_data_df.collect():
    table_name = row["table_name"]
    try:
        count = spark.sql(f"select count(*) from dp.{row['vertica_schema_name']}.{row['table_name']}").collect()[0][0]
    except:
        count = 0
    print(table_name, count)

# COMMAND ----------

meta_data_df.count()

# COMMAND ----------

dbutils.fs.ls('s3://sd-databricks-vertica-prd-data/migrated-data/dwh/2025/05/08/analytics_logistics/new_essential_subcats')