# Databricks notebook source
pip install vertica_python

# COMMAND ----------

import time 
from pyspark.sql.functions import col 
import vertica_python
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from pyspark.sql import Row 
from pyspark.sql.types import *
from datetime import datetime
from queue import Queue
from threading import Lock
import pandas as pd

# COMMAND ----------

# # Configuration for Vertica connection
# catalog_name = "dwh_preprod_vertica_digivate" #to test
# CONFIG = {
#     "host": "vertica.ops.snapdeal.io",
#     "port": 5433,
#     "database": "snapdealdwh",
#     "user": dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_user"),
#     "password": dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_password")
# }

catalog_name = "sd_dwh_prd" #to test
CONFIG = {
    "host": "vertica.ops.snapdeal.io",
    "port": 5433,
    "database": "snapdealdwh",
    # "user": dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_user"),
    # "password": dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_password")
    'user': dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_user") ,
    'password' : dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_password") 
}

# COMMAND ----------


# Add audit configuration
AUDIT_CATALOG = "sd_digivate_stage_catalog"
AUDIT_SCHEMA = "default"
AUDIT_LOG_TABLE = "600_table_not_exist_in_s3_Final_1"

audit_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("rows_processed", LongType(), True),
    StructField("error_message", StringType(), True)
])

# empty_df = spark.createDataFrame([], audit_schema)
# empty_df.write.format("delta").mode("overwrite").saveAsTable(f"{AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}")

def create_audit_log(table_name, catalog_table, status="IN_PROGRESS"):
    row = Row(
        table_name=table_name,
        start_time=datetime.now(),
        end_time=None,
        status=status,
        rows_processed=0,
        error_message=None
    )
    row_df = spark.createDataFrame([row], audit_schema)
    row_df.write.format("delta").mode("append").saveAsTable(f"{AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}")


# Add global queue and lock for audit updates
audit_queue = Queue()
audit_lock = Lock()

def process_audit_queue():
    while not audit_queue.empty():
        with audit_lock:
            try:
                update = audit_queue.get()
                end_time = datetime.now()
                spark.sql(f"""
                    UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
                    SET 
                        end_time = '{end_time}',
                        status = '{update["status"]}',
                        rows_processed = {update["rows_processed"]},
                        error_message = '{update["error_message"] if update["error_message"] else "NULL"}'
                    WHERE table_name = '{update["table_name"]}'
                """)
                audit_queue.task_done()
            except Exception as e:
                print(f"Error processing audit update: {str(e)}")
                # Put back in queue for retry
                audit_queue.put(update)

def update_audit_log(table_name, status, rows_processed=0, error_message=None):
    # Add update to queue instead of direct update
    audit_queue.put({
        "table_name": table_name,
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message
    })

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

#meta_df = spark.table("sd_digivate_stage_catalog.default.vertica_metadata_delta_load")#.limit(2)
# meta_df = spark.table("sd_digivate_stage_catalog.default.4377_vertica_table_row_counts_orderby").orderBy("row_count").limit(10).offset(2855)
#     (col("schema_name") == 'analytics_pricing') & 
#     (col("table_name") == "guid_ims_mapping_as")
# ) 



# COMMAND ----------


import pandas as pd
df = pd.read_csv("/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/IMP/540_from_vetica(600).csv")
# df = df.iloc[1:11]
meta_df = spark.createDataFrame(df)
meta_df.display()
tables_to_process = meta_df.select("schema_name", "table_name").collect()
total_tables = len(tables_to_process)
print(f"Total tables to process: {total_tables}")

# COMMAND ----------


def read_vertica_and_insert(schema_name, table_name):
    full_table_name = f"{schema_name}.{table_name}"
    catalog_table = f"{catalog_name}.{schema_name}.{table_name}"
    base_name = f"{schema_name}_{table_name}".replace(".", "_").replace("-", "_")
    temp_view = f"temp_view_{base_name}"

    table_running_stats = get_audit_log(full_table_name)
 
    if table_running_stats in ("SUCCESS","IN_PROGRESS"):
        print(f"Skipping {full_table_name} as it is already processed (Status: {table_running_stats})")
        return False
 

    try:
        print(f" Starting: {full_table_name}")
        create_audit_log(full_table_name, catalog_table)

        # Load full table from Vertica 
        vertica_query = f"(SELECT * FROM {schema_name}.{table_name}) AS subq"
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:vertica://{CONFIG['host']}:{CONFIG['port']}/{CONFIG['database']}") \
            .option("user", CONFIG['user']) \
            .option("password", CONFIG['password']) \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", vertica_query) \
            .load()
        #print(temp_view)
        df.createOrReplaceTempView(temp_view)
        spark.sql("SET spark.databricks.delta.isolationLevel=SERIALIZABLE")

        # Create table and insert with isolation level
        quoted_columns = ', '.join(f"`{col}`" for col in df.columns)
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {catalog_table} 
            USING DELTA
            TBLPROPERTIES (
                'delta.isolationLevel' = 'SERIALIZABLE',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.columnMapping.mode' = 'name'
            )
            AS SELECT {quoted_columns} FROM {temp_view} WHERE 1=0
        """
        spark.sql(create_table)

        spark.sql(f"""
            -- INSERT INTO {catalog_table}
            INSERT OVERWRITE TABLE {catalog_table}
            SELECT * FROM {temp_view}
        """)

        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {temp_view}").first()["cnt"]
        update_audit_log(full_table_name, "SUCCESS", rows_processed=row_count)
        print(f" {row_count} rows inserted into {catalog_table}")
        spark.sql(f"DROP VIEW IF EXISTS {temp_view}")

    except Exception as e:
        error_msg = str(e)
        print(f" Error processing {full_table_name}: {error_msg}") 
        update_audit_log(full_table_name, "ERROR", error_message=error_msg)


def main():
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(read_vertica_and_insert, row["schema_name"], row["table_name"])
                   for row in tables_to_process]
        
        # Process audit updates while waiting for table loads
        while futures:
            done, futures = wait(futures, timeout=5)
            process_audit_queue()
            
        # Process any remaining audit updates
        process_audit_queue()

main()