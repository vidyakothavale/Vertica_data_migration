# Databricks notebook source
#for numeric out of range and decimal has fix length(decimal 36,36)

# COMMAND ----------

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

# AUDIT_CATALOG = "sd_digivate_stage_catalog"
# AUDIT_SCHEMA = "default"
# AUDIT_LOG_TABLE = "logtable2"
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

# AUDIT_CATALOG = "sd_digivate_stage_catalog"
# AUDIT_SCHEMA = "default"
# AUDIT_LOG_TABLE = "logtable2"

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


# def update_audit_log(original_table, status, partition_columns, rows_processed=0, error_message=None):

#     end_time=datetime.now()

#     spark.sql(f"""
#         UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
#         SET end_time = '{end_time}', status = "{status}", partition_columns="{partition_columns}", rows_processed = "{rows_processed}", error_message = "{error_message}"
#         WHERE table_name = "{original_table}"
#     """)
    
def update_audit_log(original_table, status, partition_columns, rows_processed=0, error_message=None):

    end_time=datetime.now()

    spark.sql(f"""
        UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
        SET end_time = '{end_time}', status = "{status}", partition_columns="{partition_columns}", rows_processed = "{rows_processed}", error_message = "{error_message}"
        WHERE table_name = "{original_table}"
    """)

def get_audit_log(original_table):
    try:
        result = spark.sql(f"""
            SELECT status FROM {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
            WHERE table_name = '{original_table}'
            ORDER BY start_time DESC
            LIMIT 1
        """).collect()

        if result:
            return result[0][0]
        else:
            return None
    except Exception as e:
        print(f"Audit log fetch error for {original_table}: {str(e)}")
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

import math
from concurrent.futures import ThreadPoolExecutor, as_completed

def export_vertica_to_s3_parquet(spark, vertica_connection_info, table_name, s3_bucket, s3_prefix, partition_columns=None):
    print
    try:
        audit_status = get_audit_log(table_name)
        
        # Fix NoneType check
        if audit_status and audit_status == "SUCCESS":
        #if audit_status == "SUCCESS":
            print(f"Table {table_name} already processed successfully")
            return

        s3_path = f"s3a://{s3_bucket}/{s3_prefix}"
        print(f"Exporting table {table_name} to S3 path: {s3_path}")
        chunk_size = 5000000
        fetch_size = 5000000
        partitions = 32
        max_workers = 16

        try:
            schema_name = table_name.split('.')[0] if '.' in table_name else 'public'
            table_only = table_name.split('.')[-1]
            full_table_name = f"{schema_name}.{table_only}"
            print("full table name:",full_table_name)
            create_audit_log(full_table_name, s3_path, "IN_PROGRESS", "no_partition_columns", full_table_name)

            # Count total rows
            row_count_query = f"(SELECT COUNT(*) AS cnt FROM {full_table_name}) AS row_count_subq"
            row_count_df = spark.read.format("jdbc") \
                .option("url", f"jdbc:vertica://{vertica_connection_info['host']}:{vertica_connection_info['port']}/{vertica_connection_info['database']}") \
                .option("user", vertica_connection_info["user"]) \
                .option("password", vertica_connection_info["password"]) \
                .option("driver", "com.vertica.jdbc.Driver") \
                .option("dbtable", row_count_query) \
                .load()

            total_rows = row_count_df.collect()[0]["cnt"]
            total_rows_processed=total_rows
            print(f"Total rows to process: {total_rows:,}")

            # Handle empty tables directly without chunking
            if total_rows == 0:
                print(f"Table {full_table_name} is empty, writing directly without chunks")
                empty_schema = spark.read.format("jdbc") \
                    .option("url", f"jdbc:vertica://{hostname}:{database_port}/{database_name}") \
                    .option("dbtable", full_table_name) \
                    .option("user", username) \
                    .option("password", password) \
                    .option("driver", "com.vertica.jdbc.Driver") \
                    .load()

                empty_schema.printSchema()

                select_expr = []
                for field in empty_schema.schema.fields:
                    if isinstance(field.dataType, DecimalType):
                        #select_expr.append(f'CAST("{field.name}" AS DECIMAL(38,28)) AS "{field.name}"')
                        select_expr.append(f'CAST("{field.name}" AS DOUBLE PRECISION) AS "{field.name}"') #for table analytics_pricing.seller_software_mapping
                    else:
                        select_expr.append(f'"{field.name}"')

                final_query = f'SELECT {", ".join(select_expr)} FROM {full_table_name}'

                #print(f"Generated Query:\n{final_query}")

                #print(f"Generated Query:\n{final_query}")
                empty_df = spark.read.format("jdbc") \
                    .option("url", f"jdbc:vertica://{hostname}:{database_port}/{database_name}") \
                    .option("query", final_query) \
                    .option("user", username) \
                    .option("password", password) \
                    .option("driver", "com.vertica.jdbc.Driver") \
                    .load()
                #empty_df.printSchema()
                #display(empty_df)
                
                empty_df.write.option('delta.columnMapping.mode', 'name').format("parquet") \
                    .option("compression", "snappy") \
                    .mode("overwrite") \
                    .save(f"{s3_path}/final")
                
                update_audit_log(full_table_name, "SUCCESS", [], rows_processed=0, error_message="Empty table")
                print(f"Empty table {full_table_name} processed successfully")
                return
            # Calculate chunks
            chunk_offsets = [(i, i * chunk_size) for i in range(math.ceil(total_rows / chunk_size))]
            print(f"Number of chunks: {len(chunk_offsets)}")

            def process_chunk(index, offset):
                dfschema = spark.read.format("jdbc") \
                    .option("url", f"jdbc:vertica://{hostname}:{database_port}/{database_name}") \
                    .option("dbtable", full_table_name) \
                    .option("user", username) \
                    .option("password", password) \
                    .option("driver", "com.vertica.jdbc.Driver") \
                    .load()

                dfschema.printSchema()

                select_expr = []
                for field in dfschema.schema.fields:
                    if isinstance(field.dataType, DecimalType):
                        #select_expr.append(f'CAST("{field.name}" AS DECIMAL(38,28)) AS "{field.name}"')
                        select_expr.append(f'CAST("{field.name}" AS DOUBLE PRECISION) AS "{field.name}"') #for table analytics_pricing.seller_software_mapping 
                    else:
                        select_expr.append(f'"{field.name}"')

                final_query = f'SELECT {", ".join(select_expr)} FROM {full_table_name}'

                #print(f"Generated Query:\n{final_query}")
              
                query = f"({final_query} LIMIT {chunk_size} OFFSET {offset}) AS chunked"

                print(f"[Chunk {index+1}] Reading rows OFFSET {offset}...")


                df = spark.read.format("jdbc") \
                    .option("url", f"jdbc:vertica://{vertica_connection_info['host']}:{vertica_connection_info['port']}/{vertica_connection_info['database']}") \
                    .option("dbtable", query) \
                    .option("user", vertica_connection_info["user"]) \
                    .option("password", vertica_connection_info["password"]) \
                    .option("driver", "com.vertica.jdbc.Driver") \
                    .option("fetchsize", fetch_size) \
                    .option("queryTimeout", "1800") \
                    .load()
                     
                count = df.count()
                print(f"[Chunk {index+1}] Row count: {count}, Writing to S3...")
                
                chunk_path = f"{s3_path}/chunk_{index}"
                df.repartition(partitions).write.option('delta.columnMapping.mode', 'name').format("parquet") \
                    .option("compression", "snappy") \
                    .mode("overwrite") \
                    .save(chunk_path)

                return {"index": index, "count": count, "path": chunk_path}

            # Process chunks in parallel
            chunk_results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_chunk, idx, offset) for idx, offset in chunk_offsets]
                for future in as_completed(futures):
                    result = future.result()
                    chunk_results.append(result)

            # Consolidate results
            all_chunk_paths = [result["path"] for result in chunk_results]
            total_processed = sum(result["count"] for result in chunk_results)

            if abs(total_rows - total_processed) > 0:
                raise Exception(f"Row count mismatch! Original: {total_rows}, Processed: {total_processed}")

            # Read and write final output
            final_df = spark.read.parquet(*all_chunk_paths)
           

            final_count = final_df.count()

            if abs(total_rows - final_count) > 0:
                raise Exception(f"Final count mismatch! Original: {total_rows}, Final: {final_count}")

            # Write final output
            final_df.write.option('delta.columnMapping.mode', 'name').format("parquet") \
                .option("compression", "snappy") \
                .mode("overwrite") \
                .save(f"{s3_path}/final")

            # Cleanup temp chunks
            for path in all_chunk_paths:
                dbutils.fs.rm(path, recurse=True)

            update_audit_log(full_table_name, "SUCCESS", [], rows_processed=final_count, error_message="No Error")
            print(f"Migration completed successfully. Final row count: {final_count:,}")

        except Exception as exc:
            error_msg = str(exc)
            print(f"Error: {error_msg}")
            update_audit_log(table_name, "ERROR", "no_partition_columns", rows_processed=0, error_message=error_msg)

    except Exception as exc:
        error_msg = str(exc)
        print(f"Error: {error_msg}")
        update_audit_log(table_name, "ERROR", "no_partition_columns", rows_processed=0, error_message=error_msg)



# COMMAND ----------


from pyspark.sql.functions import col, lit
meta_df = spark.table("sd_digivate_stage_catalog.default.vertica_to_s_3_495_tables")
meta_df = meta_df.filter((col("schema_name") == 'dwh') & (col("table_name") == "dwh_vendor_price"))

tables_to_process = meta_df.select("schema_name", "table_name").collect()
total_tables = len(tables_to_process)
print(f"Total tables to process: {total_tables}")

# Loop tables
for row in tables_to_process:
    schema = row["schema_name"]
    table_name = row["table_name"]
    full_table_name = f"{schema}.{table_name}"
    print(f"\nProcessing table: {full_table_name}")
    s3_prefix = f"digivate/vertica_production_data/{schema}/{table_name}"
    export_vertica_to_s3_parquet(
            spark=spark,
            vertica_connection_info=vertica_connection_info,
            table_name=full_table_name,
            s3_bucket="sd-preprod-databricks-data",
            s3_prefix=s3_prefix
        )

    #         cursor.execute(f"SELECT 1 FROM {full_table_name} LIMIT 1")
    #     except Exception as table_exc:
    #         print(f"Error validating table {full_table_name}: {str(table_exc)}")
    #         update_audit_log(
    #             full_table_name, 
    #             "ERROR", 
    #             "no_partition_columns", 
    #             rows_processed=0, 
    #             error_message=f"Table validation failed: {str((table_exc))}"
    #         )
    #         continue
            
    #     export_vertica_to_s3_parquet(
    #         spark=spark,
    #         vertica_connection_info=vertica_connection_info,
    #         table_name=full_table_name,
    #         s3_bucket="sd-preprod-databricks-test",
    #         s3_prefix=s3_prefix
    #     )
    # except Exception as exc:
    #     error_msg = str(exc)
    #     print(error_msg)
    #     if "PARSE_SYNTAX_ERROR" in error_msg:
    #         print(f"Syntax error in table {full_table_name}, skipping: {error_msg}")
    #     else:
    #         print(f"Error processing {full_table_name}: {error_msg}")
            
    #     try:
    #         safe_error = error_msg.replace("'", "''")
    #         update_audit_log(
    #             full_table_name,
    #             "ERROR", 
    #             "no_partition_columns",
    #             rows_processed=0,
    #             error_message=error_msg
    #         )
    #     except Exception as log_exc:
    #         print(f"Failed to update audit log: {str(log_exc)}")
    #     continue

print("\nProcessing complete!")
    