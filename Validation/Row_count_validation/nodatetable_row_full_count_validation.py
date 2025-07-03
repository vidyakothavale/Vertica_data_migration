# Databricks notebook source
hostname = "vertica.ops.snapdeal.io"
username = dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_user")
password = dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_password")
database_port = 5433
database_name = "snapdealdwh"

qry_verti = f"""WITH num_rows AS (
  SELECT
    sc.schema_name,
    p.anchor_table_name AS table_name,
    SUM(sc.total_row_count - sc.deleted_row_count) AS rows
  FROM v_monitor.storage_containers sc
  JOIN v_catalog.projections p
    ON sc.projection_id = p.projection_id
    AND p.is_super_projection = true
  GROUP BY sc.schema_name, p.anchor_table_name, sc.projection_id

)

SELECT
  schema_name,
  table_name,
  MAX(rows) AS estimated_row_count
FROM num_rows
GROUP BY schema_name, table_name
ORDER BY estimated_row_count DESC
"""

df_qry_verti = spark.read.format("jdbc") \
    .option("url", f"jdbc:vertica://vertica.ops.snapdeal.io:5433/snapdealdwh") \
    .option("query", qry_verti) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.vertica.jdbc.Driver") \
    .option("host", hostname)\
    .option("database", database_name)\
    .load()    

display(df_qry_verti)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, year, month
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql import DataFrame
from functools import reduce
from datetime import datetime
import concurrent.futures
import pandas as pd
import gc

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col

# pdf = pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/vertica_schema_data.csv")
# csv_df = spark.createDataFrame(pdf)
csv_df = spark.read.table('sd_digivate_stage_catalog.default.vertica_col_data_7_june')#spark.createDataFrame(pdf)

from pyspark.sql.functions import collect_list, when, size

# Get columns where data_type is 'date'
date_columns_df = csv_df.filter((col("data_type") == "date") | (col("data_type") == "timestamp") | (col("data_type") == "timestamp_ntz")| (col("column_name").isin("date", "created", "updated", "createdAt", "updatedAt"))) \
    .groupBy("schema_name", "table_name") \
    .agg(collect_list("column_name").alias("date_columns"))

# Identify schema_name, table_name where none of the column_name have data_type as 'date'
all_tables_df = csv_df.select("schema_name", "table_name").distinct()
no_date_columns_df = all_tables_df.join(date_columns_df, on=["schema_name", "table_name"], how="anti") \
    .select("schema_name", "table_name")

display(date_columns_df)
display(no_date_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC row count validation for nodate column **tables**

# COMMAND ----------

pip install tqdm

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

hostname = "vertica.ops.snapdeal.io"
username = dbutils.secrets.get(scope="sd-prd-veritca", key="vertica_user") 
password = dbutils.secrets.get(scope="sd-prd-veritca", key="vertica_password") 
database_port = 5433
database_name = "snapdealdwh"

meta_df = no_date_columns_df#.limit(50)
tables_to_process = meta_df.select("schema_name", "table_name").collect()

def process_table(row):
    schema = row["schema_name"]
    table = row["table_name"]
    full_table = f"{schema}.{table}"
    catalog_schema = schema

    vertica_count = None
    catalog_count = None
    vertica_error = ""
    catalog_error = ""
    status = "UNKNOWN"

    try:
        query = f"SELECT COUNT(*) AS row_count FROM {full_table}"
        count_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:vertica://{hostname}:{database_port}/{database_name}") \
            .option("query", query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "com.vertica.jdbc.Driver") \
            .load()
        vertica_count = count_df.collect()[0]["row_count"]
    except Exception as e:
        vertica_error = str(e)

    try:
        catalog_df = spark.sql(f"SELECT COUNT(*) AS row_count FROM sd_dwh_prd.{catalog_schema}.{table}")
        catalog_count = catalog_df.collect()[0]["row_count"]
    except Exception as e:
        catalog_error = str(e)

    if vertica_count is not None and catalog_count is not None:
        status = "SUCCESS" if vertica_count == catalog_count else "FAIL"
    elif vertica_count is None:
        status = "VERTICA_QUERY_FAIL"
    elif catalog_count is None:
        status = "CATALOG_QUERY_FAIL"

    return (
        schema, table,
        f"{schema}.{table}",
        f"sd_dwh_prd.{catalog_schema}.{table}",
        vertica_count, catalog_count,
        status, vertica_error, catalog_error
    )

count_results = []

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_table, row) for row in tables_to_process]
    for future in tqdm(as_completed(futures), total=len(futures)):
        count_results.append(future.result())

result_schema = StructType([
    StructField("vertica_schema", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("vertica_table", StringType(), True),
    StructField("catalog_table", StringType(), True),
    StructField("vertica_count", LongType(), True),
    StructField("prd_catalog_count", LongType(), True),
    StructField("status", StringType(), True),
    StructField("vertica_error", StringType(), True),
    StructField("catalog_error", StringType(), True),
])
result_df = spark.createDataFrame(count_results, schema=result_schema) 
result_df.write.mode("append").saveAsTable("sd_digivate_stage_catalog.default.fullvalidation_nodatecoldf_7june")
display(result_df) 

# COMMAND ----------

full_load=result_df.filter(result_df.status=='FAIL')
full_load.display()

# COMMAND ----------

df = spark.sql("select * from sd_digivate_stage_catalog.default.fullvalidation_nodatecoldf_7june")
df_filtered = df.filter((df.status == 'FAIL') | (df.status == 'CATALOG_QUERY_FAIL'))
display(df_filtered)