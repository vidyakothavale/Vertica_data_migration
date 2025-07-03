# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import Row
from pyspark.sql.functions import col
from tqdm import tqdm


# bucket_name = "sd-preprod-databricks-data/"
# migration_folder = "digivate/one_month_data_common_table_data/"
# s3_migration_path = f"s3://{bucket_name}{migration_folder}"

hostname = "vertica.ops.snapdeal.io"
username = dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_user")
password = dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_password")
database_port = 5433
database_name = "snapdealdwh"
jdbc_url = f"jdbc:vertica://{hostname}:{database_port}/{database_name}"
connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.vertica.jdbc.Driver"
}

migrate_table_df = spark.read.table("sd_digivate_stage_catalog.default.exclude_vertica_emr_tables")
migrate_tables = [(row["table_schema"], row["table_name"]) for row in migrate_table_df.collect()]

tables_without_date = []
log_entries = []

def inspect_table(schema_name, table_name):
    try:
        query = f"(SELECT * FROM {schema_name}.{table_name} LIMIT 1) AS sub"
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

        date_columns = [col_name for col_name, dtype in df.dtypes if dtype in ["date", "timestamp"]]
        if not date_columns:
            print(f"{schema_name}.{table_name} →  No date/timestamp columns.")
            tables_without_date.append((schema_name, table_name))

        log_entries.append(Row(table_schema=schema_name, table_name=table_name, status="success", error=None))

    except Exception as e:
        error_msg = str(e)
        print(f"{schema_name}.{table_name} →  ERROR: {error_msg}")
        log_entries.append(Row(table_schema=schema_name, table_name=table_name, status="failed", error=error_msg))


def run_inspection(table_list):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(inspect_table, schema, table) for schema, table in table_list]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Inspecting tables"):
            pass

run_inspection(migrate_tables)

if tables_without_date:
    no_date_df = spark.createDataFrame(tables_without_date, ["table_schema", "table_name"])
    no_date_df.display()

log_df = spark.createDataFrame(log_entries)

log_table_name = "sd_digivate_stage_catalog.default.vertica_no_date_column_logs"
log_df.write.mode("overwrite").saveAsTable(log_table_name)

print(f"\n Log table written to: {log_table_name}")
success_count = log_df.filter("status = 'success'").count()
failed_count = log_df.filter("status = 'failed'").count()
print(f"Success: {success_count},  Failed: {failed_count}")



# COMMAND ----------

no_date_df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.vertica_no_date_column_logs