# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd  
csv_path = "/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/dwh_19may/ETL_source_tables_288_.csv"
table_df = pd.read_csv(csv_path)#.head(5)
tables = table_df['Table_name'].dropna().tolist()
#print(tables)
spark.sql("""
CREATE TABLE IF NOT EXISTS sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_157_27_may (
    table_name STRING,
    source_table STRING,
    target_table STRING,
    status STRING,
    source_count LONG,
    target_count LONG,
    error_message STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP
) USING DELTA
""")

def migrate_table(full_table):
    start_time = datetime.now()
    schema, table = full_table.split(".")
    source_table = f"sd_dwh_prd.{schema}.{table}"
    target_table = f"dwh_etl.{schema}.{table}"

    try:
        print(f"\n Starting migration: {full_table}")
        print(f"   → Source: {source_table}")
        print(f"   → Target: {target_table}")

        spark.sql(f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT * FROM {source_table}
        """)

        # Row count validation
        source_count = int(spark.sql(f"SELECT COUNT(*) AS cnt FROM {source_table}").collect()[0]['cnt'])
        target_count = int(spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_table}").collect()[0]['cnt'])

        print(f" Completed: {target_table}")
        print(f"   → Source count: {source_count}")
        print(f"   → Target count: {target_count}")

        status = "SUCCESS"
        error_message = None

    except Exception as e:
        source_count = target_count = 0
        status = "FAILED"
        error_message = str(e)
        #print(f" Failed for {full_table}: {error_message}")
        print(f" Failed for {full_table}: Table Not Found")

    end_time = datetime.now()

    # Write to log table
    log_data = [
        (
            full_table, 
            source_table, 
            target_table, 
            status, 
            source_count, 
            target_count, 
            error_message if error_message else '', 
            start_time, 
            end_time
        )
    ]
    columns = ["table_name", "source_table", "target_table", "status", "source_count", "target_count", "error_message", "start_time", "end_time"]
    spark.createDataFrame(log_data, columns).write.mode("append").format("delta").saveAsTable("sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_157_27_may")

    return (full_table, status, source_count, target_count)

MAX_THREADS = 8
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = [executor.submit(migrate_table, table) for table in tables]
    for future in as_completed(futures): 
        _ = future.result()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_157_27_may

# COMMAND ----------

# MAGIC %md
# MAGIC not found table (add _dwh at the end)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd

csv_path = "/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/dwh_19may/ETL_table_not_found_21.csv"
table_df = pd.read_csv(csv_path)#.head(1)
tables = table_df['table_name'].dropna().tolist() 
print(tables)


spark.sql("""
CREATE TABLE IF NOT EXISTS sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_21_not_found1_27may (
    table_name STRING,
    source_table STRING,
    target_table STRING,
    status STRING,
    source_count LONG,
    target_count LONG,
    error_message STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP
) USING DELTA
""")

def migrate_table(full_table):
    start_time = datetime.now()
    schema, table = full_table.split(".")
    schema_dwh = f"{schema}_dwh"
    source_table = f"sd_dwh_prd.{schema_dwh}.{table}"
    target_table = f"dwh_etl.{schema_dwh}.{table}"

    try:
        print(f"\n Starting migration: {full_table}")
        print(f"   → Source: {source_table}")
        print(f"   → Target: {target_table}")

        spark.sql(f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT * FROM {source_table}
        """)

        # Row count validation
        source_count = int(spark.sql(f"SELECT COUNT(*) AS cnt FROM {source_table}").collect()[0]['cnt'])
        target_count = int(spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_table}").collect()[0]['cnt'])

        print(f" Completed: {target_table}")
        print(f"   → Source count: {source_count}")
        print(f"   → Target count: {target_count}")

        status = "SUCCESS"
        error_message = None

    except Exception as e:
        source_count = target_count = 0
        status = "FAILED"
        error_message = str(e)
        #print(f" Failed for {full_table}: {error_message}")
        print(f" Failed for {full_table}: Table Not Found")

    end_time = datetime.now()

    # Write to log table
    log_data = [
        (
            full_table, 
            source_table, 
            target_table, 
            status, 
            source_count, 
            target_count, 
            error_message if error_message else '', 
            start_time, 
            end_time
        )
    ]
    columns = ["table_name", "source_table", "target_table", "status", "source_count", "target_count", "error_message", "start_time", "end_time"]
    spark.createDataFrame(log_data, columns).write.mode("append").format("delta").saveAsTable("sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_21_not_found1_27may")

    return (full_table, status, source_count, target_count)

MAX_THREADS = 8

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = [executor.submit(migrate_table, table) for table in tables]
    for future in as_completed(futures):
        _ = future.result()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_21_not_found1_27may

# COMMAND ----------

# MAGIC %md
# MAGIC 57 intermediate table 

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd

csv_path = "/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/dwh_19may/ETL_inm_tgt_57_tables.csv"
table_df = pd.read_csv(csv_path)#.head(5)
tables = table_df['table_name'].dropna().tolist()
#print(tables)
spark.sql("""
CREATE TABLE IF NOT EXISTS sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_int_57_26_may (
    table_name STRING,
    source_table STRING,
    target_table STRING,
    status STRING,
    source_count LONG,
    target_count LONG,
    error_message STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP
) USING DELTA
""")

def migrate_table(full_table):
    start_time = datetime.now()
    schema, table = full_table.split(".")
    source_table = f"sd_dwh_prd.{schema}.{table}"
    target_table = f"dwh_etl.{schema}.{table}" 

    try:
        print(f"\n Starting migration: {full_table}")
        print(f"   → Source: {source_table}")
        print(f"   → Target: {target_table}")
        spark.sql(f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT * FROM {source_table}
        """)
    

        # Row count validation
        source_count = int(spark.sql(f"SELECT COUNT(*) AS cnt FROM {source_table}").collect()[0]['cnt'])
        target_count = int(spark.sql(f"SELECT COUNT(*) AS cnt FROM {target_table}").collect()[0]['cnt'])

        print(f" Completed: {target_table}")
        print(f"   → Source count: {source_count}")
        print(f"   → Target count: {target_count}")

        status = "SUCCESS"
        error_message = None

    except Exception as e:
        source_count = target_count = 0
        status = "FAILED"
        error_message = str(e)
        #print(f" Failed for {full_table}: {error_message}")
        print(f" Failed for {full_table}: Table Not Found")

    end_time = datetime.now()

    # Write to log table
    log_data = [
        (
            full_table, 
            source_table, 
            target_table, 
            status, 
            source_count, 
            target_count, 
            error_message if error_message else '', 
            start_time, 
            end_time
        )
    ]
    columns = ["table_name", "source_table", "target_table", "status", "source_count", "target_count", "error_message", "start_time", "end_time"]
    spark.createDataFrame(log_data, columns).write.mode("append").format("delta").saveAsTable("sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_int_57_26_may")

    return (full_table, status, source_count, target_count)

MAX_THREADS = 8

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = [executor.submit(migrate_table, table) for table in tables]
    for future in as_completed(futures):
        _ = future.result()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.ELT_table_migration_log_dwh_etl_int_57_26_may