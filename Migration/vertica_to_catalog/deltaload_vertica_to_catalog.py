# Databricks notebook source
pip install vertica_python

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True)
])
data = [("analytics_logistics", "rto_basedata_v4")]
meta_df = spark.createDataFrame(data, schema)



# COMMAND ----------

from pyspark.sql.functions import col
import vertica_python
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
import time 

catalog_name = "dwh_etl" #to test
CONFIG = {
    "host": "vertica.ops.snapdeal.io",
    "port": 5433,
    "database": "snapdealdwh",
    # "user": dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_user"),
    # "password": dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_password")
    'user': dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_user") ,
    'password' : dbutils.secrets.get(scope = "sd-prd-veritca", key = "vertica_password") 
}

AUDIT_CATALOG = "sd_digivate_stage_catalog"
AUDIT_SCHEMA = "vertica_deltaload_log"
AUDIT_LOG_TABLE = "deltaload_log2"

audit_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("rows_processed", IntegerType(), True),
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

# def update_audit_log(table_name, status, rows_processed=0, error_message=None):
#     end_time = datetime.now()
#     spark.sql(f"""
#         UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
#         SET end_time = '{end_time}',
#             status = '{status}',
#             rows_processed = {rows_processed},
#             error_message = '{error_message if error_message else "NULL"}'
#         WHERE table_name = '{table_name}'
        
#     """)


import time

def update_audit_log(table_name, status, rows_processed=0, error_message=None, max_retries=4):

    for attempt in range(max_retries):
        try:
            end_time = datetime.now()
            # Use optimistic concurrency control
            sql = f"""
                UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
                SET 
                    end_time = '{end_time}',
                    status = '{status}',
                    rows_processed = {rows_processed},
                    error_message = '{error_message if error_message else "NULL"}'
                WHERE table_name = '{table_name}'
                
            """
            spark.sql(sql)
            return  # Success - exit function
            
        except Exception as e:
            if "ConcurrentAppendException" in str(e) and attempt < max_retries - 1:
                print(f"Retrying audit update for {table_name} (attempt {attempt + 1})")
                time.sleep(1)  # Add small delay between retries
                continue
            else:
                print(f"Failed to update audit log after {attempt + 1} attempts: {str(e)}")
                raise

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
    

#meta_df = spark.table("sd_digivate_stage_catalog.default.deltaload_1_3000_sheet")#.limit(40)
#meta_df = spark.table("sd_digivate_stage_catalog.default.deltaload_3001_6000_sheet")
#meta_df = spark.table("sd_digivate_stage_catalog.default.deltaload_9001_10155_sheet")
#meta_df = spark.table("sd_digivate_stage_catalog.default.deltaload_6001_9000_sheet")
#meta_df = spark.table("sd_digivate_stage_catalog.default.133_in_progress_deltaload")
# meta_df = meta_df.filter(
#     (col("schema_name") == 'stage_temp') & 
#     (col("table_name") == "tmp_transaction_master")
# )



tables_to_process = meta_df.select("schema_name", "table_name").collect()
print(tables_to_process)
total_tables = len(tables_to_process)
print(f"Total tables to process: {total_tables}")

#Function to get date column name
def get_date_column(cursor, schema_name, table_name):
    cursor.execute(f"""
        SELECT column_name FROM columns
        WHERE table_name = '{table_name}'
        AND table_schema = '{schema_name}'
        AND data_type IN ('date', 'timestamp', 'timestamptz')
        ORDER BY ordinal_position LIMIT 1
    """)
    result = cursor.fetchone()
    print("date col",result[0])
    return result[0] if result else None


def check_table_exists(spark, catalog_table):
    try:
        spark.sql(f"DESCRIBE TABLE {catalog_table}")
        return True
    except Exception:
        return False


# COMMAND ----------


# Function to read Vertica data and insert into the catalog after deduplication
def read_vertica_and_insert(schema_name, table_name):
    full_table_name = f"{schema_name}.{table_name}"
    catalog_table = f"{catalog_name}.{schema_name}.{table_name}"
    print("full_table_name",full_table_name)
    # print("schema name",schema_name)
    # print("table name",table_name)
    table_running_stats = get_audit_log(full_table_name)
    if table_running_stats in ("ERROR","SUCCESS"):
        print(f"Skipping {full_table_name} as it is already processed (Status: {table_running_stats})")
        return False
    try:
        create_audit_log(full_table_name, catalog_table)
        # Check if target table exists first
        if not check_table_exists(spark, catalog_table):
            error_msg = f"Target table {full_table_name} does not exist"
            print(error_msg)
            update_audit_log(full_table_name, "ERROR", error_message=error_msg)
            return False                                                                                                                                                                                                             
        # Create initial audit entry
        # create_audit_log(full_table_name, catalog_table)
        
        # Get target table schema
        #target_columns = [f.name for f in spark.table(catalog_table).schema.fields]
        
        # Establish Vertica connection and get the date column
        conn = vertica_python.connect(**CONFIG)
        cursor = conn.cursor()
        print('schema_name',schema_name)
        print('table_name',table_name)
        date_col = get_date_column(cursor, schema_name, table_name)
        print('date col',date_col)
        conn.close()

        if not date_col:
            print(f" No date column in {schema_name}.{table_name}, skipping.")
            error_message=f"No date/timestamp column found in {schema_name}.{table_name}"
            #update_audit_log(full_table_name, status="ERROR", rows_processed=0, error_message=error_message)
            return False
            
        # Construct filtered query for Vertica
        
        vertica_query = f"(SELECT * FROM {schema_name}.{table_name} WHERE {date_col} > '2025-03-01') AS subq"
        # Read data from Vertica into a DataFrame
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:vertica://{CONFIG['host']}:{CONFIG['port']}/{CONFIG['database']}") \
            .option("user", CONFIG['user']) \
            .option("password", CONFIG['password']) \
            .option("driver", "com.vertica.jdbc.Driver") \
            .option("dbtable", vertica_query) \
            .load()
        
        base_name = f"{schema_name}_{table_name}".replace(".", "_").replace("-", "_")
        temp_view = f"temp_view_{base_name}"
        existing_view = f"temp_existing_data_{base_name}"
        new_data_view = f"temp_new_data_{base_name}"
        #temp_view = f"tmp_{schema_name.replace('_', '')}_{table_name}"

        df.createOrReplaceTempView(temp_view)

        print(f"Successfully created temporary view: {temp_view}")

        catalog_table = f"{catalog_name}.{schema_name}.{table_name}"
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {existing_view} AS
            SELECT * FROM {catalog_table}
            WHERE {date_col} > '2025-03-01'
        """)

        # Perform an anti-join in SQL to filter out existing rows
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {new_data_view} AS
            SELECT * FROM {temp_view} AS new
            WHERE NOT EXISTS (
                SELECT 1 FROM {existing_view} AS existing
                WHERE {" AND ".join([f"new.{c} = existing.{c}" for c in df.columns])} 
            )
        """)

        # Insert unique rows into the catalog table using SQL
        spark.sql(f"""
            INSERT INTO {catalog_table} 
            SELECT * FROM {new_data_view}
        """)

        # After successful insert
        rows_inserted = spark.sql(f"SELECT COUNT(*) as cnt FROM {new_data_view}").first()['cnt']
        update_audit_log(full_table_name, "SUCCESS", rows_processed=rows_inserted)
        print(f"Inserted into {catalog_table} from Vertica {schema_name}.{table_name}")
        spark.sql(f"DROP VIEW IF EXISTS {temp_view}")
        spark.sql(f"DROP VIEW IF EXISTS {existing_view}")
        spark.sql(f"DROP VIEW IF EXISTS {new_data_view}")
        return True

    except Exception as e:
        error_msg = str(e).replace("'", "''").replace('"', '')  # Better error message escaping
        print(f" Failed to insert {full_table_name}: {error_msg}")
        update_audit_log(full_table_name, "ERROR", error_message=error_msg)
        return False
   




# COMMAND ----------

def main():
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(read_vertica_and_insert, row["schema_name"], row["table_name"])
            for row in tables_to_process
        ] 
        wait(futures, return_when=ALL_COMPLETED) 
main() 