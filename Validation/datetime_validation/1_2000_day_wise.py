# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col
pdf = pd.read_csv("/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/datetime_validation/tables_batch_1_2000.csv")
date_columns_df = spark.createDataFrame(pdf)

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, StringType

array_string_schema = ArrayType(StringType())
date_columns_df = date_columns_df.withColumn(
    "date_columns", from_json(col("date_columns"), array_string_schema)
)

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, year, month, dayofmonth, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, ArrayType
from pyspark.sql import DataFrame
from functools import reduce
from datetime import datetime
import concurrent.futures
import pandas as pd
import gc

VERTICA = {
    "url": "jdbc:vertica://vertica.ops.snapdeal.io:5433/snapdealdwh",
    "user": dbutils.secrets.get(scope="sd-prd-veritca", key="vertica_user"),
    "password": dbutils.secrets.get(scope="sd-prd-veritca", key="vertica_password"),
    "driver": "com.vertica.jdbc.Driver"
}
OUTPUT_TABLE = "sd_digivate_stage_catalog.default.trial_validation_20th_may_monthwise_1_2000_01"
MAX_WORKERS = 20
APPEND_BATCH_SIZE = 50
START_DATE_FOR_VALIDATION = "2025-01-01"

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

table_names_list = date_columns_df.collect()
if not table_names_list:
    print("No tables found in the input DataFrame. Exiting.")
    dbutils.notebook.exit("No tables found.")

table_schema_pairs = [(row.schema_name, row.table_name) for row in table_names_list]
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

table_df = date_columns_df.join(row_count_df, on=["schema_name", "table_name"], how="left").fillna(0)
table_list = table_df.orderBy(col("estimated_row_count").desc()).collect()

print(f"Fetched list of {len(table_list)} tables to process.")

def detect_date_column(row_obj):
    date_columns = row_obj['date_columns']
    if date_columns and len(date_columns) > 0:
        return date_columns[0]
    return None

def validate_table(schema, table, date_col_pre_identified):
    vertica_table = f"{schema}.{table}"
    uc_table = f"sd_dwh_prd.{schema}.{table}"

    start_time = datetime.now()
    print(f"Starting validation for {vertica_table}...")

    try:
        spark.sql(f"SELECT 1 FROM {uc_table} LIMIT 1").first()
    except Exception as e:
        print(f"ERROR: UC table {uc_table} missing or inaccessible → {str(e)[:200]}")
        return spark.createDataFrame([(uc_table, None, None, None, None, None, f"ERROR: UC missing/access - {str(e)[:200]}", None)], RESULT_SCHEMA)

    try:
        date_col = date_col_pre_identified
        
        if date_col:
            print(f"Validating {vertica_table} using date column `{date_col}` (day-wise counts from {START_DATE_FOR_VALIDATION})")
            
            query_vertica_daily = f"""
                SELECT EXTRACT(YEAR FROM {date_col}) AS dpYear,
                       EXTRACT(MONTH FROM {date_col}) AS dpMonth,
                       EXTRACT(DAY FROM {date_col}) AS dpDay,
                       COUNT(*) AS source_count
                FROM {vertica_table}
                WHERE {date_col} IS NOT NULL
                  AND {date_col} >= '{START_DATE_FOR_VALIDATION}'
                GROUP BY 1, 2, 3
            """
            try:
                v_df_daily = spark.read.format("jdbc") \
                    .option("url", VERTICA["url"]) \
                    .option("query", query_vertica_daily) \
                    .option("user", VERTICA["user"]) \
                    .option("password", VERTICA["password"]) \
                    .option("driver", VERTICA["driver"]) \
                    .load()
            except Exception as e:
                print(f"ERROR fetching Vertica daily counts for {vertica_table} → {str(e)[:200]}")
                return spark.createDataFrame([(uc_table, None, None, None, None, None, f"ERROR: Vertica daily query - {str(e)[:200]}", date_col)], RESULT_SCHEMA)

            try:
                uc_df_daily = spark.sql(f"""
                    SELECT YEAR({date_col}) AS dpYear,
                           MONTH({date_col}) AS dpMonth,
                           DAY({date_col}) AS dpDay,
                           COUNT(*) AS target_count
                    FROM {uc_table}
                    WHERE {date_col} IS NOT NULL
                      AND to_date({date_col}) >= to_date('{START_DATE_FOR_VALIDATION}')
                    GROUP BY 1, 2, 3
                """)
            except Exception as e:
                print(f"ERROR fetching UC daily counts for {uc_table} → {str(e)[:200]}")
                return spark.createDataFrame([(uc_table, None, None, None, None, None, f"ERROR: UC daily query - {str(e)[:200]}", date_col)], RESULT_SCHEMA)

            has_vertica_data_2025 = v_df_daily.filter(col("dpYear") == 2025).count() > 0
            has_uc_data_2025 = uc_df_daily.filter(col("dpYear") == 2025).count() > 0
            
            if not has_vertica_data_2025 and not has_uc_data_2025:
                print(f"No data found for 2025 onwards in either source or target for {vertica_table}.")
                return spark.createDataFrame([(uc_table, None, None, None, None, None, "not_data_found_for_2025", date_col)], RESULT_SCHEMA)

            final_results_df = v_df_daily.join(uc_df_daily, ["dpYear", "dpMonth", "dpDay"], "full") \
                .fillna(0, subset=["source_count", "target_count"]) \
                .withColumn("target_table_name", lit(uc_table)) \
                .withColumn("validation_status", when(col("source_count") == col("target_count"), "SUCCESS").otherwise("FAILURE")) \
                .withColumn("date_column_used", lit(date_col)) \
                .select(RESULT_SCHEMA.fieldNames())

        else:
            print(f"Skipping {vertica_table}: No suitable date column provided.")
            final_results_df = spark.createDataFrame([(uc_table, None, None, None, None, None, "SKIPPED: No Date Column", None)], RESULT_SCHEMA)
            
        print(f"Done validation for {vertica_table} in {(datetime.now() - start_time).total_seconds():.2f} sec")
        return final_results_df

    except Exception as e:
        print(f"UNEXPECTED ERROR during validation for {vertica_table}: {str(e)[:200]}")
        return spark.createDataFrame([(uc_table, None, None, None, None, None, f"ERROR: Unexpected - {str(e)[:200]}", None)], RESULT_SCHEMA)

def run_validation():
    num_tables_to_process = len(table_list)
    print(f"Starting validation for {num_tables_to_process} tables using {MAX_WORKERS} workers")
    start_run_time = datetime.now()

    batch_results_dfs = []
    processed_count = 0

    def align_schema(df):
        return df.select(
            col("target_table_name").cast(StringType()),
            col("dpYear").cast(IntegerType()),
            col("dpMonth").cast(IntegerType()),
            col("dpDay").cast(IntegerType()),
            col("source_count").cast(LongType()),
            col("target_count").cast(LongType()),
            col("validation_status").cast(StringType()),
            col("date_column_used").cast(StringType())
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(validate_table, row.schema_name, row.table_name, detect_date_column(row)): (row.schema_name, row.table_name)
            for row in table_list
        }

        for future in concurrent.futures.as_completed(futures):
            schema, table = futures[future]
            processed_count += 1

            try:
                result_df = future.result()
                if result_df is not None:
                    batch_results_dfs.append(align_schema(result_df))

            except Exception as e:
                print(f"ERROR processing task for {schema}.{table} in thread: {str(e)}")

            if len(batch_results_dfs) >= APPEND_BATCH_SIZE or processed_count == num_tables_to_process:
                if batch_results_dfs:
                    try:
                        combined_df = reduce(DataFrame.unionByName, batch_results_dfs)

                        print(f"Writing batch {len(batch_results_dfs)} results. Processed: {processed_count}/{num_tables_to_process}")
                        combined_df.write.mode("append").saveAsTable(OUTPUT_TABLE)
                        print(f"Batch written successfully.")

                    except Exception as e:
                        print(f"ERROR writing batch to {OUTPUT_TABLE}: {str(e)}")

                    batch_results_dfs.clear()
                    gc.collect()

    if batch_results_dfs:
        try:
            print(f"Writing final batch {len(batch_results_dfs)} results.")
            combined_df = reduce(DataFrame.unionByName, batch_results_dfs)
            combined_df.write.mode("append").saveAsTable(OUTPUT_TABLE)
            print(f"Final batch written successfully.")
            combined_df.unpersist()
        except Exception as e:
            print(f"ERROR writing final batch to {OUTPUT_TABLE}: {str(e)}")

    total_time_minutes = (datetime.now() - start_run_time).total_seconds() / 60
    print(f"Validation RUN COMPLETE for {num_tables_to_process} tables in {total_time_minutes:.2f} minutes.")

run_validation()