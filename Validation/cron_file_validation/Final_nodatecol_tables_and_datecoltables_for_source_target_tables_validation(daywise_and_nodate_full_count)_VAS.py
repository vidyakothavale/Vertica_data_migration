# Databricks notebook source
# MAGIC %md
# MAGIC combine script for date col tables and nodatecolumn table-
# MAGIC
# MAGIC if date column then daywise row count validation
# MAGIC
# MAGIC if no date col present in table then full row count

# COMMAND ----------

import os
import re
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

result = []

# COMMAND ----------

import os
import re

def get_schema_table_names(folder_path):
    source_pattern = re.compile(r'(?:FROM|JOIN)\s+(\w+)\.(\w+)', re.IGNORECASE)
    target_pattern = re.compile(r'(?:INTO|TABLE|UPDATE|MERGE INTO)\s+(\w+)\.(\w+)', re.IGNORECASE)
    
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                source_tables = set()
                target_tables = set()
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                        # Extract source tables
                        for match in source_pattern.findall(content):
                            source_tables.add(f"{match[0]}.{match[1]}")
                        # Extract target tables
                        for match in target_pattern.findall(content):
                            target_tables.add(f"{match[0]}.{match[1]}")
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                # Append each file's data to the result list
                result.append({
                    "file_path": file_path,
                    "source_table_names": sorted(source_tables),
                    "target_table_names": sorted(target_tables)
                })
    

# Example usage
folder_path = f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/'
results = get_schema_table_names(folder_path)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

schema = StructType([
    StructField("file_path", StringType(), True),
    StructField("source_table_names",ArrayType(StringType()), True),
    StructField("target_table_names", ArrayType(StringType()), True)
])

dataFrme = spark.createDataFrame(result, schema)

# COMMAND ----------

display(dataFrme)

# COMMAND ----------

file_paths = dataFrme.select("file_path").toPandas()["file_path"].tolist()
print(len(file_paths))
print(file_paths[0])

# COMMAND ----------

def get_row_count(catalog, schema, table):
    try:
        df = spark.read.table(f"{catalog}.{schema}.{table}")
        return df.count()
    except:
        return None

# COMMAND ----------


def date_column_value_processor_for_source(date_columns_df): 
    # schema_name , table_name , file_path , list(datacolumns)
    from pyspark.sql.functions import col, lit, year, month, dayofmonth, to_date, count
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
    from pyspark.sql import DataFrame
    from datetime import datetime
    import gc

    START_DATE_FOR_VALIDATION = "2025-01-01"
    RESULT_SCHEMA = StructType([
        StructField("target_table_name", StringType()),
        StructField("dpYear", IntegerType(), True),
        StructField("dpMonth", IntegerType(), True),
        StructField("dpDay", IntegerType(), True),
        StructField("source_count", LongType()),
        StructField("fetch", StringType()),
        StructField("date_column_used", StringType(), True),
        StructField("file_path",StringType(),True)
    ])

    table_names_list = date_columns_df.collect()
    if not table_names_list:
        # dbutils.notebook.exit("No tables found.")
        print("Empty source dataframe")

    result_data = []

    def detect_date_column(row_obj): # schema_name , table_name , file_path , list(datacolumns)
        date_columns = row_obj['date_columns']
        if date_columns and len(date_columns) > 0:
            return date_columns[0]
        return None

    for row in table_names_list: # schema_name , table_name , file_path , list(datacolumns)
        schema = row.schema_name
        table = row.table_name
        file_path_ = row.file_path
        date_col_pre_identified = detect_date_column(row)
        uc_table = f"sd_dwh_prd.{schema}.{table}"
        start_time = datetime.now()
        print(f"Starting validation for {uc_table}...")
        try:
            spark.sql(f"SELECT 1 FROM {uc_table} LIMIT 1").first()
        except Exception as e:
            print(f"ERROR: UC table {uc_table} missing or inaccessible → {str(e)[:200]}")
            result_data.append((uc_table, None, None, None, None, f"ERROR: UC missing/access - {str(e)[:200]}", None ,file_path_))
            continue

        date_col = date_col_pre_identified 
        if date_col:
            print(f"Processing {uc_table} using date column `{date_col}` (day-wise counts from {START_DATE_FOR_VALIDATION})")
            try:
                uc_df_daily = spark.sql(f"""
                    SELECT 
                    YEAR({date_col}) AS dpYear,
                        MONTH({date_col}) AS dpMonth,
                        DAY({date_col}) AS dpDay,
                        COUNT(*) AS target_count
                        
                    FROM {uc_table}
                    WHERE {date_col} IS NOT NULL
                    AND to_date({date_col}) >= to_date('{START_DATE_FOR_VALIDATION}')
                    GROUP BY 1, 2, 3 
                """)
                daily_counts = uc_df_daily.filter(col("dpYear") == 2025).collect()
                if not daily_counts:
                    result_data.append((uc_table, None, None, None, None, "no_data_for_2025", date_col , file_path_))
                else:
                    for dc in daily_counts:
                        result_data.append((uc_table, dc.dpYear, dc.dpMonth, dc.dpDay, dc.target_count, "SUCCESS", date_col,file_path_))
            except Exception as e:
                print(f"ERROR fetching UC daily counts for {uc_table} → {str(e)[:200]}")
                result_data.append((uc_table, None, None, None, None, f"ERROR: UC daily query - {str(e)[:200]}", date_col , file_path_))
        else:
            print(f"Skipping {uc_table}: No suitable date column provided.")
            result_data.append((uc_table, None, None, None, None, "SKIPPED: No Date Column", None , file_path_))

        print(f"Done processing for {uc_table} in {(datetime.now() - start_time).total_seconds():.2f} sec")
        gc.collect()
    result_df_source_to_compare = spark.createDataFrame(result_data, RESULT_SCHEMA)

    return result_df_source_to_compare
        

# COMMAND ----------

def date_column_value_processor_for_target(date_columns_df):

    from pyspark.sql.functions import col, lit, year, month, dayofmonth, to_date, count
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
    from pyspark.sql import DataFrame
    from datetime import datetime
    import gc

    START_DATE_FOR_VALIDATION = "2025-01-01"

    RESULT_SCHEMA = StructType([
        StructField("target_table_name", StringType()),
        StructField("dpYear", IntegerType(), True),
        StructField("dpMonth", IntegerType(), True),
        StructField("dpDay", IntegerType(), True),
        StructField("target_count", LongType()),
        StructField("fetch", StringType()),
        StructField("date_column_used", StringType(), True),
        StructField("file_path",StringType(),True)
    ])

    table_names_list = date_columns_df.collect() #(schema_name  , table_name , [date_columns])
    if not table_names_list:
        # dbutils.notebook.exit("No tables found.")
        print("Empty target dataframe")

    result_data = []

    def detect_date_column(row_obj): # #(schema_name  , table_name , [date_columns])
        date_columns = row_obj['date_columns']
        if date_columns and len(date_columns) > 0:
            return date_columns[0]
        return None

    for row in table_names_list:  #(schema_name  , table_name , [date_columns])
        schema = row.schema_name
        table = row.table_name
        file_path_ = row.file_path
        date_col_pre_identified = detect_date_column(row) #(schema_name  , table_name , [date_columns])
        uc_table = f"dwh_etl.{schema}.{table}"
        start_time = datetime.now()
        print(f"Starting validation for {uc_table}...")
        try:
            spark.sql(f"SELECT 1 FROM {uc_table} LIMIT 1").first()
        except Exception as e:
            print(f"ERROR: UC table {uc_table} missing or inaccessible → {str(e)[:200]}")
            result_data.append((uc_table, None, None, None, None, f"ERROR: UC missing/access - {str(e)[:200]}", None , file_path_))
            continue

        date_col = date_col_pre_identified
        if date_col:
            print(f"Processing {uc_table} using date column `{date_col}` (day-wise counts from {START_DATE_FOR_VALIDATION})")
            try:
                uc_df_daily = spark.sql(f"""
                    SELECT 
                    
                    YEAR({date_col}) AS dpYear,
                        MONTH({date_col}) AS dpMonth,
                        DAY({date_col}) AS dpDay,
                        COUNT(*) AS target_count
                    FROM {uc_table}
                    WHERE {date_col} IS NOT NULL
                    AND to_date({date_col}) >= to_date('{START_DATE_FOR_VALIDATION}')
                    GROUP BY 1, 2, 3
                """)
                daily_counts = uc_df_daily.filter(col("dpYear") == 2025).collect() #()
                if not daily_counts:
                    result_data.append((uc_table, None, None, None, None, "no_data_for_2025", date_col , file_path_))
                else:
                    for dc in daily_counts:
                        result_data.append((uc_table, dc.dpYear, dc.dpMonth, dc.dpDay, dc.target_count, "SUCCESS", date_col , file_path_))
            except Exception as e:
                print(f"ERROR fetching UC daily counts for {uc_table} → {str(e)[:200]}")
                result_data.append((uc_table, None, None, None, None, f"ERROR: UC daily query - {str(e)[:200]}", date_col , file_path_))
        else:
            print(f"Skipping {uc_table}: No suitable date column provided.")
            result_data.append((uc_table, None, None, None, None, "SKIPPED: No Date Column", None, file_path_))

        print(f"Done processing for {uc_table} in {(datetime.now() - start_time).total_seconds():.2f} sec")
        gc.collect()

    result_df_target_of_client = spark.createDataFrame(result_data, RESULT_SCHEMA)
    return result_df_target_of_client
        

# COMMAND ----------

def final_data_frame(result_df_source_to_compare ,result_df_target_of_client ):
    from pyspark.sql.functions import split, element_at

    result_df_source_to_compare_spilited = ( result_df_source_to_compare.withColumn("schema_name", element_at(split("target_table_name", "\\."), 2)).withColumn("table_name", element_at(split("target_table_name", "\\."), 3)) )
   


    result_df_target_to_compare_spilited = ( result_df_target_of_client.withColumn("schema_name", element_at(split("target_table_name", "\\."), 2)).withColumn("table_name", element_at(split("target_table_name", "\\."), 3)))

    return result_df_source_to_compare_spilited ,result_df_target_to_compare_spilited

# COMMAND ----------

def final_data_frame_which_gives_result(result_df_source_to_compare_spilited, result_df_target_to_compare_spilited):

    import pandas as pd

    result_df_pandas = result_df_source_to_compare_spilited.toPandas()
    # display(result_df_pandas)
    # type(result_df_pandas)
    df_vertica_pandas = result_df_target_to_compare_spilited.toPandas()
    # display(df_vertica_pandas)
    # type(df_vertica_pandas)

    join_columns = ['schema_name', "table_name", "dpYear", "dpMonth", "dpDay"]
    merged_df = pd.merge(result_df_pandas, df_vertica_pandas, on=join_columns, how='outer', suffixes=('_target', '_source'))
    # display(merged_df)
    def validate_counts(row):
        if pd.isna(row['target_count']) or pd.isna(row['source_count']): #
            return 'Not Applicable'
        elif row['target_count'] == row['source_count']:
            return 'Success'
        else:
            return 'Failure'

    def validate_date_columns(row):
        if row['date_column_used_target'] == row['date_column_used_source']:
            return 'Success'
        else:
            return 'Failure'

    merged_df['validation_status'] = merged_df.apply(validate_counts, axis=1)
    merged_df['date_column_validation'] = merged_df.apply(validate_date_columns, axis=1)
    # print("2nd merge_df")
    # display(merged_df)
    final_df_outer = merged_df[[
        'target_table_name_source',
        'target_table_name_target',
        'schema_name',
        'table_name',
        'dpYear',
        'dpMonth',
        'dpDay',
        'target_count',
        'source_count',
        'validation_status',
        'date_column_used_target',
        'date_column_used_source',
        'file_path_target',
        'date_column_validation'
        
    ]]

    return(final_df_outer)

# COMMAND ----------

failed_sql_files = []
sql_file_ready_for_run = []
master_csv_read = pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/vertica_schema_data.csv")
csv_df_spark = spark.createDataFrame(master_csv_read) #master sheet

# COMMAND ----------



def run_files(file):
    source_pattern = re.compile(r'(?:FROM|JOIN)\s+(\w+)\.(\w+)', re.IGNORECASE)
    target_pattern = re.compile(r'(?:INTO|TABLE|UPDATE|MERGE INTO)\s+(\w+)\.(\w+)', re.IGNORECASE)
    if file.endswith('.sql'):
        source_tables = set()
        target_tables = set()
        try:
            with open(file, 'r') as f:
                content = f.read()
                # Extract source tables
                for match in source_pattern.findall(content):
                    source_tables.add(f"{match[0]}.{match[1]}")
                # Extract target tables
                for match in target_pattern.findall(content):
                    target_tables.add(f"{match[0]}.{match[1]}")
        except Exception as e:
            print(f"Error reading file {file}: {e}")
        # Append each file's data to the result list
        dict1 = ({
            "file_path": file,
            "source_table_names": sorted(source_tables),
            "target_table_names": sorted(target_tables)
        })

        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

        schema = StructType([
            StructField("file_path", StringType(), True),
            StructField("source_table_names",ArrayType(StringType()), True),
            StructField("target_table_names", ArrayType(StringType()), True)
        ])


        temp_df = spark.createDataFrame([dict1], schema)
        # display(temp_df)

       

        # Remove duplicates from source_table_names and target_table_names
        dataFrme = temp_df.withColumn("source_table_names", array_distinct(temp_df.source_table_names))
        dataFrme = dataFrme.withColumn("target_table_names", array_distinct(dataFrme.target_table_names))
        # print("dataFrme")
        # display(dataFrme)
        
        filtered_df = dataFrme.filter(size(dataFrme.source_table_names) > 0)
        
        if filtered_df.count() == 0:
            temp_dict = {"file_path": file ,"error": "No source table found in this sql file"}
            failed_sql_files.append(temp_dict)
        else:    
            # Explode source_table_names and split it into schema_name and table_name
            source_df_spark = (filtered_df.select("file_path", explode("source_table_names").alias("source_table_name"))
            .withColumn("schema_name", split("source_table_name", "\.").getItem(0)) 
            .withColumn("table_name", split("source_table_name", "\.").getItem(1)))
            # print("source_df_spark")
            # display(source_df_spark)

            spark_df = source_df_spark.select("file_path","schema_name","table_name")
            print("spark_df")        
            # spark_df.display()

            source_df_wdj=csv_df_spark.join(spark_df, on=['schema_name','table_name'], how='right') 
            print("source_df_wdj")  
            # source_df_wdj.display()

            
            

            # there is possibility of no date type columns in source_df_wdj so log that one
            # we are only processing for the date columns

            date_columns_df = source_df_wdj.filter((col("column_name").isin("date", "created", "updated", "createdAt", "updatedAt") | (col("data_type") == "date") | (col("data_type") == "timestamp") | (col("data_type") == "timestamp_ntz") )) \
                .groupBy("schema_name", "table_name","file_path") \
                .agg(collect_list("column_name").alias("date_columns"))  # schema_name , table_name , file_path , list(datacolumns)
            # print("date_columns_df")    
            # display(date_columns_df)
            # print(f"date_columns_df count : {date_columns_df.count()}")
            ############################################################################################

            

            all_tables_df = source_df_wdj.select("schema_name", "table_name").distinct()
            no_date_columns_df = all_tables_df.join(date_columns_df, on=["schema_name", "table_name"], how="anti")
            # print(f"all_tables_df count")
            # # display(all_tables_df)
            # print(f"no_date_columns_df count")
            # display(no_date_columns_df)


            filtered_source_df = source_df_spark.join(no_date_columns_df, on=['schema_name', 'table_name'], how='inner')
            print(f"filtered_source_df count")
            
            # print(f"filtered_source_df count:{filtered_source_df.count()}")

            # display(filtered_source_df)

            if filtered_source_df.count() > 0:

                source_df_pd = filtered_source_df.toPandas()

                schema = source_df_pd['schema_name']
                table_name = source_df_pd['table_name']
                source_report = []
                for i in range(len(schema)):
                    count_src = get_row_count("sd_dwh_prd", schema[i], table_name[i])
                    count_tgt = get_row_count("dwh_etl", schema[i], table_name[i])
                    match = count_src == count_tgt and count_src is not None and count_src is not None
                        

                    validation_row = {
                            "sql_file": file,
                            "schema_name": schema[i],
                            "table_name": table_name[i],
                            "source_count": count_src,
                            "target_count": count_tgt,
                            "validation_status": "Source count match" if match else "Not Validated",
                            "execution_status": "Passed" if match else "Failed"
                        }
                    source_report.append(validation_row) 

                source_explain_status_df = pd.DataFrame([
                        {
                            "sql_file": row["sql_file"],
                            "schema_name": row["schema_name"],
                            "table_name": row["table_name"],
                            "source_count": row["source_count"],
                            "target_count": row["target_count"],
                            "execution_status": row["execution_status"],
                            "explain_status": row["validation_status"]
                        }
                        for row in source_report 
                    ])

                summary_df = spark.createDataFrame(source_explain_status_df).groupBy("sql_file").agg(
                    F.min(F.when(F.col("execution_status") == "Passed", 1).otherwise(0)).alias("all_passed_flag"),
                    F.sum(F.when(F.col("execution_status") == "Passed", 1).otherwise(0)).alias("pass_count"),
                    F.sum(F.when(F.col("execution_status") == "Failed", 1).otherwise(0)).alias("fail_count"),
                    F.count("*").alias("total_tables")
                )

                summary_df = summary_df.withColumn(
                    "all_passed",
                    F.expr("all_passed_flag == 1")
                ).drop("all_passed_flag")


                passed_files = summary_df.filter("all_passed = true").select("sql_file").distinct()

                passed_files_list = [row.sql_file for row in passed_files.collect()]
                print(f"passed_files_list : {len(passed_files_list)}")
            else:
                    passed_files_list = None
                
                        
            # ##############################################################################################

            if date_columns_df.count() == 0:

                temp_dict = {"file_path": file ,
                        "error": "dataframe does not contain date column in it "}
                    
                failed_sql_files.append(temp_dict)
                
            else:
                
                source_result_df = date_column_value_processor_for_source(date_columns_df) # ["target_table_name", "dpYear", "dpMonth", "dpDay", "source_count", "fetch", "date_column_used", "file_path"]
                # print("source_result_df")
                # display(source_result_df) 

                if source_result_df.count() == 0:
                    temp_dict = {"file_path": file ,
                        "error": "NO data available for  2025 for source table"}
                    failed_sql_files.append(temp_dict)
                else:                        
                    source_result_df_1 = source_result_df.withColumn("source_schema_table", regexp_replace("target_table_name", "^sd_dwh_prd\\.", "")).select('source_schema_table','dpYear' , 'dpMonth' , 'dpDay' ) #('source_schema_table','dpYear' , 'dpMonth' , 'dpDay' )
                    # print("source_result_df_1")
                    # display(source_result_df_1)
                    
                    target_result_df=date_column_value_processor_for_target(date_columns_df)   # schema_name , table_name , file_path , list(datacolumns)
                    #target_result_df= ["target_table_name", "dpYear", "dpMonth", "dpDay", "target_count", "fetch", "date_column_used", "file_path"]

                    if target_result_df.count() == 0:
                        temp_dict = {"file_path": file ,
                        "error": "NO data available for  2025 for target table"}
                        failed_sql_files.append(temp_dict)
                    else:

                        target_result_df = target_result_df.withColumn("source_schema_table", regexp_replace("target_table_name", "^dwh_etl\\.", ""))
                        # print("target_result_df")
                        # display(target_result_df)

                        target_result_df = target_result_df.join(source_result_df_1 , on=['source_schema_table','dpYear' , 'dpMonth' , 'dpDay'], how='inner')
                        # print("target_result_df")
                        # display(target_result_df)
                        if target_result_df.count() == 0:
                            temp_dict = {"file_path": file ,
                        "error": "NO common data between source and target"}
                            failed_sql_files.append(temp_dict)

                        else:

                            result_df_source_to_compare_spilited ,result_df_target_to_compare_spilited    = final_data_frame(source_result_df,target_result_df)  # split doing
                            
                            
                            unsuccessful_count = final_data_frame_which_gives_result(result_df_source_to_compare_spilited, result_df_target_to_compare_spilited)

                            
                            # ['target_table_name_source','target_table_name_target','schema_name','table_name','dpYear','dpMonth','dpDay','target_count','source_count','validation_status','date_column_used_target','date_column_used_source','file_path_target','date_column_validation']
                            print(f"file path is -> {file}")
                            print(type(unsuccessful_count))
                            display(unsuccessful_count)

                            unsuccessful_count_filtered = unsuccessful_count[~unsuccessful_count['validation_status'].isin(["Success"])]["validation_status"].count()
                            print("unsuccessful_count_filtered")
                            display(unsuccessful_count_filtered)
                                                            
                            if passed_files_list is not None:

                                if unsuccessful_count_filtered == 0 and len(passed_files_list) == 1:
                                    print(f"No of unsuccessfull rows : {unsuccessful_count_filtered}")
                                    print(f"Notebook to run", file)
                                    sql_file_ready_for_run.append(file)
                                    
                                else:
                                    print(f"No of unsuccessfull rows : {unsuccessful_count_filtered}")
                                    print(f"NO Notebook to run")
                                    temp_dict = {"file_path": file ,
                                        "error": "date wise row count doesnt match between source and target table "}
                                    failed_sql_files.append(temp_dict)
                            else:
                                
                                if unsuccessful_count_filtered == 0 :
                                    print(f"No of unsuccessfull rows : {unsuccessful_count_filtered}")
                                    print(f"Notebook to run", file)
                                    sql_file_ready_for_run.append(file)
                                    
                                else:
                                    print(f"No of unsuccessfull rows : {unsuccessful_count_filtered}")
                                    print(f"NO Notebook to run")
                                    temp_dict = {"file_path": file ,
                                        "error": "date wise row count doesnt match between source and target table "}
                                    failed_sql_files.append(temp_dict)




        

# COMMAND ----------

# run_files('/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/product_vertica/quality_metrics_sk/daily.sql')

# COMMAND ----------

print(failed_sql_files)

# COMMAND ----------

print(sql_file_ready_for_run)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

with ThreadPoolExecutor(max_workers=100) as executor:
    futures = [executor.submit(run_files, file) for file in file_paths]
    print(futures)                  
    print("*" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing source day wise row count is not matching failed sql files

# COMMAND ----------

failed_sql_files_dataFrame_spark = spark.createDataFrame(failed_sql_files)

# COMMAND ----------

failed_sql_files_dataFrame_spark.count()

# COMMAND ----------

failed_sql_files_dataFrame_spark.write.saveAsTable("sd_digivate_stage_catalog.default.day_wise_validation_failed_sql_files_29_May_2025_v1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing source day wise row count is matching success sql files

# COMMAND ----------

day_wise_validation_passed_file_paths = spark.createDataFrame(sql_file_ready_for_run , schema = "file_path string" )

# COMMAND ----------

display(day_wise_validation_passed_file_paths)

# COMMAND ----------

day_wise_validation_passed_file_paths.write.saveAsTable("sd_digivate_stage_catalog.default.day_wise_validation_passed_sql_files_29_May_2025_v1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## running successful validation files 

# COMMAND ----------

from pyspark.sql import Row
import os

from concurrent.futures import ThreadPoolExecutor, as_completed
 
results_final_answer = []

# COMMAND ----------

display(results_final_answer)

# COMMAND ----------

sql_file_run("/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/product_vertica/Daily_Morning_Capsule/mailbody.sql")

# COMMAND ----------


 
def sql_file_run(sql_file_path):
    file_name = os.path.basename(sql_file_path)
    print(f"\nProcessing file: {sql_file_path}")
   
    with open(sql_file_path, "r") as f:
        sql_content = f.read()
    
       
    sql_statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()] # select 1 ;
   
    # Run each query
    for idx, query in enumerate(sql_statements):
        print(f"Running query {idx + 1} from file {file_name}")
        try:
            query_n = """
                USE CATALOG dwh_etl;
                {query}
            """
            spark.sql(query)
            results_final_answer.append(Row(file_name=file_name, query=query, status="Success", error_message=None , sql_file_path=sql_file_path))
        except Exception as e:
            results_final_answer.append(Row(file_name=file_name, query=query, status="Failed", error_message=str(e) , sql_file_path=sql_file_path))
 
# error_df = spark.createDataFrame(results)
# error_df.filter("status = 'Failed'").display()

# COMMAND ----------

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(sql_file_run, file) for file in sql_file_ready_for_run]
    print("*" * 100)

# COMMAND ----------

sql_query_execution_result_df = spark.createDataFrame(results_final_answer)

# COMMAND ----------

display(sql_query_execution_result_df)

# COMMAND ----------

sql_query_execution_result_df.write.saveAsTable("sd_digivate_stage_catalog.default.validation_passed_query_execution_result_29_May_2025_v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.validation_passed_query_execution_result_29_May_2025_v1;

# COMMAND ----------

sql_file_run_complete = sql_query_execution_result_df.groupBy("sql_file_path").agg(collect_set("Status").alias("status"))
sql_file_successfully_ran = sql_file_run_complete.withColumn("sql_file_execution_status", when( array_contains("status", "Failed"),"Failed").otherwise("Success"))
# print(type(sql_file_successfully_ran))
display(sql_file_successfully_ran)


# COMMAND ----------

sql_file_successfully_ran.filter("sql_file_execution_status == 'Success'").display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### experiment

# COMMAND ----------

print(failed_sql_files)

# COMMAND ----------

source_df_pd = filtered_source_df.toPandas()

sql_file_groups = list(source_df_pd.groupby("file_path"))
print("sql_file_groups")
print(type(sql_file_groups))

# COMMAND ----------

# Source Validation
source_file_groups = list(source_df_pd.groupby("file_path"))
print("source_file_groups")
print((source_file_groups[0]))


# COMMAND ----------



# COMMAND ----------

# Source Validation
source_file_groups = list(source_df_pd.groupby("file_path"))

source_report = []

for sql_file, group in source_file_groups:
    source_tables = group[["schema_name", "table_name"]]
    source_validation = []

    for _, row in source_tables.iterrows():
        schema, table = row["schema_name"], row["table_name"]
        count_src = get_row_count("sd_dwh_prd", schema, table)
        count_tgt = get_row_count("dwh_etl", schema, table)
        match = count_src == count_tgt and count_src is not None and count_src is not None

        validation_row = {
            "sql_file": sql_file,
            "schema_name": schema,
            "table_name": table,
            "source_count": count_src,
            "target_count": count_tgt,
            "validation_status": "Source count match" if match else "Not Validated",
            "execution_status": "Passed" if match else "Failed"
        }

        source_validation.append(validation_row)

    source_report.extend(source_validation)

source_explain_status_df = pd.DataFrame([
    {
        "sql_file": row["sql_file"],
        "schema_name": row["schema_name"],
        "table_name": row["table_name"],
        "source_count": row["source_count"],
        "target_count": row["target_count"],
        "execution_status": row["execution_status"],
        "explain_status": row["validation_status"]
    }
    for row in source_report
])

print("Source Table Explain & Execution Status") 
spark.createDataFrame(source_explain_status_df).display()

# COMMAND ----------

from pyspark.sql import functions as F


summary_df = spark.createDataFrame(source_explain_status_df).groupBy("sql_file").agg(
    F.min(F.when(F.col("execution_status") == "Passed", 1).otherwise(0)).alias("all_passed_flag"),
    F.sum(F.when(F.col("execution_status") == "Passed", 1).otherwise(0)).alias("pass_count"),
    F.sum(F.when(F.col("execution_status") == "Failed", 1).otherwise(0)).alias("fail_count"),
    F.count("*").alias("total_tables")
)

summary_df = summary_df.withColumn(
    "all_passed",
    F.expr("all_passed_flag == 1")
).drop("all_passed_flag")

summary_df.display()

# COMMAND ----------

passed_files = summary_df.filter("all_passed = true").select("sql_file").distinct()
display(passed_files)
passed_files_list = [row.sql_file for row in passed_files.collect()]
print(len(passed_files_list))

# COMMAND ----------

# sorce table list with sql file name
# # target table list with sql file name

# check if table present in catalog then 

# suppose we have 100 tables in source need to find out list of tables that have .isin("date", "created", "updated", "createdAt", "updatedAt") this col name if this column preent in table then daywise validation needed  
# if source count mathch (for source table)then run sql file (if from sql file all tables present then run sql file) 
# after running cron then check source and taregt count for target tables if match then do hash validation  



# if table not present this col then full row count validation needed    
# source count check if match then run sql file

# check target table count for both catalog  target count match then hash validation



# source -> sd_dwh_prd
# target -> dwh_etl

# step -> i have to check if those tables exists or not 
# if exists then i have to chek the count of both tables 



# suppose we have 100 tables in sourece need to find out list of tables that have .isin("date", "created", "updated", "createdAt", "updatedAt") this col name if this column preent in table then daywise validation needed 

# if table not present this col then full row count validation needed 



# COMMAND ----------

# Daywise validation (for date col)
# -source count validation 
# -run sql file
# -target table validation
# -hash validation

# No datecol validation (full row count)
# -source count validation
# -run sql file
# -target table validation

# COMMAND ----------

folder_path = '/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/product_vertica/bank_promo_report'

# COMMAND ----------

import os
import re
import pandas as pd
from pyspark.sql.functions import collect_list, col

def get_schema_table_names_with_files(folder_path):
    source_pattern = re.compile(r'(?:FROM|JOIN)\s+(\w+)\.(\w+)', re.IGNORECASE)
    target_pattern = re.compile(r'(?:INTO|TABLE|UPDATE|MERGE INTO)\s+(\w+)\.(\w+)', re.IGNORECASE)

    source_table_names = []
    target_table_names = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()

                        source_matches = source_pattern.findall(content)
                        for match in source_matches:
                            schema, table = match
                            source_table_names.append((f"{schema}.{table}", file_path))

                        target_matches = target_pattern.findall(content)
                        for match in target_matches:
                            schema, table = match
                            target_table_names.append((f"{schema}.{table}", file_path))
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")

    return sorted(source_table_names), sorted(target_table_names)


#Load data and prepare DataFrames ---

#folder_path = '/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/BusinessExcellence_vertica'
source_tables, target_tables = get_schema_table_names_with_files(folder_path)

source_df = pd.DataFrame(source_tables, columns=["full_table_name", "file_path"])
target_df = pd.DataFrame(target_tables, columns=["full_table_name", "file_path"])

source_df[["schema_name", "table_name"]] = source_df["full_table_name"].str.split(".", expand=True) 
# ["full_table_name", "file_path","schema_name", "table_name"]
target_df[["schema_name", "table_name"]] = target_df["full_table_name"].str.split(".", expand=True) 
# ["full_table_name", "file_path","schema_name", "table_name"]

source_df = source_df[["schema_name", "table_name", "file_path"]]
target_df = target_df[["schema_name", "table_name", "file_path"]]

source_df_spark = spark.createDataFrame(source_df)
target_df_spark = spark.createDataFrame(target_df)
# print('count of source_df_spark')
# source_df_spark.count()
# Load Vertica master sheet and find date columns ---

pdf = pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/vertica_schema_data.csv")
csv_df_spark = spark.createDataFrame(pdf)

source_df_wdj = csv_df_spark.join(source_df_spark, on=['schema_name', 'table_name'], how='right')

casv = source_df_wdj

date_columns_df = casv.filter(
    (col("column_name").isin("date", "created", "updated", "createdAt", "updatedAt")) |
    (col("data_type").isin("date", "timestamp", "timestamp_ntz"))
).groupBy("schema_name", "table_name").agg(collect_list("column_name").alias("date_columns"))

all_tables_df = casv.select("schema_name", "table_name").distinct()
no_date_columns_df = all_tables_df.join(date_columns_df, on=["schema_name", "table_name"], how="anti")

print('no_date_columns_df:')
display(no_date_columns_df)


#  Filter source/target tables with no date columns

filtered_source_df = source_df_spark.join(no_date_columns_df, on=['schema_name', 'table_name'], how='inner')
filtered_target_df = target_df_spark.join(no_date_columns_df, on=['schema_name', 'table_name'], how='inner')
filtered_source_df.display()
filtered_target_df.display()
source_df_pd = filtered_source_df.toPandas()
target_df_pd = filtered_target_df.toPandas()

sql_file_groups = list(source_df_pd.groupby("file_path"))


def get_row_count(catalog, schema, table):
    try:
        df = spark.read.table(f"{catalog}.{schema}.{table}")
        return df.count()
    except:
        return None



# COMMAND ----------

# MAGIC %md
# MAGIC source validation

# COMMAND ----------

# Source Validation
source_file_groups = list(source_df_pd.groupby("file_path"))

source_report = []

for sql_file, group in source_file_groups:
    source_tables = group[["schema_name", "table_name"]]
    source_validation = []

    for _, row in source_tables.iterrows():
        schema, table = row["schema_name"], row["table_name"]
        count_src = get_row_count("sd_dwh_prd", schema, table)
        count_tgt = get_row_count("dwh_etl", schema, table)
        match = count_src == count_tgt and count_src is not None

        validation_row = {
            "sql_file": sql_file,
            "schema_name": schema,
            "table_name": table,
            "source_count": count_src,
            "target_count": count_tgt,
            "validation_status": "Source count match" if match else "Not Validated",
            "execution_status": "Passed" if match else "Failed"
        }

        source_validation.append(validation_row)

    source_report.extend(source_validation)

source_explain_status_df = pd.DataFrame([
    {
        "sql_file": row["sql_file"],
        "schema_name": row["schema_name"],
        "table_name": row["table_name"],
        "source_count": row["source_count"],
        "target_count": row["target_count"],
        "execution_status": row["execution_status"],
        "explain_status": row["validation_status"]
    }
    for row in source_report
])

print("Source Table Explain & Execution Status") 
spark.createDataFrame(source_explain_status_df).display()

# COMMAND ----------

# schema='analytics_BD'
# table='footwear_cat_j'
# get_row_count("sd_dwh_prd", schema, table)
# def get_row_count(catalog, schema, table):
#     try:
#         df = spark.read.table(f"{catalog}.{schema}.{table}")
#         return df.count()
#     except:
#         return None

# COMMAND ----------

# MAGIC %md
# MAGIC groupby based on file to check file  all table status is passed

# COMMAND ----------

from pyspark.sql import functions as F


summary_df = spark.createDataFrame(source_explain_status_df).groupBy("sql_file").agg(
    F.min(F.when(F.col("execution_status") == "Passed", 1).otherwise(0)).alias("all_passed_flag"),
    F.sum(F.when(F.col("execution_status") == "Passed", 1).otherwise(0)).alias("pass_count"),
    F.sum(F.when(F.col("execution_status") == "Failed", 1).otherwise(0)).alias("fail_count"),
    F.count("*").alias("total_tables")
)

summary_df = summary_df.withColumn(
    "all_passed",
    F.expr("all_passed_flag == 1")
).drop("all_passed_flag")

summary_df.display()



# COMMAND ----------

passed_files = summary_df.filter("all_passed = true").select("sql_file").distinct()
passed_files_list = [row.sql_file for row in passed_files.collect()]
print(passed_files_list)

# COMMAND ----------

len(passed_files_list)

# COMMAND ----------

# MAGIC %md
# MAGIC Run sql file

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
import os

results = []

catalog_name = "dwh_etl"  
spark.sql(f"USE CATALOG {catalog_name}")

# passed_files_list1 = [
#     '/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/cron_validation_VSA_26may/testing.sql'
# ]

for sql_file_path in passed_files_list:
    file_name = os.path.basename(sql_file_path)
    print(f"\nProcessing file: {sql_file_path}")
    
    with open(sql_file_path, "r") as f:
        sql_content = f.read()
        
    sql_statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
    
    # Run each query
    for idx, query in enumerate(sql_statements):
        print(f"Running query {idx + 1} from file {file_name}")
        try:
            query = """
                USE CATALOG dwh_etl;
                {query}
            """
            spark.sql(query)
            results.append((sql_file_path, query, "Success", None))
        except Exception as e:
            results.append((sql_file_path, query, "Failed", str(e)))

schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("query", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
])

error_df = spark.createDataFrame(results, schema=schema)
error_df.display()


# COMMAND ----------

error_df.display()

# COMMAND ----------

error_df.write.mode('append').saveAsTable("sd_digivate_stage_catalog.default.sql_query_cron_nodate_validation_errors_28_may")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.sql_query_cron_nodate_validation_errors_28_may

# COMMAND ----------

# MAGIC %md
# MAGIC Target tables validation

# COMMAND ----------

# Target Validation Only for target_df_pd

target_file_groups = list(target_df_pd.groupby("file_path"))
print(target_df_pd.count())
target_report = []
if target_df_pd is not None and not target_df_pd.empty:
    for sql_file, group in target_file_groups:
        target_tables = group[["schema_name", "table_name"]]
        target_validation = []

        for _, row in target_tables.iterrows():
            schema, table = row["schema_name"], row["table_name"]
            count_intermediate = get_row_count("dwh_etl", schema, table)         
            count_final = get_row_count("sd_dwh_prd", schema, table)             
            match = count_intermediate == count_final and count_intermediate is not None

            validation_row = {
                "sql_file": sql_file,
                "schema_name": schema,
                "table_name": table,
                "intermediate_count": count_intermediate,
                "final_count": count_final,
                "validation_status": "Target count match" if match else "Not Validated",
                "execution_status": "Passed" if match else "Failed"
            }

            target_validation.append(validation_row)

        target_report.extend(target_validation)

    target_explain_status_df = pd.DataFrame([
        {
            "sql_file": row["sql_file"],
            "schema_name": row["schema_name"],
            "table_name": row["table_name"],
            "intermediate_count": row["intermediate_count"],
            "final_count": row["final_count"],
            "execution_status": row["execution_status"],
            "explain_status": row["validation_status"]
        }
        for row in target_report
    ])

    print(" Target Table Explain & Execution Status")
    spark.createDataFrame(target_explain_status_df).display()
else:
    print("target count is zero")

# COMMAND ----------

# MAGIC %md
# MAGIC final status for taget table

# COMMAND ----------

#Target SQL File Summary
if target_explain_status_df  is not None and not target_explain_status_df.empty:
    target_file_summary = []

    for sql_file, group_df in pd.DataFrame(target_explain_status_df).groupby("sql_file"):
        file_passed = all(group_df["execution_status"] == "Passed")
        target_file_summary.append({
            "sql_file": sql_file,
            "status": "Passed" if file_passed else "Failed"
        })

    target_file_summary_df = pd.DataFrame(target_file_summary)

    pass_count = target_file_summary_df[target_file_summary_df["status"] == "Passed"].shape[0]
    fail_count = target_file_summary_df[target_file_summary_df["status"] == "Failed"].shape[0]

    print("\n=== Target SQL File Validation Summary ===")
    print(f" Passed SQL Files: {pass_count}")
    print(f" Failed SQL Files: {fail_count}")

    spark.createDataFrame(target_file_summary_df).display()

else:
    print('target_explain_status_df count zero ')

# COMMAND ----------

if target_explain_status_df  is not None and not target_explain_status_df.empty:

    target_file_summary_spark_df = spark.createDataFrame(target_file_summary_df)
    target_file_summary_spark_df.write.mode('append').saveAsTable("sd_digivate_stage_catalog.default.cron_nodate_validation_errors_28_may_filestatus")
else:
    print('target count is zero')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.cron_nodate_validation_errors_28_may_filestatus