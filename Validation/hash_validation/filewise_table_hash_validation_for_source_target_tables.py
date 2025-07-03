# Databricks notebook source
from pyspark.sql.utils import AnalysisException
import os
import re 
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col, when, count, sum
from pyspark.sql import Row

# COMMAND ----------

result = []

# COMMAND ----------

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
    
#folder_path = f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/'
#folder_path = f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/BusinessExcellence_vertica'
#folder_path = f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/customer_support_vertica'
#folder_path = f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/cxo_vertica'
# folder_path=f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/finance_vertica'
#folder_path=f'/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/logistics_vertica'
folder_path='/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/BusinessExcellence_vertica'
results = get_schema_table_names(folder_path)

# COMMAND ----------

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

sorted_file_paths = sorted(file_paths)
first_100 = sorted_file_paths#[50:100]
#first_100=sorted_file_paths

# COMMAND ----------

failed_sql_files = []
sql_file_ready_for_run = []
master_csv_read = pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/vertica_schema_data.csv")
csv_df_spark = spark.createDataFrame(master_csv_read) #master sheet

# COMMAND ----------


def get_row_count(catalog, schema, table, date_col , file):
    START_DATE_FOR_VALIDATION = "2025-04-30"
    uc_table = f"{catalog}.{schema}.{table}"
    #print(date_col)
    try:
       
        df = spark.sql(f"""
            SELECT *
            FROM {uc_table}
            WHERE {date_col} IS NOT NULL
              AND to_date({date_col}) >= to_date('{START_DATE_FOR_VALIDATION}')
        """)

        hash_df = df.withColumn("row_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in df.columns]), 256))
        #return row_count, hash_df
        return hash_df

    except AnalysisException as e:
        schema = StructType([
        StructField("file_path", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("error", StringType(), True)
        ])
        writing = {
            "file_path":file,
            "table_name":uc_table,
            "error": f"{str(e)[:200]}"
        }
        df  = spark.createDataFrame(writing, schema)
        df.write.format("delta").mode("append").saveAsTable("sd_digivate_stage_catalog.default.hash_validation_table_not_found_2_june_2025_4june")
        print(f"Table not found : {uc_table} | Error: {str(e)[:200]}")
        return None

    except Exception as e:
        print(f"Error processing table {uc_table}: {str(e)[:200]}")
        return None

# COMMAND ----------


from pyspark.sql.types import *
def get_row_count_no_date(catalog, schema, table ,file):
    uc_table = f"{catalog}.{schema}.{table}"
    try:
        df = spark.sql(f"""
            SELECT *
            FROM {uc_table}
        """)
        hash_df = df.withColumn("row_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in df.columns]), 256))
        
        #return row_count, hash_df
        return hash_df

    except AnalysisException as e:
        schema = StructType([
        StructField("file_path", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("error", StringType(), True)
        ])
        writing = {
            "file_path":file,
            "table_name":uc_table,
            "error": f"{str(e)[:200]}"
        }
        df = spark.crdf = spark.createDataFrame(writing, schema)
        df.write.format("delta").mode("append").saveAsTable("sd_digivate_stage_catalog.default.hash_validation_table_not_found_2_june_2025_4june")
        print(f"Table not found : {uc_table} | Error: {str(e)[:200]}")
        return None

    except Exception as e:
        print(f"Error processing table {uc_table}: {str(e)[:200]}")
        return None


# COMMAND ----------


failed_sql_filesd = []
success_tables=[]


# COMMAND ----------

# MAGIC %md
# MAGIC ##source table validation

# COMMAND ----------


def file_run(file):
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
            #print("spark_df")        
            # spark_df.display()

            source_df_wdj=csv_df_spark.join(spark_df, on=['schema_name','table_name'], how='right') 
            #print("source_df_wdj")  
            # source_df_wdj.display()

            date_columns_df = source_df_wdj.filter((col("column_name").isin("date", "created", "updated", "createdAt", "updatedAt") | (col("data_type") == "date") | (col("data_type") == "timestamp") | (col("data_type") == "timestamp_ntz") )) \
                .groupBy("schema_name", "table_name","file_path") \
                .agg(collect_list("column_name").alias("date_columns"))  # schema_name , table_name , file_path , list(datacolumns)
            # print("date_columns_df")    
            # display(date_columns_df)
            source_report = []
            if date_columns_df.count() == 0:
                temp_dict = {"file_path": file ,"error": "No date columns found in this sql file"}
                failed_sql_files.append(temp_dict)
            else:
                
                for row in date_columns_df.collect():
                    schema = row['schema_name']
                    table = row['table_name']
                    file_path = row['file_path']
                    date_col = row['date_columns'][0]
                    print(date_col)
                    s_df = get_row_count("sd_dwh_prd", schema, table, date_col , file)
                    t_df = get_row_count("dwh_etl", schema, table, date_col , file)
                    s_df= s_df.distinct()
                    t_df= t_df.distinct()
                    if s_df is not None and t_df is not None:
                        print("s_df")
                        s_df.display()
                        s_df_count = s_df.count()
                        t_df_count = t_df.count()
                        com_df = s_df.join(t_df, on='row_hash', how='inner')
                        com_df_count = com_df.count()
                        
                        if com_df_count == s_df_count and com_df_count == t_df_count:
                            validation_status = "Match"
                            print(validation_status)
                            success_tables.append(f"{schema}.{table}")
                        else:
                            validation_status = "Not Match"
                            print(validation_status)

                        
                        validation_row = {
                                "sql_file": file,
                                "schema_name": schema,
                                "table_name": table,
                                "source_hash_count": s_df_count,
                                "target_hash_count": t_df_count,
                                "validation_status": validation_status
                            }
                        source_report.append(validation_row) 

                    else:

                        if s_df is None and t_df is None:
                            temp_dict = {"file_path": file ,"error": "source and target DataFrames are empty"}
                            failed_sql_files.append(temp_dict)

                        elif s_df is None:
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_files.append(temp_dict)

                            print('empty df')

                        else: # This else is incorrectly indented and should be aligned with the previous if statement.
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_filesd.append(temp_dict)

#################################################date is completed #############################
            all_tables_df = source_df_wdj.select("schema_name", "table_name").distinct()
            no_date_columns_df = all_tables_df.join(date_columns_df, on=["schema_name", "table_name"], how="anti")            
            filtered_source_df = source_df_spark.join(no_date_columns_df, on=['schema_name', 'table_name'], how='inner')
            #print(f"filtered_source_df count")                      

            if filtered_source_df.count() > 0:
                source_df_pd = filtered_source_df.collect()

                # schema = source_df_pd['schema_name']
                # table_name = source_df_pd['table_name']
                # source_report = []
                for row in source_df_pd:                         
                    print(f"printing scheam_name -> {row['schema_name']}")
                    print(f"printing table name -> {row['table_name']}")
                    schema = row['schema_name']
                    table = row['table_name']
                    s_df = get_row_count_no_date("sd_dwh_prd", row['schema_name'],row['table_name'],file)
                    t_df = get_row_count_no_date("dwh_etl", row['schema_name'],row['table_name'],file)
                    s_df= s_df.distinct()
                    t_df= t_df.distinct()
                    if s_df is not None and t_df is not None:
                        # print("s_df")
                        # s_df.display()
                        s_df_count = s_df.count()
                        t_df_count = t_df.count()
                        com_df = s_df.join(t_df, on='row_hash', how='inner')
                        com_df_count = com_df.count()
                        
                        if com_df_count == s_df_count and com_df_count == t_df_count:
                            validation_status = "Match"
                            print(validation_status)
                            success_tables.append(f"{schema}.{table}")
                        else:
                            validation_status = "Not Match"
                            print(validation_status)

                        
                        validation_row = {
                                "sql_file": file,
                                "schema_name": schema,
                                "table_name": table,
                                "source_hash_count": s_df_count,
                                "target_hash_count": t_df_count,
                                "validation_status": validation_status
                            }
                        source_report.append(validation_row)
                    else:

                        if s_df is None and t_df is None:
                            temp_dict = {"file_path": file ,"error": "source and target DataFrames are empty"}
                            failed_sql_files.append(temp_dict)

                            
                        elif s_df is None:
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_files.append(temp_dict)

                            print('empty df')

                        else: # This else is incorrectly indented and should be aligned with the previous if statement.
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_filesd.append(temp_dict)


        source_explain_status_df = pd.DataFrame(source_report , columns = ["sql_file",
                                "schema_name",
                                "table_name",
                                "source_hash_count",
                                "target_hash_count",
                                "validation_status"])
        
        
        final_df = spark.createDataFrame(source_explain_status_df)
        print("writing to table ")
        final_df.write.format("delta").mode("append").saveAsTable("sd_digivate_stage_catalog.default.hash_validation_table_not_found_2_june_2025_4june")

###############################################Non date complete ###################################################
        # summary_df = spark.createDataFrame(source_explain_status_df).groupBy("sql_file").agg(
        #             F.min(F.when(F.col("validation_status") == "Match", 1).otherwise(0)).alias("all_passed_flag"),
        #             F.sum(F.when(F.col("validation_status") == "Match", 1).otherwise(0)).alias("pass_count"),
        #             F.sum(F.when(F.col("validation_status") == "Not Match", 1).otherwise(0)).alias("fail_count"),
        #             F.count("*").alias("total_tables")
        #         )
        # summary_df = summary_df.withColumn(
        #             "all_passed",
        #             F.expr("all_passed_flag == 1")
        #         ).drop("all_passed_flag")
        # passed_files = summary_df.filter("all_passed = true").select("sql_file").distinct()
        # passed_files_list = [row.sql_file for row in passed_files.collect()]
        # print(f"passed_files_list : {len(passed_files_list)}")
                            


# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

with ThreadPoolExecutor(max_workers=6) as executor:
    futures = [executor.submit(file_run, file) for file in first_100]
    print(futures)                     
    print("*" * 100)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sd_digivate_stage_catalog.default.hash_validation_table_not_found_2_june_2025_v2

# COMMAND ----------

from pyspark.sql import functions as F
summary_df = spark.table("sd_digivate_stage_catalog.default.hash_validation_table_not_found_2_june_2025_v2").groupBy("sql_file").agg(
                    F.min(F.when(F.col("validation_status") == "Match", 1).otherwise(0)).alias("all_passed_flag"),
                    F.sum(F.when(F.col("validation_status") == "Match", 1).otherwise(0)).alias("pass_count"),
                    F.sum(F.when(F.col("validation_status") == "Not Match", 1).otherwise(0)).alias("fail_count"),
                    F.count("*").alias("total_tables")
                )
summary_df = summary_df.withColumn(
            "all_passed",
            F.expr("all_passed_flag == 1")
        ).drop("all_passed_flag")

passed_files = summary_df.filter("all_passed = true").select("sql_file").distinct()
passed_files_list = [row.sql_file for row in passed_files.collect()]
print(f"passed_files_list : {len(passed_files_list)}")

# COMMAND ----------

fail_files = summary_df.filter("all_passed = false").select("sql_file").distinct()
display(fail_files) 

# COMMAND ----------

display(passed_files_list)

# COMMAND ----------

df = spark.table("sd_digivate_stage_catalog.default.hash_validation_table_not_found_2_june_2025_v2")
result = df.groupBy("sql_file").agg(
    count("*").alias("total_count"),
    sum(when(col("validation_status") != "Match", 1).otherwise(0)).alias("not_match_count")
).withColumn(
    "file_status",
    when(col("not_match_count") == 0, "Pass").otherwise("Fail")
)
result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run sql file for- success files in source validation

# COMMAND ----------

results = []
catalog_name = "dwh_etl"  
spark.sql(f"USE CATALOG {catalog_name}")

# passed_files_list1 = [
#     '/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/cron_validation_VSA_26may/testing.sql'
# ]
#passed_files_list
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

error_df.write.mode('append').saveAsTable("sd_digivate_stage_catalog.default.sql_query_cron_nodate_validation_errors_2_june")

# COMMAND ----------

# MAGIC %md
# MAGIC ####target validation for source succes files:

# COMMAND ----------

##
def file_run(file):
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
        
        filtered_df = dataFrme.filter(size(dataFrme.target_table_names) > 0)
        
        if filtered_df.count() == 0:
            temp_dict = {"file_path": file ,"error": "No source table found in this sql file"}
            failed_sql_files.append(temp_dict)
        else:    
            # Explode source_table_names and split it into schema_name and table_name
            source_df_spark = (filtered_df.select("file_path", explode("target_table_names").alias("target_table_name"))
            .withColumn("schema_name", split("target_table_name", "\.").getItem(0)) 
            .withColumn("table_name", split("target_table_name", "\.").getItem(1)))
            # print("source_df_spark")
            # display(source_df_spark)

            spark_df = source_df_spark.select("file_path","schema_name","table_name")
            print("spark_df")        
            # spark_df.display()

            source_df_wdj=csv_df_spark.join(spark_df, on=['schema_name','table_name'], how='right') 
            print("source_df_wdj")  
            # source_df_wdj.display()

            date_columns_df = source_df_wdj.filter((col("column_name").isin("date", "created", "updated", "createdAt", "updatedAt") | (col("data_type") == "date") | (col("data_type") == "timestamp") | (col("data_type") == "timestamp_ntz") )) \
                .groupBy("schema_name", "table_name","file_path") \
                .agg(collect_list("column_name").alias("date_columns"))  # schema_name , table_name , file_path , list(datacolumns)
            # print("date_columns_df")    
            # display(date_columns_df)
            source_report = []
            if date_columns_df.count() == 0:
                temp_dict = {"file_path": file ,"error": "No date columns found in this sql file"}
                failed_sql_files.append(temp_dict)
            else:
                
                for row in date_columns_df.collect():
                    schema = row['schema_name']
                    table = row['table_name']
                    file_path = row['file_path']
                    date_col = row['date_columns'][0]
                    print(date_col)
                    s_df = get_row_count("sd_dwh_prd", schema, table, date_col , file)
                    t_df = get_row_count("dwh_etl", schema, table, date_col , file)
                    s_df= s_df.distinct()
                    t_df= t_df.distinct()
                    if s_df is not None and t_df is not None:
                        print("s_df")
                        s_df.display()
                        s_df_count = s_df.count()
                        t_df_count = t_df.count()
                        com_df = s_df.join(t_df, on='row_hash', how='inner')
                        com_df_count = com_df.count()
                        
                        if com_df_count == s_df_count and com_df_count == t_df_count:
                            validation_status = "Match"
                            print(validation_status)
                            success_tables.append(f"{schema}.{table}")
                        else:
                            validation_status = "Not Match"
                            print(validation_status)

                        
                        validation_row = {
                                "sql_file": file,
                                "schema_name": schema,
                                "table_name": table,
                                "source_hash_count": s_df_count,
                                "target_hash_count": t_df_count,
                                "validation_status": validation_status
                            }
                        source_report.append(validation_row) 

                    else:

                        if s_df is None and t_df is None:
                            temp_dict = {"file_path": file ,"error": "source and target DataFrames are empty"}
                            failed_sql_files.append(temp_dict)

                            
                        elif s_df is None:
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_files.append(temp_dict)

                            print('empty df')

                        else: # This else is incorrectly indented and should be aligned with the previous if statement.
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_filesd.append(temp_dict)

     
#################################################date is completed #############################
            all_tables_df = source_df_wdj.select("schema_name", "table_name").distinct()
            no_date_columns_df = all_tables_df.join(date_columns_df, on=["schema_name", "table_name"], how="anti")            
            filtered_source_df = source_df_spark.join(no_date_columns_df, on=['schema_name', 'table_name'], how='inner')
            print(f"filtered_source_df count")                      

            if filtered_source_df.count() > 0:

                source_df_pd = filtered_source_df.collect()

                # schema = source_df_pd['schema_name']
                # table_name = source_df_pd['table_name']
                # source_report = []
                for row in source_df_pd:                         
                    print(f"printing scheam_name -> {row['schema_name']}")
                    print(f"printing table name -> {row['table_name']}")
                    schema = row['schema_name']
                    table = row['table_name']
                    s_df = get_row_count_no_date("sd_dwh_prd", row['schema_name'],row['table_name'],file)
                    t_df = get_row_count_no_date("dwh_etl", row['schema_name'],row['table_name'],file)
                    s_df= s_df.distinct()
                    t_df= t_df.distinct()
                    if s_df is not None and t_df is not None:
                        # print("s_df")
                        # s_df.display()
                        s_df_count = s_df.count()
                        t_df_count = t_df.count()
                        com_df = s_df.join(t_df, on='row_hash', how='inner')
                        com_df_count = com_df.count()
                        
                        if com_df_count == s_df_count and com_df_count == t_df_count:
                            validation_status = "Match"
                            print(validation_status)
                            success_tables.append(f"{schema}.{table}")
                        else:
                            validation_status = "Not Match"
                            print(validation_status)

                        
                        validation_row = {
                                "sql_file": file,
                                "schema_name": schema,
                                "table_name": table,
                                "source_hash_count": s_df_count,
                                "target_hash_count": t_df_count,
                                "validation_status": validation_status
                            }
                        source_report.append(validation_row)
                    else:

                        if s_df is None and t_df is None:
                            temp_dict = {"file_path": file ,"error": "source and target DataFrames are empty"}
                            failed_sql_files.append(temp_dict)

                            
                        elif s_df is None:
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_files.append(temp_dict)

                            print('empty df')

                        else: # This else is incorrectly indented and should be aligned with the previous if statement.
                            temp_dict = {"file_path": file ,"error": "source DataFrame is empty"}
                            failed_sql_filesd.append(temp_dict)


        source_explain_status_df = pd.DataFrame(source_report , columns = ["sql_file",
                                "schema_name",
                                "table_name",
                                "source_hash_count",
                                "target_hash_count",
                                "validation_status"])
        
        
        final_df = spark.createDataFrame(source_explain_status_df)
        final_df.write.format("delta").mode("append").saveAsTable("sd_digivate_stage_catalog.default.hash_validation_report_30_may_2025_v3")
