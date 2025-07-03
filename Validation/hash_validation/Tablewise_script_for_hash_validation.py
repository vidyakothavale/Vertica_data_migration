# Databricks notebook source
folder_path='/Workspace/Users/sivanesan.govindaraj@digivatelabs.com/Cron_reports/Cron_report_SQL/BusinessExcellence_vertica'

# COMMAND ----------

# MAGIC %md
# MAGIC find source and target table list

# COMMAND ----------

import os
import re

def get_schema_table_names(folder_path):
    source_pattern = re.compile(r'(?:FROM|JOIN)\s+(\w+)\.(\w+)', re.IGNORECASE)
    target_pattern = re.compile(r'(?:INTO|TABLE|UPDATE|MERGE INTO)\s+(\w+)\.(\w+)', re.IGNORECASE)
    
    #schema_table_names = set()
    target_table_names = set()
    source_table_names = set()
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                        
                        source_matches = source_pattern.findall(content)
                        for match in source_matches:
                            source_table_names.add(f"{match[0]}.{match[1]}")
                        
                        target_matches = target_pattern.findall(content)
                        for match in target_matches:
                            target_table_names.add(f"{match[0]}.{match[1]}") #target_table_names target_table_list
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")

    return sorted(list(source_table_names)),sorted(list(target_table_names))

#folder_path = dbutils.widgets.get("folder_path")
source_table_names,target_table_names = get_schema_table_names(folder_path)

for i in source_table_names:
    print(f"{i}")

# for i in target_table_names:
#     print(f"{i}")

# COMMAND ----------

for table_name in source_table_names:
    query = f"DESCRIBE sd_dwh_prd.{table_name}"
    schema_df = spark.sql(query)
    date_columns = [row['col_name'] for row in schema_df.collect() if 'date' in row['data_type'].lower()]
    for date_column in date_columns:
        query = f"""
        SELECT MIN({date_column}) as min_date, MAX({date_column}) as max_date FROM sd_dwh_prd.{table_name}
        """
        sd_dates = spark.sql(query).collect()[0]
        query = f"""
        SELECT MIN({date_column}) as min_date, MAX({date_column}) as max_date FROM dwh_etl.{table_name}
        """
        dwh_dates = spark.sql(query).collect()[0]
        print(f"{table_name} - {date_column}: sd_dwh_prd({sd_dates['min_date']} to {sd_dates['max_date']}) vs dwh_etl({dwh_dates['min_date']} to {dwh_dates['max_date']})")

# COMMAND ----------

# MAGIC %md
# MAGIC hash validation

# COMMAND ----------

from pyspark.sql.functions import col, sha2, concat_ws, lit
from datetime import datetime, timedelta

def compare_table_data(source_table_names):
    mismatch_counts = []

    for table_name in source_table_names:
        query = f"DESCRIBE sd_dwh_prd.{table_name}"
        schema_df = spark.sql(query)
        date_columns = [row['col_name'] for row in schema_df.collect() if 'date' in row['data_type'].lower()]

        if date_columns:
            date_column = date_columns[0]  
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            query = f"""
            SELECT *, sha2(concat_ws(',', *) , 256) as hash_value FROM sd_dwh_prd.{table_name}
            WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'
            """
            sd_df = spark.sql(query)

            query = f"""
            SELECT *, sha2(concat_ws(',', *) , 256) as hash_value FROM dwh_etl.{table_name}
            WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'
            """
            dwh_df = spark.sql(query)
        else:
            query = f"""
            SELECT *, sha2(concat_ws(',', *) , 256) as hash_value FROM sd_dwh_prd.{table_name}
            """
            sd_df = spark.sql(query)

            query = f"""
            SELECT *, sha2(concat_ws(',', *) , 256) as hash_value FROM dwh_etl.{table_name}
            """
            dwh_df = spark.sql(query)

        sd_hashes = sd_df.select("hash_value")
        dwh_hashes = dwh_df.select("hash_value")

        mismatched_count = sd_hashes.subtract(dwh_hashes).count() + dwh_hashes.subtract(sd_hashes).count()
        mismatch_counts.append((table_name, mismatched_count))

    for table_name, count in mismatch_counts:
        print(f"{table_name}: {count} mismatched rows")
compare_table_data(source_table_names)