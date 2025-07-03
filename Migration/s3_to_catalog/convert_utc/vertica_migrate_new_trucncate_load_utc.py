# Databricks notebook source
external_table = dbutils.widgets.text('external_table', "", "External Table")
s3_location = dbutils.widgets.text('s3_location', "", "S3 Location")
original_table = dbutils.widgets.text('original_table', "", "Original Table")
final_table = dbutils.widgets.text('final_table', "", "Final Table")
original_schema = dbutils.widgets.text('original_schema', "", "Vertica Schema")

# COMMAND ----------

external_table = dbutils.widgets.get('external_table')
s3_location = dbutils.widgets.get('s3_location')
original_table = dbutils.widgets.get('original_table')
final_table = dbutils.widgets.get('final_table')
original_schema = dbutils.widgets.get('original_schema')


# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.broadcastTimeout", 1800)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800)
spark.conf.set("spark.sql.shuffle.spill.compress", True)
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
# spark.conf.set("spark.sql.parquet.compression.codec", "gzip")


# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1073741824)  
spark.conf.set("spark.databricks.delta.optimize.minFileSize", 134217728)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

# try:
spark.catalog.clearCache()
combined_df = spark.read \
    .format("parquet") \
    .option("inferSchema", "true") \
    .option("ignoreCorruptFiles", "true") \
    .option("ignoreMissingFiles", "true") \
    .option("ignoreEmptyFiles", "true") \
    .parquet(s3_location)
if combined_df is not None:
    view_name = f"temp_{external_table.split('.')[-1]}"
    combined_df.createOrReplaceTempView(view_name)
    print(f"Successfully created view {view_name}")
else:
    error_msg = "LOAD_FAILURE : Could not create combined dataframe"
    print(error_msg)
    dbutils.notebook.exit(error_msg)
# except Exception as exc:
#     error_msg = f"LOAD_FAILURE : {str(exc)}"
#     print(error_msg)
#     dbutils.notebook.exit(error_msg)


# COMMAND ----------

display(combined_df.limit(10))

# COMMAND ----------

# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType, IntegerType, LongType

# def identify_and_fix_date_columns(source_view, overwrite=False):

#     df=spark.table(f'{source_view}')

#     for column in df.columns:
#         dtype = df.schema[column].dataType
#         col_expr = None

#         if isinstance(dtype, StringType):
#             col_expr = F.when(
#                 F.col(column).rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
#                 F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")
#             ).when(
#                 F.length(F.col(column)) == 14,
#                 F.to_timestamp(F.col(column), "yyyyMMddHHmmss")
#             ).when(
#                 F.length(F.col(column)) == 8,
#                 F.to_timestamp(F.col(column), "yyyyMMdd")
#             )

#         elif isinstance(dtype, (IntegerType, LongType)):
#             col_expr = F.when(
#                 (F.length(F.col(column).cast("string")) == 10),
#                 F.from_unixtime(F.col(column).cast("long"))
#             ).when(
#                 (F.length(F.col(column).cast("string")) == 13),
#                 F.from_unixtime((F.col(column).cast("long") / 1000))
#             ).when(
#                 (F.col(column).cast(IntegerType()).between(0, 36500)),
#                 F.expr(f"date_add(DATE '1970-01-01', CAST({column} AS INT))")
#             ).when(
#                 F.length(F.col(column).cast("string")) == 8,
#                 F.to_date(F.col(column).cast("string"), "yyyyMMdd")
#             )

#         # Apply conversion if a valid transformation expression was found
#         if col_expr is not None:
#             df = df.withColumn(column, col_expr)

#     return df

# COMMAND ----------

spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')

# COMMAND ----------

import pandas as pd
vert_columns_df = ((spark.createDataFrame(pd.read_csv("/Workspace/Users/aarthy.ramachandran@digivatelabs.com/vertica_migration/vertica_schema_data.csv")))
         .select("schema_name","table_name","column_name","data_type")
         .withColumn("data_type", col("data_type").cast("string"))
)

table_name = original_table.split('.')[-1]
schema_name = original_table.split('.')[-2]

vert_columns_df2 = (vert_columns_df.filter("data_type = 'timestamp' or data_type = 'date'")
            .filter(f"lower(table_name) = lower('{table_name}') and lower(schema_name) = lower('{schema_name}')"))
date_timestamp_columns = vert_columns_df2.collect()
date_timestamp_columns_list = [i['column_name'] for i in date_timestamp_columns]
date_timestamp_columns_dict = {i['column_name']: i['data_type'] for i in date_timestamp_columns}

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

def identify_and_fix_date_columns(source_view,format = 'yyyy-MM-dd HH:mm:ss'):
    print('modifying date columns')
    timestamp_regex=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    time_regex = r"^\d{2}:\d{2}:\d{2}$"
    df=spark.table(f'{source_view}')
    for column in df.columns:
        if column in date_timestamp_columns_list:
            dtype=df.schema[column].dataType
            if isinstance(dtype, StringType):
                sample=df.select(column).filter(f'{column} is not null').limit(1).collect()
                print(sample)
                if sample:
                    value=sample[0][0]
                    if isinstance(value,str) and re.match(timestamp_regex,value):
                        print('converting string to timestamp')
                        df=df.withColumn(column, to_timestamp(col(column), format))
                    elif isinstance(value,str) and re.match(time_regex,value):
                        print('converting string to timestamp')
                        df=df.withColumn(column, to_timestamp(concat(lit("0000-00-00 "),col(column)), format))


            elif isinstance(dtype, (IntegerType,LongType)):
                sample=df.select(column).filter(f'{column} is not null').limit(1).collect()
                print(sample)
                if len(sample)==1:
                    value=sample[0][0]
                    print(value)
                    if isinstance(value,int):
                        digits=len(str(value))
                        print(digits)

                        if digits==10:
                            df=df.withColumn(column, from_unixtime(col(column).cast('timestamp')))

                        elif digits==13:
                            df = df.withColumn(column, from_unixtime((col(column) / 1000).cast('long')).cast('timestamp'))
                        
                        elif digits==8:
                            df=df.withColumn(column, to_date(col(column), 'yyyyMMdd'))

                        elif (value > -500000 and value < 500000) and date_timestamp_columns_dict[column] == 'date':
                            df=df.withColumn(column, expr(f"date_add(date'1970-01-01', {column})"))       

                elif len(sample)==0:
                    df=df.withColumn(column, lit(None).cast(date_timestamp_columns_dict[column]))
            
    return df
                        
    
    

# identify_and_fix_date_columns(view_name)



# COMMAND ----------

from pyspark.sql.functions import *

def convert_to_utc(df):
    print('converting to utc')
    for column in df.columns:
        dtype=df.schema[column].dataType
        print(dtype)
        if isinstance(dtype,TimestampType):
            print(f'converting {column} to utc')
            df = df.withColumn(column, to_utc_timestamp(col(column),"Asia/Kolkata"))
    return df

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 536870912) 
spark.conf.set("spark.databricks.delta.optimize.minFileSize", 67108864) 
spark.conf.set("spark.sql.shuffle.partitions", "200")  
spark.conf.set("spark.default.parallelism", "200") 

# COMMAND ----------

# def schema_match(df1,df2):
#     return df1.schema==df2.schema

# COMMAND ----------

source_count = spark.sql(f"SELECT COUNT(*) FROM {view_name}").collect()[0][0]
print("Overall Table Count : ", source_count)

def write_to_db(source_view):
    # transformed_view=identify_and_fix_date_columns(source_view)
    insert_data_query = f"""
        INSERT INTO {final_table}
        SELECT * FROM {source_view}
    """

    print(f"Appending data to table {final_table}")
    spark.sql(insert_data_query)


    
try:

    import gc
    gc.collect()

    final_catalog = final_table.split('.')[0]

    spark.sql(f"USE CATALOG {final_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {original_schema}")

    transformed_df= identify_and_fix_date_columns(view_name)
    # print(transformed_df.display())
    converted_df=convert_to_utc(transformed_df)
    # print(converted_df.display())
    transformed_view=f'{view_name}_transformed'
    converted_df.createOrReplaceTempView(transformed_view)



    # display(spark.sql(f'select * from {transformed_view}'))

    if spark.catalog.tableExists(f'{final_table}'):

        target_df=spark.table(f'{final_table}')
        

        source_schema = [(f.name.lower(), str(f.dataType).lower()) for f in converted_df.schema]
        target_schema = [(f.name.lower(), str(f.dataType).lower()) for f in target_df.schema]

    # Check column count mismatch
        if len(source_schema) != len(target_schema):
            raise Exception(f"Column count mismatch: source={len(source_schema)}, target={len(target_schema)}")

        mismatches = []
        for idx, ((src_col, src_type), (tgt_col, tgt_type)) in enumerate(zip(source_schema, target_schema), start=1):
            if src_col != tgt_col or src_type != tgt_type:
                mismatches.append(
                    f"Column {idx}: source `{src_col}` ({src_type}) ≠ target `{tgt_col}` ({tgt_type})"
                )

        if mismatches:
            mismatch_msg = "\n".join(mismatches)
            raise Exception(f"Schema mismatch found:\n{mismatch_msg}")
          
            
        print(f'truncating {final_table}')
        spark.sql(f'truncate table {final_table}')
        insert_data_query = f"""
            INSERT INTO {final_table}
            SELECT * FROM {transformed_view}
        """

        print(f"Appending data to table {final_table}")
        spark.sql(insert_data_query)

    else:
        create_table = f"""
            CREATE OR REPLACE TABLE {final_table} 
            USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.tuneFileSizesForRewrites' = 'true'
            )
            AS 
            SELECT * FROM {transformed_view} WHERE 1=0
        """

        print(f"Creating empty table {final_table}")
        spark.sql(create_table)

        combined_df.printSchema()

        write_to_db(transformed_view)

    print(f"Successfully created table {final_table}")
except Exception as exc:
    error_msg = f"COPY_FAILURE : {str(exc)}"
    print(f"Error details: {error_msg}")
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# from pyspark.sql.functions import *
# from pyspark.sql.types import DateType, IntegerType, TimestampType

# source_count = spark.sql(f"SELECT COUNT(*) FROM {view_name}").collect()[0][0]
# print("Overall Table Count : ", source_count)

# # def identify_and_fix_date_columns(source_view,format = 'yyyy-MM-dd HH:mm:ss'):
    
# #     try:
# #         timestamp_regex=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
# #         df=spark.table(f'{source_view}')
# #         for column in df.columns:
# #             if isinstance(df.schema[column].dataType, StringType):
# #                 sample=df.select(column).limit(1).collect()
# #                 if sample:
# #                     value=sample[0][0]
# #                     if isinstance(value,str) and re.match(timestamp_regex,value):
# #                         df=df.withColumn(column, to_timestamp(col(column), format))
# #         return df
# #     except Exception as e:
# #         print(str(e))
    
    


# # def identify_and_fix_date_columns(source_view, target_table):
# #     source_schema = spark.sql(f"DESCRIBE {source_view}").collect()
    
# #     # target_exists = False
# #     # try:
# #     #     target_schema = spark.sql(f"DESCRIBE {target_table}").collect()
# #     #     target_exists = True
# #     # except:
# #     #     target_schema = []

# #     source_columns = {row['col_name']: row['data_type'] for row in source_schema if row['col_name'] != ''}
# #     print(f'source col {source_columns}')
    
# #     # target_columns = {row['col_name']: row['data_type'] for row in target_schema if row['col_name'] != ''} if target_exists else {}
    
    
# #     transformations = {}
    
# #     date_column_keywords = ['date', 'dt', 'day']
# #     timestamp_column_keywords = ['time', 'timestamp']
    
# #     # if target_exists:
# #     #     for col_name, source_type in source_columns.items():
# #     #         if col_name in target_columns:
# #     #             target_type = target_columns[col_name]
                
# #     #             if 'int' in source_type.lower() and 'date' in target_type.lower():
# #     #                 transformations[col_name] = 'date_from_unix_date'
                
# #     #             elif 'int' in source_type.lower() and ('timestamp' in target_type.lower() or 'datetime' in target_type.lower()):
# #     #                 transformations[col_name] = 'to_timestamp'
# #     # else:
# #     for col_name, source_type in source_columns.items():
# #         col_lower = col_name.lower()
        
# #         if 'int' in source_type.lower():
# #             if any(keyword in col_lower for keyword in date_column_keywords):
# #                 transformations[col_name] = 'date_from_unix_date'
# #             elif any(keyword in col_lower for keyword in timestamp_column_keywords):
# #                 transformations[col_name] = 'to_timestamp'
    
# #     if transformations:
# #         print(f"Identified {len(transformations)} columns that need type conversion:")
# #         for col_name, transform_type in transformations.items():
# #             print(f"  - {col_name}: {source_columns.get(col_name, 'unknown')} -> {transform_type}")
# #     else:
# #         print("No date/timestamp columns identified for transformation")
# #     print(f'transformations:{transformations}')
# #     transformed_view_name = f"{source_view}_transformed"
    
# #     select_columns = []
# #     for col_name in source_columns.keys():
# #         if col_name in transformations:
# #             transform_type = transformations[col_name]
# #             if transform_type == 'date_from_unix_date':
# #                 select_columns.append(f"DATE_FROM_UNIX_DATE({col_name}) AS {col_name}")
# #             elif transform_type == 'to_timestamp':
# #                 select_columns.append(f"TO_TIMESTAMP(try_cast({col_name} AS BIGINT)) AS {col_name}")
# #         else:
# #             select_columns.append(col_name)
    
# #     transform_query = f"""
# #         CREATE OR REPLACE TEMPORARY VIEW {transformed_view_name} AS
# #         SELECT 
# #             {', '.join(select_columns)}
# #         FROM {source_view}
# #     """
    
# #     print(f"Creating transformed view with proper date conversions")
# #     spark.sql(transform_query)
    
# #     return transformed_view_name, transformations

# def write_to_db(source_view,target_table):
#     # transformed_view=identify_and_fix_date_columns(source_view)
#     insert_data_query = f"""
#         INSERT INTO {target_table}
#         SELECT * FROM {source_view}
#     """

#     print(f"Appending data to table {target_table}")
#     spark.sql(insert_data_query)

# try:
#     spark.conf.set("spark.sql.adaptive.enabled", "true")
#     spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
#     spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
#     spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 536870912) 
#     spark.conf.set("spark.databricks.delta.optimize.minFileSize", 67108864) 
#     spark.conf.set("spark.sql.shuffle.partitions", "200")  
#     spark.conf.set("spark.default.parallelism", "200") 
    
#     import gc
#     gc.collect()

#     final_catalog = final_table.split('.')[0]

#     spark.sql(f"USE CATALOG {final_catalog}")
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {original_schema}")
    
#     # table_exists = False
#     # try:
#     #     spark.sql(f"DESCRIBE TABLE {final_table}")
#     #     table_exists = True
#     # except:
#     #     table_exists = False
    
#     # if not table_exists:
#     transformed_df= identify_and_fix_date_columns(view_name)
#     transformed_view=f'{view_name}_transformed'
#     transformed_df.createOrReplaceTempView(transformed_view)
    
#     create_table = f"""
#         CREATE OR REPLACE TABLE {final_table} 
#         USING DELTA
#         TBLPROPERTIES (
#             'delta.autoOptimize.optimizeWrite' = 'true',
#             'delta.autoOptimize.autoCompact' = 'true',
#             'delta.tuneFileSizesForRewrites' = 'true'
#         )
#         AS 
#         SELECT * FROM {transformed_view} WHERE 1=0
#     """
    
#     print(f"Creating empty table {final_table}")
#     spark.sql(create_table)

#     combined_df.printSchema()

#     write_to_db(transformed_view, final_table)
    
#     print(f"Successfully created table {final_table}")
# except Exception as exc:
#     error_msg = f"COPY_FAILURE : {str(exc)}"
#     print(f"Error details: {error_msg}")
#     dbutils.notebook.exit(error_msg)

# COMMAND ----------

from datetime import datetime

def update_audit_log(original_table, status, rows_processed=0, error_message=None):

    AUDIT_CATALOG = "sd_digivate_stage_catalog"
    AUDIT_SCHEMA = "vertica_migration"
    AUDIT_LOG_TABLE = "vertica_migration_s3_to_uc_prod_log_apr29"

    end_time=datetime.now()

    spark.sql(f"""
        UPDATE {AUDIT_CATALOG}.{AUDIT_SCHEMA}.{AUDIT_LOG_TABLE}
        SET end_time = "{end_time}", status = "{status}", rows_processed = "{rows_processed}", error_message = "{error_message}"
        WHERE table_name = "{original_table}"
    """)

update_audit_log(original_table, "SUCCESS", 0, None)

rows_processed = spark.sql(f"SELECT COUNT(*) FROM {final_table}").collect()[0][0]

print(f"Rows Processed: {rows_processed}")
print(f"Source Count: {source_count}")

if rows_processed == source_count:
    update_audit_log(original_table, "VALIDATION_SUCCESS", rows_processed, None)
else:
    update_audit_log(original_table, "VALIDATION_FAILED", rows_processed, f"Source Count: {source_count}, Target Count: {rows_processed}")
    
# Clean up resources
try:
    spark.sql(f"DROP VIEW IF EXISTS {view_name}")
    if combined_df:
        combined_df.unpersist()
    gc.collect()
except Exception as e:
    print(f"Error during cleanup: {str(e)}")



# COMMAND ----------

dbutils.notebook.exit("SUCCESS")