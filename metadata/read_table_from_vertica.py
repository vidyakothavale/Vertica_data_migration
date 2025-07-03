# Databricks notebook source
hostname = "vertica.ops.snapdeal.io"
database_port = 5433
database_name = "snapdealdwh"
username = dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_user")
password = dbutils.secrets.get(scope="sd-preprod-veritca", key="vertica_password")
table_name = "stage_temp.qna_post_cycle"

schema, tbl = table_name.split('.')
col_query = f"""(SELECT column_name, data_type FROM v_catalog.columns 
                 WHERE table_schema = '{schema}' AND table_name = '{tbl}') AS col_info"""

col_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:vertica://{hostname}:{database_port}/{database_name}") \
    .option("dbtable", col_query) \
    .option("user", username).option("password", password) \
    .option("driver", "com.vertica.jdbc.Driver").load()

select_expr = []
for row in col_df.collect():
    col, dtype = row["column_name"], row["data_type"].lower()
    if "decimal" in dtype or "numeric" in dtype:
        select_expr.append(f'CAST("{col}" AS NUMERIC(38,18)) AS "{col}"')
    elif "interval" in dtype:
        select_expr.append(f'CAST("{col}" AS VARCHAR) AS "{col}"')
    else:
        select_expr.append(f'"{col}"')

query = f'SELECT {", ".join(select_expr)} FROM {table_name}'

final_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:vertica://{hostname}:{database_port}/{database_name}") \
    .option("query", query) \
    .option("user", username).option("password", password) \
    .option("driver", "com.vertica.jdbc.Driver").load()

final_df.display()

 