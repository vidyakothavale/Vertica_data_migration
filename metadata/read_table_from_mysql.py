# Databricks notebook source
database_name="shipping_dwh"
tableName="shipping_delivery"

database_port = 3306  
hostname = dbutils.secrets.get(scope="sd-preprod-veritca", key=f"mysql_{database_name}_ip")  
username = dbutils.secrets.get(scope="sd-preprod-veritca", key=f"mysql_{database_name}_user")  
password = dbutils.secrets.get(scope="sd-preprod-veritca", key=f"mysql_{database_name}_pass")  

jdbc_url = f"jdbc:mysql://{hostname}:{database_port}/{database_name}"  

df = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("user", username) \
  .option("password", password) \
  .option("dbtable", f"(SELECT * FROM {tableName}) as temp_table") \
  .option("numPartitions", 8) \
  .load()