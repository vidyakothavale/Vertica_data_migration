# Databricks notebook source
# Connection details for Vertica
hostname = "vertica.ops.snapdeal.io"
username = dbutils.secrets.get(scope = "sd-preprod-veritca", key = "vertica_user") 
password = dbutils.secrets.get(scope = "sd-preprod-veritca", key = "vertica_password") 
database_port = 5433
database_name = "snapdealdwh"

# 1. Consolidated Table-Level Metadata
qry_table_level=f"""SELECT 
    t.table_schema,
    t.table_name,
    t.is_temp_table,
    t.is_flextable,
    COALESCE(v.view_definition, '') AS view_definition,
    COALESCE(ps.size_mb, 0) AS size_mb,
    COALESCE(ps.size_gb, 0) AS size_gb,
    COALESCE(ps.row_count, 0) AS row_count,
    COALESCE(p.is_segmented, false) AS is_segmented,
    COALESCE(p.segment_expression, '') AS segment_expression,
    t.owner_name,
    CASE WHEN COUNT(pk.constraint_name) > 0 THEN true ELSE false END AS has_primary_key,
    CASE WHEN COUNT(fk.constraint_name) > 0 THEN true ELSE false END AS has_foreign_keys
FROM 
    v_catalog.tables t
LEFT JOIN 
    v_catalog.views v ON t.table_id = v.table_id
LEFT JOIN (
    SELECT 
        ps.anchor_table_schema,
        ps.anchor_table_name,
        SUM(ps.used_bytes) / (1024*1024) AS size_mb,
        SUM(ps.used_bytes) / (1024*1024*1024) AS size_gb,
        SUM(ps.row_count) AS row_count
    FROM 
        v_monitor.projection_storage ps
    GROUP BY 
        ps.anchor_table_schema, ps.anchor_table_name
) ps ON t.table_schema = ps.anchor_table_schema AND t.table_name = ps.anchor_table_name
LEFT JOIN (
    SELECT DISTINCT (anchor_table_id)
        anchor_table_id,
        is_segmented,
        segment_expression
    FROM 
        v_catalog.projections
    ORDER BY 
        anchor_table_id DESC
) p ON t.table_id = p.anchor_table_id
LEFT JOIN (
    SELECT 
        tc.table_id,
        tc.constraint_name
    FROM 
        v_catalog.table_constraints tc
    WHERE 
        tc.constraint_type = 'p'
) pk ON t.table_id = pk.table_id
LEFT JOIN (
    SELECT 
        tc.table_id,
        tc.constraint_name
    FROM 
        v_catalog.table_constraints tc
    WHERE 
        tc.constraint_type = 'f'
) fk ON t.table_id = fk.table_id
WHERE 
    t.is_system_table = false
GROUP BY
    t.table_schema,
    t.table_name,
    t.is_temp_table,
    t.is_flextable,
    v.view_definition,
    ps.size_mb,
    ps.size_gb,
    ps.row_count,
    p.is_segmented,
    p.segment_expression,
    t.owner_name
ORDER BY 
    t.table_schema, t.table_name"""
    

table_level_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:vertica://vertica.ops.snapdeal.io:5433/snapdealdwh") \
    .option("query", qry_table_level) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.vertica.jdbc.Driver") \
    .option("host", hostname) \
    .option("database", database_name) \
    .load()
table_level_df.display()


# COMMAND ----------


qry_col_level = f"""SELECT 
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.ordinal_position,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key,
    COALESCE(pk.constraint_name, '') AS pk_constraint_name,
    CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END AS is_foreign_key,
    COALESCE(fk.constraint_name, '') AS fk_constraint_name
FROM 
    v_catalog.columns c
LEFT JOIN (
    SELECT 
        cc.column_name,
        cc.table_id,
        tc.constraint_name
    FROM 
        v_catalog.constraint_columns cc
    JOIN 
        v_catalog.table_constraints tc ON cc.constraint_id = tc.constraint_id
    WHERE 
        tc.constraint_type = 'p'
) pk ON c.table_id = pk.table_id AND c.column_name = pk.column_name
LEFT JOIN (
    SELECT 
        cc.column_name,
        cc.table_id,
        tc.constraint_name
    FROM 
        v_catalog.constraint_columns cc
    JOIN 
        v_catalog.table_constraints tc ON cc.constraint_id = tc.constraint_id
    WHERE 
        tc.constraint_type = 'f'
) fk ON c.table_id = fk.table_id AND c.column_name = fk.column_name
JOIN 
    v_catalog.tables t ON c.table_id = t.table_id
WHERE 
    t.is_system_table = false
ORDER BY 
    c.table_schema, c.table_name, c.ordinal_position"""
    

col_level_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:vertica://vertica.ops.snapdeal.io:5433/snapdealdwh") \
    .option("query", qry_col_level) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.vertica.jdbc.Driver") \
    .option("host", hostname) \
    .option("database", database_name) \
    .load()    

col_level_df.display()

# COMMAND ----------

col_level_df.count()

# COMMAND ----------

qry_col_level = f"""SELECT 
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.ordinal_position,
    c.character_maximum_length,
    c.numeric_precision, 
    c.numeric_scale, 
    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key,
    COALESCE(pk.constraint_name, '') AS pk_constraint_name,
    CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END AS is_foreign_key,
    COALESCE(fk.constraint_name, '') AS fk_constraint_name 
FROM 
    v_catalog.columns c
LEFT JOIN (
    SELECT 
        cc.column_name,
        cc.table_id,
        tc.constraint_name
    FROM 
        v_catalog.constraint_columns cc
    JOIN 
        v_catalog.table_constraints tc ON cc.constraint_id = tc.constraint_id
    WHERE 
        tc.constraint_type = 'p'
) pk ON c.table_id = pk.table_id AND c.column_name = pk.column_name
LEFT JOIN (
    SELECT 
        cc.column_name,
        cc.table_id,
        tc.constraint_name
    FROM 
        v_catalog.constraint_columns cc
    JOIN 
        v_catalog.table_constraints tc ON cc.constraint_id = tc.constraint_id
    WHERE 
        tc.constraint_type = 'f'
) fk ON c.table_id = fk.table_id AND c.column_name = fk.column_name
JOIN 
    v_catalog.tables t ON c.table_id = t.table_id
WHERE 
    t.is_system_table = false
ORDER BY 
    c.table_schema, c.table_name, c.ordinal_position"""
    

col_level_df = spark.read.format("jdbc") \
    .option("url", f"jdbc:vertica://vertica.ops.snapdeal.io:5433/snapdealdwh") \
    .option("query", qry_col_level) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.vertica.jdbc.Driver") \
    .option("host", hostname) \
    .option("database", database_name) \
    .load()    

col_level_df.display()

# COMMAND ----------

col_level_df.count()