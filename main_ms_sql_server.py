# Databricks notebook source
# MAGIC %run ./config/mssql_server_database_rules

# COMMAND ----------

# MAGIC %run ./business/database_service

# COMMAND ----------

def generate_config(origin, destiny, incremental_tables):
    env = "env"
    jdbc_conn = "jdbc:sqlserver://database.windows.net:1433;encrypt=true;trustServerCertificate=true;loginTimeout=30;database="
    conn_string = f"{jdbc_conn}{origin}"
    conn_properties = {
        "user": "user",
        "password": "pass",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    return {
        'database_type': DatabaseType.MSSQL_SERVER,
        'catalog': f"catalog_{env}_v1",
        'conn_string': conn_string,
        'conn_properties': conn_properties,
        'env': env,
        'origin': origin,
        'destiny': destiny,
        'incremental_tables': incremental_tables
    }

# COMMAND ----------

DatabaseService().execute(get_db_rules(), generate_config)
