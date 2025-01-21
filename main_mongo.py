# Databricks notebook source
# MAGIC %run ./config/mongo_database_rules

# COMMAND ----------

# MAGIC %run ./business/database_service

# COMMAND ----------

def generate_config(origin, destiny, incremental_tables):
    env = "env"
    conn_string = "mongodb://mongo-srv"
    conn_properties = {
        "db_name": origin
    }

    return {
        'database_type': DatabaseType.MONGO,
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
