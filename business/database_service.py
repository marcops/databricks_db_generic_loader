# Databricks notebook source
# MAGIC %run ./table_helper

# COMMAND ----------

# MAGIC %run ./database_loader

# COMMAND ----------

class DatabaseService:
    def execute(self, databases_rules, config_function):
        for db in databases_rules:            
            origin = db['origin']
            destiny = db['destiny']
            incremental_tables = db.get('incremental_tables', None)
            
            config = config_function(origin, destiny,  incremental_tables)
            print(f"loading from: {origin} \r\n  to: {destiny} \r\n {incremental_tables}\r\n --")
            DatabaseLoader(config).import_all_tables()
        
