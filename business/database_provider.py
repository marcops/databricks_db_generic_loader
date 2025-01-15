# Databricks notebook source
# MAGIC %run ./provider/mssql_server_provider

# COMMAND ----------

# MAGIC %run ./provider/mongo_provider

# COMMAND ----------

# MAGIC %run ./database_type

# COMMAND ----------

class DatabaseProvider:
    def __init__(self):
        self.providers = {
            DatabaseType.MSSQL_SERVER: MSSqlServerProvider,
            DatabaseType.MONGO: MongoProvider
        }
    
    def get_provider(self, db_type: DatabaseType):
        if db_type not in self.providers:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        return self.providers[db_type]
