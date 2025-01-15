# Databricks notebook source
# MAGIC %pip install pymongo

# COMMAND ----------

#%restart_python

# COMMAND ----------

# MAGIC %run ./abstract_database_provider

# COMMAND ----------

import time
from datetime import datetime
from pymongo import MongoClient
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType

class MongoProvider(AbstractDatabaseProvider):
    def get_all_tables_from_db(self):
        client = MongoClient(self.conn_str)
        db = client[self.conn_properties.get("db_name")]
        collections = db.list_collection_names()
        return [{'schema': "_", 'table': collection} for collection in collections]

    def get_all_from_table(self, schema: str, table: str) -> "DataFrame":
        return self._query_executor([], table)

    def get_incremental_from_table(self, schema: str, table: str, column_where: str, last_update) -> "DataFrame":
        last_update = datetime.strptime(last_update, '%Y-%m-%d')

        query = [{
            '$match': {
                column_where: {
                    '$gte': last_update,
                    '$lt': datetime.now()
                }
            }
        }]
        return self._query_executor(query, table)

    def _query_executor(self, query: list, table: str) -> "DataFrame":
        df = (spark.read
            .format("mongo")
            .option('spark.mongodb.input.uri', self.conn_str)
            .option('spark.mongodb.input.database', self.conn_properties.get("db_name"))
            .option('spark.mongodb.input.collection', table)
            .option("pipeline", query)
            .load())
        
        if df.isEmpty(): return df
        return (df
            .withColumn('id', col('_id').cast(StringType())) 
            .withColumn('id', expr("substring(id, 2, length(id) - 2)")) 
            .drop('_id')
            .drop('_class'))
