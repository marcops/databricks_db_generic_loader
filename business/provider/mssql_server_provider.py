# Databricks notebook source
# MAGIC %run ./abstract_database_provider

# COMMAND ----------

import time
from datetime import datetime

class MSSqlServerProvider(AbstractDatabaseProvider):
    def get_all_tables_from_db(self):
        df_tables = self._query_executor("""
            (SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' ) AS t""")
        df_list = df_tables.select("TABLE_SCHEMA", "TABLE_NAME").collect()
        return [{'schema': row['TABLE_SCHEMA'], 'table': row['TABLE_NAME']} for row in df_list]


    def get_all_from_table(self, schema: str, table: str) -> "DataFrame":
        query = f"(SELECT * FROM [{schema}].[{table}]) AS t"
        return self._query_executor(query)

    def get_incremental_from_table(self, schema: str, table: str, column_where: str, last_update) -> "DataFrame":
        return self._query_executor(f"""
            (SELECT v.* FROM [{schema}].[{table}] as v 
                WHERE CONVERT(DATE, v.{column_where}) >= CONVERT(DATE, '{last_update}') AND
                    CONVERT(DATE, v.{column_where}) < CONVERT(DATE, GETDATE()) ) AS t""")

    def _query_executor(self, query: str) -> "DataFrame":
        return spark.read.jdbc(url=self.conn_str, table=query, properties=self.conn_properties)
