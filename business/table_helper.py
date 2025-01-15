# Databricks notebook source
import re

class TableHelper:
    def _format_table_name(self, name: str) -> str:
        cleaned_name = re.sub(r'[\[\]]', '', name)
        cleaned_name = cleaned_name.split('.')[-1]
        cleaned_name = re.sub(r'(?<!^)(?=[A-Z])', '_', cleaned_name).lower()
        return re.sub(r'_+', '_', cleaned_name)
    
    def _lower_column_names(self, dataframe: "DataFrame") -> "DataFrame":
        columns_name = dataframe.columns
        return dataframe.toDF(*[column_name.lower() for column_name in columns_name])

    def create_table(self, df: "DataFrame", catalog: str, schema: str, tier: str, table: str, 
                     mode: str = "overwrite", version: str = "v1", partition_by: str = None) -> None:
        f_table = self._format_table_name(table)
        n_table = f"{tier}_{f_table}_{version}"
        (
            self._lower_column_names(df)
            .write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(f"{catalog}.{schema}.{n_table}")
        )
        return df
    
    def create_schema(self, catalog: str, schema: str) -> None:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
