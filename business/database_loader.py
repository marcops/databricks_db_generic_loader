# Databricks notebook source
# MAGIC %run ./database_provider

# COMMAND ----------

class DatabaseLoader:
    def __init__(self, config):
        self.table_helper = TableHelper()
        self.config = config
        self._validate_config()
        provider = DatabaseProvider().get_provider(self.config.get('database_type'))
        self.database_provider = provider(self.config.get('conn_string'), self.config.get('conn_properties'))

    def _validate_config(self):
        self._validate_required_fields()
        self._validate_database_type()

    def _validate_required_fields(self):
        required_fields = [
            'database_type',
            'catalog', 
            'conn_string', 
            'conn_properties', 
            'origin',
            'destiny',
            'incremental_tables',
            'env'
        ]
        missing_fields = [field for field in required_fields if field not in self.config]
        
        if missing_fields:
            raise ValueError(f"Incomplete configuration. Missing fields: {', '.join(missing_fields)}")

    def _validate_database_type(self):
        database_type = self.config.get('database_type')
        if database_type not in [e for e in DatabaseType]:
            raise ValueError(f"Invalid 'database_type' ({database_type}). Must be one of {[e.value for e in DatabaseType]}.")
    
    def import_all_tables(self):
        self.table_helper.create_schema(self.config.get("catalog"), self.config.get("destiny"))
        origin_table_list = self.database_provider.get_all_tables_from_db()
        print(f" --- TABLES TO LOAD  --- \r\n" + ', '.join([f"{item['schema']}.{item['table']}" for item in origin_table_list]) + "\r\n ---   --- ")
        
        for item in origin_table_list:
            origin_schema, origin_table = item['schema'], item['table']

            column = next((item["column"] for item in self.config.get("incremental_tables") if item["name"] == origin_table), None) if self.config.get("incremental_tables") is not None else None
            self.import_table(origin_schema, origin_table, column)   

    def import_table(self, schema:str, table: str, column_where: str) -> None:
        start_time = time.time()

        try:
            if column_where is not None:
                mode = "append"
                last_update = self._check_and_update_last_execution(schema, table)
                print(f"Loading {schema}.{table}  using Incremental - collecting {column_where} >= {last_update} and {column_where} < {time.strftime("%Y-%m-%d", time.localtime(start_time))} ", end='')
                df = self.database_provider.get_incremental_from_table(schema, table, column_where, last_update)
            else:
                mode = "overwrite"
                print(f"Loading {schema}.{table} using full load", end='')
                df = self.database_provider.get_all_from_table(schema, table)

            self.table_helper.create_table(df, self.config.get("catalog"), self.config.get("destiny"), "bronze", table, mode)
            print(f" - took: {(time.time() - start_time):.4f} seconds")
        except Exception as e:
            print(f"Ocorreu um erro ao processar a tabela '{schema}.{table}'. Erro: {str(e)[:100]}")
        
        if (time.time() - start_time) > 60: print(f"---\r\nUSE INCREMENTAL FOR THE {schema}.{table}  BECAUSE TOOK: {(time.time() - start_time):.4f} seconds\r\n---")

    def _check_and_update_last_execution(self, origin_schema, origin_table: str) -> str:
        today_date = datetime.today().strftime('%Y-%m-%d')
        table_name = f"""{self.config.get("origin")}.{origin_schema}.{origin_table}"""
        catalog = self.config.get("catalog")

        df = spark.sql(f"SELECT last_execution FROM {catalog}.default.config_sql_executor WHERE table_name = '{table_name}'")
        if df.count() > 0:
            last_execution = df.select("last_execution").first()[0]
            spark.sql(f"""
                UPDATE {catalog}.default.config_sql_executor
                SET last_execution = '{today_date}'
                WHERE table_name = '{table_name}'
            """)            
            return last_execution.strftime('%Y-%m-%d')
        else:
            spark.sql(f"""
                INSERT INTO {catalog}.default.config_sql_executor (table_name, last_execution)
                VALUES ('{table_name}', '{today_date}')
            """)
        
        return "1900-01-01"
