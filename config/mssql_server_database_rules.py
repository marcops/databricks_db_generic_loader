# Databricks notebook source
def get_db_rules():
    return [
        { 
            "origin": "database_xxx_prod",
            "destiny": "database_xxx_v1",
            "incremental_tables": [
                {
                    "name": "TableX", 
                    "column": "createdate"
                }
            ]
        }
    ]
