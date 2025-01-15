# Databricks notebook source
from enum import Enum

class DatabaseType(Enum):
    MONGO = "mongo"
    SQL_SERVER = "mssql_server"
