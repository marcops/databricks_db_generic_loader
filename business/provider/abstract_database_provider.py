# Databricks notebook source
from abc import ABC, abstractmethod

class AbstractDatabaseProvider(ABC):
    def __init__(self, conn_str: str, conn_properties: dict):
        self.conn_str = conn_str
        self.conn_properties = conn_properties

    @abstractmethod
    def get_all_tables_from_db(self):
        raise NotImplementedError("Subclasses should implement this method")

    @abstractmethod
    def get_all_from_table(self, table:str):
        raise NotImplementedError("Subclasses should implement this method")

    @abstractmethod
    def get_incremental_from_table(sel, table: str, column_where: str, last_update) -> "DataFrame":
        raise NotImplementedError("Subclasses should implement this method")
