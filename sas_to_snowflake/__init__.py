"""
SAS Data Step to Snowflake SQL Converter
=========================================
Converts SAS DATA step code (including macro variables) to Snowflake SQL.

Usage:
    from sas_to_snowflake import convert
    sql = convert(sas_code, macro_vars={"mylib": "MY_DB.MY_SCHEMA"})
"""

from .converter import convert, SASToSnowflakeConverter

__version__ = "1.0.0"
__all__ = ["convert", "SASToSnowflakeConverter"]
