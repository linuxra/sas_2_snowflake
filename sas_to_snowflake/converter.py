"""
SAS to Snowflake Converter - Main Entry Point
==============================================
Provides the high-level API for converting SAS DATA step code to Snowflake SQL.
"""

from typing import Dict, Optional, List
from .tokenizer import tokenize
from .parser import SASParser, DataStep
from .codegen import SnowflakeCodeGen


class SASToSnowflakeConverter:
    """
    Main converter class.

    Usage:
        converter = SASToSnowflakeConverter(macro_vars={"mylib": "MY_DB.MY_SCHEMA"})
        result = converter.convert(sas_code)
        print(result.sql)
        print(result.warnings)
    """

    def __init__(self, macro_vars: Optional[Dict[str, str]] = None):
        """
        Initialize the converter.

        Args:
            macro_vars: Dictionary of SAS macro variable names to their values.
                        e.g., {"mylib": "MY_DB.MY_SCHEMA", "dt": "2024-01-01"}
        """
        self.macro_vars = macro_vars or {}

    def convert(self, sas_code: str) -> 'ConversionResult':
        """
        Convert SAS DATA step code to Snowflake SQL.

        Args:
            sas_code: SAS DATA step code as a string.

        Returns:
            ConversionResult with .sql, .warnings, and .ast attributes.
        """
        # Step 1: Tokenize
        tokens = tokenize(sas_code)

        # Step 2: Parse
        parser = SASParser(tokens)
        steps = parser.parse()

        # Merge parser-discovered macro vars with user-provided ones
        all_macros = {**parser.macro_vars, **self.macro_vars}

        # Step 3: Generate Snowflake SQL
        codegen = SnowflakeCodeGen(macro_vars=all_macros)
        sql = codegen.generate(steps)

        return ConversionResult(
            sql=sql,
            warnings=codegen.warnings,
            ast=steps,
            macro_vars=all_macros
        )


class ConversionResult:
    """Result of a SAS-to-Snowflake conversion."""

    def __init__(self, sql: str, warnings: List[str], ast: List[DataStep],
                 macro_vars: Dict[str, str]):
        self.sql = sql
        self.warnings = warnings
        self.ast = ast
        self.macro_vars = macro_vars

    def __str__(self):
        return self.sql

    def __repr__(self):
        return f"ConversionResult(sql='{self.sql[:80]}...', warnings={len(self.warnings)})"


def convert(sas_code: str, macro_vars: Optional[Dict[str, str]] = None) -> str:
    """
    Convenience function: convert SAS DATA step code to Snowflake SQL.

    Args:
        sas_code: SAS DATA step code.
        macro_vars: Optional macro variable definitions.

    Returns:
        Snowflake SQL as a string.
    """
    converter = SASToSnowflakeConverter(macro_vars=macro_vars)
    result = converter.convert(sas_code)
    return result.sql
