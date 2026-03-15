"""
sas_to_snowpark - Convert SAS PROC FREQ code to Snowflake Snowpark Python code.

Supports all major PROC FREQ features:
  - One-way frequency tables
  - Two-way / N-way crosstabulations
  - LIST and CROSSLIST output formats
  - Statistical options (CHISQ, FISHER, CMH, MEASURES, EXACT, etc.)
  - Table options (NOCUM, NOPERCENT, NOFREQ, NOROW, NOCOL, MISSING, SPARSE, etc.)
  - OUTPUT datasets (OUT=, OUTCUM, OUTPCT, OUTEXPECT)
  - WEIGHT statement
  - BY statement (group-level processing)
  - WHERE clause / data filtering
  - FORMAT statement
  - ORDER= option (FREQ, DATA, FORMATTED, INTERNAL)
  - Multiple TABLES statements in one PROC FREQ block
"""

__version__ = "1.0.0"

from .converter import convert, ProcFreqConverter
from .parser import ProcFreqParser, ProcFreqBlock
from .generator import SnowparkGenerator

__all__ = [
    "convert",
    "ProcFreqConverter",
    "ProcFreqParser",
    "ProcFreqBlock",
    "SnowparkGenerator",
]
