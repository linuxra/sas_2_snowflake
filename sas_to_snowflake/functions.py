"""
SAS to Snowflake Function Mapping
=================================
Maps SAS functions to their Snowflake SQL equivalents.
Handles string, numeric, date/time, missing value, and macro functions.
"""

from typing import List, Optional, Callable


# ──────────────────── Function Mapping Registry ────────────────────

# Each entry: "SAS_FUNC_NAME" -> (snowflake_template, arg_transformer)
# Template uses {0}, {1}, ... for positional args
# arg_transformer is optional callable(args) -> transformed_args

FUNCTION_MAP = {}


def register(sas_name: str, template: str, arg_transform: Optional[Callable] = None, min_args: int = 0, max_args: int = 99):
    """Register a SAS-to-Snowflake function mapping."""
    FUNCTION_MAP[sas_name.upper()] = {
        "template": template,
        "arg_transform": arg_transform,
        "min_args": min_args,
        "max_args": max_args,
    }


# ═══════════════════════════ STRING FUNCTIONS ═══════════════════════════

register("SUBSTR", "SUBSTR({0}, {1}, {2})", min_args=2, max_args=3)
register("SCAN", "SPLIT_PART({0}, {1}, {2})", min_args=2, max_args=3)
register("TRIM", "TRIM({0})", min_args=1, max_args=1)
register("LEFT", "LTRIM({0})", min_args=1, max_args=1)
register("RIGHT", "RTRIM({0})", min_args=1, max_args=1)
register("STRIP", "TRIM({0})", min_args=1, max_args=1)
register("COMPRESS", None, min_args=1, max_args=3)  # Special handling
register("TRANWRD", "REPLACE({0}, {1}, {2})", min_args=3, max_args=3)
register("TRANSLATE", "TRANSLATE({0}, {2}, {1})", min_args=3, max_args=3)  # SAS arg order differs
register("INDEX", "POSITION({1} IN {0})", min_args=2, max_args=2)
register("INDEXC", "REGEXP_INSTR({0}, '[' || {1} || ']')", min_args=2, max_args=2)
register("FIND", "POSITION({1} IN {0})", min_args=2, max_args=4)
register("UPCASE", "UPPER({0})", min_args=1, max_args=1)
register("LOWCASE", "LOWER({0})", min_args=1, max_args=1)
register("PROPCASE", "INITCAP({0})", min_args=1, max_args=1)
register("CAT", "CONCAT({args})", min_args=1, max_args=99)
register("CATS", None, min_args=1, max_args=99)  # Special: CONCAT(TRIM(a), TRIM(b), ...)
register("CATX", None, min_args=2, max_args=99)  # Special: CONCAT_WS(delim, TRIM(a), ...)
register("CATT", None, min_args=1, max_args=99)  # Special: trim trailing
register("LENGTH", "LENGTH({0})", min_args=1, max_args=1)
register("LENGTHN", "LENGTH({0})", min_args=1, max_args=1)
register("LENGTHC", "LENGTH({0})", min_args=1, max_args=1)
register("REVERSE", "REVERSE({0})", min_args=1, max_args=1)
register("REPEAT", "REPEAT({0}, {1})", min_args=2, max_args=2)
register("INPUT", None, min_args=2, max_args=2)  # Special handling
register("PUT", None, min_args=2, max_args=2)  # Special handling
register("BYTE", "CHR({0})", min_args=1, max_args=1)
register("RANK", "ASCII({0})", min_args=1, max_args=1)
register("VERIFY", None, min_args=2, max_args=2)  # Special
register("COUNTW", None, min_args=1, max_args=2)  # Special
register("COUNT", None, min_args=2, max_args=2)  # Special

# ═══════════════════════════ NUMERIC FUNCTIONS ═══════════════════════════

register("SUM", None, min_args=1, max_args=99)  # Special: handles multiple args
register("MEAN", None, min_args=1, max_args=99)  # Special
register("MIN", "LEAST({args})", min_args=1, max_args=99)
register("MAX", "GREATEST({args})", min_args=1, max_args=99)
register("ABS", "ABS({0})", min_args=1, max_args=1)
register("CEIL", "CEIL({0})", min_args=1, max_args=1)
register("FLOOR", "FLOOR({0})", min_args=1, max_args=1)
register("ROUND", "ROUND({0}, {1})", min_args=1, max_args=2)
register("ROUNDE", "ROUND({0}, {1})", min_args=1, max_args=2)
register("MOD", "MOD({0}, {1})", min_args=2, max_args=2)
register("INT", "TRUNC({0})", min_args=1, max_args=1)
register("LOG", "LN({0})", min_args=1, max_args=1)
register("LOG2", "LOG(2, {0})", min_args=1, max_args=1)
register("LOG10", "LOG(10, {0})", min_args=1, max_args=1)
register("EXP", "EXP({0})", min_args=1, max_args=1)
register("SQRT", "SQRT({0})", min_args=1, max_args=1)
register("SIGN", "SIGN({0})", min_args=1, max_args=1)
register("RANUNI", "UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())", min_args=1, max_args=1)
register("RAND", "UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())", min_args=1, max_args=2)

# ═══════════════════════════ DATE/TIME FUNCTIONS ═══════════════════════════

register("TODAY", "CURRENT_DATE()", min_args=0, max_args=0)
register("DATE", "CURRENT_DATE()", min_args=0, max_args=0)
register("DATETIME", "CURRENT_TIMESTAMP()", min_args=0, max_args=0)
register("TIME", "CURRENT_TIME()", min_args=0, max_args=0)
register("DATEPART", "CAST({0} AS DATE)", min_args=1, max_args=1)
register("TIMEPART", "CAST({0} AS TIME)", min_args=1, max_args=1)
register("INTCK", None, min_args=3, max_args=3)  # Special: DATEDIFF
register("INTNX", None, min_args=3, max_args=4)  # Special: DATEADD
register("MDY", "DATE_FROM_PARTS({2}, {0}, {1})", min_args=3, max_args=3)
register("YYQ", None, min_args=2, max_args=2)  # Special
register("DHMS", "TIMESTAMP_FROM_PARTS(YEAR({0}), MONTH({0}), DAY({0}), {1}, {2}, {3})", min_args=4, max_args=4)
register("HMS", "TIME_FROM_PARTS({0}, {1}, {2})", min_args=3, max_args=3)
register("YEAR", "YEAR({0})", min_args=1, max_args=1)
register("MONTH", "MONTH({0})", min_args=1, max_args=1)
register("DAY", "DAY({0})", min_args=1, max_args=1)
register("HOUR", "HOUR({0})", min_args=1, max_args=1)
register("MINUTE", "MINUTE({0})", min_args=1, max_args=1)
register("SECOND", "SECOND({0})", min_args=1, max_args=1)
register("WEEKDAY", "DAYOFWEEK({0})", min_args=1, max_args=1)
register("QTR", "QUARTER({0})", min_args=1, max_args=1)

# ═══════════════════════════ MISSING VALUE FUNCTIONS ═══════════════════════════

register("MISSING", "{0} IS NULL", min_args=1, max_args=1)
register("COALESCE", "COALESCE({args})", min_args=1, max_args=99)
register("COALESCEC", "COALESCE({args})", min_args=1, max_args=99)
register("NMISS", None, min_args=1, max_args=99)  # Special
register("CMISS", None, min_args=1, max_args=99)  # Special
register("N", None, min_args=1, max_args=99)  # Special: count non-missing
register("IFN", "CASE WHEN {0} THEN {1} ELSE {2} END", min_args=3, max_args=4)
register("IFC", "CASE WHEN {0} THEN {1} ELSE {2} END", min_args=3, max_args=4)

# ═══════════════════════════ TYPE CONVERSION ═══════════════════════════

register("INPUTN", "TRY_TO_NUMBER({0})", min_args=2, max_args=2)
register("INPUTC", "TRY_TO_VARCHAR({0})", min_args=2, max_args=2)

# ═══════════════════════════ LAG/DIF ═══════════════════════════

register("LAG", "LAG({0}) OVER (ORDER BY 1)", min_args=1, max_args=1)
register("LAG1", "LAG({0}, 1) OVER (ORDER BY 1)", min_args=1, max_args=1)
register("LAG2", "LAG({0}, 2) OVER (ORDER BY 1)", min_args=1, max_args=1)
register("LAG3", "LAG({0}, 3) OVER (ORDER BY 1)", min_args=1, max_args=1)
register("DIF", "({0} - LAG({0}) OVER (ORDER BY 1))", min_args=1, max_args=1)
register("DIF1", "({0} - LAG({0}, 1) OVER (ORDER BY 1))", min_args=1, max_args=1)

# ═══════════════════════════ BOOLEAN / LOGICAL ═══════════════════════════

register("WHICHN", None, min_args=2, max_args=99)  # Special
register("WHICHC", None, min_args=2, max_args=99)  # Special


# ──────────────────── SAS Date Format to Snowflake Format Mapping ────────────────────

SAS_DATE_FORMATS = {
    # SAS informat/format -> Snowflake format string
    'DATE9.': 'DDMONYYYY',
    'DATE7.': 'DDMONYY',
    'MMDDYY10.': 'MM/DD/YYYY',
    'MMDDYY8.': 'MM/DD/YY',
    'DDMMYY10.': 'DD/MM/YYYY',
    'DDMMYY8.': 'DD/MM/YY',
    'YYMMDD10.': 'YYYY-MM-DD',
    'YYMMDD8.': 'YY-MM-DD',
    'YYMMDDN8.': 'YYYYMMDD',
    'MONYY7.': 'MONYYYY',
    'MONYY5.': 'MONYY',
    'DATETIME20.': 'YYYY-MM-DD HH24:MI:SS',
    'DATETIME18.': 'YYYY-MM-DD HH24:MI',
    'TIME8.': 'HH24:MI:SS',
    'TIME5.': 'HH24:MI',
    'BEST12.': None,  # Numeric format, not date
    'BEST.': None,
    'COMMA12.': None,
    'DOLLAR12.': None,
    '$': None,
    'Z5.': None,
    'PERCENT8.2': None,
}

# SAS interval to Snowflake interval mapping
SAS_INTERVALS = {
    'DAY': 'DAY',
    'MONTH': 'MONTH',
    'YEAR': 'YEAR',
    'QTR': 'QUARTER',
    'WEEK': 'WEEK',
    'HOUR': 'HOUR',
    'MINUTE': 'MINUTE',
    'SECOND': 'SECOND',
    'DTDAY': 'DAY',
    'DTMONTH': 'MONTH',
    'DTYEAR': 'YEAR',
    'DTQTR': 'QUARTER',
    'DTWEEK': 'WEEK',
    'DTHOUR': 'HOUR',
    'DTMINUTE': 'MINUTE',
    'DTSECOND': 'SECOND',
}


def get_snowflake_function(sas_func: str) -> Optional[dict]:
    """Look up a SAS function and return its Snowflake mapping info."""
    return FUNCTION_MAP.get(sas_func.upper())


def get_snowflake_date_format(sas_format: str) -> Optional[str]:
    """Convert a SAS date format to Snowflake date format string."""
    return SAS_DATE_FORMATS.get(sas_format.upper(), None)


def get_snowflake_interval(sas_interval: str) -> str:
    """Convert a SAS interval to Snowflake interval."""
    clean = sas_interval.upper().strip("'\"")
    return SAS_INTERVALS.get(clean, clean)
