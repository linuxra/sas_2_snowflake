"""
CLI entry point for SAS to Snowflake converter.

Usage:
    python -m sas_to_snowflake input.sas [-o output.sql] [-m key=value ...]

Examples:
    python -m sas_to_snowflake mycode.sas
    python -m sas_to_snowflake mycode.sas -o result.sql
    python -m sas_to_snowflake mycode.sas -m mylib=PROD_DB.SCHEMA -m dt=2024-01-01
    echo "data x; set y; run;" | python -m sas_to_snowflake -
"""

import sys
import argparse
from .converter import SASToSnowflakeConverter


def main():
    parser = argparse.ArgumentParser(
        description="Convert SAS DATA step code to Snowflake SQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m sas_to_snowflake input.sas
  python -m sas_to_snowflake input.sas -o output.sql
  python -m sas_to_snowflake input.sas -m mylib=PROD_DB.SCHEMA -m dt=2024-01-01
  echo "data x; set y; run;" | python -m sas_to_snowflake -
        """
    )
    parser.add_argument("input", help="SAS input file (use '-' for stdin)")
    parser.add_argument("-o", "--output", help="Output SQL file (default: stdout)")
    parser.add_argument(
        "-m", "--macro", action="append", default=[],
        help="Macro variable as key=value (can be used multiple times)"
    )
    parser.add_argument(
        "-w", "--warnings", action="store_true",
        help="Show conversion warnings"
    )

    args = parser.parse_args()

    # Parse macro variables
    macro_vars = {}
    for m in args.macro:
        if '=' in m:
            key, value = m.split('=', 1)
            macro_vars[key.strip()] = value.strip()

    # Read input
    if args.input == '-':
        sas_code = sys.stdin.read()
    else:
        with open(args.input, 'r') as f:
            sas_code = f.read()

    # Convert
    converter = SASToSnowflakeConverter(macro_vars=macro_vars)
    result = converter.convert(sas_code)

    # Output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(result.sql)
            f.write('\n')
        print(f"Converted SQL written to {args.output}", file=sys.stderr)
    else:
        print(result.sql)

    # Warnings
    if args.warnings and result.warnings:
        print("\n-- Warnings:", file=sys.stderr)
        for w in result.warnings:
            print(f"--   {w}", file=sys.stderr)


if __name__ == "__main__":
    main()
