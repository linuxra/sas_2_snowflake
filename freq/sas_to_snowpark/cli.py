"""
Command-line interface for sas_to_snowpark.

Usage::

    sas2snowpark input.sas                    # prints to stdout
    sas2snowpark input.sas -o output.py       # writes to file
    sas2snowpark input.sas --prefix my_freq   # custom function prefix
"""

from __future__ import annotations

import argparse
import sys

from .converter import convert


def main():
    parser = argparse.ArgumentParser(
        description="Convert SAS PROC FREQ code to Snowflake Snowpark Python."
    )
    parser.add_argument("input", help="Path to the SAS source file")
    parser.add_argument("-o", "--output", help="Output Python file (default: stdout)")
    parser.add_argument(
        "--prefix",
        default="proc_freq",
        help="Function name prefix (default: proc_freq)",
    )
    args = parser.parse_args()

    with open(args.input, "r") as f:
        sas_code = f.read()

    python_code = convert(sas_code, func_prefix=args.prefix)

    if args.output:
        with open(args.output, "w") as f:
            f.write(python_code)
        print(f"Snowpark code written to {args.output}")
    else:
        print(python_code)


if __name__ == "__main__":
    main()
