# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SAS DATA step to Snowflake SQL converter. Three-stage compiler pipeline (tokenizer → parser → code generator) with a FastAPI backend, React frontend, and Streamlit app.

## Commands

```bash
# Run all tests (28 test cases, 15 categories)
pytest test_converter.py -v

# Run a single test class
pytest test_converter.py::TestQualify -v

# Run a single test
pytest test_converter.py::TestArrayUnpivot::test_array_do_loop_with_output_unpivot -v

# Start FastAPI backend (port 8000)
python api_server.py

# Start React frontend (port 5173, proxies /api to backend)
cd frontend && npm run dev

# Start Streamlit app (port 8501, standalone)
streamlit run streamlit_app.py

# CLI usage
python -m sas_to_snowflake input.sas
python -m sas_to_snowflake input.sas -o result.sql -m mylib=PROD_DB.SCHEMA

# Regenerate docs PDFs
python docs/generate_docs.py
python docs/generate_github_guide.py
```

## Architecture

### Compiler Pipeline

```
SAS Code → Tokenizer → Parser → Code Generator → Snowflake SQL
           (tokens)    (AST)     (SQL string)
```

**Tokenizer** (`sas_to_snowflake/tokenizer.py`): Lexes SAS code into typed tokens. `TokenType` enum has 100+ types. `[`/`]` map to LBRACE/RBRACE for array access. Handles macro variables (`&var`), date literals, block/line comments.

**Parser** (`sas_to_snowflake/parser.py`): Recursive descent parser producing AST nodes. Key entry: `SASParser(tokens).parse() → List[DataStep]`. Each `DataStep` contains statements like `SetStatement`, `MergeStatement`, `IfThenElse`, `DoLoop`, `Assignment`, `ArrayDecl`, `RetainStatement`, etc. `Assignment` has an `is_sum` flag for SAS accumulator statements (`var + expr;`). `IfThenElse` has `is_subsetting_if` for row-filtering IF statements. `_expand_var_range()` handles SAS variable ranges like `month1-month12`.

**Code Generator** (`sas_to_snowflake/codegen.py`): Walks AST to emit Snowflake SQL. Key patterns:
- Simple SET → `CREATE OR REPLACE TABLE ... AS SELECT ... FROM`
- Multiple SET → `UNION ALL`
- MERGE with IN= → JOIN type inferred from subsetting IF (`if a and b` → INNER, `if a` → LEFT)
- IF/THEN/ELSE → `CASE WHEN` via `_conditional_to_case()`
- Array + DO loop + OUTPUT → `UNPIVOT INCLUDE NULLS` with CTEs (numbered_src → unpivoted → indexed → computed)
- RETAIN → `LAG()` window function in CTE
- FIRST./LAST. → `ROW_NUMBER() OVER (PARTITION BY ...)`
- Subsetting IF on computed columns → `QUALIFY` (not WHERE, since WHERE evaluates before SELECT)

**Function Registry** (`sas_to_snowflake/functions.py`): Template-based mapping of 50+ SAS functions to Snowflake equivalents. Uses `{0}`, `{1}` placeholders. Special functions (SUM, CATS, CATX, INTCK, INPUT, PUT) handled in codegen's `_handle_special_function()`.

**Converter** (`sas_to_snowflake/converter.py`): Orchestrates the pipeline. `SASToSnowflakeConverter.convert()` returns `ConversionResult(sql, warnings, ast, macro_vars)`. `convert()` convenience function returns just the SQL string.

### Web Interfaces

**FastAPI** (`api_server.py`): `POST /api/convert` accepts `{sas_code, macro_vars}`, returns `{sql, warnings}`. CORS for localhost:5173. Serves React build from `frontend/dist`.

**React** (`frontend/`): Vite + React 19. Split-pane editor UI. `vite.config.js` proxies `/api` to port 8000.

**Streamlit** (`streamlit_app.py`): Standalone app with 10 preloaded examples, macro variable sidebar, dark theme. Deployed on Streamlit Cloud.

### PROC FREQ Subproject

`freq/sas_to_snowpark/`: Separate converter for SAS PROC FREQ → Snowpark Python. Has its own parser, converter, generator, and tests.

## Testing Patterns

Tests use pytest. Each test converts SAS code and asserts specific SQL patterns exist in the output:

```python
def test_example(self):
    sql = run_convert("data x; set y; if a > 1; run;")
    assert "WHERE" in sql
    assert "a > 1" in sql
```

Key assertion messages document *why* a pattern is expected (e.g., `"Should use QUALIFY, not WHERE"`). When adding new SAS patterns, add a test class with assertions checking for the correct Snowflake SQL constructs.

## CI

GitHub Actions (`.github/workflows/ci.yml`): Runs `pytest test_converter.py -v` on push to main and PRs. Python 3.11 on ubuntu-latest.

## Snowflake SQL Conventions

These Snowflake-specific patterns are intentional and should be preserved:
- `UNPIVOT INCLUDE NULLS` (not `UNION ALL`) for wide-to-long — single table scan
- `QUALIFY` (not CTE/subquery) for filtering on computed columns — evaluated after SELECT
- `PARTITION BY` in window functions to scope per entity (e.g., `_row_id_`)
- `ROW_NUMBER() OVER (ORDER BY (SELECT NULL))` for synthetic row IDs before UNPIVOT
- `COALESCE(val, 0)` wrapping for SAS `sum()` which treats missing as zero
- `* EXCLUDE (col)` for DROP, `* RENAME (old AS new)` for RENAME
