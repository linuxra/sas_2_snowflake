"""
Test Suite for SAS to Snowflake Converter
==========================================
Uses pytest with assertions to verify each conversion pattern.
Each test checks for specific SQL keywords/patterns in the output.
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sas_to_snowflake import convert, SASToSnowflakeConverter


def run_convert(sas_code, macro_vars=None):
    converter = SASToSnowflakeConverter(macro_vars=macro_vars)
    result = converter.convert(sas_code)
    return result.sql


# ── 1. BASIC SET WITH KEEP/DROP/RENAME ──────────────────────────

class TestBasicSetOperations:

    def test_simple_set_with_keep(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            keep id name salary department;
        run;
        """)
        assert "CREATE OR REPLACE TABLE work.output" in sql
        assert "FROM work.input" in sql
        assert "inp.id" in sql
        assert "inp.name" in sql
        assert "inp.salary" in sql
        assert "inp.department" in sql
        assert "EXCLUDE" not in sql  # KEEP should select specific cols, not EXCLUDE

    def test_set_with_drop(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            drop temp_var debug_flag;
        run;
        """)
        assert "CREATE OR REPLACE TABLE work.output" in sql
        assert "EXCLUDE (temp_var, debug_flag)" in sql

    def test_set_with_rename(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            rename old_name = new_name old_id = new_id;
        run;
        """)
        assert "CREATE OR REPLACE TABLE work.output" in sql
        assert "RENAME (old_name AS new_name, old_id AS new_id)" in sql


# ── 2. COMPUTED COLUMNS / ASSIGNMENTS ───────────────────────────

class TestComputedColumns:

    def test_assignments_with_expressions(self):
        sql = run_convert("""
        data work.output;
            set work.employees;
            annual_salary = monthly_salary * 12;
            full_name = cats(first_name, ' ', last_name);
            age = intck('year', birth_date, today());
            tax = round(annual_salary * 0.22, 2);
        run;
        """)
        assert "(monthly_salary * 12) AS annual_salary" in sql
        assert "CONCAT" in sql  # cats → CONCAT
        assert "DATEDIFF('YEAR'" in sql  # intck → DATEDIFF
        assert "ROUND(" in sql
        assert "CURRENT_DATE()" in sql  # today() → CURRENT_DATE()


# ── 3. IF/THEN/ELSE → CASE WHEN ────────────────────────────────

class TestIfThenElse:

    def test_simple_if_then_else(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            if score >= 90 then grade = 'A';
            else if score >= 80 then grade = 'B';
            else if score >= 70 then grade = 'C';
            else grade = 'F';
        run;
        """)
        assert "CASE WHEN (score >= 90) THEN 'A'" in sql
        assert "WHEN (score >= 80) THEN 'B'" in sql
        assert "WHEN (score >= 70) THEN 'C'" in sql
        assert "ELSE 'F' END AS grade" in sql

    def test_subsetting_if_filter(self):
        sql = run_convert("""
        data work.active_employees;
            set work.employees;
            if status = 'ACTIVE';
        run;
        """)
        assert "WHERE" in sql
        assert "status = 'ACTIVE'" in sql

    def test_if_with_delete(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            if age < 18 then delete;
        run;
        """)
        assert "WHERE" in sql
        assert "NOT" in sql
        assert "age < 18" in sql


# ── 4. MERGE WITH IN= VARIABLES ────────────────────────────────

class TestMergeJoins:

    def test_inner_join(self):
        sql = run_convert("""
        data work.matched;
            merge work.customers (in=a) work.orders (in=b);
            by customer_id;
            if a and b;
        run;
        """)
        assert "INNER JOIN" in sql
        assert "ON a.customer_id = b.customer_id" in sql

    def test_left_join(self):
        sql = run_convert("""
        data work.all_customers;
            merge work.customers (in=a) work.orders (in=b);
            by customer_id;
            if a;
        run;
        """)
        assert "LEFT OUTER JOIN" in sql
        assert "ON a.customer_id = b.customer_id" in sql

    def test_full_outer_join_with_computed_cols(self):
        sql = run_convert("""
        data work.combined;
            merge work.table1 (in=a) work.table2 (in=b);
            by id;
            if a and b then match_flag = 'BOTH';
            else if a then match_flag = 'LEFT_ONLY';
            else match_flag = 'RIGHT_ONLY';
        run;
        """)
        assert "CASE WHEN" in sql
        assert "'BOTH'" in sql
        assert "'LEFT_ONLY'" in sql
        assert "'RIGHT_ONLY'" in sql
        assert "AS match_flag" in sql


# ── 5. MACRO VARIABLES ─────────────────────────────────────────

class TestMacroVariables:

    def test_inline_macro_let(self):
        sql = run_convert("""
        %let input_lib = PROD_DB.RAW_SCHEMA;
        %let output_lib = PROD_DB.ANALYTICS;
        %let cutoff_date = 2024-01-01;

        data &output_lib..summary;
            set &input_lib..transactions;
            where transaction_date >= "&cutoff_date."d;
            keep customer_id amount transaction_date;
        run;
        """)
        assert "PROD_DB.ANALYTICS.summary" in sql
        assert "PROD_DB.RAW_SCHEMA.transactions" in sql
        assert "TO_DATE('2024-01-01'" in sql
        # Macro variables should be resolved, not literal
        assert "&output_lib" not in sql
        assert "&input_lib" not in sql

    def test_external_macro_vars(self):
        sql = run_convert("""
        data &mylib..output;
            set &mylib..input;
            if region = "&region." then flag = 1;
            else flag = 0;
        run;
        """, macro_vars={"mylib": "DW.SCHEMA1", "region": "WEST"})
        assert "DW.SCHEMA1.output" in sql
        assert "DW.SCHEMA1.input" in sql
        assert "'WEST'" in sql
        assert "&mylib" not in sql


# ── 6. SAS FUNCTIONS → SNOWFLAKE FUNCTIONS ──────────────────────

class TestFunctionConversion:

    def test_string_functions(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            upper_name = upcase(name);
            lower_name = lowcase(name);
            proper_name = propcase(name);
            name_len = length(name);
            first3 = substr(name, 1, 3);
            word2 = scan(full_text, 2, ' ');
            clean_val = compress(raw_value);
            replaced = tranwrd(text, 'old', 'new');
            pos = index(text, 'find_me');
            combined = catx('-', part1, part2, part3);
        run;
        """)
        assert "UPPER(name) AS upper_name" in sql
        assert "LOWER(name) AS lower_name" in sql
        assert "INITCAP(name) AS proper_name" in sql
        assert "LENGTH(name) AS name_len" in sql
        assert "SUBSTR(name, 1, 3) AS first3" in sql
        assert "SPLIT_PART(" in sql  # scan → SPLIT_PART
        assert "REPLACE(" in sql  # compress/tranwrd → REPLACE
        assert "POSITION(" in sql  # index → POSITION
        assert "CONCAT_WS('-'" in sql  # catx → CONCAT_WS

    def test_date_functions(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            current_dt = today();
            yr = year(hire_date);
            mo = month(hire_date);
            dy = day(hire_date);
            tenure_years = intck('year', hire_date, today());
            next_review = intnx('month', hire_date, 6);
            created = mdy(1, 15, 2024);
            qtr = qtr(report_date);
        run;
        """)
        assert "CURRENT_DATE() AS current_dt" in sql
        assert "YEAR(hire_date) AS yr" in sql
        assert "MONTH(hire_date) AS mo" in sql
        assert "DAY(hire_date) AS dy" in sql
        assert "DATEDIFF('YEAR'" in sql  # intck → DATEDIFF
        assert "DATEADD('MONTH', 6" in sql  # intnx → DATEADD
        assert "DATE_FROM_PARTS(2024, 1, 15)" in sql  # mdy → DATE_FROM_PARTS
        assert "QUARTER(report_date) AS qtr" in sql

    def test_numeric_and_missing_functions(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            total = sum(val1, val2, val3);
            avg_val = mean(val1, val2, val3);
            min_val = min(val1, val2, val3);
            max_val = max(val1, val2, val3);
            abs_val = abs(difference);
            rounded = round(amount, 0.01);
            null_count = nmiss(val1, val2, val3);
            first_valid = coalesce(val1, val2, val3);
        run;
        """)
        assert "COALESCE(val1, 0)" in sql  # SAS sum handles nulls
        assert "AS total" in sql
        assert "LEAST(" in sql  # min → LEAST
        assert "GREATEST(" in sql  # max → GREATEST
        assert "ABS(difference)" in sql
        assert "ROUND(amount, 0.01)" in sql
        assert "IS NULL THEN 1" in sql  # nmiss
        assert "COALESCE(val1, val2, val3) AS first_valid" in sql

    def test_input_put_conversions(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            date_val = input(date_str, date9.);
            num_val = input(num_str, best12.);
            date_str = put(date_val, mmddyy10.);
            char_val = put(num_val, best12.);
        run;
        """)
        assert "TO_DATE(" in sql  # input with date format
        assert "TRY_TO_NUMBER(" in sql  # input with numeric format
        assert "TO_VARCHAR(" in sql  # put → TO_VARCHAR


# ── 7. RETAIN → WINDOW FUNCTIONS ───────────────────────────────

class TestRetain:

    def test_retain_running_total(self):
        sql = run_convert("""
        data work.running;
            set work.transactions;
            by account_id;
            retain running_total 0;
            running_total = running_total + amount;
        run;
        """)
        assert "CREATE OR REPLACE TABLE work.running" in sql
        assert "LAG(" in sql  # RETAIN → LAG window function
        assert "running_total" in sql


# ── 8. FIRST./LAST. BY-GROUP PROCESSING ────────────────────────

class TestFirstLast:

    def test_first_and_last_processing(self):
        sql = run_convert("""
        data work.first_last;
            set work.sorted_data;
            by customer_id;
            if first.customer_id then group_seq = 0;
            group_seq + 1;
            if last.customer_id then output;
        run;
        """)
        assert "ROW_NUMBER() OVER (PARTITION BY customer_id" in sql
        assert "_first_flag_" in sql
        assert "_last_flag_" in sql


# ── 9. MULTIPLE DATASETS (UNION ALL) ───────────────────────────

class TestMultipleDatasets:

    def test_set_with_multiple_datasets(self):
        sql = run_convert("""
        data work.combined;
            set work.q1_data work.q2_data work.q3_data work.q4_data;
        run;
        """)
        assert "UNION ALL" in sql
        assert "work.q1_data" in sql
        assert "work.q2_data" in sql
        assert "work.q3_data" in sql
        assert "work.q4_data" in sql


# ── 10. SELECT/WHEN BLOCK ──────────────────────────────────────

class TestSelectWhen:

    def test_select_when_block(self):
        sql = run_convert("""
        data work.output;
            set work.input;
            select (region_code);
                when ('NE') region_name = 'Northeast';
                when ('SE') region_name = 'Southeast';
                when ('MW') region_name = 'Midwest';
                when ('W') region_name = 'West';
                otherwise region_name = 'Unknown';
            end;
        run;
        """)
        assert "CASE" in sql
        assert "region_code = 'NE'" in sql or "'NE'" in sql
        assert "'Northeast'" in sql
        assert "'Southeast'" in sql
        assert "'Midwest'" in sql
        assert "'West'" in sql
        assert "'Unknown'" in sql
        assert "AS region_name" in sql


# ── 11. WHERE CLAUSE ───────────────────────────────────────────

class TestWhereClause:

    def test_where_with_complex_conditions(self):
        sql = run_convert("""
        data work.output;
            set work.input (where=(status in ('ACTIVE', 'PENDING') and amount > 1000));
            where department ne 'INTERNAL';
        run;
        """)
        assert "WHERE" in sql
        assert "IN ('ACTIVE', 'PENDING')" in sql
        assert "amount > 1000" in sql
        assert "department <> 'INTERNAL'" in sql  # ne → <>


# ── 12. COMPLEX REAL-WORLD EXAMPLES ────────────────────────────

class TestComplexExamples:

    def test_full_real_world_data_step(self):
        sql = run_convert("""
        %let report_date = 2024-03-31;
        %let schema = PROD_DW.ANALYTICS;

        data &schema..customer_summary;
            merge &schema..customers (in=a keep=customer_id name email status)
                  &schema..orders (in=b);
            by customer_id;
            if a;

            order_flag = (b = 1);
            full_name = catx(' ', first_name, last_name);
            account_age = intck('month', account_open_date, "&report_date."d);

            if status = 'ACTIVE' and order_flag = 1 then segment = 'Active Buyer';
            else if status = 'ACTIVE' then segment = 'Active Non-Buyer';
            else segment = 'Inactive';

            format account_open_date date9.;
            label segment = 'Customer Segment'
                  account_age = 'Account Age in Months';

            keep customer_id name email status order_flag segment account_age;
        run;
        """, macro_vars={"schema": "PROD_DW.ANALYTICS"})
        assert "PROD_DW.ANALYTICS.customer_summary" in sql
        assert "LEFT OUTER JOIN" in sql
        assert "CONCAT_WS(' '" in sql  # catx
        assert "DATEDIFF('MONTH'" in sql  # intck
        assert "'Active Buyer'" in sql
        assert "'Active Non-Buyer'" in sql
        assert "'Inactive'" in sql
        assert "AS segment" in sql

    def test_lag_function(self):
        sql = run_convert("""
        data work.changes;
            set work.daily_prices;
            by stock_id;
            prev_price = lag(close_price);
            daily_return = (close_price - prev_price) / prev_price;
            keep stock_id trade_date close_price prev_price daily_return;
        run;
        """)
        assert "LAG(close_price, 1) OVER (PARTITION BY stock_id" in sql
        assert "AS prev_price" in sql
        assert "AS daily_return" in sql

    def test_array_processing(self):
        sql = run_convert("""
        data work.output;
            set work.survey;
            array responses{5} q1-q5;
            array flags{5} flag1-flag5;
            do i = 1 to 5;
                if responses{i} > 3 then flags{i} = 1;
                else flags{i} = 0;
            end;
            total_high = sum(flag1, flag2, flag3, flag4, flag5);
            drop i;
        run;
        """)
        assert "CREATE OR REPLACE TABLE work.output" in sql
        assert "EXCLUDE" in sql  # drop i
        assert "AS total_high" in sql


# ── 13. DATASET OPTIONS ────────────────────────────────────────

class TestDatasetOptions:

    def test_set_with_dataset_options(self):
        sql = run_convert("""
        data work.output;
            set work.large_table (where=(year >= 2023) keep=id name year amount);
            new_amount = amount * 1.1;
        run;
        """)
        assert "WHERE" in sql
        assert "year >= 2023" in sql
        assert "(amount * 1.1) AS new_amount" in sql


# ── 14. COMPLEX ARRAY + DO LOOP + UNPIVOT ──────────────────────

class TestArrayUnpivot:

    def test_array_do_loop_with_output_unpivot(self):
        sql = run_convert("""
        data work.premium_analysis;
            set work.raw_insurance_data;
            array monthly_prems[12] month1-month12;
            retain ytd_total 0;
            do month_idx = 1 to 12;
                current_premium = monthly_prems[month_idx];
                if not missing(current_premium) then do;
                    if current_premium > 500 then do;
                        surcharge_flag = "Yes";
                        current_premium = current_premium * 1.05;
                    end;
                    else do;
                        surcharge_flag = "No ";
                    end;
                    ytd_total + current_premium;
                end;
                output;
            end;
            drop month1-month12 month_idx;
            format current_premium ytd_total dollar10.2;
        run;
        """)
        assert "UNPIVOT INCLUDE NULLS" in sql, "Should use UNPIVOT INCLUDE NULLS, not UNION ALL"
        assert "month1, month2, month3" in sql, "Should unpivot all 12 month columns"
        assert "PARTITION BY _row_id_" in sql, "Running total must be partitioned by row"
        assert "SUM(" in sql, "ytd_total should use SUM window function"
        assert "surcharge_flag" in sql
        assert "current_premium" in sql
        assert "ROW_NUMBER()" in sql, "Need ROW_NUMBER for _row_id_ before UNPIVOT"


# ── 15. SUBSETTING IF ON COMPUTED COLUMN (QUALIFY) ──────────────

class TestQualify:

    def test_subsetting_if_on_computed_column(self):
        sql = run_convert("""
        data work.test_success;
            set work.raw_customers;
            if age >= 18 then age_group = 'Adult';
            else age_group = 'Minor';
            if age_group = 'Adult';
        run;
        """)
        assert "QUALIFY" in sql, "Computed column filter should use QUALIFY, not WHERE"
        assert "age_group = 'Adult'" in sql
        assert "CASE WHEN (age >= 18) THEN 'Adult' ELSE 'Minor' END AS age_group" in sql
        assert "WHERE" not in sql, "Should use QUALIFY instead of WHERE for computed columns"

    def test_multiple_computed_columns_qualify(self):
        sql = run_convert("""
        data work.filtered_customers;
            set work.raw_customers;
            if age >= 18 then age_group = 'Adult';
            else age_group = 'Minor';
            if total_spent > 1000 then loyalty_tier = 'Gold';
            else loyalty_tier = 'Silver';
            if age_group = 'Adult' and loyalty_tier = 'Gold';
        run;
        """)
        assert "QUALIFY" in sql, "Should use QUALIFY for filtering on computed columns"
        assert "age_group = 'Adult'" in sql
        assert "loyalty_tier = 'Gold'" in sql
        assert "CASE WHEN (age >= 18)" in sql
        assert "CASE WHEN (total_spent > 1000)" in sql
