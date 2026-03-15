"""
Test Suite for SAS to Snowflake Converter
==========================================
Tests various SAS DATA step patterns including:
- Basic SET and column selection
- MERGE with IN= variables
- IF/THEN/ELSE → CASE WHEN
- DO loops
- RETAIN → LAG window functions
- FIRST./LAST. by-group processing
- ARRAY → CASE expressions
- Macro variable substitution
- SAS function → Snowflake function conversion
- FORMAT/LABEL/LENGTH
- Complex real-world examples
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sas_to_snowflake import convert, SASToSnowflakeConverter


def banner(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def test_case(name, sas_code, macro_vars=None, show_warnings=True):
    print(f"\n--- {name} ---")
    print(f"SAS Input:")
    for line in sas_code.strip().split('\n'):
        print(f"  {line}")
    print()

    converter = SASToSnowflakeConverter(macro_vars=macro_vars)
    result = converter.convert(sas_code)

    print(f"Snowflake SQL Output:")
    for line in result.sql.strip().split('\n'):
        print(f"  {line}")

    if show_warnings and result.warnings:
        print(f"\n  Warnings:")
        for w in result.warnings:
            print(f"    - {w}")
    print()
    return result


# ═══════════════════════════════════════════════════════════════
banner("1. BASIC SET WITH KEEP/DROP/RENAME")
# ═══════════════════════════════════════════════════════════════

test_case("Simple SET with KEEP", """
data work.output;
    set work.input;
    keep id name salary department;
run;
""")

test_case("SET with DROP", """
data work.output;
    set work.input;
    drop temp_var debug_flag;
run;
""")

test_case("SET with RENAME", """
data work.output;
    set work.input;
    rename old_name = new_name old_id = new_id;
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("2. COMPUTED COLUMNS / ASSIGNMENTS")
# ═══════════════════════════════════════════════════════════════

test_case("Assignments with expressions", """
data work.output;
    set work.employees;
    annual_salary = monthly_salary * 12;
    full_name = cats(first_name, ' ', last_name);
    age = intck('year', birth_date, today());
    tax = round(annual_salary * 0.22, 2);
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("3. IF/THEN/ELSE → CASE WHEN")
# ═══════════════════════════════════════════════════════════════

test_case("Simple IF/THEN/ELSE", """
data work.output;
    set work.input;
    if score >= 90 then grade = 'A';
    else if score >= 80 then grade = 'B';
    else if score >= 70 then grade = 'C';
    else grade = 'F';
run;
""")

test_case("Subsetting IF (filter)", """
data work.active_employees;
    set work.employees;
    if status = 'ACTIVE';
run;
""")

test_case("IF with DELETE", """
data work.output;
    set work.input;
    if age < 18 then delete;
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("4. MERGE WITH IN= VARIABLES")
# ═══════════════════════════════════════════════════════════════

test_case("Inner join (MERGE with IF a AND b)", """
data work.matched;
    merge work.customers (in=a) work.orders (in=b);
    by customer_id;
    if a and b;
run;
""")

test_case("Left join (MERGE with IF a)", """
data work.all_customers;
    merge work.customers (in=a) work.orders (in=b);
    by customer_id;
    if a;
run;
""")

test_case("Full outer join with computed cols", """
data work.combined;
    merge work.table1 (in=a) work.table2 (in=b);
    by id;
    if a and b then match_flag = 'BOTH';
    else if a then match_flag = 'LEFT_ONLY';
    else match_flag = 'RIGHT_ONLY';
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("5. MACRO VARIABLES")
# ═══════════════════════════════════════════════════════════════

test_case("Macro variables in data step", """
%let input_lib = PROD_DB.RAW_SCHEMA;
%let output_lib = PROD_DB.ANALYTICS;
%let cutoff_date = 2024-01-01;

data &output_lib..summary;
    set &input_lib..transactions;
    where transaction_date >= "&cutoff_date."d;
    keep customer_id amount transaction_date;
run;
""")

test_case("Macro vars with external values", """
data &mylib..output;
    set &mylib..input;
    if region = "&region." then flag = 1;
    else flag = 0;
run;
""", macro_vars={"mylib": "DW.SCHEMA1", "region": "WEST"})

# ═══════════════════════════════════════════════════════════════
banner("6. SAS FUNCTIONS → SNOWFLAKE FUNCTIONS")
# ═══════════════════════════════════════════════════════════════

test_case("String functions", """
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

test_case("Date functions", """
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

test_case("Numeric and missing functions", """
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

test_case("INPUT/PUT conversions", """
data work.output;
    set work.input;
    date_val = input(date_str, date9.);
    num_val = input(num_str, best12.);
    date_str = put(date_val, mmddyy10.);
    char_val = put(num_val, best12.);
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("7. RETAIN → WINDOW FUNCTIONS")
# ═══════════════════════════════════════════════════════════════

test_case("RETAIN for running total", """
data work.running;
    set work.transactions;
    by account_id;
    retain running_total 0;
    running_total = running_total + amount;
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("8. FIRST./LAST. BY-GROUP PROCESSING")
# ═══════════════════════════════════════════════════════════════

test_case("FIRST and LAST processing", """
data work.first_last;
    set work.sorted_data;
    by customer_id;
    if first.customer_id then group_seq = 0;
    group_seq + 1;
    if last.customer_id then output;
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("9. MULTIPLE DATASETS (UNION ALL)")
# ═══════════════════════════════════════════════════════════════

test_case("SET with multiple datasets", """
data work.combined;
    set work.q1_data work.q2_data work.q3_data work.q4_data;
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("10. SELECT/WHEN BLOCK")
# ═══════════════════════════════════════════════════════════════

test_case("SELECT WHEN block", """
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

# ═══════════════════════════════════════════════════════════════
banner("11. WHERE CLAUSE")
# ═══════════════════════════════════════════════════════════════

test_case("WHERE with complex conditions", """
data work.output;
    set work.input (where=(status in ('ACTIVE', 'PENDING') and amount > 1000));
    where department ne 'INTERNAL';
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("12. COMPLEX REAL-WORLD EXAMPLE")
# ═══════════════════════════════════════════════════════════════

test_case("Full real-world data step", """
%let report_date = 2024-03-31;
%let schema = PROD_DW.ANALYTICS;

data &schema..customer_summary;
    merge &schema..customers (in=a keep=customer_id name email status)
          &schema..orders (in=b);
    by customer_id;
    if a;

    /* Computed columns */
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

test_case("LAG function usage", """
data work.changes;
    set work.daily_prices;
    by stock_id;
    prev_price = lag(close_price);
    daily_return = (close_price - prev_price) / prev_price;
    keep stock_id trade_date close_price prev_price daily_return;
run;
""")

test_case("Array processing", """
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

# ═══════════════════════════════════════════════════════════════
banner("13. DATASET OPTIONS")
# ═══════════════════════════════════════════════════════════════

test_case("SET with dataset options", """
data work.output;
    set work.large_table (where=(year >= 2023) keep=id name year amount);
    new_amount = amount * 1.1;
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("14. COMPLEX ARRAY + DO LOOP + UNPIVOT")
# ═══════════════════════════════════════════════════════════════

test_case("Array DO-loop with OUTPUT (wide-to-long unpivot)", """
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

# ═══════════════════════════════════════════════════════════════
banner("15. SUBSETTING IF ON COMPUTED COLUMN (QUALIFY)")
# ═══════════════════════════════════════════════════════════════

test_case("Subsetting IF on computed column uses QUALIFY", """
data work.test_success;
    set work.raw_customers;
    if age >= 18 then age_group = 'Adult';
    else age_group = 'Minor';
    if age_group = 'Adult';
run;
""")

# ═══════════════════════════════════════════════════════════════
banner("ALL TESTS COMPLETE")
# ═══════════════════════════════════════════════════════════════

print("\nAll test cases executed successfully!")
print("Review the output above to verify correctness of each conversion.")
