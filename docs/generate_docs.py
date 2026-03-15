"""
Generate PDF documentation for SAS to Snowflake Converter.
Run: python docs/generate_docs.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fpdf import FPDF
from sas_to_snowflake import SASToSnowflakeConverter


class ConverterDoc(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 8, "SAS to Snowflake Converter - Feature Reference Guide", align="C")
            self.ln(4)
            self.set_draw_color(200, 200, 200)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def cover_page(self):
        self.add_page()
        self.ln(50)
        # Title
        self.set_font("Helvetica", "B", 32)
        self.set_text_color(25, 60, 120)
        self.cell(0, 15, "SAS to Snowflake", align="C")
        self.ln(14)
        self.cell(0, 15, "SQL Converter", align="C")
        self.ln(20)
        # Subtitle
        self.set_font("Helvetica", "", 16)
        self.set_text_color(80, 80, 80)
        self.cell(0, 10, "Feature Reference Guide", align="C")
        self.ln(15)
        # Line
        self.set_draw_color(25, 60, 120)
        self.set_line_width(0.8)
        self.line(60, self.get_y(), 150, self.get_y())
        self.ln(15)
        # Description
        self.set_font("Helvetica", "", 11)
        self.set_text_color(100, 100, 100)
        self.multi_cell(0, 6,
            "A comprehensive guide to all supported SAS DATA step patterns\n"
            "and their Snowflake SQL equivalents.\n\n"
            "28 test cases across 15 feature categories.",
            align="C")
        self.ln(30)
        self.set_font("Helvetica", "I", 10)
        self.set_text_color(150, 150, 150)
        self.cell(0, 8, "github.com/linuxra/sas_2_snowflake", align="C")

    def section_title(self, number, title):
        self.ln(6)
        # Section number badge
        self.set_fill_color(25, 60, 120)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 12)
        badge = f"  {number}  "
        badge_w = self.get_string_width(badge) + 4
        self.cell(badge_w, 8, badge, fill=True)
        # Section title
        self.set_text_color(25, 60, 120)
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, f"  {title}")
        self.ln(10)
        self.set_draw_color(25, 60, 120)
        self.set_line_width(0.4)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def test_title(self, name):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(60, 60, 60)
        self.set_fill_color(240, 244, 250)
        self.cell(0, 7, f"  {name}", fill=True)
        self.ln(8)

    def code_block(self, label, code, bg_color):
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, label)
        self.ln(5)

        self.set_fill_color(*bg_color)
        self.set_font("Courier", "", 7.5)
        self.set_text_color(30, 30, 30)

        lines = code.strip().split("\n")
        x_start = self.get_x()
        y_start = self.get_y()
        block_height = len(lines) * 4 + 4

        # Check if we need a new page
        if self.get_y() + block_height > 270:
            self.add_page()
            y_start = self.get_y()

        self.rect(10, y_start, 190, block_height, "F")
        self.set_y(y_start + 2)

        for line in lines:
            self.set_x(14)
            # Truncate very long lines
            if len(line) > 110:
                line = line[:107] + "..."
            self.cell(0, 4, line)
            self.ln(4)
        self.ln(3)

    def assertions_block(self, assertions):
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, "ASSERTIONS (what CI checks):")
        self.ln(5)

        for a in assertions:
            check, desc = a
            if self.get_y() > 270:
                self.add_page()
            self.set_x(14)
            self.set_font("Courier", "", 7)
            self.set_text_color(0, 130, 60)
            self.cell(4, 4, "[OK]")
            self.set_text_color(50, 50, 50)
            self.set_font("Courier", "", 7)
            self.cell(0, 4, f" {check}")
            self.ln(4)
            if desc:
                self.set_x(20)
                self.set_font("Helvetica", "I", 7)
                self.set_text_color(130, 130, 130)
                self.cell(0, 3.5, desc)
                self.ln(4)
        self.ln(4)

    def toc_page(self, sections):
        self.add_page()
        self.set_font("Helvetica", "B", 20)
        self.set_text_color(25, 60, 120)
        self.cell(0, 12, "Table of Contents")
        self.ln(12)
        self.set_draw_color(25, 60, 120)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(8)

        for num, title, test_count in sections:
            self.set_font("Helvetica", "B", 11)
            self.set_text_color(25, 60, 120)
            self.cell(12, 7, f"{num}.")
            self.set_text_color(40, 40, 40)
            self.set_font("Helvetica", "", 11)
            self.cell(140, 7, title)
            self.set_font("Helvetica", "I", 9)
            self.set_text_color(130, 130, 130)
            self.cell(0, 7, f"{test_count} test(s)", align="R")
            self.ln(8)


def convert(sas_code, macro_vars=None):
    converter = SASToSnowflakeConverter(macro_vars=macro_vars)
    return converter.convert(sas_code).sql


# ─── Define all test cases ──────────────────────────────────────

test_cases = [
    {
        "section_num": 1,
        "section": "Basic SET with KEEP / DROP / RENAME",
        "tests": [
            {
                "name": "Simple SET with KEEP",
                "sas": """data work.output;
    set work.input;
    keep id name salary department;
run;""",
                "assertions": [
                    ("CREATE OR REPLACE TABLE work.output", "Creates target table"),
                    ("inp.id, inp.name, inp.salary, inp.department", "Selects only KEEP columns"),
                    ("No EXCLUDE clause", "KEEP uses explicit column list"),
                ],
            },
            {
                "name": "SET with DROP",
                "sas": """data work.output;
    set work.input;
    drop temp_var debug_flag;
run;""",
                "assertions": [
                    ("EXCLUDE (temp_var, debug_flag)", "DROP maps to Snowflake EXCLUDE"),
                ],
            },
            {
                "name": "SET with RENAME",
                "sas": """data work.output;
    set work.input;
    rename old_name = new_name old_id = new_id;
run;""",
                "assertions": [
                    ("RENAME (old_name AS new_name, old_id AS new_id)", "RENAME maps to Snowflake RENAME"),
                ],
            },
        ],
    },
    {
        "section_num": 2,
        "section": "Computed Columns / Assignments",
        "tests": [
            {
                "name": "Assignments with expressions",
                "sas": """data work.output;
    set work.employees;
    annual_salary = monthly_salary * 12;
    full_name = cats(first_name, ' ', last_name);
    age = intck('year', birth_date, today());
    tax = round(annual_salary * 0.22, 2);
run;""",
                "assertions": [
                    ("(monthly_salary * 12) AS annual_salary", "Arithmetic expressions preserved"),
                    ("CONCAT(...) AS full_name", "cats() -> CONCAT with TRIM/CAST"),
                    ("DATEDIFF('YEAR', ...) AS age", "intck() -> DATEDIFF"),
                    ("ROUND(...) AS tax", "round() preserved"),
                    ("CURRENT_DATE()", "today() -> CURRENT_DATE()"),
                ],
            },
        ],
    },
    {
        "section_num": 3,
        "section": "IF / THEN / ELSE -> CASE WHEN",
        "tests": [
            {
                "name": "Simple IF/THEN/ELSE chain",
                "sas": """data work.output;
    set work.input;
    if score >= 90 then grade = 'A';
    else if score >= 80 then grade = 'B';
    else if score >= 70 then grade = 'C';
    else grade = 'F';
run;""",
                "assertions": [
                    ("CASE WHEN (score >= 90) THEN 'A'", "First condition"),
                    ("WHEN (score >= 80) THEN 'B'", "ELSE IF -> WHEN"),
                    ("ELSE 'F' END AS grade", "ELSE clause with alias"),
                ],
            },
            {
                "name": "Subsetting IF (row filter)",
                "sas": """data work.active_employees;
    set work.employees;
    if status = 'ACTIVE';
run;""",
                "assertions": [
                    ("WHERE (status = 'ACTIVE')", "Subsetting IF -> WHERE clause"),
                ],
            },
            {
                "name": "IF with DELETE",
                "sas": """data work.output;
    set work.input;
    if age < 18 then delete;
run;""",
                "assertions": [
                    ("WHERE NOT (age < 18)", "IF...DELETE -> WHERE NOT"),
                ],
            },
        ],
    },
    {
        "section_num": 4,
        "section": "MERGE with IN= Variables (Joins)",
        "tests": [
            {
                "name": "Inner join (IF a AND b)",
                "sas": """data work.matched;
    merge work.customers (in=a) work.orders (in=b);
    by customer_id;
    if a and b;
run;""",
                "assertions": [
                    ("INNER JOIN", "IF a AND b -> INNER JOIN"),
                    ("ON a.customer_id = b.customer_id", "BY -> ON clause"),
                ],
            },
            {
                "name": "Left join (IF a)",
                "sas": """data work.all_customers;
    merge work.customers (in=a) work.orders (in=b);
    by customer_id;
    if a;
run;""",
                "assertions": [
                    ("LEFT OUTER JOIN", "IF a -> LEFT OUTER JOIN"),
                    ("ON a.customer_id = b.customer_id", "BY -> ON clause"),
                ],
            },
            {
                "name": "Full outer join with computed columns",
                "sas": """data work.combined;
    merge work.table1 (in=a) work.table2 (in=b);
    by id;
    if a and b then match_flag = 'BOTH';
    else if a then match_flag = 'LEFT_ONLY';
    else match_flag = 'RIGHT_ONLY';
run;""",
                "assertions": [
                    ("CASE WHEN ... 'BOTH' ... 'LEFT_ONLY' ... 'RIGHT_ONLY'", "IF/ELSE on IN= -> CASE WHEN"),
                    ("AS match_flag", "Computed column from join logic"),
                ],
            },
        ],
    },
    {
        "section_num": 5,
        "section": "Macro Variable Substitution",
        "tests": [
            {
                "name": "Inline %LET macro variables",
                "sas": """%let input_lib = PROD_DB.RAW_SCHEMA;
%let output_lib = PROD_DB.ANALYTICS;
%let cutoff_date = 2024-01-01;

data &output_lib..summary;
    set &input_lib..transactions;
    where transaction_date >= "&cutoff_date."d;
    keep customer_id amount transaction_date;
run;""",
                "assertions": [
                    ("PROD_DB.ANALYTICS.summary", "Output macro resolved"),
                    ("PROD_DB.RAW_SCHEMA.transactions", "Input macro resolved"),
                    ("TO_DATE('2024-01-01')", "Date literal converted"),
                    ("No &output_lib or &input_lib in output", "All macros resolved"),
                ],
            },
            {
                "name": "External macro vars passed at runtime",
                "sas": """data &mylib..output;
    set &mylib..input;
    if region = "&region." then flag = 1;
    else flag = 0;
run;""",
                "assertions": [
                    ("DW.SCHEMA1.output / DW.SCHEMA1.input", "External macro resolved"),
                    ("'WEST'", "Macro in string literal resolved"),
                ],
                "macro_vars": {"mylib": "DW.SCHEMA1", "region": "WEST"},
            },
        ],
    },
    {
        "section_num": 6,
        "section": "SAS Functions -> Snowflake Functions",
        "tests": [
            {
                "name": "String functions",
                "sas": """data work.output;
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
run;""",
                "assertions": [
                    ("UPPER(name)", "upcase -> UPPER"),
                    ("LOWER(name)", "lowcase -> LOWER"),
                    ("INITCAP(name)", "propcase -> INITCAP"),
                    ("LENGTH(name)", "length -> LENGTH"),
                    ("SUBSTR(name, 1, 3)", "substr -> SUBSTR"),
                    ("SPLIT_PART(...)", "scan -> SPLIT_PART"),
                    ("REPLACE(...)", "compress/tranwrd -> REPLACE"),
                    ("POSITION('find_me' IN text)", "index -> POSITION"),
                    ("CONCAT_WS('-', ...)", "catx -> CONCAT_WS"),
                ],
            },
            {
                "name": "Date functions",
                "sas": """data work.output;
    set work.input;
    current_dt = today();
    yr = year(hire_date);
    mo = month(hire_date);
    dy = day(hire_date);
    tenure_years = intck('year', hire_date, today());
    next_review = intnx('month', hire_date, 6);
    created = mdy(1, 15, 2024);
    qtr = qtr(report_date);
run;""",
                "assertions": [
                    ("CURRENT_DATE()", "today() -> CURRENT_DATE()"),
                    ("YEAR / MONTH / DAY", "Direct mappings"),
                    ("DATEDIFF('YEAR', ...)", "intck -> DATEDIFF"),
                    ("DATEADD('MONTH', 6, ...)", "intnx -> DATEADD"),
                    ("DATE_FROM_PARTS(2024, 1, 15)", "mdy -> DATE_FROM_PARTS"),
                    ("QUARTER(report_date)", "qtr -> QUARTER"),
                ],
            },
            {
                "name": "Numeric and missing-value functions",
                "sas": """data work.output;
    set work.input;
    total = sum(val1, val2, val3);
    avg_val = mean(val1, val2, val3);
    min_val = min(val1, val2, val3);
    max_val = max(val1, val2, val3);
    abs_val = abs(difference);
    rounded = round(amount, 0.01);
    null_count = nmiss(val1, val2, val3);
    first_valid = coalesce(val1, val2, val3);
run;""",
                "assertions": [
                    ("COALESCE(val1, 0) + ...", "SAS sum() handles NULLs via COALESCE"),
                    ("LEAST(...)", "min -> LEAST"),
                    ("GREATEST(...)", "max -> GREATEST"),
                    ("ABS(difference)", "abs -> ABS"),
                    ("ROUND(amount, 0.01)", "round -> ROUND"),
                    ("IS NULL THEN 1 ...", "nmiss -> NULL counting"),
                    ("COALESCE(val1, val2, val3)", "coalesce -> COALESCE"),
                ],
            },
            {
                "name": "INPUT / PUT type conversions",
                "sas": """data work.output;
    set work.input;
    date_val = input(date_str, date9.);
    num_val = input(num_str, best12.);
    char_val = put(num_val, best12.);
run;""",
                "assertions": [
                    ("TO_DATE(...)", "INPUT with date format -> TO_DATE"),
                    ("TRY_TO_NUMBER(...)", "INPUT with numeric format -> TRY_TO_NUMBER"),
                    ("TO_VARCHAR(...)", "PUT -> TO_VARCHAR"),
                ],
            },
        ],
    },
    {
        "section_num": 7,
        "section": "RETAIN -> Window Functions",
        "tests": [
            {
                "name": "RETAIN for running total",
                "sas": """data work.running;
    set work.transactions;
    by account_id;
    retain running_total 0;
    running_total = running_total + amount;
run;""",
                "assertions": [
                    ("LAG(...) OVER (...)", "RETAIN -> LAG window function"),
                    ("running_total", "Running total column preserved"),
                ],
            },
        ],
    },
    {
        "section_num": 8,
        "section": "FIRST. / LAST. By-Group Processing",
        "tests": [
            {
                "name": "FIRST and LAST flags",
                "sas": """data work.first_last;
    set work.sorted_data;
    by customer_id;
    if first.customer_id then group_seq = 0;
    group_seq + 1;
    if last.customer_id then output;
run;""",
                "assertions": [
                    ("ROW_NUMBER() OVER (PARTITION BY customer_id ...)", "FIRST./LAST. -> ROW_NUMBER"),
                    ("_first_flag_ / _last_flag_", "Helper flags generated"),
                ],
            },
        ],
    },
    {
        "section_num": 9,
        "section": "Multiple Datasets (UNION ALL)",
        "tests": [
            {
                "name": "SET with multiple datasets",
                "sas": """data work.combined;
    set work.q1_data work.q2_data work.q3_data work.q4_data;
run;""",
                "assertions": [
                    ("UNION ALL", "Multiple SET -> UNION ALL"),
                    ("All 4 tables referenced", "Each source table included"),
                ],
            },
        ],
    },
    {
        "section_num": 10,
        "section": "SELECT / WHEN Block",
        "tests": [
            {
                "name": "SELECT WHEN -> CASE",
                "sas": """data work.output;
    set work.input;
    select (region_code);
        when ('NE') region_name = 'Northeast';
        when ('SE') region_name = 'Southeast';
        when ('MW') region_name = 'Midwest';
        when ('W') region_name = 'West';
        otherwise region_name = 'Unknown';
    end;
run;""",
                "assertions": [
                    ("CASE WHEN region_code = 'NE' THEN 'Northeast'", "SELECT/WHEN -> CASE WHEN"),
                    ("ELSE 'Unknown' END AS region_name", "OTHERWISE -> ELSE"),
                ],
            },
        ],
    },
    {
        "section_num": 11,
        "section": "WHERE Clause",
        "tests": [
            {
                "name": "WHERE with complex conditions",
                "sas": """data work.output;
    set work.input (where=(status in ('ACTIVE', 'PENDING') and amount > 1000));
    where department ne 'INTERNAL';
run;""",
                "assertions": [
                    ("IN ('ACTIVE', 'PENDING')", "IN operator preserved"),
                    ("amount > 1000", "Numeric comparison preserved"),
                    ("department <> 'INTERNAL'", "ne -> <>"),
                ],
            },
        ],
    },
    {
        "section_num": 12,
        "section": "Complex Real-World Examples",
        "tests": [
            {
                "name": "Full real-world data step (merge + macros + computed + labels)",
                "sas": """%let report_date = 2024-03-31;
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
    keep customer_id name email status order_flag segment account_age;
run;""",
                "assertions": [
                    ("PROD_DW.ANALYTICS.customer_summary", "Macro-resolved output table"),
                    ("LEFT OUTER JOIN", "IF a -> LEFT JOIN"),
                    ("CONCAT_WS / DATEDIFF", "Function conversions"),
                    ("'Active Buyer' / 'Inactive'", "CASE WHEN for segments"),
                ],
                "macro_vars": {"schema": "PROD_DW.ANALYTICS"},
            },
            {
                "name": "LAG function for daily returns",
                "sas": """data work.changes;
    set work.daily_prices;
    by stock_id;
    prev_price = lag(close_price);
    daily_return = (close_price - prev_price) / prev_price;
    keep stock_id trade_date close_price prev_price daily_return;
run;""",
                "assertions": [
                    ("LAG(close_price, 1) OVER (PARTITION BY stock_id ...)", "LAG with PARTITION BY"),
                    ("AS prev_price / AS daily_return", "Computed columns from LAG"),
                ],
            },
            {
                "name": "Array processing with DO loop",
                "sas": """data work.output;
    set work.survey;
    array responses{5} q1-q5;
    array flags{5} flag1-flag5;
    do i = 1 to 5;
        if responses{i} > 3 then flags{i} = 1;
        else flags{i} = 0;
    end;
    total_high = sum(flag1, flag2, flag3, flag4, flag5);
    drop i;
run;""",
                "assertions": [
                    ("EXCLUDE (i)", "DROP i -> EXCLUDE"),
                    ("AS total_high", "Sum of array elements"),
                ],
            },
        ],
    },
    {
        "section_num": 13,
        "section": "Dataset Options",
        "tests": [
            {
                "name": "SET with WHERE and KEEP options",
                "sas": """data work.output;
    set work.large_table (where=(year >= 2023) keep=id name year amount);
    new_amount = amount * 1.1;
run;""",
                "assertions": [
                    ("WHERE (year >= 2023)", "Dataset WHERE option -> WHERE clause"),
                    ("(amount * 1.1) AS new_amount", "Computed column after filter"),
                ],
            },
        ],
    },
    {
        "section_num": 14,
        "section": "Complex Array + DO Loop + UNPIVOT",
        "tests": [
            {
                "name": "Array DO-loop with OUTPUT (wide-to-long)",
                "sas": """data work.premium_analysis;
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
                surcharge_flag = "No";
            end;
            ytd_total + current_premium;
        end;
        output;
    end;
    drop month1-month12 month_idx;
run;""",
                "assertions": [
                    ("UNPIVOT INCLUDE NULLS", "Uses native Snowflake UNPIVOT (not UNION ALL)"),
                    ("month1, month2, ... month12", "All 12 columns unpivoted"),
                    ("PARTITION BY _row_id_", "Running total scoped per source row"),
                    ("SUM(...) OVER (...)", "ytd_total via SUM window function"),
                    ("ROW_NUMBER() before UNPIVOT", "_row_id_ assigned before unpivot"),
                    ("CASE WHEN for surcharge_flag", "Nested IF/THEN -> nested CASE"),
                ],
            },
        ],
    },
    {
        "section_num": 15,
        "section": "Subsetting IF on Computed Column (QUALIFY)",
        "tests": [
            {
                "name": "Single computed column filter",
                "sas": """data work.test_success;
    set work.raw_customers;
    if age >= 18 then age_group = 'Adult';
    else age_group = 'Minor';
    if age_group = 'Adult';
run;""",
                "assertions": [
                    ("QUALIFY (age_group = 'Adult')", "Uses QUALIFY, not WHERE"),
                    ("CASE WHEN ... AS age_group", "Computed column in SELECT"),
                    ("No WHERE clause", "QUALIFY replaces WHERE for computed columns"),
                ],
            },
            {
                "name": "Multiple computed columns filter",
                "sas": """data work.filtered_customers;
    set work.raw_customers;
    if age >= 18 then age_group = 'Adult';
    else age_group = 'Minor';
    if total_spent > 1000 then loyalty_tier = 'Gold';
    else loyalty_tier = 'Silver';
    if age_group = 'Adult' and loyalty_tier = 'Gold';
run;""",
                "assertions": [
                    ("QUALIFY ... age_group = 'Adult' AND loyalty_tier = 'Gold'", "Multiple conditions in QUALIFY"),
                    ("Two CASE WHEN expressions", "Both computed columns in SELECT"),
                ],
            },
        ],
    },
]


def main():
    pdf = ConverterDoc()
    pdf.alias_nb_pages()

    # Cover page
    pdf.cover_page()

    # Table of Contents
    sections_toc = []
    for tc in test_cases:
        sections_toc.append((tc["section_num"], tc["section"], len(tc["tests"])))
    pdf.toc_page(sections_toc)

    # Feature pages
    for tc in test_cases:
        pdf.add_page()
        pdf.section_title(tc["section_num"], tc["section"])

        for test in tc["tests"]:
            # Check space
            if pdf.get_y() > 220:
                pdf.add_page()

            pdf.test_title(test["name"])

            # SAS input
            pdf.code_block("SAS INPUT:", test["sas"], (255, 248, 240))

            # Convert and show SQL
            macro_vars = test.get("macro_vars")
            sql = convert(test["sas"], macro_vars=macro_vars)
            pdf.code_block("SNOWFLAKE SQL OUTPUT:", sql, (240, 248, 255))

            # Assertions
            pdf.assertions_block(test["assertions"])

    # Summary page
    pdf.add_page()
    pdf.ln(10)
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(25, 60, 120)
    pdf.cell(0, 12, "Summary", align="C")
    pdf.ln(15)

    total_tests = sum(len(tc["tests"]) for tc in test_cases)
    total_sections = len(test_cases)

    stats = [
        ("Feature Categories", str(total_sections)),
        ("Total Test Cases", str(total_tests)),
        ("CI Framework", "pytest with assertions"),
        ("CI Trigger", "Push to main / Pull requests"),
    ]

    for label, value in stats:
        pdf.set_font("Helvetica", "", 12)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(90, 8, label, align="R")
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(25, 60, 120)
        pdf.cell(90, 8, f"  {value}")
        pdf.ln(10)

    pdf.ln(15)
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 8, "Generated from live converter output", align="C")

    # Save
    output_path = os.path.join(os.path.dirname(__file__), "SAS_to_Snowflake_Feature_Guide.pdf")
    pdf.output(output_path)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    main()
