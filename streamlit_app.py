"""
SAS to Snowflake SQL Converter - Streamlit App
Deploy on Streamlit Cloud: https://streamlit.io/cloud
"""

import streamlit as st
from sas_to_snowflake import SASToSnowflakeConverter

st.set_page_config(
    page_title="SAS to Snowflake Converter",
    page_icon="&#x2744;",
    layout="wide",
)

# ── Custom styling ──────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .block-container { padding-top: 2rem; }
    h1 { text-align: center; }
    .stTextArea textarea {
        font-family: 'Courier New', monospace;
        font-size: 14px;
    }
    .stCodeBlock { font-size: 14px; }
    div[data-testid="stAlert"] { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# ── Header ──────────────────────────────────────────────────────
st.title("SAS to Snowflake SQL Converter")
st.caption("Paste SAS DATA step code and get optimized Snowflake SQL")

# ── Sidebar with examples and macro vars ────────────────────────
with st.sidebar:
    st.header("Settings")

    st.subheader("Macro Variables")
    st.caption("Add SAS macro variables (optional)")
    macro_text = st.text_area(
        "One per line: name=value",
        placeholder="mylib=PROD_DW.ANALYTICS\nregion=WEST",
        height=100,
        label_visibility="collapsed",
    )

    macro_vars = {}
    if macro_text.strip():
        for line in macro_text.strip().split("\n"):
            if "=" in line:
                key, val = line.split("=", 1)
                macro_vars[key.strip()] = val.strip()

    st.divider()
    st.subheader("Example SAS Code")
    example = st.selectbox("Load an example", [
        "-- Select --",
        "Simple KEEP",
        "IF/THEN/ELSE",
        "MERGE (Inner Join)",
        "MERGE (Left Join)",
        "Macro Variables",
        "String Functions",
        "Date Functions",
        "UNPIVOT (Array + DO Loop)",
        "QUALIFY (Computed Filter)",
        "Real-World (Full Example)",
    ])

    examples = {
        "Simple KEEP": """data work.output;
    set work.input;
    keep id name salary department;
run;""",
        "IF/THEN/ELSE": """data work.output;
    set work.input;
    if score >= 90 then grade = 'A';
    else if score >= 80 then grade = 'B';
    else if score >= 70 then grade = 'C';
    else grade = 'F';
run;""",
        "MERGE (Inner Join)": """data work.matched;
    merge work.customers (in=a) work.orders (in=b);
    by customer_id;
    if a and b;
run;""",
        "MERGE (Left Join)": """data work.all_customers;
    merge work.customers (in=a) work.orders (in=b);
    by customer_id;
    if a;
run;""",
        "Macro Variables": """%let schema = PROD_DW.ANALYTICS;
%let cutoff_date = 2024-01-01;

data &schema..summary;
    set &schema..transactions;
    where transaction_date >= "&cutoff_date."d;
    keep customer_id amount transaction_date;
run;""",
        "String Functions": """data work.output;
    set work.input;
    upper_name = upcase(name);
    lower_name = lowcase(name);
    name_len = length(name);
    first3 = substr(name, 1, 3);
    combined = catx('-', part1, part2, part3);
run;""",
        "Date Functions": """data work.output;
    set work.input;
    current_dt = today();
    yr = year(hire_date);
    tenure_years = intck('year', hire_date, today());
    next_review = intnx('month', hire_date, 6);
run;""",
        "UNPIVOT (Array + DO Loop)": """data work.premium_analysis;
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
        "QUALIFY (Computed Filter)": """data work.filtered_customers;
    set work.raw_customers;
    if age >= 18 then age_group = 'Adult';
    else age_group = 'Minor';
    if total_spent > 1000 then loyalty_tier = 'Gold';
    else loyalty_tier = 'Silver';
    if age_group = 'Adult' and loyalty_tier = 'Gold';
run;""",
        "Real-World (Full Example)": """%let report_date = 2024-03-31;
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
    }

# ── Main content ────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("SAS Input")
    default_code = examples.get(example, "") if example != "-- Select --" else ""
    sas_code = st.text_area(
        "Paste your SAS DATA step code here",
        value=default_code,
        height=400,
        label_visibility="collapsed",
        placeholder="data work.output;\n    set work.input;\n    ...\nrun;",
    )

with col2:
    st.subheader("Snowflake SQL Output")
    sql_placeholder = st.empty()

# ── Convert button ──────────────────────────────────────────────
col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 1])
with col_btn2:
    convert_clicked = st.button("Convert", type="primary", use_container_width=True)

if convert_clicked and sas_code.strip():
    try:
        converter = SASToSnowflakeConverter(macro_vars=macro_vars if macro_vars else None)
        result = converter.convert(sas_code)

        with col2:
            sql_placeholder.code(result.sql, language="sql")

            if result.warnings:
                for w in result.warnings:
                    st.warning(w)

    except Exception as e:
        with col2:
            sql_placeholder.empty()
            st.error(f"Conversion error: {str(e)}")

elif convert_clicked:
    with col2:
        st.info("Please enter SAS code to convert.")

# ── Footer ──────────────────────────────────────────────────────
st.divider()
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    st.caption("15 SAS patterns supported")
with col_f2:
    st.caption("28 test cases with CI")
with col_f3:
    st.caption("[GitHub](https://github.com/linuxra/sas_2_snowflake)")
