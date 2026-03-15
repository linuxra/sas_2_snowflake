/* ============================================================
   SAS to Snowflake Converter - Example SAS Code
   ============================================================
   This file contains common SAS DATA step patterns that the
   converter can translate to Snowflake SQL.
   ============================================================ */

/* --- Example 1: Basic data selection with filtering --- */
%let schema = PROD_DW.ANALYTICS;

data &schema..active_customers;
    set &schema..customers;
    if status = 'ACTIVE' and account_balance > 0;
    keep customer_id name email status account_balance;
run;


/* --- Example 2: MERGE (JOIN) with computed columns --- */
data &schema..customer_orders;
    merge &schema..customers (in=a keep=customer_id name email)
          &schema..orders    (in=b);
    by customer_id;
    if a;

    order_flag = (b = 1);
    if order_amount > 1000 then tier = 'PREMIUM';
    else if order_amount > 100 then tier = 'STANDARD';
    else tier = 'BASIC';
run;


/* --- Example 3: String and date manipulation --- */
data work.processed;
    set work.raw_data;

    full_name   = catx(' ', upcase(first_name), propcase(last_name));
    email_domain = scan(email, 2, '@');
    account_age  = intck('month', open_date, today());
    next_review  = intnx('year', open_date, 1);
    clean_phone  = compress(phone, '()-. ');
run;


/* --- Example 4: BY-group processing with FIRST/LAST --- */
data work.deduped;
    set work.transactions;
    by customer_id transaction_date;
    if first.customer_id;
run;


/* --- Example 5: UNION ALL (stacking datasets) --- */
data work.all_regions;
    set work.region_north
        work.region_south
        work.region_east
        work.region_west;
run;


/* --- Example 6: SELECT/WHEN (multi-way branching) --- */
data work.categorized;
    set work.products;
    select (category_code);
        when ('EL') category = 'Electronics';
        when ('CL') category = 'Clothing';
        when ('FD') category = 'Food';
        when ('HW') category = 'Hardware';
        otherwise category = 'Other';
    end;
run;
