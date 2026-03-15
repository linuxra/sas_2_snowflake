"""
Snowflake SQL Code Generator
=============================
Traverses the SAS AST and generates Snowflake SQL.
Handles all SAS DATA step constructs including:
- SET, MERGE (with IN=), BY, FIRST./LAST.
- IF/THEN/ELSE, DO loops, SELECT/WHEN
- RETAIN (→ LAG/window functions), arrays (→ CASE/UNPIVOT)
- OUTPUT, DELETE, KEEP/DROP/RENAME, WHERE
- FORMAT, LABEL, LENGTH
- Macro variable substitution (&var, &&var, %LET)
- SAS functions → Snowflake functions
"""

import re
from typing import List, Dict, Any, Optional, Set, Tuple
from .parser import (
    ASTNode, DataStep, SetStatement, MergeStatement, ByStatement,
    IfThenElse, DoLoop, Assignment, OutputStatement, DeleteStatement,
    RetainStatement, KeepStatement, DropStatement, RenameStatement,
    WhereStatement, FormatStatement, LabelStatement, LengthStatement,
    ArrayDecl, SelectBlock, PutStatement, MacroLet,
    BinaryOp, UnaryOp, FunctionCall, MacroVarRef, VariableRef,
    Literal, ArrayAccess, InOperator, Expression
)
from .functions import (
    get_snowflake_function, get_snowflake_date_format,
    get_snowflake_interval, SAS_DATE_FORMATS, FUNCTION_MAP
)


class SnowflakeCodeGen:
    """
    Generates Snowflake SQL from a SAS DATA step AST.
    """

    def __init__(self, macro_vars: Optional[Dict[str, str]] = None):
        self.macro_vars = macro_vars or {}
        self.indent_level = 0
        self.indent_str = "    "
        self.output_lines: List[str] = []
        self.warnings: List[str] = []

        # State tracking for current DATA step
        self.current_step: Optional[DataStep] = None
        self.by_vars: List[str] = []
        self.by_descending: List[bool] = []
        self.has_merge = False
        self.merge_datasets: List[Dict] = []
        self.has_retain = False
        self.retain_vars: List[Dict] = []
        self.has_first_last = False
        self.arrays: Dict[str, ArrayDecl] = {}
        self.keep_vars: List[str] = []
        self.drop_vars: List[str] = []
        self.renames: Dict[str, str] = {}
        self.where_conditions: List[Any] = []
        self.formats: Dict[str, str] = {}
        self.labels: Dict[str, str] = {}
        self.lengths: List[Dict] = []
        self.set_datasets: List[Dict] = []
        self.has_output_stmt = False
        self.has_delete_stmt = False
        self.assignments: List[Assignment] = []
        self.conditionals: List[ASTNode] = []
        self.do_loops: List[DoLoop] = []
        self.select_blocks: List[SelectBlock] = []
        self.macro_lets: Dict[str, str] = {}
        self.used_in_vars: Dict[str, str] = {}  # in_var_name -> dataset alias
        self.array_loop_info: Optional[Dict] = None  # DO loop over array with OUTPUT

    def generate(self, steps: List[DataStep]) -> str:
        """Generate Snowflake SQL for all DATA steps."""
        results = []

        for step in steps:
            self._reset_state()
            self.current_step = step
            self._analyze_step(step)
            self._detect_array_loop_pattern()
            sql = self._generate_step(step)
            results.append(sql)

        return '\n\n'.join(results)

    def _reset_state(self):
        """Reset state for a new DATA step."""
        self.by_vars = []
        self.by_descending = []
        self.has_merge = False
        self.merge_datasets = []
        self.has_retain = False
        self.retain_vars = []
        self.has_first_last = False
        self.arrays = {}
        self.keep_vars = []
        self.drop_vars = []
        self.renames = {}
        self.where_conditions = []
        self.formats = {}
        self.labels = {}
        self.lengths = []
        self.set_datasets = []
        self.has_output_stmt = False
        self.has_delete_stmt = False
        self.assignments = []
        self.conditionals = []
        self.do_loops = []
        self.select_blocks = []
        self.used_in_vars = {}
        self.array_loop_info = None
        self.output_lines = []
        self.indent_level = 0

    def _analyze_step(self, step: DataStep):
        """First pass: analyze the step to determine generation strategy."""
        for stmt in step.statements:
            self._analyze_stmt(stmt)

    def _analyze_stmt(self, stmt: ASTNode):
        """Analyze a single statement for planning."""
        if isinstance(stmt, SetStatement):
            self.set_datasets = stmt.datasets
        elif isinstance(stmt, MergeStatement):
            self.has_merge = True
            self.merge_datasets = stmt.datasets
            for ds in stmt.datasets:
                if 'in' in ds.get('options', {}):
                    self.used_in_vars[ds['options']['in']] = ds['name']
        elif isinstance(stmt, ByStatement):
            self.by_vars = stmt.variables
            self.by_descending = stmt.descending
        elif isinstance(stmt, RetainStatement):
            self.has_retain = True
            self.retain_vars = stmt.variables
        elif isinstance(stmt, KeepStatement):
            self.keep_vars.extend(stmt.variables)
        elif isinstance(stmt, DropStatement):
            self.drop_vars.extend(stmt.variables)
        elif isinstance(stmt, RenameStatement):
            self.renames.update(stmt.renames)
        elif isinstance(stmt, WhereStatement):
            self.where_conditions.append(stmt.condition)
        elif isinstance(stmt, FormatStatement):
            self.formats.update(stmt.formats)
        elif isinstance(stmt, LabelStatement):
            self.labels.update(stmt.labels)
        elif isinstance(stmt, LengthStatement):
            self.lengths.extend(stmt.lengths)
        elif isinstance(stmt, ArrayDecl):
            self.arrays[stmt.name.upper()] = stmt
        elif isinstance(stmt, OutputStatement):
            self.has_output_stmt = True
        elif isinstance(stmt, DeleteStatement):
            self.has_delete_stmt = True
        elif isinstance(stmt, Assignment):
            self.assignments.append(stmt)
        elif isinstance(stmt, IfThenElse):
            self.conditionals.append(stmt)
            self._analyze_conditional(stmt)
        elif isinstance(stmt, DoLoop):
            self.do_loops.append(stmt)
        elif isinstance(stmt, SelectBlock):
            self.select_blocks.append(stmt)
        elif isinstance(stmt, MacroLet):
            self.macro_lets[stmt.name] = stmt.value

        # Check for FIRST./LAST. references
        self._check_first_last(stmt)

    def _analyze_conditional(self, stmt: IfThenElse):
        """Recursively analyze conditionals for planning."""
        for s in stmt.then_block:
            self._analyze_stmt(s)
        for s in stmt.else_block:
            self._analyze_stmt(s)

    def _check_first_last(self, node):
        """Check if node references FIRST. or LAST. variables."""
        if isinstance(node, VariableRef):
            if node.dataset and node.dataset.upper() in ('FIRST', 'LAST'):
                self.has_first_last = True
        elif isinstance(node, IfThenElse):
            self._check_first_last(node.condition)
            for s in node.then_block:
                self._check_first_last(s)
            for s in node.else_block:
                self._check_first_last(s)
        elif isinstance(node, BinaryOp):
            self._check_first_last(node.left)
            self._check_first_last(node.right)
        elif isinstance(node, Assignment):
            self._check_first_last(node.expression)
        elif isinstance(node, FunctionCall):
            for arg in node.args:
                self._check_first_last(arg)

    # ═══════════════════════════ MAIN GENERATION ═══════════════════════════

    def _generate_step(self, step: DataStep) -> str:
        """Generate SQL for a single DATA step."""
        output_table = step.output_tables[0] if step.output_tables else 'output_table'
        output_table = self._resolve_macro_name(output_table)

        lines = []

        # Add macro variable declarations as comments
        if self.macro_lets:
            lines.append("-- Macro variable declarations (use Snowflake session variables or stored procedure parameters)")
            for name, value in self.macro_lets.items():
                lines.append(f"SET {name} = '{self._resolve_macro_value(value)}';")
            lines.append("")

        # Generate the main CREATE TABLE AS SELECT
        lines.append(f"CREATE OR REPLACE TABLE {output_table} AS")

        # Build the SELECT/FROM/WHERE/etc.
        if self.has_merge:
            lines.extend(self._generate_merge_sql())
        elif self.array_loop_info:
            lines.extend(self._generate_array_loop_sql())
        elif self.set_datasets:
            lines.extend(self._generate_set_sql())
        else:
            lines.extend(self._generate_computed_sql())

        # Add labels as comments
        if self.labels:
            lines.append("")
            lines.append("-- Column labels (use COMMENT ON COLUMN for Snowflake metadata)")
            for var, label in self.labels.items():
                var = self._apply_rename(var)
                lines.append(f"-- COMMENT ON COLUMN {output_table}.{var} IS '{label}';")

        return '\n'.join(lines) + ';'

    # ──────── SET-based generation ────────

    def _generate_set_sql(self) -> List[str]:
        """Generate SQL for a DATA step with SET statement(s)."""
        lines = []

        # Multiple datasets in SET = UNION ALL
        if len(self.set_datasets) > 1:
            return self._generate_union_sql()

        ds = self.set_datasets[0]
        ds_name = self._resolve_macro_name(ds['name'])
        ds_alias = self._make_alias(ds_name)
        ds_options = ds.get('options', {})

        # Build WITH clause for CTEs if needed
        cte_parts = []
        main_source = ds_name

        # If we have RETAIN or FIRST/LAST, use window functions in a CTE
        if self.has_retain or self.has_first_last:
            cte_lines = self._build_retain_first_last_cte(ds_name, ds_alias)
            if cte_lines:
                cte_parts.append(("base_with_analytics", cte_lines))
                main_source = "base_with_analytics"
                ds_alias = "bwa"

        if cte_parts:
            lines.append("WITH")
            for i, (cte_name, cte_sql) in enumerate(cte_parts):
                lines.append(f"  {cte_name} AS (")
                for cl in cte_sql:
                    lines.append(f"    {cl}")
                lines.append("  )" + ("," if i < len(cte_parts) - 1 else ""))

        # SELECT clause
        select_cols = self._build_select_columns(ds_alias)
        lines.append("SELECT")
        for i, col in enumerate(select_cols):
            comma = "," if i < len(select_cols) - 1 else ""
            lines.append(f"    {col}{comma}")

        # FROM clause
        lines.append(f"FROM {main_source} {ds_alias}")

        # WHERE clause
        where_parts = []
        if 'where' in ds_options:
            where_parts.append(self._expr_to_sql(ds_options['where']))
        for wc in self.where_conditions:
            where_parts.append(self._expr_to_sql(wc))

        # Collect computed column names (assignments + conditionals)
        computed_names = set()
        for stmt in self.current_step.statements:
            if isinstance(stmt, Assignment) and not stmt.is_sum:
                computed_names.add(stmt.target)
            elif isinstance(stmt, IfThenElse) and not stmt.is_subsetting_if:
                case_cols = self._conditional_to_case(stmt)
                computed_names.update(case_cols.keys())

        # Handle subsetting IF (convert to WHERE)
        subsetting_where_parts = []
        for stmt in self.current_step.statements:
            if isinstance(stmt, IfThenElse) and stmt.is_subsetting_if:
                subsetting_where_parts.append(self._expr_to_sql(stmt.condition))

        # Handle DELETE with conditionals (inverted WHERE)
        delete_wheres = self._extract_delete_conditions()
        where_parts.extend(delete_wheres)

        # Check if any subsetting IF references a computed column
        # If so, wrap in a CTE so WHERE runs after SELECT
        subsetting_refs_computed = False
        if subsetting_where_parts and computed_names:
            for wp in subsetting_where_parts:
                for cn in computed_names:
                    if cn in wp:
                        subsetting_refs_computed = True
                        break

        if subsetting_refs_computed:
            # Use QUALIFY to filter on computed columns (runs after SELECT)
            if where_parts:
                lines.append("WHERE " + " AND ".join(f"({w})" for w in where_parts))
            lines.append("QUALIFY " + " AND ".join(f"({w})" for w in subsetting_where_parts))
        else:
            where_parts.extend(subsetting_where_parts)
            if where_parts:
                lines.append("WHERE " + " AND ".join(f"({w})" for w in where_parts))

        return lines

    def _generate_union_sql(self) -> List[str]:
        """Generate UNION ALL for multiple datasets in SET."""
        lines = []
        lines.append("SELECT * FROM (")
        for i, ds in enumerate(self.set_datasets):
            ds_name = self._resolve_macro_name(ds['name'])
            if i > 0:
                lines.append("    UNION ALL")
            lines.append(f"    SELECT * FROM {ds_name}")
            ds_options = ds.get('options', {})
            if 'where' in ds_options:
                lines.append(f"    WHERE {self._expr_to_sql(ds_options['where'])}")
        lines.append(")")

        where_parts = []
        for wc in self.where_conditions:
            where_parts.append(self._expr_to_sql(wc))
        if where_parts:
            lines.append("WHERE " + " AND ".join(f"({w})" for w in where_parts))

        return lines

    # ──────── Array-loop-output (UNPIVOT) generation ────────

    def _detect_array_loop_pattern(self):
        """Detect DO loop over array with OUTPUT (wide-to-long unpivot pattern)."""
        for loop in self.do_loops:
            if not (loop.loop_var and loop.start and loop.end):
                continue

            array_assign = None
            has_output = False

            def walk(stmts):
                nonlocal array_assign, has_output
                for stmt in stmts:
                    if isinstance(stmt, Assignment) and not stmt.is_sum:
                        if isinstance(stmt.expression, ArrayAccess):
                            if stmt.expression.array_name.upper() in self.arrays:
                                idx = stmt.expression.index
                                if isinstance(idx, VariableRef) and idx.name == loop.loop_var:
                                    array_assign = stmt
                    if isinstance(stmt, OutputStatement):
                        has_output = True
                    if isinstance(stmt, IfThenElse):
                        walk(stmt.then_block)
                        walk(stmt.else_block)
                    if isinstance(stmt, DoLoop):
                        walk(stmt.body)

            walk(loop.body)

            if array_assign and has_output:
                arr_decl = self.arrays[array_assign.expression.array_name.upper()]
                self.array_loop_info = {
                    'loop': loop,
                    'array_assign': array_assign,
                    'array_decl': arr_decl,
                    'target_var': array_assign.target,
                }
                return

    def _find_sum_statements(self, stmts: List[ASTNode]) -> List[Assignment]:
        """Recursively find all sum statements in a list of statements."""
        sums = []
        for stmt in stmts:
            if isinstance(stmt, Assignment) and stmt.is_sum:
                sums.append(stmt)
            elif isinstance(stmt, IfThenElse):
                sums.extend(self._find_sum_statements(stmt.then_block))
                sums.extend(self._find_sum_statements(stmt.else_block))
            elif isinstance(stmt, DoLoop):
                sums.extend(self._find_sum_statements(stmt.body))
        return sums

    def _generate_array_loop_sql(self) -> List[str]:
        """Generate SQL for DO loop over array with OUTPUT (UNPIVOT pattern)."""
        info = self.array_loop_info
        loop = info['loop']
        arr_decl = info['array_decl']
        target_var = info['target_var']
        loop_var = loop.loop_var

        ds = self.set_datasets[0]
        ds_name = self._resolve_macro_name(ds['name'])

        # Resolve array variables
        arr_vars = arr_decl.variables
        if not arr_vars and arr_decl.size:
            arr_vars = [f"{arr_decl.name}{i}" for i in range(1, arr_decl.size + 1)]

        lines = []

        # Build the column name list for UNPIVOT
        arr_col_list = ", ".join(arr_vars)

        # CTE 0: Assign row identifier BEFORE unpivoting
        lines.append("WITH")
        lines.append("numbered_src AS (")
        lines.append(f"    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS _row_id_")
        lines.append(f"    FROM {ds_name}")
        lines.append("),")

        # CTE 1: UNPIVOT (single table scan, reads source only once)
        lines.append("unpivoted AS (")
        lines.append(f"    SELECT *")
        lines.append(f"    FROM numbered_src")
        lines.append(f"    UNPIVOT INCLUDE NULLS ({target_var} FOR _col_name_ IN ({arr_col_list}))")
        lines.append("),")

        # CTE 2: Derive numeric month_idx from the UNPIVOT column name
        lines.append("indexed AS (")
        lines.append(f"    SELECT")
        lines.append(f"        *,")
        lines.append(f"        REGEXP_SUBSTR(_col_name_, '\\\\d+')::INT AS {loop_var}")
        lines.append(f"    FROM unpivoted")
        lines.append("),")

        # Process loop body for computed columns (excluding array assign and OUTPUT)
        computed_cols = {}
        known_vars = {target_var, loop_var}
        for stmt in loop.body:
            if isinstance(stmt, IfThenElse):
                case_cols = self._conditional_to_case(stmt, known_vars=known_vars)
                computed_cols.update(case_cols)
            elif isinstance(stmt, Assignment) and not stmt.is_sum:
                if stmt is not info['array_assign']:
                    expr_sql = self._expr_to_sql(stmt.expression)
                    computed_cols[stmt.target] = expr_sql

        # Find sum statements in loop body
        sum_stmts = self._find_sum_statements(loop.body)

        # Determine which computed cols reassign an existing variable
        reassigned = {}
        other_computed = {}
        for name, expr in computed_cols.items():
            if name == target_var:
                reassigned[name] = f"_adj_{name}"
                other_computed[reassigned[name]] = expr
            else:
                other_computed[name] = expr

        # CTE 2: Computed columns
        if other_computed:
            lines.append("computed AS (")
            lines.append("    SELECT")
            comp_items = ["        *"]
            for name, expr in other_computed.items():
                comp_items.append(f"        {expr} AS {name}")
            lines.append(",\n".join(comp_items))
            lines.append("    FROM indexed")
            lines.append(")")
            final_source = "computed"
        else:
            final_source = "indexed"
            # Remove trailing comma from unpivoted CTE
            for i in range(len(lines) - 1, -1, -1):
                if lines[i].strip() == "),":
                    lines[i] = ")"
                    break

        # Final SELECT
        lines.append("SELECT")

        # Build EXCLUDE list: array columns + any dropped vars
        exclude_vars = list(arr_vars)
        for dv in self.drop_vars:
            if dv not in exclude_vars:
                exclude_vars.append(dv)

        # If target_var was reassigned, exclude the original too
        if reassigned:
            if target_var not in exclude_vars:
                exclude_vars.append(target_var)

        # Exclude internal columns from UNPIVOT machinery
        exclude_vars.append("_row_id_")
        exclude_vars.append("_col_name_")

        exclude_list = ", ".join(exclude_vars)
        lines.append(f"    * EXCLUDE ({exclude_list}),")

        # Add renamed reassigned columns
        for orig, temp in reassigned.items():
            lines.append(f"    {temp} AS {orig},")

        # Add SUM() OVER() for sum statements
        for si, sum_stmt in enumerate(sum_stmts):
            addend_sql = self._expr_to_sql(sum_stmt.expression)
            # Substitute reassigned variable names
            for orig, temp in reassigned.items():
                addend_sql = addend_sql.replace(orig, temp)
            # Get initial value from RETAIN if available
            init_val = "0"
            for rv in self.retain_vars:
                if rv['name'] == sum_stmt.target and rv.get('initial_value') is not None:
                    init_val = str(rv['initial_value'])
            is_last = (si == len(sum_stmts) - 1)
            comma = "" if is_last else ","
            lines.append(f"    SUM(COALESCE({addend_sql}, {init_val})) OVER (PARTITION BY _row_id_ ORDER BY {loop_var} ROWS UNBOUNDED PRECEDING) AS {sum_stmt.target}{comma}")

        lines.append(f"FROM {final_source}")

        return lines

    # ──────── MERGE-based generation ────────

    def _generate_merge_sql(self) -> List[str]:
        """Generate SQL for a DATA step with MERGE statement."""
        lines = []
        datasets = self.merge_datasets

        if len(datasets) < 2:
            # Single dataset merge is just a SELECT
            return self._generate_set_sql_from_datasets(datasets)

        # Determine join type based on IN= usage
        join_type = self._determine_join_type()

        # Build CTE for FIRST/LAST if needed
        cte_parts = []
        if self.has_first_last:
            for ds in datasets:
                ds_name = self._resolve_macro_name(ds['name'])
                alias = self._make_alias(ds_name)
                cte_lines = self._build_first_last_cte_for_merge(ds_name, alias)
                if cte_lines:
                    cte_parts.append((f"{alias}_fl", cte_lines))

        if cte_parts:
            lines.append("WITH")
            for i, (name, sql) in enumerate(cte_parts):
                lines.append(f"  {name} AS (")
                for cl in sql:
                    lines.append(f"    {cl}")
                lines.append("  )" + ("," if i < len(cte_parts) - 1 else ""))

        # SELECT clause with computed columns
        select_cols = self._build_merge_select_columns()
        lines.append("SELECT")
        for i, col in enumerate(select_cols):
            comma = "," if i < len(select_cols) - 1 else ""
            lines.append(f"    {col}{comma}")

        # FROM / JOIN
        d1 = datasets[0]
        d1_name = self._resolve_macro_name(d1['name'])
        d1_alias = self._make_alias(d1_name, 'a')
        lines.append(f"FROM {d1_name} {d1_alias}")

        for idx, ds in enumerate(datasets[1:], 1):
            ds_name = self._resolve_macro_name(ds['name'])
            ds_alias = self._make_alias(ds_name, chr(ord('a') + idx))
            on_clause = " AND ".join(
                f"{d1_alias}.{bv} = {ds_alias}.{bv}" for bv in self.by_vars
            )
            lines.append(f"{join_type} {ds_name} {ds_alias}")
            lines.append(f"    ON {on_clause}")

        # WHERE clause for IN= conditions
        in_wheres = self._build_in_var_conditions(datasets)
        where_parts = list(in_wheres)
        for wc in self.where_conditions:
            where_parts.append(self._expr_to_sql(wc))
        if where_parts:
            lines.append("WHERE " + " AND ".join(f"({w})" for w in where_parts))

        return lines

    def _determine_join_type(self) -> str:
        """Determine JOIN type from IN= variable usage in conditional logic."""
        # Analyze IF conditions for IN= patterns
        in_vars = set(self.used_in_vars.keys())
        if not in_vars:
            return "FULL OUTER JOIN"

        # Look for patterns: IF a AND b -> INNER, IF a -> LEFT, etc.
        for stmt in self.current_step.statements:
            if isinstance(stmt, IfThenElse) and stmt.is_subsetting_if:
                return self._infer_join_from_condition(stmt.condition, in_vars)
            elif isinstance(stmt, IfThenElse):
                return self._infer_join_from_condition(stmt.condition, in_vars)

        return "FULL OUTER JOIN"

    def _infer_join_from_condition(self, condition, in_vars: set) -> str:
        """Infer JOIN type from IF condition using IN= variables."""
        if isinstance(condition, BinaryOp):
            if condition.op == 'AND':
                # Both IN vars present -> INNER JOIN
                left_vars = self._extract_var_refs(condition.left)
                right_vars = self._extract_var_refs(condition.right)
                all_vars = left_vars | right_vars
                if in_vars.issubset(all_vars):
                    return "INNER JOIN"
            elif condition.op == '=':
                # Single IN var = 1 -> LEFT or RIGHT
                if isinstance(condition.left, VariableRef) and condition.left.name in in_vars:
                    return "LEFT OUTER JOIN"
        elif isinstance(condition, VariableRef):
            if condition.name in in_vars:
                return "LEFT OUTER JOIN"
        return "FULL OUTER JOIN"

    def _extract_var_refs(self, expr) -> set:
        """Extract variable reference names from expression."""
        refs = set()
        if isinstance(expr, VariableRef):
            refs.add(expr.name)
        elif isinstance(expr, BinaryOp):
            refs |= self._extract_var_refs(expr.left)
            refs |= self._extract_var_refs(expr.right)
        return refs

    # ──────── Computed SQL (no SET/MERGE) ────────

    def _generate_computed_sql(self) -> List[str]:
        """Generate SQL for a DATA step without SET or MERGE (pure computation)."""
        lines = []
        lines.append("SELECT")

        computed_cols = []
        for stmt in self.current_step.statements:
            if isinstance(stmt, Assignment):
                expr_sql = self._expr_to_sql(stmt.expression)
                target = self._apply_rename(stmt.target)
                computed_cols.append(f"    {expr_sql} AS {target}")

        if not computed_cols:
            computed_cols = ["    1 AS _dummy_"]

        for i, col in enumerate(computed_cols):
            comma = "," if i < len(computed_cols) - 1 else ""
            lines.append(f"{col}{comma}")

        return lines

    # ──────── Column selection ────────

    def _build_select_columns(self, alias: str = "") -> List[str]:
        """Build the SELECT column list."""
        cols = []
        prefix = f"{alias}." if alias else ""

        # Start with computed columns from assignments
        computed = {}
        for stmt in self.current_step.statements:
            if isinstance(stmt, Assignment):
                target = stmt.target
                expr_sql = self._expr_to_sql(stmt.expression)
                target_out = self._apply_rename(target)
                computed[target_out] = expr_sql

        # Handle conditionals that produce assignments
        for stmt in self.current_step.statements:
            if isinstance(stmt, IfThenElse) and not stmt.is_subsetting_if:
                case_cols = self._conditional_to_case(stmt)
                computed.update(case_cols)

        # Handle SELECT blocks
        for stmt in self.current_step.statements:
            if isinstance(stmt, SelectBlock):
                case_cols = self._select_block_to_case(stmt)
                computed.update(case_cols)

        # RETAIN columns as window functions
        if self.has_retain:
            for rv in self.retain_vars:
                name = rv['name']
                init_val = rv.get('initial_value')
                if name not in computed:
                    # Add LAG-based column if not already computed
                    init = f", {init_val}" if init_val is not None else ""
                    order_by = ", ".join(self.by_vars) if self.by_vars else "1"
                    computed[name] = f"LAG({prefix}{name}{init}) OVER (ORDER BY {order_by})"

        # FIRST./LAST. columns
        if self.has_first_last and self.by_vars:
            order_by = ", ".join(self.by_vars)
            partition_by = ", ".join(self.by_vars)
            cols.append(f"ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) AS _row_in_group_")
            cols.append(f"COUNT(*) OVER (PARTITION BY {partition_by}) AS _group_count_")

        # Base columns
        if self.keep_vars:
            for v in self.keep_vars:
                v_out = self._apply_rename(v)
                if v_out in computed:
                    cols.append(f"{computed[v_out]} AS {v_out}")
                else:
                    cols.append(f"{prefix}{v}" + (f" AS {v_out}" if v != v_out else ""))
        elif self.drop_vars:
            cols.append(f"{prefix}*")
            # Note: Snowflake supports SELECT * EXCLUDE (col1, col2)
            # but we'll add a comment for clarity
            if self.drop_vars:
                drop_list = ", ".join(self.drop_vars)
                cols = [f"{prefix}* EXCLUDE ({drop_list})"]
        else:
            # All columns plus computed ones
            if computed:
                cols.append(f"{prefix}*")
            else:
                cols.append(f"{prefix}*")

        # Add computed columns that aren't base columns
        for name, expr in computed.items():
            col_str = f"{expr} AS {name}"
            if col_str not in cols and not any(name in c for c in cols):
                cols.append(col_str)

        # Apply renames using Snowflake RENAME syntax for * selections
        if self.renames and not self.keep_vars:
            # If we have a * selection, use Snowflake's RENAME syntax
            new_cols = []
            for c in cols:
                c_stripped = c.strip()
                if c_stripped.endswith('*') or '* EXCLUDE' in c_stripped:
                    rename_pairs = ", ".join(f"{old} AS {new}" for old, new in self.renames.items())
                    if '* EXCLUDE' in c_stripped:
                        new_cols.append(f"{c_stripped} RENAME ({rename_pairs})")
                    else:
                        new_cols.append(f"{c_stripped} RENAME ({rename_pairs})")
                else:
                    for old, new in self.renames.items():
                        if c_stripped == f"{prefix}{old}" or c_stripped == old:
                            c = f"{prefix}{old} AS {new}"
                    new_cols.append(c)
            cols = new_cols

        return cols if cols else [f"{prefix}*"]

    def _build_merge_select_columns(self) -> List[str]:
        """Build SELECT columns for MERGE (JOIN)."""
        cols = []
        datasets = self.merge_datasets

        # BY columns (use COALESCE for FULL OUTER JOIN)
        aliases = [self._make_alias(self._resolve_macro_name(ds['name']), chr(ord('a') + i))
                   for i, ds in enumerate(datasets)]

        for bv in self.by_vars:
            coalesce_parts = ", ".join(f"{a}.{bv}" for a in aliases)
            cols.append(f"COALESCE({coalesce_parts}) AS {bv}")

        # Non-BY columns from each dataset
        for i, ds in enumerate(datasets):
            alias = aliases[i]
            ds_options = ds.get('options', {})
            keep = ds_options.get('keep', [])
            drop = ds_options.get('drop', [])

            if keep:
                for v in keep:
                    if v not in self.by_vars:
                        cols.append(f"{alias}.{v}")
            elif drop:
                cols.append(f"{alias}.* EXCLUDE ({', '.join(drop + self.by_vars)})")
            else:
                # All non-BY columns
                cols.append(f"{alias}.* EXCLUDE ({', '.join(self.by_vars)})")

        # IN= indicator columns
        for ds in datasets:
            if 'in' in ds.get('options', {}):
                in_var = ds['options']['in']
                alias = aliases[datasets.index(ds)]
                by_col = self.by_vars[0] if self.by_vars else '*'
                cols.append(f"CASE WHEN {alias}.{by_col} IS NOT NULL THEN 1 ELSE 0 END AS {in_var}")

        # Computed columns from assignments
        for stmt in self.current_step.statements:
            if isinstance(stmt, Assignment):
                expr_sql = self._expr_to_sql(stmt.expression)
                target = self._apply_rename(stmt.target)
                cols.append(f"{expr_sql} AS {target}")
            elif isinstance(stmt, IfThenElse) and not stmt.is_subsetting_if:
                case_cols = self._conditional_to_case(stmt)
                for name, expr in case_cols.items():
                    cols.append(f"{expr} AS {name}")

        return cols if cols else ["*"]

    # ──────── Conditional → CASE WHEN ────────

    def _conditional_to_case(self, node: IfThenElse, depth: int = 0, known_vars: Optional[Set[str]] = None) -> Dict[str, str]:
        """Convert IF/THEN/ELSE chain to CASE WHEN expressions.

        Flattens chained IF/ELSE IF/ELSE IF/ELSE into a single CASE with
        multiple WHEN clauses.
        """
        # First, flatten the chain: collect all (condition, assignments) pairs + final else
        chain = self._flatten_if_chain(node)
        # chain is: [{"condition": expr_sql, "assigns": {target: expr_sql}}, ...]
        # Last entry may have condition=None for the final ELSE

        # Collect all target variables across the chain
        all_targets = set()
        for entry in chain:
            all_targets |= set(entry["assigns"].keys())

        result = {}
        for target in all_targets:
            when_parts = []
            # Default ELSE: keep original value if known, else NULL
            if known_vars is not None:
                else_val = target if target in known_vars else "NULL"
            else:
                else_val = target  # Default: keep original value
            for entry in chain:
                cond = entry["condition"]
                expr = entry["assigns"].get(target)
                if expr is None:
                    continue
                if cond is None:
                    # This is the final ELSE
                    else_val = expr
                else:
                    when_parts.append(f"WHEN {cond} THEN {expr}")

            target_out = self._apply_rename(target)
            if when_parts:
                case_body = " ".join(when_parts)
                result[target_out] = f"CASE {case_body} ELSE {else_val} END"

        return result

    def _flatten_if_chain(self, node: IfThenElse) -> List[Dict]:
        """Flatten a chained IF/ELSE IF/ELSE into a list of condition-assignment pairs."""
        chain = []
        current = node

        while current is not None:
            cond_sql = self._expr_to_sql(current.condition)
            assigns = self._extract_assignments(current.then_block)
            chain.append({"condition": cond_sql, "assigns": assigns})

            # Check else block
            if current.else_block:
                if (len(current.else_block) == 1 and
                        isinstance(current.else_block[0], IfThenElse) and
                        not current.else_block[0].is_subsetting_if):
                    # Chained ELSE IF -> continue flattening
                    current = current.else_block[0]
                else:
                    # Final ELSE block
                    else_assigns = self._extract_assignments(current.else_block)
                    chain.append({"condition": None, "assigns": else_assigns})
                    current = None
            else:
                current = None

        return chain

    def _extract_assignments(self, stmts: List[ASTNode]) -> Dict[str, str]:
        """Extract variable assignments from a block of statements."""
        assigns = {}
        for stmt in stmts:
            if isinstance(stmt, Assignment) and not stmt.is_sum:
                assigns[stmt.target] = self._expr_to_sql(stmt.expression)
            elif isinstance(stmt, IfThenElse) and not stmt.is_subsetting_if:
                # Nested IF -> nested CASE
                nested = self._conditional_to_case(stmt)
                assigns.update(nested)
            elif isinstance(stmt, OutputStatement):
                pass  # Handled elsewhere
            elif isinstance(stmt, DeleteStatement):
                pass  # Handled elsewhere
        return assigns

    def _extract_delete_conditions(self) -> List[str]:
        """Extract DELETE conditions and invert them for WHERE clause."""
        conditions = []
        for stmt in self.current_step.statements:
            if isinstance(stmt, IfThenElse):
                delete_cond = self._find_delete_in_conditional(stmt)
                if delete_cond:
                    conditions.append(delete_cond)
        return conditions

    def _find_delete_in_conditional(self, node: IfThenElse) -> Optional[str]:
        """Find DELETE in conditional and return inverted WHERE condition."""
        for stmt in node.then_block:
            if isinstance(stmt, DeleteStatement):
                return f"NOT ({self._expr_to_sql(node.condition)})"
        for stmt in node.else_block:
            if isinstance(stmt, DeleteStatement):
                return self._expr_to_sql(node.condition)
        return None

    # ──────── SELECT block → CASE ────────

    def _select_block_to_case(self, block: SelectBlock) -> Dict[str, str]:
        """Convert SELECT/WHEN block to CASE expressions."""
        result = {}
        all_targets = set()

        for wc in block.when_clauses:
            assigns = self._extract_assignments(wc.get('statements', []))
            all_targets |= set(assigns.keys())

        otherwise_assigns = self._extract_assignments(block.otherwise)
        all_targets |= set(otherwise_assigns.keys())

        for target in all_targets:
            parts = []
            for wc in block.when_clauses:
                cond = self._expr_to_sql(wc['condition'])
                assigns = self._extract_assignments(wc.get('statements', []))
                if target in assigns:
                    if block.select_expr:
                        sel_expr = self._expr_to_sql(block.select_expr)
                        parts.append(f"WHEN {sel_expr} = {cond} THEN {assigns[target]}")
                    else:
                        parts.append(f"WHEN {cond} THEN {assigns[target]}")

            otherwise_val = otherwise_assigns.get(target, target)
            if parts:
                case_body = " ".join(parts)
                target_out = self._apply_rename(target)
                result[target_out] = f"CASE {case_body} ELSE {otherwise_val} END"

        return result

    # ──────── RETAIN / FIRST.LAST CTE ────────

    def _build_retain_first_last_cte(self, ds_name: str, alias: str) -> List[str]:
        """Build a CTE with window functions for RETAIN and FIRST/LAST."""
        lines = []
        cols = ["*"]
        order_by = ", ".join(self.by_vars) if self.by_vars else "1"

        if self.has_retain:
            for rv in self.retain_vars:
                name = rv['name']
                init_val = rv.get('initial_value')
                if init_val is not None:
                    cols.append(f"COALESCE(LAG({name}) OVER (ORDER BY {order_by}), {init_val}) AS _retain_{name}")
                else:
                    cols.append(f"LAG({name}) OVER (ORDER BY {order_by}) AS _retain_{name}")

        if self.has_first_last and self.by_vars:
            partition = ", ".join(self.by_vars)
            cols.append(f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_by}) AS _rn_")
            cols.append(f"COUNT(*) OVER (PARTITION BY {partition}) AS _cnt_")
            cols.append(f"CASE WHEN ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_by}) = 1 THEN 1 ELSE 0 END AS _first_flag_")
            cols.append(f"CASE WHEN ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_by} DESC) = 1 THEN 1 ELSE 0 END AS _last_flag_")

        lines.append("SELECT")
        for i, c in enumerate(cols):
            comma = "," if i < len(cols) - 1 else ""
            lines.append(f"  {c}{comma}")
        lines.append(f"FROM {ds_name}")
        return lines

    def _build_first_last_cte_for_merge(self, ds_name: str, alias: str) -> List[str]:
        """Build CTE for FIRST/LAST in MERGE context."""
        if not self.by_vars:
            return []
        return self._build_retain_first_last_cte(ds_name, alias)

    # ──────── IN= conditions ────────

    def _build_in_var_conditions(self, datasets: List[Dict]) -> List[str]:
        """Build WHERE conditions based on IN= variables used in IF statements."""
        conditions = []
        # The IN= conditions are typically handled by the JOIN type
        # Additional filtering based on IF conditions with IN vars
        return conditions

    # ═══════════════════════════ EXPRESSION TO SQL ═══════════════════════════

    def _expr_to_sql(self, expr) -> str:
        """Convert an expression AST node to Snowflake SQL string."""
        if expr is None:
            return "NULL"

        if isinstance(expr, Literal):
            return self._literal_to_sql(expr)

        if isinstance(expr, VariableRef):
            return self._varref_to_sql(expr)

        if isinstance(expr, MacroVarRef):
            return self._macro_var_to_sql(expr)

        if isinstance(expr, BinaryOp):
            left = self._expr_to_sql(expr.left)
            right = self._expr_to_sql(expr.right)
            op = expr.op
            if op == '||':
                return f"CONCAT({left}, {right})"
            return f"({left} {op} {right})"

        if isinstance(expr, UnaryOp):
            operand = self._expr_to_sql(expr.operand)
            return f"{expr.op} ({operand})"

        if isinstance(expr, FunctionCall):
            return self._function_to_sql(expr)

        if isinstance(expr, ArrayAccess):
            return self._array_access_to_sql(expr)

        if isinstance(expr, InOperator):
            operand = self._expr_to_sql(expr.operand)
            values = ", ".join(self._expr_to_sql(v) for v in expr.values)
            neg = "NOT " if expr.negated else ""
            return f"{operand} {neg}IN ({values})"

        if isinstance(expr, str):
            return expr

        return str(expr)

    def _literal_to_sql(self, lit: Literal) -> str:
        """Convert a literal to SQL."""
        if lit.literal_type == "missing":
            return "NULL"
        if lit.literal_type == "number":
            return str(lit.value)
        if lit.literal_type == "string":
            value = str(lit.value)
            # Resolve macro variables inside double-quoted strings
            value = self._resolve_macro_name(value)
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if lit.literal_type == "date":
            # SAS date literal like '01JAN2020'd
            value = self._resolve_macro_name(str(lit.value))
            # Clean up any spacing artifacts from macro resolution
            value = re.sub(r'\s+', '', value) if re.search(r'\d', value) else value
            # Try to detect if it's an actual date or a macro-resolved string
            if re.match(r'\d{4}-\d{2}-\d{2}$', value):
                return f"TO_DATE('{value}', 'YYYY-MM-DD')"
            if re.match(r'\d{8}$', value):
                return f"TO_DATE('{value}', 'YYYYMMDD')"
            return f"TO_DATE('{value}', 'DDMONYYYY')"
        return str(lit.value)

    def _varref_to_sql(self, ref: VariableRef) -> str:
        """Convert a variable reference to SQL."""
        if ref.dataset:
            ds_upper = ref.dataset.upper()
            if ds_upper == 'FIRST':
                # FIRST.var -> _first_flag_ (from CTE)
                if self.has_first_last:
                    return "_first_flag_"
                else:
                    partition = ", ".join(self.by_vars) if self.by_vars else ref.name
                    order_by = ", ".join(self.by_vars) if self.by_vars else "1"
                    return f"(ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_by}) = 1)"
            elif ds_upper == 'LAST':
                if self.has_first_last:
                    return "_last_flag_"
                else:
                    partition = ", ".join(self.by_vars) if self.by_vars else ref.name
                    order_by = ", ".join(self.by_vars) if self.by_vars else "1"
                    return f"(ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_by} DESC) = 1)"
            else:
                return f"{ref.dataset}.{ref.name}"
        return ref.name

    def _macro_var_to_sql(self, ref: MacroVarRef) -> str:
        """Convert a macro variable reference to SQL."""
        name = ref.name
        # Check if we have a value for this macro var
        value = self.macro_vars.get(name) or self.macro_lets.get(name)
        if value:
            return self._resolve_macro_value(value)
        # Use Snowflake session variable syntax
        return f"${name}"

    def _function_to_sql(self, func: FunctionCall) -> str:
        """Convert a SAS function call to Snowflake SQL."""
        name = func.name.upper()
        args_sql = [self._expr_to_sql(a) for a in func.args]

        # Special-case functions
        special = self._handle_special_function(name, args_sql, func.args)
        if special is not None:
            return special

        # Look up in mapping
        mapping = get_snowflake_function(name)
        if mapping:
            template = mapping['template']
            if template is None:
                # Should have been handled by special handler
                return f"{name}({', '.join(args_sql)})"
            if '{args}' in template:
                return template.replace('{args}', ', '.join(args_sql))
            # Substitute positional args
            result = template
            for i, arg in enumerate(args_sql):
                result = result.replace(f'{{{i}}}', arg)
            return result

        # Unknown function: pass through (many SAS functions have same name in Snowflake)
        self.warnings.append(f"Unknown SAS function '{name}' - passed through as-is")
        return f"{name}({', '.join(args_sql)})"

    def _handle_special_function(self, name: str, args_sql: List[str], args_raw: List) -> Optional[str]:
        """Handle SAS functions that need special conversion logic."""

        # ── COMPRESS ──
        if name == 'COMPRESS':
            if len(args_sql) == 1:
                return f"REPLACE({args_sql[0]}, ' ', '')"
            elif len(args_sql) == 2:
                return f"REGEXP_REPLACE({args_sql[0]}, '[' || {args_sql[1]} || ']', '')"
            elif len(args_sql) == 3:
                modifiers = args_sql[2].strip("'\"").lower()
                if 'd' in modifiers:
                    return f"REGEXP_REPLACE({args_sql[0]}, '[0-9]', '')"
                elif 'a' in modifiers:
                    return f"REGEXP_REPLACE({args_sql[0]}, '[a-zA-Z]', '')"
                elif 's' in modifiers:
                    return f"REGEXP_REPLACE({args_sql[0]}, '\\\\s', '')"
                elif 'k' in modifiers:
                    return f"REGEXP_REPLACE({args_sql[0]}, '[^' || {args_sql[1]} || ']', '')"
                return f"REGEXP_REPLACE({args_sql[0]}, '[' || {args_sql[1]} || ']', '')"

        # ── CATS (strip + concat) ──
        if name == 'CATS':
            trimmed = [f"TRIM(CAST({a} AS VARCHAR))" for a in args_sql]
            return f"CONCAT({', '.join(trimmed)})"

        # ── CATX (delimiter + strip + concat) ──
        if name == 'CATX':
            delim = args_sql[0]
            trimmed = [f"TRIM(CAST({a} AS VARCHAR))" for a in args_sql[1:]]
            return f"CONCAT_WS({delim}, {', '.join(trimmed)})"

        # ── CATT (trim trailing + concat) ──
        if name == 'CATT':
            trimmed = [f"RTRIM(CAST({a} AS VARCHAR))" for a in args_sql]
            return f"CONCAT({', '.join(trimmed)})"

        # ── SUM (row-level, ignores missing) ──
        if name == 'SUM':
            coalesced = [f"COALESCE({a}, 0)" for a in args_sql]
            return f"({' + '.join(coalesced)})"

        # ── MEAN (row-level average) ──
        if name == 'MEAN':
            n = len(args_sql)
            coalesced = [f"COALESCE({a}, 0)" for a in args_sql]
            return f"(({' + '.join(coalesced)}) / NULLIF({n}, 0))"

        # ── NMISS / CMISS (count NULLs) ──
        if name in ('NMISS', 'CMISS'):
            parts = [f"CASE WHEN {a} IS NULL THEN 1 ELSE 0 END" for a in args_sql]
            return f"({' + '.join(parts)})"

        # ── N (count non-missing) ──
        if name == 'N':
            parts = [f"CASE WHEN {a} IS NOT NULL THEN 1 ELSE 0 END" for a in args_sql]
            return f"({' + '.join(parts)})"

        # ── COUNTW ──
        if name == 'COUNTW':
            if len(args_sql) >= 2:
                return f"REGEXP_COUNT(TRIM({args_sql[0]}), '[^' || {args_sql[1]} || ']+') "
            return f"REGEXP_COUNT(TRIM({args_sql[0]}), '\\\\S+')"

        # ── COUNT (character occurrences) ──
        if name == 'COUNT':
            return f"(LENGTH({args_sql[0]}) - LENGTH(REPLACE({args_sql[0]}, {args_sql[1]}, ''))) / LENGTH({args_sql[1]})"

        # ── SCAN ──
        if name == 'SCAN':
            if len(args_sql) == 2:
                return f"TRIM(SPLIT_PART({args_sql[0]}, ' ', {args_sql[1]}))"
            elif len(args_sql) >= 3:
                return f"TRIM(SPLIT_PART({args_sql[0]}, {args_sql[2]}, {args_sql[1]}))"

        # ── INPUT (type conversion with informat) ──
        if name == 'INPUT':
            if len(args_raw) >= 2 and isinstance(args_raw[1], VariableRef):
                fmt = args_raw[1].name.upper()
                sf_fmt = get_snowflake_date_format(fmt)
                if sf_fmt:
                    return f"TO_DATE({args_sql[0]}, '{sf_fmt}')"
                # Numeric informat
                if any(x in fmt for x in ('BEST', 'COMMA', '.')):
                    return f"TRY_TO_NUMBER({args_sql[0]})"
            return f"TRY_TO_NUMBER({args_sql[0]})"

        # ── PUT (format output) ──
        if name == 'PUT':
            if len(args_raw) >= 2 and isinstance(args_raw[1], VariableRef):
                fmt = args_raw[1].name.upper()
                sf_fmt = get_snowflake_date_format(fmt)
                if sf_fmt:
                    return f"TO_CHAR({args_sql[0]}, '{sf_fmt}')"
                if '$' in fmt:
                    return f"CAST({args_sql[0]} AS VARCHAR)"
                if any(x in fmt for x in ('BEST', 'COMMA', 'Z', 'DOLLAR')):
                    return f"TO_VARCHAR({args_sql[0]})"
            return f"TO_VARCHAR({args_sql[0]})"

        # ── INTCK (interval between dates) ──
        if name == 'INTCK':
            interval = args_sql[0].strip("'\"")
            sf_interval = get_snowflake_interval(interval)
            return f"DATEDIFF('{sf_interval}', {args_sql[1]}, {args_sql[2]})"

        # ── INTNX (add interval to date) ──
        if name == 'INTNX':
            interval = args_sql[0].strip("'\"")
            sf_interval = get_snowflake_interval(interval)
            alignment = ""
            if len(args_sql) >= 4:
                align = args_sql[3].strip("'\"").upper()
                if align in ('B', 'BEGINNING'):
                    alignment = f"\n    /* Note: Alignment='BEGINNING' - may need DATE_TRUNC('{sf_interval}', ...) */"
                elif align in ('E', 'END'):
                    alignment = f"\n    /* Note: Alignment='END' - may need LAST_DAY/additional logic */"
            return f"DATEADD('{sf_interval}', {args_sql[2]}, {args_sql[1]}){alignment}"

        # ── YYQ (year + quarter -> date) ──
        if name == 'YYQ':
            return f"DATE_FROM_PARTS({args_sql[0]}, (({args_sql[1]} - 1) * 3) + 1, 1)"

        # ── MISSING ──
        if name == 'MISSING':
            return f"({args_sql[0]} IS NULL)"

        # ── IFN / IFC ──
        if name in ('IFN', 'IFC'):
            if len(args_sql) >= 4:
                return f"CASE WHEN ({args_sql[0]}) IS NULL THEN {args_sql[3]} WHEN {args_sql[0]} THEN {args_sql[1]} ELSE {args_sql[2]} END"
            return f"CASE WHEN {args_sql[0]} THEN {args_sql[1]} ELSE {args_sql[2]} END"

        # ── VERIFY ──
        if name == 'VERIFY':
            return f"REGEXP_INSTR({args_sql[0]}, '[^' || {args_sql[1]} || ']')"

        # ── LAG with partition ──
        if name in ('LAG', 'LAG1', 'LAG2', 'LAG3'):
            n = {'LAG': 1, 'LAG1': 1, 'LAG2': 2, 'LAG3': 3}.get(name, 1)
            order_by = ", ".join(self.by_vars) if self.by_vars else "1"
            partition = f"PARTITION BY {', '.join(self.by_vars)} " if self.by_vars else ""
            return f"LAG({args_sql[0]}, {n}) OVER ({partition}ORDER BY {order_by})"

        # ── DIF ──
        if name in ('DIF', 'DIF1'):
            order_by = ", ".join(self.by_vars) if self.by_vars else "1"
            partition = f"PARTITION BY {', '.join(self.by_vars)} " if self.by_vars else ""
            return f"({args_sql[0]} - LAG({args_sql[0]}, 1) OVER ({partition}ORDER BY {order_by}))"

        # ── ROUND with default ──
        if name == 'ROUND':
            if len(args_sql) == 1:
                return f"ROUND({args_sql[0]})"
            return None  # Use default template

        # ── Macro functions ──
        if name.startswith('%'):
            return self._handle_macro_function(name, args_sql)

        return None  # Not a special function

    def _handle_macro_function(self, name: str, args_sql: List[str]) -> str:
        """Handle SAS macro functions."""
        name_upper = name.upper()
        if name_upper == '%EVAL':
            return f"({', '.join(args_sql)})"
        if name_upper == '%SYSFUNC':
            # Try to convert the inner function
            return args_sql[0] if args_sql else "NULL"
        if name_upper in ('%STR', '%NRSTR'):
            return args_sql[0] if args_sql else "''"
        if name_upper == '%UPCASE':
            return f"UPPER({args_sql[0]})" if args_sql else "''"
        if name_upper == '%LOWCASE':
            return f"LOWER({args_sql[0]})" if args_sql else "''"
        if name_upper == '%SCAN':
            return f"SPLIT_PART({args_sql[0]}, ' ', {args_sql[1]})" if len(args_sql) >= 2 else args_sql[0]
        if name_upper == '%SUBSTR':
            if len(args_sql) >= 3:
                return f"SUBSTR({args_sql[0]}, {args_sql[1]}, {args_sql[2]})"
            return f"SUBSTR({args_sql[0]}, {args_sql[1]})" if len(args_sql) >= 2 else args_sql[0]
        return f"/* {name}({', '.join(args_sql)}) */"

    # ──────── Array access ────────

    def _array_access_to_sql(self, access: ArrayAccess) -> str:
        """Convert SAS array access to Snowflake column reference."""
        arr_name = access.array_name.upper()
        idx_sql = self._expr_to_sql(access.index)

        if arr_name in self.arrays:
            arr_decl = self.arrays[arr_name]
            if arr_decl.variables:
                # If index is a constant, directly reference the column
                if isinstance(access.index, Literal) and access.index.literal_type == "number":
                    idx = int(access.index.value) - 1  # SAS is 1-based
                    if 0 <= idx < len(arr_decl.variables):
                        return arr_decl.variables[idx]

                # Dynamic index: use CASE
                parts = []
                for i, var in enumerate(arr_decl.variables, 1):
                    parts.append(f"WHEN {idx_sql} = {i} THEN {var}")
                return f"CASE {' '.join(parts)} END"

        # Unknown array: best-effort
        return f"/* ARRAY {arr_name}[{idx_sql}] */"

    # ═══════════════════════════ HELPERS ═══════════════════════════

    def _resolve_macro_name(self, name: str) -> str:
        """Resolve macro variables in a dataset or variable name."""
        if not name:
            return name
        result = name
        # Replace &var. and &var patterns
        def replace_macro(match):
            var_name = match.group(1)
            return self.macro_vars.get(var_name, self.macro_lets.get(var_name, f"${var_name}"))
        result = re.sub(r'&(\w+)\.?', replace_macro, result)
        # Clean up spacing around dots (e.g., "PROD_DB . SCHEMA" -> "PROD_DB.SCHEMA")
        result = re.sub(r'\s*\.\s*', '.', result)
        return result

    def _resolve_macro_value(self, value: str) -> str:
        """Resolve macro variables within a value string."""
        return self._resolve_macro_name(value)

    def _apply_rename(self, name: str) -> str:
        """Apply RENAME mapping to a variable name."""
        return self.renames.get(name, name)

    def _make_alias(self, name: str, default: str = '') -> str:
        """Generate a table alias from a dataset name."""
        # Remove library prefix
        parts = name.split('.')
        base = parts[-1] if parts else name
        # Remove macro variable syntax
        base = re.sub(r'[\$&%]', '', base)
        if default:
            return default
        return base[:3].lower() if base else 'src'

    def _generate_set_sql_from_datasets(self, datasets: List[Dict]) -> List[str]:
        """Generate SQL from dataset list (fallback for single MERGE dataset)."""
        lines = []
        ds = datasets[0] if datasets else {"name": "unknown"}
        ds_name = self._resolve_macro_name(ds['name'])
        lines.append(f"SELECT * FROM {ds_name}")
        return lines
