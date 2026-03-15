"""
SAS Data Step Parser
====================
Parses tokenized SAS DATA step code into an AST (Abstract Syntax Tree).
Handles SET, MERGE, IF/THEN/ELSE, DO loops, RETAIN, arrays, OUTPUT,
KEEP/DROP/RENAME, WHERE, FORMAT, LABEL, LENGTH, macro variables, and more.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
from .tokenizer import Token, TokenType, tokenize


# ──────────────────────────── AST Node Definitions ────────────────────────────

@dataclass
class ASTNode:
    """Base class for all AST nodes."""
    node_type: str = "base"


@dataclass
class DataStep(ASTNode):
    """Top-level DATA step node."""
    node_type: str = "data_step"
    output_tables: List[str] = field(default_factory=list)
    statements: List[ASTNode] = field(default_factory=list)


@dataclass
class SetStatement(ASTNode):
    node_type: str = "set"
    datasets: List[Dict[str, Any]] = field(default_factory=list)
    # Each dataset: {"name": str, "options": {"where": ..., "keep": ..., "rename": ..., "in": ...}}
    nobs_var: Optional[str] = None
    end_var: Optional[str] = None


@dataclass
class MergeStatement(ASTNode):
    node_type: str = "merge"
    datasets: List[Dict[str, Any]] = field(default_factory=list)
    by_vars: List[str] = field(default_factory=list)
    by_descending: List[bool] = field(default_factory=list)


@dataclass
class ByStatement(ASTNode):
    node_type: str = "by"
    variables: List[str] = field(default_factory=list)
    descending: List[bool] = field(default_factory=list)


@dataclass
class IfThenElse(ASTNode):
    node_type: str = "if_then_else"
    condition: Any = None  # Expression node
    then_block: List[ASTNode] = field(default_factory=list)
    else_block: List[ASTNode] = field(default_factory=list)
    is_subsetting_if: bool = False  # IF without THEN (subsetting IF)


@dataclass
class DoLoop(ASTNode):
    node_type: str = "do_loop"
    loop_var: Optional[str] = None
    start: Any = None
    end: Any = None
    by: Any = None
    while_condition: Any = None
    until_condition: Any = None
    body: List[ASTNode] = field(default_factory=list)


@dataclass
class Assignment(ASTNode):
    node_type: str = "assignment"
    target: str = ""
    expression: Any = None
    is_sum: bool = False  # For var + expr (sum statement)


@dataclass
class OutputStatement(ASTNode):
    node_type: str = "output"
    dataset: Optional[str] = None


@dataclass
class DeleteStatement(ASTNode):
    node_type: str = "delete"


@dataclass
class RetainStatement(ASTNode):
    node_type: str = "retain"
    variables: List[Dict[str, Any]] = field(default_factory=list)
    # Each: {"name": str, "initial_value": Any}


@dataclass
class KeepStatement(ASTNode):
    node_type: str = "keep"
    variables: List[str] = field(default_factory=list)


@dataclass
class DropStatement(ASTNode):
    node_type: str = "drop"
    variables: List[str] = field(default_factory=list)


@dataclass
class RenameStatement(ASTNode):
    node_type: str = "rename"
    renames: Dict[str, str] = field(default_factory=dict)  # old -> new


@dataclass
class WhereStatement(ASTNode):
    node_type: str = "where"
    condition: Any = None


@dataclass
class FormatStatement(ASTNode):
    node_type: str = "format"
    formats: Dict[str, str] = field(default_factory=dict)  # var -> format


@dataclass
class LabelStatement(ASTNode):
    node_type: str = "label"
    labels: Dict[str, str] = field(default_factory=dict)  # var -> label


@dataclass
class LengthStatement(ASTNode):
    node_type: str = "length"
    lengths: List[Dict[str, Any]] = field(default_factory=list)
    # Each: {"name": str, "type": "char"|"num", "length": int}


@dataclass
class ArrayDecl(ASTNode):
    node_type: str = "array_decl"
    name: str = ""
    size: Any = None  # int or '*'
    variables: List[str] = field(default_factory=list)
    is_temporary: bool = False
    initial_values: List[Any] = field(default_factory=list)


@dataclass
class SelectBlock(ASTNode):
    node_type: str = "select_block"
    select_expr: Any = None  # Expression being selected on (optional)
    when_clauses: List[Dict[str, Any]] = field(default_factory=list)
    # Each: {"condition": expr, "statements": [ASTNode]}
    otherwise: List[ASTNode] = field(default_factory=list)


@dataclass
class PutStatement(ASTNode):
    node_type: str = "put"
    items: List[Any] = field(default_factory=list)


@dataclass
class MacroLet(ASTNode):
    node_type: str = "macro_let"
    name: str = ""
    value: str = ""


# ──────────────────────────── Expression Nodes ────────────────────────────

@dataclass
class Expression(ASTNode):
    node_type: str = "expression"


@dataclass
class BinaryOp(Expression):
    node_type: str = "binary_op"
    op: str = ""
    left: Any = None
    right: Any = None


@dataclass
class UnaryOp(Expression):
    node_type: str = "unary_op"
    op: str = ""
    operand: Any = None


@dataclass
class FunctionCall(Expression):
    node_type: str = "function_call"
    name: str = ""
    args: List[Any] = field(default_factory=list)


@dataclass
class MacroVarRef(Expression):
    node_type: str = "macro_var_ref"
    name: str = ""  # Without & prefix
    original: str = ""  # Original text like &var.


@dataclass
class VariableRef(Expression):
    node_type: str = "variable_ref"
    name: str = ""
    dataset: Optional[str] = None  # For FIRST.var or dataset.var


@dataclass
class Literal(Expression):
    node_type: str = "literal"
    value: Any = None
    literal_type: str = "string"  # "string", "number", "date", "missing"


@dataclass
class ArrayAccess(Expression):
    node_type: str = "array_access"
    array_name: str = ""
    index: Any = None


@dataclass
class InOperator(Expression):
    node_type: str = "in_operator"
    operand: Any = None
    values: List[Any] = field(default_factory=list)
    negated: bool = False


# ──────────────────────────── Parser ────────────────────────────

class SASParser:
    """
    Parses SAS DATA step tokens into an AST.
    """

    def __init__(self, tokens: List[Token]):
        self.tokens = [t for t in tokens if t.type not in (TokenType.COMMENT, TokenType.NEWLINE)]
        self.pos = 0
        self.macro_vars: Dict[str, str] = {}

    def parse(self) -> List[DataStep]:
        """Parse all DATA steps in the token stream."""
        steps = []
        while not self._at_end():
            if self._check(TokenType.DATA):
                steps.append(self._parse_data_step())
            elif self._check(TokenType.MACRO_LET):
                ml = self._parse_macro_let()
                # Store macro var for later resolution
                self.macro_vars[ml.name] = ml.value
            else:
                self._advance()  # Skip non-data-step tokens
        return steps

    def _parse_data_step(self) -> DataStep:
        """Parse a DATA step from DATA to RUN."""
        step = DataStep()
        self._expect(TokenType.DATA)

        # Parse output dataset names
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            ds = self._parse_dataset_reference()
            step.output_tables.append(ds["name"])
            if self._check(TokenType.COMMA):
                self._advance()
        self._expect(TokenType.SEMICOLON)
        # Fix: If output table ended up as just a keyword name (e.g., 'output'),
        # it was consumed as a statement keyword. This is handled in _parse_dataset_reference.

        # Parse statements until RUN;
        while not self._at_end():
            if self._check(TokenType.RUN):
                self._advance()
                if self._check(TokenType.SEMICOLON):
                    self._advance()
                break
            stmt = self._parse_statement()
            if stmt:
                step.statements.append(stmt)

        return step

    def _parse_statement(self) -> Optional[ASTNode]:
        """Parse a single statement inside a DATA step."""
        if self._at_end():
            return None

        tok = self._current()

        if tok.type == TokenType.SET:
            return self._parse_set()
        elif tok.type == TokenType.MERGE:
            return self._parse_merge()
        elif tok.type == TokenType.BY:
            return self._parse_by()
        elif tok.type == TokenType.IF or tok.type == TokenType.MACRO_IF:
            return self._parse_if()
        elif tok.type == TokenType.DO:
            return self._parse_do()
        elif tok.type == TokenType.SELECT:
            return self._parse_select_block()
        elif tok.type == TokenType.OUTPUT:
            return self._parse_output()
        elif tok.type == TokenType.DELETE:
            self._advance()
            self._expect(TokenType.SEMICOLON)
            return DeleteStatement()
        elif tok.type == TokenType.RETAIN:
            return self._parse_retain()
        elif tok.type == TokenType.KEEP:
            return self._parse_keep()
        elif tok.type == TokenType.DROP:
            return self._parse_drop()
        elif tok.type == TokenType.RENAME:
            return self._parse_rename()
        elif tok.type == TokenType.WHERE:
            return self._parse_where()
        elif tok.type == TokenType.FORMAT:
            return self._parse_format()
        elif tok.type == TokenType.INFORMAT:
            return self._parse_format()  # Treat similarly
        elif tok.type == TokenType.LABEL:
            return self._parse_label()
        elif tok.type == TokenType.LENGTH:
            return self._parse_length()
        elif tok.type == TokenType.ARRAY:
            return self._parse_array_decl()
        elif tok.type == TokenType.PUT:
            return self._parse_put()
        elif tok.type == TokenType.RETURN:
            self._advance()
            self._expect(TokenType.SEMICOLON)
            return ASTNode(node_type="return")
        elif tok.type == TokenType.MACRO_LET:
            return self._parse_macro_let()
        elif tok.type == TokenType.SEMICOLON:
            self._advance()
            return None
        elif tok.type == TokenType.END:
            return None  # Handled by DO parser
        elif tok.type in (TokenType.IDENTIFIER, TokenType.MACRO_VAR):
            return self._parse_assignment_or_call()
        else:
            self._advance()
            return None

    # ──────── SET ────────

    def _parse_set(self) -> SetStatement:
        self._expect(TokenType.SET)
        stmt = SetStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            ds = self._parse_dataset_reference()
            stmt.datasets.append(ds)
        self._expect(TokenType.SEMICOLON)
        return stmt

    # ──────── MERGE ────────

    def _parse_merge(self) -> MergeStatement:
        self._expect(TokenType.MERGE)
        stmt = MergeStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            ds = self._parse_dataset_reference()
            stmt.datasets.append(ds)
        self._expect(TokenType.SEMICOLON)
        return stmt

    # ──────── BY ────────

    def _parse_by(self) -> ByStatement:
        self._expect(TokenType.BY)
        stmt = ByStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            desc = False
            if self._check(TokenType.DESCENDING):
                desc = True
                self._advance()
            if self._check(TokenType.IDENTIFIER) or self._check(TokenType.MACRO_VAR):
                stmt.variables.append(self._current().value)
                stmt.descending.append(desc)
                self._advance()
            else:
                break
        self._expect(TokenType.SEMICOLON)
        return stmt

    # ──────── IF/THEN/ELSE ────────

    def _parse_if(self) -> IfThenElse:
        self._advance()  # skip IF / %IF
        node = IfThenElse()
        node.condition = self._parse_expression()

        if self._check(TokenType.THEN) or self._check(TokenType.MACRO_THEN):
            self._advance()  # skip THEN
            if self._check(TokenType.DO) or self._check(TokenType.MACRO_DO):
                node.then_block = self._parse_do_block()
            else:
                stmt = self._parse_statement()
                if stmt:
                    node.then_block = [stmt]

            if self._check(TokenType.ELSE) or self._check(TokenType.MACRO_ELSE):
                self._advance()  # skip ELSE
                if self._check(TokenType.IF) or self._check(TokenType.MACRO_IF):
                    # ELSE IF
                    node.else_block = [self._parse_if()]
                elif self._check(TokenType.DO) or self._check(TokenType.MACRO_DO):
                    node.else_block = self._parse_do_block()
                else:
                    stmt = self._parse_statement()
                    if stmt:
                        node.else_block = [stmt]
        else:
            # Subsetting IF (no THEN)
            node.is_subsetting_if = True
            self._expect(TokenType.SEMICOLON)

        return node

    # ──────── DO ────────

    def _parse_do(self) -> DoLoop:
        self._advance()  # skip DO
        node = DoLoop()

        if self._check(TokenType.WHILE):
            self._advance()
            self._expect(TokenType.LPAREN)
            node.while_condition = self._parse_expression()
            self._expect(TokenType.RPAREN)
        elif self._check(TokenType.UNTIL):
            self._advance()
            self._expect(TokenType.LPAREN)
            node.until_condition = self._parse_expression()
            self._expect(TokenType.RPAREN)
        elif self._check(TokenType.IDENTIFIER) or self._check(TokenType.MACRO_VAR):
            # Check if this is an iterative DO (var = start TO end)
            saved_pos = self.pos
            var_name = self._current().value
            self._advance()
            if self._check(TokenType.EQUALS):
                self._advance()
                node.loop_var = var_name
                node.start = self._parse_expression()
                self._expect(TokenType.TO)
                node.end = self._parse_expression()
                if self._check(TokenType.BY):
                    self._advance()
                    node.by = self._parse_expression()
            else:
                # Not an iterative DO, restore position
                self.pos = saved_pos

        self._expect(TokenType.SEMICOLON)

        # Parse body
        node.body = self._parse_block_body()
        return node

    def _parse_do_block(self) -> List[ASTNode]:
        """Parse DO; ... END; as a block of statements."""
        self._advance()  # skip DO / %DO
        self._expect(TokenType.SEMICOLON)
        return self._parse_block_body()

    def _parse_block_body(self) -> List[ASTNode]:
        """Parse statements until END;"""
        stmts = []
        while not self._at_end():
            if self._check(TokenType.END) or self._check(TokenType.MACRO_END):
                self._advance()
                if self._check(TokenType.SEMICOLON):
                    self._advance()
                break
            stmt = self._parse_statement()
            if stmt:
                stmts.append(stmt)
        return stmts

    # ──────── SELECT ────────

    def _parse_select_block(self) -> SelectBlock:
        self._expect(TokenType.SELECT)
        node = SelectBlock()
        if self._check(TokenType.LPAREN):
            self._advance()
            node.select_expr = self._parse_expression()
            self._expect(TokenType.RPAREN)
        self._expect(TokenType.SEMICOLON)

        while not self._at_end():
            if self._check(TokenType.END):
                self._advance()
                if self._check(TokenType.SEMICOLON):
                    self._advance()
                break
            elif self._check(TokenType.WHEN):
                self._advance()
                self._expect(TokenType.LPAREN)
                cond = self._parse_expression()
                self._expect(TokenType.RPAREN)
                stmts = []
                while not self._check(TokenType.WHEN) and not self._check(TokenType.OTHERWISE) and not self._check(TokenType.END) and not self._at_end():
                    s = self._parse_statement()
                    if s:
                        stmts.append(s)
                node.when_clauses.append({"condition": cond, "statements": stmts})
            elif self._check(TokenType.OTHERWISE):
                self._advance()
                if self._check(TokenType.SEMICOLON):
                    self._advance()
                while not self._check(TokenType.END) and not self._at_end():
                    s = self._parse_statement()
                    if s:
                        node.otherwise.append(s)
            else:
                self._advance()

        return node

    # ──────── OUTPUT ────────

    def _parse_output(self) -> OutputStatement:
        self._expect(TokenType.OUTPUT)
        node = OutputStatement()
        if self._check(TokenType.IDENTIFIER):
            node.dataset = self._current().value
            self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── RETAIN ────────

    def _parse_retain(self) -> RetainStatement:
        self._expect(TokenType.RETAIN)
        node = RetainStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            name = self._current().value
            self._advance()
            init_val = None
            if self._check(TokenType.NUMBER):
                init_val = self._current().value
                self._advance()
            elif self._check(TokenType.STRING):
                init_val = f"'{self._current().value}'"
                self._advance()
            elif self._check(TokenType.DOT):
                init_val = None  # missing
                self._advance()
            node.variables.append({"name": name, "initial_value": init_val})
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── KEEP / DROP ────────

    def _parse_keep(self) -> KeepStatement:
        self._expect(TokenType.KEEP)
        node = KeepStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            if self._check(TokenType.IDENTIFIER) or self._check(TokenType.MACRO_VAR):
                var_name = self._current().value
                self._advance()
                # Check for variable range: var1-var12
                if self._check(TokenType.MINUS):
                    saved = self.pos
                    self._advance()
                    if self._check(TokenType.IDENTIFIER):
                        end_var = self._current().value
                        expanded = self._expand_var_range(var_name, end_var)
                        if len(expanded) > 2:
                            node.variables.extend(expanded)
                            self._advance()
                            continue
                    self.pos = saved
                node.variables.append(var_name)
            else:
                self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    def _parse_drop(self) -> DropStatement:
        self._expect(TokenType.DROP)
        node = DropStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            if self._check(TokenType.IDENTIFIER) or self._check(TokenType.MACRO_VAR):
                var_name = self._current().value
                self._advance()
                # Check for variable range: var1-var12
                if self._check(TokenType.MINUS):
                    saved = self.pos
                    self._advance()
                    if self._check(TokenType.IDENTIFIER):
                        end_var = self._current().value
                        expanded = self._expand_var_range(var_name, end_var)
                        if len(expanded) > 2:
                            node.variables.extend(expanded)
                            self._advance()
                            continue
                    self.pos = saved
                node.variables.append(var_name)
            else:
                self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    def _expand_var_range(self, start_var: str, end_var: str) -> List[str]:
        """Expand SAS variable range like month1-month12."""
        import re
        m1 = re.match(r'^(.*?)(\d+)$', start_var)
        m2 = re.match(r'^(.*?)(\d+)$', end_var)
        if m1 and m2 and m1.group(1).lower() == m2.group(1).lower():
            prefix = m1.group(1)
            start_n = int(m1.group(2))
            end_n = int(m2.group(2))
            if end_n >= start_n:
                return [f"{prefix}{i}" for i in range(start_n, end_n + 1)]
        return [start_var, end_var]

    # ──────── RENAME ────────

    def _parse_rename(self) -> RenameStatement:
        self._expect(TokenType.RENAME)
        node = RenameStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            old_name = self._current().value
            self._advance()
            self._expect(TokenType.EQUALS)
            new_name = self._current().value
            self._advance()
            node.renames[old_name] = new_name
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── WHERE ────────

    def _parse_where(self) -> WhereStatement:
        self._expect(TokenType.WHERE)
        node = WhereStatement()
        if self._check(TokenType.LPAREN):
            self._advance()
            node.condition = self._parse_expression()
            self._expect(TokenType.RPAREN)
        else:
            node.condition = self._parse_expression()
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── FORMAT ────────

    def _parse_format(self) -> FormatStatement:
        self._advance()  # skip FORMAT/INFORMAT
        node = FormatStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            if self._check(TokenType.IDENTIFIER) or self._check(TokenType.MACRO_VAR):
                var_name = self._current().value
                self._advance()
                if self._check(TokenType.IDENTIFIER):
                    fmt = self._current().value
                    self._advance()
                    node.formats[var_name] = fmt
                elif self._check(TokenType.DOLLAR):
                    self._advance()
                    if self._check(TokenType.IDENTIFIER):
                        fmt = '$' + self._current().value
                        self._advance()
                        node.formats[var_name] = fmt
                else:
                    node.formats[var_name] = ''
            else:
                self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── LABEL ────────

    def _parse_label(self) -> LabelStatement:
        self._expect(TokenType.LABEL)
        node = LabelStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            if self._check(TokenType.IDENTIFIER):
                var_name = self._current().value
                self._advance()
                self._expect(TokenType.EQUALS)
                if self._check(TokenType.STRING):
                    node.labels[var_name] = self._current().value
                    self._advance()
                else:
                    # Unquoted label
                    label_parts = []
                    while not self._check(TokenType.SEMICOLON) and not self._check(TokenType.IDENTIFIER) and not self._at_end():
                        label_parts.append(self._current().value)
                        self._advance()
                    node.labels[var_name] = ' '.join(label_parts)
            else:
                self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── LENGTH ────────

    def _parse_length(self) -> LengthStatement:
        self._expect(TokenType.LENGTH)
        node = LengthStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            is_char = False
            if self._check(TokenType.DOLLAR):
                is_char = True
                self._advance()
            if self._check(TokenType.IDENTIFIER):
                name = self._current().value
                self._advance()
                if self._check(TokenType.DOLLAR):
                    is_char = True
                    self._advance()
                length = 8  # default
                if self._check(TokenType.NUMBER):
                    length = int(self._current().value)
                    self._advance()
                node.lengths.append({
                    "name": name,
                    "type": "char" if is_char else "num",
                    "length": length
                })
            else:
                self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── ARRAY ────────

    def _parse_array_decl(self) -> ArrayDecl:
        self._expect(TokenType.ARRAY)
        node = ArrayDecl()
        node.name = self._current().value
        self._advance()

        # Size: {n} or {*} or (n) or (*)
        if self._check(TokenType.LBRACE) or self._check(TokenType.LPAREN):
            close = TokenType.RBRACE if self._check(TokenType.LBRACE) else TokenType.RPAREN
            self._advance()
            if self._check(TokenType.STAR):
                node.size = '*'
                self._advance()
            elif self._check(TokenType.NUMBER):
                node.size = int(self._current().value)
                self._advance()
            self._expect(close)

        # Check for $ (character array)
        is_char = False
        if self._check(TokenType.DOLLAR):
            is_char = True
            self._advance()

        # Check for _TEMPORARY_
        if self._check(TokenType.IDENTIFIER) and self._current().value.upper() == '_TEMPORARY_':
            node.is_temporary = True
            self._advance()

        # Variable list or initial values in ()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            if self._check(TokenType.LPAREN):
                # Initial values
                self._advance()
                while not self._check(TokenType.RPAREN) and not self._at_end():
                    if self._check(TokenType.NUMBER) or self._check(TokenType.STRING):
                        node.initial_values.append(self._current().value)
                    elif self._check(TokenType.DOT):
                        node.initial_values.append(None)
                    self._advance()
                self._expect(TokenType.RPAREN)
            elif self._check(TokenType.IDENTIFIER):
                var_name = self._current().value
                self._advance()
                # Check for variable range: var1-var12
                if self._check(TokenType.MINUS):
                    saved = self.pos
                    self._advance()
                    if self._check(TokenType.IDENTIFIER):
                        end_var = self._current().value
                        expanded = self._expand_var_range(var_name, end_var)
                        if len(expanded) > 2:
                            node.variables.extend(expanded)
                            self._advance()
                            continue
                    self.pos = saved
                node.variables.append(var_name)
            else:
                self._advance()

        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── PUT ────────

    def _parse_put(self) -> PutStatement:
        self._expect(TokenType.PUT)
        node = PutStatement()
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            node.items.append(self._current().value)
            self._advance()
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── %LET ────────

    def _parse_macro_let(self) -> MacroLet:
        self._expect(TokenType.MACRO_LET)
        node = MacroLet()
        node.name = self._current().value
        self._advance()
        self._expect(TokenType.EQUALS)
        # Read value until semicolon
        parts = []
        while not self._check(TokenType.SEMICOLON) and not self._at_end():
            parts.append(self._current().value)
            self._advance()
        node.value = ' '.join(parts)
        self._expect(TokenType.SEMICOLON)
        return node

    # ──────── Assignment or function call ────────

    def _parse_assignment_or_call(self) -> Optional[ASTNode]:
        """Parse: var = expr; or var + expr; (sum statement) or function()"""
        target_tok = self._current()
        self._advance()

        # Check for dotted variable (FIRST.var, dataset.var)
        target = target_tok.value
        if self._check(TokenType.DOT):
            self._advance()
            if self._check(TokenType.IDENTIFIER):
                target = target + '.' + self._current().value
                self._advance()

        if self._check(TokenType.EQUALS):
            self._advance()
            expr = self._parse_expression()
            self._expect(TokenType.SEMICOLON)
            return Assignment(target=target, expression=expr)
        elif self._check(TokenType.PLUS):
            # Sum statement: var + expr; (SAS accumulator)
            self._advance()
            expr = self._parse_expression()
            self._expect(TokenType.SEMICOLON)
            return Assignment(target=target, expression=expr, is_sum=True)
        elif self._check(TokenType.LPAREN):
            # Function call as statement (rare in DATA step)
            self.pos -= 1
            if self._check(TokenType.DOT):
                self.pos -= 1
            expr = self._parse_expression()
            self._expect(TokenType.SEMICOLON)
            return Assignment(target='_result_', expression=expr)
        else:
            # Skip to semicolon
            while not self._check(TokenType.SEMICOLON) and not self._at_end():
                self._advance()
            if self._check(TokenType.SEMICOLON):
                self._advance()
            return None

    # ──────── Expression Parsing (Pratt parser / precedence climbing) ────────

    def _parse_expression(self) -> Any:
        return self._parse_or()

    def _parse_or(self) -> Any:
        left = self._parse_and()
        while self._check(TokenType.OR):
            self._advance()
            right = self._parse_and()
            left = BinaryOp(op='OR', left=left, right=right)
        return left

    def _parse_and(self) -> Any:
        left = self._parse_not()
        while self._check(TokenType.AND):
            self._advance()
            right = self._parse_not()
            left = BinaryOp(op='AND', left=left, right=right)
        return left

    def _parse_not(self) -> Any:
        if self._check(TokenType.NOT):
            self._advance()
            operand = self._parse_comparison()
            return UnaryOp(op='NOT', operand=operand)
        return self._parse_comparison()

    def _parse_comparison(self) -> Any:
        left = self._parse_addition()

        comp_ops = {
            TokenType.EQUALS: '=', TokenType.NE: '<>',
            TokenType.LT: '<', TokenType.GT: '>',
            TokenType.LE: '<=', TokenType.GE: '>=',
        }

        if self._current_type() in comp_ops:
            op = comp_ops[self._current_type()]
            self._advance()
            right = self._parse_addition()
            return BinaryOp(op=op, left=left, right=right)

        # IN operator
        if self._check(TokenType.IN) or (self._check(TokenType.NOT) and self._peek_is(1, TokenType.IN)):
            negated = False
            if self._check(TokenType.NOT):
                negated = True
                self._advance()
            self._advance()  # skip IN
            self._expect(TokenType.LPAREN)
            values = []
            while not self._check(TokenType.RPAREN) and not self._at_end():
                values.append(self._parse_addition())
                if self._check(TokenType.COMMA):
                    self._advance()
            self._expect(TokenType.RPAREN)
            return InOperator(operand=left, values=values, negated=negated)

        return left

    def _parse_addition(self) -> Any:
        left = self._parse_multiplication()
        while self._check(TokenType.PLUS) or self._check(TokenType.MINUS) or self._check(TokenType.PIPE):
            op_tok = self._current()
            op = op_tok.value
            if op_tok.type == TokenType.PIPE:
                op = '||'
            self._advance()
            right = self._parse_multiplication()
            left = BinaryOp(op=op, left=left, right=right)
        return left

    def _parse_multiplication(self) -> Any:
        left = self._parse_unary()
        while self._check(TokenType.STAR) or self._check(TokenType.SLASH):
            op = self._current().value
            self._advance()
            right = self._parse_unary()
            left = BinaryOp(op=op, left=left, right=right)
        return left

    def _parse_unary(self) -> Any:
        if self._check(TokenType.MINUS):
            self._advance()
            operand = self._parse_primary()
            return UnaryOp(op='-', operand=operand)
        if self._check(TokenType.NOT):
            self._advance()
            operand = self._parse_primary()
            return UnaryOp(op='NOT', operand=operand)
        return self._parse_primary()

    def _parse_primary(self) -> Any:
        tok = self._current()

        if tok.type == TokenType.NUMBER:
            self._advance()
            return Literal(value=tok.value, literal_type="number")

        if tok.type == TokenType.STRING:
            self._advance()
            return Literal(value=tok.value, literal_type="string")

        if tok.type == TokenType.DATE_LITERAL:
            self._advance()
            return Literal(value=tok.value, literal_type="date")

        if tok.type == TokenType.DOT:
            self._advance()
            return Literal(value=None, literal_type="missing")

        if tok.type == TokenType.MACRO_VAR:
            self._advance()
            # Extract name from &var. format
            name = tok.value.lstrip('&').rstrip('.')
            return MacroVarRef(name=name, original=tok.value)

        if tok.type == TokenType.LPAREN:
            self._advance()
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN)
            return expr

        # SAS keywords that can also be used as function names or variable names in expressions
        _keyword_as_name = (
            TokenType.IDENTIFIER, TokenType.INPUT, TokenType.PUT,
            TokenType.LENGTH, TokenType.OUTPUT, TokenType.FORMAT,
            TokenType.INFORMAT, TokenType.LABEL, TokenType.DATA,
            TokenType.STOP, TokenType.RETURN, TokenType.LEAVE,
            TokenType.CONTINUE,
        )
        if tok.type in _keyword_as_name:
            name = tok.value
            self._advance()

            # Check for dotted name (FIRST.var, LAST.var, lib.table)
            if self._check(TokenType.DOT):
                self._advance()
                if self._is_any_word():
                    suffix = self._current().value
                    self._advance()
                    return VariableRef(name=suffix, dataset=name)
                else:
                    return VariableRef(name=name)

            # Function call
            if self._check(TokenType.LPAREN):
                self._advance()
                args = []
                while not self._check(TokenType.RPAREN) and not self._at_end():
                    args.append(self._parse_expression())
                    if self._check(TokenType.COMMA):
                        self._advance()
                self._expect(TokenType.RPAREN)
                return FunctionCall(name=name.upper(), args=args)

            # Array access: arr{i} or arr[i]
            if self._check(TokenType.LBRACE):
                self._advance()
                idx = self._parse_expression()
                self._expect(TokenType.RBRACE)
                return ArrayAccess(array_name=name, index=idx)

            return VariableRef(name=name)

        # Handle macro function calls like %EVAL(), %SYSFUNC()
        if tok.type in (TokenType.MACRO_EVAL, TokenType.MACRO_SYSFUNC,
                        TokenType.MACRO_STR, TokenType.MACRO_SUBSTR,
                        TokenType.MACRO_SCAN, TokenType.MACRO_UPCASE,
                        TokenType.MACRO_LOWCASE):
            name = tok.value
            self._advance()
            if self._check(TokenType.LPAREN):
                self._advance()
                args = []
                while not self._check(TokenType.RPAREN) and not self._at_end():
                    args.append(self._parse_expression())
                    if self._check(TokenType.COMMA):
                        self._advance()
                self._expect(TokenType.RPAREN)
                return FunctionCall(name=name.upper(), args=args)
            return VariableRef(name=name)

        # Skip unknown tokens in expressions
        self._advance()
        return Literal(value=tok.value, literal_type="string")

    # ──────── Dataset Reference ────────

    def _is_any_word(self) -> bool:
        """Check if current token is any word-like token (identifier or keyword used as name)."""
        t = self._current_type()
        return t in (TokenType.IDENTIFIER, TokenType.MACRO_VAR, TokenType.OUTPUT,
                     TokenType.INPUT, TokenType.PUT, TokenType.DATA, TokenType.SET,
                     TokenType.MERGE, TokenType.BY, TokenType.IF, TokenType.THEN,
                     TokenType.ELSE, TokenType.DO, TokenType.END, TokenType.RETAIN,
                     TokenType.KEEP, TokenType.DROP, TokenType.RENAME, TokenType.WHERE,
                     TokenType.FORMAT, TokenType.INFORMAT, TokenType.LABEL, TokenType.LENGTH,
                     TokenType.ARRAY, TokenType.DELETE, TokenType.STOP, TokenType.RUN,
                     TokenType.IN, TokenType.AND, TokenType.OR, TokenType.NOT,
                     TokenType.TO, TokenType.WHILE, TokenType.UNTIL, TokenType.SELECT,
                     TokenType.WHEN, TokenType.OTHERWISE, TokenType.RETURN)

    def _parse_dataset_reference(self) -> Dict[str, Any]:
        """Parse dataset reference with options: lib.name (where= keep= drop= rename= in=)"""
        ds = {"name": "", "options": {}}

        # Dataset name (possibly lib.name) - keywords can be valid dataset names after DOT
        if self._is_any_word():
            name = self._current().value
            self._advance()
            if self._check(TokenType.DOT):
                self._advance()
                if self._is_any_word():
                    name = name + '.' + self._current().value
                    self._advance()
            ds["name"] = name

        # Dataset options in parentheses
        if self._check(TokenType.LPAREN):
            self._advance()
            while not self._check(TokenType.RPAREN) and not self._at_end():
                if self._check(TokenType.WHERE):
                    self._advance()
                    self._expect(TokenType.EQUALS)
                    self._expect(TokenType.LPAREN)
                    cond = self._parse_expression()
                    self._expect(TokenType.RPAREN)
                    ds["options"]["where"] = cond
                elif self._check(TokenType.KEEP):
                    self._advance()
                    self._expect(TokenType.EQUALS)
                    vars_list = []
                    while not self._check(TokenType.RPAREN) and not self._at_end() and not self._current().value.upper() in ('DROP', 'RENAME', 'IN', 'WHERE', 'OBS', 'FIRSTOBS'):
                        if self._check(TokenType.IDENTIFIER):
                            vars_list.append(self._current().value)
                        self._advance()
                    ds["options"]["keep"] = vars_list
                elif self._check(TokenType.DROP):
                    self._advance()
                    self._expect(TokenType.EQUALS)
                    vars_list = []
                    while not self._check(TokenType.RPAREN) and not self._at_end() and not self._current().value.upper() in ('KEEP', 'RENAME', 'IN', 'WHERE', 'OBS', 'FIRSTOBS'):
                        if self._check(TokenType.IDENTIFIER):
                            vars_list.append(self._current().value)
                        self._advance()
                    ds["options"]["drop"] = vars_list
                elif self._check(TokenType.RENAME):
                    self._advance()
                    self._expect(TokenType.EQUALS)
                    self._expect(TokenType.LPAREN)
                    renames = {}
                    while not self._check(TokenType.RPAREN) and not self._at_end():
                        old = self._current().value
                        self._advance()
                        self._expect(TokenType.EQUALS)
                        new = self._current().value
                        self._advance()
                        renames[old] = new
                    self._expect(TokenType.RPAREN)
                    ds["options"]["rename"] = renames
                elif self._check(TokenType.IN):
                    self._advance()
                    self._expect(TokenType.EQUALS)
                    ds["options"]["in"] = self._current().value
                    self._advance()
                else:
                    self._advance()
            self._expect(TokenType.RPAREN)

        return ds

    # ──────── Helpers ────────

    def _current(self) -> Token:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return Token(type=TokenType.EOF, value='')

    def _current_type(self) -> TokenType:
        return self._current().type

    def _check(self, ttype: TokenType) -> bool:
        return self._current_type() == ttype

    def _at_end(self) -> bool:
        return self.pos >= len(self.tokens) or self._current_type() == TokenType.EOF

    def _advance(self) -> Token:
        tok = self._current()
        if not self._at_end():
            self.pos += 1
        return tok

    def _expect(self, ttype: TokenType) -> Token:
        if self._check(ttype):
            return self._advance()
        # Graceful recovery: skip
        return self._advance()

    def _peek_is(self, offset: int, ttype: TokenType) -> bool:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx].type == ttype
        return False


def parse(code: str) -> List[DataStep]:
    """Convenience function: tokenize and parse SAS code."""
    tokens = tokenize(code)
    return SASParser(tokens).parse()
