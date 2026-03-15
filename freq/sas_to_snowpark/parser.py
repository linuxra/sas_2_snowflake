"""
SAS PROC FREQ Parser
====================
Parses all major variations of SAS PROC FREQ syntax into an intermediate
representation (ProcFreqBlock / TableSpec) that the generator can consume.

Supported SAS syntax elements
------------------------------
PROC FREQ DATA=<libref.>dataset <ORDER=FREQ|DATA|FORMATTED|INTERNAL> <NLEVELS> <PAGE>;
  TABLES var1 <* var2 <* var3 ...>>  / <options> ;
  TABLES (var1 var2) * var3           / <options> ;   -- parenthesised expansions
  BY var1 <DESCENDING> var2 ;
  WEIGHT var ;
  WHERE <expression> ;
  FORMAT var1 fmt1. var2 fmt2. ;
  OUTPUT OUT=dsname <OUTCUM> <OUTPCT> <OUTEXPECT> <keyword=name ...> ;
RUN;

Table options parsed
--------------------
CHISQ, CMH, MEASURES, FISHER, EXACT <test-list>, BINOMIAL, AGREE, TREND,
RELRISK, RISKDIFF, OR, SCORES=<type>,
NOCUM, NOPERCENT, NOFREQ, NOROW, NOCOL, NOPRINT,
MISSING, MISSPRINT, SPARSE, LIST, CROSSLIST,
OUT=dataset, OUTPCT, OUTCUM, OUTEXPECT,
ALPHA=<value>, CL, PLCL, CLWT, CELLCHI2, EXPECTED, DEVIATION, TOTPCT
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple


# ---------------------------------------------------------------------------
# Data classes for the intermediate representation
# ---------------------------------------------------------------------------

@dataclass
class TableSpec:
    """One TABLES statement (possibly multi-way)."""

    # Variables in cross-tab order, e.g. ["region", "product"] for region*product
    variables: List[str] = field(default_factory=list)

    # If parenthesised expansions were used we store multiple combos
    # e.g. TABLES (a b) * c  =>  expansions = [["a","c"], ["b","c"]]
    expansions: List[List[str]] = field(default_factory=list)

    # --- boolean / simple options ----
    chisq: bool = False
    cmh: bool = False
    measures: bool = False
    fisher: bool = False
    exact_tests: List[str] = field(default_factory=list)  # e.g. ["CHISQ","FISHER"]
    binomial: bool = False
    agree: bool = False
    trend: bool = False
    relrisk: bool = False
    riskdiff: bool = False
    odds_ratio: bool = False

    nocum: bool = False
    nopercent: bool = False
    nofreq: bool = False
    norow: bool = False
    nocol: bool = False
    noprint: bool = False
    missing: bool = False
    missprint: bool = False
    sparse: bool = False

    list_format: bool = False       # LIST
    crosslist_format: bool = False   # CROSSLIST

    cellchi2: bool = False
    expected: bool = False
    deviation: bool = False
    totpct: bool = False
    cl: bool = False
    plcl: bool = False
    clwt: bool = False

    # --- valued options ---
    out_dataset: Optional[str] = None
    outpct: bool = False
    outcum: bool = False
    outexpect: bool = False
    alpha: Optional[float] = None
    scores: Optional[str] = None      # TABLE, RANK, RIDIT, MODRIDIT


@dataclass
class OutputSpec:
    """Standalone OUTPUT statement."""
    out_dataset: str = ""
    outcum: bool = False
    outpct: bool = False
    outexpect: bool = False
    keyword_vars: Dict[str, str] = field(default_factory=dict)  # e.g. {"PCHI": "p_value"}


@dataclass
class ProcFreqBlock:
    """Complete parsed representation of one PROC FREQ … RUN; block."""

    input_dataset: str = ""
    library: Optional[str] = None

    # PROC-level options
    order: Optional[str] = None     # FREQ, DATA, FORMATTED, INTERNAL
    nlevels: bool = False
    page: bool = False

    tables: List[TableSpec] = field(default_factory=list)

    by_vars: List[str] = field(default_factory=list)
    by_descending: Dict[str, bool] = field(default_factory=dict)

    weight_var: Optional[str] = None

    where_clause: Optional[str] = None

    formats: Dict[str, str] = field(default_factory=dict)  # var -> format

    output_specs: List[OutputSpec] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class ProcFreqParser:
    """
    Parse a SAS PROC FREQ block (from ``PROC FREQ`` to ``RUN;`` or ``QUIT;``)
    and return a :class:`ProcFreqBlock`.
    """

    # Regex helpers --------------------------------------------------------

    _RE_PROC_FREQ = re.compile(
        r"PROC\s+FREQ\b", re.IGNORECASE
    )
    _RE_SEMICOLON = re.compile(r";")

    # Match DATA=<lib.>dataset
    _RE_DATA = re.compile(
        r"DATA\s*=\s*(?:(\w+)\.)?(\w+)", re.IGNORECASE
    )
    _RE_ORDER = re.compile(
        r"ORDER\s*=\s*(FREQ|DATA|FORMATTED|INTERNAL)", re.IGNORECASE
    )
    _RE_NLEVELS = re.compile(r"\bNLEVELS\b", re.IGNORECASE)
    _RE_PAGE = re.compile(r"\bPAGE\b", re.IGNORECASE)

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def parse(self, sas_code: str) -> List[ProcFreqBlock]:
        """
        Parse *all* PROC FREQ blocks found in ``sas_code``.
        Returns a list of :class:`ProcFreqBlock` objects.
        """
        blocks: List[ProcFreqBlock] = []
        # Find each PROC FREQ … (RUN|QUIT); region
        pattern = re.compile(
            r"PROC\s+FREQ\b(.*?)(?:RUN|QUIT)\s*;",
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(sas_code):
            raw = match.group(0)
            blocks.append(self._parse_block(raw))
        return blocks

    # -----------------------------------------------------------------------
    # Internal: block-level parsing
    # -----------------------------------------------------------------------

    def _parse_block(self, raw: str) -> ProcFreqBlock:
        block = ProcFreqBlock()

        # ---- Split into statements (by semicolons) ----
        # First, grab everything after PROC FREQ ... ; as the proc-line
        proc_match = re.match(
            r"PROC\s+FREQ\b(.*?);", raw, re.IGNORECASE | re.DOTALL
        )
        if proc_match:
            self._parse_proc_options(proc_match.group(1).strip(), block)

        # Remaining statements
        rest = raw[proc_match.end():] if proc_match else raw
        statements = self._split_statements(rest)

        for stmt in statements:
            stmt_stripped = stmt.strip()
            upper = stmt_stripped.upper()
            if upper.startswith("TABLES") or upper.startswith("TABLE"):
                block.tables.append(self._parse_tables_stmt(stmt_stripped))
            elif upper.startswith("BY"):
                self._parse_by(stmt_stripped, block)
            elif upper.startswith("WEIGHT"):
                self._parse_weight(stmt_stripped, block)
            elif upper.startswith("WHERE"):
                self._parse_where(stmt_stripped, block)
            elif upper.startswith("FORMAT"):
                self._parse_format(stmt_stripped, block)
            elif upper.startswith("OUTPUT"):
                block.output_specs.append(self._parse_output(stmt_stripped))
            # RUN / QUIT handled by outer regex

        return block

    # ---- Proc-line options -------------------------------------------------

    def _parse_proc_options(self, text: str, block: ProcFreqBlock):
        m = self._RE_DATA.search(text)
        if m:
            block.library = m.group(1)
            block.input_dataset = m.group(2)

        m = self._RE_ORDER.search(text)
        if m:
            block.order = m.group(1).upper()

        if self._RE_NLEVELS.search(text):
            block.nlevels = True
        if self._RE_PAGE.search(text):
            block.page = True

    # ---- TABLES statement --------------------------------------------------

    def _parse_tables_stmt(self, stmt: str) -> TableSpec:
        spec = TableSpec()

        # Separate variable part from options after "/"
        # Remove leading TABLE(S) keyword
        body = re.sub(r"^TABLES?\s+", "", stmt, flags=re.IGNORECASE).strip()

        if "/" in body:
            var_part, opt_part = body.split("/", 1)
        else:
            var_part = body
            opt_part = ""

        var_part = var_part.strip()
        opt_part = opt_part.strip()

        # --- Parse variable specification ---
        self._parse_table_vars(var_part, spec)

        # --- Parse options ---
        self._parse_table_options(opt_part, spec)

        return spec

    def _parse_table_vars(self, var_part: str, spec: TableSpec):
        """
        Handle forms:
          var1
          var1*var2
          var1*var2*var3
          (var1 var2)*var3
          var1*(var2 var3)
          (a b)*(c d)
        """
        # Check for parenthesised groups
        paren_pattern = re.compile(r"\(([^)]+)\)")
        groups = paren_pattern.findall(var_part)

        if groups:
            # Replace parenthesised groups with placeholders
            segments = []
            remaining = var_part
            idx = 0
            for g in paren_pattern.finditer(var_part):
                pre = remaining[:g.start() - idx]
                remaining = remaining[g.end() - idx:]
                idx = g.end()
                # pre might contain standalone vars joined by *
                pre_vars = [v.strip() for v in pre.replace("*", " ").split() if v.strip()]
                group_vars = g.group(1).split()
                segments.append(("group", [v.strip() for v in group_vars]))
                if pre_vars:
                    segments.insert(len(segments) - 1, ("singles", pre_vars))

            # Any trailing
            tail_vars = [v.strip() for v in remaining.replace("*", " ").split() if v.strip()]
            if tail_vars:
                segments.append(("singles", tail_vars))

            # Build expansions using cartesian product
            self._expand_groups(var_part, spec)
        else:
            # Simple: split on *
            vars_list = [v.strip() for v in var_part.split("*") if v.strip()]
            spec.variables = vars_list

    def _expand_groups(self, var_part: str, spec: TableSpec):
        """
        Expand parenthesised variable groups into individual table combos.
        E.g. ``(a b) * c`` -> [["a","c"], ["b","c"]]
        """
        # Tokenise: each token is either a group [...vars...] or a single var
        tokens = []
        i = 0
        text = var_part.strip()
        while i < len(text):
            if text[i] == "(":
                j = text.index(")", i)
                inner = text[i + 1:j].split()
                tokens.append([v.strip() for v in inner if v.strip()])
                i = j + 1
            elif text[i] == "*":
                i += 1
            elif text[i].strip():
                # read a single variable name
                m = re.match(r"(\w+)", text[i:])
                if m:
                    tokens.append([m.group(1)])
                    i += m.end()
                else:
                    i += 1
            else:
                i += 1

        # Cartesian product of token lists
        from itertools import product as cart_product
        combos = list(cart_product(*tokens))
        spec.expansions = [list(c) for c in combos]
        # Also store the first combo as .variables for convenience
        if combos:
            spec.variables = list(combos[0])

    def _parse_table_options(self, opt_text: str, spec: TableSpec):
        upper = opt_text.upper()
        tokens = upper.split()

        # Boolean flags (order matches typical SAS documentation)
        flag_map = {
            "CHISQ": "chisq",
            "CMH": "cmh",
            "MEASURES": "measures",
            "FISHER": "fisher",
            "BINOMIAL": "binomial",
            "AGREE": "agree",
            "TREND": "trend",
            "RELRISK": "relrisk",
            "RISKDIFF": "riskdiff",
            "OR": "odds_ratio",
            "NOCUM": "nocum",
            "NOPERCENT": "nopercent",
            "NOFREQ": "nofreq",
            "NOROW": "norow",
            "NOCOL": "nocol",
            "NOPRINT": "noprint",
            "MISSING": "missing",
            "MISSPRINT": "missprint",
            "SPARSE": "sparse",
            "LIST": "list_format",
            "CROSSLIST": "crosslist_format",
            "CELLCHI2": "cellchi2",
            "EXPECTED": "expected",
            "DEVIATION": "deviation",
            "TOTPCT": "totpct",
            "CL": "cl",
            "PLCL": "plcl",
            "CLWT": "clwt",
            "OUTPCT": "outpct",
            "OUTCUM": "outcum",
            "OUTEXPECT": "outexpect",
        }

        for token in tokens:
            clean = token.strip(",").strip()
            if clean in flag_map:
                setattr(spec, flag_map[clean], True)

        # OUT= dataset
        m = re.search(r"OUT\s*=\s*(\w+)", opt_text, re.IGNORECASE)
        if m:
            spec.out_dataset = m.group(1)

        # ALPHA= value
        m = re.search(r"ALPHA\s*=\s*([\d.]+)", opt_text, re.IGNORECASE)
        if m:
            spec.alpha = float(m.group(1))

        # SCORES= type
        m = re.search(r"SCORES\s*=\s*(\w+)", opt_text, re.IGNORECASE)
        if m:
            spec.scores = m.group(1).upper()

        # EXACT with optional test list  e.g. EXACT CHISQ FISHER
        exact_match = re.search(
            r"\bEXACT\b\s*((?:\w+\s*)*)", opt_text, re.IGNORECASE
        )
        if exact_match:
            tests = exact_match.group(1).strip().split()
            spec.exact_tests = [t.upper() for t in tests if t.upper() not in flag_map or t.upper() in ("CHISQ", "FISHER", "BINOMIAL", "TREND")]
            if not spec.exact_tests:
                spec.exact_tests = ["CHISQ"]  # default

    # ---- BY statement ------------------------------------------------------

    def _parse_by(self, stmt: str, block: ProcFreqBlock):
        body = re.sub(r"^BY\s+", "", stmt, flags=re.IGNORECASE).strip()
        tokens = body.split()
        i = 0
        while i < len(tokens):
            if tokens[i].upper() == "DESCENDING" and i + 1 < len(tokens):
                var = tokens[i + 1]
                block.by_vars.append(var)
                block.by_descending[var] = True
                i += 2
            else:
                block.by_vars.append(tokens[i])
                i += 1

    # ---- WEIGHT statement --------------------------------------------------

    def _parse_weight(self, stmt: str, block: ProcFreqBlock):
        body = re.sub(r"^WEIGHT\s+", "", stmt, flags=re.IGNORECASE).strip()
        block.weight_var = body.split()[0] if body else None

    # ---- WHERE clause ------------------------------------------------------

    def _parse_where(self, stmt: str, block: ProcFreqBlock):
        body = re.sub(r"^WHERE\s+", "", stmt, flags=re.IGNORECASE).strip()
        # Strip optional outer parens
        if body.startswith("(") and body.endswith(")"):
            body = body[1:-1].strip()
        block.where_clause = body

    # ---- FORMAT statement --------------------------------------------------

    def _parse_format(self, stmt: str, block: ProcFreqBlock):
        body = re.sub(r"^FORMAT\s+", "", stmt, flags=re.IGNORECASE).strip()
        # Pairs: var fmt.  (the format may start with $ and ends with a period)
        pairs = re.findall(r"(\w+)\s+(\$?\w+\.?\w*\.)", body, re.IGNORECASE)
        for var, fmt in pairs:
            block.formats[var] = fmt

    # ---- OUTPUT statement --------------------------------------------------

    def _parse_output(self, stmt: str) -> OutputSpec:
        ospec = OutputSpec()
        body = re.sub(r"^OUTPUT\s+", "", stmt, flags=re.IGNORECASE).strip()
        m = re.search(r"OUT\s*=\s*(\w+)", body, re.IGNORECASE)
        if m:
            ospec.out_dataset = m.group(1)
        if re.search(r"\bOUTCUM\b", body, re.IGNORECASE):
            ospec.outcum = True
        if re.search(r"\bOUTPCT\b", body, re.IGNORECASE):
            ospec.outpct = True
        if re.search(r"\bOUTEXPECT\b", body, re.IGNORECASE):
            ospec.outexpect = True

        # keyword=name pairs  e.g.  PCHI=p_val
        for km in re.finditer(r"(\w+)\s*=\s*(\w+)", body, re.IGNORECASE):
            key, val = km.group(1).upper(), km.group(2)
            if key not in ("OUT",):
                ospec.keyword_vars[key] = val

        return ospec

    # ---- Utility -----------------------------------------------------------

    @staticmethod
    def _split_statements(text: str) -> List[str]:
        """Split SAS text on semicolons, ignoring RUN/QUIT."""
        parts = re.split(r";", text)
        result = []
        for p in parts:
            s = p.strip()
            if s and s.upper() not in ("RUN", "QUIT", ""):
                result.append(s)
        return result
