"""
SAS Tokenizer
=============
Tokenizes SAS DATA step code into a stream of typed tokens.
Handles macro variables (&var, &&var, &var.), comments, strings, and SAS keywords.
"""

import re
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Optional


class TokenType(Enum):
    # Keywords
    DATA = auto()
    SET = auto()
    MERGE = auto()
    BY = auto()
    IF = auto()
    THEN = auto()
    ELSE = auto()
    DO = auto()
    END = auto()
    OUTPUT = auto()
    RETAIN = auto()
    KEEP = auto()
    DROP = auto()
    RENAME = auto()
    WHERE = auto()
    FORMAT = auto()
    INFORMAT = auto()
    LABEL = auto()
    LENGTH = auto()
    ARRAY = auto()
    DELETE = auto()
    STOP = auto()
    RUN = auto()
    IN = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    TO = auto()
    WHILE = auto()
    UNTIL = auto()
    DESCENDING = auto()
    SELECT = auto()
    WHEN = auto()
    OTHERWISE = auto()
    LEAVE = auto()
    CONTINUE = auto()
    PUT = auto()
    INPUT = auto()
    RETURN = auto()

    # Macro keywords
    MACRO_LET = auto()       # %LET
    MACRO_IF = auto()        # %IF
    MACRO_THEN = auto()      # %THEN
    MACRO_ELSE = auto()      # %ELSE
    MACRO_DO = auto()        # %DO
    MACRO_END = auto()       # %END
    MACRO_EVAL = auto()      # %EVAL
    MACRO_SYSFUNC = auto()   # %SYSFUNC
    MACRO_STR = auto()       # %STR
    MACRO_NRSTR = auto()     # %NRSTR
    MACRO_SCAN = auto()      # %SCAN
    MACRO_SUBSTR = auto()    # %SUBSTR
    MACRO_UPCASE = auto()    # %UPCASE
    MACRO_LOWCASE = auto()   # %LOWCASE

    # Macro variable references
    MACRO_VAR = auto()       # &var or &var. or &&var

    # Literals
    NUMBER = auto()
    STRING = auto()
    DATE_LITERAL = auto()    # '01JAN2020'd

    # Identifiers and operators
    IDENTIFIER = auto()
    DOT = auto()             # .
    SEMICOLON = auto()       # ;
    LPAREN = auto()          # (
    RPAREN = auto()          # )
    LBRACE = auto()          # {
    RBRACE = auto()          # }
    COMMA = auto()           # ,
    EQUALS = auto()          # =
    PLUS = auto()            # +
    MINUS = auto()           # -
    STAR = auto()            # *
    SLASH = auto()           # /
    LT = auto()              # <
    GT = auto()              # >
    LE = auto()              # <=
    GE = auto()              # >=
    NE = auto()              # ^= or ~= or <>
    COLON = auto()           # :
    AMPERSAND = auto()       # & (standalone)
    HASH = auto()            # #
    PIPE = auto()            # ||
    DOLLAR = auto()          # $

    # Special
    COMMENT = auto()
    NEWLINE = auto()
    EOF = auto()
    UNKNOWN = auto()


@dataclass
class Token:
    type: TokenType
    value: str
    line: int = 0
    col: int = 0
    original: str = ""  # Original text before any processing


# SAS keywords (case-insensitive)
SAS_KEYWORDS = {
    'DATA': TokenType.DATA, 'SET': TokenType.SET, 'MERGE': TokenType.MERGE,
    'BY': TokenType.BY, 'IF': TokenType.IF, 'THEN': TokenType.THEN,
    'ELSE': TokenType.ELSE, 'DO': TokenType.DO, 'END': TokenType.END,
    'OUTPUT': TokenType.OUTPUT, 'RETAIN': TokenType.RETAIN,
    'KEEP': TokenType.KEEP, 'DROP': TokenType.DROP, 'RENAME': TokenType.RENAME,
    'WHERE': TokenType.WHERE, 'FORMAT': TokenType.FORMAT,
    'INFORMAT': TokenType.INFORMAT, 'LABEL': TokenType.LABEL,
    'LENGTH': TokenType.LENGTH, 'ARRAY': TokenType.ARRAY,
    'DELETE': TokenType.DELETE, 'STOP': TokenType.STOP, 'RUN': TokenType.RUN,
    'IN': TokenType.IN, 'AND': TokenType.AND, 'OR': TokenType.OR,
    'NOT': TokenType.NOT, 'TO': TokenType.TO, 'WHILE': TokenType.WHILE,
    'UNTIL': TokenType.UNTIL, 'DESCENDING': TokenType.DESCENDING,
    'SELECT': TokenType.SELECT, 'WHEN': TokenType.WHEN,
    'OTHERWISE': TokenType.OTHERWISE, 'LEAVE': TokenType.LEAVE,
    'CONTINUE': TokenType.CONTINUE, 'PUT': TokenType.PUT,
    'INPUT': TokenType.INPUT, 'RETURN': TokenType.RETURN,
}

# SAS macro keywords
MACRO_KEYWORDS = {
    '%LET': TokenType.MACRO_LET, '%IF': TokenType.MACRO_IF,
    '%THEN': TokenType.MACRO_THEN, '%ELSE': TokenType.MACRO_ELSE,
    '%DO': TokenType.MACRO_DO, '%END': TokenType.MACRO_END,
    '%EVAL': TokenType.MACRO_EVAL, '%SYSFUNC': TokenType.MACRO_SYSFUNC,
    '%STR': TokenType.MACRO_STR, '%NRSTR': TokenType.MACRO_NRSTR,
    '%SCAN': TokenType.MACRO_SCAN, '%SUBSTR': TokenType.MACRO_SUBSTR,
    '%UPCASE': TokenType.MACRO_UPCASE, '%LOWCASE': TokenType.MACRO_LOWCASE,
}


class SASTokenizer:
    """Tokenizes SAS DATA step code into a stream of tokens."""

    def __init__(self, code: str):
        self.code = code
        self.pos = 0
        self.line = 1
        self.col = 1
        self.tokens: List[Token] = []

    def tokenize(self) -> List[Token]:
        """Tokenize the entire SAS code and return list of tokens."""
        self.tokens = []
        while self.pos < len(self.code):
            self._skip_whitespace()
            if self.pos >= len(self.code):
                break

            ch = self.code[self.pos]

            # Comments: /* ... */ or * ... ;
            if ch == '/' and self._peek(1) == '*':
                self._read_block_comment()
                continue
            if ch == '*' and (not self.tokens or
                              self.tokens[-1].type == TokenType.SEMICOLON or
                              self.tokens[-1].type == TokenType.NEWLINE):
                self._read_line_comment()
                continue

            # Macro keywords (%LET, %IF, etc.)
            if ch == '%' and self._peek(1) and self._peek(1).isalpha():
                self._read_macro_keyword()
                continue

            # Macro variable references (&var, &&var, &var.)
            if ch == '&':
                self._read_macro_var()
                continue

            # Strings (single or double quoted)
            if ch in ('"', "'"):
                self._read_string()
                continue

            # Numbers
            if ch.isdigit() or (ch == '.' and self._peek(1) and self._peek(1).isdigit()):
                self._read_number()
                continue

            # Identifiers and keywords
            if ch.isalpha() or ch == '_':
                self._read_identifier()
                continue

            # Two-character operators
            if ch == '|' and self._peek(1) == '|':
                self._add_token(TokenType.PIPE, '||')
                self.pos += 2
                self.col += 2
                continue
            if ch == '<' and self._peek(1) == '=':
                self._add_token(TokenType.LE, '<=')
                self.pos += 2
                self.col += 2
                continue
            if ch == '>' and self._peek(1) == '=':
                self._add_token(TokenType.GE, '>=')
                self.pos += 2
                self.col += 2
                continue
            if ch == '^' and self._peek(1) == '=':
                self._add_token(TokenType.NE, '^=')
                self.pos += 2
                self.col += 2
                continue
            if ch == '~' and self._peek(1) == '=':
                self._add_token(TokenType.NE, '~=')
                self.pos += 2
                self.col += 2
                continue
            if ch == '<' and self._peek(1) == '>':
                self._add_token(TokenType.NE, '<>')
                self.pos += 2
                self.col += 2
                continue
            if ch == 'n' and self._peek(1) == 'e' and not self._peek(2, alpha=True):
                # ne operator
                pass  # handled by identifier

            # Single-character operators
            single_ops = {
                ';': TokenType.SEMICOLON, '(': TokenType.LPAREN,
                ')': TokenType.RPAREN, '{': TokenType.LBRACE,
                '}': TokenType.RBRACE, '[': TokenType.LBRACE,
                ']': TokenType.RBRACE, ',': TokenType.COMMA,
                '=': TokenType.EQUALS, '+': TokenType.PLUS,
                '-': TokenType.MINUS, '*': TokenType.STAR,
                '/': TokenType.SLASH, '<': TokenType.LT,
                '>': TokenType.GT, ':': TokenType.COLON,
                '#': TokenType.HASH, '$': TokenType.DOLLAR,
                '.': TokenType.DOT,
            }

            if ch in single_ops:
                self._add_token(single_ops[ch], ch)
                self.pos += 1
                self.col += 1
                continue

            # Newlines
            if ch == '\n':
                self.pos += 1
                self.line += 1
                self.col = 1
                continue

            # Unknown character
            self._add_token(TokenType.UNKNOWN, ch)
            self.pos += 1
            self.col += 1

        self._add_token(TokenType.EOF, '')
        return self.tokens

    def _peek(self, offset=1, alpha=False):
        """Look ahead by offset characters."""
        idx = self.pos + offset
        if idx >= len(self.code):
            return None
        ch = self.code[idx]
        if alpha:
            return ch.isalpha()
        return ch

    def _skip_whitespace(self):
        """Skip spaces and tabs (but not newlines)."""
        while self.pos < len(self.code) and self.code[self.pos] in (' ', '\t', '\r', '\n'):
            if self.code[self.pos] == '\n':
                self.line += 1
                self.col = 1
            else:
                self.col += 1
            self.pos += 1

    def _add_token(self, ttype: TokenType, value: str, original: str = None):
        """Add a token to the list."""
        self.tokens.append(Token(
            type=ttype,
            value=value,
            line=self.line,
            col=self.col,
            original=original or value
        ))

    def _read_block_comment(self):
        """Read a /* ... */ block comment."""
        start = self.pos
        self.pos += 2
        self.col += 2
        while self.pos < len(self.code):
            if self.code[self.pos] == '*' and self._peek(1) == '/':
                self.pos += 2
                self.col += 2
                break
            if self.code[self.pos] == '\n':
                self.line += 1
                self.col = 1
            else:
                self.col += 1
            self.pos += 1

    def _read_line_comment(self):
        """Read a * ... ; line comment."""
        start = self.pos
        self.pos += 1
        self.col += 1
        while self.pos < len(self.code) and self.code[self.pos] != ';':
            if self.code[self.pos] == '\n':
                self.line += 1
                self.col = 1
            else:
                self.col += 1
            self.pos += 1
        if self.pos < len(self.code):
            self.pos += 1  # skip ;
            self.col += 1

    def _read_macro_keyword(self):
        """Read a %KEYWORD macro."""
        start = self.pos
        self.pos += 1  # skip %
        self.col += 1
        while self.pos < len(self.code) and (self.code[self.pos].isalnum() or self.code[self.pos] == '_'):
            self.pos += 1
            self.col += 1
        word = self.code[start:self.pos].upper()
        if word in MACRO_KEYWORDS:
            self._add_token(MACRO_KEYWORDS[word], word, self.code[start:self.pos])
        else:
            # Treat as identifier with % prefix (e.g., %MACRO, %MEND, user macros)
            self._add_token(TokenType.IDENTIFIER, self.code[start:self.pos])

    def _read_macro_var(self):
        """Read a macro variable reference: &var, &&var, &var."""
        start = self.pos
        # Count leading ampersands
        while self.pos < len(self.code) and self.code[self.pos] == '&':
            self.pos += 1
            self.col += 1

        if self.pos >= len(self.code) or not (self.code[self.pos].isalpha() or self.code[self.pos] == '_'):
            # Standalone ampersand
            self._add_token(TokenType.AMPERSAND, '&')
            return

        # Read variable name
        while self.pos < len(self.code) and (self.code[self.pos].isalnum() or self.code[self.pos] == '_'):
            self.pos += 1
            self.col += 1

        # Optional trailing dot
        if self.pos < len(self.code) and self.code[self.pos] == '.':
            self.pos += 1
            self.col += 1

        text = self.code[start:self.pos]
        self._add_token(TokenType.MACRO_VAR, text, text)

    def _read_string(self):
        """Read a quoted string. Handles both single and double quotes."""
        quote = self.code[self.pos]
        start = self.pos
        self.pos += 1
        self.col += 1
        value_chars = []

        while self.pos < len(self.code):
            ch = self.code[self.pos]
            if ch == quote:
                self.pos += 1
                self.col += 1
                # Check for escaped quote (doubled)
                if self.pos < len(self.code) and self.code[self.pos] == quote:
                    value_chars.append(quote)
                    self.pos += 1
                    self.col += 1
                    continue
                break
            if ch == '\n':
                self.line += 1
                self.col = 1
            else:
                self.col += 1
            value_chars.append(ch)
            self.pos += 1

        value = ''.join(value_chars)
        original = self.code[start:self.pos]

        # Check for date/time/datetime literal suffix
        if self.pos < len(self.code) and self.code[self.pos].lower() in ('d', 't', 'n'):
            suffix = self.code[self.pos].lower()
            self.pos += 1
            self.col += 1
            self._add_token(TokenType.DATE_LITERAL, value, original + suffix)
        else:
            self._add_token(TokenType.STRING, value, original)

    def _read_number(self):
        """Read a numeric literal."""
        start = self.pos
        has_dot = False

        while self.pos < len(self.code):
            ch = self.code[self.pos]
            if ch.isdigit():
                self.pos += 1
                self.col += 1
            elif ch == '.' and not has_dot:
                has_dot = True
                self.pos += 1
                self.col += 1
            elif ch.lower() == 'e' and self.pos > start:
                self.pos += 1
                self.col += 1
                if self.pos < len(self.code) and self.code[self.pos] in ('+', '-'):
                    self.pos += 1
                    self.col += 1
            else:
                break

        text = self.code[start:self.pos]
        self._add_token(TokenType.NUMBER, text)

    def _read_identifier(self):
        """Read an identifier or keyword."""
        start = self.pos
        while self.pos < len(self.code) and (self.code[self.pos].isalnum() or self.code[self.pos] == '_'):
            self.pos += 1
            self.col += 1

        word = self.code[start:self.pos]
        upper = word.upper()

        # Check for SAS comparison operators: EQ, NE, LT, GT, LE, GE, IN
        if upper == 'EQ':
            self._add_token(TokenType.EQUALS, '=', word)
        elif upper == 'NE':
            self._add_token(TokenType.NE, '<>', word)
        elif upper == 'LT':
            self._add_token(TokenType.LT, '<', word)
        elif upper == 'GT':
            self._add_token(TokenType.GT, '>', word)
        elif upper == 'LE':
            self._add_token(TokenType.LE, '<=', word)
        elif upper == 'GE':
            self._add_token(TokenType.GE, '>=', word)
        elif upper in SAS_KEYWORDS:
            self._add_token(SAS_KEYWORDS[upper], word, word)
        else:
            # Check for SAS format/informat references like date9. or $char20.
            if self.pos < len(self.code) and self.code[self.pos] == '.':
                # Could be a format reference (e.g., date9.) or a dataset.variable
                # Peek ahead to decide
                next_pos = self.pos + 1
                if next_pos < len(self.code) and (self.code[next_pos].isalpha() or self.code[next_pos] == '_'):
                    # It's a dotted name (e.g., lib.table or first.var)
                    self._add_token(TokenType.IDENTIFIER, word, word)
                else:
                    # It's a format reference like date9.
                    self.pos += 1
                    self.col += 1
                    self._add_token(TokenType.IDENTIFIER, word + '.', word + '.')
            else:
                self._add_token(TokenType.IDENTIFIER, word, word)


def tokenize(code: str) -> List[Token]:
    """Convenience function to tokenize SAS code."""
    return SASTokenizer(code).tokenize()
