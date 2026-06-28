"""A small hand-written tokenizer + recursive-descent parser for MiniDB SQL.

Supported grammar (case-insensitive keywords):

    CREATE TABLE t (col INT|TEXT [PRIMARY KEY], ...)
    INSERT INTO t VALUES (v, ...)
    SELECT a, b | *  FROM t
        [JOIN t2 ON t.x = t2.y]
        [WHERE p [AND p ...]]
    DELETE FROM t [WHERE p [AND p ...]]

A predicate p is:  column OP value   where OP in = != < <= > >=
"""

import re

from .ast import (CreateTable, Insert, Select, Delete, Predicate, JoinClause)

_TOKEN = re.compile(r"""
    \s*(?:
        (?P<punc>[(),*]) |
        (?P<op><=|>=|!=|=|<|>) |
        (?P<num>-?\d+(?:\.\d+)?) |
        (?P<str>'[^']*') |
        (?P<word>[A-Za-z_][A-Za-z0-9_.]*)
    )
""", re.VERBOSE)


class ParseError(Exception):
    pass


def tokenize(sql: str):
    tokens, pos = [], 0
    sql = sql.strip().rstrip(";")
    while pos < len(sql):
        m = _TOKEN.match(sql, pos)
        if not m or m.end() == pos:
            if sql[pos:].strip() == "":
                break
            raise ParseError(f"unexpected input near: {sql[pos:pos+20]!r}")
        pos = m.end()
        kind = m.lastgroup
        val = m.group(kind)
        tokens.append((kind, val))
    return tokens


class _Parser:
    def __init__(self, tokens):
        self.t = tokens
        self.i = 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def next(self):
        tok = self.peek()
        self.i += 1
        return tok

    def expect_word(self, word):
        kind, val = self.next()
        if kind != "word" or val.lower() != word.lower():
            raise ParseError(f"expected {word!r}, got {val!r}")

    def expect_punc(self, p):
        kind, val = self.next()
        if val != p:
            raise ParseError(f"expected {p!r}, got {val!r}")

    def _value(self):
        kind, val = self.next()
        if kind == "num":
            return float(val) if "." in val else int(val)
        if kind == "str":
            return val[1:-1]
        if kind == "word":              # bare word value (treat as text)
            return val
        raise ParseError(f"expected a value, got {val!r}")

    # --- statements ---------------------------------------------------------
    def parse(self):
        kind, val = self.peek()
        if kind != "word":
            raise ParseError("empty statement")
        kw = val.lower()
        if kw == "create":
            return self._create()
        if kw == "insert":
            return self._insert()
        if kw == "select":
            return self._select()
        if kw == "delete":
            return self._delete()
        raise ParseError(f"unsupported statement: {val}")

    def _create(self):
        self.expect_word("create")
        self.expect_word("table")
        _, name = self.next()
        self.expect_punc("(")
        cols, pk = [], 0
        while True:
            _, cname = self.next()
            _, ctype = self.next()
            ctype = ctype.upper()
            # optional PRIMARY KEY
            kind, val = self.peek()
            if kind == "word" and val.lower() == "primary":
                self.next(); self.expect_word("key")
                pk = len(cols)
            cols.append((cname, ctype))
            kind, val = self.next()
            if val == ")":
                break
            if val != ",":
                raise ParseError("expected , or ) in column list")
        return CreateTable(name, cols, pk)

    def _insert(self):
        self.expect_word("insert")
        self.expect_word("into")
        _, name = self.next()
        self.expect_word("values")
        self.expect_punc("(")
        values = []
        while True:
            values.append(self._value())
            kind, val = self.next()
            if val == ")":
                break
            if val != ",":
                raise ParseError("expected , or ) in values")
        return Insert(name, values)

    def _columns(self):
        cols = []
        while True:
            kind, val = self.next()
            cols.append(val)               # '*' or column name
            kind, val = self.peek()
            if val == ",":
                self.next()
                continue
            break
        return cols

    def _select(self):
        self.expect_word("select")
        cols = self._columns()
        self.expect_word("from")
        _, table = self.next()
        joins, where = [], []
        while True:
            kind, val = self.peek()
            if kind == "word" and val.lower() == "join":
                self.next()
                _, jt = self.next()
                self.expect_word("on")
                _, lcol = self.next()
                self.expect_punc("=")
                _, rcol = self.next()
                joins.append(JoinClause(jt, lcol, rcol))
            elif kind == "word" and val.lower() == "where":
                where = self._where()
                break
            else:
                break
        return Select(cols, table, joins, where)

    def _delete(self):
        self.expect_word("delete")
        self.expect_word("from")
        _, table = self.next()
        where = []
        kind, val = self.peek()
        if kind == "word" and val.lower() == "where":
            where = self._where()
        return Delete(table, where)

    def _where(self):
        self.expect_word("where")
        preds = []
        while True:
            _, col = self.next()
            _, op = self.next()
            value = self._value()
            preds.append(Predicate(col, op, value))
            kind, val = self.peek()
            if kind == "word" and val.lower() == "and":
                self.next()
                continue
            break
        return preds


def parse(sql: str):
    return _Parser(tokenize(sql)).parse()
