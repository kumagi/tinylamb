"""Portable, deterministic workloads; no engine-specific fast-path substitutions."""

import calendar
import datetime
import decimal
import math
import numbers
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OLTP = (
    "noop", "point_read", "nonkey_read", "range_read", "update",
    "hot_update", "transfer", "read_write", "insert_delete", "rollback", "htap",
)


def olap_queries(rows):
    return {
        "count": "SELECT COUNT(*) FROM sales",
        "sum": "SELECT SUM(amount), SUM(qty) FROM sales",
        "filter_1pct": "SELECT SUM(amount) FROM sales WHERE category = 7",
        "filter_10pct": "SELECT SUM(amount) FROM sales WHERE category < 10",
        "expression": "SELECT SUM(amount * qty + day) FROM sales WHERE amount > 5000",
        "group_100": "SELECT category, COUNT(*), SUM(amount) FROM sales GROUP BY category ORDER BY category",
        "group_10000": "SELECT account_id, SUM(amount) FROM sales GROUP BY account_id ORDER BY account_id",
        "distinct": "SELECT COUNT(DISTINCT account_id) FROM sales",
        "top_100": "SELECT id, amount FROM sales ORDER BY amount DESC, id LIMIT 100",
        "join": "SELECT COUNT(*), SUM(s.amount) FROM sales s JOIN accounts a ON s.account_id = a.id WHERE a.bucket < 10",
        "join_group": "SELECT a.bucket, SUM(s.amount) FROM sales s JOIN accounts a ON s.account_id = a.id GROUP BY a.bucket ORDER BY a.bucket",
        "left_join": "SELECT a.bucket, COUNT(s.id) FROM accounts a LEFT JOIN sales s ON a.id = s.account_id AND s.category < 10 GROUP BY a.bucket ORDER BY a.bucket",
        "exists": "SELECT COUNT(*) FROM accounts a WHERE EXISTS (SELECT 1 FROM sales s WHERE s.account_id = a.id AND s.category = 7)",
        "scalar_subquery": "SELECT COUNT(*) FROM sales WHERE amount > (SELECT AVG(amount) FROM sales)",
        "cte": "WITH totals AS (SELECT category, SUM(amount) AS total FROM sales GROUP BY category) SELECT SUM(total) FROM totals WHERE total > 1000",
        "having": "SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 10000 ORDER BY category",
        "union_all": "SELECT SUM(n) FROM (SELECT COUNT(*) AS n FROM sales WHERE category < 10 UNION ALL SELECT COUNT(*) AS n FROM sales WHERE category >= 90) u",
        "window": "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC, id) AS rn FROM sales WHERE id < 1000 ORDER BY id",
    }


def schema():
    return [
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, bucket BIGINT, balance BIGINT, padding VARCHAR(80))",
        "CREATE TABLE sales (id BIGINT, account_id BIGINT, category BIGINT, amount BIGINT, qty BIGINT, day BIGINT, label VARCHAR(32))",
        "CREATE TABLE churn (id BIGINT PRIMARY KEY, value BIGINT)",
    ]


def secondary_indexes(suite):
    """Identical secondary keys; physical index types differ (DuckDB ART)."""
    columns = ({"accounts": ["bucket, id"],
                "sales": ["account_id", "category, amount", "amount, id"]}
               if suite == "micro" else
               {"lineitem": ["l_orderkey", "l_shipdate", "l_partkey, l_suppkey"],
                "orders": ["o_orderkey", "o_orderdate", "o_custkey"],
                "customer": ["c_custkey", "c_mktsegment"],
                "supplier": ["s_suppkey", "s_nationkey"],
                "part": ["p_partkey", "p_size, p_type"],
                "partsupp": ["ps_partkey, ps_suppkey"],
                "nation": ["n_nationkey", "n_regionkey"],
                "region": ["r_regionkey"]})
    return [f"CREATE INDEX bench_{table}_{i} ON {table} ({keys})"
            for table, indexes in columns.items() for i, keys in enumerate(indexes)]


def accounts(rows):
    for i in range(rows):
        yield (i, i % 100, 100000, "x" * 64)


def sales(rows, account_rows):
    for i in range(rows):
        # Multipliers are coprime to the key domains used by the default profiles.
        yield (i, (i * 7919) % account_rows, (i * 37) % 100,
               (i * 104729) % 10000 + 1, i % 20 + 1, i % 365, f"item-{i % 1000:04}")


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def insert_sql(table, batch):
    return f"INSERT INTO {table} VALUES " + ",".join(
        "(" + ",".join(map(literal, row)) + ")" for row in batch
    )


def transaction(name, rng, worker, sequence, account_rows):
    key = rng.randrange(account_rows)
    if name == "noop":
        return ["SELECT 1"], 0
    if name == "point_read":
        return [f"SELECT balance, padding FROM accounts WHERE id = {key}"], 0
    if name == "nonkey_read":
        return [f"SELECT id, balance FROM accounts WHERE bucket = {key % 100} ORDER BY id LIMIT 10"], 0
    if name == "range_read":
        return [f"SELECT SUM(balance) FROM accounts WHERE id BETWEEN {key} AND {min(key + 99, account_rows - 1)}"], 0
    if name in ("update", "hot_update"):
        key = key if name == "update" else key % 16
        return [f"UPDATE accounts SET balance = balance + 1 WHERE id = {key}"], 1
    if name in ("transfer", "read_write", "htap"):
        # Globally ordered write acquisition avoids deliberate deadlocks.
        other = (key + 1 + rng.randrange(account_rows - 1)) % account_rows
        lo, hi = sorted((key, other))
        sql = ["BEGIN"]
        if name in ("read_write", "htap"):
            sql.extend(f"SELECT balance FROM accounts WHERE id = {rng.randrange(account_rows)}" for _ in range(4))
        sql.extend((f"UPDATE accounts SET balance = balance - 1 WHERE id = {lo}",
                    f"UPDATE accounts SET balance = balance + 1 WHERE id = {hi}", "COMMIT"))
        return sql, 0
    if name == "insert_delete":
        unique = worker * 1000000000 + sequence
        return ["BEGIN", f"INSERT INTO churn VALUES ({unique}, 1)",
                f"DELETE FROM churn WHERE id = {unique}", "COMMIT"], 0
    if name == "rollback":
        return ["BEGIN", f"UPDATE accounts SET balance = balance + 1 WHERE id = {key}", "ROLLBACK"], 0
    raise ValueError(name)


def equivalent(left, right):
    """Compare ordered rows, exact integers/NULL/text and tolerant FP aggregates."""
    if len(left) != len(right):
        return False
    for a, b in zip(left, right):
        if len(a) != len(b):
            return False
        for x, y in zip(a, b):
            if isinstance(x, numbers.Number) and isinstance(y, numbers.Number):
                if isinstance(x, (int, decimal.Decimal)) and isinstance(y, (int, decimal.Decimal)):
                    if x != y:
                        return False
                elif not math.isclose(float(x), float(y), rel_tol=1e-7, abs_tol=0.01):
                    return False
            elif x != y:
                return False
    return True


def tpch_schema():
    # Keep the existing benchmark as the source of truth for column order/types.
    source = (ROOT / "benchmark/tpch_benchmark.cpp").read_text()
    tables = []
    for match in re.finditer(r'\.name = "(\w+)".*?\.ddl\s*=\s*((?:"[^"]*"\s*)+),\s*\.fields = \{(.*?)\}', source, re.S):
        name, strings, fields = match.groups()
        ddl = "".join(re.findall(r'"([^"]*)"', strings))
        ddl = re.sub(r"\bINT64\b", "BIGINT", ddl)
        ddl = re.sub(r"\bSTRING\b", "VARCHAR(255)", ddl)
        ddl = re.sub(r"\bNUMERIC\b", "DOUBLE PRECISION", ddl)
        tables.append((name, ddl, re.findall(r"FieldKind::k(\w+)", fields)))
    if len(tables) != 8:
        raise ValueError("could not read eight TPC-H schemas")
    return tables


def tpch_queries(scale):
    queries = re.findall(r'R"sql\((.*?)\)sql"', (ROOT / "benchmark/tpch_queries.hpp").read_text(), re.S)
    if len(queries) != 22:
        raise ValueError("expected 22 TPC-H query templates")
    queries[10] = queries[10].replace("0.0001000000", format(0.0001 / scale, ".17g"))
    return {f"Q{i:02}": q for i, q in enumerate(queries, 1)}


def adapt_sql(sql, engine):
    if engine == "tinylamb":
        sql = sql.replace("DOUBLE PRECISION", "FLOAT64")
    # The templates only use literal date_add/sub; fold these identically on all engines.
    def date_arithmetic(match):
        operation, date, count, unit = match.groups()
        value = datetime.date.fromisoformat(date)
        count = int(count) * (1 if operation.lower() == "add" else -1)
        if unit.lower() == "day":
            value += datetime.timedelta(days=count)
        else:
            months = value.year * 12 + value.month - 1 + count * (12 if unit.lower() == "year" else 1)
            year, month = divmod(months, 12)
            value = value.replace(year=year, month=month + 1, day=min(value.day, calendar.monthrange(year, month + 1)[1]))
        return f"DATE '{value.isoformat()}'"
    sql = re.sub(r"date_(add|sub)\(date\s+'([\d-]+)',\s*INTERVAL\s+(\d+)\s+(day|month|year)\)", date_arithmetic, sql, flags=re.I)
    if engine == 'oracle':
        from oracle_support import adapt_sql as oracle_sql
        sql = oracle_sql(sql)
    return sql
