#!/usr/bin/env python3
"""Fixed-SF grouped-bar comparison of five *derived* analytics benchmarks.

This is a standalone tool (see README-analytics.md).  It does not modify the
existing 3-product harness.  The five families reproduce the *shape* (schema,
join graph, skewed / correlated / NULL-bearing columns) of well known stress
benchmarks with synthetic, SF-scaled data, in the same spirit as the existing
"TPC-H-derived" workloads.  They are engineering probes, not certified scores
and not the real IMDb / StackExchange / LDBC / TPCx-BB data sets.

  JOB          join-order / cardinality-estimation stress (IMDb-like schema)
  STATS-CEB    star + chain + FK-FK subplan cardinality stress (StackExchange)
  H2O          high-cardinality GROUP BY / JOIN kernels (h2oai db-benchmark)
  LDBC-SNB     multi-hop self joins + aggregations (social network schema)
  BIGBENCH     scan / text / star join pipelines (AMPLab + TPCx-BB like)

Three engines are compared: tinylamb (PostgreSQL simple-query server),
DuckDB (embedded) and ClickHouse (HTTP interface).  Drivers are imported
lazily; an engine that cannot be reached is reported as unavailable and the
plot is still produced from the remaining engines.

Typical use::

    python benchmark/comparison/analytics_bench.py --output /tmp/ab --scale 1
    python benchmark/comparison/analytics_bench.py --report /tmp/ab   # re-plot
"""
from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import random
import socket
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
ENGINES = ("tinylamb", "duckdb", "clickhouse")
ALL_FAMILIES = ("job", "stats_ceb", "h2o", "ldbc_snb", "bigbench")


# --------------------------------------------------------------------------
# Portable schema description
# --------------------------------------------------------------------------

COLUMN_TYPES = {
    "tinylamb": {"I": "INT64", "F": "FLOAT64", "S": "STRING"},
    "duckdb": {"I": "BIGINT", "F": "DOUBLE", "S": "VARCHAR"},
    "clickhouse": {"I": "Int64", "F": "Float64", "S": "String"},
}


class Column:
    __slots__ = ("name", "kind", "nullable")

    def __init__(self, name, kind, nullable=False):
        if kind not in ("I", "F", "S"):
            raise ValueError(f"unknown column kind {kind!r}")
        self.name = name
        self.kind = kind
        self.nullable = nullable

    def ddl(self, engine):
        base = COLUMN_TYPES[engine][self.kind]
        if engine == "clickhouse" and self.nullable:
            return f"Nullable({base})"
        return base


class Table:
    __slots__ = ("name", "columns", "base_rows")

    def __init__(self, name, columns, base_rows):
        self.name = name
        self.columns = tuple(columns)
        self.base_rows = int(base_rows)

    def rows(self, scale):
        return max(1, int(round(self.base_rows * scale)))

    def ddl(self, engine):
        cols = ", ".join(f"{c.name} {c.ddl(engine)}" for c in self.columns)
        if engine == "clickhouse":
            return (f"CREATE TABLE IF NOT EXISTS {self.name} ({cols}) "
                    f"ENGINE = MergeTree ORDER BY {self.columns[0].name}")
        return f"CREATE TABLE IF NOT EXISTS {self.name} ({cols})"


class Query:
    __slots__ = ("name", "sql")

    def __init__(self, name, sql):
        self.name = name
        self.sql = " ".join(line.strip() for line in sql.strip().splitlines())


class Family:
    __slots__ = ("key", "title", "origin", "tables", "queries", "generator")

    def __init__(self, key, title, origin, tables, queries, generator):
        self.key = key
        self.title = title
        self.origin = origin
        self.tables = tuple(tables)
        self.queries = tuple(queries)
        self.generator = generator


# --------------------------------------------------------------------------
# Deterministic data generation helpers
# --------------------------------------------------------------------------

def csv_field(value):
    """NULL is backslash-N (DuckDB/ClickHouse CSV); empty strings stay quoted."""
    if value is None:
        return r"\N"
    if isinstance(value, str):
        return '"' + value.replace('"', '""') + '"'
    return str(value)


def write_csv(path, table, rows):
    with path.open("w", newline="") as handle:
        handle.write(",".join(c.name for c in table.columns) + "\n")
        for row in rows:
            if len(row) != len(table.columns):
                raise ValueError(f"{table.name}: {len(row)} values for "
                                 f"{len(table.columns)} columns")
            handle.write(",".join(csv_field(v) for v in row) + "\n")


def skew(rng, size, power=2.6):
    """Power-law-ish index: a few small ids dominate (hard to estimate)."""
    return min(size - 1, int((rng.random() ** power) * size))


def static_rows(spec):
    for row in spec:
        yield row


def generate_family(family, scale, seed, dataset):
    """Write one CSV per table; return row counts and a SHA-256 of all data."""
    dataset.mkdir(parents=True, exist_ok=True)
    rng = random.Random(f"{seed}:{family.key}")
    counts = family.generator(rng, scale, dataset, _emit)
    digest = hashlib.sha256()
    for table in family.tables:
        path = dataset / f"{table.name}.csv"
        if not path.is_file():
            raise ValueError(f"generator did not create {path.name}")
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        with path.open() as handle:
            counts[table.name] = sum(1 for _ in handle) - 1
    return counts, digest.hexdigest()


def _emit(directory, table, rows):
    write_csv(directory / f"{table.name}.csv", table, rows)


# --- JOB (IMDb-like) -------------------------------------------------------
JOB_TABLES = (
    Table("kind_type", (Column("id", "I"), Column("kind", "S")), 5),
    Table("company_type", (Column("id", "I"), Column("kind", "S")), 3),
    Table("info_type", (Column("id", "I"), Column("info", "S")), 4),
    Table("title", (Column("id", "I"), Column("kind_id", "I"),
                    Column("production_year", "I"), Column("title", "S"),
                    Column("note", "S", True)), 60000),
    Table("company_name", (Column("id", "I"), Column("name", "S"),
                           Column("country_code", "S")), 8000),
    Table("keyword", (Column("id", "I"), Column("keyword", "S")), 2000),
    Table("name", (Column("id", "I"), Column("name", "S"),
                   Column("gender", "S")), 20000),
    Table("movie_companies", (Column("id", "I"), Column("movie_id", "I"),
                              Column("company_id", "I"),
                              Column("company_type_id", "I")), 120000),
    Table("movie_keyword", (Column("id", "I"), Column("movie_id", "I"),
                            Column("keyword_id", "I")), 100000),
    Table("movie_info", (Column("id", "I"), Column("movie_id", "I"),
                         Column("info_type_id", "I"), Column("info", "S")), 120000),
    Table("movie_info_idx", (Column("id", "I"), Column("movie_id", "I"),
                             Column("info_type_id", "I"),
                             Column("info", "S", True)), 60000),
    Table("cast_info", (Column("id", "I"), Column("person_id", "I"),
                        Column("movie_id", "I"), Column("role_id", "I")), 200000),
    Table("aka_title", (Column("id", "I"), Column("movie_id", "I"),
                        Column("title", "S", True)), 20000),
)
JOB_QUERIES = (
    Query("J1", """
        SELECT COUNT(*) AS n, COALESCE(SUM(t.production_year), 0) AS y
        FROM title t JOIN kind_type kt ON t.kind_id = kt.id
        JOIN movie_companies mc ON t.id = mc.movie_id
        JOIN company_name cn ON mc.company_id = cn.id
        JOIN company_type ct ON mc.company_type_id = ct.id
        WHERE kt.kind = 'movie' AND ct.kind = 'production'
          AND cn.country_code = '[us]' AND t.production_year BETWEEN 1990 AND 2000
    """),
    Query("J2", """
        SELECT t.production_year, COUNT(*) AS n
        FROM title t JOIN movie_keyword mk ON t.id = mk.movie_id
        JOIN keyword k ON mk.keyword_id = k.id
        JOIN movie_info_idx mi ON t.id = mi.movie_id
        WHERE k.keyword = 'drama' AND mi.info_type_id = 2
          AND mi.info >= '7.0' AND t.production_year > 1985
        GROUP BY t.production_year ORDER BY t.production_year
    """),
    Query("J3", """
        SELECT n.gender, COUNT(*) AS n, AVG(t.production_year) AS avg_year
        FROM cast_info ci JOIN name n ON ci.person_id = n.id
        JOIN title t ON ci.movie_id = t.id
        JOIN kind_type kt ON t.kind_id = kt.id
        WHERE n.gender = 'F' AND kt.kind = 'movie'
          AND t.production_year BETWEEN 1970 AND 2010 AND ci.role_id IN (1, 2)
        GROUP BY n.gender
    """),
    Query("J4", """
        SELECT k.keyword, COUNT(*) AS n
        FROM movie_keyword mk JOIN keyword k ON mk.keyword_id = k.id
        JOIN movie_info mi ON mk.movie_id = mi.movie_id
        WHERE mi.info_type_id = 3 AND mi.info LIKE 'year-19%'
        GROUP BY k.keyword ORDER BY n DESC
    """),
    Query("J5", """
        SELECT COUNT(*) AS n
        FROM aka_title a2 JOIN title t ON a2.movie_id = t.id
        JOIN movie_info_idx mi ON t.id = mi.movie_id
        WHERE a2.title LIKE 'aka-1%' AND mi.info IS NOT NULL
          AND t.production_year < 2000
    """),
)


def gen_job(rng, scale, directory, emit):
    t = next(x for x in JOB_TABLES if x.name == "title")
    tn = t.rows(scale)
    kinds = {1: "movie", 2: "tv movie", 3: "episode", 4: "video", 5: "short"}
    emit(directory, JOB_TABLES[0],
         ((i, kinds.get(i, "movie")) for i in range(1, 6)))
    emit(directory, JOB_TABLES[1],
         ((1, "production"), (2, "distributor"), (3, "special")))
    emit(directory, JOB_TABLES[2],
         ((1, "genres"), (2, "rating"), (3, "decade"), (4, "language")))
    kind_of = [0] * (tn + 1)
    with (directory / "title.csv").open("w", newline="") as handle:
        handle.write(",".join(c.name for c in t.columns) + "\n")
        for i in range(1, tn + 1):
            year = 1950 + rng.randrange(65)
            # Old films are mostly 'movie'; 1990-2000 deliberately overlaps
            # so the JOB-style year+kind filters have a non-empty answer.
            if year < 1980 or (1990 <= year <= 2000 and i % 2 == 0):
                kind = 1
            else:
                kind = 2 if year < 2000 else 3
            kind_of[i] = kind
            note = None if i % 5 == 0 else f"note-{i % 97}"
            handle.write(f"{i},{kind},{year},\"title-{i}\",{csv_field(note)}\n")

    countries = ["[us]"] * 35 + ["[gb]"] * 15 + ["[de]"] * 10 + ["[fr]"] * 8 + \
                ["[it]"] * 7 + ["[jp]"] * 5 + ["[ca]"] * 5 + ["[xx]"] * 15
    cn = next(x for x in JOB_TABLES if x.name == "company_name").rows(scale)
    emit(directory, next(x for x in JOB_TABLES if x.name == "company_name"),
         ((i, f"company-{i}", rng.choice(countries)) for i in range(1, cn + 1)))
    keywords = ["drama", "comedy", "action", "documentary", "romance",
                "thriller", "family", "music"]
    emit(directory, next(x for x in JOB_TABLES if x.name == "keyword"),
         ((i + 1, keywords[i % 8]) for i in range(
             next(x for x in JOB_TABLES if x.name == "keyword").rows(scale))))
    emit(directory, next(x for x in JOB_TABLES if x.name == "name"),
         ((i, f"person-{i}", "F" if i % 3 == 0 else "M") for i in range(
             1, next(x for x in JOB_TABLES if x.name == "name").rows(scale) + 1)))

    def fact(name, extra):
        table = next(x for x in JOB_TABLES if x.name == name)
        emit(directory, table, ((i,) + extra(i) for i in range(1, table.rows(scale) + 1)))

    fact("movie_companies", lambda i: (skew(rng, tn) + 1,
                                       skew(rng, cn) + 1,
                                       rng.choice((1, 1, 1, 2, 3))))
    fact("movie_keyword", lambda i: (
        skew(rng, tn) + 1, (kind_of[skew(rng, tn) + 1] - 1) * 250 + rng.randrange(250) + 1))
    fact("movie_info", lambda i: (
        skew(rng, tn) + 1, rng.randrange(4) + 1,
        f"year-{1950 + rng.randrange(7) * 10}"))
    fact("movie_info_idx", lambda i: (
        skew(rng, tn) + 1, 2,
        None if i % 11 == 0 else f"{rng.randrange(40, 96) / 10:.1f}"))
    fact("cast_info", lambda i: (
        skew(rng, 20000) + 1, skew(rng, tn) + 1, rng.choice((1, 1, 2, 3, 4, 5))))
    emit(directory, next(x for x in JOB_TABLES if x.name == "aka_title"),
         ((i, skew(rng, tn) + 1, None if i % 7 == 0 else f"aka-{i}")
          for i in range(1, next(x for x in JOB_TABLES if x.name == "aka_title").rows(scale) + 1)))
    return {}


# --- STATS-CEB (StackExchange-like) ---------------------------------------
CEB_TABLES = (
    Table("users", (Column("id", "I"), Column("reputation", "I"),
                    Column("creation_yr", "I"), Column("location", "S", True)), 5000),
    Table("posts", (Column("id", "I"), Column("owner_user_id", "I"),
                    Column("post_type", "I"), Column("score", "I"),
                    Column("view_count", "I"), Column("creation_yr", "I"),
                    Column("tags", "S", True)), 40000),
    Table("comments", (Column("id", "I"), Column("post_id", "I"),
                       Column("user_id", "I"), Column("score", "I"),
                       Column("creation_yr", "I")), 80000),
    Table("votes", (Column("id", "I"), Column("post_id", "I"),
                    Column("vote_type", "I"), Column("user_id", "I", True)), 70000),
    Table("badges", (Column("id", "I"), Column("user_id", "I"),
                     Column("badge_type", "I")), 20000),
    Table("tags", (Column("id", "I"), Column("tag_name", "S"),
                   Column("tag_count", "I")), 1000),
    Table("postlinks", (Column("id", "I"), Column("post_id", "I"),
                        Column("related_post_id", "I"), Column("link_type", "I")), 15000),
    Table("links", (Column("id", "I"), Column("from_user", "I"),
                    Column("to_user", "I"), Column("link_type", "I")), 12000),
)
CEB_QUERIES = (
    Query("C1", """
        SELECT p.post_type, COUNT(*) AS n, SUM(c.score) AS s
        FROM posts p JOIN comments c ON p.id = c.post_id
        JOIN votes v ON p.id = v.post_id
        WHERE p.score > 5 AND p.view_count < 10000 AND v.vote_type = 1
        GROUP BY p.post_type ORDER BY p.post_type
    """),
    Query("C2", """
        SELECT u.id, COUNT(*) AS c
        FROM users u JOIN posts p ON u.id = p.owner_user_id
        JOIN comments c ON p.id = c.post_id
        JOIN users u2 ON c.user_id = u2.id
        WHERE u.creation_yr >= 2012 AND u2.reputation > 1000
        GROUP BY u.id ORDER BY c DESC
    """),
    Query("C3", """
        SELECT l.link_type, COUNT(*) AS n, AVG(p.score) AS avg_score
        FROM posts p JOIN postlinks l ON p.id = l.post_id
        JOIN posts r ON l.related_post_id = r.id
        WHERE l.link_type IN (1, 3) AND r.post_type = 1
        GROUP BY l.link_type
    """),
    Query("C4", """
        SELECT COUNT(*) AS n, COALESCE(SUM(p.view_count), 0) AS v
        FROM posts p LEFT JOIN tags t ON p.tags = t.tag_name
        WHERE (p.tags IS NULL OR p.tags LIKE '%sql%')
          AND p.creation_yr BETWEEN 2010 AND 2018
    """),
    Query("C5", """
        SELECT p.creation_yr, p.post_type, COUNT(*) AS n
        FROM posts p JOIN users u ON p.owner_user_id = u.id
        JOIN badges b ON u.id = b.user_id
        WHERE u.reputation >= 100 AND b.badge_type = 1
        GROUP BY p.creation_yr, p.post_type ORDER BY p.creation_yr
    """),
)


def gen_ceb(rng, scale, directory, emit):
    un = next(x for x in CEB_TABLES if x.name == "users").rows(scale)
    pn = next(x for x in CEB_TABLES if x.name == "posts").rows(scale)
    emit(directory, CEB_TABLES[0],
         ((i, int(rng.random() ** 3 * 10000) + 1, 2008 + rng.randrange(18),
           None if i % 9 == 0 else f"city-{i % 300}")
          for i in range(1, un + 1)))
    # Post score correlates with view_count and with recency; skew which posts
    # attract comments/votes/tags (the classic CEB estimation failure mode).
    score_of = [0] * (pn + 1)
    with (directory / "posts.csv").open("w", newline="") as handle:
        handle.write(",".join(c.name for c in CEB_TABLES[1].columns) + "\n")
        for i in range(1, pn + 1):
            yr = 2008 + rng.randrange(18)
            base = rng.randrange(0, 40)
            score = base + (yr - 2008) * 2 + rng.randrange(0, 10)
            score_of[i] = score
            views = score * rng.randrange(50, 300) + rng.randrange(100)
            tags = None if i % 13 == 0 else ("sql" if i % 4 == 0 else f"tag-{i % 60}")
            handle.write(f"{i},{skew(rng, un) + 1},{1 if i % 3 else 2},"
                         f"{score},{views},{yr},{csv_field(tags)}\n")
    emit(directory, CEB_TABLES[2],
         ((i, skew(rng, pn) + 1, skew(rng, un) + 1, rng.randrange(-2, 15),
           2008 + rng.randrange(18))
          for i in range(1, CEB_TABLES[2].rows(scale) + 1)))
    emit(directory, CEB_TABLES[3],
         ((i, skew(rng, pn) + 1, rng.choice((1, 1, 2, 3, 5)),
           None if i % 17 == 0 else skew(rng, un) + 1)
          for i in range(1, CEB_TABLES[3].rows(scale) + 1)))
    emit(directory, CEB_TABLES[4],
         ((i, int(rng.random() ** 2 * un) + 1, rng.randrange(1, 4))
          for i in range(1, CEB_TABLES[4].rows(scale) + 1)))
    emit(directory, CEB_TABLES[5],
         ((i, "sql" if i % 4 == 0 else f"tag-{i}", rng.randrange(10, 10000))
          for i in range(1, CEB_TABLES[5].rows(scale) + 1)))
    emit(directory, CEB_TABLES[6],
         ((i, skew(rng, pn) + 1, skew(rng, pn) + 1, rng.choice((1, 3, 3, 7)))
          for i in range(1, CEB_TABLES[6].rows(scale) + 1)))
    emit(directory, CEB_TABLES[7],
         ((i, skew(rng, un) + 1, skew(rng, un) + 1, rng.randrange(1, 5))
          for i in range(1, CEB_TABLES[7].rows(scale) + 1)))
    return {}


# --- H2O db-benchmark ------------------------------------------------------
H2O_TABLES = (
    Table("x", tuple(Column(f"id{k}", "I") for k in range(1, 7)) +
          (Column("v1", "I"), Column("v2", "I"), Column("v3", "I")), 200000),
    Table("y", tuple(Column(f"id{k}", "I") for k in range(1, 7)) +
          (Column("v1", "I"), Column("v2", "I"), Column("v3", "I")), 120000),
)
H2O_QUERIES = (
    Query("H1", "SELECT id1, COUNT(*) AS n FROM x GROUP BY id1 ORDER BY n DESC"),
    Query("H2", "SELECT id1, id2, COUNT(*) AS n FROM x GROUP BY id1, id2 ORDER BY n DESC"),
    Query("H3", "SELECT id1, SUM(v1) AS s1, SUM(v2) AS s2, SUM(v3) AS s3 FROM x GROUP BY id1"),
    Query("H4", "SELECT id1, id2, AVG(v1) AS a FROM x WHERE id3 < 4 GROUP BY id1, id2"),
    Query("H5", """
        SELECT x.id1, SUM(x.v1) AS sx, SUM(y.v2) AS sy
        FROM x JOIN y ON x.id4 = y.id4 GROUP BY x.id1 ORDER BY sx DESC
    """),
    Query("H6", """
        SELECT COUNT(*) AS n, COALESCE(SUM(v1), 0) AS s
        FROM x WHERE id1 IN (1, 2, 3, 7, 11) AND id2 BETWEEN 10 AND 40
    """),
)


def gen_h2o(rng, scale, directory, emit):
    for table in H2O_TABLES:
        n = table.rows(scale)
        def row(i):
            return (skew(rng, 400) + 1, rng.randrange(100), rng.randrange(10),
                    rng.randrange(1000) + 1, rng.randrange(100), rng.randrange(50),
                    rng.randrange(1000), rng.randrange(1000), rng.randrange(1000))
        emit(directory, table, (row(i) for i in range(n)))
    return {}


# --- LDBC SNB BI -----------------------------------------------------------
LDBC_TABLES = (
    Table("place", (Column("id", "I"), Column("name", "S"),
                    Column("country", "S")), 200),
    Table("person", (Column("id", "I"), Column("location", "I"),
                     Column("creation_yr", "I"), Column("age", "I")), 8000),
    Table("person_knows", (Column("id", "I"), Column("person1", "I"),
                           Column("person2", "I")), 40000),
    Table("forum", (Column("id", "I"), Column("moderator", "I"),
                    Column("creation_yr", "I")), 3000),
    Table("forum_member", (Column("id", "I"), Column("forum_id", "I"),
                           Column("person_id", "I"), Column("join_yr", "I")), 30000),
    Table("message", (Column("id", "I"), Column("author", "I"),
                      Column("forum", "I"), Column("parent_id", "I", True),
                      Column("creation_yr", "I"), Column("score", "I")), 60000),
    Table("person_likes", (Column("id", "I"), Column("person_id", "I"),
                           Column("message_id", "I")), 50000),
)
LDBC_QUERIES = (
    Query("L1", """
        SELECT pl.country, COUNT(*) AS n
        FROM person p JOIN person_knows k ON p.id = k.person1
        JOIN person p2 ON k.person2 = p2.id
        JOIN person_likes l ON p2.id = l.person_id
        JOIN message m ON l.message_id = m.id
        JOIN place pl ON p.location = pl.id
        WHERE m.creation_yr >= 2011 AND p2.creation_yr < 2013
        GROUP BY pl.country ORDER BY n DESC
    """),
    Query("L2", """
        SELECT f.id, m.creation_yr, COUNT(*) AS n, SUM(m.score) AS s
        FROM forum f JOIN forum_member fm ON f.id = fm.forum_id
        JOIN person p ON fm.person_id = p.id
        JOIN message m ON m.forum = f.id
        WHERE fm.join_yr >= 2010 AND p.age BETWEEN 20 AND 50
        GROUP BY f.id, m.creation_yr ORDER BY n DESC
    """),
    Query("L3", """
        SELECT a.creation_yr, COUNT(*) AS replies, AVG(b.score) AS avg_score
        FROM message a JOIN message b ON b.parent_id = a.id
        WHERE a.creation_yr BETWEEN 2010 AND 2014 AND b.score > 0
        GROUP BY a.creation_yr ORDER BY a.creation_yr
    """),
    Query("L4", """
        SELECT f.id, COUNT(*) AS n, AVG(m.score) AS a
        FROM message m JOIN forum f ON m.forum = f.id
        WHERE m.creation_yr = 2012 AND m.parent_id IS NOT NULL
        GROUP BY f.id ORDER BY n DESC
    """),
    Query("L5", """
        SELECT pl.country, COUNT(*) AS n, AVG(p.age) AS avg_age
        FROM person p JOIN place pl ON p.location = pl.id
        JOIN person_knows k ON p.id = k.person1
        WHERE p.creation_yr >= 2010
        GROUP BY pl.country ORDER BY n DESC
    """),
)


def gen_ldbc(rng, scale, directory, emit):
    countries = ["US", "GB", "DE", "FR", "IT", "JP", "CA", "BR", "IN", "AU"]
    pn = next(x for x in LDBC_TABLES if x.name == "person").rows(scale)
    emit(directory, LDBC_TABLES[0],
         ((i, f"place-{i}", countries[i % len(countries)])
          for i in range(1, next(x for x in LDBC_TABLES if x.name == "place").rows(scale) + 1)))
    # Friends/messages follow degree skew; message year correlates with author.
    yr_of = [0] * (pn + 1)
    with (directory / "person.csv").open("w", newline="") as handle:
        handle.write(",".join(c.name for c in LDBC_TABLES[1].columns) + "\n")
        for i in range(1, pn + 1):
            yr = 2008 + rng.randrange(16)
            yr_of[i] = yr
            handle.write(f"{i},{rng.randrange(1, 201)},{yr},{18 + rng.randrange(55)}\n")
    emit(directory, LDBC_TABLES[2],
         ((i, skew(rng, pn) + 1, skew(rng, pn) + 1)
          for i in range(1, LDBC_TABLES[2].rows(scale) + 1)))
    fn = next(x for x in LDBC_TABLES if x.name == "forum").rows(scale)
    emit(directory, LDBC_TABLES[3],
         ((i, skew(rng, pn) + 1, 2008 + rng.randrange(16))
          for i in range(1, fn + 1)))
    emit(directory, LDBC_TABLES[4],
         ((i, skew(rng, fn) + 1, skew(rng, pn) + 1, 2008 + rng.randrange(16))
          for i in range(1, LDBC_TABLES[4].rows(scale) + 1)))
    mn = next(x for x in LDBC_TABLES if x.name == "message").rows(scale)
    with (directory / "message.csv").open("w", newline="") as handle:
        handle.write(",".join(c.name for c in LDBC_TABLES[5].columns) + "\n")
        for i in range(1, mn + 1):
            author = skew(rng, pn) + 1
            yr = min(2024, yr_of[author] + rng.randrange(0, 5))
            parent = None if i % 3 == 0 else skew(rng, mn) + 1
            handle.write(f"{i},{author},{skew(rng, fn) + 1},"
                         f"{csv_field(parent)},{yr},{rng.randrange(0, 100)}\n")
    emit(directory, LDBC_TABLES[6],
         ((i, skew(rng, pn) + 1, skew(rng, mn) + 1)
          for i in range(1, LDBC_TABLES[6].rows(scale) + 1)))
    return {}


# --- AMPLab Big Data / TPCx-BB like ---------------------------------------
BIG_TABLES = (
    Table("rankings", (Column("id", "I"), Column("page_url", "S"),
                       Column("page_rank", "I")), 20000),
    Table("uservisits", (Column("id", "I"), Column("source_ip", "S"),
                         Column("dest_url", "S"), Column("visit_yr", "I"),
                         Column("ad_revenue", "I"), Column("country_code", "S"),
                         Column("search_word", "S")), 150000),
    Table("store", (Column("id", "I"), Column("store_name", "S"),
                    Column("region", "S")), 100),
    Table("item", (Column("id", "I"), Column("category", "S"),
                   Column("price", "I")), 2000),
    Table("customer", (Column("id", "I"), Column("region", "S"),
                       Column("since_yr", "I")), 6000),
    Table("store_sales", (Column("id", "I"), Column("store_id", "I"),
                          Column("item_id", "I"), Column("customer_id", "I"),
                          Column("yr", "I"), Column("quantity", "I"),
                          Column("revenue", "I")), 180000),
)
BIG_QUERIES = (
    Query("B1", """
        SELECT dest_url, SUM(ad_revenue) AS total
        FROM uservisits GROUP BY dest_url ORDER BY total DESC
    """),
    Query("B2", """
        SELECT u.country_code, COUNT(*) AS n, SUM(u.ad_revenue) AS r
        FROM uservisits u JOIN rankings k ON u.dest_url = k.page_url
        WHERE k.page_rank > 40 GROUP BY u.country_code ORDER BY r DESC
    """),
    Query("B3", """
        SELECT s.region, i.category, ss.yr, SUM(ss.revenue) AS total
        FROM store_sales ss JOIN store s ON ss.store_id = s.id
        JOIN item i ON ss.item_id = i.id
        JOIN customer c ON ss.customer_id = c.id
        WHERE c.since_yr <= 2012 AND ss.yr BETWEEN 2011 AND 2016
        GROUP BY s.region, i.category, ss.yr ORDER BY total DESC
    """),
    Query("B4", "SELECT COUNT(*) AS n, COALESCE(SUM(ad_revenue), 0) AS r "
                "FROM uservisits WHERE search_word LIKE '%data%' OR country_code = 'US'"),
    Query("B5", """
        SELECT yr, SUM(revenue) AS total FROM (
            SELECT ss.yr, ss.revenue FROM store_sales ss
            JOIN item i ON ss.item_id = i.id WHERE i.price > 100
        ) priced GROUP BY yr ORDER BY yr
    """),
)


def gen_bigbench(rng, scale, directory, emit):
    un = next(x for x in BIG_TABLES if x.name == "rankings").rows(scale)
    emit(directory, BIG_TABLES[0],
         ((i, f"page-{skew(rng, un) + 1}", rng.randrange(0, 100))
          for i in range(1, un + 1)))
    # Revenue correlates with year; URL popularity is heavily skewed.
    with (directory / "uservisits.csv").open("w", newline="") as handle:
        handle.write(",".join(c.name for c in BIG_TABLES[1].columns) + "\n")
        for i in range(1, BIG_TABLES[1].rows(scale) + 1):
            yr = 2010 + rng.randrange(10)
            handle.write(f"{i},\"10.0.{i % 250}.{i % 250}\","
                         f"\"page-{skew(rng, un) + 1}\",{yr},"
                         f"{rng.randrange(1, 100) + (yr - 2010) * 5},"
                         f"\"{rng.choice(('US', 'GB', 'DE', 'FR', 'JP'))}\","
                         f"\"{rng.choice(('data', 'cloud', 'sql', 'news'))}-{i % 500}\"\n")
    regions = ["North", "South", "East", "West"]
    emit(directory, BIG_TABLES[2],
         ((i, f"store-{i}", regions[i % 4])
          for i in range(1, BIG_TABLES[2].rows(scale) + 1)))
    categories = ["electronics", "books", "clothing", "home", "sports"]
    emit(directory, BIG_TABLES[3],
         ((i, categories[i % 5], rng.randrange(5, 500))
          for i in range(1, BIG_TABLES[3].rows(scale) + 1)))
    emit(directory, BIG_TABLES[4],
         ((i, regions[i % 4], 2005 + rng.randrange(15))
          for i in range(1, BIG_TABLES[4].rows(scale) + 1)))
    emit(directory, BIG_TABLES[5],
         ((i, skew(rng, BIG_TABLES[2].rows(scale)) + 1,
           skew(rng, BIG_TABLES[3].rows(scale)) + 1,
           skew(rng, BIG_TABLES[4].rows(scale)) + 1,
           2010 + rng.randrange(10), rng.randrange(1, 20), rng.randrange(10, 5000))
          for i in range(1, BIG_TABLES[5].rows(scale) + 1)))
    return {}


FAMILIES = (
    Family("job", "JOB (join order)", "IMDb-like, correlated filters",
           JOB_TABLES, JOB_QUERIES, gen_job),
    Family("stats_ceb", "STATS-CEB (cardinality)", "StackExchange-like, star+chain+FK-FK",
           CEB_TABLES, CEB_QUERIES, gen_ceb),
    Family("h2o", "H2O db-benchmark", "GROUP BY / JOIN kernels, skewed keys",
           H2O_TABLES, H2O_QUERIES, gen_h2o),
    Family("ldbc_snb", "LDBC SNB (BI)", "multi-hop self joins, degree skew",
           LDBC_TABLES, LDBC_QUERIES, gen_ldbc),
    Family("bigbench", "AMPLab / TPCx-BB", "scan + text + star pipelines",
           BIG_TABLES, BIG_QUERIES, gen_bigbench),
)
FAMILY_BY_KEY = {f.key: f for f in FAMILIES}


# --------------------------------------------------------------------------
# Answer comparison (multiset, tolerant floats)
# --------------------------------------------------------------------------

def cell_equal(a, b):
    if a is None or b is None:
        return a is b
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=1e-7, abs_tol=0.01)
    return a == b


def _quantized(row):
    """Hashable key; two cells equal under cell_equal share this key.

    cell_equal's absolute tolerance is 0.01, so two floats in the same
    0.01-wide rounding bucket always satisfy it.  A differing bucket only
    means "try the slower exact path", never a false pass.
    """
    return tuple(round(cell, 2) if isinstance(cell, float) else cell for cell in row)


def compare_rows(a, b):
    """Return None when the two result multisets are equivalent."""
    if len(a) != len(b):
        return f"row count {len(a)} != {len(b)}"
    from collections import Counter
    if Counter(map(_quantized, a)) == Counter(map(_quantized, b)):
        return None
    # Fallback for float results that straddle a quantization boundary.
    unused = list(b)
    for row in a:
        for i, other in enumerate(unused):
            if len(row) == len(other) and all(cell_equal(x, y) for x, y in zip(row, other)):
                unused.pop(i)
                break
        else:
            return f"unmatched row {row!r}"
    return None


# --------------------------------------------------------------------------
# Engine adapters
# --------------------------------------------------------------------------

class EngineUnavailable(RuntimeError):
    pass


class DuckDBEngine:
    name = "duckdb"

    def __init__(self, args, work):
        self.path = getattr(args, "duckdb_path", None) or (work / "duckdb.duckdb")
        self.threads = args.threads
        self.conn = None

    def available(self):
        try:
            import duckdb  # noqa: F401
        except ImportError as exc:
            raise EngineUnavailable(f"duckdb module missing: {exc}")
        return True

    def setup(self):
        import duckdb
        self.conn = duckdb.connect(str(self.path))
        self.conn.execute(f"SET threads={self.threads}")

    def create_table(self, table):
        self.conn.execute(table.ddl("duckdb"))

    def load(self, table, csv_path):
        self.conn.execute(
            f"COPY {table.name} FROM '{csv_path}' "
            f"(FORMAT CSV, HEADER TRUE, NULL '\\N')")

    def query(self, sql):
        cur = self.conn.execute(sql)
        return cur.fetchall()

    def version(self):
        import duckdb
        return duckdb.__version__

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


# Same localhost-only / UTC profile used by clickbench.py init-clickhouse and
# the local TPC-H / TPC-DS ClickHouse runs (build/benchmarks/core-scaling-*).
CLICKHOUSE_CONFIG = """<clickhouse>
<logger><level>warning</level><log>{root}/server.log</log>
<errorlog>{root}/error.log</errorlog></logger>
<timezone>UTC</timezone>
<listen_host>127.0.0.1</listen_host><http_port>{http}</http_port>
<tcp_port>{native}</tcp_port><path>{root}/data/</path>
<tmp_path>{root}/tmp/</tmp_path><user_files_path>{root}/user_files/</user_files_path>
<mark_cache_size>536870912</mark_cache_size>
<background_schedule_pool_size>8</background_schedule_pool_size>
<background_buffer_flush_schedule_pool_size>4</background_buffer_flush_schedule_pool_size>
<background_pool_size>16</background_pool_size>
<profiles><default><max_threads>{threads}</max_threads>
<max_memory_usage>34359738368</max_memory_usage><use_query_cache>0</use_query_cache>
<date_time_input_format>best_effort</date_time_input_format></default></profiles>
<users><default><password></password><networks><ip>127.0.0.1</ip></networks>
<profile>default</profile><quota>default</quota><access_management>1</access_management>
</default></users><quotas><default><interval><duration>3600</duration>
<queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows>
<execution_time>0</execution_time></interval></default></quotas>
</clickhouse>"""


class ClickHouseEngine:
    """HTTP interface; can also spawn a dedicated local server like clickbench.py."""

    name = "clickhouse"

    def __init__(self, args, work):
        self.url = args.clickhouse_url.rstrip("/")
        self.database = args.clickhouse_database
        self.timeout = args.timeout
        self.threads = getattr(args, "threads", 2)
        self.binary = getattr(args, "clickhouse_binary", None)
        self.http_port = getattr(args, "clickhouse_http_port", 28130)
        self.native_port = getattr(args, "clickhouse_native_port", 29007)
        self.root = Path(getattr(args, "clickhouse_root", None) or (work / "clickhouse"))
        self.process = None
        self.log = None

    def _request(self, sql, payload=None, query=None):
        params = {"database": self.database}
        if query:
            params.update(query)
        url = self.url + "/?" + urllib.parse.urlencode(params)
        if payload is None:
            data = sql.encode()  # ordinary SELECT / DDL goes in the POST body
        else:
            body = payload.encode() if isinstance(payload, str) else payload
            # INSERT ... FORMAT CSV needs the statement and the CSV in one
            # POST body, separated by a newline.
            data = sql.encode() + b"\n" + body
        request = urllib.request.Request(url, data=data, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace").strip()[:400]
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc

    def _probe(self):
        # Probe through `default`: our benchmark database is only created in
        # setup(), and the existing ClickBench/TPC-H harnesses do the same.
        self._request("SELECT 1", query={"database": "default"})

    def _start_server(self):
        try:
            self._probe()
            return  # an already running server is reused
        except (OSError, urllib.error.URLError, RuntimeError):
            pass
        if not self.binary:
            raise EngineUnavailable(
                f"ClickHouse at {self.url} is not reachable; "
                "start it or pass --clickhouse-binary")
        binary = Path(self.binary).resolve()
        if not binary.is_file():
            raise EngineUnavailable(f"ClickHouse binary not found: {binary}")
        if self.http_port == self.native_port:
            raise EngineUnavailable("ClickHouse HTTP and native ports must differ")
        reservations = []
        try:
            for port in (self.http_port, self.native_port):
                reservation = socket.socket()
                reservations.append(reservation)
                reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                reservation.bind(("127.0.0.1", port))
            root = self.root
            root.mkdir(parents=True, exist_ok=True)
            for directory in ("data", "tmp", "user_files"):
                (root / directory).mkdir(exist_ok=True)
            from html import escape
            (root / "config.xml").write_text(CLICKHOUSE_CONFIG.format(
                root=escape(str(root.resolve())), http=self.http_port,
                native=self.native_port, threads=getattr(self, "threads", 2)))
        finally:
            for reservation in reservations:
                reservation.close()
        self.log = (root / "stdout.log").open("w")
        environment = dict(os.environ, TZ="UTC")
        self.process = subprocess.Popen(
            [str(binary), "server", "--config-file=" + str(root / "config.xml")],
            stdout=self.log, stderr=subprocess.STDOUT,
            start_new_session=True, env=environment)
        (root / "server.pid").write_text(str(self.process.pid) + "\n")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise EngineUnavailable("ClickHouse exited; see " + str(root / "stdout.log"))
            try:
                self._probe()
                return
            except (OSError, urllib.error.URLError, RuntimeError):
                time.sleep(0.2)
        raise EngineUnavailable("ClickHouse startup exceeded 60 seconds")

    def available(self):
        try:
            self._start_server()
        except (OSError, urllib.error.URLError, RuntimeError) as exc:
            raise EngineUnavailable(f"ClickHouse at {self.url} unreachable: {exc}")
        return True

    def setup(self):
        self._request(f"CREATE DATABASE IF NOT EXISTS {self.database}",
                      query={"database": "default"})

    def create_table(self, table):
        self._request(f"CREATE TABLE IF NOT EXISTS {self.database}.{table.ddl('clickhouse').split('IF NOT EXISTS ', 1)[1]}")

    def load(self, table, csv_path):
        body = csv_path.read_bytes()
        self._request(f"INSERT INTO {self.database}.{table.name} FORMAT CSV", body)

    def query(self, sql):
        text = self._request(sql + " FORMAT TabSeparatedWithNamesAndTypes")
        lines = text.splitlines()
        if not lines:
            return []
        types = lines[1].split("\t") if len(lines) > 1 else []
        rows = []
        for line in lines[2:]:
            fields = line.split("\t")
            rows.append(tuple(_click_value(field, kind)
                              for field, kind in zip(fields, types)))
        return rows

    def version(self):
        return self._request("SELECT version()").strip()

    def close(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None


def _click_value(field, kind):
    if "Nullable" in kind and field == r"\N":
        return None
    if field == r"\N":
        return None
    if kind.startswith("String") or "String" in kind:
        return field
    try:
        return int(field) if "Int" in kind else float(field)
    except ValueError:
        return field


class TinylambEngine:
    name = "tinylamb"

    def __init__(self, args, work):
        self.args = args
        self.work = work
        self.conn = None
        self.process = None
        self.log = None

    def available(self):
        try:
            import psycopg2  # noqa: F401
        except ImportError as exc:
            raise EngineUnavailable(f"psycopg2 module missing: {exc}")
        self._connect()  # may spawn the server
        return True

    def _connect(self):
        import psycopg2
        if self.conn is not None:
            return
        args = self.args
        if args.server and not args.tinylamb_dsn:
            port = args.tinylamb_port
            self.log = (self.work / "tinylamb-server.log").open("w")
            db = self.work / "tinylamb.db"
            cmd = [str(Path(args.server).resolve()), str(db),
                   "--port", str(port), "--read-workers", str(args.read_workers)]
            self.process = subprocess.Popen(cmd, stdout=self.log,
                                            stderr=subprocess.STDOUT)
            endpoint = dict(host="127.0.0.1", port=port, user="bench", dbname="bench")
            for _ in range(80):
                try:
                    self.conn = psycopg2.connect(connect_timeout=1, **endpoint)
                    self.conn.autocommit = True
                    self.endpoint = endpoint
                    break
                except psycopg2.Error:
                    if self.process.poll() is not None:
                        raise EngineUnavailable("tinylamb server exited during startup")
                    time.sleep(0.1)
            if self.conn is None:
                raise EngineUnavailable("tinylamb server did not become ready")
        else:
            dsn = args.tinylamb_dsn or "host=127.0.0.1 port=55433 user=bench dbname=bench"
            try:
                self.conn = psycopg2.connect(dsn, connect_timeout=3)
                self.conn.autocommit = True
                self.endpoint = dsn
            except psycopg2.Error as exc:
                raise EngineUnavailable(f"tinylamb connection failed: {exc}")

    def setup(self):
        pass

    def create_table(self, table):
        with self.conn.cursor() as cur:
            cur.execute(table.ddl("tinylamb"))

    def load(self, table, csv_path):
        columns = ",".join(c.name for c in table.columns)
        with csv_path.open() as handle:
            header = handle.readline()
            if header.strip() != columns:
                raise ValueError(f"CSV header mismatch for {table.name}")
            batch = []
            def flush():
                if not batch:
                    return
                values = ",".join(
                    "(" + ",".join(_tiny_literal(v, c.kind)
                                   for v, c in zip(row, table.columns)) + ")"
                    for row in batch)
                with self.conn.cursor() as cur:
                    cur.execute(f"INSERT INTO {table.name} ({columns}) VALUES {values}")
                batch.clear()
            for line in handle:
                row = next(csv.reader([line]))
                batch.append(tuple(None if v == r"\N" else v for v in row))
                if len(batch) >= 100:
                    flush()
            flush()

    def query(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def version(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")
            return "postgres-protocol/simple-query"

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None


def _tiny_literal(value, kind="S"):
    """Render a CSV field as a GoogleSQL literal using the column's type.

    CSV gives us strings for every field; deciding by type rather than by
    content keeps numeric-looking *strings* (ratings, tags) as strings.
    """
    if value is None:
        return "NULL"
    if kind == "I":
        return str(int(value))
    if kind == "F":
        return str(float(value))
    return "'" + str(value).replace("'", "''") + "'"


def make_engine(name, args, work):
    return {"duckdb": DuckDBEngine, "clickhouse": ClickHouseEngine,
            "tinylamb": TinylambEngine}[name](args, work)


# --------------------------------------------------------------------------
# Benchmark execution
# --------------------------------------------------------------------------

def load_families(dataset_root, families, scale, seed):
    manifest = {}
    for family in families:
        target = dataset_root / family.key
        target.mkdir(parents=True, exist_ok=True)
        counts, digest = generate_family(family, scale, seed, target)
        manifest[family.key] = {"origin": family.origin,
                                "tables": counts, "sha256": digest}
    (dataset_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def run_benchmark(args, out):
    families = [FAMILY_BY_KEY[k] for k in args.families]
    engines = list(args.engines)
    work = out / "work"
    work.mkdir(exist_ok=True)
    dataset = out / "dataset"
    if not (dataset / "manifest.json").is_file():
        load_families(dataset, families, args.scale, args.seed)
    manifest = json.loads((dataset / "manifest.json").read_text())

    available, unavailable = {}, {}
    for name in engines:
        engine = make_engine(name, args, work)
        try:
            engine.available()
            engine.setup()
            for family in families:
                for table in family.tables:
                    engine.create_table(table)
                    engine.load(table, dataset / family.key / f"{table.name}.csv")
            available[name] = engine
        except Exception as exc:  # driver errors vary by engine; always clean up
            unavailable[name] = f"{type(exc).__name__}: {exc}"
            try:
                engine.close()
            except Exception:  # pragma: no cover - best effort cleanup
                pass

    records = []
    order = list(available)
    query_index = 0
    for family in families:
        for query in family.queries:
            per_engine = {}
            for index in range(len(order)):
                # Rotate the start engine per query so warm-cache/order effects
                # do not always favour the same engine.
                name = order[(index + query_index) % len(order)]
                engine = available[name]
                samples, answers, error = [], [], None
                try:
                    for i in range(args.warmup + args.repeats):
                        start = time.perf_counter()
                        rows = engine.query(query.sql)
                        elapsed = (time.perf_counter() - start) * 1000.0
                        answers.append(rows)
                        if i >= args.warmup:
                            samples.append(elapsed)
                except Exception as exc:  # one bad query must not lose the run
                    error = f"{type(exc).__name__}: {exc}"
                per_engine[name] = {
                    "engine": name, "family": family.key, "query": query.name,
                    "status": "error" if error else "ok", "error": error,
                    "samples_ms": samples,
                    "median_ms": statistics.median(samples) if samples else None,
                    "answer_sha256": hashlib.sha256(
                        repr(answers[-1] if answers else None).encode()).hexdigest(),
                    "answer": answers[-1] if answers and error is None else None,
                    "sql": query.sql,
                }
            mismatch = None
            ok = [v for v in per_engine.values() if v["status"] == "ok"]
            if len(ok) > 1:
                base = ok[0]
                for other in ok[1:]:
                    mismatch = compare_rows(base["answer"], other["answer"])
                    if mismatch:
                        break
            for value in per_engine.values():
                value["answer_match"] = mismatch is None if len(ok) > 1 else None
                if mismatch and value["status"] == "ok":
                    value["status"] = "mismatch"
                    value["error"] = mismatch
                value.pop("answer", None)
                records.append(value)
            query_index += 1
            print(f"  {family.key:10} {query.name:3} " +
                  " ".join(f"{k}={per_engine[k]['median_ms']:.1f}ms"
                           for k in per_engine if per_engine[k]['median_ms'] is not None),
                  flush=True)

    versions = {name: engine.version() for name, engine in available.items()}
    env = {
        "tool": "analytics_bench.py",
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "scale": args.scale, "seed": args.seed, "threads": args.threads,
        "warmup": args.warmup, "repeats": args.repeats, "timeout": args.timeout,
        "engines": engines, "available": sorted(available),
        "unavailable": unavailable, "versions": versions,
        "families": [f.key for f in families],
        "family_titles": {f.key: f.title for f in families},
        "family_queries": {f.key: [q.name for q in f.queries] for f in families},
        "dataset": manifest,
        "git_head": _git_head(),
    }
    _dump(out / "environment.json", env)
    with (out / "results.jsonl").open("w") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    for engine in available.values():
        engine.close()
    return env, records


def _git_head():
    try:
        return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                                       text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _dump(path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


# --------------------------------------------------------------------------
# Reporting / plotting
# --------------------------------------------------------------------------

def metric_table(records, env):
    """family -> engine -> sum of per-query median milliseconds (None if missing)."""
    table = {key: {name: None for name in env["engines"]}
             for key in env["families"]}
    for family, queries in env["family_queries"].items():
        for engine in env["engines"]:
            values = [r["median_ms"] for r in records
                      if r["family"] == family and r["engine"] == engine
                      and r["status"] == "ok" and r["median_ms"] is not None
                      and r["query"] in queries]
            if values and len(values) == len(queries):
                table[family][engine] = sum(values)
    return table


def plot(out):
    env = json.loads((out / "environment.json").read_text())
    records = [json.loads(line) for line in (out / "results.jsonl").read_text().splitlines()]
    table = metric_table(records, env)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(f"matplotlib is required for --report: {exc}")

    families = env["families"]
    engines = [e for e in env["engines"] if any(table[f][e] is not None for f in families)]
    if not engines:
        print("WARNING: no engine produced a complete measurement; "
              "figures will be empty. See environment.json['unavailable'].",
              file=sys.stderr)
    colors = {"tinylamb": "#0072B2", "duckdb": "#EC9B24", "clickhouse": "#009E73"}
    palette = [colors.get(e, f"C{i}") for i, e in enumerate(engines)]
    width = 0.8 / max(1, len(engines))
    fig, ax = plt.subplots(figsize=(1.7 * len(families) + 4, 5.4), constrained_layout=True)
    positions = list(range(len(families)))
    for i, (engine, color) in enumerate(zip(engines, palette)):
        values = [table[f][engine] for f in families]
        offsets = [p + (i - (len(engines) - 1) / 2) * width for p in positions]
        bars = ax.bar(offsets, [v if v is not None else 0 for v in values],
                      width=width, color=color, label=engine)
        for bar, value in zip(bars, values):
            if value is None:
                continue
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"{value:.0f}", ha="center", va="bottom", fontsize=7)
    ax.set_yscale("log")
    ax.set_xticks(positions, [env["family_titles"].get(f, f) for f in families],
                  fontsize=9)
    ax.set_ylabel("Sum of per-query median latency (ms, log; lower is better)")
    ax.grid(axis="y", alpha=.25)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=9)
    ax.set_title(f"Derived analytics benchmarks, SF={env['scale']:g} "
                 f"({env['repeats']} timed repeats)\n"
                 "synthetic correlated data; engineering comparison, not a certified score")
    for suffix in ("png", "svg"):
        fig.savefig(out / f"grouped_bars.{suffix}", dpi=170)
    plt.close(fig)

    # A second, per-query view makes the individual kernels visible.
    rows = [r for r in records if r["status"] == "ok" and r["median_ms"] is not None]
    labels, grouped = [], {}
    for family in families:
        for query in env["family_queries"][family]:
            label = f"{family}:{query}"
            labels.append(label)
            grouped[label] = {e: next((r["median_ms"] for r in rows
                                       if r["family"] == family and r["query"] == query
                                       and r["engine"] == e), None) for e in engines}
    fig, ax = plt.subplots(figsize=(max(12, len(labels) * .62), 5),
                           constrained_layout=True)
    for i, (engine, color) in enumerate(zip(engines, palette)):
        offsets = [p + (i - (len(engines) - 1) / 2) * width for p in range(len(labels))]
        ax.bar(offsets, [grouped[l][engine] or 0 for l in labels], width=width,
               color=color, label=engine)
    ax.set_yscale("log")
    ax.set_xticks(range(len(labels)), labels, rotation=60, ha="right", fontsize=7)
    ax.set_ylabel("Median latency (ms, log)")
    ax.grid(axis="y", alpha=.25)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=9)
    ax.set_title(f"Per-query detail, SF={env['scale']:g}")
    for suffix in ("png", "svg"):
        fig.savefig(out / f"per_query.{suffix}", dpi=170)
    plt.close(fig)

    summary = {
        "family_totals_ms": table,
        "engines": engines,
        "available": env["available"],
        "unavailable": env["unavailable"],
        "fastest_family": {f: min(((table[f][e], e) for e in engines
                                   if table[f][e] is not None), default=(None, None))[1]
                           for f in families},
        "answer_match": {r["engine"]: r.get("answer_match") for r in records},
    }
    _dump(out / "summary.json", summary)
    print(json.dumps(summary, indent=2))


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", type=Path, help="new result directory")
    p.add_argument("--scale", type=float, default=1.0, help="scale factor (SF)")
    p.add_argument("--seed", type=int, default=20261123)
    p.add_argument("--families", nargs="+", choices=ALL_FAMILIES,
                   default=list(ALL_FAMILIES))
    p.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--generate-only", action="store_true",
                   help="only write the synthetic dataset, then exit")
    p.add_argument("--report", type=Path,
                   help="re-plot an existing result directory (no DB needed)")
    p.add_argument("--list-families", action="store_true")
    p.add_argument("--list-engines", action="store_true")
    p.add_argument("--duckdb-path", type=Path,
                   help="where to place the embedded DuckDB file (default: output/work)")
    p.add_argument("--clickhouse-url", default=None,
                   help="ClickHouse HTTP endpoint (default: localhost:<http-port>)")
    p.add_argument("--clickhouse-database", default="analytics_bench")
    p.add_argument("--clickhouse-binary", type=Path,
                   help="auto-start a dedicated local ClickHouse server with the "
                        "same UTC/localhost profile as clickbench.py init-clickhouse")
    p.add_argument("--clickhouse-http-port", type=int, default=28130)
    p.add_argument("--clickhouse-native-port", type=int, default=29007)
    p.add_argument("--clickhouse-root", type=Path,
                   help="auto-started server work dir (default: output/work/clickhouse)")
    p.add_argument("--server", type=Path,
                   help="tinylamb server binary to spawn (default: connect to --tinylamb-dsn)")
    p.add_argument("--tinylamb-port", type=int, default=55433)
    p.add_argument("--read-workers", type=int, default=2)
    p.add_argument("--tinylamb-dsn", help="libpq DSN for an already running tinylamb server")
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list_families:
        for key in ALL_FAMILIES:
            f = FAMILY_BY_KEY[key]
            print(f"{key:12} {f.title:26} {len(f.tables)} tables, "
                  f"{len(f.queries)} queries - {f.origin}")
        return 0
    if args.list_engines:
        for name in ENGINES:
            print(name)
        return 0
    if args.report:
        if not (args.report / "results.jsonl").is_file():
            parser.error(f"no results.jsonl in {args.report}")
        plot(args.report)
        return 0
    if not args.output:
        parser.error("--output is required (or use --report / --generate-only)")
    if args.scale <= 0 or args.repeats < 1 or args.warmup < 0 or args.threads < 1:
        parser.error("positive --scale/--repeats/--threads and nonnegative --warmup required")
    if args.clickhouse_url is None:
        args.clickhouse_url = f"http://127.0.0.1:{args.clickhouse_http_port}"
    if args.clickhouse_http_port == args.clickhouse_native_port or \
            min(args.clickhouse_http_port, args.clickhouse_native_port) < 1:
        parser.error("ClickHouse HTTP and native ports must be distinct and positive")
    out = args.output.resolve()
    if out.exists() and any(out.iterdir()):
        parser.error(f"{out} is not empty; choose a new --output directory")
    out.mkdir(parents=True, exist_ok=True)
    print(f"analytics_bench SF={args.scale:g} -> {out}")
    if args.generate_only:
        load_families(out / "dataset", [FAMILY_BY_KEY[k] for k in args.families],
                      args.scale, args.seed)
        print("dataset generated")
        return 0
    env, records = run_benchmark(args, out)
    plot(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
