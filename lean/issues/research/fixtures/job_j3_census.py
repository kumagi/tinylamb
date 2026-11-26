"""Inspect recorded JOB medians and independently replay J3 on its original CSVs."""
import argparse
import json
from pathlib import Path

import duckdb


def main(run):
    env = json.loads((run / "environment.json").read_text())
    by_query = {}
    for line in (run / "results.jsonl").read_text().splitlines():
        row = json.loads(line)
        if row["family"] == "job" and row["engine"] in {"tinylamb", "duckdb"}:
            assert row["status"] == "ok" and row["answer_match"]
            by_query.setdefault(row["query"], {})[row["engine"]] = row
    print("run", env["generated_at"], env["scale"], env["threads"], env["repeats"])
    for name in sorted(by_query):
        tl, dd = (by_query[name][engine] for engine in ("tinylamb", "duckdb"))
        assert tl["answer_sha256"] == dd["answer_sha256"]
        print(name, round(tl["median_ms"], 3), round(dd["median_ms"], 3))
    with duckdb.connect(":memory:") as db:
        for table in ("cast_info", "name", "title", "kind_type"):
            path = run / "dataset" / "job" / (table + ".csv")
            db.execute(f"create table {table} as select * from read_csv_auto(?)", [str(path)])
        db.execute("""
            create view edges as select * from cast_info where role_id in (1,2)
        """)
        db.execute("create view people as select id from name where gender='F'")
        db.execute("""
            create view movies as select t.id, t.production_year as y
            from title t join kind_type k on t.kind_id=k.id
            where k.kind='movie' and t.production_year between 1970 and 2010
        """)
        print("role rows, distinct people, movies, pairs, person bounds", db.execute("""
            select count(*), count(distinct person_id), count(distinct movie_id),
                   count(distinct (person_id,movie_id)), min(person_id),max(person_id) from edges
        """).fetchone())
        print("person-first survivors", db.execute(
            "select count(*) from edges join people on person_id=people.id").fetchone()[0])
        print("movie-first survivors and demanded distinct people, 256-key slices", db.execute("""
            select count(*), count(distinct person_id), count(distinct person_id // 256)
            from edges join movies on movie_id=movies.id
        """).fetchone())
        # Independent aggregate. A person may have several matching build rows.
        weighted = db.execute("""
            with f as (select id,count(*) w from people group by id),
                 credits as (select movie_id,sum(w) w from edges join f on person_id=f.id group by movie_id)
            select sum(w),sum(w*y) from credits join movies on movie_id=movies.id
        """).fetchone()
        ref = db.execute(by_query["J3"]["duckdb"]["sql"]).fetchall()
        assert len(ref) == 1 and ref[0][0] == "F"
        assert ref[0][1] == weighted[0] and ref[0][2] == weighted[1] / weighted[0]
        print("J3 count, year sum, result", *weighted, ref)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    main(parser.parse_args().run)
