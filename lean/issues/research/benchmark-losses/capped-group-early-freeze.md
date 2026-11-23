# 無順序 `GROUP BY ... LIMIT K` の固定グループ数凍結・既存キー限定更新

**未証明。** `ORDER BY` も `HAVING` も持たない `SELECT k, COUNT(*) FROM T GROUP BY k LIMIT K` において、最初に遭遇した高々 `K` 個の一意キーをハッシュ表に確保した時点で新規キーの挿入を凍結（以後、未登録キーは全廃棄し、既登録キーのカウントだけを全行末尾まで更新継続）すると、厳密な全表 `COUNT(*)` と一致する `min K (#distinct)` 個のグループが得られ、サイズ `> K` のハッシュ表やマージを全廃できることを検証する。

## 対象敗北クエリとボトルネック

- **ClickBench Q18** (`27.07 ms` vs ClickHouse `8.66 ms`, `3.13x` 差): `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10` で `ORDER BY` が無いにもかかわらず数千万規模のグループ状態が走査・集約されており、早期グループ上限凍結で ClickHouse に 3 倍差をつけられている。

## 提出型

[`CappedGroupEarlyFreeze.Goal`](../../../TinylambOpt/ResearchGoals/CappedGroupEarlyFreeze.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze.Goal` を提出する。

出力件数 `min lim full.length`、一意性 `Nodup`、全出力ペアの厳密一致 `∀ p ∈ out, p ∈ full`、上界 `out.length ≤ lim`、および単に先頭 `K` 行で走査を止める誤った早期終了 `badEarlyStopAfterKRows` の反例を要求する。

## 境界例と適用範囲

- 登録済みキーに対するインクリメントを途中で止めると末尾行分の `COUNT(*)` が欠落するため、スキャン自体は続けるが **新規グループ確保とハッシュリサイズだけを凍結** する点が本質である。
- `HAVING` や `ORDER BY` を伴うクエリ（例：Q17）には適用できない。

## 実装との照合先

- `executor/detail/typed_stream_aggregate.cpp` / `executor/detail/pipeline/ooc_group_agg_sink.cpp`
- `query/grouped_pipeline_lowering.cpp`
