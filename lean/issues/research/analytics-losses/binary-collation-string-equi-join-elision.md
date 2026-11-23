# バイナリ照合文字列等値キーの結合残差述語除去（Binary-Collation Equi-Join Residual Elision）

**未証明。** 現在の `ResidualJoinPredicates` は `kInt64` と `kDate` の等値キーのみを残差述語から除去しており、`VARCHAR` 文字列の等値結合キー `L.s = R.s` は必ず残差述語に残り、ハッシュ一致後の全候補ペアに対して毎回汎用バイトコード述語評価が走っている。照合順序（collation）が `binary`（厳密バイト一致）であれば、`INNER JOIN` / `LEFT OUTER JOIN` および任意のハッシュ関数（完全衝突を含む）のもとで残差述語を除去してハッシュバケット内の直接バイト列比較へ統合しても完全同値であり、逆に `asciiCaseInsensitive` や `padSpace` では壊れることを検証する。

## 対象敗北クエリとボトルネック

- **STATS-CEB C4** (`103.5 ms` [srv `109.5 ms`] vs ClickHouse `4.4 ms`, `23.52x` 差): `p.tags = t.tag_name` が `residual=1` に落ち、`1,144,500` 件全ペアに文字列等値残差バイトコードが実行され `join_ms = 105.77 ms` を消費。
- **BigBench B2** (`44.7 ms` [srv `40.2 ms`] vs DuckDB `10.6 ms`, `4.21x` 差): `u.dest_url = k.page_url` が `residual=1` に落ち、`1,476,707` 件全ペアに文字列等値残差が実行され `join_ms = 35.80 ms` を消費。

## 提出型

[`BinaryCollationStringEquiJoinElision.Goal`](../../../TinylambOpt/ResearchGoals/BinaryCollationStringEquiJoinElision.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BinaryCollationStringEquiJoinElision.Goal` を提出する。

1. `Collation.binary` での全称一致（`isLeft ∈ {false, true}`、任意のハッシュ `h`、全リスト `ls, rs`）
2. `asciiCaseInsensitive` および `padSpace` での具体不一致反例の存在

## 境界例と適用範囲

- `"SQL"` vs `"sql"`（大文字小文字無視照合）および `"a"` vs `"a "`（末尾空白パディング照合）では、生バイト一致 `xs == ys` だけの高速比較は偽の不一致（`LEFT JOIN` では偽の NULL 拡張）を生むため除去不可。
- バイナリ照合であっても `NULL` キー（`none`）はハッシュ一致から確実に除外し、`LEFT JOIN` 時のみ `(l.id, none)` へ落とす契約が不可欠。

## 実装との照合先

- `executor/detail/planning_heuristics.cpp` (`ResidualJoinPredicates`, Lines 717–744)
- `executor/detail/fused_grouped_join.cpp` (Lines 1450–1523 `residual_programs`)
