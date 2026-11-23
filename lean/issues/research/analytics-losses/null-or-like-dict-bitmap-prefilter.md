# `(col IS NULL OR LIKE ...)` の辞書ビットマップ一括先行判定（Dictionary Bitmap Prefilter for Nullable Disjunctive LIKE）

**未証明。** `(p.tags IS NULL OR p.tags LIKE '%sql%')` のような選言 NULL 許容パターン述語を辞書圧縮列（`dict : List String`, `codes : List (Option Nat)`）上で評価する際、全行に対して文字列デコードと `LIKE` 照合を毎回やり直す代わりに、ブロック辞書配列のユニークエントリに対してのみ `matchStr` ビットマップを 1 回事前構築し、行走査では `none => true` / `some c => mask[c]` の O(1) ビット参照へ置換する方式が、SQL の 3 値論理（`Tri`）のもとで全行・全追加述語 `extra` と完全同値になることを検証する。

## 対象敗北クエリとボトルネック

- **STATS-CEB C4** (`103.5 ms` [srv `109.5 ms`] vs ClickHouse `4.4 ms`, `23.52x` 差): `posts.tags` の `(p.tags IS NULL OR p.tags LIKE '%sql%')` に対して行単位デコード＆評価が走り、`scan_ms=3.30 ms`, `filter_ms=3.29 ms` で 20,894 行中 7,069 行をフィルタ（さらに低カーディナリティ文字列辞書上での重複照合）。
- **JOB J4** (`8.8 ms`) / **JOB J5** (`2.8 ms`): `mi.info LIKE 'year-19%'`, `a2.title LIKE 'aka-1%'` 等の辞書／列挙文字列上の反復 LIKE 評価。

## 提出型

[`NullOrLikeDictBitmapPrefilter.Goal`](../../../TinylambOpt/ResearchGoals/NullOrLikeDictBitmapPrefilter.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter.Goal` を提出する。

すべての非 NULL コードが有効範囲内（`c < dict.length`）であれば、`optAccepts` と 3 値論理参照評価 `refAccepts` が完全に一致することを示す。

## 境界例と適用範囲

- `v = none`（NULL）は `IS NULL` で `TRUE` になり `TRUE OR UNKNOWN = TRUE` にショートサーキットするため `optAccepts` でも `true` 扱いとなる。
- 逆に `IS NULL AND LIKE`（`AND` 結合）では `TRUE AND UNKNOWN = UNKNOWN`（除外）となり、`NOT (col = 'x') OR LIKE` では NULL に対し `UNKNOWN OR UNKNOWN = UNKNOWN`（`buggyNotEqOrAccept` の反例）になるため、対象は厳密に `IS NULL OR match(col)` 形（または一般 3VL マスクを別管理する場合）に限定する必要がある。

## 実装との照合先

- `executor/detail/parallel_table_scan.cpp` (Line 1239 付近のワーカー側フィルタ評価)
- `executor/columnar_tlcs_scan_executor.cpp` (TLCS 辞書ブロックフィルタ)
- `table/tlcs_file.hpp` (ブロック局所辞書)
