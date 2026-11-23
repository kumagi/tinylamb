# 複数ブロック局所辞書のコード添字 2 段階集約（Multi-Block Local-Dictionary Two-Stage Aggregation）

**未証明。** TLCS の各ブロックが独立した局所文字列辞書（`dict : List String`, `rows : List (Option Nat × Option Int)`）を持つ列上で `GROUP BY str_col`（例: `dest_url`）を集約する場合、Stage 1 ではブロック内の文字列実体に一切触れずローカルコード番号 `[0..dict.length]`（末尾は `NULL`）配列へ部分集計し、Stage 2 で実際にヒットした非空スロット（`cnt > 0`）のみ文字列化してブロック跨ぎ合算する 2 段階集約が、全ブロックを最初から文字列デコードして集約する参照実装と完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **BigBench B1** (`29.0 ms` [srv `15.2 ms`] vs DuckDB `11.5 ms`, `2.51x` 差): `SELECT dest_url, SUM(ad_revenue) FROM uservisits GROUP BY dest_url`（150,000 行 → 19,646 グループ）で `[fusion]` 側へ入らず、`HashAggregate` が `scan_ms = 13.04 ms`, `aggregate_ms = 8.48 ms` で文字列ハッシュ表を構築（`dictionary_aggregations = 0`）。
- **JOB J4** (`8.8 ms`) / **LDBC L1** (`81.9 ms`) / **LDBC L5** (`3.2 ms`): ブロック局所辞書上の文字列グループ集約。

## 提出型

[`MultiBlockLocalDictTwoStageAggregate.Goal`](../../../TinylambOpt/ResearchGoals/MultiBlockLocalDictTwoStageAggregate.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.MultiBlockLocalDictTwoStageAggregate.Goal` を提出する。

すべてのコードが `code < b.dict.length` である任意のブロック列 `blocks` と任意キー `target : Option String` について `optTwoStageAgg blocks target = refGlobalAgg blocks target` を示す。

## 境界例と適用範囲

- 異なるブロック間で同一文字列 `"page-1"` に別コード（例: block1=`0`, block2=`1`）が振られていても、Stage 2 で `b.dict[code]` を解決して合算するため整合が崩れない。
- ブロック自身の局所辞書内に同じ文字列が重複登録されているケース（例: `dict = ["page-1", "page-2", "page-1"]`）でも Stage 2 の加算集約で重複スロットが正しく合算される。

## 実装との照合先

- `executor/detail/typed_stream_aggregate_dict.cpp` (Lines 383–2910 `dictionary_aggregations`)
- `table/tlcs_file.hpp` (TLCS ブロック辞書レイアウト)
