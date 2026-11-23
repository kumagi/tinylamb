# 空文字・NULL 頻出列の整数キー分離・低コスト述語先行・整数 `AVG` 遅延除算

**未証明。** (1) `GROUP BY (uid, phrase)` において `phrase = ""`（ClickBench では全行の 86.8%）および `phrase = NULL` を単一整数キー `uid` だけの純整数ハッシュ／平坦配列パスへ分離して文字列ハッシュ・比較を全廃しても Top-K が一致すること、(2) 全域述語下で `phrase ≠ ""` を `URL LIKE '%google%'` より前に評価する並び替え、および (3) `AVG(CAST(x AS FLOAT64))` の整数 `(sum, cnt)` 累積と末尾除算の合成法則を検証する。

## 対象敗北クエリとボトルネック

- **ClickBench Q17** (`458.07 ms` vs DuckDB `454.70 ms`, `+3.37 ms`): 9999 万行中 8682 万行（86.8%）で `SearchPhrase = ''` なのに、毎回 `(UserID, SearchPhrase)` 複合キーの文字列処理が走っている。
- **ClickBench Q22** (`384.20 ms` vs DuckDB `340.61 ms`, `+43.59 ms`): 86.8% を落とす `SearchPhrase <> ''` より高コストな `URL LIKE '%google%'` 評価が支配。
- **ClickBench Q31** (`161.71 ms` vs ClickHouse `142.07 ms`)・**Q32** (`244.37 ms` vs `166.24 ms`, `+78.13 ms`): `AVG(CAST(ResolutionWidth AS FLOAT64))` の型変換と高カーディナリティ 2 キー集約コスト。

## 提出型

[`EmptyStringKeyPartitionSplit.Goal`](../../../TinylambOpt/ResearchGoals/EmptyStringKeyPartitionSplit.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit.Goal` を提出する。

`optSplitTopK k rows = refGroupTopK k rows`、`evalPredReordered = evalPredNaive`、およびチャンク結合時の整数 `(sum, cnt)` 加法性 `avgStateOfOpts (xs ++ ys) = (s1 + s2, c1 + c2)` を要求する。

## 境界例と適用範囲

- 空文字 `some ""`・NULL `none`・非空 `some s` は互いに排他な 3 分割であり、同一 `uid` でも `(.emptyKey uid)` と `(.nullKey uid)` は SQL `GROUP BY` 上で別グループとして保たれる。
- `containsUrl` は全要素に対し停止する純粋述語であることが交換の前提（部分式エラーがあり得る場合は `FilterEvalOrder` を適用）。

## 実装との照合先

- `executor/detail/typed_stream_aggregate.cpp` / `typed_stream_aggregate_dict.cpp`
- `expression/rewrite.cpp` / `table/tlcs_file.cpp`
