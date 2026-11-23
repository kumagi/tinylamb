# `OFFSET ≫ LIMIT` の逆順最小 Top 抽出双対性と `TIMESTAMP_TRUNC` 単調バケット走査

**未証明。** (1) グループ数が `N` で `OFFSET O ≫ LIMIT L`（例：`N = 10,948, O = 10,000, L = 10`）のとき、`N ≤ O` ならソート無しで即座に `[]` を返し、`N > O` ならサイズ `O + L` の降順ヒープではなく逆順序（`ascLe a b = descLe b a`）で **下位 `N - O` 件（例：948 件）のヒープを取り、反転して先頭 `L` 件を切る** 双対性と、(2) `TIMESTAMP_TRUNC(EventTime, MINUTE)` 相当の `Int.ediv t 60` が単調であるため `[lo..hi]` 範囲を密配列バケットへ直接集計すればソート自体を消去できることを検証する。

## 対象敗北クエリとボトルネック

- **ClickBench Q42** (`1.66 ms` vs DuckDB `1.55 ms`)・**Q41** (`2.77 ms` vs `1.72 ms`)・**Q40** (`35.40 ms` vs `24.24 ms`): `ORDER BY PageViews DESC LIMIT 10 OFFSET 10000` のように出力よりはるかに大きい `OFFSET` を持つクエリで `offset + limit = 10,010` サイズの巨大 Top-N ヒープを管理している（Q42 ではグループ総数 `10,948` に対し逆順 Top-`948` で済む）。
- **ClickBench Q43** (`1.79 ms` vs DuckDB `1.78 ms`): 2 日間（高々 2,880 分）の `TIMESTAMP_TRUNC(EventTime, MINUTE)` 集約とソートに汎用ハッシュ＋ソートを使っている。

## 提出型

[`OffsetHeavyReverseTopNAndQuotientBucket.Goal`](../../../TinylambOpt/ResearchGoals/OffsetHeavyReverseTopNAndQuotientBucket.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.OffsetHeavyReverseTopNAndQuotientBucket.Goal` を提出する。

`xs.Nodup` 下での `optReverseAscSlice off lim xs = refDescSlice off lim xs`、`Int.ediv t 60` の広義単調性、および `[lo..hi]` 上の `optDenseMinuteSweep = refQuotientGroupSort` を要求する。

## 境界例と適用範囲

- 同点（同 `score`）が存在する場合、`ascLe` はスコア昇順かつ **タイ識別子降順**（`descLe b a`）としなければ反転時の順序が逆転するため、完全双対順序 `descLe b a` を使う必要がある。
- 負の時刻（例：`t = -5 → Int.ediv (-5) 60 = -1`）を含む全整数領域で `t1 ≤ t2 → t1/60 ≤ t2/60` が成立する。

## 実装との照合先

- `executor/detail/relational_sort_limit.cpp` / `executor/sort.cpp`
- `executor/detail/typed_stream_aggregate.cpp` (`dense_small_key` quotient bucket path)
