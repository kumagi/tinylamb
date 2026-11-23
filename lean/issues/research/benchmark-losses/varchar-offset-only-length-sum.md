# `VarChar` オフセット終端配列 `O(1)` 総バイト長算出と payload 非参照非空カウント

**未証明。** 累積終端オフセット列 `ends` とゼロ長 NULL スパン不変条件（`ValidVarcharBlock`: `none` 行は `ends[i] = prevEnd(i)`、`some s` 行は `ends[i] - prevEnd(i) = s.length`）を満たす TLCS VarChar ブロックに対し、全非空文字列の総バイト長が文字列本体にもループにも一切触れず **`ends.getLast!`** の `O(1)` 参照で求まり、かつ `URL <> ''` の件数判定もオフセット配列だけ（`prevEnd < end ∧ !null`）で完結することを検証する。

## 対象敗北クエリとボトルネック

- **ClickBench Q28** (`181.75 ms` vs ClickHouse `96.78 ms`, `+84.97 ms / 1.88x` 差): `SELECT CounterID, AVG(CAST(BYTE_LENGTH(URL) AS FLOAT64)), COUNT(*) FROM hits WHERE URL <> '' GROUP BY CounterID` で、全 1 億行の `URL` 文字列領域や長さを 1 行ずつ読まずに `(ends, nulls)` オフセットだけで解決できる余地が残っている。

## 提出型

[`VarcharOffsetOnlyLengthSum.Goal`](../../../TinylambOpt/ResearchGoals/VarcharOffsetOnlyLengthSum.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.VarcharOffsetOnlyLengthSum.Goal` を提出する。

`ValidVarcharBlock b` の下での `optO1EndsLastLenSum b = refNonEmptyLenSum b.rows` 及び `optOffsetOnlyNonEmptyCount b = refNonEmptyCount b.rows`、さらに NULL 行が非ゼロバイト区間を持つ不正エンコードブロック `badNullNonzeroSpan` での過大計上反例を要求する。

## 境界例と適用範囲

- NULL 行に対してオフセットが前要素より進んでいるブロック（パディング込みエンコードなど）では `ends.getLast!` が偽のバイト長を加算するため、オフセットエンコーダのゼロ長 NULL 不変条件が必須となる。
- ブロックに `HAVING COUNT(*) > 100000` と組み合わせ、さらにグループ先決め・定数／単一 `CounterID` ブロックでの `O(1)` 畳み込みへ接続する。

## 実装との照合先

- `table/tlcs_file.cpp` (`varchar_lengths_only`, `varchar_ends`)
- `executor/detail/typed_stream_aggregate_dict.cpp` (`BYTE_LENGTH` fast path)
