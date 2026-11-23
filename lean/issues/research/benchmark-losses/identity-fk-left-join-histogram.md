# `is_identity_1_to_n` 左表に対する片側ヒストグラム直計算

**未証明。** 左表が `1..N` の連続一意キー（`ColumnSynopsis::is_identity_1_to_n`）である `L LEFT OUTER JOIN (filter R)` 上の 2 段集約 `GROUP BY L.id → COUNT(val) → GROUP BY cnt` は、左表 `L` を一切スキャンせず右表 `R` のみを 1 回走査して `[1..N]` 内の一意キーごとの非 NULL 件数を集計し、`hist(0) = N - nonzeroKeys` を補間するだけで完全同値になることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-H Q13** (`162.01 ms` vs DuckDB `157.61 ms`, `+4.40 ms / 1.03x` 差): TLCS v3 で `customer.c_custkey` が `is_identity_1_to_n`（150 万件）と判明しているにもかかわらず、`orders`（1500 万件）との左外部結合・ソート／ハッシュコストが高い。

## 提出型

[`IdentityFkLeftJoinHistogram.Goal`](../../../TinylambOpt/ResearchGoals/IdentityFkLeftJoinHistogram.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.IdentityFkLeftJoinHistogram.Goal` を提出する。

`∀ (n : Nat) (rs : List RRow) (c : Nat), optHist n rs c = refHist n rs c` を要求し、`N = 0` や範囲外 FK、NULL 値を含む境界ケースも含め無条件に一致することを示す。

## 境界例と適用範囲

- `r.pred = true` かつ `r.fk = some k (1 ≤ k ≤ N)` であっても、`r.val = none` の行は SQL `COUNT(val)` では 0 件と数えられるため、`nonzeroKeys` に含めないことで `hist(0) = N - nonzeroKeys` が厳密に一致する。
- `fk = 99 > N` の孤立外部キーや `fk = none` は左外部結合でマッチしないため `optHist` でも必ず除外する必要がある。

## 実装との照合先

- `executor/detail/fused_grouped_join_stream.cpp` (`is_identity_1_to_n` shortcut)
- `table/tlcs_file.hpp` (`ColumnSynopsis::is_identity_1_to_n`)
