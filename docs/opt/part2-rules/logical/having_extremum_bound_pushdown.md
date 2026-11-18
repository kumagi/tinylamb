# having_extremum_bound_pushdown

- 状態: draft   /   執筆基準リビジョン: `9ce7837` + Lean拡張 (2026-11-18)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"having_extremum_bound_pushdown"`）、および `query/select_rewrites.cpp` の `PushHavingExtremumBounds`
- Lean 形式証明: `lean/TinylambOpt/AvgMinMaxHavingDistinct.lean`（定理 `having_max_gt_pushdown_to_where_sound`, `having_min_lt_pushdown_to_where_sound`）

## 概要

`HAVING MAX(col) > c`（または `>= c`、`MIN(col) < c` / `<= c`）を持ち、かつクエリ内の全集約がその単一の `MAX(col)`（または `MIN(col)`）のみである場合、集約前の基底入力へ `col > c`（または `col < c`）の事前選択フィルタを押し下げます。

$$\sigma_{\text{MAX}(x) > c}\bigl(\gamma_{k,\, \text{MAX}(x)}(R)\bigr) \equiv \sigma_{\text{MAX}(x) > c}\bigl(\gamma_{k,\, \text{MAX}(x)}(\sigma_{x > c}(R))\bigr)$$

インデックススキャンや外部結合の内部結合化（`ReduceOuterJoins`）を集約前に解禁する強力なルールです。

## 適用条件と反例ガード

1. **単一極値集約条件**: クエリ内の全集約が同一の `MAX(col)`（または `MIN(col)`）のみであること。`COUNT(*)` や `SUM(y)`、`MIN(x)`（`MAX` 側の場合）が共存する場合は、除外行が他集約の値を変えるため発火を禁止します（Lean 反例 `having_max_gt_breaks_sibling_avg_bug`）。
2. **不等号の向き**: `MAX(col)` に対しては下界（`>`, `>=`）のみ、`MIN(col)` に対しては上界（`<`, `<=`）のみを押し下げます。逆向き（`HAVING MAX(col) < c`）は上限を超える行を隠蔽して偽陽性グループを生むため禁止します（Lean 反例 `having_min_gt_pushed_to_where_bug`）。
3. **GoogleSQL `DOUBLE` `NaN` 汚染ガード**: GoogleSQL ではグループ内に 1 つでも `NaN` が存在すると `MIN`/`MAX` が `NaN` になります。`DOUBLE` 列で `col > 10.0` を先に適用すると `NaN` 行が脱落し、`NaN` だったグループが `20.0 > 10.0` に化けるため、`INT64` / `DATE` / `VARCHAR` の非 `DOUBLE` 型に厳格に限定します。
4. **上位 `HAVING` の維持**: スカラー集約（`GROUP BY` なし）で全行がフィルタ落ちした際に 1 行の `(NULL)` を誤出力しないよう、上位の `HAVING` 述語自体は残します。

## 検証テスト

- `CascadesTest.HavingExtremumBoundPushdownFiresForIntAndRefusesDoubleNaN`
- `SelectRewritesTest.PushHavingExtremumBoundsPushesIntAndPreservesDoubleNaN`
