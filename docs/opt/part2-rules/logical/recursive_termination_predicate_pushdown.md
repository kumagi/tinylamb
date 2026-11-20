# recursive_termination_predicate_pushdown

- 状態: done   /   執筆基準リビジョン: `3880673` (2026-09-12)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`(登録式。登録コメントは
  "Push monotonic termination condition into recursive CTE child")

## 変換と適用条件

`Selection(RecursiveCte, p)` の述語から深さの上下限を読み取り、`RecursiveDepthSpec` の範囲を狭めた CTE を同じ Memo グループに追加します。例えば元の範囲が 1〜100 なら、`depth > 5 AND depth <= 10` に対する代替案は 6〜10 だけを生成します。

対象は子を持たず深さ仕様を持つ再帰 CTE です。すべての連言が深さ列と非 NULL の INT64 定数との大小比較であり、修飾名がある場合は CTE 名と一致する必要があります。定数が左辺にある比較は向きを反転します。実行時パラメータ、未対応の比較、境界の加減算でオーバーフローする場合には元の Selection を維持します。

## 実装

```cpp
            if (!recognized || !narrowed || lower > upper) {
              continue;
            }
            LogicalExpression narrowed_cte = cte;
            narrowed_cte.depth_spec = RecursiveDepthSpec{
                .column = spec.column, .lower = lower, .upper = upper};
            memo.AddExpression(group, std::move(narrowed_cte));
```

新しい範囲が空になる場合や、元の上下限と同じ場合にも代替案を追加しません。

## 検証テスト

`CascadesTest.RecursiveTerminationPredicatePushdown` と `CascadesTest.RecursiveTerminationNonDepthPredicateStays` が深さ条件を取り込む場合と通常の述語を保持する場合を検証します。
