# 最適化トグルビットをキーに含むプランキャッシュ無効化の完全再現性

**未証明。** `PreparedPlanCache` やスレッドローカル `CurrentCompiledPlans` に加え、`TINYLAMB_DISABLE_*` 等の実行時最適化トグルビット列（`BytecodeOptToggleBits`）をキャッシュキーへ組み込むことで、トグル切替時の古いプラン再利用を防ぎつつヒット時は再コンパイルを省く契約を検証する。

## 提出型

[`PlanCacheToggleInvalidation.Goal`](../../../TinylambOpt/ResearchGoals/PlanCacheToggleInvalidation.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.PlanCacheToggleInvalidation.Goal` を提出する。

任意の `compile` / `exec` 関数と操作列（クエリ実行・スキーマ世代更新・`optBits` 切替）に対し、キャッシュ有り実行がキャッシュ無し実行と同一の `(Plan, Result)` 列を返し、既出の `(sqlId, schemaGen, optBits)` では絶対に再コンパイルしないことを要求する。

## 境界例と適用範囲

`optBits` をキーから省く `badCachedRunNoOptBits` は、最適化 ON→OFF 切替直後に古い ON プランを再利用してしまう反例を固定している。

## 実装との照合先

- `query/plan_cache.hpp` (`PreparedPlanCache`, `CurrentCompiledPlans`)
- `expression/bytecode.cpp` (`BytecodeOptToggleBits`)
