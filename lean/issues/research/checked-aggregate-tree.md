# 部分和の順序情報を持つ並列 SUM

## 証明済みの命題

チャンクの総和・最小/最大 prefix・非 NULL 件数を合成し、逐次 INT64 SUM と同じ値または最初の失敗行を返す。

## Lean での提出型

[CheckedAggregateTree.Goal](../../TinylambOpt/ResearchGoals/CheckedAggregateTree.lean) を import し、別モジュールで次の型の定理を提出する。

```lean
import TinylambOpt.ResearchGoals.CheckedAggregateTree
open TinylambOpt.ResearchGoals.CheckedAggregateTree

-- 提出する宣言の型
#check (Goal : Prop)
-- theorem checked_aggregate_tree_sound : Goal := <証明項>
```

入力は一グループの行順を保存する任意の木。参照器は成功時に nullable SUM、失敗時に最初に失敗した入力行の ordinal を返す。最適化側は準備済みの木を使い、全 prefix が安全な部分木での実行トレースが summary 一回だけになることも証明対象に含む。一般定理は未証明。ファイル末尾の例は仕様の境界を検査する。

## モデルと適用条件

入力順を保存する木、負値、空チャンク、グループごとの疎な位置を許す。境界集計は数学整数。

## 回帰例

[MAX,1,-1] の最終値は MAX。任意順の部分和マージでは途中失敗とその行番号を失う。

## 完了条件

木の括り方によらず結果と失敗 ordinal が一致し、安全な部分木を丸ごと飛ばせることを示す。

実装との照合先は [executor/detail/pipeline/ooc_group_agg_sink.cpp](../../../executor/detail/pipeline/ooc_group_agg_sink.cpp)。
