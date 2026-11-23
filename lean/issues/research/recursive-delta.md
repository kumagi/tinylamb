# 再帰 UNION DISTINCT で新しく発見した頂点だけを展開する

**証明済み。** 証明は `lean/TinylambOpt/ResearchProofs/RecursiveDelta.lean` の `theorem sound` にあります。 seed から有向辺をたどる再帰 CTE。参照仕様は有限回の辺遷移による到達可能性で、seed 自身も含む。

## 証明対象

[RecursiveDelta.Goal](../../TinylambOpt/ResearchGoals/RecursiveDelta.lean) を証明済み。提出済みの `theorem sound` は `ResearchProofs/RecursiveDelta.lean` にあります。

隣接索引を一度構築し、直前の反復で初めて見つかった頂点だけを展開する。出力の健全性と完全性、重複なし、終了時の frontier が空、各到達頂点の展開が一度だけという四つの性質を要求する。重複辺、閉路、自己ループ、孤立 seed の例を検査する。

## 適用範囲

頂点 ID だけを射影する正の再帰。経路長や経路自体を射影すると重複判定の単位が変わる。反復上限エラーを持つ実行経路には別途対応が必要。 [実装との照合先](../../../executor/recursive_cte.cpp)
