# ブロック絶対値上界和による無検査並列 INT64 SUM ＆定数ブロック O(1) 畳込み

**未証明。** 並列 `exact_integer_total` において、定数ブロックを O(1) の乗算 `cnt * val` で畳み込み、各フラットブロックをオーバーフロー検査なしで並列加算してよい安全条件を検証する。

## 提出型

[`ExactIntTotalOverflowGuard.Goal`](../../../TinylambOpt/ResearchGoals/ExactIntTotalOverflowGuard.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.ExactIntTotalOverflowGuard.Goal` を提出する。

各ブロックが絶対値上界 `maxAbs` を満たし、`∑ (nonNullCount * maxAbs) ≤ INT64_MAX`（`2^63 - 1`）であれば、並列無検査 SUM が逐次オーバーフロー検査付き SQL SUM と値およびエラー有無の両面で完全一致することを要求する。

## 境界例と適用範囲

最終総和だけが範囲に収まる `[INT64_MAX, 1, -1]` は逐次評価で第 2 要素の時点でオーバーフローする一方、並列順では相殺して成功するため不健全である。絶対値上界和の条件によりこの種の評価順依存性を排除できる。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`exact_integer_total`, `kConstant` O(1) 判定)
