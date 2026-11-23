# NTH_VALUE IGNORE NULLS と行除外を順位索引で評価する

**未証明。** 整列済みパーティションに、任意の半開フレーム、現在行、整数の順位を指定する。`EXCLUDE CURRENT ROW` が除くのはその出現だけ。同じ値の別行は残る。

## 提出型

[NthNonNullFrame.Goal](../../TinylambOpt/ResearchGoals/NthNonNullFrame.lean) を別モジュールから import し、`theorem sound : TinylambOpt.ResearchGoals.NthNonNullFrame.Goal` を提出する。

各位置直前の非 NULL 件数と、非 NULL 値の配列を前計算する。フレーム内で除かれる非 NULL 行が目標順位以前にあるときだけ読取位置を一つ進める。任意の要求列で値、NULL、不正順位エラーが逐次走査と一致すること。元から空のフレームでは順位を検査せず、除外で空になる場合は順位を検査する。

## 適用範囲

値は評価済みの整数または NULL。順位は整数に型解決済み。元の式の副作用・評価エラー、フレーム境界の算出、同順位行群の除外は対象外。 [実装との照合先](../../../executor/detail/window_eval.cpp)
