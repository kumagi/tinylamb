# push_topn_with_ties_through_left_join

- 状態: draft / 更新: 2026-11-23
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `push_topn_with_ties_through_left_join`、パターン: `TopN(Any("input"))`）

## 概要

左外部結合の上にある `TopN ... WITH TIES` の順序キーが保存側（左入力）の列だけに依存する場合、`limit + offset` 件の TopN を左入力へ押し込み、元の TopN を結合の上に残す代替式を生成します。左外部結合は左の行を失わないため、境界キーと同値の行を含めて左側を制限できます。

## 適用条件

- TopN の `limit_count` が正で、`limit_count + limit_offset` がオーバーフローしません。
- 既存の prior offset がなく、sort key と昇降順の配列が対応します。
- 入力グループに述語を持つ左外部結合があり、TopN の sort key は安全に評価でき、左側だけの式として扱えます。
- 派生した同じ変換を重ねて適用しません。`TINYLAMB_DISABLE_OUTER_FETCH_TIES` の値が空でなく、先頭文字が `0` でない場合も発火しません。

## 意味論

各左行は左外部結合の結果に少なくとも1行を出します。TopN が左側の順序キーだけで順位を決めるなら、結果に入り得る左行は左側単独の上位 `offset + limit` 件に限られます。`WITH TIES` によって境界キーの同順位行をすべて残し、右側との結合後に元の TopN が最終結果と offset を確定します。

## 検証テスト

- `query/select_rewrites_test.cpp`: `SelectRewritesTest.WindowHaloAndOuterFetchTiesSqlParity` が Outer Fetch Ties を含む SQL の結果一致を検証します。
