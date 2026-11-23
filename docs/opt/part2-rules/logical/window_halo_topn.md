# window_halo_topn

- 状態: draft / 更新: 2026-11-23
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `window_halo_topn`、パターン: `TopN(Window(...))`）

## 概要

同じ順序キーを持つ TopN と、パーティション分割のない bounded ROWS Window が並ぶ場合に、Window の入力を結果に必要な範囲のソート済み halo に絞る代替式を追加します。Window の後に TopN を保ち、境界の前後に必要な行を残すことでフレーム計算と最終的な LIMIT/OFFSET を維持します。

## 適用条件

- TopN に `WITH TIES` がなく、`limit_count` が正で、既存の prior offset がありません。
- sort key と Window の順序指定が一致します。パーティション指定のある Window や、サポート外のフレーム・集約関数は対象外です。
- `offset + limit` と halo 境界の計算でオーバーフローしないこと。
- `TINYLAMB_DISABLE_WINDOW_HALO` の値が空でなく、先頭文字が `0` でない場合は発火しません。

## 意味論

ROWS フレームが参照する最大の preceding/following 幅をそれぞれ `p` / `f` とすると、Window 出力の `[offset, offset + limit)` に必要な入力は `[max(0, offset - p), offset + limit + f)` に収まります。内側 TopN でこの halo の末尾を制限し、Window 後の prior offset で先頭側を調整します。TopN の順序が Window と異なる場合や、未対応のフレームでは変換しません。

## 検証テスト

- `query/select_rewrites_test.cpp`: `SelectRewritesTest.WindowHaloAndOuterFetchTiesSqlParity` が Window halo を含む SQL の結果一致を検証します。
