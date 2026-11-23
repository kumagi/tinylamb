# FILTER 付き集約の段階評価と分割結合

## 提案する命題

`SUM(x) FILTER (WHERE p)` を、述語を全行まとめて評価する段階経路と、行順の参照経路で比べる。さらに部分チャンクの状態を結合しても、最初のエラー位置（絶対 ordinal）と値が変わらないことを要求する。

[ProposedGoals/FilterAggregateContract.Goal](../../TinylambOpt/ProposedGoals/FilterAggregateContract.lean) は `staged = scan` と `merged(prefix,suffix) = scan(prefix++suffix)` を固定する。述語は三値 `TRUE/FALSE/UNKNOWN` を返し、`none` は SQL エラーを表す。`UNKNOWN` は通過させない。

## モデルと適用条件

述語は決定的で副作用がないこと。最初のエラーより後のレーンを投機的に評価しても観測は変わらない。エラーの ordinal は元の行番号で、suffix 側の失敗は prefix 長だけずらす。NULL 値は和に寄与しない。

## 回帰例

Fixture A は値 `[2, 0]` と `p(0)=error` を使い、ordinal 1 で失敗することを確認する。Fixture B は値 `[1, 3, 5]` と `p(3)=UNKNOWN`（他は TRUE）を使い、UNKNOWN 行を除外して合計 6 になることを確認する。これらは独立した回帰ケースで、同じ入力・同じ述語を共有しない。

## 実装との照合先

TODO.md の「桁あふれ・0 除算の検査をループ内に出す」「エラー保留フラグ」と `AggregateAccumulator::CanMerge()`。
