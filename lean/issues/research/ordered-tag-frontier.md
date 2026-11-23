# 入力途中で履歴を落としても勝者のキャプチャを保存する

[タグ付き DFA 化](regex-tagged-determinize.md) の差し戻しの第一段階。参照器の辺順・ε 展開順で最初に受理した履歴を勝者とする。遷移はタグの内容を分岐に使わない。この条件なら同じ状態から進める辺は同じになる。

## 証明対象

[`OrderedTagFrontier.Goal`](../../TinylambOpt/ResearchGoals/OrderedTagFrontier.lean) の定義です。証明済みの `theorem sound` は `ResearchProofs/OrderedTagFrontier.lean` にあります。

ε 展開の各ラウンドと各バイトの直後に、同じ状態へ来た最初の履歴だけを保持する。任意の ε 展開上限で、参照側が選んだ一つのキャプチャ列と一致し、最終 frontier の状態 ID は重複しない。タグを設定する経路と設定しない経路が同じ状態に入る例では、前者の `(0,1)` が勝つ。

## 適用範囲

同じ上限・開始位置から入力全体を消費する二つの評価器の比較。実 API の左端探索、greedy/lazy の優先順位、DFA 遷移表の生成、上限超過時の fallback は後続の接続条件。
