# 非対応式の Tier 分割と生成コードの翻訳検証

## 提案する命題

VARCHAR / JSON / UDF など生成エンジンが直接扱えない式をホスト呼び出しへ逃がしてステージ分割しても、値・NULL・最初のエラーが AST 評価と一致する。

[ProposedGoals/GeneratedExprTierSplit.Goal](../../TinylambOpt/ProposedGoals/GeneratedExprTierSplit.lean) は AST を単純なバイトコード（`push/load/add/host`）へコンパイルしたスタック評価が、木 walker の参照評価と全入力で一致することを要求する。

## モデルと適用条件

ホスト関数は決定的で、引数と tag のみで結果が決まること。スタックアンダーフローや存在しない列はエラー値として観測する。生成コードは `Tier 1` カーネル、ホスト呼び出しは `Tier 0` に対応する。

## 回帰例

`2 + neg(5) = -3`。未知 tag のホスト呼び出しは `.err tag` となり、参照評価と同じく伝播する。fixture で固定する。

## 実装との照合先

TODO.md の「生成できない式はステージ分割して Tier 0 に逃がす」「WASM 生成は bytecode からだけ行う」。
