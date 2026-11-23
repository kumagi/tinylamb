# 新規提案: SQL 拡充に効く命題

既存の [未証明問題](../README.md) と [研究 Issue](../research/README.md) を補完するため、
SQL 機能追加・オプティマイザ拡張の前に固定しておくと便利な命題を 8 件提案する。
各課題には型検査済みの `Goal : Prop` を [ProposedGoals](../../TinylambOpt/ProposedGoals/) に置いた。
提出は仕様を import した別ファイルの `theorem sound : Goal` 形式を想定する。

## 一覧

- [GROUPING SETS を最細粒度の状態から合成する](grouping-sets-state-reuse.md)
- [FILTER 付き集約の段階評価と分割結合](filter-aggregate-contract.md)
- [DISTINCT 集約のマージ契約](distinct-agg-merge-contract.md)
- [照合キー正規化による group/order 書き換え](collation-key-canonicalization.md)
- [非対応式の Tier 分割と生成コードの翻訳検証](generated-expr-tier-split.md)
- [QUALIFY と WHERE の交換条件](qualify-pushdown.md)
- [DISTINCT ON を group ごと top-1 状態に落とす](distinct-on-top1.md)
- [順序集合集約 PERCENTILE_DISC の二つの定義](ordered-set-percentile.md)

## 選定の観点

TODO.md に未実装・未検証項目として挙がっている機能（ROLLUP/CUBE、FILTER/DISTINCT 集約の merge 契約、
collation、生成コードのステージ分割）を中心に、追加機能の相互作用で壊れやすい三値論理・NULL・
同率・エラー順序を提出型に含めた。各提案の検証状況は [RESULTS.md](RESULTS.md) に記録する。
