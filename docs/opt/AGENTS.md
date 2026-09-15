# `docs/opt/` — tinylamb Cascades オプティマイザ解説書・執筆プロジェクト

このディレクトリは、tinylamb のクエリオプティマイザ(Cascades 実装)を
C++ コードの実例つきで解説する書籍の原稿を置く場所である。執筆は ZCode との
複数ターン対話で進める。

**このファイルは執筆プロジェクトの憲法である。** 執筆ターンを開始したら
必ず最初に本ファイルと `docs/opt/00-index.md`(目次 + 進捗トラッカー)を読む。
方針の決定・変更はここに反映する。原稿だけ読んで書き始めない。

- 基準リビジョン: `3880673` (2026-09-12)。本文中のコード引用はこの時点の実装。
- 執筆は文書のみを対象とする。本ディレクトリの作業で製品コード・テストは
  変更しない(検証のためにビルド済みバイナリや `EXPLAIN` を実行することはある)。
- 本ディレクトリは既存 `docs/` 配下の書籍専用スペースである。`docs/` 直下の
  規範文書(`cascades_optimizer.md` 等)と役割分担する(§1.5)。

## 1. 本のコンセプト

1. **主題**: tinylamb 内部の Cascades オプティマイザ実装。Memo / Group /
   Pattern / Rule / SearchEngine の仕組みを、実際の C++ コードを読み進めながら
   理解させる。
2. **軸は「1 Rule = 1 解説単位」**。tinylamb に実装された Rule を一つずつ
   列挙し、本を読み切れば全 Rule の(ア)適用条件と(イ)最適化内容を
   説明できる構成にする。
3. **懇切丁寧**。各 Rule について次を省略しない:
   - 何が最適化されるのか(変換前後のプラン図つき)
   - どの条件で発火するのか(pattern と guard の正確な列挙)
   - なぜその条件が必要なのか(条件を外したときの反例)
   - 実装のどこがそれを実現しているのか(コード引用と行単位の解説)
   - 「この Rule が適切に適用されればツリーがこうなる」「こう書き換えると
     意味が変わるため、チェックして発火を禁止している」——この 2 点を
     漏らさず書く。tinylamb 実装の細部への固執より、この論理の完全さを
     優先する。
4. **Rule を 1 本も飛ばさない**。式書き換え Rule を含め、実装されている
   全 Rule を解説対象にする。自明度の高いものは基礎編で一括してよいが、
   省略・一覧だけでの済ませはしない。
4. **理論の一般論より「このコードではこうなっている」**。Cascades の一般論
   (Columbia / ORCA 等の再解説)は最小限にし、既存資料に譲る。本書の価値は
   tinylamb 実装への正確な案内にある。一般論に触れるときも、必ず tinylamb の
   どの実装がそれに対応するかを添える。
5. 既存の正規文書 `docs/cascades_optimizer.md`(規範・要約)、
   `docs/optimizer_todo.md`、`docs/optimizer_improvements.md` と競合する記述を
   書かない。本書は「学習書」、それらは「規範」という役割分担を保つ。数値や
   一覧がずれたら必ず規範側か本書側を直す。

## 2. 対象読者と二部構成(決定: 2026-09-13)

**二部構成**とする。

- **第1部(基礎編)**: C++ が読める DB 実装学習者を想定。Cascades / Memo の
  事前知識を前提としない。SQL とインデックスの基礎知識はあるものとする。
  インフラ要素(Memo、Pattern、SearchEngine、コスト、PhysicalProperties)を
  コードつきで丁寧に解説する。
- **第2部(Rule リファレンス)**: 1 ルール 1 ファイルの辞書的構成。第1部を
  読んだ読者には自足的に、Cascades 既知の実装者には第2部から直接読める
  ようにする。各章から第1部の該当節へリンクする。

## 3. ソース・オブ・トゥルースと鮮度運用

- **コードが正、本文はコードに従属する。** 引用と実装が食い違うなら本文が誤り。
- 各章の先頭に執筆時点のリビジョンを記録する
  (`git rev-parse --short HEAD` + 日付)。
- 製品コードを変更したら、影響する章を同じ作業単位で更新する。
  「コードを直したが本書は古いまま」の状態を作らない。
- 引用は実ファイルからの抜粋のみ。省略は `// ...(省略)` と明示する。
  説明のために単純化したコードを載せるときは「簡略化した疑似コード」と
  明記し、実コードの関数名を併記して照合できるようにする。
- 参照は行番号ではなくシンボル名ベースで行う
  (例: `plan/cascades.cpp` の `RuleSet::Default()`)。行番号はすぐ腐るため
  本体ではない。ファイルへのリンクはリポジトリ相対パス
  (`plan/cascades.cpp` など)で書く。

## 4. 解説対象の全体像: 3 層の Rule セット(2026-09-13 時点)

tinylamb の最適化は 3 層の独立した Rule セットで構成される
(`docs/cascades_optimizer.md` 参照)。Rule はグローバル登録ではなく値であり、
`OptimizerOptions` 経由で差し替え・無効化できる。

| 層 | 定義場所 | 本数 | 役割 |
|---|---|---|---|
| 式書き換え `ExpressionRuleSet` | `expression/rewrite.cpp` の `ExpressionRuleSet::Default()` | 約 74 | 定数畳み込み・比較の正規化・ブール恒等式などスカラー式の正規化 |
| 論理同値 `cascades::RuleSet` | `plan/cascades.cpp` の `RuleSet::Default()` | 117 | Memo へ同値な論理式を追加(探索) |
| 物理 `cascades::ImplementationRuleSet` | `plan/implementation_rules.cpp` の `DefaultImplementationRules()` | 52 | 論理式を物理プランへ実装(コスト評価の対象) |

第1部で説明すべきインフラ要素:

- `Memo` / `Group` / `GroupId` / `LogicalExpression`(`plan/cascades.hpp`)
  — Group は関係集合 + タグでキー付けされ、各 Group が自身の scan filter を
  持つ。式上限 4096 と `Memo::Degraded()` の縮退動作。
- `Pattern` と `cascades::dsl`(`Join(Any("left"), Any("right"))` 型の DSL)と
  `Bindings`。
- `SearchEngine` — append-only ワークリストによる探索、`(group,
  PhysicalProperties)` キーでの最良プランのキャッシュ、branch-and-bound 的
  枝刈り、`search_step_budget` による縮退。
- `PhysicalProperties`(`require_row_position` / `ordering` / `limit_hint` /
  `access_method` / 予約済み `distribution`)と enforcement。
- `ImplementationRule` が返す `PlanAlternative` と `BestPlan`、`local_cost` と
  カーディナリティ推定。
- `RuleContext`(カタログ・統計の注入)、`OptimizerOptions`(Rule の値渡し、
  名前での Remove / 差し替え)。
- Rule の事前条件ゲートと `docs/design.md` D5 監査(無効化済み Rule:
  `push_down_limit_through_join`, `topn_push_through_inner_join`)。
  「間違った最適化は最適化ではない」を貫く設計思想として扱う。
- 特に重要な正しさの論点: SINGLE リレーション Group では scan filter が唯一の
  述語評価点になるため、filter を濃くする Rule は最適化ではなく**正しさの変更**
  になる(『docs/cascades_optimizer.md』サーチフロー節)。outer join 経由の
  プッシュダウン禁止(null-rejection 解析未実装)など「ガードレール」の話は
  各 Rule 章で必ず触れる。

## 5. ファイル構成(決定: 二部構成 + 1 ルール 1 ファイル)

```
docs/opt/
  AGENTS.md                        本ファイル(執筆憲法)
  00-index.md                      目次 + 進捗トラッカー(次ターンで作成)
  part1-foundations/               第1部 基礎編(通読向け)
    10-cascades-overview.md        Cascades とは / tinylamb の全体フロー
    20-memo-structure.md           Memo / Group / LogicalExpression
    30-pattern-and-binding.md      Pattern DSL と Bindings(Rule の書き方)
    40-search-and-cost.md          SearchEngine / コスト / 枝刈り / 縮退
    50-physical-properties.md      PhysicalProperties と enforcement
    60-rule-governance.md          Rule 追加・無効化・事前条件ゲート(D5)
    70-expression-rewrite.md       式書き換え: フレームワークと自明な Rule の一括解説
  part2-rules/                     第2部 Rule リファレンス(辞書向け)
    logical/<rule-name>.md         論理 Rule 117 本(1 ルール 1 ファイル)
    implementation/<rule-name>.md  実装 Rule 52 本(1 ルール 1 ファイル)
    expression/<rule-name>.md      式書き換え Rule の非自明なもの(個別章)
```

第1部を先に固め、その後に第2部をファミリー順(§11 の分類)に執筆する。
ファイル番号の重複・欠番・挿入は 00-index.md で管理する。

## 6. Rule 章のテンプレート(固定)

`part2-rules/` 配下の各ファイルはこの順で書く。省略できる節はない。

**自足性(決定: 2026-09-13)**: 第2部はどの章から読み始めてもよいことを
目標にする。他章で定義した用語に依存するときは、その場で 1 文で再説明するか
第1部の該当節へリンクする。「第1章を読んでいないと分からない」章を作らない。

```markdown
# <rule-name>

- 状態: draft | review | done   /   執筆基準リビジョン: <hash> (<date>)
- 定義位置: <file> の <シンボル or 登録式>

## 概要
(客観的に 1〜2 段落。変換内容と目的、理論的背景)

## 変換前後の関係
(図。§7 のスタイルに従う)

## 適用条件
(pattern と guard をコード引用つきで正確に列挙。「〜のとき発火しない」
否条件も明示する)

## 意味論的根拠と〇〇
(代数的一致、三値論理、短絡評価と例外消去防止、D6 規律、重複度保存等、
条件を外したときの反例や意味論的保証の根拠を詳述)

## 実装の詳細
(コード引用 + アルゴリズム・データ構造の精密な段落解説)

## 最適化効果
(何が減り何が増えるか、探索空間の質やコスト評価上の意味)

## 関連 Rule との相互作用
(前後に発火する Rule、競合・抑止の関係)

## 検証テスト
(該当テストファイルとテストケース名、検証内容)
```

## 7. 図のスタイル(決定: Mermaid 中心)

- プラン木は `graph TD`、Memo 内の Group 関係も `graph TD` で Group を
  ノードにする。

  ```mermaid
  graph TD
    subgraph before["変換前: Selection(Selection(X, p1), p2)"]
      S1["Selection p2"] --> S2["Selection p1"] --> X["Scan t"]
    end
    subgraph after["変換後: Selection(X, p1 AND p2)(merge_selections)"]
      S3["Selection p1 AND p2"] --> X2["Scan t"]
    end
  ```

- ノードラベルは演算子名 + 要約(述語・キー)。Group 図では
  `Group#7 {t1, t2}` のように id と関係集合を併記する。
- 番号付き手順(タスクループ等)は Mermaid の flowchart か、表で代替してよい。
- Mermaid がレンダリングされない環境でも、ソースのテキストとして意味が
  通るノードラベルを付ける。

## 8. 文体・用語

- 日本語。本文はですます調(敬体)。コード・識別子・Rule 名は英語のまま。
- 専門用語の初出は原語を併記する(例: メモ(Memo))。
- Rule 名・識別子はバッククォートで囲む。
- 数の主張(「117 本」等)は基準リビジョン時点の值であり、ずれたら §3 の運用
  で直す。

## 9. 複数ターン執筆ワークフロー

1. ターン開始: 本ファイル + `docs/opt/00-index.md` を読む。進捗表で
   「次に書く章」を確認する。
2. 原稿対象コードを読む(章の主題 + 関連テスト)。引用はその場でファイルから
   取る。記憶からコードを再構成しない。
3. 執筆: 1 ターンで基礎章 1 本、または Rule 章なら 1〜3 本を原則の目安とする。
   完成度が条件で、量は条件ではない。
4. ターン終了前に `00-index.md` の進捗表を更新する(状態: 未執筆 → draft →
   review → done)。
5. 引用した全コード片について、ファイル内に存在するかを grep 等で確認する。
   `EXPLAIN` 実行例は載せない(決定: 2026-09-13)。プランの例は図で示す。
6. 分割できないほど長い 1 ファイルは避ける。1 Rule 章が 300 行を超えるようなら
   第1部に切り出すべき内容(インフラ解説)が混ざっていないか見直す。

## 10. 検証に使える道具

- 本書に `EXPLAIN` の実行例は載せない(決定: 2026-09-13)。プランの例は
  すべて変換前後の図で示す。「この Rule が適用されると木がどう変わるか」
  「なぜそれが意味保存なのか」を語ることを優先する。
- テスト: `./build/plan_test`, `./build/optimizer_test`, `./build/cascades_test`,
  `./build/plan_extra_test`。各 Rule 章の「テスト」節で対応を示す。
- Rule の切替実験: `OptimizerOptions` で特定 Rule を Remove するとプランが
  どう変わるか(`docs/cascades_optimizer.md` のサンプル参照)。
- ファザー: `plan/rule_fuzzer.cpp`(Rule 適用の健全性)、
  `plan/plan_memo_oracle.cpp`(Memo の不変条件)。第1部の「どう検証しているか」
  で紹介する。

## 11. Rule インベントリ(網羅リスト / 分類は暫定)

権威ある一覧はコード。執筆順はこの分類にこだわらず、第1部で必要になる概念から
先に解説してよい。各 Rule の進捗は 00-index.md のトラッカーで管理する。

### 論理 Rule(117 本)

- **A. 結合の順序・形状 (11)**: `join_commutativity` / `join_enumeration` /
  `join_associativity_left` / `join_associativity_right` /
  `join_to_cross_if_no_predicate` / `cross_to_inner_with_predicate` /
  `outer_join_associativity` / `semi_join_commutativity` /
  `semi_join_inner_join_reorder` / `greedy_join_order_fallback` /
  `star_join_reorder`
- **B. フィルタのマージとプッシュダウン (25)**: `merge_selections` /
  `merge_adjacent_filters` / `push_selection_into_scan` /
  `push_selection_through_join` / `split_selection_over_join` /
  `push_filter_through_distinct` / `push_filter_through_sort` /
  `push_filter_through_left_join_left_side` /
  `push_selection_through_projection` / `push_selection_through_aggregation` /
  `infer_join_predicates` / `infer_filter_from_equivalence_class` /
  `join_predicate_transitivity` / `inferred_inequality_pushdown` /
  `dynamic_filter_pushdown_join` / `having_to_filter_rewrite` /
  `filter_pull_up_for_extreme_selectivity` /
  `functional_dependency_filter_reduction` /
  `scan_zone_map_filter_integration` / `check_constraint_predicate_intake` /
  `unnest_filter_pushdown` / `recursive_termination_predicate_pushdown` /
  `cast_pushdown_on_comparison` / `extract_year_sargable` /
  `comparison_self_predicates`
- **C. 削除・簡約 (20)**: `eliminate_false_selection` /
  `eliminate_true_selection` / `join_on_false_to_empty` /
  `join_empty_simplification` / `setop_empty_simplification` /
  `setop_empty_identity` / `eliminate_identity_projection` /
  `eliminate_double_sort` / `eliminate_sort_under_unordered_consumer` /
  `no_op_window_elimination` / `one_row_cross_join_elimination` /
  `self_join_elimination` / `unused_join_elimination` / `fk_join_elimination` /
  `foreign_key_outer_join_elimination` / `redundant_join_predicate_elimination` /
  `pk_unique_distinct_elimination` / `not_null_is_not_null_elimination` /
  `any_value_elimination` / `join_identity_dummy`
- **D. Projection (9)**: `merge_projections` / `merge_adjacent_projections` /
  `push_projection_through_join` / `push_projection_through_union` /
  `push_projection_through_aggregation` / `projection_cse_and_pruning` /
  `projection_constant_propagation` /
  `push_projection_below_join_width_control` / `aggregate_projection_merge`
- **E. LIMIT / TOP-N / SORT (9)**: `merge_limits` /
  `push_limit_through_projection` / `topn_push_through_projection` /
  `union_all_push_limit` / `push_limit_through_union_all` /
  `push_limit_through_left_join` / `limit_push_through_sort` /
  `sort_merge_of_compatible_orders` / `rank_row_number_to_topn`
- **F. 集約 (14)**: `distinct_over_group_by` / `distinct_over_distinct` /
  `distinct_and_group_by_interchange` / `count_star_without_group_rewrite` /
  `count_star_rewrite_on_not_null` /
  `group_by_functional_dependency_reduction` /
  `unique_group_key_aggregate_elimination` / `count_distinct_expansion` /
  `grouping_sets_expansion` / `eager_aggregation_over_join` /
  `aggregate_join_transpose` / `aggregate_union_transpose` /
  `push_aggregation_through_union_all` / `filter_aggregate_pushdown`
- **G. ウィンドウ関数 (5)**: `split_window` / `merge_adjacent_windows` /
  `push_selection_through_window` / `window_frame_sort_sharing` /
  `window_after_filter_partition_pushdown`
- **H. 結合種別の変換 (11)**: `unique_semi_to_inner` / `outer_to_anti_join` /
  `right_to_left_outer_join` / `full_outer_join_decomposition` /
  `outer_to_inner_join_on_null_rejecting_filter` /
  `semijoin_to_inner_plus_distinct` / `mark_join_to_filter` /
  `in_list_to_semi_join` / `intersect_to_semijoin` / `except_to_antijoin` /
  `push_semi_join_through_inner_join`
- **I. 相関サブクエリ・Apply (5)**: `apply_to_join` /
  `hoist_correlated_selection_to_apply` / `push_selection_through_apply` /
  `push_apply_through_join` / `decorrelate_aggregate_apply`
- **J. 集合演算 (6)**: `push_filter_past_setop` /
  `union_to_union_all_plus_distinct` / `union_all_merge` /
  `union_distinct_hash_sort_choice` / `intersect_except_cost_based_lowering` /
  `values_fold_into_union`
- **K. その他の式書き換え (2)**: `push_not_through_expression` /
  `order_by_redundant_column_removal`

### 実装 Rule(52 本)

- **スキャン (2)**: `full_scan` / `index_scan`(forward/reverse、IndexOnly /
  Bitmap / MinMax への分岐を含む)
- **リレーション供給 (6)**: `empty` / `dummy_scan` / `constant_table` /
  `values` / `generate_series` / `relational_ir`
- **選択・射影 (2)**: `selection` / `projection`
- **結合 (17)**: `nested_loop_join` / `hash_join` / `single_hash_join` /
  `merge_join` / `index_join` / `cross_join` / `outer_nested_loop` /
  `outer_hash_join` / `semi_hash_join` / `semi_merge_join` / `anti_hash_join` /
  `anti_merge_join` / `mark_hash_join` / `batch_nested_loop` /
  `batch_nested_loop_semi` / `batch_nested_loop_anti` /
  `batch_nested_loop_outer`
- **集約・DISTINCT (4)**: `aggregation` / `distinct` / `sort_distinct` /
  `skip_scan_distinct`
- **ソート・行制限 (4)**: `sort` / `topn` / `limit` / `max1_row`
- **集合演算 (6)**: `union` / `union_all` / `intersect` / `intersect_all` /
  `except` / `except_all`
- **その他 (7)**: `unnest` / `apply` / `recursive_cte` / `materialize` /
  `eager_spool` / `lazy_spool` / `window`
- **分散プレースホルダ (4)**: `exchange_noop` / `gather_noop` /
  `broadcast_noop` / `redistribute_noop`

### 式書き換え Rule(約 74 本)(決定: 2026-09-13 — 1 本も飛ばさない)

`expression/rewrite.cpp` の `ExpressionRuleSet::Default()` は全 Rule を解説
する。定数畳み込み(`x + 0`、`x * 1` など)のように自明度の高いものは
第1部 `70-expression-rewrite.md` で一覧つきで一括解説し、ド・モルガン、
吸収則、LIKE の等値化、OR からの共通因子の因子出しなど非自明なものは
第2部 `part2-rules/expression/<rule-name>.md` で個別章にする。どちらに置くか
の最終判定は 70 章の執筆時に確定させ、00-index.md に反映する。

## 12. 決定事項と未決事項

### 決定済み(2026-09-13)

- 配置: `docs/opt/`(既存 `docs/` 配下)。
- 読者像: 二部構成(第1部 基礎編=入門者向け、第2部 Rule リファレンス=辞書)。
- Rule 章の粒度: 1 ルール 1 ファイル。
- 図: Mermaid 中心。
- 式書き換え Rule: **1 本も飛ばさず**全 Rule を解説する。自明なものは
  第1部 70 章で一括、非自明なもの(ド・モルガン程度以上)は第2部で個別章。
- `EXPLAIN` 実行例は載せない。「適用後のツリー」と「意味を変える書き換えを
  どう検知・禁止しているか」を漏らさず説明する方針で代替する。
- 文体: ですます調(敬体)。
- 執筆順: 第1部(基礎編)から順に。第2部はどこから読み始めてもよい自足性を
  保つ。

### 未決

(現在なし。新しい判断が必要になったらここに追記する)
