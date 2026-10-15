# Cascades ルール到達性監査（2026-10-02）

監査対象は `RuleSet::Default()` と `DefaultImplementationRules()`、
`Optimizer::Optimize` / `SqlEngine` の生成形状、Memo の再探索、物理選択と統計です。
開始時の作業ツリーを基準に、論理 121 → 83、物理 57 → 46 登録へ整理しました。
物理側は未使用 12 本を削除し `sort_incremental` を追加しています。
削除した演算子の enum・DSL・専用テスト・旧ルール解説も除去しています。
既存の未コミット変更とは別に監査開始時のファイルを保存して比較しました。

## 判定と修正

| 撤去した登録 | 理由と残す経路 |
|---|---|
| `aggregate_join_transpose` | 同型の eager 変換とともに撤去。通常の JOIN 後の集約は維持。 |
| `aggregate_projection_merge` | 射影の別名や計算式を代入せず射影を除去していた。入力列削減は出力射影を保持する push_projection_through_aggregation。 |
| `apply_to_join` | 二子 Apply 専用だが SQL は一子 Apply + relational_statement を生成する。SQL の実際の decorrelation と unary Apply の経路を維持し、架空の二子変換を撤去。 |
| `count_star_without_group_rewrite` | 定数表という名前で実表を再走査し COUNT を計算していた。通常の aggregation と index-only scan で扱う。 |
| `decorrelate_aggregate_apply` | 二子 Apply 専用だが SQL は一子 Apply + relational_statement を生成する。SQL の実際の decorrelation と unary Apply の経路を維持し、架空の二子変換を撤去。 |
| `filter_pull_up_for_extreme_selectivity` | 選択性を読まず、意味の異なるサイトが同じ派生タグを共有する。 |
| `greedy_join_order_fallback` | Memo 初期構築の GreedyConnectedSplit と重複。初期分割に絞り込み後の行数を接続する。 |
| `grouping_sets_expansion` | grouping_sets は単一 GROUP BY のキー列ベクトル。二列の GROUP BY を二つの grouping set と誤解する変換を撤去。SQL の GROUPING SETS 展開は継続。 |
| `hoist_correlated_selection_to_apply` | 二子 Apply 専用だが SQL は一子 Apply + relational_statement を生成する。SQL の実際の decorrelation と unary Apply の経路を維持し、架空の二子変換を撤去。 |
| `intersect_except_cost_based_lowering` | intersect_to_semijoin / except_to_antijoin に統合。NULL-safe 行同値と DISTINCT を維持。 |
| `mark_join_to_filter` | 製品経路に MarkJoin の生成元がなく、物理実装も marker を出さない。通常の半結合・反結合の経路は維持。 |
| `merge_adjacent_filters` | merge_selections に統合。述語の結合だけでなく内側 Selection を取り除く。 |
| `merge_adjacent_projections` | merge_projections に統合し output_schema を保持する。 |
| `one_row_cross_join_elimination` | タグ名から行数を推測し、最大一行と必ず一行を混同する。証明可能な dummy の除去は join_identity_dummy。 |
| `order_by_redundant_column_removal` | 短縮したキーの順序証明が元の ORDER BY へ伝わらず、SQL 層が全体ソートを追加する。sort_incremental による実行可能な prefix 利用へ置換。 |
| `push_aggregation_through_union_all` | aggregate_union_transpose と重複。最終集約の引数を部分集約の出力名に束縛し直さない旧変換を撤去。 |
| `push_apply_through_join` | 二子 Apply 専用だが SQL は一子 Apply + relational_statement を生成する。SQL の実際の decorrelation と unary Apply の経路を維持し、架空の二子変換を撤去。 |
| `push_limit_through_union_all` | union_all_push_limit に統合。OFFSET と加算オーバーフローの安全条件を一箇所にする。 |
| `push_projection_below_join_width_control` | push_projection_through_join に統合。出力射影を失う旧変換を撤去。 |
| `push_selection_through_apply` | 二子 Apply 専用だが SQL は一子 Apply + relational_statement を生成する。SQL の実際の decorrelation と unary Apply の経路を維持し、架空の二子変換を撤去。 |
| `rank_row_number_to_topn` | 実際の window payload と guard が矛盾して不発火。人工形状では順位列を失い window 関数を sort key にしてしまう。 |
| `scan_zone_map_filter_integration` | 参照されない派生 Group を作るだけ。zone map の実際の適用は scan/Selection 実行器が行う。 |
| `star_join_reorder` | join_enumeration と重複し、非接続の dimension 同士を直積にする候補を追加する。 |
| `union_distinct_hash_sort_choice` | union_to_union_all_plus_distinct と同じ変換。物理 distinct/sort_distinct で方式を比較する。 |
| `window_after_filter_partition_pushdown` | push_selection_through_window に統合。実際の window call の PARTITION BY を使う。 |
| `window_frame_sort_sharing` | merge_adjacent_windows と重複する上、内側 window の出力を消す。出力を両方保持する正規ルールへ統合。 |
| `broadcast_noop` | 生成元なし。実際の分散処理をせず子をそのまま返す登録を撤去。 |
| `constant_table` | values と重複する空の器。COUNT の偽定数化と合わせて撤去。 |
| `eager_spool` | 生成元なし。三登録とも同じ MaterializePlan を返すだけ。SQL の CTE materialization は既存の共有セル経路で実行する。 |
| `exchange_noop` | 生成元なし。実際の分散処理をせず子をそのまま返す登録を撤去。 |
| `gather_noop` | 生成元なし。実際の分散処理をせず子をそのまま返す登録を撤去。 |
| `generate_series` | SQL で生成されない論理演算子向けの登録。独立した実行器や直接 Plan API は必要な呼び出し元を持つものを維持。 |
| `lazy_spool` | 生成元なし。三登録とも同じ MaterializePlan を返すだけ。SQL の CTE materialization は既存の共有セル経路で実行する。 |
| `mark_hash_join` | 生成元なし。marker_column を実装せず SEMI に代用していた。 |
| `materialize` | 生成元なし。三登録とも同じ MaterializePlan を返すだけ。SQL の CTE materialization は既存の共有セル経路で実行する。 |
| `max1_row` | SQL の scalar subquery は別経路。人工 Memo の生成器だけが使う演算子・DSL・物理登録を撤去。 |
| `redistribute_noop` | 生成元なし。実際の分散処理をせず子をそのまま返す登録を撤去。 |
| `single_hash_join` | 生成元なし。Single の複数行エラー契約を実装せず LEFT OUTER に代用していた。 |
| `having_to_filter_rewrite` | SQL は HAVING を opaque grouped finish で処理し、Aggregation.predicate の生成元がない。人工 Memo 専用の登録を撤去。 |
| `any_value_elimination` | ANY_VALUE を持つ GROUP BY は relational 経路で、対象 payload が生成されない。実際の ANY_VALUE 評価は維持。 |
| `count_distinct_expansion` | scalar COUNT DISTINCT の既存 accumulator に対して、同じ入力を重複除去する pass と再集約を追加するだけ。索引 skip scan 等へ接続せず、正規コストで勝てない変換を撤去。 |
| `fk_join_elimination` | SQL の Join は target_list を持たず発火しない。人工形状での変換も FK の除去ではなく SEMI 化で、unused join と重複。 |
| `foreign_key_outer_join_elimination` | SQL が設定しない Scan.output_schema 上の FK を要求し、NOT NULL/FK の証明も同時に必要。SQL の outer-join slice の既存除去を維持し、この架空の証明経路を撤去。 |
| `unused_join_elimination` | 空の Scan.output_schema にある UNIQUE を要求し SQL で不発火。人工形状では Projection を消し、target_list を無視する semi 実装へ渡すため出力契約も壊す。 |
| `check_constraint_predicate_intake` | 同じ未設定 schema を前提とし、CHECK 文字列の部分一致を論理的証明と誤認する。OR を含む CHECK 等で不健全。SQL の制約検査は維持。 |
| `semijoin_to_inner_plus_distinct` | 射影を兼ねる SemiJoin の target_list を前提とするが、通常 SQL の半結合は射影を別ノードに持つ。撤去した unused join 由来以外は人工形状だけ。 |
| `unique_group_key_aggregate_elimination` | 未設定の Scan.output_schema の UNIQUE を要求し SQL から不発火。現行の keys + aggregate 出力契約で必要なキーも落とす。名前上の一意集約除去を撤去し、通常の集約を維持。 |
| `eager_aggregation_over_join` | 1:1 証明を作る経路は ORDER BY + LIMIT の二表 SELECT のみで、対応する grouped SQL の Memo payload とは交わらない。人工形状でも最終集約の引数を部分集約の出力名に再束縛しない。 |
| `group_by_functional_dependency_reduction` | target_list にないキーを落とすが、現行 AggregationPlan は全 grouping key を暗黙に出力する。親の Projection が必要とするキーまで消え、実行可能な同一 Group の代替にならない。出力復元のない変換を撤去。 |
| `projection_constant_propagation` | merge_projections が同じ置換を行い、内側射影も除去する。二段のまま定数だけ置換する旧登録は同額または余分な一走査で勝てない。定数伝搬の回帰を merge_projections 単独で検証する。 |

## 周辺の式書き換えと旧解説

`ExpressionRuleSet` の常時 no-op `nondeterministic_barrier`、
`safe_divide_rewrite`、`function_volatility_classification` を撤去しました。
volatile 関数の定数畳み込み禁止とゼロ除算の扱いは実際の評価・fold の guard に残します。
同内容で二度登録されていた `abs_of_abs` も一つにしました。
式ルールは 70 登録です（Cascades 論理・物理の本数とは別）。
既に登録が存在しない `eliminate_identity_projection` /
`projection_cse_and_pruning` の旧章も削除。恒等射影の除去はスキーマを比較する
物理 helper `RemoveIdentityProjection` が担当します。

## 探索と実行可能性

- 単一表の通常 SELECT も Memo を通します。従来の包括的な迂回経路は撤去し、
  正確な索引端点取得だけを `OptimizeMinMaxIndex` として残しました。
- NOT NULL の証明は未設定の Scan.output_schema ではなく、catalog から導出する LogicalProperties を読みます。
- 子を先に探索し、子の候補・scan filter が変われば既処理の親を再評価します。
  これで子に現れた Projection/Selection 等を待つ多段パターンが発火できます。
  検索予算・式数上限・worklist 上限は維持します。
- `push_selection_into_scan` は Selection の結果 Group に filtered scan を追加します。
  共有入力の filter を破壊的に狭めません。
- UNION の枝ごとの filter/projection/limit は元の child ID をタグに含めます。
  同じ関係集合を持つ別枝が同じ Group に混ざる問題を防ぎます。
- 空集合の子はスキーマ参照だけなので実行コストに加算しません。同額でも空集合を優先します。
- 順序を要求した子の選択スコアと実際の plan cost を分けます。
  まだ生成していない sort の罰則を親へ累積して二重加算しません。
- 集約と sort は通常の子に加え、キー順を要求する子も比較します。
  stream aggregation / blocked nested loop / sort distinct は従来方式との同額固定を解消します。
- COUNT(column) の引数評価・NULL 検査を全集約方式のコストに含め、NOT NULL による COUNT(*) 化が常に同点負けする問題を修正しました。SQL EXPLAIN の選択結果も検証します。
- SELECT DISTINCT の可視射影を Memo に接続し、hash / sort / index skip の方式を比較します。
  pk_unique_distinct_elimination は実際の子 Projection の入力キーで証明し、別名も保持します。
  scan のコピーは filter を伴います。skip_scan_distinct は射影後の列 0 の統計を使い、
  DISTINCT の子にも無制約 index-only scan を候補として渡します。
- grouped executor の EXPLAIN に実際の入力行数と出力 group 数を表示します。
- 単一表・列キー・直接 COUNT/SUM/AVG/MIN/MAX の単純 GROUP BY は、走査と集約を
  同じ Memo で計画します。別名と SELECT 順は最終 Projection で復元します。
  aggregate modifier、HAVING、ORDER/LIMIT、複雑な式などは既存の grouped finish を使います。
  group key を SELECT しない場合も、全キーで重複除去してから出力射影します。
- 通常の二子 join、set operation、window、scalar aggregate と、それらの派生形状は
  引き続き Memo 上で扱います。高度な SQL 全体を無条件に Memo へ取り込む変更ではありません。
- MergeJoin と BatchNestedLoopJoin の EXPLAIN に両入力を表示します。
  新たに選ばれた方式の下にある filter / Values / access path を確認できます。
- Selection の batch zone-map pruning は UINT64 を signed bounds で落とさないようにしました。
  ルールを有効化して二段 Selection が現れたことで検出した実結果の回帰です。

## 統計の接続

`ColumnStats::EstimateJoinMatches` は既存の ANALYZE が収集する MCV 頻度、
non-null/NULL 件数、NDV、typed histogram の範囲を読んで結合行数を推定します。
共通の頻出値を積算し、残りの質量を残余 NDV へ分配します。範囲が交わらなければ
非 NULL の一致は 0。NULL-safe equality の場合だけ NULL 件数の積を加えます。
複数キーの補正は `EstimateEquijoinSelectivity` の指数バックオフを使います。

`TableStatistics::EstimateDistinctRows` は GROUP BY / DISTINCT / incremental sort の
共通推定です。NULL を一つの distinct 値として数え、重複キーを除き、NDV の降順に
指数 1, 1/2, 1/4, … で合成して入力行数で上限を設定します。
キーを増やすと行数が減る旧式を撤去しました。これは多列の実測相関ではなく近似です。

`ColumnStats::ClusteringStrength` は既存の隣接昇順率から方向に依存しない
局所性 `abs(2 * Correlation - 1)` を計算します。昇順・降順どちらの連続走査も
範囲 I/O には有利です。一方、実際の昇順保証には従来の Correlation を使い、
このスコアを順序の証明に流用しません。

大きな結合（16 表超）の初期分割は、各基底表の WHERE 適用後行数を使います。
残りの接続性を保てる候補のうち大きい表を後ろに置きます。

今回必要な分布情報は既存の収集・永続化済み統計で足りるため、統計ファイル形式は
変更しません。未使用だった別系統の Histogram/Pattern/StarJoin/Calibration API は撤去。
実測の多列 NDV・多列相関、実行時フィードバックによる再最適化は未実装であり、
単なる helper の存在を実装済みと数えません。

## ファザーの再現性

`rule_fuzzer` は実際の登録数と base seed を起動時に表示し、
`--seed=<unsigned integer>` で乱数系列を再指定できます。
`--trace-cases` は worker・iteration・oracle pass を各実行前に記録します。
シードだけではスレッドの実行順までは固定しません。

## 検証の入口

- `cascades_test`: 親の再訪、共有 filter、枝の同一性、統計付き大規模 join seed、各論理変換。
- `optimizer_test` / `cascades_rules_coverage_test`: SQL からの到達、選択された実行方式と結果。
- `aggregate_shape_test` / `sort_shape_test`: child ordering を含む方式間のコスト比較。
- `table_statistics_test`: MCV skew、NULL-safe join、disjoint ranges、NDV の単調性。
- 全体 CTest、expression differential、SQL/セッション oracle、rule_fuzzer、include-lint。

## 検証結果と留保

- 全体 CTest: **3,766 件、失敗 0、意図的 skip 1**（442.90 秒）。
  最後の射影統合・式 no-op 撤去前の全体実行です。
- 最終ソースの関連 CTest: **746 件、失敗 0**（24.69 秒）。
  Cascades、optimizer、SQL 到達性、aggregate/sort shape、統計、
  expression differential、bytecode/JIT、rewrite/function-call を含みます。
- `check_layering.py`: 成功。65 個の既存 allowlist 辺のみ、追加なし。
- 登録一覧はソースと順序まで照合し、撤去した論理 38 登録すべてに
  判定理由があることと、文字列登録がソースに残らないことを確認しました。
- SQL の `AggregateUnionRewriteIsReachableFromSqlOr` は、OR の UNION ALL 化で
  `aggregate_union_transpose` が実際に到達することと COUNT/SUM の結果を検証します。
  `CountNonNullColumnRewritesToCountStar` は選択された COUNT(*) と、
  観測 NULL がゼロでも nullable 列は変換しないことを検証します。
- ASan/UBSan のシード付き実行: `--seed=2026100201 --threads=4 --duration=600`、
  **2,350 反復、失敗 0**。これは最後の no-op 撤去前の版です。

途中の監査版の無シード記録のファズで SIGSEGV が一回、別の ASan/UBSan 実行で
`std::vector::operator[]` の null reference が一回発生しました。後者は当時
stack trace を有効にしておらず、発生箇所・原因を特定できていません。
その後の GDB 実行（8,142 反復）と stack trace 付き ASan/UBSan 実行
（2,422 反復）、上記の固定シード実行では再現しませんでした。
これらの成功を根拠に「修正済み」「今回の変更とは無関係」とは断定しません。
今回追加した seed と case trace は、再発時の追跡に使います。

## 残した全登録

以下は現行ソースの登録を一件ずつ列挙したものです。論理登録は SQL の初期形状だけでなく
他ルールの生成する派生形状にも適用します。個別の guard と payload は登録本体が正です。
物理登録では、単なる候補の生成と最終選択は区別します。

### 論理

1. `join_enumeration`
2. `join_commutativity`
3. `join_associativity_left`
4. `join_associativity_right`
5. `join_to_cross_if_no_predicate`
6. `eliminate_false_selection`
7. `merge_selections`
8. `push_selection_into_scan`
9. `push_selection_through_join`
10. `split_selection_over_join`
11. `push_filter_past_setop`
12. `push_filter_through_distinct`
13. `push_projection_through_union`
14. `push_projection_through_join`
15. `merge_projections`
16. `push_selection_through_projection`
17. `push_limit_through_projection`
18. `topn_push_through_projection`
19. `union_all_push_limit`
20. `push_selection_through_aggregation`
21. `infer_join_predicates`
22. `merge_limits`
23. `sort_merge_of_compatible_orders`
24. `eliminate_true_selection`
25. `setop_empty_simplification`
26. `setop_empty_identity`
27. `union_to_union_all_plus_distinct`
28. `union_all_merge`
29. `push_filter_through_left_join_left_side`
30. `join_on_false_to_empty`
31. `eliminate_double_sort`
32. `distinct_over_group_by`
33. `push_projection_through_aggregation`
34. `limit_push_through_sort`
35. `push_filter_through_sort`
36. `eliminate_sort_under_unordered_consumer`
37. `distinct_over_distinct`
38. `cross_to_inner_with_predicate`
39. `join_empty_simplification`
40. `intersect_to_semijoin`
41. `except_to_antijoin`
42. `distinct_and_group_by_interchange`
43. `self_join_elimination`
44. `unique_semi_to_inner`
45. `outer_to_anti_join`
46. `right_to_left_outer_join`
47. `full_outer_join_decomposition`
48. `outer_to_inner_join_on_null_rejecting_filter`
49. `outer_join_associativity`
50. `semi_join_inner_join_reorder`
51. `semi_join_commutativity`
52. `push_selection_through_window`
53. `infer_filter_from_equivalence_class`
54. `push_semi_join_through_inner_join`
55. `push_limit_through_left_join`
56. `merge_adjacent_windows`
57. `push_not_through_expression`
58. `no_op_window_elimination`
59. `dynamic_filter_pushdown_join`
60. `semi_reduction_pushdown_semi_join`
61. `semi_reduction_pushdown_anti_join`
62. `join_predicate_transitivity`
63. `inferred_inequality_pushdown`
64. `redundant_join_predicate_elimination`
65. `unnest_filter_pushdown`
66. `recursive_termination_predicate_pushdown`
67. `in_list_to_semi_join`
68. `functional_dependency_filter_reduction`
69. `filter_aggregate_pushdown`
70. `aggregate_union_transpose`
71. `pk_unique_distinct_elimination`
72. `not_null_is_not_null_elimination`
73. `join_identity_dummy`
74. `count_star_rewrite_on_not_null`
75. `topn_push_through_proven_one_to_one_join`
76. `or_to_union`
77. `distinct_eager_over_join`
78. `topn_push_through_union_all`
79. `split_window`
80. `values_fold_into_union`
81. `cast_pushdown_on_comparison`
82. `extract_year_sargable`
83. `comparison_self_predicates`

### 物理

1. `relational_ir`
2. `dummy_scan`
3. `values`
4. `unnest`
5. `apply`
6. `recursive_cte`
7. `index_scan`
8. `full_scan`
9. `selection`
10. `sample`
11. `projection`
12. `aggregation`
13. `sort_incremental`
14. `sort`
15. `sort_external_merge`
16. `window`
17. `topn`
18. `empty`
19. `distinct`
20. `sort_distinct`
21. `skip_scan_distinct`
22. `union`
23. `union_all`
24. `intersect`
25. `intersect_all`
26. `except`
27. `except_all`
28. `limit`
29. `hash_join`
30. `merge_join`
31. `band_join`
32. `zigzag_join`
33. `outer_hash_join`
34. `cross_join`
35. `semi_hash_join`
36. `anti_hash_join`
37. `semi_merge_join`
38. `anti_merge_join`
39. `index_join`
40. `late_materialization_join`
41. `nested_loop_join`
42. `outer_nested_loop`
43. `batch_nested_loop`
44. `batch_nested_loop_semi`
45. `batch_nested_loop_anti`
46. `batch_nested_loop_outer`
