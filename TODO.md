# Cascade Optimizer 高速化 TODO（2026-09-16 策定）

前回考察（chat: cascade ルール追加提案）の推奨着手順を、他エージェントへ
引き継げる明細として分解したもの。現状の土台は以下を参照：

- `docs/cascades_optimizer.md`（D5 監査表・探索フロー・健全性不変条件）
- `docs/optimizer_todo.md`（P0〜P8 の到達/未達マップ）
- `docs/optimizer_improvements.md`（Phase 8/9 の経路地図 M4〜M9）
- `docs/executor_todo.md`（物理オペレータ側の未達）
- `docs/opt/part2-rules/logical/*.md`（各 Rule の現状メモ・監査指摘 A〜E）

全体不変条件（破ったら即 revert）：

1. `Memo::NewJoin` を経由しない結合合成は禁止（conjunct の二重適用/欠落）。
2. `LimitPlan`/`TopNPlan` は required ordering を満たす子の上にしか畳まない（D6）。
3. 外側結合への pushdown は null-rejection 解析が先。
4. `Selection(Limit)` と `Limit(Selection)` は一般に非等価。交換しない。
5. 論理規則は memo groups + 論理 operators のみに触れる。
   catalog/executor/storage 型は実装規則 + `RuleContext` の背後に隔離。
6. 変更後は `ctest` + `python3 scripts/check_layering.py`（exit 0）+
   `expression/differential_test` を通す。触った `*.hpp/*.cpp` は
   `clang-format -i`。

---

## 1. FK + clustering factor（実装済み 2026-09-16）（前提工事：封印規則の解禁と誤選択防止）

狙い：D5 監査表で Disabled の `push_down_limit_through_join` /
`topn_push_through_inner_join` を安全に復活させ、index vs full-scan の
誤選択（非クラスタ化 index のランダム I/O 過小評価）を直す。
波及が最大なので最初にやる。

- [x] 1a. `ColumnStats` に物理順序相関 `correlation_`（0..1）を追加
  - 実装済み (2026-09-16): `table/table_statistics.{hpp,cpp}`、
    `common/decoder.hpp` に `AtEnd()` を追加し旧 payload は 1.0 既定で読取。
    `operator==` は既定メンバ比較に追随、`ostream` 表示は不変。
    回帰: `table_statistics_test` の `Correlation_*` 2件
  - `table/table_statistics.hpp`（`ColumnStats` に `double correlation_{1.0}`
    + `Correlation()` アクセサ）
  - `table/table_statistics.cpp`（`Update()` の reservoir sampling 時に
    (physical_pos, sorted_pos) の順序相関を推定。`Encoder/Decoder` 永続化、
    `operator==`・`operator<<`・`Assign`・`Duplicate`・`operator*=` の追随）
  - 互換性注意：永続化フォーマットを変える場合は旧 blob 読取の
    downgrade path（`table/README.md` の legacy min/max upgrade が前例）を
    用意し、`table_statistics_test` に round-trip 回帰を追加
- [x] 1b. range scan の index コストに clustering penalty を適用
  - `plan/implementation_rules.cpp` の `index_scan` 規則内
    （`BuildIndexScan` 候補の `local_cost` 計算箇所。
    先頭キー equality-prefix のみの point lookup は対象外、
    range を含む候補のみ `local_cost *= (1 + k*(1-correlation))`。
    `k` は `CalibrateOperatorCost`（`plan/cascades.cpp:10304`）側の較正係数と
    並べ、テストで full/index 選択が flip することを pin）
  - `optimizer_cardinality_costing_high_expectations.test` 系に期待値追加
- [x] 1c. `topn_push_through_proven_one_to_one_join` 論理規則（kForeign ゲート版）
  - 実装名は FK＋snapshot 両対応のため `topn_push_through_proven_one_to_one_join`
    （`plan/cascades.cpp` の DISABLED コメント直下）。発火条件は D5 表の新規行を参照
  - `plan/cascades.cpp:9347` の DISABLED コメント直下に新規則を追加。
    発火条件：`TopN(Join(L,R))`（inner のみ）＋ 右結合キーが
    `IsUniqueOn` ＋ 左結合列の schema 制約が `Constraint::kForeign` で
    参照先が右 relation と一致（`foreign_key_outer_join_elimination`
    `plan/cascades.cpp:5343` の参照先照合ロジックを再利用）＋
    `!with_ties` ＋ 有限 limit。書換えは `Join(TopN(L'), R)` ではなく
    D5 表の反例を避けるため **1:1 証明が揃う場合のみ**
    左側へ `TopN(limit+offset)` を押す形（`push_limit_through_left_join`
    `plan/cascades.cpp:5703` が前例。右キー uniqueness 証明付き左押込み）。
  - 反例テスト：`TopNPushThroughInnerJoinOnForeignKey`（既存）と対になる
    肯定テスト `TopNPushThroughInnerJoinWithForeignKeyFires` を
    `plan/cascades_test.cpp` に追加。D5 監査表へ行追加
- [x] 1d. snapshot による参照整合性証明（DDL なしで今日速くする本体）
  - `plan/cascades.hpp` の `Memo` に `proven_one_to_one_`（relation-pair set）
    + `MarkProvenOneToOne(a,b)` / `IsProvenOneToOne(a,b)` を追加
    （純データ構造の拡張であり層違反ではない）
  - `plan/optimizer.cpp` の QueryData→Memo 翻訳時（`TryEliminateUnusedOuterJoin`
    の `kMaxJoinEliminationProofRows = 1<<16` snapshot 走査が前例
    `plan/optimizer.cpp:406`）に、TopN/LIMIT 直下の単一 inner join について
    左外部キーの全 non-NULL 値が右 unique 列に存在することを証明し、
    通れば `MarkProvenOneToOne`
  - 1c 規則の発火条件を「kForeign **または** `IsProvenOneToOne`」に一般化。
    論理規則は依然 memo のみに触れる（snapshot I/O は optimizer.cpp 側）ため
    「論理パターンは memo のみ」の規律を維持
  - `optimizer_test.cpp` に end-to-end（TPC-H Q3/Q10 型 ORDER BY+LIMIT が
    TopN push で計画されること）を追加

残件（別タスク化可）：`FOREIGN KEY` DDL（parser `VisitCreate`
`query/googlesql_ast_visitor.cpp:6109` が制約を捨てている。外部 pinned parser
の AST dump に FK 情報があるか確認が先。なければ DDL は見送り）

## 2. CTE predicate pushdown（M4 の行削減）（実装済み 2026-09-16）

狙い：`MaterializeCtes`（`query/sql_engine.cpp:3799`）の共有 eager cell が
外側 WHERE を被らず全行実体化している問題を解消。1024行予算
（`kMaxMaterializedCteRows`）の浪費と cell scan の I/O を削減。

- [x] 2a. 全サイト弱化による cell 絞り込み（単一サイトは特例として包含）
  - 実装済み: `TryNarrowCteBody`（`query/sql_engine.cpp`）。単一参照限定ではなく
    全サイトのフィルタの論理帰結（列毎の下限min・上限max・全会一致の等価）のみ注入。
    外側フィルタは残置。GROUP BY/DISTINCT/LIMIT/QUALIFY/WITH TIES/window/派生・
    star・複数表 body は対象外。矛盾サイトは除外、型不一致等価は列無効化
  - `MaterializeCtes` の body 実行直前（fresh `PrepareStatement` 前）に
    `TryPushCteFilterIntoBody(body, outer_where, cte_alias)` を挿入
  - ゲート：body が単一 plain base table・行保存
    （`InlineSingleUseCtes` `query/sql_engine.cpp:2789` の body 判定を再利用）
    ＋ CTE 参照サイトが単一（`CountCteReferencesInStatement == 1`。
    共有 cell への site-specific filter 混入を防止）＋ 外側 conjunct が
    `alias.out_col OP const`（`OP ∈ {=,<,<=,>,>=}`、const immutable、
    volatile/subquery なし）＋ `out_col` が body SELECT の単一 base 列の
    素通し（`base_col [AS out_col]` のみ。式・集約は不可）
  - 注入は body WHERE への AND のみ。**外側フィルタは残す**
    （residual 再評価＝安全網。単一表 scan filter の exactness 規約
    `docs/cascades_optimizer.md:28` に抵触させない）
  - 三値論理：`NULL` 比較は UNKNOWN で両側とも落とすため等価。
    `IS NULL`/`IS NOT NULL` 形は v1 対象外
- [x] 2b. 回帰テスト（`query/query_test.cpp` 系）
  - `SqlEngineMultiSiteCtePushesWeakenedFilter`（弱化＋全会一致等価＋Values リーフ）、
    `SqlEngineWeakenedCteFilterFitsCellBudget`（発火証明：1024行予算を超過するbodyが
    絞り込みで lift する）、`SqlEngineMultiUseCteCellIgnoresSiteSpecificFilter`
    （共有 cell 汚染防止）、`SqlEngineGroupedCteBodyKeepsGroupsUnderFilter`（集約境界）
  - `CteFilterPushdownNarrowsMaterializedCell`（plan ダンプ/EXPLAIN で
    body 側に範囲が付くこと＋結果一致）
  - 複数参照 CTE では発火しないこと（共有 cell 汚染防止の否条件）
  - `OptimizerAndRelationalPathsAgree` 監査に CTE+フィルタ形を追加

残件：複数サイト共通フィルタの intersection push、CTE推移的多段 push、
`cte_predicate_propagation`（等価クラス経由の join 定数伝播。M4 葉待ち）

## 3. LATERAL decorrelation（per-row 再実行の排除）（実装済み 2026-09-16）

狙い：`has_lateral` → `ExecuteUnnestSelect`/relational fallback
（`query/sql_engine.cpp:5558`）の `O(N*M)` を等価相関の hash join 化で
`O(N+M)` に。M5 葉化の最小スライス。

- [x] 3a. 単一表・等価相関 LATERAL の statement-level decorrelation
  - 実装済み: `DecorrelateSingleTableLaterals`（`query/sql_engine.cpp`、
    `FlattenDerivedSources` 直後）。相関等値を ON へ移動（派生別名へ再束縛、
    非出力キーは射影拡張）＋ lateral clear ＋ kCross→kInner 昇格（LEFT 維持）＋
    M5 flatten へ委譲。volatile/集約/非等価/複数表 inner・RIGHT/FULL・star は対象外
  - `FlattenDerivedSources`（`query/sql_engine.cpp:3372`）の直後に
    `DecorrelateSingleTableLateral(outer)` を挿入（routing 前。
    `InlineSingleUseCtes→MaterializeCtes→LiftRecursiveCtes→FlattenDerivedSources`
    の呼出順 `query/sql_engine.cpp:5258` を崩さない）
  - ゲート（`FlattenOneDerivedSource` `query/sql_engine.cpp:3399` の判定を準用）：
    site が `is_lateral` ＋ inner が単一 plain base table・行保存
    （group/agg/distinct/limit/window/setop/star/CTE なし）＋ inner WHERE の
    conjunct にちょうど1つの `outer_col = inner_col` 等価相関があり、
    それ以外の inner 式（SELECT/WHERE/ORDER）に outer 参照なし
    （scope 外参照検出は `DerivedSourceIsLocal` 系ヘルパと
    `CanUseDecorrelatedSubqueryOptimizer` `query/sql_engine.cpp:278` の
    相関キー検査を再利用）＋ outer が grouped/window でない
  - 書換え：相関等式を inner WHERE から outer の `join_condition`
    （`SelectSource::join_type = kInner`、cross からの昇格）へ移動し、
    inner 式を merged scope へ再束縛（`RebindInnerExpression` 前例あり
    `query/sql_engine.cpp:3531`）、`is_lateral` を clear。
    移動（copy ではない）＋ outer 側に residual を残さないのは
    join_condition が等価に評価されるため。非等価相関は対象外
- [x] 3b. 回帰テスト
  - `SqlEngineSingleTableLateralDecorrelatesToJoin`（HashJoin 化＋Apply 消滅＋行一致）、
    `SqlEngineLeftLateralDecorrelatesWithNullPadding`（NULL pad 維持）、
    `SqlEngineNonEquiAndAggregateLateralStayCorrect`（非等価・集約は旧経路で一致）。
    既存 `LateralJoinExpansion` は decorrelated 期待値へ更新（行アサートは等価性の証拠として維持）
  - `LateralSingleTableEqualityDecorrelates`（結果一致＋EXPLAIN が
    hash join になること）
  - 否条件：複数相関・非等価相関・集約 inner・outer 参照残存は
    relational 維持（`PostRewriteNeedsRelational` が true のまま）

残件：複数表 LATERAL、LATERAL + 集約の `decorrelate_aggregate_apply`
対応（memo 葉化 M5 待ち。一般形は `docs/optimizer_todo.md:491` のまま）

## 4. 論理規則3点（OR-to-UNION / 集約 duality / TopN-through-Union）（実装済み 2026-09-16）

いずれも `plan/cascades.cpp:RuleSet::Default` への additive rule
（`memo.AddExpression(group, ...)` のみ。既存式の削除・置換なし）＋
`plan/cascades_test.cpp` の肯定/反例テスト＋必要なら D5 行追加。

- [x] 4a. `or_to_union`（選言の union 化）
  - 実装済み: `or_to_union`（`plan/cascades.cpp`）。2選言・単一relation・安定述語のみ
    `kUnion`（DISTINCT）代替を追加。同列定数等式は `or_to_in` に譲る。分岐タグは選言内容で
    アドレス指定（D1）。`OrToUnion*` 3件
  - パターン `Selection(OR(d1,d2))`。ゲート：ちょうど2 disjuncts＋各
    disjunct は conjunct 単位（AND 可、そのまま branch predicate 化）＋
    グループが単一 relation＋両 disjunct 非 volatile・subquery なし
    （`distribute_or_over_and_budgeted`
    `expression/rewrite.cpp:3466` の `ExpressionCannotThrow` /
    `SafeToReduceEvaluationCount` 検査を再利用）＋派生タグ
    `or_union_branch:` での循環防止（`union_all_push_limit`
    `plan/cascades.cpp:2988` の `union-limit-setop:` ガードが前例）
  - 書換え：`EnsureDerivedGroup(rels, "or_union_branch:0/1")` に
    `Selection(d_i)` を載せ、同一 group へ `kUnion`（DISTINCT。
    overlap があっても重複排除で等価。Selection は TRUE のみ保持するので
    三値論理でも `FALSE OR NULL`≡両枝で落とす、で一致）を追加。
    memo 検証（`plan/cascades.cpp:1442`：children relations の union が
    group と一致）は同-relation children で充足
  - v1 は `kUnion` 固定（disjoint 証明が要る `UnionAll` 化は将来。
    同一列の定数等式 OR は既存 `or_to_in` が担当のため重複回避）。
    反例テスト：volatile 述語・複数 relation・3 disjunct で不発
- [x] 4b. 集約 duality：`distinct_eager_over_join`＋lazy 共存保証
  - 実装済み: `distinct_eager_over_join`（inner 両側＋LEFT 保存側、他側キー unique
    ゲート。 containment 不要の証明を D5 表に記録）。純粋 lazy 規則は不要と確定：
    eager が additive なため B&B が自動選択する。`DistinctEagerOverJoin*` 2件＋
    `EagerAndLazyAggregationCoexistForCostChoice`（両形状の共存 pin）で保証
  - 背景：`lazy_aggregation` は eager の双対だが、eager が additive
    （元形状 `Agg(Join)` は memo に残り B&B cost が選ぶ）なため、
    純粋な「遅延配置規則」は空転する。真の不足は (i) DISTINCT 版 eager が
    ないこと (ii) selective-join 時に cost が join-first を選ぶことの保証
  - 規則 `distinct_eager_over_join`：パターン `Distinct(Join(L,R))`、
    ゲート inner/left のみ＋右結合キーが `IsUniqueOn`
    （`RightSideJoinKeysAreUnique` `plan/cascades.cpp:137` を再利用）＋
    DISTINCT 対象列が全て左 relations 由来。書換え
    `Join(Distinct(L'), R)`（`eager_agg_left/eager_agg_join`
    `plan/cascades.cpp:6642` と同型の2段化ではなく DISTINCT は単段。
    outer は LEFT のみ、RIGHT/FULL は対象外）
  - テスト：肯定（`SELECT DISTINCT l.* FROM l JOIN r` で narrow 側 DISTINCT
    が選ばれ得ること）＋反例（右キー非 unique・FULL・右列参照で不発）。
    さらに `LazyAggregationPrefersJoinFirstWhenSelective`（eager 代替が
    存在しても selective join では単段 agg が勝つ cost 順序テスト）で
    lazy 側を pin
- [x] 4c. `topn_push_through_union_all`（順序付き版）
  - 実装済み: `topn_push_through_union_all`（`plan/cascades.cpp`）。各枝に
    `TopN(limit+offset)`＋親 TopN 維持。`!with_ties`・同形枝 cap 済み検査・枝 GroupId
    タグ（D1）。既存 limit 系2規則は不変。`TopNPushThroughUnionAll*` 2件
  - 既存 `union_all_push_limit` / `push_limit_through_union_all`
    （`plan/cascades.cpp:2991/5155`、無順序 LIMIT の branch cap。重複登録の
    統合は `docs/optimizer_todo.md:1054` の監査 C 項として別途）は触らず、
    `TopN(keys,limit,offset)` over `UnionAll` の新規則を追加
  - ゲート：`!with_ties`（ties 境界は行数保証が崩れる）＋有限 limit＋
    各枝が既に `TopN(count<=limit+offset)` を持たないこと
    （`already_limited` 検査 `plan/cascades.cpp:5178` を再利用）＋
    派生タグ `topn_union_child:` / 合流 `topn_union_pushed`
    （既存 limit 系タグと衝突させない）
  - 書換え：各枝に `TopN(keys, limit+offset, 0)`、親に元 `TopN` を維持。
    親 TopN が順序を確立するため D6 適合。各枝 TopN は
    `implement_set_operation`（`plan/implementation_rules.cpp:2428`）の
    `MergeAppend` 経路（順序要求時の子 Sort 強制挿入）と相乗
  - テスト：肯定（各枝 TopN＋親 TopN の形状）＋反例（with_ties・無限・
    二重適用防止）。`union_all_push_limit` との相互作用テスト
    （両規則が共存しても `ExploreTwiceReachesSameFixpoint` が保たれる）

## 5. 並列の実装規則分岐（DOP のコスト化・第一級化）（実装済み 2026-09-16）

狙い：`kParallelScanMinRows/kParallelAggregationMinRows = 8192`
（`plan/parallel_thresholds.hpp`）の閾値駆動は scan/agg のみ。
実行器は `SharedBuildParallelHashJoin`（`executor/parallel_hash_join.*`）、
`ParallelMergeJoin`（`executor/parallel_merge_join.*`）を持ちながら
`ProductPlan::EmitExecutor` / `MergeJoinPlan::EmitExecutor`
（`executor/relational_factory.cpp:453/470`）が常に逐次形を出す。

- [x] 5a. join 並列閾値を `parallel_thresholds.hpp` に追加
  - 実装済み: `kParallelHashJoinMinRows` / `kParallelMergeJoinMinRows`（=8192、scan/agg と同根拠）
  - `kParallelHashJoinMinRows` / `kParallelMergeJoinMinRows`
    （初期値 8192。scan/agg と同一根拠：スレッド起動+morsel handoff を
    ペイする規模。コメントに根拠を明記）
- [x] 5b. factory の自動選択（scan/agg と同型）
  - 実装済み: `executor/relational_factory.cpp`。inner equi hash は
    `SharedBuildParallelHashJoin`（null-safe キー除外—並列側に符号化なし）、
    inner 無 residual merge は `ParallelMergeJoin`。worker は `min(16, hw)`（集約と同型）
  - `ProductPlan::EmitExecutor`：inner equi hash join で
    `left_src_->EmitRowCount() + right_src_->EmitRowCount()` が閾値以上なら
    `SharedBuildParallelHashJoin`（worker 数は
    `relational_factory.cpp:327` の `min(16, hardware_concurrency)` が前例。
    コンストラクタ引数は `parallel_hash_join.hpp:72` に合わせる。
    semi/anti/outer/null-aware・residual 付き・index join は v1 対象外で
    逐次維持）
  - `MergeJoinPlan::EmitExecutor`：両側行数合計が閾値以上なら
    `ParallelMergeJoin`（`parallel_merge_join.hpp:31`。outer/semi/anti・
    residual 付きは対象外）
  - `Dump`/`EXPLAIN` 名が並列形を区別すること
    （`sort.cpp:668` の `"ParallelSort (N workers)"` が前例）
- [x] 5c. コスト側の扱い→意図的に不変（判断記録）
  - scan/agg の前例通り emit 時選択のみとし `local_cost` は変えない。
    並列化は全代替をほぼ一様に速くするため plan 選択は変わらず、コスト変更は
    golden churn のみを生む。DOP 明示配分・Exchange 実装は分散枠に残す（従来通り）
  - `implementation_rules.cpp` の hash/merge join `local_cost` に
    閾値超過時の speedup divisor（`CalibrateOperatorCost`
    `plan/cascades.cpp:10304` の hash `L*1.2+R*1.5` / merge `*1.1` と
    同オーダーで整合。並列効率は Amdahl 的に `1/min(workers, morsels)` の
    保守形）を適用し、逐次/並列の cost 順序が flip することを
    `optimizer_test.cpp` で pin（`ParallelScanEmittedForLargeAnalyzedTable`
    `plan/optimizer_test.cpp:3147` が前例）
  - spill 連携：`EstimateMemorySpillCost`（`plan/cascades.cpp:10288`）の
    hybrid 罰則と二重割引にならないよう、spill 支配時は speedup を適用しない
- [x] 5d. 回帰テスト
  - `executor_test.cpp` 4件：大入力で `SharedBuildParallelHashJoin` /
    `ParallelMergeJoin`（各16000行の結果一致）、小入力で逐次維持
  - `ParallelHashJoinEmittedForLargeInputs` /
    `ParallelMergeJoinEmittedForLargeInputs`（大小両側の閾値テスト＋
    逐列一致）＋ semi/outer が逐次のままの否条件

残件（分散枠）：明示 DOP 配分・`Exchange` 実装（今は no-op 4規則
`plan/implementation_rules.cpp:2973`）・colocated/broadcast の cost 比較は
単一ノードでは等価のため対象外（`docs/optimizer_todo.md:920` の整理通り）

---

## 作業順序（このファイルの上から順に実装する）

1 → 2 → 3 → 4a → 4b → 4c → 5 の順。各項目は
「実装 → 対象層テスト → `ctest` 該当 suite → `check_layering.py`」の
小刻みコミット。`docs/cascades_optimizer.md` の D5 表・
`docs/optimizer_todo.md` の `[ ]→[x]` は同じ変更で更新する
（`docs/AGENTS.md` の「doc と振る舞いの同時更新」規則）。
