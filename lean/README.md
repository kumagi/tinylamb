# `TinylambOpt` — Lean 4 による `tinylamb` Optimizer 形式検証・ルール探索ラボ

`tinylamb` のスカラー式書き換え（`expression/rewrite.cpp`）および Cascades 論理書き換えルール（`plan/cascades.cpp`）の
**バグ（反例）発見**と**さらにアグレッシブな書き換えルールの導出・機械証明**を行う Lean 4 プロジェクトです。

## セットアップ & 使い方（`sudo` 不要）

ツールチェイン（`elan` + Lean `v4.36.0`）は `~/.elan/bin` にインストール済みです。
普段使うシェルの `~/.bashrc` / `~/.zshrc` に以下を足しておくと `lake` / `lean` に直接パスが通ります：

```bash
export PATH="$HOME/.elan/bin:$PATH"
```

### 1. すべての定理・証明をコンパイル検証する

```bash
cd lean
~/.elan/bin/lake build
```

- `Build completed successfully` と出れば、全ファイル内の `theorem` が **抜け穴（`sorry`）ゼロで全入力パターンに対して数学的に検証済み** です。
- わざと誤った書き換えルールを `theorem` として書くと、コンパイルが通らず止まります。

### 2. 反例・書き換え比較デモを実行する

```bash
cd lean
~/.elan/bin/lake exe demo
```

### 3. VS Code / エディタで触るときのおすすめ

VS Code 拡張 **`lean4` (`leanprover.lean4`)** を入れて [lean/](file:///home/kumagi/develop/tinylamb/lean) 内の `.lean` ファイルを開き、`by` ブロックの中の各行にカーソルを置くと、右側の **Lean InfoView** に「今どの仮定を持っていて、あと何を証明すればゴールか」がリアルタイム表示されます（これが新ルールの前提条件を逆算する最強の武器になります）。

## ファイル構成（読む順番）

1. `TinylambOpt/ThreeValued.lean` — **Step 1: SQL 三値論理とスカラー式ルール**
   - `TRUE / FALSE / NULL`（Kleene 三値論理）と NULL 許容比較・IEEE `NaN` の定義
   - `double_negation` / `de_morgan` / `absorption_and` の証明と、`x = x`・`NOT (x < y)` の罠の反証
   - `or_to_union` の排他ガードが三値論理の 9 パターンすべてで多重度 1 を保つことの証明
2. `TinylambOpt/BagRel.lean` — **Step 2: Bag（重複行あり）関係代数と Cascades ルール**
   - `Selection` / `UnionAll` / `InnerJoin` / `LeftOuterJoin` / `AntiJoin` の定義
   - `or_to_union_preserves_bag`（Bag 順列同値 `List.Perm` 保存）の完全証明
   - `outer_to_inner_join_sound`（Null-Rejecting フィルタによる外部結合→内部結合降格）と `COALESCE` 反例
3. `TinylambOpt/Discovery.lean` — **Step 3: よりアグレッシブな書き換えルールの発見（`outer_to_anti_join`）**
   - 現行 `plan/cascades.cpp` の `outer_to_anti_join` が課している 2 つの制限（① カタログ `NOT NULL` 必須、② 残余連言禁止）を分析
   - カタログ `NOT NULL` が無くても **`ON` 条件自体が検査対象列で Null-Rejecting（例: `ON l.id = r.id` で `WHERE r.id IS NULL`）なら安全に AntiJoin 化でき、さらに残余条件 `q(L, R)` の `R` を `NULL` 置換して `AntiJoin` の左入力へ事前プッシュダウンできる** ことを導出・完全証明（`outer_to_anti_join_aggressive`）
4. `TinylambOpt/AggBase.lean` — **Step 4-A: SQL 集約演算（`GROUP BY / SUM / COUNT / MIN / MAX`）と有限 Fubini 定理**
   - 空集合で `NULL` になる `SUM` と `0` になる `COUNT`、および `NULL` キーを 1 グループに束ねる `GROUP BY` の厳密モデル
   - 可換モノイド集約のバケット分割定理（`aggFold_split_by_key`）と二重走査交換（`aggFold_comm`）
5. `TinylambOpt/AggJoinDiscovery.lean` — **Step 4-B: Join 越しの多段集約（Eager / Factorized Aggregation）4 候補の発見と証明**
   - **候補1**: 対向側に Unique 制約が無い M:N 結合でも成立する 2 段片側事前集約 ＋ `NULL` キー早期刈り込み（`eager_group_agg_over_mn_join_sound`）とスカラー `COUNT` 空結合 `NULL` バグの修復（`coalesce_sqlSum_eq_countFold`）
   - **候補2**: M:N 結合の**両側同時事前集約**によるテーブル横断式 `SUM(L.x * R.y)`・`SUM(L.x + R.y)` の分解定理（`dual_eager_cross_expr_over_mn_join_sound`）と `COUNT(*)` 重み付けバグの反例
   - **候補3**: `LEFT OUTER JOIN` 右側の事前集約における `COUNT(*)` の `COALESCE(cnt_star, 1)` 定理（`eager_agg_right_of_left_join_sound`）
   - **候補4**: `Fact ⋈ Dim1 ⋈ Dim2` で Join の段ごとに不要外部キーを消去する 3 段スター・カスケード集約（`star_cascade_two_hop_agg_sound`）
6. `TinylambOpt/AvgMinMaxHavingDistinct.lean` — **Step 5: `AVG / MIN / MAX` × `LEFT JOIN` × `HAVING` × `DISTINCT` 集約の網羅検証**
   - `AVG` の `(sum, cntCol)` 分解と M:N `InnerJoin`・`LEFT JOIN`（右側・左側の両方向！）事前集約の正当性＋内包 `COALESCE` の罠
   - `MIN(DISTINCT) = MIN` / `MAX(DISTINCT) = MAX` の無条件簡約、3 キー事前重複排除、および冪等 5 種集約 `(MIN, MAX, COUNT/SUM/AVG(DISTINCT))` における **M:N `InnerJoin` → `SemiJoin` 縮退** と **Unique 制約不要の `LEFT JOIN` 完全消去**
   - `HAVING MAX(x) > c` / `HAVING MIN(x) < c` の基底 `WHERE` 貫通押し込み、および **`HAVING` の右列集約 Null-Rejecting 性による `LEFT JOIN` → `INNER JOIN` 降格**（と `COUNT(*)` 同居トラップ）
7. `TinylambOpt/CppRuleAudit.lean` — **Step 6: 既存 C++ オプティマイザ実装ルールの Lean 4 直訳監査（第 1 弾）**
   - C++ 側の実際のガード関数（`query/join_reduction.cpp` の `IsStrictOnQualifiers` / `RejectsNulls` / `NeverFalse`、`expression/rewrite.cpp` の `IsNullStrictWhere`、`plan/cascades_search.cpp` の `DeriveLogicalProperties(kOuterJoin)`、`plan/cascades.cpp` の `push_selection_through_aggregation` / `distinct_over_group_by` / `distinct_and_group_by_interchange` / `aggregate_union_transpose` / `filter_aggregate_pushdown`）をそのまま Lean 4 に直訳し、7 件の実在バグ反例と修復健全性を証明
8. `TinylambOpt/HighRiskRulesAudit.lean` — **Step 7: 高リスク領域の網羅監査とカバレッジ拡大（第 2 弾：Cascades 28 ルール ＋ スカラー式 12 ルール）**
   - 集合演算（`intersect_to_semijoin` / `except_to_antijoin` の `DOUBLE NaN` 不整合＆計算式エイリアス誤解決バグを含む 9 ルール）、外部結合結合律・完全外部結合分解・準結合リオーダ（8 ルール）、主キー/一意性による結合・重複消去（`distinct_eager_over_join` の対向側自己等号バグを含む 7 ルール）、等価クラス・冗長等号消去（`redundant_join_predicate_elimination` の自己等号 `x = x` 誤削除バグ・`in_list_to_semi_join` の `;` タグ衝突バグを含む 5 ルール）、Sort/Limit 合成（`eliminate_sort_under_unordered_consumer` の `HAVING` 消失＆入れ子 `STRING_AGG` 見逃しバグを含む 3 ルール）、および三値論理スカラー式（`XOR` / `NOT IN (..., NULL)` / `IS TRUE = FALSE` / `COALESCE` / `NULLIF` 等 12 ルール）を機械証明
9. `TinylambOpt/WindowTopNDecorrelate.lean` — **Step 8: Window / Sort / Limit / TopN / 相関除去 / Unnest / 再帰CTE / SARGable 境界の監査（第 3 弾前半）**
   - Window 関数代数（`push_selection_through_window` の混在 `PARTITION BY` バグ、`split_window` の非連続列順スワップバグ＆`Memo::groups_` 再確保ダングリング参照バグ、`no_op_window_elimination` の `SELECT *` / 修飾列見落としバグ、`limit_prefix_{unordered,ordered}_window`）、安定ソート・TopN 押し下げ（`sort_merge_of_compatible_orders` 双方向、`union_all_push_limit`、`topn_push_through_union_all`、`push_limit_through_left_join`、`topn_push_through_proven_one_to_one_join`）、`TryDecorrelate` / `DecorrelateOneLateralSource`（`SELECT *` 幽霊列混入バグ） / `MergeCompatibleUnionBranches`、`unnest_filter_pushdown`（既定列 `unnest`・空配列例外バグ）、`recursive_termination_predicate_pushdown`、`cast_pushdown_on_comparison`、`extract_year_sargable` 全 9 方向、`dynamic_filter_pushdown_join`（派生サブプラン消去バグ）、および `ParameterSlot` 定数凍結ガードを完全証明
10. `TinylambOpt/ScalarOverflowAndCoreAlgebra.lean` — **Step 9: コア関係代数・`INT64` オーバーフロー境界・スカラ式 3 値エラー保存の網羅証明（第 3 弾後半：Cascades 91/91 = 100% 達成！）**
   - コア関係代数（Join 交換・結合・列挙、Selection 分割・押し下げ、Projection 合成・透過、`push_projection_through_aggregation` の `HAVING` 列刈り落としバグ、`InferJoinConstants` / `InferJoinInequalities` / `PropagateEqualityConstants` の IEEE-754 $2^{53}$ `INT64`/`DOUBLE` 非推移性バグ）
   - 符号付き 64-bit 整数 (`INT64`) の厳密オーバーフロー検査付き再結合 4 ルール・符号正規化・二重否定・ビット反転・`abs(abs(x))` の全 $2^{64}$ 領域＋例外一致証明
   - 短絡エラー非対称性（`boolean_identity`）、`NaN` 対応 `FlipComparison`、`CASE`/`IF` 分配、`IN`/`LIKE`、`inner_join_not_null_inference` の値文脈反例とフィルタ文脈健全性を証明（`expression/rewrite.cpp` 68/71 = 95.8% カバー）
11. `TinylambOpt/TpcdsDiscovery.lean` — **Step 10: TPC-DS クエリ群に向けた新規アグレッシブ最適化ルール 6 系統の探索と証明**
    - 実測 SF=1 ベンチマーク（Q76, Q65, Q4/Q11/Q74, Q5/Q75, Q72/Q78, Q38/Q87/Q97）のボトルネックに対する 6 系統の書き換え定理と 7 つの三値論理・NULL・空集合トラップ反例を `sorry` ゼロで完全証明
12. `TinylambOpt/ClickBenchDiscovery.lean` — **Step 11: ClickBench（全 43 クエリ・1 億行単一ワイド表）向け新規最適化ルール 6 系統の探索と証明**
    - 実測 1 億行レポート（Q18, Q35, Q36, Q10, Q22, Q23, Q29, Q40, Q43）のボトルネックに直結する 6 系統の最適化定理（`GROUP BY` 関数従属・無オーバーフロー算術キー縮約、順序なし `GROUP BY ... LIMIT K` キー・ファイバー厳密集約、Top-N 勝者決定後の遅延ペイロード集約実体化、辞書コード準同型・順序保存辞書整数 `MIN`、条件付き `CASE WHEN` 排他パーティション分割、単調 `TIMESTAMP_TRUNC` 昇順ストリーム早期打ち切り）と 6 つの SQL 意味論トラップ反例を `sorry` ゼロで完全証明
13. `TinylambOpt/BranchlessJitCodegen.lean` — **Step 12: 物理プラン後段（Bytecode → Branchless LLVM JIT / SIMD）の形式検証**
    - 2レーン `(BitVec 64, Bool)` および 64行ビットスライス Kleene 3値論理、`bv_decide` による分岐なし `ABS`・無トラップ投機除数サニタイズ (`safeDivisorBV`)・NULL ゴミペイロード耐性、前方分岐スタック Bytecode DAG 全体の単一ブロック Predicated SSA If-Conversion 完全同値定理 (`if_conversion_sound_and_complete`)、および ZoneMap 4端点区間伝播・プレフィックス包絡線ガードによるオーバーフロー検査除去 (`zonemap_interval_mul_4corners_sound`, `zonemap_prefix_sum_masked_sound`) を `sorry` ゼロで完全証明
14. `TinylambOpt/TpcdsExecCodegen.lean` — **Step 13: TPC-DS 向け証明付き実行カーネル・物理ローワリング規則**
    - 定数集合 `IN (...)` の 3 値 O(1) 判定、ASCII 1-始まり `SUBSTR` ゼロコピースライス、二値述語 `NOT` 対合正規化、事前構築 `EXISTS` 外側非 NULL ドメイン制限、融合 `AntiJoin` 集約カーネル、共有 `UNION` CTE への選言グループキー制限貫通を `sorry` ゼロで完全証明
15. `TinylambOpt/TpchSf10Discovery.lean` — **Step 14: TPC-H SF=10（約 8,658 万行）対 DuckDB 敗北クエリ群向け新規最適化ルール 6 系統の探索と証明**
    - 過去ベンチマーク記録（`tpch-scaling-20261117`, `gate47`）および SF=10 全 22 問 `EXPLAIN` プロファイルで特定した敗北・スピル超過クエリ（Q19, Q13, Q18, Q9, Q15, Q10, Q8, Q20, Q11, Q16）のボトルネックを解消する 6 系統の書き換え定理（Kleene 3値共通連言括り出し・Join 横断 `OR` 選言包絡線＆区間 Hull プッシュダウン、`LEFT JOIN` 右側先行集約＋左 `UNIQUE NOT NULL` による内側 `GROUP BY` 完全消去、自己準結合 `HAVING SUM` 1 パス吸収、共有 CTE 最大値 `IS NOT NULL` 付き `WITH TIES` 還元＆`HAVING` 全体和 Fubini 1 パス還元、狭キー準結合付き Top-N 遅延ディメンション属性結合＆推移的等価クラス閉包、相関 `SUM` 不等号の `INNER JOIN` 相関除去＆非空非 NULL `NOT IN` の `AntiJoin` 還元）と 8 つの SQL 三値論理・NULL・空集合・多重度トラップ反例を `sorry` ゼロで完全証明
16. `Main.lean` — 具体的な行データで反例と新ルール・C++ バグ監査・TPC-DS / ClickBench / TPC-H SF=10 探索デモを確認する実行エントリポイント

## C++ コード側の対応コメント規約

Lean で検証済みの各 C++ ルール・前処理パス・プロパティ導出には、直上に以下の統一コメントを付与しています：

```cpp
// Lean proof: lean/TinylambOpt/<File>.lean (<theorem_names>)
```

全対応箇所はリポジトリルートから 1 コマンドで一覧できます：

```bash
rg -n "Lean proof:" plan/ query/ expression/ executor/
```
