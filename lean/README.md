# `TinylambOpt` — Lean 4 による `tinylamb` Optimizer 形式検証・ルール探索ラボ

`tinylamb` のスカラー式書き換え（`expression/rewrite.cpp`）および Cascades 論理書き換えルール（`plan/cascades.cpp`）の
**バグ（反例）発見**と**さらにアグレッシブな書き換えルールの導出・機械証明**を行う Lean 4 プロジェクトです。

最適化候補ごとの命題・適用条件・境界例は [証明課題の一覧](issues/README.md) にまとめています。

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

- 既定ターゲットの `CheckProofs` は、インポートされたプロジェクトの定理が直接・間接に `sorryAx` に依存すると失敗します。各定理の保証範囲はその型と仮定で決まります。
- 既定ターゲットの `CheckStorageExecutionProofs` は、圧縮・SQL 実行の固定 1,000 件の `Goal` すべてが証明または反証定理で覆われ、余分な公理に依存しないことを検査します。個別結果は [storage-execution-results](issues/research/storage-execution-results/README.md) にあります。
- わざと誤った書き換えルールを `theorem` として書くと、コンパイルが通らず止まります。

### 2. 反例・書き換え比較デモを実行する

```bash
cd lean
~/.elan/bin/lake exe demo
```

### 3. VS Code / エディタで触るときのおすすめ

VS Code 拡張 **`lean4` (`leanprover.lean4`)** を入れて [`./TinylambOpt/`](./TinylambOpt/) 内の `.lean` ファイルを開き、`by` ブロックの中の各行にカーソルを置くと、右側の **Lean InfoView** に「今どの仮定を持っていて、あと何を証明すればゴールか」がリアルタイム表示されます（これが新ルールの前提条件を逆算する最強の武器になります）。

## 既存の補題を探す

新しい証明を書くときは、まず同じモデルを使う証明と、そのモデルの共通補題を探します。例えば `rg -n 'theorem nsum_|def nsum\b' TinylambOpt` で、NULL を含む整数の `SUM` の定義と補題を見つけられます。

| 証明したいこと | 再利用する補題 |
| --- | --- |
| 入力を分割して集計し、部分和を結合する | `nsum_append`、`nsum_map_nsum` |
| 選択したキーごとに先に集計する | `nsum_group_by_key` |
| 行の並べ替えで和が変わらないことを示す | `nsum_perm` |
| 結合の多重度で和をスケールする | `nsum_replicate`、`nsum_flatMap_replicate`、`nsum_flatMap_const` |
| 先頭行を加えた和を `getD 0` で比較する | `nsum_cons_getD` |

これらは [`NullableSum.lean`](TinylambOpt/ResearchProofs/QueryRewrites/NullableSum.lean) にあります。[既存の `nsum` モデル](TinylambOpt/ResearchGoals/QueryRewrites/Common.lean#L11) は正確な整数和を扱い、空入力と全行 NULL では `none` を返します。利用例は [結合の多重度を使う証明](TinylambOpt/ResearchProofs/QueryRewrites/AnalyticalJoins/FactorizedNullableJoinSums.lean) と [部分和を統合する証明](TinylambOpt/ResearchProofs/QueryRewrites/SalesAndReturns/NullableTotalFromChannelSubtotals.lean) にあります。

キーで分ける前後で値とその多重度が保たれることは、[KeyGrouping.lean の `forKey_filter_perm`](TinylambOpt/ResearchProofs/QueryRewrites/KeyGrouping.lean) で示せます。[国別の年齢集計](TinylambOpt/ResearchProofs/QueryRewrites/AnalyticalJoins/PlaceGroupedCountryAgeTotals.lean) では、同じ補題から行数と非 NULL 件数の一致も導いています。

```lean
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

open TinylambOpt.ResearchGoals.QueryRewrites (nsum)
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_map_nsum)

example (groups : List (List (Option Int))) :
    nsum (groups.map nsum) = nsum groups.flatten :=
  nsum_map_nsum groups
```

## ファイル構成（読む順番）

1. `TinylambOpt/ThreeValued.lean` — **Step 1: SQL 三値論理とスカラー式ルール**
   - `TRUE / FALSE / NULL`（Kleene 三値論理）と NULL 許容比較・IEEE `NaN` の定義
   - `double_negation` / `de_morgan` / `absorption_and` の証明と、`x = x`・`NOT (x < y)` の罠の反証
   - `or_to_union` の排他ガードが三値論理の 9 パターンすべてで多重度 1 を保つことの証明
2. `TinylambOpt/BagRel.lean` — **Step 2: Bag（重複行あり）関係代数と Cascades ルール**
   - `Selection` / `UnionAll` / `InnerJoin` / `LeftOuterJoin` / `AntiJoin` の定義
   - `or_to_union_preserves_bag`（Bag 順列同値 `List.Perm` 保存）の完全証明
   - `outer_to_inner_join_sound`（Null-Rejecting フィルタによる外部結合→内部結合降格）と `COALESCE` 反例
3. `TinylambOpt/OuterJoinToAntiJoin.lean` — **Step 3: よりアグレッシブな書き換えルールの発見（`outer_to_anti_join`）**
   - 現行 `plan/cascades.cpp` の `outer_to_anti_join` が課している 2 つの制限（① カタログ `NOT NULL` 必須、② 残余連言禁止）を分析
   - カタログ `NOT NULL` が無くても **`ON` 条件自体が検査対象列で Null-Rejecting（例: `ON l.id = r.id` で `WHERE r.id IS NULL`）なら安全に AntiJoin 化でき、さらに残余条件 `q(L, R)` の `R` を `NULL` 置換して `AntiJoin` の左入力へ事前プッシュダウンできる** ことを導出・完全証明（`outer_to_anti_join_aggressive`）
4. `TinylambOpt/AggBase.lean` — **Step 4-A: SQL 集約演算（`GROUP BY / SUM / COUNT / MIN / MAX`）と有限 Fubini 定理**
   - 空集合で `NULL` になる `SUM` と `0` になる `COUNT`、および `NULL` キーを 1 グループに束ねる `GROUP BY` の厳密モデル
   - 可換モノイド集約のバケット分割定理（`aggFold_split_by_key`）と二重走査交換（`aggFold_comm`）
5. `TinylambOpt/JoinPreaggregation.lean` — **Step 4-B: Join 越しの多段集約（Eager / Factorized Aggregation）4 候補の発見と証明**
   - **候補1**: 対向側に Unique 制約が無い M:N 結合でも成立する 2 段片側事前集約 ＋ `NULL` キー早期刈り込み（`eager_group_agg_over_mn_join_sound`）とスカラー `COUNT` 空結合 `NULL` バグの修復（`coalesce_sqlSum_eq_countFold`）
   - この事前集約定理は全域的な可換モノイドを仮定します。C++ の `eager_aggregation_over_join` は途中で失敗し得る `SUM` を対象外とします。未結合キーの具体例は `TinylambOpt/BranchlessJitCodegen.lean` の `eager_sum_unmatched_key_overflow` です。
   - **候補2**: M:N 結合の**両側同時事前集約**によるテーブル横断式 `SUM(L.x * R.y)`・`SUM(L.x + R.y)` の分解定理（`dual_eager_cross_expr_over_mn_join_sound`）と `COUNT(*)` 重み付けバグの反例
   - **候補3**: `LEFT OUTER JOIN` 右側の事前集約における `COUNT(*)` の `COALESCE(cnt_star, 1)` 定理（`eager_agg_right_of_left_join_sound`）
   - **候補4**: `Fact ⋈ Dim1 ⋈ Dim2` で Join の段ごとに不要外部キーを消去する 3 段スター・カスケード集約（`star_cascade_two_hop_agg_sound`）
6. `TinylambOpt/AvgMinMaxHavingDistinct.lean` — **Step 5: `AVG / MIN / MAX` × `LEFT JOIN` × `HAVING` × `DISTINCT` 集約の網羅検証**
   - `AVG` の `(sum, cntCol)` 分解と M:N `InnerJoin`・`LEFT JOIN`（右側・左側の両方向！）事前集約の正当性＋内包 `COALESCE` の罠
   - `MIN(DISTINCT) = MIN` / `MAX(DISTINCT) = MAX` の無条件簡約、3 キー事前重複排除、および冪等 5 種集約 `(MIN, MAX, COUNT/SUM/AVG(DISTINCT))` における **M:N `InnerJoin` → `SemiJoin` 縮退** と **Unique 制約不要の `LEFT JOIN` 完全消去**
   - `HAVING MAX(x) > c` / `HAVING MIN(x) < c` の基底 `WHERE` 貫通押し込み、および **`HAVING` の右列集約 Null-Rejecting 性による `LEFT JOIN` → `INNER JOIN` 降格**（と `COUNT(*)` 同居トラップ）
7. `TinylambOpt/NullRejectionAndGroupRewrites.lean` — **Step 6: 既存 C++ オプティマイザ実装ルールの Lean 4 直訳監査（第 1 弾）**
   - C++ 側の実際のガード関数（`query/join_reduction.cpp` の `IsStrictOnQualifiers` / `RejectsNulls` / `NeverFalse`、`expression/rewrite.cpp` の `IsNullStrictWhere`、`plan/cascades_search.cpp` の `DeriveLogicalProperties(kOuterJoin)`、`plan/cascades.cpp` の `push_selection_through_aggregation` / `distinct_over_group_by` / `distinct_and_group_by_interchange` / `aggregate_union_transpose` / `filter_aggregate_pushdown`）をそのまま Lean 4 に直訳し、8 件の実在バグ反例と修復健全性を証明
8. `TinylambOpt/SetJoinAndScalarRewrites.lean` — **Step 7: 高リスク領域の網羅監査とカバレッジ拡大（第 2 弾：Cascades 32 ルール ＋ スカラー式 12 ルール）**
   - 集合演算（`intersect_to_semijoin` / `except_to_antijoin` の `DOUBLE NaN` 不整合＆計算式エイリアス誤解決バグを含む 9 ルール）、外部結合結合律・完全外部結合分解・準結合リオーダ（8 ルール）、主キー/一意性による結合・重複消去（`distinct_eager_over_join` の対向側自己等号バグを含む 7 ルール）、等価クラス・冗長等号消去（`redundant_join_predicate_elimination` の自己等号 `x = x` 誤削除バグ・`in_list_to_semi_join` の `;` タグ衝突バグを含む 5 ルール）、Sort/Limit 合成（`eliminate_sort_under_unordered_consumer` の `HAVING` 消失＆入れ子 `STRING_AGG` 見逃しバグを含む 3 ルール）、および三値論理スカラー式（`XOR` / `NOT IN (..., NULL)` / `IS TRUE = FALSE` / `COALESCE` / `NULLIF` 等 12 ルール）を機械証明
9. `TinylambOpt/WindowTopNDecorrelate.lean` — **Step 8: Window / Sort / Limit / TopN / 相関除去 / Unnest / 再帰CTE / SARGable 境界の監査（第 3 弾前半）**
   - Window 関数代数（`push_selection_through_window` の混在 `PARTITION BY` バグ、`split_window` の非連続列順スワップバグ＆`Memo::groups_` 再確保ダングリング参照バグ、`no_op_window_elimination` の `SELECT *` / 修飾列見落としバグ、`limit_prefix_{unordered,ordered}_window`）、安定ソート・TopN 押し下げ（`sort_merge_of_compatible_orders` 双方向、`union_all_push_limit`、`topn_push_through_union_all`、`push_limit_through_left_join`、`topn_push_through_proven_one_to_one_join`）、`TryDecorrelate` / `DecorrelateOneLateralSource`（`SELECT *` 幽霊列混入バグ） / `MergeCompatibleUnionBranches`、`unnest_filter_pushdown`（既定列 `unnest`・空配列例外バグ）、`recursive_termination_predicate_pushdown`、`cast_pushdown_on_comparison`、`extract_year_sargable` 全 9 方向、`dynamic_filter_pushdown_join`（派生サブプラン消去バグ）、および `ParameterSlot` 定数凍結ガードを完全証明
10. `TinylambOpt/ScalarOverflowAndCoreAlgebra.lean` — **Step 9: コア関係代数・`INT64` オーバーフロー境界・スカラ式 3 値エラー保存の網羅証明（第 3 弾後半。登録ルールの現行一覧は `docs/optimizer_rule_audit.md` を参照）**
   - コア関係代数（Join 交換・結合・列挙、Selection 分割・押し下げ、Projection 合成・透過、`push_projection_through_aggregation` の `HAVING` 列刈り落としバグ、`InferJoinConstants` / `InferJoinInequalities` / `PropagateEqualityConstants` の IEEE-754 $2^{53}$ `INT64`/`DOUBLE` 非推移性バグ）
   - 符号付き 64-bit 整数 (`INT64`) の厳密オーバーフロー検査付き再結合 4 ルール・符号正規化・二重否定・ビット反転・`abs(abs(x))` の全 $2^{64}$ 領域＋例外一致証明
   - 短絡エラー非対称性（`boolean_identity`）、`NaN` 対応 `FlipComparison`、`CASE`/`IF` 分配、`IN`/`LIKE`、`inner_join_not_null_inference` の値文脈反例とフィルタ文脈健全性を証明。`FloatIdentities.lean` は、表現可能な値の丸めがその値を保つという仮定の下で、DOUBLE の 1 除算と、数値を FLOAT64 に変換した後の 1 乗が符号付きゼロ・無限大・NaN・NULL・先行エラーを保存することを証明します。C++ の型変換と libm は証明の対象外です。
11. `TinylambOpt/FloatIdentities.lean` — FLOAT64 への数値昇格・除算・1 乗の特殊値／エラー保存
   - 表現可能な値への丸めがその値を保つという仮定の下で、DOUBLE の 1 除算と FLOAT64 変換後の 1 乗が符号付きゼロ・無限大・NaN・NULL・先行エラーを保存することを証明（Step 9 の `ScalarOverflowAndCoreAlgebra.lean` から参照）
12. `TinylambOpt/UnionAndJoinRewrites.lean` — **Step 10: TPC-DS クエリ群に向けた新規アグレッシブ最適化ルール 6 系統の探索と証明**
    - 実測 SF=1 ベンチマーク（Q76, Q65, Q4/Q11/Q74, Q5/Q75, Q72/Q78, Q38/Q87/Q97）のボトルネックに対する 6 系統の書き換え定理と 7 つの三値論理・NULL・空集合トラップ反例を `sorry` ゼロで完全証明
13. `TinylambOpt/GroupKeyAndDictionaryRewrites.lean` — **Step 11: ClickBench（全 43 クエリ・1 億行単一ワイド表）向け新規最適化ルール 6 系統の探索と証明**
    - 実測 1 億行レポート（Q18, Q35, Q36, Q10, Q22, Q23, Q29, Q40, Q43）のボトルネックに直結する 6 系統の最適化定理（`GROUP BY` 関数従属・無オーバーフロー算術キー縮約、順序なし `GROUP BY ... LIMIT K` キー・ファイバー厳密集約、Top-N 勝者決定後の遅延ペイロード集約実体化、辞書コード準同型・順序保存辞書整数 `MIN`、条件付き `CASE WHEN` 排他パーティション分割、単調 `TIMESTAMP_TRUNC` 昇順ストリーム早期打ち切り）と 6 つの SQL 意味論トラップ反例を `sorry` ゼロで完全証明
14. `TinylambOpt/BranchlessJitCodegen.lean` — **Step 12: 物理プラン後段（Bytecode → Branchless LLVM JIT / SIMD）の形式検証**
    - 2レーン `(BitVec 64, Bool)` および 64行ビットスライス Kleene 3値論理、`bv_decide` による分岐なし `ABS`・無トラップ投機除数サニタイズ (`safeDivisorBV`)・NULL ゴミペイロード耐性、前方分岐スタック Bytecode DAG 全体の単一ブロック Predicated SSA If-Conversion 完全同値定理 (`if_conversion_sound_and_complete`)、および ZoneMap 4端点区間伝播・プレフィックス包絡線ガードによるオーバーフロー検査除去 (`zonemap_interval_mul_4corners_sound`, `zonemap_prefix_sum_masked_sound`) を `sorry` ゼロで完全証明
15. `TinylambOpt/PredicateAndRelationalLowering.lean` — **Step 13: TPC-DS 向け証明付き実行カーネル・物理ローワリング規則**
    - 定数集合 `IN (...)` の 3 値 O(1) 判定、ASCII 1-始まり `SUBSTR` ゼロコピースライス、二値述語 `NOT` 対合正規化、事前構築 `EXISTS` 外側非 NULL ドメイン制限、融合 `AntiJoin` 集約カーネル、共有 `UNION` CTE への選言グループキー制限貫通を `sorry` ゼロで完全証明
16. `TinylambOpt/JoinAndAggregatePushdown.lean` — **Step 14: TPC-H SF=10（約 8,658 万行）対 DuckDB 敗北クエリ群向け新規最適化ルール 6 系統の探索と証明**
    - 過去ベンチマーク記録（`tpch-scaling-20261117`, `gate47`）および SF=10 全 22 問 `EXPLAIN` プロファイルで特定した敗北・スピル超過クエリ（Q19, Q13, Q18, Q9, Q15, Q10, Q8, Q20, Q11, Q16）のボトルネックを解消する 6 系統の書き換え定理（Kleene 3値共通連言括り出し・Join 横断 `OR` 選言包絡線＆区間 Hull プッシュダウン、`LEFT JOIN` 右側先行集約＋左 `UNIQUE NOT NULL` による内側 `GROUP BY` 完全消去、自己準結合 `HAVING SUM` 1 パス吸収、共有 CTE 最大値 `IS NOT NULL` 付き `WITH TIES` 還元＆`HAVING` 全体和 Fubini 1 パス還元、狭キー準結合付き Top-N 遅延ディメンション属性結合＆推移的等価クラス閉包、相関 `SUM` 不等号の `INNER JOIN` 相関除去＆非空非 NULL `NOT IN` の `AntiJoin` 還元）と 9 つの SQL 三値論理・NULL・空集合・多重度トラップ反例を `sorry` ゼロで完全証明
17. `TinylambOpt/CountBoundsAndBlockAggregates.lean` — **Step 15: ClickBench 第2弾（CB-7..CB-12: 1億行 `.tlcs` ＆ 1,000万行 `LSTableStore` 残存敗北クエリ群）向け証明付き最適化**
    - 高基数 `GROUP BY ... ORDER BY COUNT(*) DESC LIMIT K` に対する商ハッシュバケット上界 Exact Heavy-Hitter 刈り込み（Q34, Q35, Q17, Q19, Q13, Q15, Q16）、Markov 鳩の巣原理による `HAVING COUNT >= T` 基数上界と `LIMIT` 不要・`ORDER BY` 遅延対応 2 段集約（Q28, Q29, Q33）、アフィン線形和 `SUM(col + c)` の準同型分解（Q30）、無オーバーフロー証明付き並列 `SUM(INT64)` ＆定数ブロック `O(1)` 畳み込み（Q03, Q30）、辞書厳密頻度 `O(|D|)` 畳み込み（Q02, Q03, Q07, Q08）、ボトム元 `""` 除外による Zone-Map 非空最小値引き締め（Q26）と 8 本の SQL 意味論トラップ反例を `sorry` ゼロで完全証明
18. `TinylambOpt/PhysicalJitFusion.lean` — TLCS の連結文字列バッファから `LIKE '%literal%'` の行選択を生成
    - 32 レーンの先頭・末尾バイト比較で完全一致を取りこぼさないこと、ベクトルロードがバッファ内に収まること、最初の一致と行末オフセットで行の判定が確定することを証明。NULL の除外と既存の候補行との積集合をモデル化。
19. `TinylambOpt/QuotientFusion.lean` — 降順 `COUNT(*)` Top-N の商バケット走査と厳密集約の融合
    - バケット上界と候補グループの厳密度数を1回の走査で求める同値性、同率を保持する順位判定、無分岐の32ビットカウンタ更新、TLCS の個別ストリーム読み出しの同値性を証明。
20. `TinylambOpt/FusedAggregateJit.lean` — LLVM の Filter → Project → SUM ループ融合
    - フィルタ・射影を実体化した場合の度数と合計の一致、区間端点による乗算・加算の安全性、バッチをまたぐ先頭部分和、SIMD 部分和の加算再結合を証明。
21. `TinylambOpt/FilterHashProbe.lean` — Filter → HashProbe の直接実行と選択行をまとめる経路
    - 行順、重複キーの展開、射影の一致、およびハッシュと SIMD マスクのビット演算を証明。
22. `TinylambOpt/TlcsBoundaryAggregate.lean` — TLCS の厳密統計による集約、上位候補のブロック除外、重複候補の絞り込み
    - 統計で返す最小値・最大値・件数、ブロック範囲とフィンガープリントによる除外、重複候補を保持するビット演算、保存した選択行を再利用した場合の同値性を証明。
23. `TinylambOpt/DelimitedCaptureJit.lean` — 貪欲なオプション接頭辞の展開、最初の区切りによるキャプチャ、範囲のビットパックの同値性。
24. `TinylambOpt/SimdGroupHash.lean` — ハッシュのレーン単位同値性、UTC 時・分の読み出し、文字列フィンガープリントの境界、重複候補リプレイの位置・タグの保存。
25. `TinylambOpt/RollupAndJoinFilterRewrites.lean` — **Step 16: DuckDB の TPC-DS プラン比較から導いた次世代アグレッシブルルール A〜E の形式的正当化**
    - A: ROLLUP 階層の状態再利用（`rollup_two_level_state_combine_sound` / `rollup_three_level_state_combine_sound`、Q67 の集約入力 4.87M → ~1M を狙う。DOUBLE の加算順序と `COUNT` の空集合トラップは反例として固定）
    - B: outer / USING 配下の内側 inner プレフィックス並べ替え（`using_join_inner_prefix_reorder_and_filter_sound` / `outer_and_using_join_prefix_reorder_sound`、Q72）
    - C: 等価キー経由の動的フィルタ移送（`filter_transfer_preserves_matches` / `filter_transfer_two_hop_preserves_matches` / `transfer_filter_through_dimension_sound`、Q72/Q78/Q23。上位集合性だけを要求し、NULL/NaN 安全）
    - D: 共有 CTE の述語和押し込みと実体化判断（`shared_cte_disjunct_pushdown_sound` / `cte_materialize_halves_body_evaluations`、非決定的本体の inline は `cte_volatile_inline_unsound` で反例固定）
    - E: 外部結合前のチャネル別事前集約（`eager_channel_preagg_before_left_join_sound`、次元キー一意が必須なことを `eager_channel_preagg_requires_unique_dimension` で固定）
26. `Main.lean` — 具体的な行データで反例と新ルール・C++ バグ監査・TPC-DS / ClickBench / TPC-H SF=10 探索デモを確認する実行エントリポイント

## C++ コード側の対応コメント規約

命題・証明・モジュールは、扱う演算や保証する性質で命名します。測定日、対戦相手、ベンチマークのクエリ番号は調査記録の対応表で管理します。

Lean で検証済みの各 C++ ルール・前処理パス・プロパティ導出には、直上に以下の統一コメントを付与しています：

```cpp
// Lean proof: lean/TinylambOpt/<File>.lean (<theorem_names>)
```

Lean ファイル末尾の `C++ references` には、そのモデルや証明を参照する C++ ファイルを記載します。改名や移動時は両側のコメントを更新します。

全対応箇所はリポジトリルートから 1 コマンドで一覧できます：

```bash
rg -n "Lean proof:" plan/ query/ expression/ executor/
```
