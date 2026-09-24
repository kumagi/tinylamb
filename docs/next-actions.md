# 次に対処すべき指摘事項(設計バックログ)

2026-09-22 加筆(2026-09-23 検証更新)。TPC-DS サブセット
(`tinylamb_tpcds_benchmark`)の整備とテスト保守で判明した未解決事項。
**2026-09-23 時点の ctest は 3152/3152 全パス(1件意図的スキップを除く)。**

## 2026-09-24 完成度向上セッションの成果と残項目

**達成:**

- **TPC-DS サブセット 16 → 27 クエリ**。新規移植: q2 (CTE内UNION ALL +
  CTE複数参照 + CASE-SUM集約)、q9 (CASE内スカラサブクエリ)、q10 (相関
  EXISTS×3 + OR)、q11 (UNION ALL CTE の4重自己結合)、q13/q15 (OR-of-ANDs
  6表結合 / 選言sarg)、q16 (EXISTS + NOT EXISTS の同一order番号比較)、
  q19/q40/q46/q47。fixture に `catalog_returns` / `web_returns` /
  `call_center` を追加し、`catalog_sales` に出荷列、`date_dim` に
  `d_week_seq`/`d_day_name`、`customer` に `c_login`/`c_email_address`、
  `item` に `i_manufact`、`store` に `s_zip` を追加。追加列の乱数は
  独立 aux RNG に分離し、既存のピン止めデータを一切変更しない。
  全27クエリがSF1で最終測定2.93秒（min-of-3、最遅q11 1.24秒、27/27成功）。
- **`GROUPING_ID(...)` を実装** (grouping sets 展開の AST 畳み込みで
  枝ごとの定数へ)。GoogleSQL ビットベクタ順 (左端引数 = 最上位ビット)。
  複数引数 `GROUPING(...)` も同じ順に統一 (従来は左端 = LSB で、
  `GroupingMultiArgBuildsBitmask` の期待値を更新)。
- **`ProductPlan::InstallParameterSlots` の SEGFAULT 修正**: index join 形
  (right_src_ null, right_tbl_ 保持) での null 参照。
  3 テスト (CascadesRulesCoverageTest ×2, compliance optimizer_join) 復帰。
- **`SlotizeLiterals` の集約属性保存**: type/child/distinct だけで再構築し
  `AGG(x WHERE p)` の inline WHERE・inner ORDER BY/LIMIT・STRING_AGG
  delimiter が落ちていた (`TemplateRebindKeepsAggregateAttributes` 復帰)。
  `ContainsParameterSlot` / `AppendParameterSlots` も全属性を辿るよう修正。
- **ordered join 経路の述語配置** (`planning_heuristics.cpp`
  `has_ordered_join`): 外部結合を含む FROM では WHERE が一切配置されず、
  カンマ結合が cross 積のまま最上位 Filter に流れていた
  (TPC-DS q40 が計画爆発で 15 分超 → 109 ms)。単一ソース述語はスキャンへ、
  カンマ結合の最後の参照表到達時に結合条件へ配置。volatile /
  サブクエリ述語は配置せず最上位評価を維持 (二重評価を防ぐ)。
- **ORDER BY 序数の部分修正**: `SELECT *` と序数の組合せで visitor が
  「ordinal out of range」を出していた点を、star 展開後に解決する形で
  planner 経由は修正 (`ApplyOrderBy` も定数キーを出力列へ解決)。
  回帰: `PinnedDumpTable` に star+序数の pin を追加。

**残項目(次回以降):**

- **ORDER BY 序数 × `SELECT *` × relational 経路** (CTE/派生表ソースなど
  `emit_relational` へ流れた文): 定数キーが無秩序ソートとして無音に機能し
  ない (q47 は `s_store_name` を明示する形に適応して回避)。
  `emit_relational` 経路での定数キー検出と解決 or エラー化が残課題。
- 監査 A 項の残りは docs/optimizer_todo.md 監査節に 2026-09-24 時点で
  解決済みとして反映 (mark_join_to_filter / eager_aggregation_over_join /
  xor_boolean_identity / distribute_or_over_and_budgeted)。

## 2026-09-23 再検証で撤回した誤診

- **「`NOT EXISTS (相関サブクエリ)` が誤行/空を返す」は誤りだった。**
  CLI は SELECT の複数行を 1 行ずつ `[v]` で出力するため、前文の 1 行結果を
  次文の結果と取り違えると「誤行」に見える。EXISTS/NOT EXISTS/IN の 15 形状
  (内側・外側・両側残差、OR、不等号相関)を 1 文 1 セッション + ユニークな
  射影マーカーで機械検証した結果、すべて正解だった。サブクエリの「誤結果」
  を疑うときは必ず 1 クエリ = 1 セッション + マーカーで再検証すること。

## 2026-09-22 カバレッジ拡充セッションで判明した既知バグ(未修正)

1. **行値式 `(a, b) IN (SELECT ...)` が常に空**(HEAD でも同様、長期課題)。
   - 2026-09-23 追試で再確認。原因は 3 層に跨る:
     (a) フロントエンドが `(a, b)` を `StructConstructorWithParens` に低減し、
     `query/googlesql_ast_visitor.cpp` の構造体コンストラクタ経路は各フィールドを
     低減時に `TryEvaluate(Row(), Schema())` で先行評価するため、列参照を含む
     フィールドが deferred になる(deferred 経路の実行時スコープ解決が不完全)。
     (b) サブクウエリ側の `ProjectSubqueryRow`(executor/detail/
     subquery_runtime.cpp)は非 AS STRUCT の場合 1 列目しか投影しないため、
     複数列行値の構造比較がそもそも表現できない。
     (c) `expression_eval.cpp` の IN 経路のハッシュメンバーシップは
     単一ドメインの Value 前提。
   - 対応には frontend 低減・名前解決・両評価器(AST/bytecode)・
     サブクエリ投影の整合が必要で、機能追加規模の作業。
3. **`WITH RECURSIVE name(col...)` の列リスト非対応**、**`UNION` の
   DISTINCT/ALL 省略非対応**(いずれもパーサ文法ギャップ、GoogleSQL では
   標準形)。2026-09-23 追記: SQL フロントエンドは SHA256 ピン留め済みの
   外部 `execute_query` バイナリ(CMakeLists `_googlesql_executable`)であり、
   両ギャップはこの外部パーサの文法が起源。リポジトリ側での SQL テキスト
   前処理による回避は Roadmap の「ad-hoc workaround 禁止」原則に抵触するため
   行わない。対応には parser アセットの更新(SHA 再ピン留め)が前提。
4. **INSERT は PRIMARY KEY の一意性を強制しない**(`sql_dml_paths_coverage_
   test` で現行契約として pin 済み)。意図仕様なら docs 側での明記を推奨。
5. **高負荷(-j 並列 ctest)時のタイミング系フレーク**:
   `TransactionCoverageTest.ReadOnlyPreCommitFailsWhenDependencyBarrierBreaks`
   と `CliMainTest.ScriptExecutesAndPrintsRowsAfterCommit` が並列実行時に
   稀に落ちる(単独実行では安定)。外部パーサ子プロセスまわりの
   レース/リソース競合の疑い。
   2026-09-23 追記: build-asan でも高負荷(-j8 ctest × tidy -j16 並走)時に
   `SqlFunctionEdgeTest.ErrorTable` / `SqlExpressionMatrixTest.ErrorTable`
   (should-throw のクエリが throw しない方向で稀に失敗)を確認。
   単独シリアル実行では 10/10 安定。`DATE '...' + INTERVAL 24 HOUR` が
   負荷下で稀に成功へ転じる経路があり、式評価/プラン選択の
   非決定性の疑い。要 root-cause。

## 2026-09-22 カバレッジ拡充セッションで修正したもの

- `Project()`(`executor/relational.cpp`)の emit 連鎖から `} else {` が
  欠落し、非グループ化射影(FROM なし SELECT、`RelationalExecutor` 経由の
  クエリ)が常に 0 行を返していた。HEAD の分岐を復元。
- 型付き並列集約(`RunTypedParallelStreamAggregate`)の int_key 高速パスが
  NULL グループキーを 0 に畳み込み、NULL グループがキー 0 に統合される
  (`GROUP BY d` で NULL 行が 0 行グループに加算される)。NULL キーを
  正規化行インデックスへ迂回させて解決。ついでに typed 経路で
  `aggregate_updates` テレメトリが加算されない問題も修正。
- 同一表を 2 回参照する文(`reusable_base_relations` 登録対象)で型付き
  並列集約が共有ベースリレーションキャッシュの部分充填を読み、スカラー
  非相関サブクエリ `(SELECT MAX(a) FROM t)` が全行で誤値を返す。
  `!reusable` 時のみ typed 経路を使うゲートで回避(根本修正はキャッシュの
  fill/read 順序管理)。
- `main.cpp`: SQL 文ごとの Prepare 失敗で LastError が空(外部パーサ子
  プロセスの一時不調)の場合に 1 回だけリトライ。

## 2026-09-22 に検出した既知バグ(未修正)

1. **READ ONLY トランザクションで派生表(集約)の結合が黙って 0 行**
   - **解消済み(2026-09-23 再検証)**。q61 コア形状・フル形状の両方を
     `BeginReadOnlyContext()` で実行し、read-write と同一の 1 行を返すことを
     独立ハーネスで確認。pgwire 経由の派生表集約結合も正常。
   - 推定箇所: 派生表の実体化と read-only スナップショット
     (`full_scan_iterator.cpp` の physical-read fast path)の組合せ。
2. **GROUP BY ROLLUP でプラン探索が爆発**(q18: 実行は一瞬、計画に ~33 秒)
   - **改善済み(2026-09-23 計測)**。同一ビルドでの q18 は、コア共有なし
     5799 ms / 共有あり 1970 ms（どちらも 37 行。セット演算は Prepare 中に
     枝を実行するため、この時間のほぼ全ては計画で、実行は以前測った
     ~0.02 秒のまま）。2026-09-22 の記録 32547 ms からの短縮は約 16 倍。
     起点は MayApply 後の「計画 ~5.7 秒 / 実行 0.02 秒」。Clang Release
     (`-O3 -DNDEBUG`) では q18 は 295 ms、16 クエリ合計 960 ms
     (`BenchmarkHistory.md` の 2026-09-23 Release 節)。入った変更:
     (a) ROLLUP の UNION ALL 各枝が同一 FROM+WHERE コアの Cascades 計画を
         共有する(`SqlEngine::shared_grouping_cores_`。後続枝は必要列が
         計画スキーマに含まれるときだけ再利用);
     (b) 連結した結合グループは csg-cmp で一度だけ列挙する;
     (c) 結合コストの `TableStatistics` は `ScaleCardinality` で行数だけ
         縮尺し、ヒストグラムと MCV は複製しない;
     (d) 複数等値キーのカーディナリティは指数バックオフ
         (`EstimateEquijoinSelectivity`)。
   - 実施済み(先行): `setop_empty_*` / `no_op_window_elimination` 等 8 規則の
     MayApply プリフィルタ(target 複数指定)で 1 枝あたり 53613 → 36731
     ステップ(-31.5%)。`TINYLAMB_RULE_STATS=1` で規則別試行数を取得可能。

## 2026-09-22 に修正したもの(参考)

- `GROUP BY GROUPING SETS (())` 等の group-all 文が空入力で
  グランドトータル行を欠落(`relational.cpp` の stream_agg 経路の
  空入力条件に `IsGroupAll()` が含まれていなかった)。
- UNION DISTINCT 派生表への外側述語押し込みが効かない
  (`OptimizeDerivedBoundaries` の `is_union` 条件を kUnion/kUnionAll
  両対応に拡張。フィルタと DISTINCT は可換のため安全)。
- 集約高速化経路(FinishQuery 直集約)が `AggregateAccumulator` へ
  入力スキーマを渡していなかったため、ARRAY_AGG の全 NULL グループで
  要素型が `ARRAY<>` に崩壊(スキーマ引き渡しを修正)。
- グループ化クエリの `ORDER BY <集約式>`(TPC-DS q42)が
  "aggregate was not prepared" で失敗(ORDER BY 項の集約ノードを
  select list の同一集約へ引き当て、準備対象に追加)。

## 2026-09-23 カバレッジ拡充セッションで修正したもの

- **量化比較の membership stash ゲート反転**(`executor/detail/
  planning_heuristics.cpp`)。`col > ANY (SELECT ...)` 等の非等価量化が
  「等価 IN のみスタッシュ可」のゲートを素通りして等価キーセットとして
  stash され、空結果に縮んでいた。反対にゲートが守るべき `= ANY` 形は
  スキップされていた。条件の誤った `!` を外し、意図通り
  (`kIn` または `= ANY` のみ stash)に修正。
  回帰: `QueryTest.QuantifiedSubqueryKeepsOperatorAndNegation`
  (WIP で追加済みのテストが本修正で初めて緑化)。
- **論理規則 8 件の MayApply プリフィルタ**(`plan/cascades.{hpp,cpp}`)。
  `setop_empty_simplification` / `setop_empty_identity` /
  `no_op_window_elimination` / `join_empty_simplification` /
  `intersect_except_cost_based_lowering` / `grouping_sets_expansion` /
  `distinct_and_group_by_interchange` / `aggregate_projection_merge` が
  `Pattern::Any()` + `std::nullopt` target で全式×全訪問に試行されていた
  (TPC-DS q18 の 1 枝あたり探索ステップの ~36%)。`Rule` に複数 target
  演算子の ctor を追加し、body の実際の根演算子集合を宣言させた。
  1 枝あたり 53613 → 36731 ステップ(-31.5%)。body のゲート変更はなし
  (プリフィルタのみで意味論不変)。
  回帰: `CascadesTest.RuleTargetListPrefiltersOtherOperators`。
- **計測器**: `TINYLAMB_RULE_STATS=1` で探索の規則別試行数を LOG(INFO)
  に出力(env ゲート、無効時ゼロコスト)。プラン時間の内訳追参考期。
- **破損WIPの修復**: 未コミット作業ツリーに置換事故由来の構文破損が
  複数ファイルで発生(tpcds_benchmark.cpp / sql_session_fuzzer.cpp /
  query_test.cpp / function_call_ast_*_sweep_test.cpp /
  sql_expression_matrix_test.cpp / sql_statement_sweep_test.cpp /
  sql_function_edge_test.cpp / expression_eval_coverage_test.cpp /
  googlesql_ast_visitor.cpp の重複行)。文脈から復元しビルド通過まで修復。

## 2026-08-24 時点の記録(参考)

2026-08-24 時点。各改善計画書(tpcc-improvements / tpch-sf10-plan / improvement3)の
実施可能項目は全て消化済み。ここには**意図的に先送りした設計判断・
大規模項目**だけを残す。完了履歴は git 履歴と BenchmarkHistory.md を参照。

## 2026-08-24 再監査で消化した項目

- **D-8: AST 直評価の release 廃止** はバックログから除外した。
  `docs/expression_evaluation.md` では AST (`Expression::Evaluate` /
  `EvaluateBinary`) が意味論の正本、bytecode はバッチ実行用 IR であり、
  AST fallback は意図した構成である。したがって AST 廃止は現行方針と矛盾する。
  実際に残っていた detail 経路の `CONCAT(..., NULL, ...)` の差は、
  AST と同じ strict NULL 伝播に統一し、differential test で固定した。
- **D-1: LockManager 完全 MVCC 統合** — production の行ロック依存を外し、
  version chain 上の write intent へ統合した。TPC-C 5.11.0 のisolation testに
  合わせ、競合writerは短時間待機後に先行commitの最新版を読んで継続する
  strict write locking とした。未stage intentは通常readerには不可視である。
- **D-2: Engine facade** — `SqlEngine::Execute` / `QueryResult` を導入し、CLI、
  pgwire、TPC-C/TPC-H benchmark の準備・stream・affected rows 処理を統一した。
- **D-3: TransactionContext 分解** — `CatalogReader` capability を抽出して
  `Database*` を除去し、executor の `thread_local active_runtime` を
  `TransactionContext` 経由の明示 runtime に置換した。
- **D-4: relational IR の Cascades memo 接続** — `kRelational` logical operator
  と `RelationalPlan` implementation rule を追加し、CTE/subquery/outer join 等の
  specialized relational IR も memo/search を経由して物理実装を選ぶ。
- **D-5: PAX ページ型の統合** — `PageType::kPaxPage`、page union、v1 directory、
  null bitmap、固定幅/VARCHAR の `DataChunk` 永続化を実装した。
- **D-7: serdes BE固定 + magic/version ヘッダ** — scalar codec、page、WAL、
  master checkpoint record を big-endian v1 に更新した。ユーザー承認により
  v0 dual reader は設けない破壊的 format bump とした。

## 継続して先送りする項目

| # | 項目 | 保留理由と再開条件 |
|---|---|---|
| D-6 | **分散実行**(`distributed*.md`) | Raft、replication、network transport、snapshot/install、障害試験が未実装の新サブシステムで、小規模リファクタではない。分散化をロードマップに戻したとき、最初のスライスで `LogStorage` interface と単一ノード動作テストを追加 |
| D-8 | **DML WHERE の非相関サブクエリ** | `DELETE/UPDATE ... WHERE u IN (SELECT ...)` は planning の range extraction が通らず "query expression requires relational evaluation" で Status 拒否(SELECT 側には `emit_relational()` フォールバックあり)。fuzzer は正しく skip 処理済み。再開条件: PrepareStatement の kDelete/kUpdate で非相関 IN/EXISTS/スカラーを事前評価して定数化(相関検出は外部名スキャン) |

意味論・公開API・disk formatを理由に保留していた項目は残っていない。
ユーザー判断により破壊的変更を許容してD-1〜D-5/D-7を消化済みで、継続保留は
分散基盤D-6とDML非相関サブクエリD-8である。

## 測定ゲート

性能系の効果検証は次のコマンドで行い、結果を BenchmarkHistory.md に記録:

- TPC-H: `tinylamb_tpch_benchmark`
- TPC-C: `tinylamb_tpcc_benchmark --clients 10 --seconds 60`
- ベンチ回帰ゲート: `scripts/bench_gate.py`(CI: .github/workflows/benchmark.yml 週次)

## 2026-09-23 コード健全性セッションの成果と残項目

**達成:**
- コンパイラ警告 0 件(GCC 15.2 / Clang 21.1、`-Wall -Wextra -Wpedantic -Wshadow
  -Wformat=2 -Wundef -Wextra-semi -Wimplicit-fallthrough -Wconversion
  -Wsign-conversion`。`build-warn-gcc` / `build-warn-clang` は
  `TINYLAMB_ENABLE_CONVERSION_WARNINGS=ON`)。
- ASAN / TSAN 両ビルドで 3152/3152 全パス(データレース・メモリ違反なし)。
- clang-format 全 659 ファイル適用済み。
- `/tmp` に蓄積していた漏出 spill ファイル(tinylamb-spill-*.bin 約 5.3GB)
  を掃除。SpillFile デストラクタは正常。蓄積は過去のファザー abort 実行の
  副作用 → 長時間ファズ sweep の後は `scripts/clean_root_artifacts.py` を
  推奨(自動化済み・CIは既に実行)。
- WAL セグメント 0 はベースファイル自身(`SegmentPath(base, 0) == base`)
  という契約を `MigrateLegacyLog` 含め確定・実装。
  `LoggerTest.AppendExponential` は 1GiB セグメントを明示指定して
  回転を回避(回転経路の asserts は別テスト群が担当)。
- `test_workspace_cleanup_env` 系の並列 ctest 競合:同一バイナリの gtest
  ケースが同一 DB 名を共有すると -j で WAL 破 corrupt → 5 テストフィクスチャの
  DB 名に `RandomString()` サフィックスを復元し解消
  (sql_function_coverage / sql_expression_matrix / executor_feature /
   sql_statement_sweep / sql_plan_and_cast_sweep / sql_extra)。
- `EmitSnapshotIfNeeded` を `StatusOr<lsn_t>` にし、FPW シンク失敗時に
  データファイル書き込みを中断させる(WAL-before-data の強化)。

**残項目(次回以降):**
- clang-tidy: プロダクションコード(common〜server)は 0 件。
  plan/executor/query のテストファイルにニット(設計初期化子・生文字列・
  `char[]`→`std::array` 等)が残存。ベースライン 376 件から大幅減。
- 行値式 `(a, b) IN (SELECT ...)`(上記 1 項目、機能追加規模)。
