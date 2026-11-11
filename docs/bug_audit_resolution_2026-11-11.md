# 2026-11-10 再監査報告への対応結果（2026-11-11）

対象は元の `docs/bug_audit_2026-11-10_rerun.md` に記載された310件（high 29、medium 120、low 161）。元の重要度と番号を引き継ぎ、検証内容・対応・残る試験範囲を以下に保存する。元ファイルはこの報告へ移行し、削除済み。

元ファイル SHA-256: `1caa2cce1af75c1953e7ee3e436c843bf9d76236a6be1c8b1e1005fe43e864a9`。元の行番号は監査当時の位置であり、現在のコードの行番号ではない。

これは310件すべてのバグが実行試験で再現・解消されたという報告ではない。文書の誤記、重複、現行コードでは成立しない指摘も含む。修正後に関連テストが成功した項目と、障害注入・境界値・対象OSなどの専用検証が残る項目を区別する。別作業が同じツリーを編集しているため、全体テストの結果は実行時の共有作業ツリーに対するもの。

## 対応の内訳

| 判定 | 件数 |
|---|---:|
| 修正＋関連テスト成功 (`fixed-tested`) | 153 |
| 説明・仕様記述の訂正 (`docs-corrected`) | 98 |
| 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`) | 15 |
| 他項目と重複・対応箇所を修正 (`duplicate-fixed`) | 10 |
| 修正＋コード確認（専用再現試験なし） (`fixed-code-reviewed`) | 4 |
| 修正済み・専用境界値テスト未実施 (`fixed-needs-boundary-test`) | 3 |
| テストで指摘を否定 (`refuted-tested`) | 3 |
| 防御を追加＋関連テスト成功 (`defensive-fix-tested`) | 3 |
| 既に対処済み＋関連テスト成功 (`already-fixed-tested`) | 3 |
| 現行コードでは指摘を確認できず (`not-reproduced-current-code`) | 2 |
| 重複指摘 (`duplicate`) | 2 |
| 既存の方言差を明記（実装未変更） (`documented-compatibility-limitation`) | 2 |
| 到達不能コードを削除 (`dead-code-removed`) | 2 |
| 説明を訂正＋関連テスト成功 (`docs-corrected-tested`) | 1 |
| 修正済み・専用キャッシュ経路テスト未実施 (`fixed-needs-cache-test`) | 1 |
| 修正済み・専用スピル経路テスト未実施 (`fixed-needs-spill-test`) | 1 |
| 修正済み・専用SQL到達経路テスト未実施 (`fixed-needs-query-test`) | 1 |
| 修正済み・問題箇所を直接通す専用テスト未実施 (`fixed-needs-direct-test`) | 1 |
| 現行コードで既に対処済み (`already-fixed-current-code`) | 1 |
| 指摘の一部を否定し、必要な箇所を修正＋テスト成功 (`partly-refuted-fixed-tested`) | 1 |
| 修正済み・対象プラットフォームで未実行 (`fixed-needs-platform-test`) | 1 |
| 前回監査の網羅性に関する注意（製品バグではない） (`audit-coverage-limitation`) | 1 |
| 現行保存形式の制約を明記（互換性の機能追加なし） (`documented-format-limitation`) | 1 |

## 主な修正

- SQL・式評価: IN/NOT INの比較・読み込みエラー伝播、明示的照合順序の競合、AST/bytecodeの評価差、整数・日時の境界値、STRUCT/JSONのエスケープや展開を修正。
- 最適化・実行: エラーを消す書き換えや未証明の順序・非NULL仮定を抑止し、集約・結合・スピル・キャッシュの誤結果やエラーの握り潰しに対処。
- 保存・回復: WAL境界、ページ再利用、失敗時の補償、PAXの破損入力検査・範囲検査などに対処。PAX構築はStatusOrで失敗を返す。
- INTERVALとSTRINGを意味的に分離。見た目がINTERVALの通常文字列はバイト比較・DISTINCTを行い、型付きINTERVALだけが数値成分で比較される。型情報を行・配列・スピル・索引キーへ保持し、対応しないPAXの経路は使用しない。
- 全体テストで発見した追加修正は下記に記録する。

## 全体検証での追加修正

### 非一意索引の共有キーに別トランザクションが追記すると自分の未コミット行を見失う

LSMスナップショットが外部の未コミット更新を除外するとき、自分のstaged WALにある最新の同一キー更新を復元する。ログチェーンmutexで参照を保護。

検証: PageManagerTest.AuditLsmScanKeepsOwnWriteUnderForeignOverwrite、SqlSessionFuzzer 64 seed、保存したaudit_lsm_shared_key_own_write.test。batch69通常版は成功。

### Bytecodeのバッチ評価がUINT64タグを失う

結果ColumnVectorのドメインを実際の非NULL値から推論する。

検証: BytecodeTest.AuditBatchProjectionPreservesUnsignedValuesAfterNull、SourceIntegerPeeksPreserveTypesNullsAndSnapshot。batch67通常版は成功。

### CTASおよび投影で狭い整数幅やINTERVALメタデータが失われる

投影・SELECT *・CTASを通して宣言ドメインを保持する。投影には元の列制約を引き継がない。

検証: SqlAuditRegressionTest.CtasPreservesDeclaredIntegerAndIntervalDomains、dml_insert compliance。batch67通常版は成功。

### 内部STRUCT関数が数値のフィールド名をVARCHARのunionメンバーから読んでいた

VARCHARのフィールド名は生の文字列を使い、それ以外はValue::AsStringで変換する。数値のunion誤読とVARCHAR名の余計な引用符をともに避ける。

検証: SqlFunctionEdgeTest.PinnedScalarTable内の__struct_json__(1,'x')。batch69成功。

### TIMESTAMP文字列をINTERVALとして誤解析する混合演算

TIMESTAMP形状の文字列と型付きINTERVALの+/-を明示的なNotImplementedにする。現状はTIMESTAMP_ADD/SUBを使用。演算子の機能実装を済ませたという意味ではない。

検証: SqlFunctionEdgeTest.ErrorTable、DeviationSweep。batch68通常版成功。

### 空JOINのROLLUP / GROUP BY ()で総計行が消える

GroupedJoinFusionSpecが空のグルーピング集合を示すIsGroupAllマーカーを尊重し、構文展開用の定数キーを実際のグループキーに含めない。空入力でも単一の集約状態を作ってからHAVINGを評価する。

検証: SqlAuditRegressionTest.EmptyJoinRollupPreservesGrandTotalWithHaving（通常/強制relational、COUNT/SUM/MIN/MAX、HAVINGの採用/除外、通常の定数GROUP BYとの区別）。query/testdata/oracle_regressions/audit_empty_join_rollup_having.testにfuzz再現を保存。通常版の回帰テスト・再現ファイル再生成功。ASan/UBSanと追加fuzzは後続の検証結果に記録。

## 検証結果

- **batch65_build**: normal all-target build and selected sanitizer build succeeded; not a clean-from-scratch warning audit
- **batch66_ctest**: 3910 CTest entries; 25 failed before follow-up fixes, see audit-batch66-ctest.log
- **batch66_asan**: 570 tests across 22 binaries passed; leak detection disabled, ASan/UBSan halt_on_error enabled
- **batch66_expr_fuzz**: 6391 runs in 301 seconds; exit 0, ASan/UBSan
- **batch66_sql_fuzz**: 292 runs in 302 seconds; exit 0, ASan/UBSan
- **batch65_66_tidy**: 22 translation units; initial 12 diagnostics corrected, 3 affected units rechecked with zero diagnostics
- **batch68_tidy**: 8 translation units; zero diagnostics
- **layering**: 78 existing allowlisted edges; pass, no new allowlist entry
- **batch69_tests**: page_manager/sql_session_fuzzer/sql_function_edge/table/index_scan_iterator/lsm_index の6バイナリが通常版で成功。保存したsession再現ファイルも追加。
- **batch69_71_tidy**: LSM修正の4 translation units。transaction.cppの指摘を修正し、batch71で再検査ゼロ。
- **batch71_asan_tests**: STRUCT名修正後、sql_audit_regression/sql_function_edge/page_manager/sql_session_fuzzer/differentialの5バイナリ、70テストがASan/UBSanで成功。
- **batch70_asan**: 15バイナリを実行。STRUCTフィールド名とNULL先頭列の型推論の2回帰を検出し修正。その他13バイナリ成功。最終の該当テスト再実行は後続バッチに記録。
- **batch71_ctest**: 3913 CTest項目を実行、失敗1（NullLeadingProjectionRetainsUniformTypes）、スキップ3。混在型の保守的な扱いを復元し、全NULL列は宣言型を保持するよう修正して再検証。
- **batch72_asan_tests**: NULL先頭列の推論修正後、NullLeadingProjectionRetainsUniformTypesとsql_audit_regression全33テストの計34テストがASan/UBSanで成功。batch70で検出した2回帰はbatch71/72でともに解消を確認。
- **batch73_build**: 通常の全ターゲットビルド成功。batch70/71/73のビルド出力にコンパイラwarningなし。ただしclean-firstによる全翻訳単位の再コンパイル検査ではない。
- **batch73_ctest**: 3913 CTest項目、失敗0、既存のスキップ3（実行3910）、87.14秒。追加ROLLUP回帰テストを加える前の結果。
- **batch71_fuzz**: expr oracleは301秒・6112 runsで成功。SQL oracleはseed 7732802137448036699で空JOINのROLLUP総計行欠落を検出して停止。ASanメモリエラーではなく実行経路間の結果不一致。再現ファイルを保存し、後続batch75で修正・再実行。
- **batch72_tidy**: relational.cppは指摘ゼロ。relational_regression_test.cppに10指摘（特殊メンバー、波括弧、optionalアクセス、reserve、raw string）を検出しbatch75で修正・再検査。
- **batch75_rollup_replay**: 通常版sql_audit_regression全34テストと、保存済みSQL oracle再現ファイル一式のReplayCommittedRegressionFilesが成功。修正前の追加テストでは通常/強制relationalの両方で総計行が欠落、fusion無効時には成功することを確認。
- **batch75_asan_replay**: sql_audit_regression全34テストとSQL oracleの保存再現ファイル再生1テスト、計35テストがASan/UBSanで成功。
- **batch75_layering**: include-lint成功。既存allowlist 78件、新規allowlist追加なし。git diff --check成功。
- **batch75_asan_relational**: relational_regression_testの176テスト（2 suites）がASan/UBSanで成功、170.394秒。batch75でのASan/UBSan最終再実行はSQL監査34＋oracle再生1＋relational176の合計211テスト。
- **batch76_build_ctest**: 通常の全ターゲットビルド成功、ビルド出力にcompiler warningなし（増分ビルド）。3914 CTest項目、失敗0、既存スキップ3（実行3911）、91.84秒。
- **batch75_tidy**: typed_column_eval.cpp、sql_audit_regression_test.cpp、relational_regression_test.cppの3 translation unitsを再検査し、指摘0・失敗0。batch72で検出した10指摘を解消。リポジトリ全体のClang-Tidy完了を意味しない。
- **batch75_sql_fuzz**: ROLLUP修正後のSQL oracle fuzzerはASan/UBSan下で302秒・274 runsを完走、exit 0。有限時間の探索結果であり、未発見バグがないことの証明ではない。

詳細な実行ログ・機械可読の台帳は `build/quality/bug-audit-2026-11-11/` に保存。初回の失敗ログも残しており、後続の成功記録で上書きしない。

## 残る制約と未実施の検証

- BOOLはINT64と同じ内部表現を使い、数値との混在演算を許す。厳密なGoogleSQLのBOOL型チェックは今回実装していない（low-111）。
- 非集約・非グループ列を許す疎なGROUP BYは既存の方言であり、代表行の選択は実行計画をまたぐSQLの保証ではない（low-114）。
- PIVOTは明示した名前付き列を持つ派生表を要求する。不正なグルーピングを返していた基底テーブル等の形は明示的に拒否する。全PIVOT構文の対応完了ではない（medium-88）。
- TIMESTAMPとINTERVALの混合演算子は明示的に未対応を返す。TIMESTAMP_ADD/SUBを使用する。
- WAL v7の異なるエンディアン間の移送は未対応（low-117）。Windowsの待機修正はWindows実機で未試験（low-6）。
- INTERVAL・宣言整数幅のメタデータおよびValueのタグ付き直列化を変更した。古いメタデータのINTERVAL表は再作成が必要。詳細は [value_storage.md](value_storage.md)。
- ClickBenchでのDuckDB全面勝利、リポジトリ全体のクリーンビルド警告ゼロ・全Clang-Tidyゼロは、この監査対応の完了条件とは別の継続作業。ここに示す部分検査を全体達成とみなさない。

専用の再現・注入・対象OS試験が残る項目:

- [high-7](#high-7): 修正済み・専用の障害注入テスト未実施。
- [high-20](#high-20): 修正済み・専用の障害注入テスト未実施。
- [medium-29](#medium-29): 修正済み・専用の障害注入テスト未実施。
- [medium-30](#medium-30): 修正済み・専用の障害注入テスト未実施。
- [medium-31](#medium-31): 修正済み・専用境界値テスト未実施。
- [medium-36](#medium-36): 修正済み・専用境界値テスト未実施。
- [medium-41](#medium-41): 修正済み・専用の障害注入テスト未実施。
- [medium-42](#medium-42): 修正済み・専用キャッシュ経路テスト未実施。
- [medium-43](#medium-43): 修正済み・専用スピル経路テスト未実施。
- [medium-45](#medium-45): 修正済み・専用境界値テスト未実施。
- [medium-46](#medium-46): 修正済み・専用SQL到達経路テスト未実施。
- [medium-50](#medium-50): 修正済み・問題箇所を直接通す専用テスト未実施。
- [medium-55](#medium-55): 修正済み・専用の障害注入テスト未実施。
- [medium-64](#medium-64): 修正済み・専用の障害注入テスト未実施。
- [medium-105](#medium-105): 修正済み・専用の障害注入テスト未実施。
- [medium-114](#medium-114): 修正＋コード確認（専用再現試験なし）。
- [medium-116](#medium-116): 修正済み・専用の障害注入テスト未実施。
- [medium-117](#medium-117): 修正済み・専用の障害注入テスト未実施。
- [low-6](#low-6): 修正済み・対象プラットフォームで未実行。
- [low-10](#low-10): 修正＋コード確認（専用再現試験なし）。
- [low-11](#low-11): 修正済み・専用の障害注入テスト未実施。
- [low-13](#low-13): 修正済み・専用の障害注入テスト未実施。
- [low-61](#low-61): 修正済み・専用の障害注入テスト未実施。
- [low-67](#low-67): 修正済み・専用の障害注入テスト未実施。
- [low-127](#low-127): 修正＋コード確認（専用再現試験なし）。
- [low-128](#low-128): 修正＋コード確認（専用再現試験なし）。
- [low-147](#low-147): 修正済み・専用の障害注入テスト未実施。

## 310件の個別記録

以下の「確認・対応記録」は各バッチ時点の調査メモ（テスト名・実装名は原文）を保持する。古いメモの「pending」は、その後の全体検証で関連バイナリが通った場合も、当該障害条件を直接注入した証拠を意味しない。最終判定に未実施試験を明記した項目は未検証として扱う。

### high-1

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/expression_eval.cpp:980`

**指摘:** サブクエリ IN の比較エラーが ThreeValuedEquals で UNKNOWN(-1) に飲み込まれ、AST ground truth(エラー伝播)と本番 relational 経路の結果が割れる。同一 relational evaluator 内でもリテラル IN (kInExp) は同種の飲み込みを「fuzz-found で ground truth から diverge した」としてエラー伝播に修正済みだが、サブクエリ IN 経路には修正が適用されていない。

**発生条件:** SELECT 1 IN (SELECT s FROM t)(t.s が VARCHAR 'abc' を含む)。AST参照・リテラルINは TryEvaluateBinary(kEquals) の "type mismatch" を伝播するが、本番 kQueryExp 経路ではエラーが UNKNOWN になり行が黙って除外される(NOT IN なら誤って TRUE)。クエリがエラーではなく誤結果を返す。

**確認・対応記録:**

- IN/NOT IN over VARCHAR subquery vs INT64 probe reproduced a silent result before fix. Checked recursive three-valued comparison now propagates errors and the rescan checks both comparison and spill statuses. AuditSubqueryInPropagatesComparisonErrors passes.
- Also erase failed/partial uncorrelated membership cache entry and propagate row-source read errors; new fault-path test pending.

### high-2

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:784`

**指摘:** TIMESTAMP/DATETIME_ADD(_SUB)の秒〜週単位の経路で、最初の変換はセッション既存タイムゾーンのオフセットを使う(line 632-637)のに、最後の逆変換がハードコードの +8h を加えて '+00' を付ける。両者は既存ゾーンのオフセットがちょうど -8h(PST)のときだけ打ち消し合い、それ以外のゾーンでは (オフセット+8h) ずれた誤結果になる。月/年単位の経路(681-688)は同じセッションゾーンで対称変換しており、この非対称が意図でないことを示している。

**発生条件:** 既定ゾーン( type/date.cpp:190 で America/Los_Angeles)のサマータイム期間(3月〜11月, オフセット -7h)で `SELECT TIMESTAMP_ADD(TIMESTAMP '2020-07-01 00:00:00+00', INTERVAL 1 HOUR)` → 633行で ct に -7h を加算し、784行で +8h を再加算 → 正解 '2020-07-01 01:00:00+00' に対し '2020-07-01 02:00:00+00' を返す(年間約8割の日数で既定設定のまま1時間ずれる)。UTC セッション(query/googlesql_compliance_test.cpp:575 が SetDefaultTimeZone("Etc/UTC") を実行、postgres_server.cpp:1067 はクライアントに TimeZone=UTC を告知)では 8時間ずれる。

**確認・対応記録:**

- Reproduced in UTC, Los Angeles and +05:30; fixed duration reverse conversion to use the actual original offset. UTC/PDT/DST transition add/sub regression passes.

### high-3

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/fused_grouped_join.cpp:1947`

**指摘:** count_nonnull_only 融合が全COUNT集約を単一のraw_countに折りたたみ、その増分を count_right_column(右側NULLable列)のNULL有無だけで gate する。左側kNotNull/kPrimaryKey列のCOUNTが、右列がNULLの一致ペアで加算されず過小カウントになる。633-639行目のコメント「Mixed shapes stay on the generic path」と矛盾し、コードは (a)非NULL列 と (b)右NULLable列 の混合形を汎用経路に落とさず融合する(648-686行目の分類で両者とも eligible になる)。

**発生条件:** SELECT o.o_orderstatus, COUNT(o.o_custkey), COUNT(l.l_comment) FROM orders o JOIN lineitem l ON o.o_orderkey=l.l_orderkey GROUP BY o.o_orderstatus(o_custkey が左PK、l_comment が右NULLable)。l_comment が NULL の一致ペアでは raw_count が増えず、COUNT(o_custkey) が実ペア数より小さく出る。emit(fused_grouped_join.cpp:3757, 3789)と direct_gid 経路(2596-2610, 3200-3205)も同一の単一raw_countを出す。

**確認・対応記録:**

- Direct INNER/LEFT fused COUNT regressions failed before fix; independent null domains now decline the shared-count shortcut. AuditFusedCountsKeepIndependentNullDomains passes.

### high-4

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/parallel_table_scan.cpp:932`

**指摘:** LSTable の DOUBLE 列ゾーンマップ判定で、ブロックに double 統計が無い場合(!dcol.has_double_min_max)を zone_rejected=ブロック丸ごとスキップと扱う。ところが lstable_bridge.cpp:160-172 では非NaN値を1つも見ていないブロック(NaNのみ/全NULL)では has_double_min_max が false のまま。NaN は kNotEquals ではマッチする(NaN <> 5 は TRUE)ので、そのブロックの行がサイレントに欠落する。同ファイルの INT peek 経路(895: `!pcol.has_min_max → peek_all_pass=false` で保守的にスキャン継続)と規約が一貫しない。

**発生条件:** FLOAT64 列 d が全行 NaN のテーブル(1ブロックに収まる行数)で `SELECT ... WHERE d <> 5`。TryCompileSimpleCompare(scan_filter.cpp:601-633)は定数5を double_payload 化し、parallel_table_scan.cpp:345-351 でゾーン述語登録。該当ブロックは 932-934 でスキップされ、NaN<>5 が TRUE の行が結果から消える(誤結果・データ欠落)。

**確認・対応記録:**

- All-NaN and mixed NaN/5/NULL blocks lost rows under <>. Added explicit has_nan metadata and conservative zone-map/all-pass guards in relational, typed aggregate and ParallelScan paths. AuditDoubleZoneMapsKeepNaNNotEqualsRows passes.

### high-5

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/scan_filter.cpp:327`

**指摘:** int_payload高速経路が列型を確認せずDATE列 vs INT64定数(およびINT64列 vs DATE定数、DATE列×INT64列のcol-col)を素のintビット比較で決定する。AST真値(TryEvaluateBinary)はDATE≠INT64の比較をtype mismatchエラーにするため、走査フィルタだけが意味論から逸脱し、エラーを黙殺して誤一致/誤欠落を起こす。

**発生条件:** SELECT * FROM t WHERE date_col = 5 (date_colはDATE)。TryCompileSimpleCompareのint_payload付与(scan_filter.cpp:597-600)は列型を見ないため定数INT64のままint_payloadになり、MatchSimpleCompareがday-count==5の行(1970-01-06等)を誤って返す。正道ではクエリはkInvalidArgumentで失敗すべき。逆に一致しない場合はエラーが0行に偽装される。同系: col-colのint_like×int_like許容(scan_filter.cpp:221-227)、eval_simple_on_lstable(parallel_table_scan.cpp:546-549)、TryMatchSimpleCompareArena(scan_filter.cpp:3502-3506)。

**確認・対応記録:**

- Direct TryCompileSimpleCompare regression AuditDateNumericComparisonUsesCheckedEvaluator verifies DATE/numeric comparisons decline fast path and AST rejects mismatched types; SQL tests also pass.

### high-6

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/scan_filter.cpp:966`

**指摘:** PrebuiltMembershipPredicateの直接fillで contains_null を列全体フラグ column_has_null で初期化するため、WHEREフィルタでNULL行が除外される場合も contains_null=true のまま残る。NOT INのクリーンミスがUNKNOWN扱いされ、本来TRUEの行が静かに欠落する(空集合決打ちの短路も無効化される)。

**発生条件:** WHERE x NOT IN (SELECT y FROM t WHERE y > 0) で t.y にフィルタ範囲外のNULLがあり t が約3万行以上(dense fill適合)のとき、フィルタ済み集合へのクリーンミスに対しMatchPrebuiltMembershipのmiss経路 `return !pred.contains_null && pred.negated`(scan_filter.cpp:2244)がfalseを返し行を捨てる。values.Empty()でも2218の決打ち `values.Empty() && !pred.contains_null` が効かない。

**確認・対応記録:**

- 33000-row filtered nullable NOT IN build reproduced wrong empty result. Null membership now starts false and is recorded only for passing build rows. AuditFilteredMembershipDoesNotRetainNulls passes.

### high-7

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/detail/subquery_runtime.cpp:1206`

**指摘:** 相関サブクエリのビルド経路で Relation::FinishSpill() と ForEachRow() の戻り Status を無視し、スピル書き込み/読み出し失敗やメモリ圧時の行落とし(sticky spill_error_)を飲み込んで、欠損した行集合で集約・EXISTS を成立見込みとして返す。relation.hpp:64-68 は「エラーチャネルを持つ呼び出し元は truncation を表面化せよ」と契約を明記し、同種の呼び出し元(stream_grouped_join.cpp:556-557、fused_grouped_join.cpp:3829-3830)は全て RETURN_IF_FAIL でチェックしている。

**発生条件:** LoadSource がメモリ圧で行を落として spill_error_ を記録した状態(または FinishSpill/ForEachRow がスピルI/Oエラーを返す状態)で相関サブクエリを評価すると、不完全な source に対して集約ビルド(1687: accumulate_one)、EXISTS キーセット(2059)、汎用行マルチマップ(2076)が回り、COUNT/SUM が小さすぎる値や EXISTS=false を正常結果として返す。parallel ビルド経路(1531)は source.rows を直接読むため !HasSpill() gate で安全性は保たれるが、直列 fallback の4箇所が無チェック。

**確認・対応記録:**

- Optional correlated cache build now declines on FinishSpill/ForEachRow failure in aggregate, EXISTS and generic multimap paths; partial cache is not published. Fault test pending.

### high-8

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/typed_stream_aggregate.cpp:2941`

**指摘:** 直接LSTable走査のブロック选別で、double列のゾーン統計が欠落(has_double_min_max==false)だとそのブロックを「該当行なし」として棄てる。has_double_min_maxは非NaN値を1つでもデコードしたときしか立たないため、NaN値を含むブロックがWHERE d <> cのようなNOT_EQUALSフィルタで丸ごと落され、静かに結果が欠落する。

**発生条件:** DOUBLE列にIEEE_DIVIDE(0,0)等でNaNを保存し、ブロック内に c と等しい有限値がmin==max==cになる(またはブロック内の非NULL値が全てNaN)状態で `SELECT k, COUNT(*) FROM t WHERE d <> 0.5 GROUP BY k` を実行。NaN行は d <> 0.5 が真なので残るべきだが、typed_stream_aggregate.cpp:2952-2956のkNotEqualsゾーン判定の前に2941-2943でブロック棄却され行が欠ける。

**確認・対応記録:**

- Same root cause and shared regression as high-4; typed aggregate pruning and all-pass logic fixed.

### high-9

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/window_eval.cpp:1175`

**指摘:** ウィンドウ版 APPROX_QUANTILES のフレーム整列コンパレータが `a < b` をそのまま使うため、DOUBLE NaN が全値と「同値」扱いになる。Value::operator< は kDouble に対し生の IEEE 比較(type/value.hpp:397)で NaN 比較は常に false、よって cmp(NaN,5)=cmp(5,NaN)=false となり厳密弱順序が破れる(5~NaN~7 で推移律崩壊)。std::sort に不正コンパレータを渡すのは UB で、libstdc++ の introsort が範囲外読み/無限ループに至り得る。PERCENTILE_CONT 側(1741-1768)は a_nan/b_nan を明示処理していて対比が明確。

**発生条件:** フレーム内に FLOAT64 の NaN が混入した入力(例: CAST('nan' AS FLOAT64) や 0.0/0.0 の結果)で `SELECT APPROX_QUANTILES(x, 2) OVER () FROM t` を実行すると、non_null は NULL だけ除去し NaN は残る(window_eval.cpp:1095-1101)。1174-1185 の std::sort が UB に突入し、SEGV または不正な分位点順。

**確認・対応記録:**

- Replaced non-strict NaN comparator in window and scalar APPROX_QUANTILES with CompareForOrderBy. SQL regression passed even before; direct comparator coverage pending.
- Direct AuditDirectWindowQuantilesUseTotalOrder pins full 1024-row NaN/finite quantiles; passes.

### high-10

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/hash_join.cpp:1692`

**指摘:** パイプライン化されたprobeがspillファイルの読み込みに失敗したとき、spill_errorはJoinStateに記録されるだけ.PercentileWith(unit='_percentile', value=...)PercentileWith

**発生条件:** 反応的spillが発生した片側spillハッシュ結合(kInMemory, pipelined)で、probe側spill区画のReadAllPositionedがI/Oエラーで失敗する。EmitNextMatch→FetchNextProbeがspill_errorを設定してfalseを返し、NextBatchは同じ呼び出し内でFailWithせず0行(またはそれまでに取得した行)を返す。標準ドレインパターン(executor/relational.cpp:5459-5466のwhile(NextBatch!=0)後にGetStatus()確認)は最初の0でループを抜け、ステータスはkSuccessのまま→結合結果の一部欠落を成功として返す。次回NextBatch呼び出し(hash_join.cpp:761-764)で初めてFailWithされるが、呼び出し側は0で停止するため到達しない。

**確認・対応記録:**

- AuditHashJoinReportsSpillReadErrorAtEof removes owned spill files after the first output row and checks immediate EOF status; passes after post-loop status propagation.

### high-11

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational.cpp:5235`

**指摘:** grouped-join fusion 発火時に HAVING の集約比較を INT64→double 変換で行うため、|値| ≥ 2^53 で丸めにより誤った行が出力/欠落する。 fusion 未発火時の正系(TryBinary による型忠実比較)と結果が乖離する。

**発生条件:** SELECT k, SUM(v) FROM t GROUP BY k HAVING SUM(v) > 9007199254740992 のように融合が発火する形状で、合計 9007199254740993 (=2^53+1) のグループは double 側で 9007199254740992 に丸められ `>` が偽となり行が欠落する(真値比較では真)。kEquals/kNotEquals でも同様に崩れる。

**確認・対応記録:**

- Integer HAVING threshold above 2^53 was rounded by fusion. Keep INT64 threshold in checked predicate path; IntegerHavingKeepsValuesBeyondDoublePrecision failed before and passes after.

### high-12

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `index/lsm_index.cpp:67`

**指摘:** 等値ポイント(exact_point)スナップショットスキャンとGetValue()が、LSMのライブ値を経由して他トランザクションの未コミット状態を、そのまま読む(dity read)。LsmIndexIterator::Value()はstable_leaf_cache_無しのときPageManager::LsmReadでライブ値を取得し、kNotExistsのときは8ゼロバイト(count=0の空リスト)を返す。LsmRead先のLookupLiveLocked(page/page_manager.cpp:110-146)はmemtableのuncommitted_txnを一切フィルタせず、スキャン用のLsmScanRange(page/page_manager.cpp:571-583)が正しくフィルタしているのと対照的。IndexScanIteratorのコンストラクタ(index/index_scan_iterator.cpp:189-196)はexact_point(begin_keyが全キー列幅かつbegin==end)のとき意図的にstable_leaf_cache_を有効化しないため、UpdateIteratorState(index/index_scan_iterator.cpp:299-303)がスナップショット値entries_[idx_].secondではなくライブ値をデコードする。これはヘッダの明示契約「Plain scans keep the default (pure snapshot read)」(index/index_scan_iterator.hpp:41-43)の違反。

**発生条件:** (1) トランザクションWが同一キーの一括更新(INCLUDE列書き換え)を実行：Table::IndexInsertGuardedがkDuplicates→existing.pos==pos→bpt.Update→LsmUpdateでmemtableに未コミットエントリを書き(table/table.cpp:1063-1071)、直後にインデックス書き込みゲートを解放(table/table.cpp:975)。(2) その裏でスナップショットRがSELECT ... WHERE k=<該当キー>を実行：optimizerが等値シーク→exact_pointスキャンが生成され(executor/relational_factory.cpp:1041-1047のprobe経路でも同じ)、UpdateIteratorStateがライブ値を読みRはWの未コミットinclude/posを得る。(3) 別系統としてWがDELETE k=<該当キー>を未コミットのまま実行した場合：Rのスキャンentries_にはコミット済みエントリが正しく残る(LsmScanRangeがWのtombstoneをフィルタ)のに、Value()のLsmReadがkNotExists→8ゼロバイト(lsm_index.cpp:74-76)→DecodeIndexValueが失敗→status_にエラーを載せてClear()し(index_scan_iterator.cpp:309-315)、コミット済みで見えるはずの行でスキャン全体がstickyエラー終了する(截断されたpluck/結果欠落)。単体テストレベルでもindex_scan_iterator_test.cpp:1294(Value(7),Value(7)等値直接構築)や:422/491(GetValue())でこの経路は実行されている。

**確認・対応記録:**

- Point scan now pins the stable LSM leaf image. AuditPointLookupKeepsSnapshotIndexValue failed before and passes after.

### high-13

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `page/page_manager.cpp:583`

**指摘:** LsmScanRange の foreign uncommitted フィルタ(page_manager.cpp:571-582)で、「他トランザクションの未コミット・非削除 memtable エントリ」が「既に live な committed 値を持つキー」に当たった場合のみ continue せず fall-through し、583 行の無条件代入 `merged[key] = {未コミット値, ...}` が committed 値を上書きする。読み取り側にダーティリード(誤結果)を返す。

**発生条件:** キー K の committed インデックスエントリ v1 がある状態で T2 が LsmUpdate(K, v2)(memtable[K]={v2, deleted=false, uncommitted_txn=T2}、page_manager.cpp:449-454)。別のリーダ T3 が LsmIndexIterator 経由でスキャン(index/lsm_index.cpp:46-47 と 160 が reader_txn_id=txn.ID() を渡すことを確認)→ foreign_uncommitted=true・deleted=false・merged[K]={v1,false} 存在、の唯一の fall-through 経路を通り 583 行で merged[K]={v2,false} になり、T3 は未コミットの v2 をスキャン結果として得る。table/table_test.cpp:1589-1600 の既存テストは「committed 値が無い場合」しかカバーしておらず本経路は未検証。

**確認・対応記録:**

- Foreign pending range updates no longer replace the committed baseline. AuditForeignUpdateCannotReplaceCommittedRangeValue failed before and passes after.

### high-14

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `page/page_manager.cpp:890`

**指摘:** ApplyTransactionAbort の kUpdateRow アンドゥも `(void)page->body.row_page.UpdateRow(r.slot, r.undo_data)` と戻り値を捨てている。UPDATE が行を縮めていた場合、アンドゥ(旧=更大的値)は FreeSize 不足で kNoSpace を返し、縮小後の(アボート済み UPDATE の)新値がスロットに残る。つまりアボートした書き込みが確定値として残る(誤結果)。

**発生条件:** T1 が行を 100B→10B に UPDATE(row_page.cpp:171 で即 Physical 反映、空き 90B がグローバルに解放。hole_sizes_[slot]=0 のため他 txn の Insert は subtract 対象にせず消費できる)。T2 の INSERT がその 90B を消費した後 T1 が Abort → UpdateRow(slot, 100B の undo_data) が row_page.cpp:201-203 の `record.size() > prev_size && FreeSize() < 差分` で kNoSpace → 890 行の (void) で握り消され、10B の新値が残る。

**確認・対応記録:**

- Rollback restores the prior row image without the new-write admission budget. AuditAbortRestoresRowsAfterFreedSpaceIsReused reproduces shrink/reuse/abort and passes after.

### high-15

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `page/page_manager.cpp:905`

**指摘:** ApplyTransactionAbort の kDeleteRow アンドゥが `(void)page->body.row_page.InsertRowAt(r.slot, r.undo_data)` と戻り値を捨てており、InsertRowAt が kNoSpace を返すと削除前のコミット済み行が物理復元されない。その後のスナップショットからその行は「削除済み」に見え、abort したはずの削除が実質確定する(データ欠落)。

**発生条件:** ブロック(予算 kBlockBudgetBytes = kPageBodySize-24 ≈32KB、row_page.hpp:41 / constants.hpp:32)内で T1 が 8KB 行を削除(RowPage::Delete が即 Physical に DeleteRow、row_page.cpp:235)。空きは FreeSize() に計上され、T2 の「行を 8KB に伸ばす UPDATE」は空き判定で全ホールを空きと見なすため(row_page.cpp:158-161、UpdateRow の再確認 201-203 も同様。locked_hole_bytes を減算しない)その予約分を消費できる。その後 T1 が Abort(通常終了経路: transaction_manager.cpp:676/696、transaction.cpp:84/250 から ApplyTransactionAbort が実行される)すると、アンドゥの InsertRowAt が row_page.cpp:136-139 の予算チェックで kNoSpace → 905 行の (void) で握り消され、行は復元されない。

**確認・対応記録:**

- Same rollback repair as high-14; delete/reuse/abort case covered by the same regression.

### high-16

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/cascades.cpp:4903`

**指摘:** IsSameTable(884-910行)が `_<数字>` 接尾辞を剥がすエイリアス正規化を行うため、実在する別テーブル同士(orders と orders_1、自己結合の自動別名 orders_1/orders_2)が「同じテーブル」と判定される。outer_to_inner_join_on_null_rejecting_filter はこの判定経由で null-rejection を null供給側に誤認し、LEFT JOIN を INNER JOIN へ変換してマッチしない保護側行を落とす(データ欠落)。

**発生条件:** FROM orders LEFT JOIN orders_1 ON orders.k = orders_1.k WHERE orders.amount > 0 (orders_1 は実在の別テーブル/自動別名)。`orders.amount > 0` は orders_1 側とは無関係だが、ExpressionRejectsNullsOnRelations(plan/cascades.cpp:1992-2002) のマッチ述語が IsSameTable("orders_1","orders")=true を返し、IsNullStrictWhere(expression/rewrite.cpp:4230-4232 の kColumnValue が match_column を直接評価)で右側関係の null rejection と誤認 → 4902-4916行で INNER JOIN 変換。元プランは NULLパディング行も残すが変換後は落される。スキャンフィルタ押し下げでは中和されない(amount>0 の押し下げは本来正しい)。

**確認・対応記録:**

- Semantic outer-join null proofs now require exact relation identity rather than suffix-stripped heuristic equality. SQL examples already passed before; direct rule regression pending.
- AuditOuterJoinProofKeepsSuffixedRelationsDistinct verifies isolated Cascades rule creates no inner alternative for left-side predicate; passes.

### high-17

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:3243`

**指摘:** OVER (w ORDER BY ...) のように名前付きウィンドウを参照しつつ句を追加するインライン参照で、参照先 w の PARTITION BY / フレーム定義が無言で捨てられる。spec が Identifier だけの裸参照(3225-3242)のみ t_named_windows から継承し、Identifier と他句が混在する場合は else 分岐で ParseWindowSpecification(3100-3149, Identifier を一切読まない)だけを適用する。WINDOW 句の定義側(6438-6459)は正しく継承するため、呼び出し側だけが継承を欠く。

**発生条件:** SELECT SUM(x) OVER (w ORDER BY y) FROM t WINDOW w AS (PARTITION BY k); — 正しくは k ごとのパーティション内で y 順に評価されるべきが、パーティションが消えた全行単位のウィンドウ計算になり誤結果を出す(エラーも出ない)。

**確認・対応記録:**

- NamedWindowExtensionsKeepPartition failed before; ParseWindowSpecification now resolves inherited named window. Passes after.

### high-18

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:6656`

**指摘:** SELECT AS STRUCT t.* が射影をハードコードの {"foo","bar"} 2列で無置換に置き換える。コンプライアンス-corpus の StructValueTable(foo INT64, bar STRING) 専用の実装で、それ以外の表では存在しない列を参照してエラーになるか、たまたま foo/bar を持つ表では残りの列が静かに欠落する(誤結果)。同型のハックが sql_udf(...).* → {"aarr","acount","amin","amax"} (6503-6519) にもある。

**発生条件:** SELECT AS STRUCT s.* FROM other_table s; (other_table が a,b,c 列) → 射影が s.foo / s.bar に化け、列解決エラーまたは foo/bar 2列のみの誤った結果。c 列のデータが結果から欠落する。

**確認・対応記録:**

- Both hardcoded field expansions removed. FunctionStructStarUsesDeclaredFields tests dynamic UDF constructor and constant STRUCT; all nine SQL audit regressions pass.

### high-19

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/select_rewrites.cpp:2717`

**指摘:** M5派生表フラット化で、derived source が index>0 かつ join_type が kCross/kInner の場合、それより後の source の join 種を全く検査していない。join チェーン上位に FULL/RIGHT JOIN があると、派生表の内側 WHERE が(ON ではなく)外側 WHERE にマージされ(3062-3074)、結合後のフィルタになるため NULL パディング行が失われる。index==0 分岐には全 source の kLeft/kRight/kFull 検査(2743-2750)があり、直前のコメント(2711-2714)も「Chains beyond two relations only qualify when every join is inner」と明言しており、index>0 経路だけ実装が欠落している。

**発生条件:** SELECT ... FROM t0, (SELECT a, b FROM t WHERE b > 5) AS d FULL OUTER JOIN t2 ON t2.k = d.a。元の問い合わせでは d が先にフィルタ済みのため、フィルタ後の d に一致しない t2 の行は NULL パディングで生存する。書換後は WHERE b>5 が FULL JOIN の後に適用され、t2 が「フィルタに落ちるペア」とだけ一致する場合その一致が WHERE で落ち、NULL パディングも発生しない(NULL 供給は結合時に決まるため) → t2 の行が静かに欠落する。PrepareStatement の書換チェーン(query/sql_engine.cpp:5252)は全 SELECT に無条件で走るため常時到達可能。上位結合が RIGHT JOIN の場合も同様に欠落する。

**確認・対応記録:**

- Decline derived flattening across a later outer join; DerivedFilterStaysBelowLaterOuterJoin failed for FULL/RIGHT before and passes after.

### high-20

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `query/sql_engine.cpp:2641`

**指摘:** ExecuteStructFieldUpdate(UPDATE SET s.f=... のdotted STRUCT/PROTOパス)がフルスキャンをIsValid()だけで回し、ループ後のGetStatus()を検査しない。スキャンが途中で失敗すると、取得できた接頭辞の行だけを更新して成功を返す

**発生条件:** UPDATE t SET s.x = 1 WHERE s.y = 2 実行中にスキャンが失敗(full_scan_iterator.cpp:150/190 のTableLease取得失敗、402/429行のページ読み込みエラー、522行のkCorrupt、547行のtail-page待ち失敗でstatus_が設定されIsValid()がfalseになる)と、マッチ行の一部のみ更新され、行数も正しい値のまま正常完了する

**確認・対応記録:**

- Struct field UPDATE checks source.GetStatus before any queued mutation. Fault injection regression pending.

### high-21

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_engine.cpp:4673`

**指摘:** DecorateDmlWhereの非相関EXISTS畳み込みが、サブクエリ実行のmid-drainエラーを「0行存在」扱いにして飲み込む。スカラー(IN 4774行/スカラー4707行/量化比較4603行)の各パスはGetStatus()!=kSuccessでレガシー経路にフォールバックするのに、EXISTSだけは失敗をFALSEに畳む

**発生条件:** DELETE FROM t WHERE NOT EXISTS (SELECT ... 集約やWHERE内でオーバーフロー/読み込みエラーを起こすサブクエリ) を実行。準備は成功し、Next()がエラーでfalseを返すと any=false → NOT EXISTSがTRUEに畳まれ、tの全行が削除される(エラーは報告されない)。EXISTSの場合は逆に何も削除されず成功扱い

**確認・対応記録:**

- DML EXISTS folds only after successful child drain; DmlExistsCannotSwallowEvaluationFailure failed before and passes after with table unchanged.

### high-22

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_template.cpp:458`

**指摘:** テンプレート再バインドの CTE 順序拒否ゲートが穴だらけ。ContainsBindableConstant の kAggregateExp は Child しか見ず(458-460)、SelectHasBindableConstant(380-439)は UnionAll()/SetOperationTree() を全く走査しない。一方 BindExpression は WhereFilter/SecondaryArg/TrailingArgs/Having修飾子/InnerOrderBy/InnerGroupBy をバインドし(211-268)、BindSelect は UNION ALL 分岐をバインドする(722-735)。その結果、リテラルがこれらの場所だけにある 2 CTE 以上の文はゲートを素通しし、CTE 本体が名前ソート順(580)でバインドされる一方、パラメータはテキスト順に抽出済みなので、キャッシュ再生時にパラメータが静かに入れ替わる。

**発生条件:** WITH b AS (SELECT sum(x) FROM t WHERE k = 5), a AS (SELECT sum(z) FROM u WHERE m = 6) SELECT * FROM b, a; を同一プロセスで2回実行(テンプレートキャッシュヒット)。1回目(素パース)は k=5, m=6。2回目はゲートが拒否できず a を先にバインドするため m=5, k=6 となり、問い合わせ結果が1回目と静かに変わる。SELECT 1 UNION ALL SELECT 2 形式の CTE 本体でも同様(分岐内リテラルが走査されない)。

**確認・対応記録:**

- Template bindable-constant detection includes set-op branches and aggregate children. Multi-CTE branch literals disable ambiguous map-order replay; CteUnionLiteralsKeepTextOrderOnTemplateReplay passes after reproducing failure.

### high-23

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `server/postgres_server.cpp:277`

**指摘:** read_worker_threads に 0 以外の明示値を渡した場合だけ 16 の上限が適用されず、固定 16 要素のワーカー配列(std::array<..., 16>)を 16 以上のインデックスで参照する範囲外アクセス(UB)になる。

**発生条件:** tinylamb_server <db> --read-workers 17 で起動(postgres_server_main.cpp:79-86 は 0 と 1024 超だけ拒否し 17〜1024 を正当値として受理)。Impl コンストラクタ(postgres_server.cpp:277-283)は 0 のときのみ hardware_concurrency を kMaxReadWorkers=16 でキャップし、明示値は無検証のまま read_worker_count_ に設定。StartReadWorkers(:1733-1741)が 17 本のスレッドを生成すると、17 本目の ReadWorkerLoop(worker=16)が待機で read_task_ready_[16](std::array<std::condition_variable,16>、:2337)を、タスク取得で read_tasks_[16](:2352)/read_worker_running_[16](:2353)を直接参照する。EnqueueSqlTask(:1624-1639)でも client_id % 17 == 16 のとき read_tasks_[preferred] が範囲外になる。std::array::operator[] の範囲外はデバッグビルドでアサート、リリースビルドで隣接メンバ(read_completions_ 等)の破壊/SEGV。

**確認・対応記録:**

- AuditExplicitWorkerCountRespectsBucketCapacity ran 24 clients at configured 17 and 1024 workers; both pass with completion bucket bound.

### high-24

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:716`

**指摘:** 非int64列(varchar/double)へのkIsNull述語がchunkアーティファクト上で「全行パス」扱いになる。ConjunctSupportedはkIsNullを列型を問わず支持し(fragment.cpp:80-83)、residualに載らないため、NeededColumnsがint64列しか持たない(394-401)ことと合わさってci<0→continueで述語が無視され、残差0件でCoverage::kFullを主張する。fragment.hpp:277-280の契約ではexecutorはresidual_conjunctsのみ再適用するので、`varchar列 IS NULL`が全行返る。

**発生条件:** Execute(chunkアーティファクト, conjunct={kind=kIsNull, col=varchar列})をFullScan::ExecuteLSTableFragment経由で呼ぶと全行がrows_proved_passで返る。run/memスナップショット経路ではdecoded[c.col].IsNull()が正しく評価される(fragment.cpp:852-857, 964-969)ため、アーティファクト種別で結果が変わる。Estimateも同様にkFullを報告(fragment.cpp:1525-1530, 1620-1622)。

**確認・対応記録:**

- Noninteger IS NULL kept as residual for shared fragment kernel; artifact comparison failed before and passes after.

### high-25

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:732`

**指摘:** ScanChunkのint64比較評価(kCompare/kIn)がNULLフラグを無視する。NULLセルはraw 0にデコードされるため(col=0 / IN(0) / !=5 / >=0 などで)NULL行がフィルタを通る。同一フィルタがScanRun(fragment.cpp:859-876)とScanMemSnapshot(971-974)ではNULL行を落とすため、アーティファクト種別で結果が変わる誤結果バグ。

**発生条件:** NULL許可int64列にNULLを含むテーブルのLSTableStoreサンプルchunk(lstable_bridge.cpp:981-983がNULL非キーセルをPutする)に対し、FullScan::ExecuteLSTableFragment(executor/full_scan.cpp:168→LSTableStore::ExecuteFragment→Execute)でConjunctKind::kCompare(int64_literal=0, kEqまたはkGe)やkIn(0を含む)を評価すると、NULL行がrows_proved_passに数えて返る。SQL意味論ではNULLは比較を満たさない。

**確認・対応記録:**

- Integer chunk comparison/IN now rejects NULL cells; three-artifact regression failed before and passes after.

### high-26

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:797`

**指摘:** ScanRunのkKeyRange上限が閉区間の境界キーを落とす。コメント(792-794)は「hi_key + 1 を探す」と述べるが、実装はそのままLowerBound(hi_key)で、key == range_hiの行がちょうどその位置に現れるため`i < end`(800)で除外される。fragment.hpp:109は「closed interval」と契約。さらにrange_hi未指定の場合もhi_key=key(INT64_MAX)となり、key == INT64_MAXの行が必ず欠落する。ScanRunのループにはScanChunk(682-692)と違いper-rowの範囲再フィルタがなく、begin/endが唯一のフィルタなので欠落が確定する。

**発生条件:** runアーティファクトを含むArtifactSetでaccess=kKeyRange, range_hi=NのExecuteを呼ぶと、キーNの行が返らない(INT64_MAXを上限とする場合range_hi省略でも同様)。同一範囲をchunkアーティファクトに適用した場合は685-692の行フィルタがkey>hiのみfailするためキーNが返り、パス間で非対称。本番配線はchunk単一(lstable_bridge.cpp:302-308)だが、IRの文書化されたrun結合・テスト/ファズ構築で到達可能。

**確認・対応記録:**

- Run upper bound uses hi+1 unless unbounded/INT64_MAX; memory access ranges also enforced. Three-artifact closed/open-high regression failed before and passes after.

### high-27

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_codecs.cpp:5115`

**指摘:** DecodeBp128Block の AVX2/BMI2 非搭載ビルド用 wide-frame フォールバック経路で、block width が 64 のとき (UINT64_C(1) << 64) - 1U が UB。x86-64 ではシフト数が mod 64 で畳まれて mask==0 になり、そのブロックの全行が minimum + 0(=ブロック最小値)として「黙って」デコードされる(UBであり、最適化前提も崩れる)。AVX2 搭載ビルドでは width==64 が DecodeBp128Block64 (5050-5053行) に振り分けられるため正当だが、同リポジトリは直近で macOS/ARM をサポートしており(commit e58bf00/d56d149。Apple Silicon には AVX2 が無い)、その構成ではこの経路が生きる。到達性は実在する: ParseBp128Region は width<=64 を許容し(codecs.cpp:4964-4970、Bp128MetadataMatches も width 64 を拒否しない—scan.cpp:8144 の ValidateWalk case 40 も Parse+MetadataMatches だけ)、エンコーダ EncBp128I64 は widths[block]=BitsNeeded(max-min) を上限 64 でそのまま出力する(verify 済み: pax_chunk_encode.cpp:2077-2078。ブロックが [INT64_MIN, INT64_MAX] を跨げば 64 が必要)。影響は kBp128 の一括デコード(DecodeCascadeI64All kBp128 arm → DecodeBp128All → DecodeBp128Block、access.cpp:3128-3136)に留まらず、kFastPfor/kDeltaFastPfor の一括デコードも同一カーネルをレーン展開に再利用する(codecs.cpp:5338-5349, 5583-5590 で leaf.width=blk.w<=64 を設定)。ポイントリード(Bp128ValueAt=UnpackBitAt)は正しいため、非AVX2ビルドで「スキャン結果だけが静かに崩れる」誤結果(high)。本疑読では実行・再現は未実施(読み取り専用)。x86 の 1<<64→0 挙動は標準的な x86-64 セマンティクスに基づく分析であることを明記する。

**発生条件:** 128行ブロックの最小 INT64_MIN・最大 INT64_MAX を含む int64 列を kBp128(kFastPfor/kDeltaFastPfor も同様)で符号化し、AVX2/BMI2 なしのビルド(Apple Silicon macOS 等)で DecodeInt64ColumnRg/VectorizedScan/ScanAggregate の一括デコードに流す。masked shift により mask==0 となり、スキャン集計・フィルタがブロック最小値の列に対して評価され、誤った SUM/COUNT/フィルタ結果をエラーなしに返す。

**確認・対応記録:**

- Normal AVX2 build did not reproduce. Scalar -mno-avx2 -mno-bmi2 UBSan build trapped on shift by 64 before; full-width mask special case passes after. /tmp/audit-pax-scalar-before and after logs.

### high-28

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/interval.cpp:543`

**指摘:** 単位範囲付きINTERVAL複合パース("hour to second"等)の H:M:S 分枝で、th/tm の int64 演算 (th*3600)+(tm*60) が無検査で、巨大な時フィールドで符号付きオーバーフローUBになる。さらに結果×1e9 の double→int64 キャストも範囲外UBになり得る。ts(秒)だけは538行で9.2e9にクランプ検査されているが th/tm は無検査。同ファイルの非単位系複合パス(632-645行)は checked_mul/checked_add と ts 範囲検査で修正済みで、この経路だけ修正漏れ。

**発生条件:** IntervalValue::TryParse("3000000000000000:0:0", "hour to second") (SQLの INTERVAL '3000000000000000:0:0' HOUR TO SECOND)。sscanf(530行)で th=3e15 が合法的に読め、538行の検査を素通りし、543行の th*3600=1.08e19 が int64_max 超えでUB。リリースビルドではラップしたゴミ nanos がエラー無しで返る(誤結果)、UBSanではクラッシュ。iv.nanos += 自体も加算検査なし。

**確認・対応記録:**

- Original one-part example was already rejected. Reachable three-part qualified time reproduced signed overflow under UBSan; checked arithmetic and final range validation pass after.

### high-29

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/value.cpp:1287`

**指摘:** interval型テキストと同形のVARCHARに対し、述語評価系の順序(Value::TryLess/LessSlow: interval フィールド順)が、ORDER BYの順序(CompareForOrderBy→CompareVarCharForOrderBy: バイト順、collation.cpp:113)およびmemcomparableインデックスキーの順序(EncodeMemcomparableFormatVarchar: バイト順、value.cpp:802-825)と矛盾する。エンジン自身のfuzzオラクル(value_fuzzer.cpp:213-241)が「memcomparable符号順はSQLの < 順に従う(インデックスキー契約)」と assert しており、このペアで assert が落る=層内の契約違反。

**発生条件:** VARCHAR列に '2-0 0 0:0:0'(24ヶ月) と '10-0 0 0:0:0'(120ヶ月) を格納。TryLess('2-0…','10-0…')は両者がLooksLikeIntervalTextを満たし両パース成功のため true(24<120 フィールド順)を返すが、エンコード順とORDER BYは先頭バイト '2'>'1' で逆順。インデックスレンジスキャンの境界と述語 `<` の判定が食い違い、再検査がなければ行の取りこぼし/過剰取得(誤結果・データ欠落)。フィールド順とバイト順が一致しない全てのinterval形テキスト対で発生。

**確認・対応記録:**

- Ordinary VARCHAR no longer receives interval equality/hash/order by spelling. Explicit INTERVAL tag 4 is retained by column metadata, row decoding, boxed batches, self-describing Values and array elements; interval index tag 7 orders (months, days, nanos). Typed VARCHAR/PAX kernels decline interval domains. Value, row, chunk and SQL regressions pass batch64 tests2, including string DISTINCT/range before/after index and real interval ORDER BY/index predicates. Full rebuild/sanitizer follow-up pending.
- Batch66: value/row/column/schema/chunk/spill/AST/bytecode/SQL audit regressions pass under ASan+UBSan; explicit interval metadata/key ordering survives row and generic spill serialization. Batch67 corrects old string-vs-interval test pins and preserves CTAS metadata.

### medium-1

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `ARCHITECTURE.md:116`

**指摘:** 「§5 現在許容している境界違反」の台帳が陳腐化している。見出しは「計 65 辺」だが直後の内訳表は合計 61 辺で自己矛盾し、さらに実態は 78 辺ある。4項目の個別数も実態と不一致(relational→sql 12→実21、relational(detail)→executor 9→実15、index→table 3→実4、plan→sql 13→実14)。

**発生条件:** 負債返済の進捗管理やPRレビューでこの台帳の辺数を根拠に「返済済み/未返済」を判断すると、台帳にない13辺分(主に executor/detail 配下の statement.hpp 参照増)が見えなくなり、返済判断や AGENTS.md:88 の「must exit 0」検証の前提が崩れる。

**確認・対応記録:**

- Recounted layering allowlist (78); corrected total and group breakdown. check_layering passes.

### medium-2

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `common/README.md:18`

**指摘:** common/README.mdの「Key Components」がRingBuffer(18行目)・VMCache(20行目)・converter.hpp(23行目)・debug.hpp(24行目)を実在コンポーネントとして説明しているが、これらはリポジトリのどこにも存在しない。

**発生条件:** READMEを読んだ開発者が「VMCacheがファイル領域をメモリマップしメモリ制約下で必須」「SPSCリングバッファが非同期通信に使われている」ことを前提に設計・レビューすると、実装はPageManager直載せ(Root AGENTS.md §2.2「PagePool is retired; pages live in RAM under PageManager」)であり前提が崩れる。存在しないヘッダをincludeしようとすればビルドが即落ちる。

**確認・対応記録:**

- Replaced common README inventory with files present in tree; retired names identified.

### medium-3

**判定:** 説明を訂正＋関連テスト成功 (`docs-corrected-tested`)

**元の対象:** `common/crc32c.hpp:368`

**指摘:** Crc32cクラスのFinalize()/Crc32cOf()は標準CRC-32Cではなく「最終XOR前の生レジスタ値」を返す。docs/page_format.md:28はPAXフッターのcrc32cフィールドを「Whole-payload CRC-32C integrity check」と文書化しているが、実際のディスク上の値は CRC-32C XOR 0xFFFFFFFF(1の補数)であり、記載されたCRC-32Cそのものではない。

**発生条件:** PAXチャンク(フォーマットv2)を書き出すと、フッターのcrc_data/crc_footerにはtable/pax_chunk/pax_chunk_scan.cpp:3350-3354でCrc32cOfの生値が格納される。エンジン内部の検証(同ファイル:9030-9040)もCrc32cOfで自己整合するためDB動作は壊れないが、外部ツールや別実装(zlib/各言語のcrc32c等)でドキュメントどおりCRC-32C検証すると全チャンクが「破損」判定になる。また将来の呼び出し元がCrc32cOfとCrc32C(common/crc32c.hpp:343-345、標準形)を混在させると誤った破損検出/通過が起きる。

**確認・対応記録:**

- Crc32cOf raw register applies to PAX chunks; PageManager snapshot uses standard Crc32C. Corrected both docs separately; known vector/incremental regression passes (5 CRC tests). Original report conflated the formats.

### medium-4

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/AGENTS.md:16`

**指摘:** docs/AGENTS.md:16-17 と ARCHITECTURE.md:5-6 が「docs/adr/ ディレクトリは実在しない」と明記しているが、docs/adr/0001-fixed-wal-logical-end.md が実在する(Status: accepted, 2026-10-30, 採用記録C34付きの65行ADR)。受け入れ済みADRが両インデックス文書の記述上は「存在しない」ことになり、ルートAGENTS.md:138-140の「Check ADRs: docs/review/, docs/adr-proposal.md」経路からも漏れる。

**発生条件:** ルートAGENTS.md §4の手順に従って docs/review/ + adr-proposal.md でADRを確認する変更作業では、固定長WAL終端スロット(v6レコード維持・--force挙動・.log.meta version=2)に関する採用済み決定を参照できず、WALフォーマット変更がADRと無関係に進む恐れ。

**確認・対応記録:**

- Linked accepted docs/adr/0001-fixed-wal-logical-end.md from root and docs architecture indexes.

### medium-5

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/design.md:392`

**指摘:** design.md D10「LSM run の再起動復元」(状態: 実装完了)はindex/lsm_tree.cppのディレクトリ走査復元・BlobFileとSortedRunの対応・LSMTreeTest.Reopen*を受け入れ条件とするが、index/lsm_tree.cppは存在せず、LSMTreeTestはリポジトリにゼロヒット。index/lsm_index.hppはB+Treeラッパ(lsm_index.hpp:82-84, 121-122のusing BPlusTree = LsmIndex)でtable/memtableのMemTableもtable/sorted_runのSortedRunも使わない。現行ツリーにはrunを永続化する者はなく(唯一の本番Sync呼び出しlstable_bridge.cpp:1009-1028はメモリ内サンプルchunk構築のみ)、「flush済みデータを再起動後にも返す」というD10の不変条件は現行実装では満たされない。

**発生条件:** docs/design.mdのLSM節(D10)を根拠に再起動復元やrun永続化の動作を期待すると実装と不一致。実態はlstable_bridgeのLSTableStoreがメモリ内サンプルchunkをLayoutGenerationタグ付きで保持し(lstable_bridge.cpp:292-309)、Sync出力blobの永続化者は存在しない。

**確認・対応記録:**

- D10 directory-run reopen mechanism retired. Documented current PageManager snapshot+WAL persistence; missing legacy mechanism does not mean committed DB data is unpersisted.

### medium-6

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/design.md:66`

**指摘:** design.md「実装状況(2026-09-02 記録)」のD決定↔テスト対応表が陳腐化し、D2/D9/D11 の受け入れテストが現行ツリーに存在しない。特に D9 の `RecoveryManagerTest.MidRecordBitFlip*` / `MixedLegacyAndCurrentVersionLogsScanCleanly` / `LegacyV1DestroyRecordStillParses`、D2 の `*ImplOperationsAreIdempotent*`・`BPlusTreeTest.NodeReclaimWALReplaysAfterCrash`、D11 の `BPlusTreeTest.LiftUpBranchOrphansSiblingRows`(有効化済みと記載)・`RandomChurnTenThousandOpsKeepsStructure` はリポジトリ全体から 0 件。LSM移行(コミット 405ff4f)で b_plus_tree ファイル群とリカバリ系テストが削除されたまま design.md が未更新。

**発生条件:** WAL CRC・クラッシュリカバリ冪等性・B+Tree lift-up 周りの改修時に design.md の対応表で受け入れテストを特定して再実行しようとすると該当テストがビルド対象に存在せず、design.md:46「D1〜D11 の受け入れ条件はすべて対応テスト付きで実装済み」の検証が不能。実際 recovery/recovery_manager_test.cpp は 1 件のみ(39行目 LogRecordTest.RoundTripSerialization)。

**確認・対応記録:**

- Historical test claims explicitly historical; replaced current test inventory with existing sources/names.

### medium-7

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/environment_variables.md:14`

**指摘:** TINYLAMB_PAGE_POOL_BYTES(:14)とTINYLAMB_CHECKPOINT_SECONDS(:16)を「Retired no-op compatibility variable」と説明するが、両方とも現行コードが読んで動作に影響する実変数。

**発生条件:** PAGE_POOL_BYTESは database/page_storage.cpp:39-46 の PagePoolCapacityFromEnv() が PageManager::Create の容量に渡す(8388608等を設定すると実際にプール容量が変わる。database/catalog_test.cpp:343 が setenv で検証している)。CHECKPOINT_SECONDSは page_storage.cpp:83-91 が checkpoint_interval として CheckpointManager に渡し、recovery/checkpoint_manager.cpp:46(interval_seconds_ != 0 でスレッド起動)/:84(周期判定)の周期チェックポイントタイマを駆動する。ドキュメントを信じて「no-opだから何を設定しても安全」と考えてベンチ/障害試験をすると、実際は周期チェックポイントやページプール容量が変わり計測が汚れる。

**確認・対応記録:**

- PAGE_POOL_BYTES active reservation hint and CHECKPOINT_SECONDS active timer documented.

### medium-8

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/environment_variables.md:3`

**指摘:** 冒頭で「Every TINYLAMB_* environment variable read by the code, in one place」と宣言しながら、実コードに一切存在しない13個の変数を実在するknobとして載せている。TINYLAMB_CHECKPOINT_FLUSH_MS(:17), WAL_RETENTION_WARN_BYTES(:19), WAL_FREE_WARN_BYTES(:20), RECOVERY_TRACE(:21), RECOVERY_WORKERS(:22), CORRUPT_TAIL_PROBE(:23), CRC_BITFLIP_PROBE(:24), DISABLE_READBUF_NOINIT(:25), TEST_UNDO_STOP_AFTER(:36), DISABLE_ANCHOR_VOID(:37), DISABLE_SEPARATOR_SWEEP(:38), DISABLE_REDO_REINSERT_FLOOR(:39), BPT_CONC_FUZZ_SEED(:144)。

**発生条件:** リカバリ挙動を調べる開発者が TINYLAMB_RECOVERY_WORKERS=4 を設定して並列リカバリを期待しても、コードは読んでおらず無視される。障害調査中に「設定したのに効かない」原因をenv表からは追えない。

**確認・対応記録:**

- Removed 13 retired knobs from active table, retained explicitly retired list.

### medium-9

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/environment_variables.md:3`

**指摘:** 逆方向: コードが読んでいるTINYLAMB_*変数のうち約30個がenv表に未記載。PAX系の TINYLAMB_NO_CRC/NO_HASH_IDX/NO_INV_IDX/NO_XOR8/SMALL_HASH_IDX(table/pax_chunk/pax_chunk_scan.cpp:3132/2338/2315/2316/2340)・TINYLAMB_FORCE_PLAIN(pax_chunk_encode.cpp:27)・FORCE_SCHEME(pax_chunk.hpp:713)、executorのハッチ TINYLAMB_DISABLE_ARENA_MEMBERSHIP(executor/detail/scan_filter.cpp:873)・DISABLE_BUILD_RESIDUAL_HOIST/DISABLE_BULK_COUNT/DISABLE_COUNT_STAR_FUSION(executor/detail/fused_grouped_join.cpp:540/1061/688)・DISABLE_HAVING_FOLD(executor/relational.cpp:368)・DISABLE_PARALLEL_MEMBERSHIP(scan_filter.cpp:1108)・DISABLE_PK_ABSORPTION(executor/detail/build_input.cpp:1774)・DISABLE_STREAM_GROUPED_JOIN(executor/detail/stream_grouped_join.cpp:63)・DISABLE_TRANSITIVE_KEY_FILTER(build_input.cpp:1451)、フロントエンドの TINYLAMB_NEGATIVE_CACHE_TTL_SEC/PARSE_CACHE_SIZE/GOOGLESQL_BIN/GOOGLESQL_ZYGOTE_SOCK(query/googlesql_frontend.cpp:89/75/131/294)、TINYLAMB_JOIN_REGRESSION_DIR(query/sql_join_fuzzer_test.cpp:190)、TINYLAMB_DBG_CNT(plan/optimizer.cpp:258)、TINYLAMB_FUZZ_TRACE_SQL(query/sql_engine.cpp:220)、TINYLAMB_VALDBG(table/pax_chunk/pax_chunk_scan.cpp:5506)など。

**発生条件:** 「A/Bハッチは全部env表にある」という同docの自己宣言(§冒頭のDISABLE_* familyの説明)を信頼して回帰切り分けをすると、表に載っていないDISABLE_STREAM_GROUPED_JOINやDISABLE_PK_ABSORPTIONなど主要な高速経路の切替弁を見落とす。

**確認・対応記録:**

- Compared getenv/GetEnv/ReadEnv names against inventory and added missing 31 controls, including new concurrent fuzzer controls.

### medium-10

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/00-index.md:38`

**指摘:** executor参照doc(演算子網羅表+各章)が、コードに存在しない約20個のクラスを実在する演算子として詳細に解説している。00-index.md:39 ChunkedScan / :44 TidScan / :52 GenerateSeriesExecutor / :53 RemoteScan / :54 AnalyzeScan / :64 Max1RowExecutor / :67 CardinalityProbe / :68 ScheduledExecutor / :85 AsOfJoin / :116-117 TwoPhaseDistinctAgg(+Executor) / :118 GroupingSetsExecutor / :119 DistributedAggFinalize / :127 ExchangeExecutor / :128 PartitionScanExecutor / :139 TruncateExecutor / :140 MergeExecutor / :141 ReturningExecutor / :145 NumaArena・OperatorMemoryReservation・QueryScheduler。30-scan-source.md §11-13、40-unary-control.md §5/§7/§8、20-data-runtime.md §8/§10/§11、70-aggregation.md §7/§8、80-window-recursion-distribution.md §3-5、90-data-modification.md §5-7 に各挙動契約まで書かれている。

**発生条件:** 生成系列の行位置契約(30-scan-source.md §11「GenerateSeriesExecutorはRowPositionを書き換えない」)やAnalyzeScanの統計収集エラー取り込み(§13)を参照してコードを読んでも該当クラスが無い。実装はスカラー関数generate_series(expression/function_call_expression.cpp:4237)とUNNEST経路、GROUPING SETSはフロントエンド展開(query/googlesql_ast_visitor_dml.cpp:406 ExpandGroupingSetsWithWindows)、TRUNCATEはDELETE脱糖(query/googlesql_ast_visitor.cpp:8225-8239)、MERGEはSqlEngine::ExecuteMerge(query/sql_engine.cpp:2773)に存在するため、docの名前でリポジトリを検索すると実装を見失う。

**確認・対応記録:**

- Confirmed absent class names across executor sources; removed phantom operators from index and replaced sections with actual ownership/retired status. Also corrected missing Partial/FinalizeAggregate and DictionaryBatchAggregation classes.

### medium-11

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/60-sort-distinct-set.md:151`

**指摘:** 「現在のDistinctExecutorはハッシュ表を使わない。既出行を保存した配列を線形探索する。最悪O(ND)/全行相違でO(N²)」と記述するが、実装はstd::unordered_setベースのハッシュ集合。記述は削除済みの旧実装のままで、計算量・メモリ特性の記述が実際と逆。

**発生条件:** 大規模DISTINCTの性能見積りやO(N²)退行の障害調査でこの節を根拠にすると誤判定する(実際はハッシュ参照でO(N) 平均)。

**確認・対応記録:**

- DistinctExecutor uses unordered_set<Row>; documented expected O(N) and collision worst case.

### medium-12

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/70-aggregation.md:119`

**指摘:** §3の『型付き処理の判定は DISTINCT/FILTER/集約内ORDER BY のみを確認し、HAVING MAX/MIN と集約内LIMIT は確認しない。JIT SUM も同じ』『共通集約器に条件値を設定しないため全行除外され、空入力の最終値を返す』は現行コードと不一致。コードは NeedsGroupContext() で HAVING/内部LIMIT/内部ORDER BY/trailing/secondary を排除し、汎用集約器へは条件式を評価して input.condition に設定する。文書が記述する高速処理の誤割当てハザードは存在しない。

**発生条件:** HAVING MAX(x) / 内部LIMIT 付き基本集約がAggregationExecutorの高速経路に割り当てられると修飾が無視されると読んで計画・テストを設計すると、実際の動作(汎用集約器で条件値設定済み・正しくフィルタ)と食い違い、文書を根拠にした最適化/回帰判断を誤る。

**確認・対応記録:**

- Aggregation NeedsGroupContext gates and AggregateInput.condition assignment verified in source; replaced obsolete hazard description.

### medium-13

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/70-aggregation.md:127`

**指摘:** §3の『子が失敗した場合も、それまでの集約状態から 1 行を組み立てる。行が返ったことだけでは成功を判断できない』『JIT SUM はバッチが0行になった後に子の状態を確認せず、部分合計と正常値を区別できない』は現行コードと不一致。両経路とも子停止後に子のsticky状態を確認し、失敗時は行を組み立てず false を返す。

**発生条件:** ドキュメント記述を信じて『集約の行が返っても子失敗の可能性がある』前提の呼び出し側コードやテストを書くと、実際には失敗が sticky Status として伝播する現行動作とズレる(文書は修正済み動作を古いハザードとして説明し続ける)。

**確認・対応記録:**

- Scalar/JIT child status checks verified; documentation now states no partial successful output on failure.

### medium-14

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/70-aggregation.md:135`

**指摘:** 『ParallelAggregationExecutor は推定入力が 8192 行以上の場合に選択候補となる』は現行コードと不一致。実際の選択閾値は 32768 行であり、8192 は並列走査/結合の閾値。

**発生条件:** ドキュメント通りに動作境界を想定したレビュー・テスト・計画調整を行うと、32768〜8191行帯の扱いを誤る。

**確認・対応記録:**

- Threshold 32768 verified in plan/parallel_thresholds.hpp and factory uses.

### medium-15

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/70-aggregation.md:143`

**指摘:** §4の『列を直接参照する論理集約は型付き処理に分類されても、対応する分岐がないので結果が NULL になる』は現行不一致。コンストラクタは論理集約を型付きへ分類せず汎用へ回し、AccumulateValue/Merge が LOGICAL_AND/OR を実装済み。NULL結果ハザードは存在しない。

**発生条件:** 『並列集約では LOGICAL_AND(col) が NULL を返す』と想定したテストや計画上の制約を置くと現行動作(正しい論理集約結果)と矛盾する。同文中の『COUNTIF/ビット/配列/文字列等は汎用既定分岐で状態を更新しない』部分は AccumulateValue の default(no-op, 298-300行)と一致しており、論理集約の記述だけが古い。

**確認・対応記録:**

- Logical aggregates routed to implemented generic states; documented supported-type gate.

### medium-16

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor_todo.md:94`

**指摘:** executor_todo.mdの複数の[x]項目が存在しない/撤去済みの実装を「接続済み」と主張: :94-95 Max1RowPlan/Max1RowExecutor、:50 CardinalityProbe…ではなく:50はQueryScheduler、:449もQueryScheduler既存、:182 GroupingSetsExecutor、:433 MergeExecutor、:435 ReturningExecutor、:438 Truncate実行器、:454 NUMAアリーナ、:506 CardinalityProbe。Max1Row撤去は docs/optimizer_rule_audit.md:49「max1_row…物理登録を撤去」が正。

**発生条件:** 「既にあるもの(再実装しない)」節と[x]マークを信じて実装を参照すると該当クラスが無い。特に:433/:435/:438はMERGE/TRUNCATE/RETURNINGが機能として存在しないように読めるが、実際はMERGE=SqlEngine::ExecuteMerge(query/sql_engine.cpp:2773)、TRUNCATE=DELETE脱糖(query/googlesql_ast_visitor.cpp:8225)、RETURNING=ゲート済み未実装(query/googlesql_ast_visitor_dml.cpp:1623)であり、三者三様の実態と[ x ]表示が食い違う。

**確認・対応記録:**

- Cross-checked current LogicalOperator enum, rule registrations, classes and SQL routes. Corrected phantom Max1Row/Mark/Spool/Exchange/NUMA/CardinalityProbe/Returning/Merge classes, actual MERGE/TABLESAMPLE/UNNEST/set-op routes and PageManager counters. Pinned execute_query rejects SQL-standard FILTER syntax but parses AGG(x WHERE p). Changes: /tmp/patch-audit-todos.py (batch33).

### medium-17

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/opt/AGENTS.md:81`

**指摘:** 憲法「Ruleを1本も飛ばさない」(AGENTS.md §1.4, §11)に対し、現行登録のうち4本がdocs/opt全167ファイルのどこにも解説を持たない: 論理 constant_unnest_to_values / limit_prefix_ordered_window / limit_prefix_unordered_window(plan/cascades.cpp の Add(Rule( 登録。後2本は docs/optimizer_todo.md:644-650 に言及があるがpart2-rulesの章が無い)と式 json_family_constant_fold(expression/rewrite.cpp:2464)。

**発生条件:** 新規参入者が00-indexの網羅表(論理83/式70)でRule一覧を作ると、実在するlimit_prefix系・constant_unnest_to_values・json_family_constant_foldが抜けた一覧になる。docs/optimizer_rule_audit.md:176-266の「残した全登録」リスト(83本)にも無く、監査文書としての完全性が崩れている。

**確認・対応記録:**

- Added four missing rule chapters using current code excerpts, gate conditions and existing regression names; tracker updated.

### medium-18

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/opt/part1-foundations/10-cascades-overview.md:142`

**指摘:** Rule本数の記載が実装と不一致: 本doc(:141-143)は「式74回Add(実効73) / 論理有効116 / 物理52」と書くが、実測は式71・論理86・物理46。docs/opt/AGENTS.md:81-83とdocs/opt/00-index.mdは「70/83/46」、docs/cascades_optimizer.md:18,55は「83 logical and 46 physical registrations」、docs/optimizer_rule_audit.md:71は「式ルールは70登録」:184-266の全登録リストは83本で、いずれも現行86/71とずれる。00-index.md:14の「全216ファイル(第2部207/207)」も実file数は167(第2部158=expr35+impl46+logical77)。

**発生条件:** 本数表は「ずれたら必ず規範側か本書側を直す」というdocs/opt/AGENTS.md:44の運用規律のチェック対象そのもの。監査リスト(optimizer_rule_audit.md)を正として新規Ruleの未登録/未doc判定をすると、実際に追加された constant_unnest_to_values 等を「監査で撤去済み」と誤認する。

**確認・対応記録:**

- Recounted exact Add registrations: expression71 logical86 physical46. Updated current inventories and file count171 after4newchapters; historical reduction counts retained as dated history.

### medium-19

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/opt/part1-foundations/20-memo-structure.md:68`

**指摘:** §20-2のLogicalOperator列挙体引用に kSingleJoin(:68), kMarkJoin(:69), kMax1Row(:76), kConstantTable(:86) が含まれ、省略コメント(:88)も kGenerateSeries / kWorkTableScan を実在のように示す。現行のenum(plan/cascades.hpp:38-66)にはこれら5種は存在せず、代わりに kRecursiveCte / kApply / kSample がある。kMax1Row登録の撤去は docs/optimizer_rule_audit.md:49 に記録済み。

**発生条件:** この章を底本にMemo実装を読むと、kMax1Row/kSingleJoinの式検査・パターンDSLを探して時間を浪費する。docs/optimizer_todo.md:38-39,94-96も同じく「kMax1Row / Max1RowPlan として実装」と[ x ]扱いで、二重に誤導する。

**確認・対応記録:**

- LogicalOperator excerpt copied from current enum; removed phantom classifications.

### medium-20

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/optimizer_todo.md:993`

**指摘:** 「MERGE 文の join 計画(MERGE 構文がなく到達不能。MergeExecutor の器は executor 側にあるが表面なし)」は両側が誤り: MERGE構文は実在し(query/googlesql_ast_visitor.cpp:8230 VisitMerge、query/googlesql_ast_visitor_dml.cpp:1732 MergeStatement生成、sql_engine.cpp:6889-6891で実行)、MergeExecutorというクラスは存在しない。同種の陳腐化が :92-93/:673-674/:970「TABLESAMPLE構文がなくSQL表面なし」(実在: VisitTableSample=visitor.cpp:4941、query/table_sample_test.cpp、kSample enum=cascades.hpp:65、sample実装Rule登録済み)と、:38-39/:94-96の kMax1Row/kAssert/kSingleJoin/Max1RowPlan[ x ]、:68 kConstantTable[ x ]、:842-848「set-opはmemoに入らず…実装規則の追加はdead code」(実際はunion/union_all/intersect/intersect_all/except/except_allの6実装RuleがDefaultImplementationRulesに登録済みで、docs/optのRule章・cascades_testも到達を検証)。

**発生条件:** TABLESAMPLEを「将来枠」として再実装しようとすると既存のSamplePlan/実装Ruleと二重実装になる。set-opの「dead code」判定を信じて6RuleをRemoveすると、docs/optのunion等のRule章が説明する到達経路が黙って消える。

**確認・対応記録:**

- Cross-checked current LogicalOperator enum, rule registrations, classes and SQL routes. Corrected phantom Max1Row/Mark/Spool/Exchange/NUMA/CardinalityProbe/Returning/Merge classes, actual MERGE/TABLESAMPLE/UNNEST/set-op routes and PageManager counters. Pinned execute_query rejects SQL-standard FILTER syntax but parses AGG(x WHERE p). Changes: /tmp/patch-audit-todos.py (batch33).

### medium-21

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/page_checksum.md:12`

**指摘:** page_checksum.md 全文が退役したページ・チェックサム機構(PagePool / CRC-32C / DecodeDisk / TYB1ページmagic)を現役仕様として記述する。実コードにはページ・チェックサムが存在しない。

**発生条件:** on-disk形式変更やページ破損対応のレビューで本docを参照すると「CRC-32C検証でfail-closedする」と誤認する。Page::SetChecksum()は空実装、IsValid()はPageType一致のみで、Pageにchecksumフィールド自体が無い。

**確認・対応記録:**

- Retired per-page checksum spec replaced with actual snapshot/PAX checksums and current API references.

### medium-22

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/recovery_invariants.md:139`

**指摘:** Testing節(136-172行)が実在しないテストを大半引用する。`recovery_extra_test`というファイルは存在せず、引用32テスト名のうち実在は5つのみ。テール切捨て・WAL CRC・D2/D3冪等・アロケータundo系の回帰テスト名は全て0件。

**発生条件:** WALローリング/テアテール挙動の回帰保証をdocsで確認しても実テストが無く、テストカバレッジの誤認につながる。

**確認・対応記録:**

- Testing section lists existing tests, explicitly disclaims deleted legacy coverage.

### medium-23

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/todo.md:79`

**指摘:** 「このファイルを唯一のTODOリストとする」(:3-4)と宣言するtodo.md自体のPhase 1ブロック(:79-93)が「以下はすべて実装・テスト合格を確認済み」としながら、列挙の大半(kSingleJoin, kMarkJoin, kConstantTable, kGenerateSeries, kMaterialize, kEagerSpool, kLazySpool, kExpand, kLateralJoin, kExchange/kGather/kBroadcast/kRedistribute, kAssert)が現行enumに存在せず、引用テスト(CascadesTest.SingleJoinAndMarkJoinAreBinaryLogicalOperators, ConstantTableAndGenerateSeriesAreLeafOperators)も存在しない。同様に:147 [ x ]As-of Join/Interval Join(2026-10-03削除、docs/executor/50-join.md:255の削除注記と矛盾)、:1479 [ x ]Exchange/Gather/Redistribute executor、:1482 [ x ]NUMA-aware arena、:1484 [ x ]batch aggregationのdictionary encoding(DictionaryBatchAggregationはコードに存在しない)、:1518 [ x ]RemoteScan/DistributedAggFinalize(削除済み)。

**発生条件:** 唯一の正TODOとして新しい実装者がこのファイルを読むと、削除済み演算子群を「実装済み」と認識し、(a)再実装しない (b)存在しないテストを回帰に追加する、のどちらかの誤判断をする。削除記録(optimizer_rule_audit.md, executor/50-join.mdの削除注記)とtodo.mdの[ x ]が直接矛盾している。

**確認・対応記録:**

- Cross-checked current LogicalOperator enum, rule registrations, classes and SQL routes. Corrected phantom Max1Row/Mark/Spool/Exchange/NUMA/CardinalityProbe/Returning/Merge classes, actual MERGE/TABLESAMPLE/UNNEST/set-op routes and PageManager counters. Pinned execute_query rejects SQL-standard FILTER syntax but parses AGG(x WHERE p). Changes: /tmp/patch-audit-todos.py (batch33).

### medium-24

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/aggregation.cpp:1221`

**指摘:** AccumulateTypedBatch が状態更新「後」の演算オーバーフローでも false を返し(641/650/658/666/737/751/767/782/794/808行)、ヘッダ契約『false=layout不一致で状態はuntouched』(aggregation.hpp:76-81)を破る。呼び出し側は同一バッチを行単位ループで再処理するため先行集約が二重加算され、再処理中のSUMはthrowingな results[i]+val(1221行)で例外として飛ぶ(本ファイルにtry/catchなし)。

**発生条件:** SELECT COUNT(*), SUM(a)(aは符号付きINT64列)で all_typed_ のバッチ処理中、SUMのpartial合計がオーバーフローすると AccumulateTypedBatch は COUNT(*) を状態へ反映済みの後で FailWith→false(765-767行)。NextGeneric のループ(1045-1048行)が同一バッチを行単位で再実行し COUNT が二重加算された後、行ループは同じ累積点で results[i] + val(1221行、Value::operator+ は ExcShimUnwrap でthrow)により std::runtime_error を NextBatch の外へ放出する。

**確認・対応記録:**

- Typed arithmetic failure incorrectly replayed batch through generic evaluator; caller now stops on failed GetStatus.
- TypedOverflowStopsWithoutReplayingTheBatch passes; full executor_extra_test 35/35.

### medium-25

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/aggregation.cpp:671`

**指摘:** kTypedConstant の倍精度SUMだけが有限→無限大のオーバーフロー検査を欠き、無限大がエラーではなく成功結果として返る。列参照経路(CheckedDoubleAdd, 560-566/786-814行)と汎用行経路(1214-1218行)は同一状況で kIsInfinity を返す。

**発生条件:** SELECT SUM(1e308) のような倍精度定数のスカラー集約が大量行(all_typed_ 成立)で処理されると、671-674行の static_cast<double>(rows) * constant + prior が無限大になっても検査されず Value(inf) が成功応答になる。docs/executor/70-aggregation.md:44『有限な倍精度入力だけから無限大になった場合もオーバーフロー(失敗)』と不一致(エラー飲み込み)。

**確認・対応記録:**

- Constant DOUBLE SUM produced inf from finite inputs; added finite-input overflow check.
- TypedOverflowStopsWithoutReplayingTheBatch tests constant 1e308 overflow; passes.

### medium-26

**判定:** 現行コードでは指摘を確認できず (`not-reproduced-current-code`)

**元の対象:** `executor/data_chunk.cpp:63`

**指摘:** UpdateZoneMapFrom が kNull 型列のNULLを一切記録せず(63-64行)、チャンク→チャンクの行copy(Append(DataChunk,index)/AppendGather 非identity/AppendRowFromColumns)で複製された全NULL列は NullCount()==0・未初期化ゾーンを報告する。JIT SUM ゲートは NullCount==0 だけを見て(aggregation.cpp:152)実列型 kNull≠kInt64 で CHECK_MSG(false)→abort する(フォールバックでなくプロセス即死)。

**発生条件:** 最初の行がNULLで推論レイアウト(kNull列)になったチャンクを行copy系APIで中継するとゾーンのnull計上が欠落し、jit_sum_eligible_ な SELECT SUM(int64列) のJITゲートが NullCount==0 でJITを選択→HasLayout は kNull 列をスキップして通過→column.Type() != kInt64 で CHECK_MSG(false, "aggregation input layout mismatch")(aggregation.cpp:155-158)→abort(common/log_message.hpp:51)。なお本精読で読んだファイルの範囲では、この中継を現行内蔵実行器で末端まで繋ぐ完全なSQLレベル再現手順は構成できていない(Append(Row) 経路は zone_maps_[i].Add でNULLを記録するため影響を受けない)。producer/consumer の不一致自体は根拠行付きで確実。

**確認・対応記録:**

- UpdateZoneMapFrom checks column.IsNull before its type switch. AuditAllNullColumnsRetainZoneNullCounts passes for both chunk copying and column projection without code changes.

### medium-27

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/aggregate_accumulator.cpp:1385`

**指摘:** MergeがSUMの符号付き/符号なしモードを調停せず、片側でもsum_is_uint64ならuint_total同士の加算のみ行い、相手(またはthis)のint_totalを黙って捨てる。TryFinish(1575-1577)もuint結果にint_totalを足さないため、混在データのSUMは部分的に欠落する。同一アキュムレータ内でも、符号付き値の蓄積後にunsigned値が来ると378-389でモード反転し既蓄積のint_totalが放棄される。

**発生条件:** UNION等で再束縛され宣言上のunsigned性が失われたINT64列(本ファイル自身のコメント195-205がUNIONスキーマでの宣言欠落を実在事象として記載)をSUMし、符号付き値とunsigned値が別ワーカーの部分状態に分かれた場合、relational.cpp:934/961・typed_stream_aggregate.cpp:5952のマージで符号付き側の合計が消える。単一の型一貫した列では発生しない。

**確認・対応記録:**

- Direct Add and Merge lost signed partial when uint arrived; both normalize exact signed/unsigned partial totals before addition.
- MixedSignedUnsignedSumsKeepBothPartialTotals tests Add/Merge both orders; passes.

### medium-28

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/aggregate_accumulator.cpp:199`

**指摘:** SUMの集約子列名に「uint64」という部分文字列が含まれるだけで、宣言型がINT64でも値をUINT64ビット列として再解釈して加算する。負値を含むINT64列でSUMが静かに誤値を返す。docs/executor/70-aggregation.md:44はSUMを型駆動(符号付き/符号なしのオーバーフローで失敗)とだけ記述しており、名前ベース再解釈はどこにも文書化されていない。

**発生条件:** CREATE TABLE t(uint64_val INT64) に -1 を挿入し `SELECT SUM(uint64_val) FROM t` を実行すると、-1ではなく18446744073709551615(符号なしビット解釈)が返る。派生表の別名 `SELECT x AS uint64_val FROM ...` を経由したSUMでも同様。

**確認・対応記録:**

- Signed columns named uint64_label and distinct x were misclassified; removed naming/value-bit guesses in relational and typed aggregate paths.
- ColumnSpellingDoesNotChangeSignedSumDomain passes for both names and DISTINCT settings.

### medium-29

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/detail/build_input.cpp:1334`

**指摘:** 非相関IN pushdownのmembership読み取りでForEachRowのStatusを無視。後続のexact_in最適化がこのフィルタを根拠にIN述語自体を走査フィルタから削除するため、spill読み出し失敗時、欠けたキーセルが唯一の裁定者として残り行が静かに欠落する。

**発生条件:** membership集合がspillしている規模で読み出しが途中失敗 → keysが部分集合 → RuntimeFilterBuilder::FromKeysでexact filter確定 → build_input.cpp:2986-3004が has_dense_bits && DistinctKeys()>=4096 を根拠にIN式をlocal predicatesから除外 → LoadSourceの走査が欠けキー行を棄て、INの再評価も行われないため結果から消える。

**確認・対応記録:**

- BuildInput membership ForEachRow status now propagates; no partial exact IN key set can be committed.

### medium-30

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/detail/build_input.cpp:205`

**指摘:** LateralExpandRelationがprefix/elementsのForEachRowおよびFinishSpillの戻りStatusを無視する。elements_statusは式評価エラーしか捕捉しないため、spill読み出し失敗時に部分展開が成功として返る(エラー飲み込み→行欠落)。

**発生条件:** lateral UNNEST/LATERAL結合でprefix関係(左側の蓄積結果)がspillしている状態でspill読み出しが途中失敗 → ループが静かに打ち切られ、失敗前までの行だけで正常終了する。LoadSource/FilterRelationは同状況でRETURN_IF_FAIL(iteration)とする方針(relation.hpp:71,95のForEachRowはStatus返し、build_input.cpp内の他呼び出しでも検査)なのにここだけ無視。

**確認・対応記録:**

- Lateral expansion checks prefix/element/replay ForEachRow and FinishSpill statuses, stops on first error, and checks output finalization.

### medium-31

**判定:** 修正済み・専用境界値テスト未実施 (`fixed-needs-boundary-test`)

**元の対象:** `executor/detail/build_input.cpp:2133`

**指摘:** PK吸収のdense-dir/bucket計算で `uint64_t span = uint64(max_k) - uint64(min_k) + 1` がINT64全域スパンで0にwrapする。下限チェックはmin<=maxのみでwrapを検出できず、第1経路は剰余算 `/ span` でゼロ除算(SIGFPE)、第2経路の非単調分岐は `shared_dense_payload[off]` に最大2^64-1のoffでアクセスしヒープ破壊(OOB書き込み)になり得る。

**発生条件:** PK列(INT64, unique nonnull, monotonic)にINT64_MINとINT64_MAXの両方をINSERTし、2列PK payload吸収の形ゲート(2 active列・join edge等)を通るクエリを実行 → store->column_min_int/max_intからspan=0 → 2207-2212 `(uint64(k)-uint64(min_k))<<12 / span` でSIGFPE。relation行由来の第2経路(3588-3589)では span=0 が 3702の `span > 16777216ULL` ゲートも d_monotonic もすり抜け、3860 `resize(span)` 不変のまま 3883-3892 `shared_dense_payload[off]` に巨大offでOOB書き込み。

**確認・対応記録:**

- Block directory span widened to uint128 (full INT64 span is 2^64); row-based dense absorption rejects wrapped zero span.

### medium-32

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/explain_format.cpp:1526`

**指摘:** EXPLAIN 推定計画の ROW_NUMBER「定数パーティション除去」判定が、一意性の証明なしに列名 event_id の文字列一致だけ(`part_col.find(table_name + ".event_id") != npos || part_col == "event_id"`)で発火し、`Window eliminated constant_partition=true` / `Project constant 1` を出力する。ランタイム側にこの除去の実装は存在せず(grep で executor/ と query/ の非テストコードに counterpart 無しを確認)、PK 制約を検査する姉妹ロジック pk_group_aggregate_eliminated(explain_format.cpp:1885-1918 の GetConstraint().IsUnique())と要件が不一致。EXPLAIN 表示が実際プランと不一致で、仮に最適化注記としても健全性がない。

**発生条件:** event_id が一意でない任意テーブルで `SELECT ROW_NUMBER() OVER (PARTITION BY event_id) FROM t` を EXPLAIN。実際はウィンドウ評価されるのに「Window eliminated constant_partition=true」(ORDER BY 無しなら「Project constant 1」)と表示され、行番号>1 の実結果と矛盾する計画を見せる。

**確認・対応記録:**

- Removed column-name-based fabricated singleton window elimination from estimated EXPLAIN. Direct formatter reproduced Project constant 1 for nonunique event_id before; SQL regression verifies actual row numbers and truthful formatter. SQL20 pass (batch27).

### medium-33

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:1390`

**指摘:** 未知のタイムゾーン名の検証が CURRENT_DATE にしかなく、兄弟関数がサイレントにフォールバックする。325-327 のコメントは「CURRENT_DATE('foo') and friends must reject unknown zone names ... instead of silently falling back」と明言するが、TryValidateTimeZoneName の呼び出しは1425行(CURRENT_DATE)のみ。CURRENT_DATETIME は未知ゾーン名を ParseTimeZoneOffset→TimeZoneOffsetAtLocalTime が nullopt を返し value_or(0)(322行)で UTC 扱いにする。TIMESTAMP 2引数(1673-1675)は既定値 -8*3600 のため未知ゾーンが PST 扱いになり8時間ずれる。EXTRACT 2引数(1084)も同根。

**発生条件:** `SELECT CURRENT_DATETIME('Bogus/Zone')` → エラーではなく UTC で現在時刻を返す(GoogleSQL は Invalid time zone を出す)。`SELECT TIMESTAMP('2020-01-01 12:00:00', 'Bogus/Zone')` → サイレントに -8h の瞬間値 '2020-01-01 04:00:00+00' を返す。CURRENT_DATE('Bogus/Zone') だけが kInvalidArgument で失敗し、同一ファミリー内で挙動が割れる。

**確認・対応記録:**

- Unknown and malformed zones reproduced through AST and SQL. Shared type/date resolver now returns StatusOr, validates complete fixed offsets and IANA names, and every former default-fallback caller propagates errors. AST7/SQL21/differential17/function-eval10/scalar19/edges9/deep1 pass. Broader function coverage58 passes after correcting two stale IF numeric-type expectations (batches28/31).

### medium-34

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:2008`

**指摘:** 配列インデックス関数(array_element_offset/ordinal/safe_*)と array_slice の生きている実装が、NULL チェックのみで `arguments[1].value.int_value` を読む。INT64 以外(DOUBLE/VARCHAR)のインデックスは Value 共用体の別メンバーのビットを int64 として再解釈し、型エラーではなくゴミのインデックスになる。弟分の offset/ordinal ハンドラ(6339-6348)は DOUBLE を明示的に変換しており、デッドの第2 array_slice(7230-7239)は型検査する——生きている経路だけが契約を欠く。

**発生条件:** `arr[OFFSET(x)]` の x が DOUBLE 列(例: 5e-324)の場合、そのビットパターンは int64 の 1 と同一なので elements[1] が返り誤結果になる。通常の DOUBLE(例 2.0 → ビット 0x4000...)は巨大正整数になり 'Array index 4616...' の不正な範囲エラー。VARCHAR インデックスでは string_view ポインタ/長をインデックスに再解釈する。

**確認・対応記録:**

- AuditArrayIndexesRejectNonIntegers reproduced union-bit interpretation; integer-only bounds now enforced for array access/slice. Also removed ordinal INT64_MIN decrement and slice end addition overflow.

### medium-35

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:521`

**指摘:** IF の INT64→FLOAT64 分岐昇格が生きている経路に存在しない。793-796 のコメントは「Branch results are normalized to the common supertype ... (GoogleSQL coerces INT64 branch arms to FLOAT64 when any arm is a float)」と述べ、その実装が 817-832 の第2の `if (name == "if")` ブロックにあるが、521-532 の先行ブロックが無条件に return するため第2ブロックは到達不能デッドコードで、コメントに記載の動作は実現されていない。AST 真実実装(function_call_expression.cpp:4995-5005)は NormalizeIfBranch(4942-4951、unsigned も考慮)で正規化しており、両経路で結果の型が不一致になる。

**発生条件:** relational 経路で `SELECT IF(TRUE, 1, 2.0)` → INT64 の 1 をそのまま返す。AST 経路では as_double=true で DOUBLE 1.0 に正規化される。下流の UNION/比較/出力型が経路によって異なり、diff テストや列型契約が崩れる。

**確認・対応記録:**

- Removed earlier IF handler that bypassed normalization. AuditControlFlowMatchesAst reproduced type difference and passes with canonical double promotion.

### medium-36

**判定:** 修正済み・専用境界値テスト未実施 (`fixed-needs-boundary-test`)

**元の対象:** `executor/detail/function_eval.cpp:694`

**指摘:** DATE/DATETIME/TIMESTAMP_ADD(_SUB) で INTERVAL の量が int64 最小値(INT64_MIN)のとき、nanosecond 単位以外の delta_days/delta_sub_ns が初期値 0 のまま放置され、加算がサイレントの無操作になる。正しい挙動は範囲エラー(kIsInfinity)か巨大シフトであり、ベース値がそのまま返るのは誤結果。

**発生条件:** INTERVAL リテラルまたは INTERVAL 式の Amount() が -9223372036854775808 になりうる場合: `SELECT TIMESTAMP_ADD(TIMESTAMP '2020-01-01 00:00:00+00', INTERVAL -9223372036854775808 DAY)` → 694-704 で nanosecond 分岐のみ uamount を処理し DAY は delta 0 のまま → ベースのタイムスタンプがそのまま返る。DATE_SUB でも同様。

**確認・対応記録:**

- INT64_MIN duration outside nanoseconds is now rejected rather than ignored; month/week/day arithmetic checks wide bounds before narrowing.

### medium-37

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:813`

**指摘:** relational 側 IF の分岐結果 FLOAT64 昇格が UINT64 タグを無視し、int64 スロットを負値のまま double 化する。AST 正規化 NormalizeIfBranch は IsUnsigned を見て uint64 経由で変換するため、同一式がエンジンで異なる値になる。

**発生条件:** IF(TRUE, <UINT64値 2^63>, 0.5) — double分岐が存在すると昇格が走り、AST は +9.223372036854776e18、relational は -9.223372036854776e18 を返す(誤結果)。UINT64値は generate_array/generate_series や CAST で現実に生成される。

**確認・対応記録:**

- IF normalization converts tagged UINT64 through uint64 before double. AuditControlFlowMatchesAst verifies UINT64_MAX follows AST.

### medium-38

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:870`

**指摘:** IFNULL が遅延短絡にならず、全引数を先に評価してから選択する。AST側の真実実装は第一引数が非NULLなら第二引数を評価しない(IFNULL(1, 1/0) → 1)が、relational 経路では第二引数のエラー(0除算・ERROR()・不正キャスト等)が上げられ問い合わせが失敗する。function_call_expression.cpp:5038-5041 のコメント「COALESCE / IFNULL short-circuit ... like the relational evaluator」と矛盾。COALESCE には詳細側に先行遅延ハンドラ(534-544)があるが、IFNULL にはない。

**発生条件:** relational 経路で `SELECT IFNULL(1, 1/0)` → 870-875 の全引数評価ループで 1/0 が raise し実行失敗。同じ文を AST 経路で評価すると 1 を返す(function_call_expression.cpp:5056-5060)。プラン形状の変化で片方に振れたときだけ結果が変わり、IFERROR でガードされた式の契約が壊れる。

**確認・対応記録:**

- IFNULL now shares early lazy COALESCE path with arity check; unreachable ERROR second branch regression passes.

### medium-39

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:928`

**指摘:** 遅延STRUCTコンストラクタ __struct_json__ の relational 実装が、sql_udf.hpp が「byte-identical 出力を保証する共有実装」と明言する EncodeStructJson を使わずローカル再実装されており、5点で出力バイトが分岐する:(a) any_ci時のVARCHAR折り畳みなし、(b) proto-text正規化を余分に実施、(c) 制御文字を\uXXXXでなく生バイト出力、(d) struct形VARCHARをverbatim埋め込み(AST側は常にquote)、(e) quotedフラグNULL時の "null" 解釈。

**発生条件:** 実行時(非定数)構築の STRUCT が COLLATE 'und:ci' の列を含む場合、relational 経路は折り畳まないため StructJsonCompare(binary_expression.cpp:135-153 の生テキスト比較)が case-sensitive になり、AST/plan折り畳み経路の同じstructとの比較結果が反転する。ネストstructでは get_field の戻りが「JSON文字列」vs「structテキスト」に分かれる。

**確認・対応記録:**

- Runtime STRUCT delegates to canonical ExecuteFunction. AuditStructEncodingMatchesAst covers control chars, nested-looking strings, NULL flags and collation; failed before and passes after.

### medium-40

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/planning_heuristics.cpp:596`

**指摘:** バイアス付き結合キー(col ± const = col2)の抽出で、BARE 側にのみ BiasedKeyTypesSupported を適用し、BIAS 側の列の型/unsigned を全く検査していない。実行時は IntegerJoinKey の生int64ビットに mod 2^64 のラップフリー加算でハッシュする一方、評価器は unsigned混在や巨大値で FLOAT64 昇格・丸めを起こすため、SQL等価が成立するペアがハッシュで候補にすら上がらず取りこぼされる。残差述語は正しく保持されるが、残差は「ハッシュが見つけた候補」の再検証しかできず取りこぼしは復元できない(287-289行目の「residual verification stays the correctness backstop regardless」は見つけたペアにしか効かない)。

**発生条件:** 符号付き/unsigned 混在の結合 `... ON a.u + 5 = b.s`(a.u が UNSIGNED INT64)で、u=2^60、s=2^60+8 のように評価器の FLOAT64 昇格・丸めで等価と判定されるペアが、バイアス付きint64キー(2^60+5 ≠ 2^60+8)では equal_range に現れず結果行が欠落する。符号付きINT64同士で |const|<=2^40 の場合は評価器の整数演算とキー算術が一致するため問題は出ない。

**確認・対応記録:**

- BiasedKeysRequireBothSignedIntegerDomains failed before, passes after both columns and constants require signed INT64.

### medium-41

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/detail/scan_filter.cpp:1137`

**指摘:** PrebuiltMembership直接fillのイテレータ走査(並列morsel fill・直列BeginFullScan fill)もGetStatusを確認しない。障害時にメンバーセルが部分集合のまま確定し、その後のIN/NOT IN判定が誤る。

**発生条件:** フィルタコンパイル時に単一テーブル単列のmembership構築がmorsel並列(1133-1151)または直列(1189-1215)で走り、途中で反復子が非例外系で無効化 → 欠けたキーセルでprebuilt確定。以後の `x IN (SELECT y FROM t)` が欠けキーの行を誤 rejected にする。なお直列fillのreserve超過fallback(1204-1215のoverflow)はstats酔い対策として実装済みだが、I/O系の短縮は無防護。

**確認・対応記録:**

- Both direct membership iterator routes check GetStatus; parallel worker statuses force checked fallback rather than publishing partial membership.

### medium-42

**判定:** 修正済み・専用キャッシュ経路テスト未実施 (`fixed-needs-cache-test`)

**元の対象:** `executor/detail/scan_filter.cpp:4176`

**指摘:** keyed runtime-filter cacheのヒット経路は走査結果を正規化(unsignedタグ再付与)前に早期returnするため、UINT64宣言列の行が非タグ付きINT64ビットパターンのまま式評価へ流れる。キャッシュ格納側(4335-4347)も正規化前のrowsを保存しており、2回目の同一フィルタ走査で発症する。

**発生条件:** UINT64列を持つ表に対し同一exact-key(≤8192キー)のruntime filterで2回走査(自己結合の2プローブや同一文内の2参照) → 2回目が4136のキャッシュにヒットし4176でreturn。4376-4404の「Values are stored as INT64 bit patterns, while the catalog retains the SQL UINT64 declaration… Reattach that declaration」正規化を通らないため、UINT64_MAX(ビットパターン-1)が `id < 0` を真とする等の誤評価になる。通常のbase_relationsキャッシュ経路は後段正規化を通過する(3856以降はearly returnしない)のに、この経路だけ素通り。

**確認・対応記録:**

- Keyed scan cache hit reattaches declared unsigned tags while projecting cached rows before early return.

### medium-43

**判定:** 修正済み・専用スピル経路テスト未実施 (`fixed-needs-spill-test`)

**元の対象:** `executor/detail/scan_filter.cpp:4208`

**指摘:** TrySelectiveIndexSource成功後の secondary runtime filter(extras)のin-place compactionが result.rows を直接書き換え/resizeするのに HasSpill() を確認しない。Relation::AddRowはグローバル予算逼迫でどんな行数でもspillし得る(relation.cpp:145-169)ため、spill済みの場合メモリ側だけ絞られ、spillファイルには未フィルタ/重複行が残る。

**発生条件:** メモリ予算が逼迫した実行(他演算子が予算を消費)でインデックス選択経路が8192行未満でもspill → extras適用のcompactで rows ベクトルだけ縮む → その後 FinishSpill/ForEachRowでspill側の全行が復活し、extras未適用行と2重化した行が下流へ流れる。同ファイル2770-2772には自ら「Spilled shards must go through ForEachRow… enumerating rows and spill by hand silently drops the tail」との契約が明記されており、ここが違反。4335のkeyed-cache格納はHasSpill()を確認済みで差がついている。

**確認・対応記録:**

- Secondary runtime filters iterate and rebuild spilled index relations with checked statuses; resident path retains in-place compaction.

### medium-44

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/scan_filter.cpp:4276`

**指摘:** LoadSourceの非並列フォールバック走査がtable::IteratorをIsValid()だけで回し、終了後にGetStatus()を一度も確認しない。例外を投げず反復子状態にだけ記録されるストレージ障害が正常な末尾として扱われ、部分結果が完全な結果に偽装される。

**発生条件:** 大きな表をフィルタ付きで走査中にページ取得/復号が非例外系で失敗 → iteratorが無効化されてループ終了 → RETURN_IF_FAIL(eval_error)は成功のまま通過し、読めた行だけのRelationがコミットされた結果として返る。同一ファイル内の他経路は確認済み: TrySelectiveIndexSource(3318,3376)と並列morsel経路(parallel_table_scan.cpp:1410)はGetStatusを確認し評価する。

**確認・対応記録:**

- Serial LoadSource fallback checks iterator.GetStatus after drain before caching/publishing rows.
- SelectiveIndexSourceKeepsHeapOrderAndSnapshot now observes kConflicts for pending append timeout instead of silently accepting incomplete prefix. After abort snapshot remains unchanged; passed.

### medium-45

**判定:** 修正済み・専用境界値テスト未実施 (`fixed-needs-boundary-test`)

**元の対象:** `executor/detail/stream_grouped_join.cpp:1101`

**指摘:** 高速 COUNT+SUM パスがブロック内の非負値を符号付き int64 blk_sum に無検査で加算する。1088行目の gate は vcol.min_int >= 0(下限)しか保証せず、ブロック内値の総和が int64 最大を超えると符号付きオーバーフロー(UB、実装上はラップ)。Partial::sum は __int128 で汎用経路はマージ時に kIsInfinity を返す(1693-1697)のに対し、この経路はラップ後の値を morsel_partials[1].sum に加え、low を更新しないため(1108-1109は high のみ)マージ時の境界検査でも検出できず誤った SUM が静かに出力されうる。

**発生条件:** 1ブロック内の列値が巨大(例: 9e18 × 2行)で min_int>=0 の zone map を持つ場合、blk_sum がオーバーフロー→ラップした負値が partial に混入し、SUM の結果が誤値のまま正常終了する。汎用経路(1150 は __int128 Partial::sum へ直接加算)と比較して三値/エラー契約が不一致。

**確認・対応記録:**

- Nonnegative block SUM now accumulates in __int128; existing final prefix bounds preserve overflow diagnostics.

### medium-46

**判定:** 修正済み・専用SQL到達経路テスト未実施 (`fixed-needs-query-test`)

**元の対象:** `executor/detail/subquery_runtime.cpp:1161`

**指摘:** aggregate_only 判定後の references_outer gate が SELECT 項目(1163)と HAVING(1165)のみを検査し ORDER BY 項目を検査しないため、外部列を参照する ORDER BY を含む相関集約サブクエリがインデックス化される。しかし preagg_int_scalars/emit_group のキャッシュキーは相関列1個のmemcomparable(1927-1941)なのに対し、プローブ側の cache_key は cache_outer_columns 全値(ORDER BY 列を含む、2322-2341)で生成されるため恒常ミスになり、フォールバックの2432-2453が空Relationで FinishQuery するためキーに一致行があっても空集約(COUNT=0/SUM=NULL)を返す。

**発生条件:** SELECT (SELECT COUNT(*) FROM lineitem l WHERE l.l_orderkey = o.o_orderkey ORDER BY o.o_orderdate) FROM orders o — cache_outer_columns={o.o_orderkey,o.o_orderdate} となり can_use_preagg_int_scalars(1698-1704 のサイズ一致条件)が外れ、emit_group は単列キーで cached_results に格納。プローブの2値cache_keyは常にミス→preaggregated 経路が空入力の集約(COUNT=0)を返し、真の COUNT=N ではなく NULL/0 になる。到達性はフロントエンドがサブクエリ ORDER BY 内の外部参照を許すかどうかに依存し、この点は本ask内では検証できていない(許す場合のみ誤結果)。

**確認・対応記録:**

- Correlated aggregate cache eligibility now checks ORDER BY outer references as well as SELECT/HAVING.

### medium-47

**判定:** 重複指摘 (`duplicate`)

**元の対象:** `executor/detail/subquery_runtime.cpp:2059`

**指摘:** 相関サブクエリのビルド3経路(EXISTS キーセット構築 2059-2070、汎用 multimap 構築 2076-2099、集約蓄積 1687)が Relation::ForEachRow の戻り Status を無視する。スピルファイル読み出し失敗(SpillFile::ForEachRow は truncation/kIOError を返す)でもエラーが握り潰され、欠落したキーセット/行/集約値が完成品としてキャッシュされ、EXISTS が誤 false、スカラーが誤値になる。直後の 2000-2013 の exists_residual 経路だけは `const Status source_status = ...` で検査しており、見落としと分かる。

**発生条件:** 大量行の相関サブクエリビルドで Relation がスピル済みの状態でスピルファイル読み出しが失敗(ディスク障害・切詰め)。`EXISTS (SELECT ... WHERE outer.k = local.k)` のビルドが部分集合のまま exists_key_set にキャッシュされ、以後の全プローブが誤って EXISTS=false を返す。

**確認・対応記録:**

- Same correlated spill status loss as high-7.

### medium-48

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/subquery_runtime.cpp:282`

**指摘:** ProjectSubqueryRow の AS STRUCT エンコードが VARCHAR メンバを `\"\" + text + \"\"` で JSON に埋め込むが引用符等のエスケープをしていない。生テキスト比較に使われるため、構造が異なる行が同一文字列に衝突し得る。例: 1フィールドの struct(値が `x\",\"f2\":\"y`)は `{\"f1\":\"x\",\"f2\":\"y\"}` と出力され、2フィールド struct(x, y)のエンコードと完全一致する。量化比較・メンバシップがこの値の等価性に依存するため、異なる SQL 値が同値判定される。

**発生条件:** `SELECT 1 WHERE (SELECT AS STRUCT 'x\",\"f2\":\"y' ) = (SELECT AS STRUCT 'x', 'y')` のような相関比較で、別個の構造体が等しいと判定される。 フィールド名を含む文字列を AS STRUCT で返すサブクエリの IN/比較で誤判定。

**確認・対応記録:**

- ProjectSubqueryRow delegates to canonical EncodeStructJson rather than unescaped manual text concatenation.
- AuditSubqueryStructEscapesFieldContents distinguishes injected one-field text from two fields and pins escaped encoding; passes.

### medium-49

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/typed_stream_aggregate.cpp:3546`

**指摘:** 辞書高速化パス群がSlimFlags(無限入力フラグ)を維持しないため、finalizeのSUMオーバーフロー判定が誤る。Q1 5-term経路はflagを一切立てず、真に無限大の入力があるだけでもfinalize(6071-6078)が「SUM overflow」を誤raiseする。逆にdup集約コピー(4255-4266)はtotalのみ複製しflagを継承せず、また4270-4309のflag再走査はブロック選別を無視して全行を見るため、選別で落ちたinf行がflagを立てて本来raiseすべき有限値オーバーフローを封じる。

**発生条件:** (a) inf値を含むDOUBLE列でQ1形状(SUM 5項・辞書グループ≤8)の集約→finalizeがSUM overflowを誤raiseして失敗。(b) WHEREでinf行が除外されるQ1形状→除外済みinf行がflagを立て、全有限入力でtotalがinfになってもエラーではなくinfが返る。

**確認・対応記録:**

- AuditFilteredInfinityCannotMaskSumOverflow reproduced Success instead of overflow before; infinity provenance rescans only selected rows of the current group. Duplicate SUM and UINT arena regressions pass.

### medium-50

**判定:** 修正済み・問題箇所を直接通す専用テスト未実施 (`fixed-needs-direct-test`)

**元の対象:** `executor/detail/typed_stream_aggregate.cpp:744`

**指摘:** Row方式のキャッシュ充填でアリーナミラーに全列を無区分で value.value.int_value として書き込むため、VARCHAR列にはstring_view共用体の先頭8バイト(size/ptr)がゴミセルとして入り、かつVARCHARバイト列/endsが一切appendされない。このアリーナが公開されると、後続のキャッシュヒット集約がarena->Varchar()でends配列を範囲外読みする。

**発生条件:** TINYLAMB_DISABLE_TYPED_FILL=1 のA/Bハッチ下でVARCHAR列を含む表をfill_cache経由で集約(ミラー生成)→同一文の2度目の集約がキャッシュヒットし、アリーナ高速化(slim+int_key)が成立すると、文字列filter_compares/inlistの解決で subquery_runtime.hpp:192-197 の Varchar() が空のends[row]を参照し、範囲外読み/誤フィルタ結果。

**確認・対応記録:**

- VARCHAR row-fill no longer publishes fixed-width-only arena; typed fill retains byte arena. DOUBLE row mirrors use bit_cast, NULL cells zero. AuditRowFillWithVarcharRemainsReusable passes with TINYLAMB_DISABLE_TYPED_FILL=1 both before/after; direct arena-path coverage still needed.

### medium-51

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/window_eval.cpp:1252`

**指摘:** ウィンドウ版 APPROX_TOP_SUM が重み値を `w.type == kDouble ? double : static_cast<double>(w.value.int_value)` で読む。Value の本体は16バイト union {int64_t; string_view; double}(value.hpp:564-568)で、VARCHAR の場合は活性でない string_view メンバのポインタ下位8バイトを double として再解釈する( technically UB)ため、巨大な不定の重みになり top-k の順位が破壊される。1481-1485 の numeric_of(VAR/STDDEV/REGR 経路)も同型。80-window doc §1.7 は「異なる型でも明示エラーにしない」ことまでは文書化するが、ポインタビットの再解釈による桁違いの重みには言及がない。

**発生条件:** `SELECT APPROX_TOP_SUM(v, w, 2) OVER () FROM t`(w が VARCHAR)で、全候補の重みがアドレスビット由来の ~1e18 程度の値となり、実重み順と無関係な top-k を返す。

**確認・対応記録:**

- AuditWindowStatisticsRejectNonNumericValues failed before for top-sum/variance/stddev/correlation; checked numeric conversion now rejects wrong types and preserves UINT64 numeric value. Passes.

### medium-52

**判定:** 現行コードで既に対処済み (`already-fixed-current-code`)

**元の対象:** `executor/detail/window_eval.cpp:2486`

**指摘:** ApplyWindows が入力 Relation の全行回収を `input.ForEachRow([&rows](const Row& row) { rows.push_back(row); });` で行い戻り Status を無視。スピル読み出し失敗時、入力の後半行が欠けたままウィンドウ計算・QUALIFY・射影が進行し、エラーなしの誤結果になる。2425-2478 の QUALIFY-only 経路(2455)も同様。直下のコメント(2482-2484)が「spilled row を落とした過去の Production Fix」と明記する重要経路なのに、排出エラーだけが未検査。

**発生条件:** メモリ予算超過でスピル済みの入力にウィンドウ関数付き SELECT を実行中、スピルファイル読み出しが失敗。rows ベクタが前半のみで構築され、RANK/SUM OVER の計算対象が欠けたまま成功ステータスで返る。

**確認・対応記録:**

- Current ApplyWindows checks FinishSpill/GetStatus/ForEachRow; QUALIFY-only path delegates checked FilterRelation. Original line numbers refer to older implementation.

### medium-53

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/executor_feature_test.cpp:1533`

**指摘:** TEST(MergeJoinTest, ResidualDivideByZeroEvaluatesNullNotError) のコメント「Divide-by-zero yields SQL NULL in this engine (not an error)」はエンジン実装と他テストの真実と矛盾する。正準評価器は0除算でNULLではなく StatusError(kIsInfinity, "division by zero") を返す(expression/binary_expression.cpp:937-938・983-991、bytecode.cpp:1328)。さらにこのテストは0除算を一度も評価していない: probe キー1と build キー0は等しくならず、MergeJoin の residual は等キーラン内でのみ評価される(merge_join.cpp:186-204 の PairPasses 呼び出し)ため residual は未評価のまま tail の NULL-pad(merge_join.cpp:313-317)で1行が出るだけ。名前とコメントが主張する「residual の0除算がNULLとしてペアを拒否し成功する」シナリオは未検証で、実際に residual がエラーを出すと実装は executor 全体を失敗させる(merge_join.cpp:109-113 で residual_error_ を latch、334-336 で FailWith)ためコメントの記述は実装上も成立しない。

**発生条件:** regression で residual のエラーが無言で飲み込まれる(例: TryEvaluate 失敗時に residual_error_ を set しない)ように変わっても、本テストは1行+GetStatus成功を期待しているので通ってしまう。逆にコメントを根拠に residual を「0除算=NULL」に「修正」すると、LEFT JOIN residual のエラーが無言の NULL-pad(=誤結果/行の消失)に変わる。dual 評価器・エラー経路の差を構造的に見逃すテスト。

**確認・対応記録:**

- Renamed unmatched-key test to state that residual is not evaluated. Added equal-key 0/0 residual case requiring non-success Status and no padded output; full executor_feature82 pass (batch32).

### medium-54

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/hash_join.cpp:1670`

**指摘:** BuildShards(1670-1672)とRunStripedProbe(1976-1978)がワーカー例外をstd::rethrow_exceptionで生のまま再送出し、NextBatch(Status境界)の外へ例外が逃げる。AGENTS.md不変項1(executor APIはStatus境界を越えて例外を放出しない)の違反。sibling実装は明示的にStatusへ変換している(parallel_merge_join.cpp:406-418のコメント"Never let an exception cross the executor boundary"、parallel_hash_join.cpp:386-400、hash_join.cpp自身のSetupBothSpilled 1881-1903も変換済み)。

**発生条件:** ビルド/probeワーカースレッド内でstd::bad_alloc等が投げられると、error_mutex下で捕獲されたexception_ptrがjoiningスレッドでrethrowされ、Materialize→MaterializeOrThrow→NextBatch→呼び出し元へそのまま伝播する。catchしない Status/size_t契約の呼び出し側で未定義挙動ではなくとも契約違反。適応切替(EmitNextMatch内2227-2231のBuildShards)経由でも同様。

**確認・対応記録:**

- HashJoin NextBatch catches exceptions across both initial materialization and adaptive probe; sticky error and empty chunk. HashJoinConvertsExceptionsToStickyStatus passes.

### medium-55

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/insert.cpp:168`

**指摘:** UPSERT競合行の読み出しで `target_->Read(*txn_, pos).Value()` がHasValueチェックなしにValue()を呼ぶ。StatusOrのValue()は失敗時にLOG(FATAL)+abort(common/status_or.hpp:98-101,124-131「accessing the value of a failed StatusOr is a contract violation and aborts」)なので、Read失敗時にプロセスがabortする

**発生条件:** UPSERT(common/index_unique_on_conflict)実行中にexisting_positionsが指すposのReadが失敗(RowPage::Read→ReadVersionのkNotExists/他)すると、Statusエラーを返すべきところをabortで死ぬ。同関数のFK経路やupdate.cpp:111-114、delete.cpp:36-39は同じReadを必ずチェックしており、ここだけ契約違反

**確認・対応記録:**

- UPSERT checks failed Read before accessing Value; propagates original status. Fault test pending.

### medium-56

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/parallel_aggregation.cpp:1363`

**指摘:** 並列集約のワーカー間SUM併合がthrowingなValue::operator+を使い、オーバーフロー時にStatusではなくstd::runtime_errorがExecute/NextBatchの外へ漏出する(Execute 1589-1741行にtry/catchなし)。逐次実行器は同一状況でStatus(kIsInfinity)を返すため、逐次/並列でエラー経路が不一致。

**発生条件:** SELECT SUM(a) が32768行以上(kParallelAggregationMinRows, plan/parallel_thresholds.hpp:20)と見積もられ2ワーカー以上で実行され、各ワーカーの部分合計はint64に収まるが合計がオーバーフローする(例: 2ワーカーが各々約2^62ずつ加算)。ワーカー内蓄積は__builtin_add_overflowで検査済み(507-548行)だが、Execute メインスレッドの Merge(1633行=fold経路、1732行=turn-taking経路)が Value::operator+(type/value.hpp:443-446 の ExcShimUnwrap がthrow、type/value.cpp:1443-1453 でオーバーフロー→invalid)を評価し、例外が NextBatch を貫通する。

**確認・対応記録:**

- ParallelSumOverflowReturnsStatus reproduced escaped exception, now canonical accumulator merges partial SUM values with checked status; 8192-row test passes.

### medium-57

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/parallel_hash_join.cpp:471`

**指摘:** SharedBuildParallelHashJoinが一致行に必ずprobe側のRowPositionを付ける(out.emplace_back(..., probe_rp)、lookupのコールバックはmatch側のpositionを捨てる515-524)。build_left_が反転した内部結合ではprobe=右入力なので一致行に右行位置が付く。docs/executor/50-join.md:202「内部結合と左外部結合の一致行…には左位置を付ける」および直列版(hash_join.cpp:2199-2203、left_builds時はb.second=左位置)と不一致。

**発生条件:** relational_factory.cpp:1651-1658は推定8192行以上の内部等値結合で本実装を選択。left_bytes < right_bytes(parallel_hash_join.cpp:269-270)で左が小さいとbuild_left_=trueになり、一致出力行のRowPositionが右側の行位置になる。位置を消費する上位計画(DML等のrow position契約、hash_join.hpp:92-94のコメントが意図を記載)で誤った行を指しうる。

**確認・対応記録:**

- SharedBuildKeepsLeftRowPositionsWhenBuildSideFlips failed with right positions; lookup callbacks now carry build position, output selects left position. Both orientations pass.

### medium-58

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/projection.cpp:90`

**指摘:** Affineコンパイル時畳み込み `*multiplier *= constant`(および96-97行 `*addend += constant`)がINT64オーバーフローでUB。畳み込み結果がラップすると、JITカーネル(302-353行)はラップ後定数に対するper-rowオーバーフロー検査のみ行い、AST意味論(((c*A)+0)*B のオーバーフローエラー)と異なる誤値を返す。三層実装(AST=真実/JITミラー)の不一致

**発生条件:** SELECT c*9223372036854775807*9223372036854775807 FROM t を2000万行超のスキャンで実行(jit_threshold_rows_既定20M、あるいは512K行の早期昇格)。AST: 1*Aは成功しA*BでkIsInfinityエラー。JIT: multiplier=A*Bのラップ値(≈1)で1*1を計算し誤値1を返す。構築時の符号付き乗算オーバーフロー自体がUB

**確認・対応記録:**

- AffineFoldingPreservesIntermediateOverflow failed for MAX*MAX and MAX*0 chains; JIT accepts only column*constant+constant matching its overflow stages. Regression compares AST status and passes.

### medium-59

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational.cpp:161`

**指摘:** InlineHavingAliases の HAVING 別名照合が大文字小文字を区別し、他の別名解決(AliasEquals・ORDER BY 別名解決の tolower 比較)と不整合。GroupByPlan 経由の FinishQuery は ResolveGroupingAliases を経ないためこの関数が唯一の別名インライン化になる。

**発生条件:** SELECT SUM(x) AS total FROM t GROUP BY k HAVING Total > 5 のように参照のcaseが宣言と異なる場合、インライン化されず未解決列として評価エラー(4883 は投げる Evaluate なので例外経路)になるか、case違いの実在列に誤 bind して HAVING フィルタが静かに変わり誤結果となる。

**確認・対応記録:**

- HavingAliasesIgnoreIdentifierCase failed before; alias map and input-name check now use case-insensitive identifiers. Passes.

### medium-60

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational.cpp:1701`

**指摘:** DistinctOf の DISTINCT ON 経路だけ重複検査が seen_keys ベクタの線形走査で行われ、行数に対し O(n²) の比較(EqualUnderCollation 込み)になる。平文 DISTINCT 経路は WorkTableIndex(ハッシュ)を使っており処理が不均衡。

**発生条件:** DISTINCT ON を数十万行の入力に使うとこのプロジェクトが同型の線形走査を「320k行の再帰が分単位」かかったとして WorkTableIndex で置き換えた経緯(relational.cpp:3692-3696)と同じ顕著な性能崩壊が再現する。

**確認・対応記録:**

- DISTINCT ON uses WorkTableIndex over canonicalized key rows instead of quadratic vector scan; input/output FinishSpill errors also checked. Focused regression pending.
- WorkTableIndex replaces quadratic DISTINCT ON scan; SQL DistinctOnKeepsFirstRowWithCollation passes (A/a/B/NULL and first-row semantics).

### medium-61

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational.cpp:1758`

**指摘:** ORDER BY の整数序数を位置ではなく列「名」参照へ変換するため、出力スキーマに同名列が重複していると先頭の同名列に解決され、順序キーが別列にずれる(誤順序=誤結果)。

**発生条件:** SELECT a AS x, b AS x FROM t ORDER BY 2 — 序数 2 は b 列を指すが、名前 "x" での再解決は Schema::Offset/ResolveOffset の先頭一致(type/schema.cpp:176-189, expression/column_value.cpp:56-70)で列 1(a) に落ちる。Project 側の collated-ordinal 経路(relational.cpp:1255-1268)と序数経路(1283-1291)も同型。

**確認・対応記録:**

- OrderByOrdinalKeepsDuplicateColumnPosition failed before; duplicate physical output bindings renamed internally and deferred relational ordinals read positional slots. SQL example passes, additional collated/star/grouped cases pending.

### medium-62

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational.cpp:4533`

**指摘:** stream_agg 経路の集約評価が投げる Evaluate(EXC-SHIM ラッパ)を使い、評価エラーが Status/GetStatus ではなく std::runtime_error として実行器境界を巻き戻る。AGENTS.md 不変項1(例外を Status 境界越えさせない)違反で、同じパターンを WHERE 経路で修正した経緯(relational.cpp:2770-2777 の oracle-fuzzer find)と矛盾する。

**発生条件:** 単一テーブル GROUP BY で集約引数/FILTER 述語/HAVING の評価がエラーになる入力(例: SELECT k, SUM(CAST(s AS INT64)) ... GROUP BY k の s に非数値文字列)。TryEvaluate なら Status が返るが、この経路では ExcShimUnwrap が throw し ExecuteQuery→Initialize→NextBatch を巻き戻し、SqlEngine 境界の catch(query/sql_engine.cpp:241 等)のみが受ける。executor を直接 drain するテスト/ファザーでは未捕捉になりうる。

**確認・対応記録:**

- Remaining stream aggregate FILTER/argument/HAVING evaluations now use TryEvaluate and propagate Status. Direct error regression pending.
- Checked TryEvaluate for streaming FILTER/argument/HAVING. StreamingAggregateErrorsStayInStatus passes; full relational 174 tests pass.

### medium-63

**判定:** 現行コードでは指摘を確認できず (`not-reproduced-current-code`)

**元の対象:** `executor/relational_factory.cpp:1284`

**指摘:** MultiIndexCoveringScanPlan::EmitExecutor が、同一インデックスルートを持つ2参加者に対し held 内の同一 IndexReadGuard を二度 std::move し、2個目の参加者に空(無保持)ガードを渡す。index-only 読みを守る read gate 契約がその参加者について破れる。

**発生条件:** 覆い走査の参加者選定(implementation_rules.cpp:1460-1495 の貪欲選択はインデックス同一性を確認しない)で同じインデックスの2範囲が選ばれると、ルート重複になり、2個目の参加者はガード無しでインデックスを読む。並行書き込み(キー除去/構造変更)が排他されず、古い/壊れた索引読みや誤結果の可能性。

**確認・対応記録:**

- A single read gate on a duplicate root remains owned by the first participant for the entire scan, protecting all reads of that root. Passing an empty second guard disables that participant's guarded heap-image cache; it does not release the first guard. Greedy covering selection gains no uncovered slots by selecting the same index again. No unsafe unguarded root access established.

### medium-64

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/skip_scan_distinct.cpp:111`

**指摘:** ResolveCurrentRowがPage::Read失敗をStatusに記録せず行を非可視扱いにする。ヘッダの自前の契約(skip_scan_distinct.hpp:79-81「Sticky page/decode failure…first failure is kept here for GetStatus()」)および同関数内のGetPage失敗経路(97-104行、status_へ記録)と不整合で、エラーが飲み込まれstatusはkSuccessのまま

**発生条件:** インデックスエントリが指すposのスロット読み出しが失敗(RowPage::Read→txn.ReadVersionがkNotExists等を返す、page/row_page.cpp:31-38)したとき、committed行がDISTINCT代表から静かに欠落し、クエリは成功扱いで完了する→DISTINCT値の欠落(データ欠落・誤結果)。GetStatus()では検出できない

**確認・対応記録:**

- SkipScanDistinct records non-kNotExists Read failures. kNotExists still skips legitimately stale/invisible index entries. Fault test pending.

### medium-65

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/unnest.cpp:69`

**指摘:** DATE宣言要素の正規化で、粗いsscanf("%d-%d-%d")ゲートを通した文字列をthrowing wrapperのValue::Dateに渡す。TryParseDateDaysは月>12・閏日違反・前後空白を拒否する(type/date.cpp:84-86,123-126)ため、ゲートを通った不正テキストがExcShimUnwrap経由でstd::runtime_errorをthrowし、Status境界を越えて脱出する

**発生条件:** 要素型DATEの配列にテキスト ' 2026-01-02'(先頭空白)や '2026-13-01' が混在すると、sscanfは3項目マッチするがValue::Dateがthrow(common/exc_shim.hpp:29-31)。UNNEST実行がStatusではなく例外で失敗し、SqlEngine最上位のcatch(query/sql_engine.cpp:241-243)に依存する。AGENTS.md不変条件1(実行器APIから例外を逸脱させない)違反。姉妹実装のset_operation.cpp:128-133はTryDateでStatus伝播させる

**確認・対応記録:**

- DATE array element coercion uses Value::TryDate and propagates Status rather than sscanf plus throwing Date; direct malformed array test pending.
- Unnest DATE uses checked conversion, sticky NextBatch status. MalformedDateUnnestReturnsStatus passes for invalid dates and whitespace.

### medium-66

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/bytecode.cpp:141`

**指摘:** EXTRACT month/day のVMカーネル(TryExtractDatePart)はDATE入力をyear_month_dayから取得するが、AST参照実装はFormatDateDays文字列のsubstr(5,2)/substr(8,2)をstollする。年が4桁に収まらない(|year|>=1000、符号込み5文字以上)とAST側のスライスが"-0"等ずれ、ASTは0、VM/高速版は真の月・日を返す。bytecode.hpp:39-40の「AST evaluatorをstring-slice+stollで完全再現、差分カバレッジが成立する」というコメントも満たさない。

**発生条件:** EXTRACT(MONTH FROM DATE '-1000-01-01')。日付値は合法(kDateMinDays=-11000000, type/date.hpp:17)。AST(function_call_expression.cpp:3431-3445)は"-1000-01-01"のsubstr(5,2)="-0"→stoll=0を返すが、VM(bytecode.cpp:141-143)と高速版(bytecode.cpp:1449-1454)はymd.month()=1を返す。bytecode有効/無効(TINYLAMB_DISABLE_BYTECODE)で結果が変わり、dayも同様に0対1で発散。年>=10000(例: 10000-01-01)でも同様にsubstr(5,2)="-0"で発散する。

**確認・対応記録:**

- DATE EXTRACT uses chrono fields, no substring/year truncation. Negative and 5-digit year differential failed before/passes after across AST, bytecode, relational; differential 17 and bytecode 17 pass.

### medium-67

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/function_call_expression.cpp:5222`

**指摘:** IFERROR の EvaluationContext オーバーロードだけが分岐結果の FLOAT64 正規化(IfBranchesPromoteToDouble + NormalizeIfBranch)を欠き、同一ファイルのコメントが保証する3オーバーロード一貫性が破れている。plain(5007-5017)・two-schema(5119-5134)は正規化し、relational 側(function_eval.cpp:833-848)も normalize する。作業ツリー未コミット差分では two-schema 側へ今まさに同じ修正が適用されたが context 側は旧形のまま取り残されている。

**発生条件:** IFERROR(int_expr, 0.5) を context 経由(A1移行の呼び出し規約。現状の本番呼び出し元は未確認、単体テストが使用)で評価すると INT64 のまま返り、plain/join/relational 経路では FLOAT64 に昇格する。評価経路で型と値が割れる。

**確認・対応記録:**

- IfErrorContextMatchesPlainPromotion reproduced INT64 vs DOUBLE; context overload now normalizes both branches like the plain overload. Passes.

### medium-68

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/in_expression.cpp:49`

**指摘:** リテラルリスト IN は collation 衝突を検査せず CI タグ付き比較を無条件に折り畳むのに対し、サブクエリ IN(AST と relational の両方)は衝突タグをエラーにする。AST 内の2つの IN 実装(InExpression と QueryExpression IN)が同一セマンティクス形状で受容/拒否が割れる。

**発生条件:** WHERE collate(c,'und:ci') IN (collate(c2,'binary')) は折り畳み一致で TRUE/FALSE を返すが、同形の WHERE collate(c,'und:ci') IN (SELECT collate(c2,'binary') FROM t) は "Collation conflict between the IN operands" で失敗する。

**確認・対応記録:**

- Literal and subquery IN now reject explicit string collation conflicts consistently, keeping prefix short-circuit evaluation. Numeric unsigned marker is not a collation. Membership hash uses only uniform exact domains and declines CI probes; rewrites/bytecode retain the check. Differential, bytecode, rewrite, SQL audit and collation suites pass batches62/63 (error message assertion corrected to inspect execution status).
- Batch66 ASan/UBSan differential, bytecode, rewrite and SQL collation suites pass; string singleton NOT IN stays an IN operation to preserve conflict errors.

### medium-69

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/json_doc.hpp:295`

**指摘:** JSONパスの配列インデックス蓄積がオーバーフローする。NavigateJsonPathはint idxに idx*10+digit を蓄積(json_doc.hpp:292-297)し、11桁以上で符号付きintオーバーフロー(UB)。ラップ結果が偶然正の境界内に収ると、範囲外パスが範囲内要素を返す(誤結果)。ParsePathSegmentsのint64_t版(json_doc.hpp:399-404)は20桁以上で同様に溢れ、JSON_SET/INSERT/REPLACE/REMOVEの変異先インデックスが誤る。

**発生条件:** SELECT JSON_EXTRACT('[1,2,3]', '$[4294967297]')。idx蓄積が2^32+1でUB(実装上はラップし1)、境界チェック(json_doc.hpp:301-304)を通りarr[1]=2を返す。正しくは範囲外→NULL。JSON_SET('[1,2,3]','$[99999999999999999999]','x')系も同様にラップした小さいインデックスで変異し得る。パスはユーザー入力の文字列そのまま。

**確認・対応記録:**

- JsonPathIndexesNeverWrap reproduced $[4294967297] reading element 1 and int64 mutation path wrapping. Checked digit accumulation rejects oversized indexes in both path parsers. Passes; UBSan still pending.

### medium-70

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/proto_text.cpp:1354`

**指摘:** SetFieldInBodyのパース失敗扱いが非対称で、保存済みTEXTペイロードがプロトテキストとして解釈不能な場合の挙動が静かに分岐する: (a)部分エントリが取れた場合はnullopt(無変更)を返し、(b)ゼロエントリの場合は元ペイロードを捨てて新フィールドだけ(またはクリアで空文字)のペイロードへ置き換える。sql_engineのUPDATE経路はnulloptでも行を変更済みとして数えるため、行数報告が嘘になる。

**発生条件:** プロト型カラムに文字列として挿入した非プロトテキスト("garbage$$$")を UPDATE ... SET col.field = 1 で更新。ParseProtoTextEntries失敗+エントリ0→(b)でcellは"field: 1"だけに無置換ドロップ、または部分パース時は(a)でcell無変更のままsql_engine.cpp:2368-2373がmodified_rows++して「Update Rows: N」を報告。ヘッダ契約(proto_text.hpp:59-62)に失敗時の記載がない。

**確認・対応記録:**

- Malformed proto body now returns InvalidArgument, including partial parse and nested edits. SQL caller uses Try APIs. Reproduced data overwrite before; after unit19 + SQL16 pass and original payload stays intact.

### medium-71

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/rewrite.cpp:1331`

**指摘:** identity_divide_oneのガードはStaticallyNonNumeric(left)だが、これは空のSchema()に対してResultTypeを解くため(rewrite.cpp:493)全てのカラムがcatch経由でfalseになり、型を問わず書換が発火する。コメント(rewrite.cpp:1326-1330)は「int型x/1は恒等式ではない」「ガードがNULL/unknown型のみ発火させる」と主張するが、VARCHAR等の非数値カラムでも発火し、ASTが「type mismatch」を投げる場面でエラーが消える。kValue文脈ではMakeValueRulesが除去するが(bytecode.cpp:58-79)、kFilter文脈では有効。

**発生条件:** WHERE s_varchar / 1 (s_varcharはSTRINGカラム)。BytecodeCompiler::Compile kFilter(selection.cpp:158-159, scan_filter.cpp:2139-2140)のDefault()ルールでWHERE s_varcharへ折りたたまれ、AST参照(非数値/非 DOUBLE除算はbinary_expression.cpp:1060でtype mismatchエラー)と異なり全非NULL行が通過する。TINYLAMB_DISABLE_BYTECODE差分で片側のみエラーになる。

**確認・対応記録:**

- RewriteKeepsUnknownColumnTypeErrors reproduced VARCHAR / 1 becoming bare column. identity_divide_one now requires statically DOUBLE. New regression passes; legacy shape expectation updated to retain division.

### medium-72

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/rewrite.cpp:333`

**指摘:** ExpressionCannotThrowがkInExpの定数アイテムと列比較を「by design」でtotalとみなす(rewrite.cpp:295-307, 324-336)が、型不一致の比較(col IN ('abc'))はASTでエラーになる(binary_expression.cpp:1060、in_expression.cpp:49のTryMatches)。この述語にゲートされた書換(absorption_and/boolean_identityのx AND FALSE/uniform_case_result/or_of_ranges_to_inのバリア判定等)がエラーを消し、AST差分が発生する。

**発生条件:** SELECT ... WHERE NULL AND (NULL OR col_i IN ('abc')) (col_iはINT64カラム)。AST: NULL AND右辺→右辺評価→NULL OR (col IN ('abc'))→メンバーシップ比較でtype mismatchエラー。書換後: absorption_and(rewrite.cpp:1214-1225, ExpressionCannotThrow(y)=true誤判定)がNULL定数へ折りたたみ、エラーなく全行拒否。bytecode有効時のみ静かにフィルタされる。

**確認・対応記録:**

- Unknown-schema IN and equality cannot be proven error-free. New NULL AND (NULL OR n IN (abc)) regression preserves type error; OR-to-IN groups only consecutive same-target alternatives to retain evaluation order. Full rewrite suite currently in validation.

### medium-73

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `page/page_manager.cpp:1056`

**指摘:** recovery_invariants.md:18-21 と wal_format.md:35-41 の「<db>.dbはbegin_lsn/DPT/ATTを保存し、replay開始はmin(begin_lsn, min_dpt_rec_lsn, min_att_first_lsn)」は偽。チェックポイントヘッダはreplay_start_lsnとmax_committed_txn_idのみで、DPT/ATT構造は存在しない。

**発生条件:** リカバリ窓・WAL保持期間の解析をDPT/ATT前提で行うと実際の再開点と食い違う。実再開点は min(外部checkpoint_lsn=常に0, .dbのreplay_lsn) をanchorとし max(anchor, log_start) から走査。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-74

**判定:** テストで指摘を否定 (`refuted-tested`)

**元の対象:** `page/row_page.cpp:55`

**指摘:** RowPage::Insert のホール走査が、自トランザクションが既に保持する write intent(同一 txn の先行ステートメントが削除したスロット)に対して txn.ReleaseWriteIntent(hole) を呼ぶ。TryAddWriteSet は write_set_ 既存エントリに対して true を返し(transaction.cpp の TryAddWriteSet)、ReleaseWriteIntent は manager からの解放成功時に write_set_ からも erase するため、自 txn のステージ済み DELETE の write set 保護が後続の INSERT によって黙って剥がされる。

**発生条件:** 同一 txn T1 がマルチホールのブロックで `DELETE`(スロット3を削除、intent+staged version 保持)の後 `INSERT` を実行。走査順にスロット1(他の理由で空き、SnapshotSeesRow=false)が先に hole_slot に選ばれると、スロット3では TryAddWriteSet→true(write_set_ 既存)だが hole_slot!=row_max のため 55 行の ReleaseWriteIntent(pos3) が実行され、T1 自身の intent が解放・write_set_ から抹消される。以後 T1 のステージ済み DELETE は競合検知の保護を失い、他 txn が同一スロットに intent 取得・書き込みでき、コミット時の LSN ガード(page_manager.cpp:671/689)で一方が黙って棄却され得る。SnapshotSeesRow は自 staged delete に対して false を返す(transaction_manager.cpp:1404-1409)ため「自スロットは選択対象になる/保護が必要」である点も確認済み。

**確認・対応記録:**

- ReleaseWriteIntent refuses staged pending writes, so scanning a later deleted hole cannot release its owner intent. Real page regression deletes two slots, reuses first, verifies third remains reserved against another transaction and abort restores all rows. Passes without production changes (batch35 before AuditHoleProbe).

### medium-75

**判定:** 重複指摘 (`duplicate`)

**元の対象:** `plan/cascades.cpp:4679`

**指摘:** outer_to_anti_join の resolves_to_right が IsSameTable 経由で保護側カラムを null供給側に誤認する(4676-4684行)。`保護側カラム IS NULL` を ANTIJOIN 変換の対象とみなし、ColumnIsDeclaredNonNull(4712-4716行)が保護側スキーマで判定されるため NOT NULL 宣言カラムでも通る → 元クエリ(必ず空)を非空の AntiJoin に置き換える誤結果ルート。

**発生条件:** FROM orders LEFT JOIN orders_1 ON ... WHERE orders.id IS NULL(orders.id は NOT NULL 宣言)。元の結果は常に0行。resolves_to_right("orders.id") が IsSameTable("orders_1","orders")=true で右側归属と誤認 → 4718-4728行で AntiJoin(orders, orders_1, ON) を追加し、マッチしない orders 行が復活する。発火には分岐局所(branch_local)等で `IS NULL` 単関係コンジャンクトがスキャンフィルタに押し込まれない文脈が必要(通常経路では split_selection_over_join のフィルタ併合が偶発的に結果を空に保つ)。

**確認・対応記録:**

- Same suffix-based relation proof issue as high-16.

### medium-76

**判定:** 防御を追加＋関連テスト成功 (`defensive-fix-tested`)

**元の対象:** `plan/cascades.cpp:769`

**指摘:** infer_join_predicates/inferred_inequality_pushdown が InferJoinConstants(736-777)/InferJoinInequalities(1004-1066) 経由で、結合等値+定数から導出したコンジャンクトを共有スキャングループの filter に MergeScanFilter する際、他の scan 強化経路が全て通す branch_local/共有消費者ガード(SelectionMayStrengthenSharedScans)を通さない。導出コンジャンクトは「その結合を通る行」でのみ有効で、スキャングループの filter はグループ全局に適用される(cascades.hpp:169-171)ため、契約違反。

**発生条件:** 呼び出し側は 3069-3079(infer_join_predicates)と 6198-6208(inferred_inequality_pushdown)で ContainsRelation チェック(759-761)のみ行う。スキャングループに「推論元の結合を経由しない消費者」が存在するメモ形状(例: full_outer_join_decomposition の anti 分岐が生の t2 ベースグループを読む等、branch_local コメント 515-539 が想定する共有スキャン形状)では、t2.y=const の大域フィルタがその消費者の行を落とす。

**確認・対応記録:**

- Original report itself refutes SQL reachability; not claimed as reproduced SQL wrong result. Handbuilt branch_local join with equality/inequality inference reproduced modifying shared b scan.
- Both inference rules now honor SelectionMayStrengthenSharedScans. Direct isolated-rule regression plus all209 cascades tests pass.

### medium-77

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/cascades_search.cpp:1461`

**指摘:** 必須順序付けの sort_ascending 欠損時の既定が OptimizeGroup では「全 ASC」(1461-1464行)なのに、本番のルート要求構築(plan/optimizer.cpp:3608-3627)は sort_ascending/sort_nulls_first を一切設定しない。その結果、DESC を1つでも含むカラム ORDER BY が、正しい順序を届けている TopN/Sort プランに対して常に行数log行数の幻のソート罰金(1573-1579行)を課され、ルート代替間のコスト比較が歪む。また PhysicalProperties::Key()(786-798行)は欠損を 'd'(DESC)としてエンコードし、ASC既定と矛盾するため、同一 SearchEngine 上で昇順既定と明示DESCの要求が同一の best_ キャッシュキーに衝突しうる。

**発生条件:** ORDER BY a ASC, b DESC LIMIT 10(全 order 式がカラム)→ properties.ordering=[a,b]、sort_ascending 空で Optimize される → required_ascending=[true,true] となり、実際に (a ASC,b DESC) を届ける TopNPlan が IsOrderedBy 失敗 → 全 TopN 代替に幻の罰金。代替間の estimated_rows 差で計画選択が変わる。キャッシュ衝突は ordering 同一・sort_ascending 空vs明示[false]の2呼び出しが同一キーになる API 利用で発火。

**確認・対応記録:**

- PhysicalProperties memo key defaults absent direction to ASC consistently with OptimizeGroup; optimizer root copies direction and NULL placement. Missing/default ASC key collision regression failed before; cascades207/plan61 pass (batch27).

### medium-78

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/implementation_rules.cpp:4520`

**指摘:** 集合演算(UNION/INTERSECT/EXCEPT系)の実装規則が、出力行数推定を children.front() のみで決めている。UNION(distinct)の真の出力は子の和(最大sum)であり得るのに第1子の行数をそのまま使い、第1子が空なら推定0になる。依頼の観点『統計伝播(estimated rows崩壊)』に直接該当する。

**発生条件:** `SELECT a FROM t1 UNION SELECT a FROM t2` で t1 が空・t2 が数百万行のとき、推定0行がこのノードの上に伝播し、後段のjoin方向・build側選択・spill判定が崩壊する(誤結果ではなく顕著な性能崩壊)。

**確認・対応記録:**

- Set operation cardinality uses sum (UNION conservative upper bound), minimum (INTERSECT), first child (EXCEPT); output stats match all-branch estimate. Empty-first UNION and INTERSECT regressions plus plan_extra12/plan60 pass (batch25).

### medium-79

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/index_only_scan_plan.cpp:81`

**指摘:** IndexOnlyScanPlan の IndexRangeStats がカーディナリティの「halfway blend」補正を無条件で適用している。双生子である plan/index_scan_plan.cpp の同名関数は equality キーが存在する場合のみ補正をかけ、その理由をコメントで明記している(「leading range key には anchor がなく、blend は range の実選択度を約半分に読み誤り、full scan に対して index を不当に high cost にする」)。index_only 側はこの修正を受けておらず、カバリングインデックスの leading range 述語が約2倍に過大見積りされる。

**発生条件:** カバリングインデックス(key=(ts,...) など)を持ち、`SELECT ... FROM t WHERE ts >= '2026-01-01' AND ts < '2026-02-01'` のように先頭キー列に範囲述語だけを付けるクエリ。IndexOnlyScanPlan のコンストラクタ(index_only_scan_plan.cpp:122-126)が begin_key 非空で IndexRangeStats を呼び、81行目の無条件 blend が実选择性を約2倍に膨らませ、同じ範囲の IndexScanPlan/FullScanPlan と比べて不当に高コスト評価され full scan が選ばれる(顕著な性能崩壊)。

**確認・対応記録:**

- Index-only range estimate applies equality-prefix damping only with equality keys, matching IndexScanPlan. Leading range differed 200 vs 600 before; both agree after. plan suites pass (batch25).

### medium-80

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/optimizer.cpp:3628`

**指摘:** ルート物理要件の limit_hint を `query.limit_offset_ + query.limit_count_` で size_t のまま加算しており、桁溢れガードがない。同じ和は cascades_search.cpp:1425-1430(kTopN の子要件)ではオーバーフロー検査付きで max にクランプされており、こちらだけ素通し。wrapped な hint が CanPushLimitIntoFullScan(plan/implementation_rules.cpp:1552-1556)の scan_limit に流れると、走査が先頭 hint 行で打ち切られ、後段 LimitPlan の OFFSET 分の行が欠落し得る。

**発生条件:** `LIMIT <巨大値> OFFSET 5`(和が wrap して hint < 必要行になる組合せ)の順序なしSELECTで、FullScanが wrap 後の行数で打ち切られ OFFSET 以降の行が返らなくなる。ただし SQL フロントエンド(parser/外部AST)が size_t 全域のリテラルを通すかは本領域外で未検証 — コード側のガード欠落(双子パスとの不整合)は確実。

**確認・対応記録:**

- Saturating root limit_hint matches child TopN guard. SQL LIMIT UINT64_MAX OFFSET5 reproduced empty result before; correct [5],[6] after.

### medium-81

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast.cpp:128`

**指摘:** 生の改行を含む文字列リテラル(トリプルクォート等)のダンプで、改行前のテキストが ` [数字-数字]` で終わっていると ParseLocation が1行目で偽成功し、複数行結合ループが short-circuit する。from_chars が末尾ジャンクを許容する(36-39: "2]" が正常パースされる)ためで、残りの行が別ノードとして解析され文全体が kUnknown で拒否されるか、継続行のインデントが偶然有効深度なら木が静かに壊れる。

**発生条件:** SELECT '''see [1-2]\nnotes'''; — ダンプ1行目 `Literal ('see [1-2]` が位置情報 [1-2] と誤認されノード化、2行目 `notes') ...` がインデント0で別行解析 → root 重複で Status::kUnknown(149-157, 164-166)となり妥当なクエリ全体がエラー。リテラル継続行が2の倍数スペースで始まる場合は誤って子ノード接続され木が壊れる。

**確認・対応記録:**

- Dump label continuation now recognizes SQL literal closing quotes rather than raw parentheses or location-like text inside strings. Five multiline literal cases pass including fake [1-2], parentheses, malformed location-like suffix. from_chars requires complete bounds consumption and ordered locations. Source parser verified against actual pinned execute_query dumps; initial pre-fix test had a malformed SQL fixture, so no valid before-runtime reproduction claimed. Full AST54 passes (batches30-32).

### medium-82

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:1543`

**指摘:** 深いネストの AST に対する無制限再帰が ExpressionDepthGuard の外に複数残る。RejectUnsupportedHints は全 Statement の最初(7997-8000)に全木を再帰走査(1543-1549)し、CloneAstNode(1340-1352)、AnalyzeUdfBody(1490-1492)、CollectFromBoundNames(3009-3011)も同様に深さ上限を持たない。ファイル自身のコメント(246-254)が「1+1+... を10万個連結した式はパーサを通る・スタックオーバーフローは回復不能」と明言しており、96 で打ち切られるのは VisitExpression のみなので、ガードが発火する前にこれらの走査が先にスタックを溢れ SEGV しうる(ASan ビルドでは実用深度 ~140 と同コメントが記す)。

**発生条件:** SELECT 1+1+1+...(10万項連結); のような深いダンプが到着したとき、kMaxExpressionDepth=96 のガード(3523-3527)より前に RejectUnsupportedHints がダンプ深さ相当の再帰を行い、スタック溢れでプロセスが落ちる。CREATE TEMP FUNCTION の深い本体でも CloneAstNode/AnalyzeUdfBody が同様。

**確認・対応記録:**

- Bound AST depth at 128 during dump construction and iteratively validate hand-built trees before any recursive visitor helper. Prevents pre-expression-guard hint/UDF/clone traversals and recursive tree destruction from unbounded depth. Before regression accepted depth512; after rejects via Status, boundary parse and full AST54 pass. ASan verification pending batch33.

### medium-83

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:4439`

**指摘:** ValidateStructCastCoercibility(1613-1691) の Status 戻り値が無視されている。関数は CAST(STRUCT literal AS STRUCT<...>) のフィールドごと coercibility 検証で失敗時に AstStatus("Cannot coerce struct field N to T") を返す(1686)が、呼び出し側は戻り値をチェックせずエラーを飲み込み、CAST がそのまま実行経路へ進む。検証の目的(1608-1612 のコメント: GoogleSQL は coercion 不能フィールドで raise する)が自壊する。

**発生条件:** CAST(STRUCT('abc') AS STRUCT<x INT64>) のような文字列→INT64 へ coerce 不能なリテラル構造体キャストが、本来の prepare 時エラーではなく実行時経路に流れる(静かに通るか、別の誤結果/遅延エラーになる)。

**確認・対応記録:**

- STRUCT literal cast validation result now propagates; SAFE_CAST returns NULL on failed validation. Invalid field cast regression failed before; full AST54 and SQL22 pass (batches29-32).

### medium-84

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:6096`

**指摘:** Visit の source が空(デフォルト引数 {})のとき、単項 UNION(= UNION DISTINCT)が kUnionAll に静的劣化する。head_kind の初期値が kUnionAll で(6096)、per-pair の SetOperationMetadata からの復元は SliceSource が空文字を返すと continue でskipされる(6119-6121)ため、分散(dedup)が静かに失われる。本番の sql_engine.cpp:1864 は常に source を渡すが、公開 API のデフォルト呼び出し(テスト・ファザー等)では仕様上通る道。

**発生条件:** GoogleSqlAstVisitor::Visit(*ast) のみで (SELECT 1 UNION SELECT 1) を訪問 → SetOperationMetadata の SetOperationType/AllOrDistinct が SliceSource 空のため復元されず、head_kind 既定の UNION ALL で組まれ、結果に重複行が残る。

**確認・対応記録:**

- Source-less UNION defaults to DISTINCT; ALL requires explicit ALL detail. Hand-built dump regression covers UNION, UNION ALL and UNION DISTINCT; failed before, full AST54 passes (batches29-32).

### medium-85

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:6255`

**指摘:** 集合演算経路の OFFSET 解析(6255-6267)は IntLiteral 子のみを扱い、定数畳み込み可能な式(テンプレートバインド後の cast(3 as int64) 等)は一切エラーを出さずオフセット0のまま通す。パイプ経路の LIMIT/OFFSET(5920-5935)も同様に非リテラル OFFSET を静的ドロップする。単一 SELECT 経路(6971-6984)は fold_int で畳んでから "OFFSET requires an integer literal" と騒がしく失敗するのに比べ、行スキップが静かに消えて誤結果(余分な行返却)になる。

**発生条件:** SELECT 1 UNION ALL SELECT 2 LIMIT 5 OFFSET cast(2 as int64) のような定数式 OFFSET(6889-6892 のコメントがテンプレート置換後にこの形が来ることを明記)が集合演算側では無視され、offset 0 で全行返る。単一 SELECT ならエラーになるため経路間で挙動不一致。

**確認・対応記録:**

- Set-op LIMIT/OFFSET evaluates immutable column-free constant expressions and propagates invalid bounds. Current raw cast OFFSET failed with an error rather than silently using zero; Offset wrapper ignored it. Direct and wrapped CAST offset regressions now pass with full AST54 (batches29-32).

### medium-86

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:6415`

**指摘:** NamedWindowsScope が previous を保存せず(previous は常に空)、デストラクタの t_named_windows.swap(previous) が実質「マップ全消去」になる。ネストした任意の VisitQuery(選択リスト内のスカラーサブクエリ、WHERE サブクエリ、FROM 派生表、VIEW 展開、CTE 本体)が完了するたびに外側クエリの名前付きウィンドウ定義が消え、以後の OVER w 解決が失敗する。

**発生条件:** SELECT (SELECT 1), SUM(x) OVER w FROM t WINDOW w AS (ORDER BY y); — 1番目の選択項目のサブクエリで VisitQuery がネスト実行され、戻ると同時に t_named_windows が空になる。2番目の OVER w が 3234 で "GoogleSQL AST: unknown window w" エラーとなり、妥当な SQL が拒否される。ORDER BY 句の OVER も FROM 派生表の後(6822)なので同様に壊れる。

**確認・対応記録:**

- Named window scope now saves/restores enclosing definitions; NestedQueryPreservesEnclosingNamedWindow failed before and passes after.

### medium-87

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:7946`

**指摘:** ValidateSelectAsProjections 内の RequireProtoFieldsPresent(7806-7825) の Status 戻り値が無視されている。SELECT AS <proto> 経路では required proto フィールドの未割り当てが検出されてもエラーが捨てられ、宣言と無関係に通ってしまう。NEW <proto>(...) 経路(7857-7863)は同一検証を正しくチェックしており、二経路で契約が不一致。

**発生条件:** SELECT AS pkg.RequiredFieldMessage(1) のように required フィールドを割り当てずに SELECT した場合、NEW 経路なら "Required protocol message field ... is not assigned" で拒否されるのに対し、SELECT AS 経路は無言で成功する。

**確認・対応記録:**

- SELECT AS proto now propagates required-field validation just like NEW. Missing required field regression failed before; full AST54 passes (batches29-32).

### medium-88

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor_dml.cpp:815`

**指摘:** ExpandPivotSource が素テーブル source(base.query == nullptr)の PIVOT を受けたとき、グループキーを1つも収集できない。キー抽出は if (base.query) ガード内しか存在せず(815-824)、代替の推論経路(907-919)は select リストから非集計項を集めるが素テーブルの場合 select リストは PIVOT 集計列だけなので空になり、GROUP BY なしの全体集計サブクエリが生成される。BigQuery PIVOT は「PIVOT 句で参照されない列すべて」がグループキーなので、入力が1行に潰れて誤結果になる。名前比較も pivot_col_name/agg_column_names がケースセンシティブ(817-818)で、コードベース全体の IdentifierEquals 慣行と不整合。

**発生条件:** FROM t PIVOT(SUM(x) FOR c IN ('a', 'b')) のように素テーブルへ直接 PIVOT する形(ビジタが base.query==null の Shape を明示的に処理するため想定内の入力)では、t の残り列がグループキーになるべきところ全体集計になり1行のみ返る。サブクエリ経由では (SELECT dept, sales FROM t) PIVOT(...) のように両列が PIVOT 句で参照される場合に限り正しく1行になるが、素テーブルでは残り列をグループ化できず静かな誤結果になる。

**確認・対応記録:**

- PIVOT without an explicit named derived projection now fails with a diagnostic rather than silently collapsing all groups. Derived-column matching is case-insensitive. Base-table and star expansion remain unsupported until catalog binding exists. SQL audit, AST, frontend suites pass batch63.

### medium-89

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_frontend.cpp:829`

**指摘:** ExtractFetchWithTies が FETCH FIRST/NEXT を伴わない素の "with ties" を WITH TIES マーカーとみなし、元テキストから9バイト無条件削除する(886-894)。リテラル/コメントはマスク済みだが識別子との衝突は見ていないため、ties という名前の CTE を含む正当な GoogleSQL が不正 SQL に破壊され、パースエラーになる(音を立てて壊れるが正当文が拒否される誤動作)。

**発生条件:** WITH ties AS (SELECT 1) SELECT * FROM ties; — マスク上に "with ties" が見つかり(829-831)、FETCH がないため erase(pos, 9) が走り "WITH  AS (SELECT 1) SELECT * FROM ties" となってパースエラー。同様にサブクエリ内 OFFSET を持つ FETCH FIRST 文も offset_pos が最初の "offset" を拾うため(839-873)カット位置がズレて不平衡な SQL になる。

**確認・対応記録:**

- WITH TIES preprocessing only identifies a ties modifier after a LIMIT/FETCH clause. Bare CTE named ties, with/without LIMIT, failed before and now succeeds; frontend28/AST54/SQL22 pass (batches29-32).
- Outer FETCH WITH TIES prepass now scans balanced parentheses and identifier boundaries after masking literals/comments; nested WITH ties and limit_prefix identifiers cannot be mistaken for the outer clause. Additional regression passes with SQL audit23 and frontend28 batch36.

### medium-90

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/query_test.cpp:1362`

**指摘:** QualifiedStarWithUnknownRelationFails が、Executor::Next() から飛び出す std::exception を「リジェクト成功」として扱う。ループ後に GetStatus() を確認しないため、エラーが Status 経路で清洁に返される実装に修正すると逆にテストが失敗する。つまり例外の Status 境界外への逸脱(AGENTS.md 不変項1「query APIs ... must not let exceptions escape across Status boundaries」違反)を必須挙動として固定している。

**発生条件:** SELECT no_such_rel.* FROM t1 を実行 → Prepare が成功し Next() が例外を投げる現行実装では rejected=true で合格。将来「曖昧/未知の修飾子を drain Status のエラーに変える」修正を入れると、例外を投げず status!=kSuccess で終わるため rejected が false のまま EXPECT_TRUE(rejected) が失敗し、契約修正が妨げられる。

**確認・対応記録:**

- Query rejection tests now drain without expecting exceptions and inspect GetStatus; ambiguous-column diagnostic asserted through Status. Both targeted tests pass (batch23).

### medium-91

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/query_test.cpp:5059`

**指摘:** SqlEngineSelfJoinAmbiguousColumnRejected も同様に、Next() から逸脱する例外のみを「リジェクト」と判定し drain Status を見ない(5067 EXPECT_TRUE(rejected))。AGENTS.md 不変項1の Status 契約に反する例外逸脱をテストが要求値として固定しており、Status 経路への修正を防ぐ。

**発生条件:** SELECT k FROM t AS a JOIN t AS b ON a.k = b.k (自己結合で k が曖昧) → Prepare 成功時に Next() が例外を投げるのが現行の合格条件。エンジンが曖昧列エラーを drain Status (kUnknown 等) で返すように修正すると rejected が false になりテストが壊れる。

**確認・対応記録:**

- Query rejection tests now drain without expecting exceptions and inspect GetStatus; ambiguous-column diagnostic asserted through Status. Both targeted tests pass (batch23).

### medium-92

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_engine.cpp:3292`

**指摘:** MERGE(3292行)、ネスト配列UPDATE(6577行)、dotted STRUCT UPDATE(6606行)のDML成功時にdatabase_->BumpSchemaEpoch()を呼び、プロセス全体のコンパイル済みプランキャッシュを全無効化する。plan_cache.hpp:26-33は無効化トリガを「CREATE TABLE / DROP TABLE / CREATE INDEX / statistics refresh (ANALYZE)」と明記し、「Row-count drift ... does not need its own invalidation trigger」とまで書いている

**発生条件:** MERGE文を1件実行するたびに、無関係な fingerprints を含む全スレッドのthread_localキャッシュ・セッションキャッシュ・グローバルPreparedPlanCacheの全エントリが次ルックアップでepoch不一致により破棄される(plan_cache.hpp:423-427, 478-485, query/sql_engine.cpp:7270-7273)。MERGE連発のOLTPワークロードではprepared-planリプレイが恒久的に空振りする

**確認・対応記録:**

- MERGE, dotted STRUCT UPDATE, nested array UPDATE each bumped CatalogEpoch without schema changes; regression reproduces all3. Removed increments, preserving ordinary row-layout invalidation. Full SQL audit24 + merge16 pass.

### medium-93

**判定:** 指摘の一部を否定し、必要な箇所を修正＋テスト成功 (`partly-refuted-fixed-tested`)

**元の対象:** `query/sql_expression_matrix_test.cpp:381`

**指摘:** 既知の JSON パーサ欠陥を「現行挙動」としてピン留め: (a) 文字列内の \" エスケープをデコードできず JSON_EXTRACT が NULL を返す(381、コメント「does not yet decode \" escapes ... pins the current edge behavior」)、(b) $["a b"] のブラケットアクセスが未対応で値でもエラーでもなく NULL を返す(396)。GoogleSQL ではどちらも有効な入力で値を返すべきで、現行ピンは有効入力に対するサイレントな NULL(誤結果/データ不可視)を正として凍結し、パーサ修正時にテストが壊れる。

**発生条件:** SELECT JSON_EXTRACT('{"a": "quote\" inside"}', '$.a') → NULL が期待値。SELECT JSON_EXTRACT('{"a b": 1}', '$["a b"]') → NULL が期待値(GoogleSQL は "1" を返す)。引用符を含む JSON 文字列やブラケットキーの抽出が失敗してもエラーにならず NULL で沈黙する。

**確認・対応記録:**

- Escaped JSON quotes already work when valid JSON reaches the evaluator: SQL raw literal regression passes before fix. Quoted bracket paths were genuinely missing: common path parser now handles double/single quoted members, rejects malformed index suffixes, and is used by relational evaluator. SQL20/matrix3/rewrite96/differential17/function-eval10 pass (batch26).

### medium-94

**判定:** テストで指摘を否定 (`refuted-tested`)

**元の対象:** `query/sql_expression_matrix_test.cpp:548`

**指摘:** ErrorTable が SELECT 1 + NULL をはじめとする NULL オペランド算術(548, 561-566, 572, 596, 623, 653 等)と SELECT NOT NULL (783) を「必ず raise」にピン留め。GoogleSQL では NULL オペランドの算術・NOT は NULL を返す(エラーではない)。さらに同ファイルは SELECT 1 = NULL → NULL (121) や true AND NULL → NULL (309) を NULL 伝播としてピン留めしており、比較/AND は NULL 伝播なのに +/*/NOT はエラーという不整合を期待値として凍結している。

**発生条件:** SELECT 1 + NULL はエラー、SELECT 1 = NULL は NULL — 同一のリテラル NULL に対して演算子群で三値論理の扱いが割れる。また実行時の列 NULL 経路は query_test.cpp:843-850 が SELECT a + b AS c ... の NULL 列行を NULL 値として受け入れており(定数経路はエラー)、AST/定数パスと typed 実行パスの不一致まで固定値化されている。

**確認・対応記録:**

- Pinned GoogleSQL arithmetic_functions_3 and logical_not_null explicitly reject literal NULL operands. Typed NULL arithmetic and NOT propagate NULL correctly (new passing regression). Updated misleading matrix comments; no production NULL semantics changed (batches24/26).

### medium-95

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/checkpoint_manager.cpp:124`

**指摘:** docs/recovery_invariants.md:23-38(§6)とdocs/wal_format.md:42-67は、セグメント再利用床が min(checkpoint begin LSN, 最古DPT recLSN, 最古アクティブtxnのfirst LSN, 最古未fsyncのfull-page image) であること、各チェックポイントが時間予算でstale dirty pageをフラッシュし、アロケータmetaページを予算外で同期フラッシュすること、<db>.dbがDPTとATTを保存すること(:18-21)を記述する。実際のfloorは min(snap時点BufferedLSN, 最古アクティブtxnのFirstLSN) だけで(checkpoint_manager.cpp:110-124)、dirty pageフラッシュもmetaフラッシュもなく(SetFlushBudgetMsForTestは空実装 checkpoint_manager.hpp:74)、WriteFuzzyCheckpointが保存するのはコミット済みブロックスロット+フリーリスト+LSM run+replay_start_lsn+max_committed_txn_idで(page/page_manager.cpp:1050-1140)、DPT/ATTは存在しない。コミット済み状態スナップショット方式としては自己整合しているが、両ドキュメントの不変式記述は現状と不一致。

**発生条件:** ドキュメントのfloor公式を信じた運用・検証(「最古アクティブトランザクションがいればDPT recLSNやFPWも床を引き上げるはず」等の障害解析、recLSN進捗のためのdirty pageフラッシュのチューニング)が、実装には存在しない項目を対象にして空振りする。AGENTS.md不変式4のWAL-before-data順序自体はコードどおり成立している(checkpoint_manager.cpp:128-133)。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-96

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/checkpoint_manager.cpp:124`

**指摘:** recycle floorの記述が不一致。docs(recovery_invariants.md:23-38, wal_format.md:42-67)は4項min(「oldest DPT recLSN」「oldest unsynced full-page image」含む)とSyncDataFile・アロケータメタページ同期フラッシュを述べるが、実floorはmin(スナップ時BufferedLSN, 最古active txnのFirstLSN)の2項。DPT・full-page-image・SyncDataFile機構自体が存在しない。

**発生条件:** 「WALが太るのはどの項にpinされるか」の障害解析をdocs通りに行うと存在しないDPT/FPW項を探す。recycle条件(セグメントi+1の終端<=floor+レコード境界validator)自体はdocと一致。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-97

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/log_record.cpp:188`

**指摘:** recovery_invariants.md:78-106 の kSystemAllocPage v6 / kSystemDestroyPage 記述(16バイトBEアロケータスナップショット、kind別undo payload、kCompensateAllocPage、破棄レコードが旧ページ型+ボディ画像を保持、redoでkFreePage化+フリーリスト再構築スキャン)は実装と不一致。alloc/destroy payloadはpage_idのみ(allocはflags1バイトのPageType追加)で、undo payload・CLR型も無い。PageType::kFreePageは存在しない。

**発生条件:** DROP TABLE系クラッシュリカバリの検証をdocsの「破棄ページ画像からの復元」「アロケータスナップショット復元」前提で行うと、実装(破棄→type kUnknown→free_pages_追加、undoもランタイムメモリ巻き戻しのみ)と食い違う。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-98

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/log_record.cpp:309`

**指摘:** docs/wal_format.mdの「Record header」テーブル(:133-146)とバージョン履歴(:148-179)・互換性節(:222-231)は、4バイトmagic 'TYB1'、4バイトversion(kWalRecordVersion=6)、2バイトtype、および「Readers accept v1〜v6」を記述する。実際のワイヤ形式は2バイトnative-endian magic 0x4C53、2バイトversion kCurrentVersion=7(log_record.hpp:54-55)、1バイトtype+1バイトflags(log_record.cpp:235-238)、オフセット4に4バイトtotal長であり、Deserializeはversion!=7を無条件で拒否するため、ドキュメントが謳うv1〜v6旧ログの可読性はコード上存在しない。また「All fixed-width fields use big-endian order」も、magic/version/total長/文字列長プレフィックスがnative-endian memcpy(log_record.cpp:36-41, 227-231)であるため不正確。

**発生条件:** v1〜v6形式で書かれた(ドキュメント上読めるはずの)既存WALを開くと、先頭レコードのDeserializeが即座に0を返しkCorrupt扱いになる。ドキュメントを信頼してフォーマット互換を前提とした変更・障害解析を行うと誤判断する。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-99

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/log_record.cpp:309`

**指摘:** wal_format.md の「Record header」(136-147行)「Version history」「Compatibility」(148-231行)が実ワイヤ形式と不一致。実装は2バイトmagic 0x4C53("LS")・2バイトversion=7固定・4バイトtotal長で、docの4バイトTYB1(kSerdesMagic)/4バイトkWalRecordVersion=6/v1〜v6互換読み込みは存在しない。

**発生条件:** WAL解析ツールやダウングレード検証をdoc通りに作ると、version 1〜6の互換読み込みを期待するが実際は version≠7 のレコードが全て破損扱い(return 0)になる。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-100

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/log_record.hpp:32`

**指摘:** wal_format.md の LogType 一覧(185-204行)とv4〜v6バージョン履歴の大半が実在しない列挙子を語る。kCompensateInsert/kCompensateDelete/kCompensateLsm*/kCompensateRowPage*/kCompensateAllocPage/kCompensateDestroyPage/kCompensateSetLowestValue/kFullPageImage は LogType に無い。CLRはkCompensateUpdateのみ。

**発生条件:** Abort/redoの実装・解析をdocのCLR種別前提で行うと、存在しないレコード型を探すことになる。recovery/AGENTS.md(現行を正しく列挙)と矛盾したまま残っている。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-101

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/logger.cpp:1473`

**指摘:** wal_format.md:13-16 の「A record may straddle a segment boundary(レコードは2ファイルに分割され得る)」は偽。AddLogは境界を跨ぐペイロードを事前にWaitForDurableしてから次セグメントへpre-rollし、レコードは必ず境界から始まる。

**発生条件:** 境界跨ぎ前提のrecycle・解析ロジック(跨ぎレコードのheadセグメント保持等)を実装・レビューすると実挙動とズレる。RecoveryManager::PreRollPaddingEndが扱うのはpre-rollが残したゼロパディング。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-102

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/recovery_manager.cpp:450`

**指摘:** docs/recovery_invariants.md:113-130の「Recovery pipeline」(UNDO未コミットトランザクション、torn-page checksum sweep全範囲、[1,max_page_count]フリーリスト再構築、フェーズ別レート制限進捗ログ)と:40-106のD2/D3節(UndoLoserChains、page_lsnガード、kFullPageImageからのページ再構築、kSystemAllocPage v6のundoペイロード、kCompensateAllocPage/kCompensateDestroyPage)は、木に存在しない機構を記述している。RecoverFromは2パスのコミットフィルタリプレイのみで、アンドパス・sweep・フリーリスト再構築は一切ない。LogType列挙(log_record.hpp:32-50)にはkFullPageImageやkCompensate系CLR型はkCompensateUpdate以外存在せず、UndoLoserChains/UnsyncedSnapshotFloor/SyncDataFileの識別子は木のどこにもない(grepで確認、.t15-reproスクラッチのみ)。recovery/AGENTS.md:16-21の記述(「losers are naturally ignored without undo passes or CLRs」)がコードに合致し、docs側がAGENTS.mdと矛盾している。

**発生条件:** recovery_invariants.mdを仕様として読んだレビュア/開発者が、CLR適用順序やFPW再構築を前提にした変更(例: kCompensateAllocPageの追加やDPT recLSNフロアの実装)を行い、実在しない前提に基づいて設計を誤る。テストもdoc記載のものがほぼ存在しないため(別finding)、docの検証手段もない。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-103

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/recovery_manager.cpp:497`

**指摘:** recovery_invariants.md:113-134 のリカバリパイプラインのうち「REDO(並列ページ毎replay)」「UNDO未コミットtxn」「テアリングページチェックサム全割当範囲スイープ(並列分類)」は実装に無い。実際は単一スレッドの2パス(commit検出→committedのみreplay)で、スイープ・並列replay・MarkRepairedInMemory・SinglePageRecoveryは存在しない。末尾のEND-OF-RECOVERY CHECKPOINTの記述のみ実装と一致。

**発生条件:** リカバリ時間のボトルネック解析や並列化設計をこの図に沿って進めると実コードと乖離する。ページテア検出はチェックサム自体が無いため原理的に不可能(別findings参照)。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-104

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `server/postgres_server.cpp:1410`

**指摘:** 1つの Query メッセージ内で明示 COMMIT の後に続く文が暗黙ブロックに戻らず「文単位の自動コミット(legacy commit-per-statement)」になり、その後に失敗する文があっても取り消されない。PostgreSQL の Simple Query 契約では、最後の明示 COMMIT 以降の文は新しい暗黙トランザクションに属し、メッセージ内の後続失敗でロールバックされる。

**発生条件:** 1つのQメッセージで `INSERT INTO t VALUES (1); COMMIT; INSERT INTO t VALUES (2); SELECT bad_syntax;` を送る。COMMIT 分岐(:1370-1384)で implicit が reset された後の INSERT(2) は implicit==null && transaction==null のため automatic コンテキスト(:1402-1413)で実行され、文ごとに即座にコミットされる(:1476-1487)。後続の構文エラーで FailStatement が呼ばれても automatic は IsFinished 済みのため何も起きず、INSERT(2) は永続化される。PostgreSQLでは INSERT(2) は破棄される。:1239-1244 のコメントが宣言する「メッセージ単位の暗黙トランザクション+明示制御文で確定」の意味論と、COMMIT 後の文の扱いが不整合。worker 経路(ScheduleSessionQuery)でも同じ ExecuteStatements が走るため両経路で再現する。

**確認・対応記録:**

- Each statement starts a new implicit block if previous COMMIT/ROLLBACK consumed it; remaining statements of one message remain atomic. Real TCP regression reproduced retained id2 before fix for both boundaries and now passes; server extra/main/protocol suites pass batch38.

### medium-105

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `server/postgres_server.cpp:691`

**指摘:** EMFILE 対策で /dev/null 予約fdの再オープンに失敗するとリスナを epoll から削除するが、復元条件である「次の AcceptClients 呼び出し」はリスナが未登録なため二度と発生せず、プロセス再起動まで新規接続を永久に受け付けない。コメント(:688-690)の「the cost is a delayed accept」は実装と矛盾する。

**発生条件:** accept が EMFILE で失敗 → CloseFile(reserve_fd_) で 1fd 空きを作り shed accept → OpenFile("/dev/null") が(ストレージ/ログ系スレッドの並行 fd 消費で)失敗すると :685-693 で poller_.Remove(listener_fd_) + listener_shed_due_to_fd_pressure_=true。しかし AcceptClients の唯一の呼び出し点はイベントループの fd == listener_fd_ 分岐(:417-418)で、削除済みfdにイベントは来ないため復元分岐(:649-664)は到達不能。以後既存接続は動作し続けるが新規 accept は行われない。

**確認・対応記録:**

- Event-loop bounded cadence retries AcceptClients when listener was unregistered after reserve-fd restore failure, independent of future listener readiness; idle sweep can release descriptors before retry. Code path reviewed; deterministic reserve-reopen failure integration test still pending.

### medium-106

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:1161`

**指摘:** AggregateKind::kApproxCountDistinctが重複を数えない DISTINCT 集計ではなく単純な行数カウント(1161-1163, 単にcount+=1)。GetCapabilitiesはnativeでagg_approx_count_distinct=trueを広告(1334)しており、この能力を信じたpushdownはCOUNT(DISTINCT col)を重複込みで過大返却する(誤結果・契約違反)。HLL等の近似構造は一切使っていない。

**発生条件:** フラグメントaggregate.kind=kApproxCountDistinct, col=重複ありint64列でExecuteするとscalar_countが総行数になる。chunk側の別実装ApproxCountDistinct(fuzz_chunk_common.hpp:519が呼ぶpax_chunkカーネル)と値も不一致になり、同一クエリがアーティファクトで違う値を返す。

**確認・対応記録:**

- Fragment approximate distinct uses exact distinct set, excludes NULL; three-artifact duplicate regression failed before and passes after.

### medium-107

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:1834`

**指摘:** 汎用TopK経路でTopKEntry::rowが一度も設定されず常に0(1833-1837)。chunk高速経路のTopKColumnはrow入りTopKEntryを返す(pax_chunk.hpp:2365-2368)ため出力契約が経路間で揺れる。加えてneededはint64列しか載せないため(392-401)、非int64列のTopKはRowReader::Valueのidx<0→0(1083-1085)で全エントリvalue=0・任意順のk行を「成功」で返す。

**発生条件:** run/memスナップショットを含むArtifactSetでtop_k(col=varchar列, k=5)のExecuteを呼ぶと、value=0/row=0の5行が返る。GetCapabilitiesはorder_limit_single_column=trueを常に広告(fragment.cpp:1337)しており呼び出し側はそれを信頼できる。

**確認・対応記録:**

- Generic TopK returns concatenated artifact row positions; unsupported column type rejected. TopKPreservesRowIdentity failed before and passes after.

### medium-108

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:783`

**指摘:** AccessMode::kPointがrun/memスナップショットで強制されない。ScanRunはkKeyRange専用分岐(784)以外begin=0/end=Size()の全件走査でpoint_keyフィルタがなく、ScanMemSnapshot(919)も全エントリを走査する。point_keyと無関係な全キー行がSurvivingRowとして返り、residual空ならrows_proved_passとして保証付きで出力される。

**発生条件:** chunk+run等の複数アーティファクトでaccess=kPoint, point_key=KのExecuteを呼ぶと、run内の全キーの行が結果に混入する(fragment.hpp:108は「key equality」と契約)。現行本番はchunk単一でkPointはPointLookupにより正しい(fragment.cpp:550-557)が、IRが文書化するrun/mem結合(またはテスト・ファズでのArtifactSet構築)で即誤結果になる。

**確認・対応記録:**

- Run and mem snapshot point access enforce point_key; three-artifact regression failed before and passes after.

### medium-109

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/memtable/memtable.cpp:307`

**指摘:** IsUnsignedなint64キーのmemcomparableタグがfragment側と不一致。MemTable::Putのfixed高速経路はIsUnsignedでEncodeMemcomparableFormatUint64To(タグ6)を使う(memtable.cpp:306-310, type/value.hpp:45-50, type/value.cpp:776のkMemcomparableUint64=6)が、fragmentのrun/memキー照会はタグ1(kInt64)のみ受理(MemcomparableInt64Key, fragment.cpp:342-343)し、探索キー生成もタグ1固定(MemcomparableInt64KeyOf, 352-358)。unsignedキーのrunに対するkPoint/kKeyRangeのLowerBoundは探索キーが全実キーより小さくなり、上限側が範囲全体の手前で切れて結果が空または誤配置になる。

**発生条件:** unsigned int64キー(Value::SetUnsigned済み)をMemTable::Put→Sync→SortedRun化し、そのrunアーティファクトにaccess=kKeyRangeでExecuteすると範囲内の行が1行も返らない。IsUnsignedはスキーマ列フラグ(lstable_bridge.cpp:372が読む)とValueフラグの二重の源があり、 memcmp_fuzzer(unsigned値生成)のような入力で混入余地が実在する。

**確認・対応記録:**

- Fragment validation rejects unsigned schema with kNotImplemented/coverage None until unsigned-aware kernels exist. Run, chunk and mem snapshots decline point/range/filter in native/Wasm modes. FragmentDomainAuditTest regression passes batch58.

### medium-110

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `table/memtable/memtable.hpp:40`

**指摘:** ShardLockのコメント「shard hold windows are single skiplist operations (nanoseconds)」と実際の保持時間が不一致。MemTable::SnapshotはシャードロックをSnapshotShardの全エントリ深コピー(RowEntry=Rowのヒープ確保を含む)の間保持し(memtable.cpp:175-179, 123-165)、保持時間はエントリ数に比例する。ShardLockはtest_and_set+pauseのスピンロック(hpp:48-54)なので、コピー中は当該シャードの書き込みがスリープせずpauseスピンでCPUを消費しながら待たされ、並行時に顕著な性能崩壊を招く。

**発生条件:** 1シャードに大量エントリ(例: Sync未実施のまま数万〜百万Put)が溜まった状態でSnapshot()を1スレッドが呼ぶと、そのシャードにPutする全ワーカーがコピー完了までスピン待機する。ナノ秒どころではなく、シングルコア過剰購読では優先順位逆転で長時間停止しうる。

**確認・対応記録:**

- ShardLock comment now states SnapshotShard holds spin lock during entire copy including allocations, with no fairness/bounded wait guarantee; removed nanosecond-only and flush-mutex bounds claims. Lock implementation unchanged.

### medium-111

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:2729`

**指摘:** ReduceColumn の kSum における __int128 オーバーフロー検査がスキーム arm 間で不整合。kBp128/kFastPfor/kDeltaFastPfor だけが mul/add_overflow を検査して ReduceResult{} を返す一方、他の全 arm と汎用フォールドは無検査で res.sum へ加算し、桁あふれ時に沈黙ラップ(誤結果)する。同じ SUM が列の圧縮スキームによって「エラー」と「誤った値」に分岐する。

**発生条件:** ヘッダ契約が想定する scaled decimal(pax_chunk.hpp:2044-2051、AggTerm.coeff=10^18≈2^60 は out_scale 18 で現実的)と 2^60 級の値が組み合わさると、1024 行フルグループの和 ≈2^73 × coeff ≈2^60 = 2^133 で __int128(2^127)を超える。Bp128 列なら invalid、kDeltaPfor/kQpp/kArith/kPLa/kMod/kLane/kSortDict/kPostings/kWaveletR/kPBitS/kRle8/kRleDelta/kWah/kConst/kRle/kRleBp/kRleDict/kHuffDict/kTopk/kTileDict/kDict/汎用ラップ経路では沈黙ラップ。ScanAggregate の多項式積 term *= tcol(c)[i](7657-7663)も 2 列 × coeff で同様に無検査。

**確認・対応記録:**

- All28 remaining ReduceColumn scheme folds now use checked TryAccumulateScaledSum for multiplication and accumulation. CONST/PLAIN/RLE overflow regression failed before, BP128 already guarded; all4 paths reject overflow and preserve exact representable sums after. Corrected test fixture to include footer trailer before comparison (batch37-before2).

### medium-112

**判定:** 防御を追加＋関連テスト成功 (`defensive-fix-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:8180`

**指摘:** GroupBySumColumns が DecodeInt64ColumnRg の std::exception を catch してその行グループを continue で黙って棄却する。エラーを呼び出し側に返さず、GROUP BY 結果からそのグループの全行が欠落した「誤った集計結果」だけが返る(エラー飲み込み→データ欠落)。

**発生条件:** hand-built/hostile footer(fuzz表面。fuzz_chunk_common.hpp:537 は GroupBySumColumn、fragment.cpp:1743 が本関数を呼ぶ)でグループ列のデコードが throw すると、当該グループが無言で出力から消える。同一チャンクでも兄弟 API GroupBySumColumn(:8088-8094)は catch せず throw を伝播させるため、1列版とN列版で同一入力に対する契約が食い違い、1列版ではエラー、N列版では欠落済みの正常値が返る。ヘッダ契約(pax_chunk.hpp:2276-2283)は「correct, and the price is documented rather than hidden」の一族方針で、棄却挙動は未文書。

**確認・対応記録:**

- Original notes correctly refute corrupt-offset reachability from current SQL builder and the claimed single-key/N-key asymmetry. Hardened public hand-built-chunk API anyway: N-key aggregate now returns StatusOr and translates decode exceptions to corruption instead of skipping groups; caller propagates error. Corrupt offset regression passes batch37; not claimed reproduced through SQL.

### medium-113

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:8214`

**指摘:** GroupBySumColumns の SUM 蓄積を int64_t のまま直接加算しており、符号付きオーバーフロー=UB。巨大値の合計で UBSan が trap、無印ビルドでは未定義の誤結果。同ファイルの確立した契約(uint64 ラップ or __int128)に唯一逆らっている。

**発生条件:** 実データ到達可能: fragment.cpp:1743 の pushdown で int64 列を GROUP BY SUM し、例えば INT64_MAX に近い値が同一グループに 2 行以上来た時点で entries[pos].sum += sbuf[i] がオーバーフローする。1列版 GroupBySumColumn は std::vector<__int128> sums(:7738)で蓄積し最後に truncation、VectorizedScanColumn は「signed int64 += on adversarial data is UB」(:4384-4386)と明記して uint64 で蓄積。N列版だけが素の int64。

**確認・対応記録:**

- N-key GROUP BY and fragment scalar/polynomial SUM use explicit unsigned modular arithmetic and bit_cast to int64, preserving storage kernel truncation contract without signed overflow (including >128-bit monomials). Boundary regressions pass batch37; sanitizer validation pending.

### medium-114

**判定:** 修正＋コード確認（専用再現試験なし） (`fixed-code-reviewed`)

**元の対象:** `table/pax_chunk/pax_chunk_encode.cpp:9036`

**指摘:** 適応型セレクタの budget_for() が、kPostings が走行中インカンベント(=inv_bytes を含まない prune_cost)のとき、非 postings 挑戦者の予算 limit = (prune_cost - inv_bytes)/m が負になり得る。`!(limit < SIZE_MAX)` のガードは負値を素通しし、負の double の static_cast<size_t> は [conv.fpint] により UB(結果を size_t が表現できない)。x86-64 では 0x8000… の巨大予算になり、以降の全候補の予算プルーニングが無効化される。

**発生条件:** TINYLAMB_FORCE_SCHEME 未指定・int64 列・AccessHint が kFilter/kAggregate・distinct が 4..64 かつ d<=n/8(inv_would_build 成立、encode.cpp:8493-8505)で kPostings が最安になった直後、consider 順序で kPostings より後の約25候補(Arith/AnsFor/OpDict/HuffDict/DeltaFp/BitWeave/Bp128/Pfor/…、encode.cpp:9612-9801)が budget_for() を呼ぶたびに負 limit の UB 変換が発生し、各候補の予算打ち切りが効かなくなる(候補を賞味バイト数で再評価する consider() のおかげで勝者は正しいが、UB + 顕著なエンコード性能劣化)。

**確認・対応記録:**

- Adaptive encoder clamps nonpositive challenger budgets to zero before floating-point-to-size_t conversion. No deterministic postings-incumbent reproduction yet; sanitizer integration pending.

### medium-115

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_scan.cpp:2450`

**指摘:** BuildFromSortedRun が BuildRowInto の失敗(false)を無視して生行を静かに欠落させる。同一の破損入力に対して、fixed-width(SOA)経路では全く異なる静かな挙動になる: BuildCellsInto は row_offset_ 異常時にセルを書かずに early-return し(pax_chunk_encode.cpp:7296-7298)、呼び出し側が無条件で ++group_fill/++total_rows するため前の行のセル内容をそのまま数え込んだ二重行がチャンクに混入する。さらに SOA 経路のペイロード破損は throw(pax_chunk_encode.cpp:7303-7331)で、そのコメント(7287-7291)は「両デコード経路が同じエラーを出す」と明示しているが、呼び出し側でそれを表面化させず飲み込んでいる。つまり破損1行に対し「欠落 / 二重化 / throw」の3通りの静かな結果があり、欠落・二重化はチャンクが正しく見えたまま誤結果(データ欠落・件数/合計/ゾーンマップ汚染)を生む。

**発生条件:** varchar 等を含むスキーマ(!use_soa)で SortedRun の blob が破損し DeserializeBounded が throw、または row_offset_ >= blob.size() のエントリが1つ混ざると、その行は削除フラグが立っていなくてもチャンクから静かに抜け落ち、flush 後の全ての読み取り(PaxChunk スキャン・PointLookup・MetadataRangeAggregate の COUNT/SUM)でその行が永久に消える。エラーはログにも Status にも現れない。int/double のみのスキーマ(use_soa)では代わりに row_offset_ 異常エントリが直前行の複製として数え上げられ、行数・ゾーンマップ・PGM・ハッシュ索引が壊れた内容で確定する。

**確認・対応記録:**

- PAX BuildFromSortedRun now returns StatusOr. Invalid live-row offsets/decoding errors abort the whole build with kCorrupt on both SOA and row paths. Production consumer checks status; regression tests corrupt offsets, truncated headers and keys, compressed/uncompressed. Batch61 codecs/storage fuzz/fragment suites all pass.
- Batch66 ASan/UBSan PAX codecs and all storage-fuzzer seed suites pass, including corrupt row/key construction regressions.

### medium-116

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `table/table.cpp:333`

**指摘:** INSERTの索引ロールバック補償(:333)とPhase1-4空きページ先行確保(TCHAIN6/7, :303-320)が、GetPage/AllocateNewPage/LinkReservedTableTailのエラーを補償なしで早期リターンする。行はすでにref->Insertで書き込まれた後なのに、索引ループ(:323)が未実行のまま失敗を返すため、失敗したINSERTの行が索引エントリゼロでコミットされ得る。

**発生条件:** ほぼ満杯ページへのINSERT成功直後にShouldPreallocateNextBlockが真で、TryReserveTableTailがkSuccess/kConflicts以外(:305-308)かAllocateNewPage失敗(:312-316)かLinkReservedTableTail失敗(:318)→挿入済み行を削除せずエラー返却。または索引iで失敗→j<iの索引削除は完了するが、:333のGetPageがエラー→ヒープ行が残置。呼び出し側がDML失敗後もトランザクションを続行・コミットできる(table_test.cpp:614-627でINSERT失敗→UPDATE成功→PreCommit成功を検証)ため、索引を持たない幽霊行が永続化し全スキャンで重複カウントされる。

**確認・対応記録:**

- Insert marks transaction failed on post-write tail claim/allocation failure (LinkReservedTableTail already did); all failed compensations mark failed. Rollback page lookup now preserves primary index error and marks failure instead of early ASSIGN_OR_RETURN. Table53 passes incl successful duplicate-key compensation; injected storage-failure path not exercised (batch54).
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### medium-117

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `table/table.cpp:758`

**指摘:** UPDATE再配置の補償経路で PageRef取得をASSIGN_OR_RETURNしており、GetPageがエラーStatusOr(kNotExists等)を返すと「予約コピーの削除」「旧キーの reinstatement」を飛ばして早期リターンする。同一関数の他の補償はLogCompensationFailure+txn.MarkFailed()で処理する(:512-531, :543-545)のと対照的で、txnは失敗マークされないためコミット可能なままヘープと索引が乖離する。

**発生条件:** 大きいUPDATEが再配置へ流れ(copy挿入成功)、(a)索引delete途中で失敗→:758のGetPageがkNotExists(同一grooveのページが並行DROP/destroyで消えた場合、page_manager.cpp:327-339がエラーを返す)→deleted個の旧キーが消えたまま行がposに残り、コミットするとその行が索引スキャンから到達不能。(b)元行Delete用の:769がエラー→new_posのコピーとposの元行の両方が残り、コミットで物理重複行が永続化。(c)元行Delete失敗後の:778がエラー→コピー削除と全旧キー復元の両方を飛ばす。table_test.cpp:1657の通りTable APIはDML失敗後のPreCommitを許すため、乖離がそのまま永続化し得る。

**確認・対応記録:**

- Relocation cleanup now handles lookup failures explicitly, marks failed, and continues reinstating old index keys even if reserved-copy deletion fails. Source-page lookup enters compensation rather than early return. Table53 passes; injected page lookup failure not exercised (batch54).
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### medium-118

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `transaction/README.md:14`

**指摘:** abortプロトコルのドキュメント記述が実装と不一致。README.md:14は「abort時、同一チェーンを辿って各変更をCLRで補償し、COMMIT型の終端レコードを追加する(ABORTレコード型は存在しない)」と述べるが、実際の実行時abortはCLRを一切出さず、staged_records_を逆順にメモリ適用するだけ(PageManager::ApplyTransactionAbort、純メモリundo)で、LogType::kAbortの単一レコードを1件追加する。ABORT型はrecovery/log_record.hpp:47に存在し、transaction_manager.cpp:701-702で実際にappendされる。また再起動リカバリは終端レコード型ではなくkCommitの有無でコミット済み集合を分類し、loserは単に再生されない(recovery/recovery_manager.cpp:517, 548-551)。transaction/AGENTS.md:19-20の「abort (backward chain walk + CLR, idempotent)」も同じく実装不一致(実行時abortにチェーンウォークもCLR連鎖もない。CLRを発するのはpage/row_page.cpp:183-193 RestorePhysicalUpdateの修復パスのみ)。README.mdは同一ファイル内の29行目「appends an ABORT log record when WAL records were emitted」とも自己矛盾する。

**発生条件:** 障害解析やリカバリ設計時にAGENTS.md/README.mdの記述を信じると、「volatileなAbort終端の喪失はCLR連鎖により再起動リカバリで再実行される」と誤解する。実際はtransaction_coverage_test.cpp:1645-1649が自认するとおり、終端の喪失は再起動loser清理と同一結果になるだけでCLR再実行の保証はなく、WAL上の終端はkAbort型である。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-119

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `transaction/transaction_manager.cpp:701`

**指摘:** recovery_invariants.md 不変項1(4-15行)と冪等節(40-76行)のARIES的記述——「Abortはundo完了後にkCommitターミネータを追記」「CLRが元レコードのprev_lsnをundo-nextに持つ」「restart loser-undo (UndoLoserChains)」「Leaf/Branch再適用」「Page::DeleteBranchImpl」——はすべて退役。現行はAbort時にkAbortを1本追記し、undoはstaged_records_のメモリ巻き戻し、リカバリはloserを単純スキップする(undoパス無し)。

**発生条件:** クラッシュ時の未コミットデータの扱いを本docで判断すると、存在しない「loser undo」「CLR連鎖」を前提にした障害解析になる。recovery/AGENTS.mdは現行正しく(「naturally ignored without undo passes or CLRs」)矛盾している。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### medium-120

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/value.cpp:1482`

**指摘:** INT64/UINT64タグ混在の除算・剰余が、符号を考慮せず生のuint64ビットパターンで計算される。比較(TryLess 1248-1266、CompareForOrderBy 1589-1606)は混合符号を明示的に符号付き判定するのに、算術は除算/剰余では符号チェック無し、加減乗算は符号無視のビットパターン演算+オーバーフロー検査のみという一貫しない方針。混在オペランドで無音の誤結果を出す。

**発生条件:** INT64(-6) / UINT64(2) → u_lhs=0xFFFFFFFFFFFFFFFA ÷ 2 = 0x7FFFFFFFFFFFFFFD、WithUnsigned で UINT64(9223372036854775805) がエラー無しで返る(-3でもエラーでもない)。同様に INT64(-1) % UINT64(2) は 1。到達性は上位層の混在昇格方針次第だが、比較系が符号認識しているのに算術系が素通りという層内の不整合は確定。

**確認・対応記録:**

- Negative signed operands mixed with UINT64 reproduced wrong integer quotient/remainder (including unsigned_max / -2). Value arithmetic now divides unsigned magnitudes, applies mathematical sign and rejects unrepresentable negative quotients.
- AST/relational DIV and MOD reuse Value unsigned handling; binary modulo mixed-negative case delegates too. SQL / already converts actual signedness to DOUBLE and remains unchanged.
- Value91, differential18, function AST8, SQL audit25 pass. Differential covers AST/relational DIV/MOD and bytecode binary modulo; DIV/MOD bytecode compilation unsupported (6 recorded path skips).

### low-1

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `AGENTS.md:73`

**指摘:** ルートAGENTS.md:73-74「re-added as unbilled reference sources only」と docs/review.md:511-514「トップレベルの `parser/` にはビルド外の参照用ソース（parser.hpp/pratt_parser.hpp/token.hpp/tokenizer.hpp...）のみが残る」は、parser/ ディレクトリが既にリポジトリに存在しない(now-fixed)状態を述べている。parser/ はコミット 405ff4f で削除済み。

**発生条件:** 「参照用ソースが残っている」前提でレガシーパーサ挙動を確認しようとするとファイルがなく、AGENTS.md の記述を信じたエージェントが存在しないソースを読もうとして無駄な探索・誤判断をする。「not built / include禁止」の運用ルール自体は空集合について真だが、参照ソースの存在記述は虚偽。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-2

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `ARCHITECTURE.md:63`

**指摘:** §2 層別表の common 行が代表ファイル `vm_cache.hpp` と責務「仮想メモリキャッシュ」を挙げるが、common/vm_cache.hpp はリポジトリのどこにも存在しない(vm_cache/VmCache のシンボル自体が 0 件)。LSM移行(405ff4f)で消滅した S3-FIFO 時代の記述が残存。

**発生条件:** 新規参加者が層の代表ファイル一覧を ARCHITECTURE.md で学習すると、実在しないキャッシュコンポーネントを前提にした読図・配置判断をする。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-3

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `CMakeLists.txt:822`

**指摘:** TINYLAMB_ENABLE_TSAN 時の `TSAN_OPTIONS=detect_deadlocks=0`(全テストに適用)の根拠コメントが、退任済みの PagePool/GetPageImpl プールラッチ機構を説明に使っている(page_pool.cpp/GetPageImpl は現行ツリーに存在しない)。抑制自体は継続中なのに根拠が消滅しており、現行の PageRef per-block shared_mutex 構成で本当のロック順転倒が TSan の deadlock 報告から隠れ続けるリスクを、根拠の陳腐化が疑えなくしている。

**発生条件:** TSAN ビルドで新規のロック順転倒(page latch × write-intent 等)が発生しても detect_deadlocks=0 により報告されず、コメント根拠(消滅済み機構)ゆえに「この抑制はまだ必要か」の再検討がトリガーされない。

**確認・対応記録:**

- Corrected retired TSAN justification (deadlock checking still explicitly disabled, not claimed covered), Value owned/borrowed payload model, nested-loop error propagation and malformed query guide formatting; source reviewed (batches20-27).

### low-4

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `common/AGENTS.md:29`

**指摘:** common/AGENTS.mdのplatform節が「OS abstraction (Linux + macOS)」「EventPoller (epoll / kqueue)」「Waker (eventfd / pipe)」と記載するが、commit 75ef025「support windows」以降、コードはWindows分岐(TINYLAMB_OS_WINDOWS)を全面的に持ち、EventPollerはWSAPoll、Waker/pipeはループバックTCPソケットペアで実現される。ドキュメントがWindowsパスとそのセマンティクス差を記述していない。

**発生条件:** 「macOSはno-op」と書かれたcpu_affinity(48行目)や「epoll/kqueue」と書かれたpollerの記述を根拠にプラットフォーム挙動を仮定したコードを書くと、Windows(WSAPoll経由・pipe=TCPソケットペア・AcceptSocket後fcntl適用)での実挙動とずれる。AGENTS.mdは「Each header documents the per-platform behaviour」と謳うがWindows挙動の記載が欠落している。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-5

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `common/log_message.cpp:34`

**指摘:** LOG()の行出力が共有std::cerrへの2回のoperator<<呼び出し(メッセージ本体、色リセット+改行)に分かれており、並行ログで行が交互混線(tearing)しうる。行単位アトミック出力のためのplatform/synced_stream.hppが存在するのに未使用。

**発生条件:** morsel駆動並列実行やサーバの複数ワーカースレッドが同時にLOG(INFO/WARN)を発すると、スレッドAのメッセージとスレッドBのメッセージが連結され(「msgAmsgB」)、その後に改行が2つ出る。障害調査時のログが行単位で信頼できなくなる。log_message.cpp:49-51のコメント「concurrent loggers on worker threads do not race」はlocaltimeの再入のみ対処しており、cerr出力自体の相互排除はしていない。

**確認・対応記録:**

- LogStream emits whole line through platform::SyncedOStream; concurrent800line regression running.
- Concurrent 800-line logging regression and all 3 log_message tests pass in batch14; line emitted through SyncedOStream.

### low-6

**判定:** 修正済み・対象プラットフォームで未実行 (`fixed-needs-platform-test`)

**元の対象:** `common/platform/event_poller.cpp:265`

**指摘:** Windows版EventPoller::Waitが「登録fdゼロ」のときtimeout_ms==-1でも即座に0を返し、epoll_wait/kqueueのように永久ブロックしない。Waitループで待つサーバacceptループ等がWindowsでビジースピン(100% CPU)するOS間セマンティクス差。

**発生条件:** pollerをOpenした直後(または全fdをRemoveした後)に`while ((n = poller.Wait(out, -1)) >= 0)`で待つコードをWindowsで実行すると、Waitが即帰還するためループが休みなく回りCPUを食み潰す。Linux(epoll_waitはfdなし+timeout=-1で永久ブロック)とmacOS(kqueueも同様)では同じコードが正しくブロックする。

**確認・対応記録:**

- Windows empty poller waits on condition_variable for registration or close and preserves finite timeout deadline for subsequent WSAPoll; Add/Close wake waiters. Cross-platform empty-poller registration/readiness regression passes on Linux normal/ASAN, all14 platform tests. Windows execution/cross-compiler unavailable; no Windows runtime claim (batch55).

### low-7

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `common/platform/extended_float.hpp:67`

**指摘:** SoftExtended::operator T()(整数変換)のexponent_ >= 0分岐が範囲外float→整数変換のUBを直接踏む。exponent_ >= 0のとき magnitude ≥ 2^63 であり、int64_t宛ての`static_cast<T>(ToDouble())`はほぼ全入力で切り捨て値が表現範囲外=UB(C++20 [conv.fpint])。

**発生条件:** Apple arm64(ExtendedFloat=SoftExtended、420-426行)でSTDDEV/集計結果をint64へキャストするコードを誰かが追加した瞬間、例えばSUMが2^63以上のときにUB(一般にはGCc/ClangではINT64_MIN等の不定哨兵)になる。現ツリーではaggregate_accumulator.cpp:1671-1726・window_eval.cpp:319-358がExtendedFloatを構築・比較・double化するだけで整数変換の呼び出し元は存在せず休眠(ublint的に危うい公開API)。

**確認・対応記録:**

- SoftExtended integral conversion now saturates before narrowing. Standalone UBSan float-cast-overflow reproducer fails before and passes after; platform suite passes (batches21/22).

### low-8

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `common/platform/int128_rt.cpp:123`

**指摘:** __fixdfti(double→signed __int128)の負の飽和経路が誤値+1を返す。F64ToU128が|d|≥2^127で全ビット1(2^128-1)に飽和し、`-static_cast<ti128>(2^128-1)`は+1になる(INT128_MINになるべき、compiler-rt規約)。

**発生条件:** ソフトint128ターゲット(32bit ARM/x86)でdoubleからti128への変換が-2^127未満(例: -1e30)でlibcallされると結果が1になる(-infも同様)。C++の範囲外変換自体はUBだが、このファイルの目的はcompiler-rt組み込みの互換実装であり、compiler-rtは負のオーバーフローをINT128_MINに飽和させる。x86_64/aarch64/Win64では25-28行のガードでファイル全体が空にコンパイルされるため現行ターゲットでは休眠。

**確認・対応記録:**

- Shared unsigned IEEE magnitude conversion handles exponent 127 without premature saturation; signed conversion clamps at signed boundaries without negating INT128_MIN. Boundary unit tests and platform suite 13 tests pass (batch22). Runtime compiler-rt fallback not executed on this native target.

### low-9

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `common/platform/process.cpp:233`

**指摘:** RunProcessのPOSIX実装がfork()後の子プロセスでasync-signal-unsafe処理(setenv・std::string/vectorアロケーション)を行ってからexecvする。マルチスレッド親プロセスで他スレッドがヒープロックを持った瞬間にforkすると、子がmallocロックでデッドロックし、親はwaitpidで永久ブロックする。

**発生条件:** マルチスレッドプロセス(サーバ・並列実行中のクエリ)からRunProcessを呼ぶと、子側の::setenv(234-235行)やexecutable.string()/std::vector構築(237-244行)がfork後にヒープアロケートし、他スレッドがmallocミューテックスを保持していた場合は子が永久停止→親は249-256行のwaitpidループで永久に固まる。現ツリーの唯一の呼び出し元はbenchmark/tpch_benchmark.cpp:370(dbgen)でdbgen起動時に余分なスレッドがなければ顕在化しない。SQLフロントエンドの子プロセス起動(query/googlesql_frontend.cpp:395-401)はposix_spawn+InitSpawnAttributesでこの問題を回避しており、platform APIとしての危うさが残る。

**確認・対応記録:**

- POSIX RunProcess uses posix_spawn with parent-built argv/environment. Child override isolation, argument boundaries, exit code and missing executable tests pass with platform suite (batch22). No stochastic deadlock reproduction claimed.

### low-10

**判定:** 修正＋コード確認（専用再現試験なし） (`fixed-code-reviewed`)

**元の対象:** `database/database.cpp:1148`

**指摘:** UpdateStatistics/RefreshStatisticsだけがcatalog_mu_を取得しない(database.cpp:1140-1163)。他の全カタログ変更は保持している(CreateTable:316、DropTable:495、RenameTable:784、CreateIndex:951、GetOrAddFunction:973)。UpsertStatistics(database.cpp:284-291)はUpdate→kNotExists→InsertのRMWなので、統計行が存在しない窓で2スレッドが同時ANALYZEすると片方のInsertがkDuplicatesを返し、ANALYZEが誤って失敗する。

**発生条件:** 新規作成直後・クラッシュリカバリ直後など統計行がまだ無いテーブルに対し、2スレッドが同時にANALYZE(RefreshStatistics)を実行→両者がkNotExistsを見てInsert→LsmInsertの重複検査で片方がkDuplicatesを返す。

**確認・対応記録:**

- UpdateStatistics now holds catalog_mu_ through table lookup and split-statistics write; RefreshStatistics delegates to it. catalog_test 38 pass, dedicated concurrent fault test not yet added.

### low-11

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `database/database.cpp:439`

**指摘:** CreateTableのエラー巻き戻しunwind_allocationで、GetPage(new_table.first_pid_)が失敗した場合DestroyPageがスキップされ、割当済みテーブルページがフリーリストに戻らない(database.cpp:438-444)。catalog_inserted後の失敗経路ではカタログ・統計・インデックスツリーは削除するのに、ページだけ静かに漏洩する(ページID永久欠番=ストレージリーク)。

**発生条件:** 一括作成中に一意インデックス構築やカタログ挿入が失敗し(unwind_allocation発動)、かつGetPageがその時点でStatusOrエラーを返した場合、当該ページが破棄されずにスキップされる。以後の割当はこのページIDを再利用できない。

**確認・対応記録:**

- CreateTable compensations now check all catalog/statistics/index/page cleanup statuses and mark transaction failed + log on failure, preventing commit of incomplete cleanup; independent cleanup attempts continue.
- Current in-memory GetPage can only miss this private fresh page if absent/wrong-type, not ordinary I/O; no deterministic failure injection yet. catalog_test39 pass.

### low-12

**判定:** テストで指摘を否定 (`refuted-tested`)

**元の対象:** `database/database.cpp:580`

**指摘:** DropTableのページ鎖破棄ループがPageTypeを検証せず`page->body.row_page.next_page_id_`(ユニオン)を読む(database.cpp:574-584)。対照的にインデックススキャンの行解決は`ref.Value()->Type() != PageType::kRowPage`を検査する(index/index_scan_iterator.cpp:395-401)。破損/非rowページが鎖上に現れると誤ったユニオン読みでnext_pidを取得し、任意のページIDを破棄対象にし得る。また`if (!page.IsValid()) break;`で鎖が途切れた場合も残ページを破棄せずForgetTableTailへ進む。

**発生条件:** 破損DBでDropTableが実行され、鎖上のページのtype_がkRowPage以外(kUnknown等)の場合、bodyをrow_pageとして解釈したnext_page_id_(他型のユニオン内容)を辿り、無関係なページをDestroyPageする恐れ。

**確認・対応記録:**

- Current PageManager::GetPage rejects non-row pages before returning a PageRef. Corrupt page type fixture makes DropTable return kNotExists and mark transaction failed without reading row-page links.
- AuditDropRejectsUnexpectedPageType passes before any production change; catalog_test all39 pass.

### low-13

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `database/database.cpp:717`

**指摘:** PutViewDefinitionがfunctions_.Readの結果をkNotExistsと成功以外区別せず、読み取りがエラーを返した場合も既存ビュー索引レコードを「今回の1件だけ」の内容で永続上書きし得る(database.cpp:716-728: index.HasValue()で成功のみ分岐し、エラー時はnamesが空のままWriteViewIndexへ)。DeleteViewDefinitionも読み取り失敗時にkSuccessを返し、索引に削除済みビュー名が残留する(database.cpp:755-763)。ビュー索引のread-modify-writeがエラーを検査しない構造は、登録ロス・ dangling参照の温床。現在のLsmReadは値かkNotExistsしか返さないため発火窓は狭いが、索引レコードは再オープン後も永続するため、将来のエラー経路追加で他ビューの再登録が静かに失われる。

**発生条件:** 同一txn内で索引読み取りがkNotExists以外の異常を返した状態でCREATE VIEWを実行すると、__view\x1e\x01__indexレコードが当該1ビューだけで上書きされ、再オープン後のListViewDefinitionsから他のビューが消える。

**確認・対応記録:**

- View definition writes/deletes now read and decode the index before mutation and propagate errors. Replace/delete/reopen regression and all 38 catalog tests pass (batch17). Current LSM read exposes no convenient I/O fault injector.

### low-14

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `database/database.hpp:189`

**指摘:** WriteCheckpointのコメントが「the periodic 5 s worker」と記述するが、PageStorage::Createが実際に渡すデフォルト間隔は10秒(database/page_storage.cpp:82 `size_t checkpoint_interval = 10;`)。CheckpointManagerの引数デフォルトは5(recovery/checkpoint_manager.hpp:51)だが、PageStorageは明示的に10を上書き渡しているため、実際の周期は10秒。

**発生条件:** ドキュメントの周期(5秒)を前提にした障害解析やWAL保持量の見積りが実際(10秒)とずれる。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-15

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `docs/AGENTS.md:13`

**指摘:** 「environment_variables.md — every `TINYLAMB_*` env var the code reads」の主張に対し、コードが getenv で読む 119 変数のうち 30 変数が environment_variables.md に未記載。エンジン本体のA/Bハッチ(TINYLAMB_DISABLE_STREAM_GROUPED_JOIN / DISABLE_COUNT_STAR_FUSION / DISABLE_PK_ABSORPTION ほか DISABLE_* 8種)やフロントエンドノブ(NEGATIVE_CACHE_TTL_SEC, PARSE_CACHE_SIZE)が文書化されていない。

**発生条件:** A/B比較や性能調査で hatch を探す際、docs/AGENTS.md の「全変数記載」を信頼して environment_variables.md だけを見ると、ストリーム結合・COUNT(*)融合・PK absorption を無効化する既存ハッチの存在に気づかず比較試験の切り替えができない。

**確認・対応記録:**

- medium-9 environment inventory.

### low-16

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `docs/AGENTS.md:16`

**指摘:** 「There is no `docs/adr/` directory」は偽。docs/adr/0001-fixed-wal-logical-end.md が実在し、wal_format.md:236-237からも参照されている。

**発生条件:** docs/AGENTS.mdのADR規約に従ってルート直下にNNN-*.mdを作ろうとすると、既存のdocs/adr/と方針が衝突する。

**確認・対応記録:**

- medium-4 accepted ADR links.

### low-17

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/AGENTS.md:7`

**指摘:** docs/AGENTS.md:7 が wal_format.md の要約として「`kWalRecordVersion = 6`」と記すが、(a) その名前の定数はコードに存在せず、(b) 実際の現行バージョンは recovery/log_record.hpp:55 の `LogRecord::kCurrentVersion = 7`。docs/wal_format.md:139 の「currently 6」も同様に陳腐で、v7 についての記述は wal_format.md に存在しない。

**発生条件:** WALレコード互換性の検証・旧ログ読み取り実装時にドキュメント記載の定数名・値(6)を信頼すると、実装(7)と突き合わず誤った互換判断をする。v7 へのバンプはコミット 405ff4f で行われたが文書は追従していない。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-18

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/AGENTS.md:7`

**指摘:** docs/AGENTS.mdがWALレイアウトを「kWalRecordVersion = 6」と案内するが、実装は LogRecord::kCurrentVersion = 7 で、kWalRecordVersionというシンボル自体が存在しない。

**発生条件:** WAL互換性判断の一次資料としてdocs/AGENTS.mdを読むと1つ古いバージョン番号を得る。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-19

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `docs/cascades_optimizer.md:18`

**指摘:** docs/cascades_optimizer.md:18 と :55、および plan/AGENTS.md:119 が論理ルール登録数を「83」と記載するが、cascades.cpp の RuleSet::Default の実登録は86件。両ドキュメントとも3件分古い(例: join_to_cross_if_no_predicate, setop_empty_identity, union_to_union_all_plus_distinct 等の世代の追加が未反映)。

**発生条件:** 「authoritative list」と明記されたドキュメント(55行目)を根拠に論理ルセットの網羅性を監査する際、実在する86件との差分3件がドキュメント抜けなのか不正登録なのか判別できず、監査結果を誤る。

**確認・対応記録:**

- medium-18 current rule inventory86/71/46.

### low-20

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `docs/cascades_optimizer.md:55`

**指摘:** ルール登録数の記載がコードと不一致。ドキュメントは論理ルール「83 registrations」(18行・55行)だが、plan/cascades.cpp の RuleSet::Default の実登録は86件。また plan/AGENTS.md:184 は物理ルール「45 physical registrations」だが、実登録は46件(docs/cascades_optimizer.md:18 の「46」が正しい)。

**発生条件:** 確認コマンド: `grep -c 'built\.Add(Rule(' plan/cascades.cpp` → 86(重複名なし、python3 で86ユニーク名を確認)。`grep -c '\.Add(' plan/implementation_rules.cpp` → 46。docs/optimizer_rule_audit.md を「complete registry」と位置づけつつ数字が陳腐化しているため、監査時のカバレッジ検算の基準がずれる。

**確認・対応記録:**

- medium-18 current rule inventory86/71/46.

### low-21

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/commit_durability.md:43`

**指摘:** Abort経路の説明(42-49行)「undoウォークはRecoveryManager::ReadLogがリングバッファ(Logger::TryCopyBuffered)から揮発レコードを直接読む」「リング外かつ未ディスク(>8MiB)のみグループコミット待ちにフォールバック(read_undo_record)」は退役。現行AbortはWALを一切読まずstaged_records_のメモリundoで、read_undo_recordというシンボルも無い。ただし結論「Abortは恒久化を待たない」自体は現行コードと一致。

**発生条件:** Abortレイテンシやシャットダウンdrainの説明・チューニングをこの節で行うと実装とズレる。TryCopyBufferedの実際の利用者はRecoveryManager側。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-22

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/distributed_migration.md:60`

**指摘:** 未実装設計書だが「現状」記述が全て陳腐: (a)「LogRecordにバージョン欄は無い」→version欄あり(kCurrentVersion=7)。(b)「WALは現在切り詰めが存在しないため無限増大する。RecoverFromが常に先頭0から走査(page_storage.cpp:52)」→セグメントrecycleは実装済み、RecoverFromはmax(anchor, log_start)から走査し、page_storage.cpp:52はLogSegmentSizeFromEnv(RecoverFrom呼び出しは:115)。(c)ファイル配置の`<dbname>.last_checkpoint`「既存形式のまま」→このsidecarは誰も書かない(テストが非生成をassert)。

**発生条件:** M0監査手順をそのまま実行すると、存在しない課題(WAL無限増大)の根拠データを取りに行く。実装着手時にStatus注記どおり履歴更新せず使うと誤設計につながる。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-23

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/environment_variables.md:26`

**指摘:** 環境変数TINYLAMB_DISABLE_CRC_LANESが「Disable the 4-lane CRC32C kernel」と文書化されているが、実装は8レーン(r0..r7の8分割)である。機能自体は動作するが説明が古い。

**発生条件:** A/B検証や性能解析で「4レーンカーネルを無効化する」つもりでこの変数を設定した担当者が、実際には8レーン実装を切り替えていることをドキュメントから読み取れず、測定結果の解釈を誤る。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-24

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/50-join.md:403`

**指摘:** §15 の方式選択表が「直近の時系列対応 | AS OF 結合」「時間帯・数値帯の全一致 | 区間結合」を依然として推奨しているが、同ドキュメント自身の §11(255行目)と §12(269行目)は AsOfJoin/IntervalJoin 実行器が2026-10-03に削除済みと記録しており、読者を存在しない実行器へ誘導する。また本ドキュメントには TryFusedGroupedJoin / TryStreamGroupedBaseJoin(fused/streamed grouped join、対象ファイル群の主要経路)の記載が一切ない。

**発生条件:** EXPLAIN/計画検討時に §15 表を参照した読者が AS OF/区間結合を選ぼうとしても該当実行器は削除済みで lowered されない。fused/streamed grouped join は GROUP BY 付き2表結合の実質的な既定経路(build_input.cpp:637-644 から呼ばれる)だがドキュメントに現れない。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-25

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/50-join.md:403`

**指摘:** 同一ファイル内の自己矛盾とインデックスの陳腐化: 50-join.mdは§11(:255)と§12(:269)でAsOfJoin/IntervalJoinの2026-10-03削除を明記しつつ、§15の方式選択表(:403-404)は依然「直近の時系列対応→AS OF結合」「時間帯・数値帯の全一致→区間結合」を選択肢として列挙する。また00-index.md:85/:116-117も削除済みAsOfJoin/TwoPhaseDistinctAggを実在演算子として掲載したまま。

**発生条件:** 選択表をプラン選択の指針として読むと、削除済みの2方式が依然選べるように見える。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-26

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/executor/80-window-recursion-distribution.md:145`

**指摘:** 再帰CTEのUNION DISTINCT重複除去を「既出行を保存したseen配列と線形比較…最悪O(M²)」と説明するが、実装はWorkTableIndex(ハッシュ索引)で、コード自体が「線形走査だった名残」のコメントを持つ。

**発生条件:** 大規模再帰CTEの性能解析で二次時間の説明としてこの節を引用すると、現行実装(1参照)と合わない。executor_todo.md:407-418はWorkTableIndex実装(320k行で3分17秒→0.27秒)を[x]として正しく記録しており、executorサブdocだけが旧実装のまま。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-27

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/lock_order.md:16`

**指摘:** docs/lock_order.md:15-17の規則6が「version shard mutex → transaction table lock」のネストを「Wound-Wait preemptionとデッドロック検出器のvictim woundingのみ」と限定するが、実際には第3系統が存在する。RecordIntentWaitTimeout(transaction_manager.cpp:1343)とDebugDumpAllIndexReservations(transaction_manager.cpp:2539)がシャードmutex保持下でtransaction_table_lockを取得する。方向自体は規則準拠(逆向きの取得路径は存在しない)なのでデッドロックリスクはないが、文書の網羅性が実装と不一致。

**発生条件:** 新規にtransaction_table_lock絡みの路径を追加・レビューする際、文書の「のみ」列挙を前提にすると、タイムアウト診断パスでのシャード→テーブルロックネストを見落とし、逆向き取得を誤って追加する土台になる。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-28

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/lock_order.md:8`

**指摘:** PageManagerのロック名が「`pages_mu_`」だが実名は blocks_mu_。他の記載(trees_mu_, table_tails_mu_, chain_mu_, enqueue latch→work mutex, VersionShard→transaction table lock, 256 shard, (page_id*131+slot)%256)はコードと一致している。

**発生条件:** docs/AGENTS.mdのルール(新ラッチ追加時はlock_order.md更新)に従い注文書きを編集する際、実在しないミューテックス名を追う。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-29

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/lock_timeout.md:22`

**指摘:** 「supports four deadlock policies」の列挙は4つだが、DeadlockPolicyは5値で kNoWait=4 が実在する(ドキュメント自身が13行目でkNoWaitに言及しており内部矛盾)。既定kDeadlockDetect・kLegacy 5msの記述は一致。

**発生条件:** ポリシー全数網羅のテストやSetDeadlockPolicy呼び出し箇所のレビューでkNoWaitを見落とす。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-30

**判定:** 前回監査の網羅性に関する注意（製品バグではない） (`audit-coverage-limitation`)

**元の対象:** `docs/opt/part2-rules/logical/ (77ファイル)`

**指摘:** 論理Rule章77本と式Rule章35本のうち、論理は全ファイルの「定義位置+登録名」検証(77/77がcascades.cppのAdd(Rule(登録と一致、file参照420件も全て実在)まで実施したが、本文全文は未読。式は9本を全読し、残り26本は登録名突合(json_family_constant_foldがdoc不在である点以外は一致)まで実施。docs/next-actions.md(3913行)は冒頭~第58報(:1-60付近)と回帰修正の検証(ScanFilterTest.ConstantResidualBoundaryNeverDecidesColumnConjuncts=executor/executor_extra_test.cpp:327、QueryTest.GroupedHavingErrorAbsorptionStaysPerRow=query/query_test.cpp:262、scan_filter.cpp:516の列参照ゲート)までで、全文は未読。

**発生条件:** 残部(式26本・論理77本の本文、next-actions中盤以降)に同種の陳腐化が潜む可能性は排除できない。

**確認・対応記録:**

- This entry reports incomplete coverage of the previous audit, not a concrete product defect. No inference of correctness for unread prose: the present audit tracks explicit defects and fixes separately. Registration identity is not evidence of semantic equivalence or complete documentation review.

### low-31

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `docs/optimizer_rule_audit.md:5`

**指摘:** docs/optimizer_rule_audit.md(2026-10-02)が論理83登録の完全列挙を『ソースと順序まで照合』(159行)と主張するが、現行ツリーには列挙にない3規則(constant_unnest_to_values, limit_prefix_ordered_window, limit_prefix_unordered_window)が追加で登録されており、一覧は最新でない。特に limit_prefix_*_window は LIMIT×WINDOW の新経路で文書化がない。

**発生条件:** 監査文書を根拠にルール到達性を議論すると、新規3規則が監査外のまま見逃される。

**確認・対応記録:**

- medium-17/18 missing chapters and regenerated current registry list in optimizer_rule_audit.

### low-32

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/todo.md:1377`

**指摘:** 「`PagePool::CacheHits()/CacheMisses()` は実測計器として既に存在 (page/page_pool.hpp)」の記述。PagePoolはAGENTS.md§2.2「PagePool is retired」の通り廃止済みで、page/page_pool.hppもクラスも存在しない。実際の計器はPageManager(page/page_manager.hpp:189 CacheHits())側にある。

**発生条件:** 計器をEXPLAIN ANALYZEへ出す実装(todo.md同項の着手点)をPagePool前提で書くと存在しないヘッダを参照する。

**確認・対応記録:**

- Cross-checked current LogicalOperator enum, rule registrations, classes and SQL routes. Corrected phantom Max1Row/Mark/Spool/Exchange/NUMA/CardinalityProbe/Returning/Merge classes, actual MERGE/TABLESAMPLE/UNNEST/set-op routes and PageManager counters. Pinned execute_query rejects SQL-standard FILTER syntax but parses AGG(x WHERE p). Changes: /tmp/patch-audit-todos.py (batch33).

### low-33

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/todo.md:300`

**指摘:** todo系どうしの判定矛盾: todo.md:300は「SQL標準FILTER (WHERE ...) をASTから実行器まで接続する[x]」に対し、executor_todo.md:180は「[ ] SQL標準 AGG(...) FILTER (WHERE …) の AST 接続」。コードで確認できた実装はGoogleSQL形 AGG(x WHERE p)(expression/aggregate_expression.hpp:93-97 WhereFilter)で、標準構文 FILTER (WHERE …) のパーサ表面までは確認できていない。

**発生条件:** どちらが正かでFILTER句の対応状況が逆に読める。GoogleSQL WHERE形のみなら、標準構文表面の追加が残項目として扱われるべき。

**確認・対応記録:**

- Cross-checked current LogicalOperator enum, rule registrations, classes and SQL routes. Corrected phantom Max1Row/Mark/Spool/Exchange/NUMA/CardinalityProbe/Returning/Merge classes, actual MERGE/TABLESAMPLE/UNNEST/set-op routes and PageManager counters. Pinned execute_query rejects SQL-standard FILTER syntax but parses AGG(x WHERE p). Changes: /tmp/patch-audit-todos.py (batch33).

### low-34

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/value_storage.md:5`

**指摘:** docs/value_storage.md の "Current model" が Value を「ValueType 判別子 + スカラー用union + kVarChar用std::stringメンバ」と記述するが、実装はさらに kArray 用の `std::shared_ptr<ArrayPayload> array_`(value.hpp:571、要素ベクタ+要素SQL型名)と、比較/直列化に影響する collation/unsigned タグ1バイト(value.hpp:574、UINT64マーカー含む)を持つ。配列とタグはストレージ設計の現状記述から欠落しており、mid-term移行表にも配列/タグ領域がない。

**発生条件:** このdocを根拠にValueレイアウトの変更・移行設計をする場面で、ArrayPayloadのshared_ptr共有やcollationタグ(直列化でもmemcomparableでも運ばれ一部経路のみ現れる)が考慮から漏れる。README(type/README.md:12-23)はタグ3=unsignedを記述しており、value_storage.mdのみが古い。

**確認・対応記録:**

- Corrected retired TSAN justification (deadlock checking still explicitly disabled, not claimed covered), Value owned/borrowed payload model, nested-loop error propagation and malformed query guide formatting; source reviewed (batches20-27).

### low-35

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/wal_format.md:36`

**指摘:** チェックポイントmagicの記述「`kFuzzyCheckpointMagic`, `TLCP`」のバイト列が誤り。実際はTLSMCKP1。

**発生条件:** .dbファイルの識別・手動解析でdoc通りのmagicを探すと見つからない。page_format.md:25は正しい値を記載しておりdoc間でも矛盾。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-36

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `docs/wal_format.md:70`

**指摘:** `TINYLAMB_WAL_RETENTION_WARN_BYTES`(既定1GiB)によるリテンション警告(pinした項の命名付き)はコードに存在しない。

**発生条件:** 長時間稼働でWALがfloorにpinされた際、docsが約束する診断警告は出ないため検出が遅れる。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-37

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/batch_nested_loop_join.cpp:92`

**指摘:** 50-join.md:77「左右の子が false を返した理由は確認せず、子の状態も転送しない。そのため、この演算子の状態だけでは、入力側で起きた失敗を判別できない」に反し、実装は右子(91-94)と左子(119-122)のGetStatus()をpredicate_error_に保存しNextBatch(208-211)でFailWithして表面化する。子の状態は転送・判別可能になっている。

**発生条件:** 右子が途中で失敗した場合、BatchNestedLoopJoinはpredicate_error_=子ステータスとして出力前に停止し、呼び出し側は子の失敗原因を知ることができる。ドキュメント記述の「判別できない」は成立しない。

**確認・対応記録:**

- Corrected retired TSAN justification (deadlock checking still explicitly disabled, not claimed covered), Value owned/borrowed payload model, nested-loop error propagation and malformed query guide formatting; source reviewed (batches20-27).

### low-38

**判定:** 既に対処済み＋関連テスト成功 (`already-fixed-tested`)

**元の対象:** `executor/detail/build_input.cpp:4423`

**指摘:** PK payload 吸収で VARCHAR を LSTableStore のバッファへ string_view のまま行に埋め込む経路(2154-2155 の pinned_stores 登録、2322-2325 の VarcharAt 参照)があり、BuildInput 末尾の再所有コピー(4423-4434)は `fusion == nullptr || !fusion->fired` のときしか走らない。fusion 発火時は TryFusedGroupedJoin に生 view の行が渡されるが、fused_grouped_join.cpp には owned_data の再所有処理が無い(grep で確認)。ストア自体は Table にキャッシュされた shared_ptr(lstable_bridge.cpp:1042-1048)なので即座の dangling は成立しにくいが、view がレイアウト世代(generation)更新後も生存しうる設計で、世代切替時期との相互作用が未検証。

**発生条件:** VARCHAR payload の PK 吸収が fired した後、同一トランザクション内で DDL/DML により LSTableStore が再構築(generation 更新)されると、融合結果行の string_view が旧バッファを参照したままになる可能性。実害の発火条件は本精読では確認できておらず、要追跡。

**確認・対応記録:**

- Current BuildInput unconditionally owns borrowed VARCHAR results whenever pinned_stores is nonempty, regardless of fusion->fired. Existing AbsorbedStringsKeepOwnershipAndSnapshots regression covers grouped result ownership and dimension mutation/snapshot survival; full relational_regression 176 tests passed under ASAN/UBSAN in batch48. Original conditional described by report is absent even in current HEAD.

### low-39

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/flat_int_key_map.hpp:247`

**指摘:** FlatIntSetのdenseモードはInsertDenseAtomic/Insert/Containsが [dense_min_, dense_max_] 境界を一切検査せず dense_bits_[off>>6] を直接索引する。予約範囲外のキー1つでヒープ破壊(OOB書込み)になる未防護契約で、防護は全て呼出側のゾーン統計正確性に委ねられている。

**発生条件:** scan_filter.cpp:961-962/1265がstoreのcolumn_min_int/max_intでTryReserveDenseした後に実セル値を挿入する構成。ゾーン統計と実デコード値の不整合(将来の統計保持変更・部分構築ストア等)が起きた時点で範囲外書込みに直結する。現行呼出側は整合を保っているため today 到達経路は確認できず。

**確認・対応記録:**

- Out-of-range dense key reproduced a vector bounds assertion before fix. Dense insert checks bounds, records overflow and membership build declines optimization before publishing incomplete set. scan_filter tests pass (batch23).

### low-40

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:4097`

**指摘:** CHR の引数型検査がなく、INT64 以外(DOUBLE/VARCHAR/DATE)は黙ってコードポイント 0 扱いになり NUL 文字 1 バイトの文字列を返す。GoogleSQL は整数以外の引数を型エラーにするため、サイレント誤結果。

**発生条件:** `SELECT CHR(CAST(2.5 AS FLOAT64))` → エラーではなく "\0" を返す。コードポイント 0 は有効範囲(0..0x10FFFF)なので 4101-4103 の範囲チェックも通過する。

**確認・対応記録:**

- CHR rejects non-INT64 non-NULL values in AST and relational evaluator. DOUBLE/STRING/DATE regressions failed before and pass after (batch23); previous permissive AST test corrected.

### low-41

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/function_eval.cpp:6760`

**指摘:** 同一エンジン内でセッション主の報告が割れる。SESSION_USER() は 1368-1380 の先行ブロックで getenv("USER")/LOGNAME の OS アカウントを返す(コメントで意図と明記)のに対し、CURRENT_USER() は 6760-6766 で常に "tinylamb" を返す。第2ブロックの session_user アームは先行ブロックによりデッドで、両者が同じ単一ユーザーを指すという契約が保てない。

**発生条件:** OS ユーザー kumagi でサーバー起動時: `SELECT SESSION_USER()` → 'kumagi'、`SELECT CURRENT_USER()` → 'tinylamb'。env の無い環境では両者とも 'tinylamb' になり、実行環境で結果が変わる。

**確認・対応記録:**

- Before: AST current_user unsupported; SQL SESSION_USER returned OS user but CURRENT_USER hardcoded tinylamb. Both evaluators now share existing USER/SESSION_USER branch for CURRENT_USER; dead hardcoded branch removed.
- Function AST sweep8, SQL audit25 all pass; /tmp/audit-batch46-tests.log.

### low-42

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/detail/planning_heuristics.cpp:1391`

**指摘:** ParallelHashProbe の前置コメントが「flush the buffers through result->AddRow one chunk at a time in strict chunk order, so output row order matches the serial loop exactly」と宣言するが、実装はチャンク順ではなく worker 順に shard を連結する。行順序自体は docs/executor/50-join.md:127(§6.2「SQL 結果全体の順序は保証しない」)どおり SQL 契約上問題ないが、コード内契約記述が実装と不一致。

**発生条件:** workers=4、チャンク0..7 の場合、チャンク割当(1427行目 chunk = worker_id; chunk += worker_count)により worker0 が 0,4 / worker1 が 1,5 … を処理し、マージ(1505-1523行目)は shard0(=チャンク0,4 の行)→shard1(=1,5)… の順で AddRow するため出力は 0,4,1,5,2,6,3,7 順となり逐次ループ(0,1,2,…7)と一致しない。LEFT JOIN の非一致行も各 shard 内で出るため全体順は serial と異なる。

**確認・対応記録:**

- Corrected ParallelHashProbe comment: static interleaved chunks, worker-order merge, per-worker spilling Relations and optional private outer-match bitmaps. No serial-order claim.
- Normal relational_regression, scan_filter, differential suites passed; /tmp/audit-batch44-tests.log. Spill-specific deferred ordering extension still to run.

### low-43

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/relation.cpp:176`

**指摘:** AddRow のスピル書き込み失敗経路で `spill_tail_.reset(); ++spilled_rows_;` とし、(a) 落とした行なのに spilled_rows_ を加算し統計を壊し、(b) 以後の AddRow が `spill`(完了済みの旧行)と `rows`(新行)の併存状態に遷移する。ForEachRow(relation.hpp:70-92)は rows → spill → spill_tail_ の順に排出するため、失敗後にメモリへ戻された新行が先に、旧スピル行が後に出力され、挿入順序契約が崩れる(ARRAY_AGG 等の順序依存出力に影響)。ドロップ自体は hpp:64-68 に文書化済みだが順序逆転は未文書。

**発生条件:** スピル中にディスクquota超過で spill_tail_->Append が失敗 → 以降の行をメモリに保持する過程で 2 バッチ目が budget 判定を通ると rows に積まれ、排出順が「新行たち → 旧スピル行」に逆転。

**確認・対応記録:**

- Expanded existing failed-append test: before fix successful spill tail disappeared and spilled_rows became2 rather than1. Retain tail and increment only after successful Append; sticky status still propagates.
- Normal relational_regression, scan_filter, differential suites passed; /tmp/audit-batch44-tests.log. Spill-specific deferred ordering extension still to run.

### low-44

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/scan_filter.cpp:2758`

**指摘:** TryParallelFilterRows が deferred 行(ワーカーでプログラムが raise した行)を各シャードの通常行より先に result へ積むため、worker 内の行順がシリアル走査と入れ替わる。scan_filter.hpp:256-258 は「emitted in worker order, so output order matches a serial scan of source」と契約明記しており、defer 発生時に契約違反。2670-2675 のコメントが「FP集約への行順を決定論的に」と言うのは成立するが serial 一致は成立しない。STRING_AGG/FP SUM のような順序敏感な下流でシリアル結果とbit一致しなくなる。

**発生条件:** 3万行超の filtered Relation で residual bytecode が一部の行でのみ raise(例: 型混合の比較)→ deferred 行が前倒しで出力され、STRING_AGG OVER 等の結合順がシリアル実行と異なる。

**確認・対応記録:**

- 40k-row OR filter with per-row division failures masked by second branch reproduced deferred rows reordered to front. Use contiguous worker ranges and deferred positions among accepted rows during merge; copy/move paths preserve source order.
- Normal relational_regression, scan_filter, differential suites passed; /tmp/audit-batch44-tests.log. Spill-specific deferred ordering extension still to run.
- Batch46 adds 64KiB-budget spilling coverage; all 4 copy/move x in-memory/spill cases retain 40000 source rows in exact order.

### low-45

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/detail/scan_filter.hpp:256`

**指摘:** scan_filter.hppのTryParallelFilterRowsコメント「output order matches a serial scan of `source`」は過剰宣言。bytecodeがraiseした行(defer)はshardのaccepted行より先にマージされるため、defer行が1つでもあると出力順はserial走査と異なる(実行ごとの決定性は保たれる)。

**発生条件:** residual式が特定行だけでbytecode raise(defer=2)するフィルタを32768行以上のrelationに適用 → 2758-2766のマージで worker w のdeferred行を先にresultへ追加し、その後shard wのaccepted行を追加。元のソース順ではdefer行はaccepted行の間に挟まれていたため、FP感受性集約に渡る順序がserial版と変わる。

**確認・対応記録:**

- Implementation now preserves input order, including deferred rows; header states contiguous worker ranges and reinsertion at original accepted-row position.
- Normal relational_regression, scan_filter, differential suites passed; /tmp/audit-batch44-tests.log. Spill-specific deferred ordering extension still to run.
- Batch46 adds 64KiB-budget spilling coverage; all 4 copy/move x in-memory/spill cases retain 40000 source rows in exact order.

### low-46

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/detail/window_eval.cpp:1157`

**指摘:** 同 doc:106「ウィンドウ版の APPROX_QUANTILES は、個数が正かどうかだけを確認する。100000 という上限と引数型を十分に検査せず」はコードと不一致。実装は引数型を明示検査し(1157-1169: Int64以外は Double は範囲+NaN 検査、その他はエラー)、1156-1173 で num <= 0 || num > 100000 をエラーにする。上限と型検査は実装済みでドキュメント記述が古い。

**発生条件:** ドキュメントを信じて個数 100000 超や非数値型が黙って通るものと説明すると、実際は StatusError(kInvalidArgument) となり食い違う。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-47

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/detail/window_eval.cpp:1403`

**指摘:** 同 doc:97「ARRAY_AGG は NULL 要素を保持せず、全要素が NULL なら NULL を返す」は実装と逆。既定(RESPECT NULLS)では values( NULL 含む)をそのまま配列化し(1402-1405, 1431-1438)、全要素 NULL でも「NULL の配列」を返す。テスト WindowArrayAggPreservesNullElements(query/sql_window_agg_coverage_test.cpp:647-667)が NULL 保持と all-NULL で配列返却を固定。

**発生条件:** ドキュメント前提で ARRAY_AGG の NULL 扱いを説明/検証すると、実際の配列内容( NULL 要素あり)と不一致。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-48

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/detail/window_eval.cpp:1907`

**指摘:** docs/executor/80-window-recursion-distribution.md:13 は「パーティションを探すときは既存のキー列を線形探索するため…最悪 O(NG)」と記述するが、実装は unordered_map(PartitionKeyHash/PartitionKeyEq)によるハッシュ表で O(N)。コード自身のコメント(1897-1906)も「linear search … made that linear search the dominant term」と線形探索が置換済みであることを明記しており、ドキュメントが古い。

**発生条件:** ドキュメント読者がウィンドウの多パーティション構築を O(NG) と見積もり、性能評価や修正優先度を誤る。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-49

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/detail/window_eval.cpp:2193`

**指摘:** 同 doc:76「現在の位置参照処理は NULL を飛ばさず…この指定を実際の処理に反映するのは、後述するパーセンタイル関数の RESPECT NULLS だけである」は、FIRST_VALUE/LAST_VALUE/NTH_VALUE の IGNORE NULLS が実装済みであること(2193-2200, 2210-2222, 2260-2268、RESPECT NULLS 時の評価は 2280-2285)および ARRAY_AGG の ignore_nulls 反映(1098, 1403-1405)と矛盾する。テスト WindowIgnoreNullsFiltersAnalyticValues(query/sql_window_agg_coverage_test.cpp:669-690)がこの挙動を固定している。

**発生条件:** ドキュメントに基づき IGNORE NULLS が無視される前提のレビュー/クエリ設計を行うと、実際の NULL スキップ結果と食い違う。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-50

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/detail/window_eval.cpp:2504`

**指摘:** 同 doc:19「複数の関数が同じ PARTITION BY と ORDER BY を使う場合も、現在の処理では配置を共有しない。W 回分の整列費用」に対し、文レベルの適用経路 ApplyWindows は WindowLayoutKey でレイアウトをキャッシュ共有する(2504-2524、ComputeOneWindowImpl の cached_layout)。Cascades の WindowExecutor 単体(executor/window.cpp:115 が cache なしで呼ぶ)には当てはまるが、「現在の処理では共有しない」はステートメント経路について不正確。

**発生条件:** 同一 P/O の複数ウィンドウ関数を含む SELECT の整列コストを W 倍と見積もり、最適化判断を誤る。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-51

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/executor_base.cpp:13`

**指摘:** docs/executor/10-execution-model.md の契約記述が実コードと正反対。§2(22行)は「仮想関数 Next、既定実装を持つ NextBatch」、§3(59行)は「NextBatch 未実装なら既定実装が Next を繰り返してバッチを組む」と述べるが、実コードは NextBatch が純仮想(既定実装なし)で、Next が非仮想の NextBatch(dst,1) ループ用ラッパ。

**発生条件:** ドキュメント通り「Next だけ実装すればバッチAPIは既定で得られる」と認識した実装者が新演算子を追加すると、NextBatch が純仮想のためコンパイルできず、逆方向の既定実装の存在を取り違える。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-52

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/grouped_aggregate_test.cpp:1075`

**指摘:** TheEmptyInputReachesNeitherGroupedSpillPath の後半で、コメント「each keeps that one group as its whole state」(1069-1070行)が主張する keyless scalar ハッシュ形状の状態検査が实际には行われていない。1071-1072行の Collect(HashShape(empty, {}, ...)) は一時オブジェクトを生成して捨てるため PeakResidentGroups() を観察できず、1075行の EXPECT_EQ(hash->PeakResidentGroups(), 0U) は 1051行で作り 1057行で既に検査済みの keyed ハッシュ executor を再検査しているだけ(vacuous な重複)。scalar hash 側の「1 group を保持」は未検証のまま(sort 側だけ 1081行で scalar_sort->PeakResidentGroups()==1 を検査)。

**発生条件:** keyless 空入力時にハッシュ形状が scalar group を正しく1つ保持できなくなった(例: peak が0のまま1行を出す、または状態を二重保持する)regression でも本テストは検知できない。1068-1085 のコメントと検査対象の不一致。

**確認・対応記録:**

- Keep scalar hash executor and assert its actual peak. Exposed missing empty-scalar metric update, now NoteResidentGroups records1; grouped32pass.

### low-53

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/grouped_aggregate_test.cpp:579`

**指摘:** HavingKeepsOnlyGroupsWhoseOutputRowIsTrue で SortShape に HAVING を適用した出力が一度も内容検査されていない。579-585行のループは Hash と Sort の両形状を Collect するが、ループ内で検査されるのは status だけ。内容アサーション(583-584行)は同じ HashShape を再生成して比較するもので、581行で収集した SortShape の行は破棄される。StreamShape の内容は 587-589行で検査されるため、三形状のうち Sort の HAVING フィルタだけが membership 未検証。

**発生条件:** SortAggregateExecutor の HAVING が「通すべき group を落とす/落とすべき group を通す」ように regress しても、本テストは Sort の status 成功のみを見るため緑のまま(HAVING コンテンツ差分は他の Sort テストが全て having=nullptr で未カバー)。

**確認・対応記録:**

- HAVING test now checks collected rows of both Hash and Sort, rather than re-running Hash; grouped32pass.

### low-54

**判定:** 防御を追加＋関連テスト成功 (`defensive-fix-tested`)

**元の対象:** `executor/hash_join.cpp:1066`

**指摘:** NULL補完幅の推論が「right_width_==0 && !right_rows.empty()」の条件のため、相手側が空で幅が未指定だと幅0のNULL補完行を生成する(MaterializeOuter:1112-1134、MaterializeSingle:870)。出力スキーマはleft+right幅を期待するため、後続で幅不一致(誤結果/クラッシュ)になる。parallel_merge_join.cpp:460-465・band_join.cpp:172-177も同じ推論方式。

**発生条件:** 第3コンストラクタ(hash_join.hpp:95-98、right_width/left_widthデフォルト0)でkLeftOuter等を直接構築し、右子が0行のとき left.first + Row(std::vector<Value>(0)) が出力される。現行の全factory経路とテストは幅を明示渡ししており(relational_factory.cpp:1556-1562・1617-1624、executor/hash_semi_reduction_test.cpp:664-669等)、本番到達は未確認の潜在契約ギャップ。

**確認・対応記録:**

- Hash outer/single joins and ParallelMergeJoin fail with InvalidArgument when an empty null-supplying side has unknown width; supplied widths produce full NULL padding. Added left-width argument to parallel merge API, kept factory dimensions explicit. Hash single also infers width from nonempty rows. All36 hash-semi-reduction tests incl direct constructor empty-side regressions pass (batch54); production factory already passed schema widths.
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### low-55

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/hash_join.cpp:1093`

**指摘:** 50-join.md:146「残余条件は、上位の Selection または残余述語を評価する別の結合方式で適用する」に反し、HashJoin自身が外部結合経路でresidual_predicate_を評価している(MaterializeOuterのpair_passes 1088-1099、一致判定1119-1134)。factoryも外部結合kindに対してSetJoinResidualでresidualをHashJoinへ接続している(relational_factory.cpp:1570-1574, 1629-1633)。

**発生条件:** 残余述語付きの左/右/完全外部ハッシュ結合では、HashJoinがキー一致対に残余を適用し、満たす対がない左行をNULL補完する(これは50-join.md:36の§1.2の記述と整合する動作)。§6.4末尾の「上位Selectionか別方式で適用」の説明が実装と不一致。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-56

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/incremental_sort.cpp:227`

**指摘:** docs/executor/60-sort-distinct-set.md:135-137のIncrementalSort記述(「現在の実装は入力全体を先に保持する…パイプラインブレーカーとなり、メモリ使用量は O(N)」「バッチ API は保持済みの sort_error を確認せずに出力を返す」「接尾キーの整列には PdqSort の順序を使う」)は現行実装と不一致。現行はPrefixRunで1区間ずつストリーミングし、接尾整列はPdqSortではなくspill可能なSortExecutor、NextBatchは入口でGetStatus()を確認する

**発生条件:** 巨大入力のORDER BY prefix,suffixで本演算子を使うと、ドキュメントのO(N)常駐ではなく1ピアグループ+SortExecutorのspill予算で動作する(グループ単位のメモリ bound)。PdqSortを前提にしたNaN順序の記述も、現行はSortExecutorのmemcomparable符号化順に従う

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-57

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/lookup_join.cpp:280`

**指摘:** 50-join.md:109は「左入力の状態は転送しない…この境界から見ると、false は常に入力の末尾を表す。そのため、下位の障害を親まで完全に伝えることはできない」と記述するが、実装は左子のステータス(279-281)と索引反復子のステータス(310-313, 344-346)をFailWithで自ステータスに貼り付け、親へ伝播している。ドキュメントは実より弱い(古い)挙動を記述している。

**発生条件:** 左子実行器またはIndexScanIteratorが失敗したとき、LookupJoin::NextBatchは0行+失敗ステータスを返し、上位はエラーを観測できる。ドキュメント通り「末尾と区別できない」わけではない。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-58

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/merge_append.cpp:149`

**指摘:** docs/executor/60-sort-distinct-set.md:217「構築時に少なくとも一つの入力が必要である。入力が 0 個だと…存在しない最初の入力の状態を参照してしまう」は現行コードと不一致。NextBatchはheads_.empty()時にsources_.empty()を確認してからFailWithChildOfを呼び、0入力は0行を正常返す

**発生条件:** 空のsourcesでMergeAppendExecutorを構築・実行しても参照違反や誤動作は起きず、0行+成功で終了する(空集合は別演算子で表す、という制約は実態として撤回済み)。ドキュメントが古い実装のクラッシュを説明し続けている

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-59

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/merge_append.cpp:96`

**指摘:** docs/executor/60-sort-distinct-set.md:27「比較ベースのパスはすべて SortKeyOrder を使う: … MergeAppendExecutor …」は不正確。MergeAppendExecutorはSortKeyOrder/CompareWithCollation/EqualUnderCollationを一切使わず(grepで0件)、Value::operator< / != にApplyKeyCollation畳み込みを組み合わせる実装。NaN対通常値はどちらの<も偽となり走査位置依存の順序になる(これはdocs:223が別途認める挙動だが、§1.1の『全パスがSortKeyOrder』という契約記述と矛盾)

**発生条件:** FLOAT64キーでNaN先頭の入力を併合すると、NaNがインデックス0に来たとき最優先で出力され、それ以外では通常値の後回しになる等、入力番号依存の順序になる。ci照合VARCHARはApplyKeyCollation畳み込みで結果的に整合するが、契約の型が違う

**確認・対応記録:**

- NaN merge ordering failed before the SortKeyOrder fix; merge_append 12 tests and sort_collation 24 tests pass (batch16).

### low-60

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/merge_join.cpp:130`

**指摘:** 50-join.md:220「子が失敗していても、その時点までに保持した行を使ってマージ処理を続けるため、部分的な出力が残る場合がある。…返された行の有無だけでは、結果が完全かどうかを判断できない」に反し、実装は左/右子の失敗を検出するとoutput_.clear()して即returnし、マージ処理は一切行われず部分出力は残らない(失敗時は常に0行+失敗ステータス)。

**発生条件:** 左子が途中で失敗した場合、FailWithChildOf(*left_)が真となりoutput_はクリアされる。NextBatchはGetStatus()!=kSuccessで0行を返す(331-333)ため、ドキュメントが警告する「部分結果と完全結果の区別がつかない」状態は存在しない。ドキュメントが旧挙動を説明したまま。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-61

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/multi_index_covering_scan.cpp:148`

**指摘:** AssembleFromHeapがcovered slotがヒープ行幅を超えるとき空Rowを返し、NextBatchはStatusを記録せずその行を静かに棄てる。本実行器の他の失敗経路(179-185,200-207)はすべてFailWithするのに、ここだけエラー飲み込みでデータ欠落

**発生条件:** heap_needed候補行のtable_.Readが成功しても行幅がoutput_slots_の最大slot+1未満に縮むレイアウト不整合(スキーマ進化・投影済み行の流入)が起きた場合、交差判定を通過した行が結果から消え、クエリは成功のまま完了する

**確認・対応記録:**

- AssembleFromHeap returns StatusOr<Row>/Corrupt for missing covered slot and NextBatch propagates; no silent row drop. Dedicated corrupt-row test pending.

### low-62

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/parallel_aggregation.hpp:92`

**指摘:** AccumulateGeneric のヘッダ契約コメント『always_generic と fallback は(both ascending)一つの昇順列として歩く』に対し、fallback(generic_scratch) は int64→double→double-binary→stat のカテゴリ順で append されるためカテゴリ間でインデックスが交差し昇順にならない。各集約が独立の状態スロットを更新するため結果への影響はないが、コメント契約は維持されていない。

**発生条件:** SELECT SUM(a), SUM(b), SUM(c) のように複数列集約が混在し(インデックス0/1/2が異なるカテゴリのtyped fast pathに割り当てられ)、かつ実行時にレイアウトが宣言型と異なるバッチ(例: 全NULLでないkNull以外の型ズレ)が来ると generic_scratch=[0,2]+[1] のように非昇順になり、コメント契約に依存した今後の改修(昇順前提の二ポインタ併合など)で誤動作しうる。

**確認・対応記録:**

- Corrected disjoint-index iteration comment: fallback input-kind order is not ascending; independent aggregate states.
- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-63

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/parallel_hash_join.cpp:560`

**指摘:** 並行版のkNullAwareAntiが「build側が空」でもNULLキーのprobe行を落とす(!probe_has_null && !lookup)。直列版はbuild_total==0でprobe_survivesを通さず全probe行(NULLキー含む)を出力し(hash_join.cpp:1413-1430)、50-join.md:29「S が空なら、x が NULL でも全左行を返す」に一致する。並行版は直列版と結果が一致しない。

**発生条件:** SharedBuildParallelHashJoinをJoinKind::kNullAwareAntiで構築し、build側0行・probe側にNULLキー行を投入すると、直列HashJoinはその行を出力するが並行版は出力しない。現行の本番経路では到達不能(relational_factory.cpp:1655はkind=kInnerをハードコード)で、テスト等の直接構築時のみ発火。

**確認・対応記録:**

- Track actual empty build (not hash-table emptiness). Empty-set NULL-aware anti emits NULL probe; reproduced before/fixed after, extra36pass.

### low-64

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational.cpp:5759`

**指摘:** RelationalExecutor::Dump が「hash_joins>0 かつ2表結合」という条件だけ学で、実際に適応切替(NestedLoop→HashJoin)が起きたか無関係に固定文字列の AdaptiveJoin 診断を出力する。

**発生条件:** 最初からハッシュ結合として実行された普通の2表クエリの Dump にも "AdaptiveJoin initial=NestedLoop final=HashJoin / switch_reason=outer_rows_exceeded_threshold" という虚の診断が出る。この出力を grep するプロファイリング/回帰確認が誤導される。

**確認・対応記録:**

- Direct RelationalExecutor 200x200 join materialized4000 rows, hash_joins1 nested_loop_joins0 but printed invented adaptive switch before fix. Removed fabricated diagnostic; actual physical HashJoin adaptive instrumentation unchanged.
- Focused relational regression2 pass, including spill/copy/move deferred filtering source-order extension; /tmp/audit-batch46-tests.log.

### low-65

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `executor/relational_regression_test.cpp:88`

**指摘:** オラクルヘルパー RelationalRun がドレイン後に executor の GetStatus() を検査しない。88-90行の while(Next()) ドレインの後、91行は context.PreCommit() のみを EXPECT し、最終行以降に発生した sticky エラー(および Next が false を返した理由がエラーか正常終了か)を無判定で返す。同ファイル ExplainAnalyze(536-539行)も同様。同一パターンが executor/executor_test.cpp:3688-3693 の RelationalRun にもある(そちらには RelationalThrow:3697-3720 が存在するが RelationalRun 自体は無検査)。多数のテストの期待値生成に使われる主要オラクルなので、post-drain エラーは無言で握り潰される。

**発生条件:** 最終行を返した後にクリーンアップ/後段フェーズでエラーが latch されるクエリ(STICKY Status を返す executor の失敗)が regress しても、RelationalRun は行を正しく返して PreCommit 成功を確認するだけでテストは緑のまま。中間エラーは行数不一致の assert で偶然捕捉されるが、末尾エラーは一切検知されない。

**確認・対応記録:**

- RelationalRun primary helper already checked status after earlier audit; added status assertions to executor_test helper and ExplainAnalyze helper. Full executor226 and relational174 pass.

### low-66

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `executor/sort.cpp:94`

**指摘:** docs/executor/60-sort-distinct-set.md:17「SortExecutor のバイト列符号化は、昇順で NaN を +∞ より後ろに置く」および:47「NaN を一つの表現へ正規化し、昇順で +∞ より後ろに置く」は現在のコードと不一致。コードは全NaNを8バイトのゼロへ正規化し(-infより前=非NULL値の先頭に置く)、コメントはCompareForOrderBy(value.cpp:1614-1622: NULL<NaN<全非NULL)と一致すると明記

**発生条件:** NaNを含むFLOAT64キーのASCソート。実際の動作は NULL < NaN < -inf < finite < +inf(TopN/PdqSortと一致)。ドキュメント記載の「NaNは+∞の後」は古い実装の挙動で、現行三方式の順序整合性を正しく説明できていない

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-67

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `executor/update.cpp:69`

**指摘:** 主キー事前検査でpending行のReadが失敗した場合、old_keys[i]に空文字列""を代入して続行する(エラーを飲み込み)。""は空文字列の実キー(UpdateKeyString("")=="")と衝突し、live_keysから誤ったキーを取り除いて主キー重複検査を不正化する。同一関数のFK経路(110-114行)は同じRead失敗でFailWithしており矛盾

**発生条件:** enforce_primary_key_有効なUPDATEで、pending位置のtarget_->Readが失敗(MVCC/競合・破損など)し、かつ表に空文字列主キーの行が存在すると、その行のキーが事前検査から外され、後続pending行が同じ空文字列キーを主張しても重複エラーにならず二重キー行が書かれる。FK検査なしの文では全体が成功扱い

**確認・対応記録:**

- UPDATE primary key precheck propagates failed old-row reads, rejects empty rows and checks full-scan iterator status; fault injection pending.

### low-68

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `expression/bytecode.cpp:459`

**指摘:** docs/expression_evaluation.md:56-57(「jumps are outside the fast interpreter's set」)とbytecode.cpp:459-460の冒頭コメント(「jumps and the varchar/date kernels stay on the canonical path」)に反し、FastOpcodeはkJump/kJumpIfFalse/kJumpIfTrue/kPopに加えkBinaryVarchar/kBinaryDateまで高速解釈器の対象に含めており、TryEvaluateRowFastがジャンプ・Pop・varchar/date委譲を実装している。動作は差分テスト済みの正実装だが、文書とコードの記述が陳腐化している。

**発生条件:** 短絡AND/ORや検索CASEを含むプログラムがCompileでfast_interpretable_を付けられ、TryEvaluateRowFastで実行される(selection.cppやscan_filter.cppのTryEvaluateRowAuto経由)。ドキュメントの読者は「CASE loweringプログラムは必ず正規VMに落ちる」と誤解する。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-69

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/differential_test.cpp:942`

**指摘:** TEST(DifferentialTest, CompileFilter_SupportedOperations_MatchesAstAndBytecode) が名前に反して AST を一切比較していない。942行で const Value ast = expr->Evaluate(...) を計算するがその後一度も使われず、948行の EXPECT_EQ(bytecode, Value(jit_out != 0)) は bytecode↔JIT のみを照合する。differential スイートの根幹契約(AST=正準、全fast pathはそれに一致、expression/AGENTS.md冒頭に明記)に対し、AST↔bytecode と AST↔JIT の2辺がこのテストでは検証されず、ASTと bytecode/JIT が同時に逸脱しても検知できない。兄弟テストの projection 版(differential_test.cpp:976-980)は EXPECT_EQ(ast, bytecode) を行っており、filter 版だけ欠落している。

**発生条件:** JIT Filter カーネルと bytecode の比較フィルタが両方同じ誤ったNULL/比較セマンティクスに変わった場合(例: NaNや境界値の扱い)、ASTが正規でも本テストは緑のまま。未使用変数は -Wunused-variable を黙らせないため警告は出るが、比較の欠落自体は修正されない。

**確認・対応記録:**

- Filter differential now compares AST to bytecode and JIT. Full differential17pass (JIT enabled).

### low-70

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/function_call_expression.cpp:4723`

**指摘:** ARRAY<VARCHAR> を含む struct のテキスト往復で、" を含む STRING 要素が解析を崩す。正準テキスト(Value::AsString)は quote 付けのみでエスケープせず(type/value.cpp:635)、JsonTextToValue の配列解析は全ての " で quoted トグルし quote 剥ぎのみで unescape しないため。

**発生条件:** get_field / __pipe_concat / TryStructSetField で struct 中の配列フィールドを参照:ARRAY<STRING>["a"b", "c"] は要素2個ではなく単一の文字列 `a"b", "c` として解析され、以降の要素が破壊され誤値を返す。

**確認・対応記録:**

- Quoted ARRAY<VARCHAR/STRING> elements now use shared lower-layer JSON escaping. Struct-member parser handles escaped quotes/backslashes/control characters and UTF-16 surrogate pairs, rejects malformed tokens, and preserves empty array. Normal and ASAN/UBSAN proto_json_helpers8, value91, differential18 and json_text10 pass (batch55).

### low-71

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/function_call_expression.cpp:4853`

**指摘:** TryStructSetField が JsonTextToValue の戻り値(bool)を無視しており、ネストメンバの格納テキストが解析不能な場合、エラーにならず当該メンバ全体が黙って null に置き換わる(エラー飲み込み→データ欠落)。

**発生条件:** __struct_set(UPDATE のドット代入)でネストフィールド a.b を設定する際、格納済みメンバ a のテキストが JSON オブジェクト/配列/スカラのいずれの解析経路にも落ちない bare token だった場合、JsonTextToValue が false を返して nested が NULL のままになり、TryStructSetField(NULL) が4818-4820でNULLを返し、メンバが "null" に書き換わる。

**確認・対応記録:**

- Nested STRUCT update now propagates JsonTextToValue failure; malformed nested input regression and AST function sweep pass (batch23).

### low-72

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/function_call_expression.cpp:5437`

**指摘:** date_add/date_sub の ResultType が引数数を検査せず args_[0] を直接索引する(空引数で out-of-bounds 読み = UB)。同関数内の他の全分岐は args_.empty() を guard しているのにこの分岐のみ漏れている。two-schema オーバーロード(5574-5575)も同様。

**発生条件:** FunctionCallExp("date_add", {}) を手組みで構築し plan 段階で ResultType が呼ばれると vector の範囲外参照。パーサ生成の DATE_ADD は常に2引数(interval ノード要求)なので潜在的だが、公開ファクトリ FunctionCallExp から到達可能。

**確認・対応記録:**

- Empty DATE_ADD/SUB ResultType returns Invalid in both schemas; prior direct test crashed, after succeeds and TryEvaluate returns failure. FunctionASTsweep4pass.

### low-73

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/function_call_test.cpp:64`

**指摘:** Scalar() ヘルパーが行数/列数期待が既に失敗した経路でデフォルト構築の Value{}(kNull)を返し、呼び出し側がその .value.double_value / .value.int_value を読むため未初期化 union 読み(UB)になる。Value() の既定コンストラクタは type のみ設定し union ペイロードを初期化しない(type/value.hpp:178「Value() : type(ValueType::kNull) {}」)。EXPECT は非致命なので失敗後も実行が継続し、例えば 94-112行の EXPECT_DOUBLE_EQ(...value.double_value...) がゴミ値と比較される。

**発生条件:** SELECT が空を返す/複数列を返す regress(例: ScalarFunctionSqlTest の任意の EXPECT_DOUBLE_EQ 対象クエリ)が起きると、まず EXPECT_EQ(rows.size(),1U) が非致命で失敗し、直後に未初期化メモリの double 読みが発生。テスト結果の解釈を污染する(たまたま一致して偽陰性風に見えることもある)。

**確認・対応記録:**

- Test Scalar helper checks exactly one row and one column before indexing; full function_call_test 30 pass (batch23).

### low-74

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/json_doc.hpp:783`

**指摘:** JValFromSqlValueがローカルstd::string textへのstring_view(raw)を持つJValを返すため、ParseDocumentが文書化する生存期間契約(「raw views ... point into text, which must outlive the document」, json_doc.hpp:630-633)を即座に破る。現時点で返り値のrawは読まれない(canonical()は再構築)ため潜伏バグだが、LaxToString(json_doc.hpp:765)などraw読み手が将来これらの木に触れると解放済みメモリを読む。

**発生条件:** JSON_ARRAY('abc')の要素変換でJValFromSqlValueが呼ばれ、返却されたJValのrawが関数リターンでダングリング。今はfamily::Evaluate(json_doc.hpp:969, 993)が即canonical()へ渡すため無事だが、raw参照の追加が即UBになる状態。

**確認・対応記録:**

- Synthetic JVal nodes share immutable backing source ownership, including detached descendants; large integer raw text and child lifetime regression passes with rewrite suite (batch23).

### low-75

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `expression/sql_udf.cpp:77`

**指摘:** BindSqlUdfArguments の DEFAULT パディングループが、REQUIRED 追尾パラメータ(defaults の null エントリ)に対してエラーではなく NULL 値を黙って束縛する。ヘッダの「wrong argument counts raise」という契約文と不一致。

**発生条件:** SqlScalarFunction{params={a,b}, defaults={DEFAULT式, null(required)}} を RegisterSqlScalarFunction で登録し f(1) と呼ぶと、arguments.size()=1 は RequiredArgs()=1 を満たすため通過し、defaults[1]==null で emplace_back() により b=NULL が束縛される(正しくは引数不足エラー)。なお現行フロントエンド VisitCreateFunction(query/googlesql_ast_visitor.cpp:7597以降)は別レジストリ(SqlUdf/UdfRegistry)を使用しており、本APIへの到達は RegisterSqlScalarFunction の呼び出し元(現状テストのみ)経由。

**確認・対応記録:**

- RequiredArgs is last required position, not count; missing defaults metadata required. Bind only pads existing optional defaults. Reproduced silentNULL before, allnewcasespass.

### low-76

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `index/README.md:11`

**指摘:** index/README.mdの「Core Components」章が実装と不一致。BPlusTreeを「独立した自己平衡B+木、fine-grained lockingの並行制御」と記述するが、実際は`using BPlusTree = LsmIndex`の別名(index/lsm_index.hpp:121-122)で、実体はPageManager内のインメモリMemTable+SortedRun+コンパクション(page/page_manager.cppのLsmTreeState)。同章のLSMTree/MemTable/Sorted Runs/Compaction/LSMViewも独立クラスとして説明されているが、LSMViewというクラスはコードベースに存在しない(grepで0件)。またBPlusTreeIteratorの「traversing keys in sorted order」も、実際はコンストラクタでレンジ全体をvectorに具現化するスナップショット型(lsm_index.cpp:46-53)。冒頭の要約部(index/README.md:9-12)は正しいので、下方の章のみが古い。

**発生条件:** READMEを読んだ開発者がB+木のページ構造や「fine-grained locking」の並行性保証を前提に変更を入れると、実体は全範囲具現化+ツリー全体mutexのLSMなので誤った設計判断をする(例: イテレータの逐次追跡を前提にした実装)。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-77

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `index/index_scan_iterator.hpp:93`

**指摘:** PartitionBoundariesのコメントが「leaf-aligned partition boundaries ... See BPlusTree::ScanPartitionBoundaries」と実リーフ境界を示唆するが、実装は実リーフ構造と無関係な固定128キー/仮想リーフのキー数ベース分割(index/lsm_index.cpp:161-163 `constexpr size_t kKeysPerVirtualLeaf = 128;`)。キー長が不均一な場合、実リーフ粒度から大きく外れた分割になり、morsel分割の粒度均等性の記述と実動作が乖離している。

**発生条件:** 巨大VARCHARキーのインデックスでRangePartitionedScanを使うと、実リーフ数とは全く無関係な境界が返り、パーティション間のデータ量偏在が起きる(正しさは保たれるが、ドキュメントの粒度保証は無い)。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-78

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `page/AGENTS.md:20`

**指摘:** page/AGENTS.md が「up to `kMaxSlotsPerBlock = 256` slots per block」と記すが、実装は page/row_page.hpp:39 `static constexpr slot_t kMaxSlotsPerBlock = 4096;` で16倍の開きがある。

**発生条件:** ブロックロールオーバー境界・morsel分割(8ページ/morsel)のサイズ感や、スロット数起因のメモリ見積りをこの定数で行うと実態と乖離する。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-79

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `page/README.md:17`

**指摘:** page/README.md が廃止済みアーキテクチャ(PagePool の LRU eviction/pinning/evict 時 write-back、固定長ページ、MetaPage/LeafPage/BranchPage/FreePage の B+Tree、読み込み時 checksum 検証)を現行仕様として記述しており、docs/page_format.md:13-15 とルート AGENTS.md(PagePool 廃止)に正面から矛盾。さらに kMaxSlotsPerBlock=256 と記載するが実コードは 4096。PageRef も「pin/unpin で eviction 対象になる」記述だが実体は shared_mutex ラッチの RAII のみ。

**発生条件:** 本 README を読んだ開発者が固定長ページ/バッファプール前提の修正(PagePool 追加やpin管理)を行い、廃止済み設計への逆行やレイヤ違反を招く。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-80

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `page/page.hpp:94`

**指摘:** docs/page_checksum.md が記述する per-page CRC-32C 機構一式が実コードに存在しない。Page::SetChecksum() は空の no-op、IsValid() は type==kRowPage の型チェックのみ。checksum フィールド、PagePool::WriteBack/ReadFrom、TYB1 マジック+DecodeDisk、GetPageForRecovery はいずれもコードベースに実在しない(docs 記載の整合性保証は現状スナップショット全体の CRC32C のみ)。

**発生条件:** docs/page_checksum.md を信じた開発者は「ページ読み込み時に checksum 検証が行われる」「PagePool が WriteBack 前に checksum を更新する」ことを前提にするが、実装は page.hpp:94-95 のスタブのみで検証は一切行われない。

**確認・対応記録:**

- medium-21 current snapshot/PAX checksum documentation.
- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-81

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `page/page_manager.cpp:1366`

**指摘:** LoadFuzzyCheckpoint がファイル由来の未検証 u32 カウント(nslots/nentries)を内容検証前に resize/reserve する(1326 の slots_.resize、1328-1329 の committed_* resize、1366 の entries.reserve)。CRC を通過した工作済み(または異常な)チェックポイントファイルに対し数 GB 級の確保を試み、std::bad_alloc が StatusOr を返す公開ストレージ API から飛び出し得る(AGENTS.md §2.1 の「Status 境界で例外を漏らさない」に反し、起動時クラッシュになる)。

**発生条件:** <db>.db が巨大 nslots/nentries を主張する有効 CRC のファイルに置き換わった状態で DB をオープン → RecoveryManager::RecoverFrom → LoadFuzzyCheckpoint(recovery_manager.cpp:455)が reserve(数e9 × sizeof(Entry)≈80B) を実施 → bad_alloc でプロセス異常終了。ConsumeU32/U64 の境界チェックは切り詰めファイルのみ検出し、確保自体は防がない。

**確認・対応記録:**

- CRC-valid tiny snapshots claiming UINT32_MAX slots/entries threw bad_alloc before fix under 1GiB memory cap. Validate count against remaining minimum record bytes and slot limit before allocation; translate allocation/decode exceptions at Status boundary. page_manager_test 5 pass (batch18).

### low-82

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `page/page_manager.cpp:368`

**指摘:** AllocateNewPageはblocks_mu_のスコープを抜けた後、ページの内容ラッチを取らずに page->SetPageLSN(lsn)(:368)と再利用ページのPageInit(:362)を実行する。他の書き系はページ排他ラッチ前提(transaction_manager.cpp:79-80, 223の契約コメント「RowPage ops run exclusively latched」)であり、この1箇所はその契約の例外になっている。

**発生条件:** 別トランザクションが同一pidをGetPageでラッチしつつpage_lsn_を読む瞬間と重なれば非同期アクセスのデータレース(page_lsn_は非atomic)。tail公開プロトコルにより未公開ページが外部から参照できないことで実質守られているが、契約上の例外として棚卸しに値する。競合相手の到達経路は本レビューでは証明できていない。

**確認・対応記録:**

- AllocateNewPage now acquires PageRef before reinitializing reused content or assigning allocation LSN; placeholder stays unavailable until WAL enqueue and latched publication. Holding reference across committed destruction regression verifies concurrent allocator cannot reset type/LSN before latch release. PageManager, table53, catalog39 pass (batch53).
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### low-83

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `page/page_manager.cpp:853`

**指摘:** ApplyTransactionAbort がアボートした txn の ID で max_committed_txn_id_ を押し上げる(852-856)。チェックポイントヘッダのこの値は docs/page_format.md:25 で "highest committed transaction ID" と文書化されており、ヘッダに書かれる実値はアボート txn を含みうる。機能影響は未確認だが文書と動作が不一致(唯一の消費者 recovery_manager.cpp:500 は SeedNextTransactionId の下限として使うため押し上げ方向は無害)。

**発生条件:** txn T(アボート)が Abort を通ると max_committed_txn_id_=T となり、その後の WriteFuzzyCheckpoint(page_manager.cpp:1057)が T をヘッダに書き出す。リカバリ時は T より大きい番号から txn id を採番するだけで破綻はないが、ドキュメントの意味論と異なる。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-84

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `page/row_page.hpp:39`

**指摘:** page/AGENTS.mdが「up to `kMaxSlotsPerBlock = 256` slots per block」と記すが、実装は4096。

**発生条件:** ブロックロールオーバー条件のレビュー・検証で定数を256と仮定する。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-85

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `plan/AGENTS.md:161`

**指摘:** 「Remaining allowlisted edges」として `plan/implementation_rules.cpp -> executor/detail/scan_filter.hpp` を挙げるが、(a) DEFAULT_ALLOWLIST(scripts/check_layering.py:80-132)にそのエントリは存在せず、(b) そもそも違反ではない(relational=executor/detail はランク7、plan はランク8 で plan→relational は下向き=合法)。文書が存在しない負債として誤記している。

**発生条件:** include-lintの負債台帳をこの記述で信じると、実在しない allowlist エントリを探したり、合法エッジを「V4負債」として返済対象に誤分類する。

**確認・対応記録:**

- plan->executor/detail legal downward edge removed from debt listing.

### low-86

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `plan/AGENTS.md:184`

**指摘:** plan/AGENTS.md が『There are 45 physical registrations』と記述するが、実際の DefaultImplementationRules() の物理登録は46件。docs/optimizer_rule_audit.md 自身は46(57→46)と正しく数えている。AGENTS.md のみ古い。

**発生条件:** 読み手が登録数をAGENTS.mdで確認すると45と誤認する。

**確認・対応記録:**

- medium-18 count46 physical.

### low-87

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `plan/AGENTS.md:184`

**指摘:** plan/AGENTS.md のルール数が陳腐化: :119「83 logical equivalences」に対し RuleSet::Default(plan/cascades.cpp:2266)の実登録は 86 件、:184-185「There are 45 physical registrations」に対し DefaultImplementationRules(plan/implementation_rules.cpp:3527)の実登録は 46 件。docs/optimizer_rule_audit.md も番号が45(最終 #45 batch_nested_loop_anti)で止まり、46個目 batch_nested_loop_outer が未収録。

**発生条件:** ルール棚卸し(D5の「ルールの追加・変更・削除時に...監査」)や audit 文書との差分確認で台帳数を信頼すると、追加済みルール(例: or_to_union 以降の新規ルール群)が監査漏れになる。

**確認・対応記録:**

- medium-18 current counts and regenerated full physical inventory includes batch_nested_loop_outer.

### low-88

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `plan/AGENTS.md:185`

**指摘:** plan/AGENTS.md が「There are 45 physical registrations」と記載するが、コードの DefaultImplementationRules(plan/implementation_rules.cpp:3527)に登録された実装ルールは46件。docs/cascades_optimizer.md:18 は「46 physical」と書いており AGENTS.md の方が古い。

**発生条件:** ドキュメントを読んだ開発者がルール登録数を監査・検証する際、AGENTS.md の 45 を基準にすると実際の46件と突き合わず、追加された1件(例: batch_nested_loop_semi 等のバッチ系)を「余分な登録」と誤認する。

**確認・対応記録:**

- medium-18 count46 physical.

### low-89

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `plan/AGENTS.md:9`

**指摘:** plan/AGENTS.md が PlanBase のインターフェースを「EmitExecutor/ScanSource/GetStats/GetSchema/AccessRowCount/EmitRowCount/IsOrderedBy/OrderIsMeasured/EnforcesLimit/Dump only」と列挙するが、実際の plan.hpp にはこれ以外に ContainsParameterSlots / InstallParameterSlots / EnforcesDistinct / OrderCollation / Explain / SetExplainInfo / ExplainInfo / ToString が宣言されており、「only」の列挙が枯れている。特に ContainsParameterSlots/InstallParameterSlots(コンパイル済みプラン再利用の中央契約)と OrderCollation/EnforcesDistinct(collation契約・D6単一強制点)は同AGENTS.md内の他節が依存する主要契約。

**発生条件:** plan/AGENTS.md のこの行だけを読んで PlanBase を実装/継承する新しいプランノードを書くと、パラメータスロット転送・OrderCollation・EnforcesDistinct のオーバーライド要否を把握できず、既定実装(再利用無効化・順序契約の消失)に沈む実装を見逃す。

**確認・対応記録:**

- PlanBase summary no longer claims exclusive list; includes parameter-slot, distinct, collation, EXPLAIN metadata.

### low-90

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `plan/cascades.cpp:5682`

**指摘:** merge_adjacent_windows のゲートが「両 Window が同一の PARTITION BY と sort 仕様を持つこと」を要求する(コメント5646-5648行)のに、sort 仕様比較(5682-5685行)は常に空ベクトル同士の比較になるため死んでいる。kWindow 式は builder が partition_by/target_list のみ設定し(plan/optimizer.cpp:3501-3505)、sort_ascending/sort_nulls_first は常に空のため `{} != {}` が絶対に成立しない。

**発生条件:** PARTITION BY は同一だが ORDER BY/フレームが異なる呼び出し(RANK() OVER (PARTITION BY p ORDER BY a) と SUM(x) OVER (PARTITION BY p ORDER BY b ROWS ...))を持つ2つの Window ノードが1ノードに融合する。今日は誤結果にならない: executor が呼び出しごとに個別レイアウトを構築するため(executor/detail/window_eval.cpp:1880-1898 の WindowLayoutKey が P:/O: を含む呼び出し単位のキー)。ただしゲートの死んだ比較が将来「ノード単一ソート」実装に変わった瞬間に誤結果化する時限爆弾。

**確認・対応記録:**

- Corrected merge_adjacent_windows contract: identical PARTITION BY and no output dependency; ORDER BY/frame remain per WindowFunctionCallExpression. Normal lowering leaves node sort flags empty; conservative handbuilt-IR mismatch gate retained. No claim of identical per-call ordering.

### low-91

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/cascades.cpp:626`

**指摘:** split_selection_over_join(always-true の relation_enabled、2444-2452行)/push_selection_through_join/PushOrLocalConditions(607-609行)が、メモの結合グラフ外リレーションに対する ContainsRelation プローブなしで EnsureGroup({table}) を呼ぶ。cascades.hpp:303-309 が「ルール本体は RelationMask の CHECK(abort) に頼らず ContainsRelation で先行確認せよ」と規定しており、EnsureGroup→RelationMask(1443-1453行)は未登録リレーションで CHECK_MSG(false) つまり即 abort になる。

**発生条件:** 現在の SQL 経路では到達不能と確認: QueryExpression::TouchedColumns はサブクエリ内部に降りない(expression/query_expression.cpp:202-203、test_ のみ)ため、Selection 述語の TouchedColumns はメモ内リレーションに留まる。しかし InferJoinConstants 側(757-761行)が「outer reference under Apply で結合グラフ外のカラムがあり得る」ことを明言してガードを入れている対比で、本経路にはガードがなく、述語の運搬経路が1つでも変わればプロセスabortになる潜在契約違反。

**確認・対応記録:**

- Handbuilt memo Selection over join referencing outside.x aborted in RelationMask before fix. Shared pushdown helpers now check ContainsRelation before EnsureGroup, preserving out-of-graph predicates as residual.
- Scalar and mixed-table OR variants plus all209 cascades tests pass.

### low-92

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `plan/cascades_search.cpp:1135`

**指摘:** docs/cascades_optimizer.md:266-270 が組み込み上限を「kMaxExploreSteps (20000 rule applications)」と説明するが、コードの kMaxExploreSteps=20000 はワークリストのポップ数(グループ再訪)を数えており、ルール適用は別定数 kMaxExploreRuleSteps=50000(cascades_search.cpp:913, 1197行)で上限管理されている。予算の意味論がドキュメントと逆対応。

**発生条件:** TINYLAMB_RULE_STATS の出力でも分かる通り、steps_(ルール適用数)と pops(steps、1131-1141行)は別々に進行する。20000ポップに到達した時点でキューに残があれば explore_limit_reached_(1153-1154行)。文書だけ読むとルール適用2万で止まる想定になるが、実際は2万グループ訪問/5万ルール適用まで進む。

**確認・対応記録:**

- Documented separate20000 worklist-pop and50000 rule-application built-in caps, ExploreLimitReached vs explicit search_step_budget/BudgetExhausted; removed duplicated contradictory budget prose.

### low-93

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/implementation_rules.cpp:1338`

**指摘:** 範囲インデックススキャン候補で、グループフィルタがスキャン本体(scan_predicate を BuildIndexScan に渡す)と !covered 時の上位 SelectionPlan の二重で適用される。直前のコメント(1326-1329)は『The scan filter is applied either by the scan itself or ... by a Selection on top』と排他を述べており、コードはその契約に反して両方積む。残差述語が生存行ごとに2回評価される性能上の無駄(結果は正しい)。

**発生条件:** `WHERE indexed_col > 100 AND other_col = 1`(等値スロットでない述語が範囲スロットに触れる)のインデックスレンジスキャンで、IndexScan実行器がフィルタを評価した後、さらに SelectionPlan が同一フィルタを再評価する。

**確認・対応記録:**

- Kept SelectionPlan logical selectivity/EXPLAIN, but factory skips its evaluation when direct IndexScan/IndexOnlyScan shares the same predicate identity; parameter traversal avoids duplicate slots. Optimizer 134, plan 61, plan_extra 12, SQLtemplate 41 suites pass in batch51; bound replay regression verifies changed seek and residual values. Also restored safe numeric/date common-OR join inference using bound column domains.
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### low-94

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/implementation_rules.cpp:4567`

**指摘:** limit 規則の SortPlan+LIMIT→TopN 畳みで TopNKey を collation 指定なしで組む(既定 Collation::kNone)。topn 規則自身は `.collation = CollationOfExpression(...)` を設定する(4207-4217)のにこの畳みだけ欠落しており、plan/AGENTS.md:30-38 の『SortKey/TopNKey は式が解決する collation を運ぶ』契約に反する。非バイナリ collation のソートキー(例: collate(x,'und:ci'))を持つ Sort をこの経路で TopN に畳むとバイナリ比較でトップN行を選び、エンジンのソート再適用もスキップされる(sql_engine.cpp:6311-6313 は collation を見ない)ため誤結果になり得る。

**発生条件:** SQL正面からは ORDER BY+LIMIT は kTopN 直構築(optimizer.cpp:3553-3572)のため到達しにくい。到達経路は memo 探索で kLimit の下に SortPlan が最善子として現れる形状(rule_fuzzer の物理規則サブセット掃引や limit 系論理規則の派生形状)。その形状で ci-collation キーを畳むと行メンバーシップが変わる。

**確認・対応記録:**

- Sort+LIMIT to TopN copies SortKey collation, including an explicit collation not embedded in expression. Regression returned B before a and reported binary order before fix; preserves CI order after; sort-shape and collation-property suites pass batch40.

### low-95

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `plan/implementation_rules.cpp:97`

**指摘:** envハッチの値意味論がdocsと不一致。コードは `std::getenv("TINYLAMB_SCAN_PROJECTION") != nullptr` で『設定されていれば任意の値でON』だが、docs/environment_variables.md:86 は『`=1` enables』と値を条件に書いている。`TINYLAMB_SCAN_PROJECTION=0`(やfalse)を設定すると無効化どころか有効化される。同一パターンが TINYLAMB_JOIN_CARD_DEBUG(implementation_rules.cpp:104-108)と TINYLAMB_PLAN_PROFILE(optimizer.cpp:3237) vs docs/environment_variables.md:104 にもある。未設定時の既定OFF自体はコードコメント(implementation_rules.cpp:89-90『Default OFF』)およびdocsと一致する。

**発生条件:** 『TINYLAMB_SCAN_PROJECTION=0 で確実に切る』という運用・診断手順が逆に常時ONにしてTPC-H Q19由来の既知の10x遅化経路(plan/implementation_rules.cpp:89-94,1595-1597のコメント)を踏む。

**確認・対応記録:**

- Environment reference states TINYLAMB_SCAN_PROJECTION is enabled by any set value, including0/empty, matching presence-based implementation.

### low-96

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/index_scan_plan.cpp:79`

**指摘:** OrderMatches の補助関数 AffineColumn が `*multiplier *= constant;` を int64 で行い、 crafted ORDER BY 式で符号付きオーバーフロー(UB)になり得る。multiplier は方向判定(符号/ゼロ)にしか使わないため値の意味は失わないが、オーバーフロー自体が UB で最悪場合誤った方向判定(ソート省略の誤判定)につながる。

**発生条件:** インデックス付き列 c1 に対し `SELECT ... FROM t WHERE c1 = 5 ORDER BY c1 * 9223372036854775807 * 2` のような式を ORDER BY に書く。IsOrderedBy → OrderMatches(index_scan_plan.cpp:116)→ AffineColumn がネストした乗算で multiplier に INT64_MAX を掛け、オーバーフロー(UB)。

**確認・対応記録:**

- Affine ordering proof uses checked multiplication and declines proof on overflow. Nested INT64_MAX multiplication incorrectly claimed order before; plan61 pass after (batch27).

### low-97

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/minmax_index_plan.cpp:61`

**指摘:** MinMaxIndexPlan::ToString() が `aggregate_.expression->AsAggregateExpression()` を無条件に呼ぶ。コンストラクタ(27-31行)は `!aggregate_.expression` や非集計式を「best-effort で既定 kInt64 を使う」と明示して許容しているのに、ToString/Dump はその状態で NULL Expression の持演算子呼び出し(NULLデレフェレス)または誤型キャストになりクラッシュ/UB。

**発生条件:** コンストラクタが許容する状態(集計式が null、または kAggregateExp でない NamedExpression)で MinMaxIndexPlan を構築し、EXPLAIN やロギングで Dump()/ToString() を呼ぶと NULLデレフェレスで落ちる。現行の唯一の生成経路 OptimizeMinMaxIndex(implementation_rules.cpp:3518-3520)は集計式を渡すため通常到達しないが、コンストラクタ契約と ToString の前提が矛盾している。

**確認・対応記録:**

- Empty SetOperationPlan statistics and malformed MinMaxIndexPlan descriptions crashed before fixes. plan_extra_test 11 pass (batch17).

### low-98

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `plan/plan_memo_oracle.hpp:80`

**指摘:** plan_memo_oracle.hpp:80-81 と plan_memo_oracle.cpp:632 が『all 105 transformation rules』と記述するが、cascades::RuleSet::Default() の論理登録実数は86件(83でも105でもない)。コメントの数字が実態と乖離している。

**発生条件:** オラクルの読み手/保守者が『105ルール全てが演習される』と信じ、登録数変更時にコメントを更新しない。

**確認・対応記録:**

- Removed stale105-rule count and unsupported claim that generator actively exercises every transformation; comment now describes applicable default rule exploration.

### low-99

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `plan/set_operation_plan.hpp:44`

**指摘:** SetOperationPlan::GetStats() だけが空 children をガードせず `children_.front()` を呼ぶ。同一クラスの GenerateSchema(set_operation_plan.cpp:44-46)、EmitRowCount(:74-76)、AccessRowCount(:66-71)はいずれも `children_.empty()` を明示ガードしており、公開コンストラクタで空 children を渡すと GetStats 参照時に UB(vector の範囲外アクセス、type/schema.hpp:52-54 の GetColumn 同様チェックなし)。

**発生条件:** 公開コンストラクタ `SetOperationPlan(std::vector<Plan>{}, kind)` で空 children を渡す(ルール経由では implementation_rules.cpp:4450 の `children.size() < 2` ガードで防がれるが、クラス契約としては構築可能)。構築したインスタンスに対しコスタリングや EXPLAIN が GetStats() を呼ぶと front() が空vector で UB。

**確認・対応記録:**

- Empty SetOperationPlan statistics and malformed MinMaxIndexPlan descriptions crashed before fixes. plan_extra_test 11 pass (batch17).

### low-100

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `query/AGENTS.md:10`

**指摘:** query/AGENTS.md の本文が字句レベルで破損している(バッククォート・空白が不自然な位置で改行された中身: `sql_engine.{ hpp, cpp }` が3行に分断、"unifies streaming.Routes set - ops" の様なトークン内分割が :10-21, :44-60, :95-190 等に多数)。他層のAGENTS.mdは正常な形式であり、SQLフロントエンド層の規範文書として可読性・機械参照性が損なわれている。

**発生条件:** 「Before touching code in a layer, read that directory's AGENTS.md first」(ルートAGENTS.md:8-9)に従って query/ 層を読む際、破損した記述(例: EXPLAIN の Prepare-level prefix 契約:41-43、FOR UPDATE 契約)の解釈コストが増大し、文言引用やgrepによる契約確認が失敗する。

**確認・対応記録:**

- Corrected retired TSAN justification (deadlock checking still explicitly disabled, not claimed covered), Value owned/borrowed payload model, nested-loop error propagation and malformed query guide formatting; source reviewed (batches20-27).

### low-101

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor.cpp:867`

**指摘:** DecodeStringEscapesImpl の \uXXXX / \UXXXXXXXX 解析(867-920)がサロゲートペア/範囲外コードポイントを検証せず、単独サロゲート(\uD800)を3バイト UTF-8 に、\U00110000 のような Unicode 範囲外の値を4バイト UTF-8 に静的にエンコードする。GoogleSQL はこれらをエラーにするため、STRING 値として不正 UTF-8 がエンジン内に混入し、比較/出力経路で顕在化しうる。

**発生条件:** SELECT '\uD83D\uDE00' ではなく SELECT '\uD800'; — 単独サロゲートが ED A0 80 の不正 UTF-8 バイト列として STRING 定数になり、エラーにならない。\U00110000 も F4 90 80 80 で通る。

**確認・対応記録:**

- Handbuilt AST scalar escapes D800,DFFF,00110000,FFFFFFFF,partial/nonhex all reproduced accepted invalid strings. Unicode decoder now returns StatusOr, validates exact hex length and Unicode scalar range; byte literals reject Unicode escapes. Template extraction declines invalid decode.
- GoogleSQL AST56, SQL audit24, SQL template41, merge16 pass. Template visitor use same status decoder; no parser bypass.

### low-102

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_ast_visitor_dml.cpp:1000`

**指摘:** ExpandUnpivotSource が IN 項目を1つも構築できない(UnpivotInItem 子なし、または ExpressionList 欠落で continue)とき、root_branch が null のまま result.query に移され、table も空の SelectSource(query==nullptr かつ table=="")が下流に渡る。どちらの形式も満たさない source を無検査で発行する unguarded 経路で、下流のエラーメッセージ/クラッシュ挙動が未定義のまま。

**発生条件:** UNPIVOT の IN リストが空 (IN ()) としてパースが通るダンプ形状が来ると、946-998 のループが1回も回らず root_branch は null → 1000-1003 で query=null の source を返す。下流は空表名の source 経路に落ちる(エラーになることを除き本ビジタでは検証なし)。

**確認・対応記録:**

- Mutated parsed UNPIVOT AST empty IN list and missing per-item ExpressionList both reproduced successful malformed source. Visitor now rejects both explicitly. GoogleSQL AST56 pass.

### low-103

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `query/googlesql_ast_visitor_dml.cpp:1273`

**指摘:** NOT NULL の処理を説明するコメントは「FOREIGN KEY / DEFAULT: the key/default keeps the slot ... the NOT NULL half stays unenforced」(DEFAULT 列はスロット保持で NOT NULL 半分は未強制になると記述)と説明し、query/AGENTS.md も DEFAULT が拒否する組み合わせを FOREIGN KEY と PRIMARY KEY のみと列挙する。実際のコードは NOT NULL を先に適用し(1277-1284)、その後 DEFAULT が空スロット前提で検査するため(1304-1309)、DEFAULT + NOT NULL は「cannot combine DEFAULT with its existing constraint」で丸ごと拒否される。GoogleSQL では正当な DEFAULT NOT NULL DDL が拒絶され、隣接コメントと AGENTS.md の両方と不一致。

**発生条件:** CREATE TABLE t (c INT64 NOT NULL DEFAULT 5); → エラーで失敗。コメントの説明どおりなら c のスロットは kDefault になり NOT NULL だけ未強制で受け付けられるはず。FOREIGN KEY 側は 1353-1354 で kNotNull との組み合わせが実際に許されており(FK がスロットを奪い NOT NULL は無効)、DEFAULT だけ挙動が違う。

**確認・対応記録:**

- Corrected NOT NULL comment: PRIMARY KEY implies non-NULL; DEFAULT/FOREIGN KEY conflicting slot rejected by later processing rather than silently ignored.

### low-104

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_compliance_file.cpp:416`

**指摘:** ParseExpectedRows の bare スカラー走査が '[' で depth を増やす一方、']' を depth に関係なくトークン終端として扱う。ネストした配列を含む素のゴールデン(関数自身が例示する ARRAY<INT64>[10, 20]、実コーパスの query/testdata/googlesql_compliance/array_path.test:525 の ARRAY<STRUCT<col ARRAY<>>>[{ARRAY<STRING>["B"]}] 等)は内側の ']' で打ち切られ、ParseArrayToken が拒否するため一致が永遠に成立しない(コンプライアンスの偽失敗)。STRUCT フィールド経路(ParseStructRow)は bracket_depth を正しく扱っており、素スカラー経路だけ壊れている。

**発生条件:** 1列結果が ARRAY 型で素スカラー形式(例: [ARRAY<INT64>[10, 20]])のゴールデンを書いた .test は、期待トークンが "ARRAY<INT64>[10, 20" に切り詰められ、ComplianceValueMatches の ParseArrayToken が rest の末尾が ']' でないとして false を返す → エンジンが正しくても常時失敗。

**確認・対応記録:**

- Nested bare ARRAY golden previously stopped at first inner closing bracket, losing following expected rows. Scanner now balances inner brackets and checks bounds after order annotation. Nested/non-nested/empty/truncated-annotation regression added. Compliance parser7 tests pass.

### low-105

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/googlesql_frontend.cpp:1491`

**指摘:** DISTINCT ON 前処理が AST ダンプのテキスト・パターン(「Select(", "Select ["等, 1484-1490)に依存し、パターン不一致や Select 行の改行欠落(1541-1545)のとき DistinctOn 注入を静かにスキップしたまま平文 DISTINCT の解析結果を ok で返す。SELECT DISTINCT ON (k) が SELECT DISTINCT として実行される暗黙の意味変更であり、同一ファイル 936-939 の自規約「書換は音を立てて失敗し、決して異なる意味に書き換えない」に違反するエラー飲み込み経路。

**発生条件:** ピン留めパーサのダンプ形式が Select ノードの綴り/インデントを変えたとき、SELECT DISTINCT ON (a, b) ... が全入力で黙って DISTINCT にダウングレードされる(現在のダンプ形式では発火しない潜在経路)。WithTies 注入(1556-1571)も同型の limit_pos 不一致時サイレント degrade を持つ。

**確認・対応記録:**

- Extracted DISTINCT ON AST injection into testable frontend helper; refuses missing Select/newline/key children and preserves original input on failure. Frontend converts injection failure to parse error rather than ordinary DISTINCT. Full frontend test suite passes (batch56).

### low-106

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_engine.cpp:189`

**指摘:** EnsureViewsRegisteredが検証成功前にviews_registered_epoch_へepochを先書きし、以後の失敗を全て握りつぶすため、一時的な失敗時に永続化VIEWがこのエンジンインスタンスで二度と登録されない

**発生条件:** ListViewDefinitionsがエラーを返す(190-194行でreturn)、または個別VIEWのparse/AST失敗(200-213行でcontinue)が一度でも起きると、epochが変わるDDLが飛ぶまで同インスタンス上の全PrepareでVIEW展開が行われず、カタログに存在するVIEWへの参照が「not found」になる

**確認・対応記録:**

- Persisted invalid view followed by corrected definition at unchanged CatalogEpoch reproduced permanent missing view. Registration now publishes epoch only when every definition loaded and parsed successfully; failures retry on next Prepare. SQL audit26 tests pass.

### low-107

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_engine.cpp:2070`

**指摘:** DMLサブクエリ畳み込みが適用されたfingerprintはDemoteVolatileFingerprint(was_fresh=false)で永久にvolatile setへ残り、そのthread_local集合(plan_cache.hpp:349-350)には容量上限がない。他のキャッシュ(thread_compiled_plans 64/256、local_templates 1024)はクリアされるのに、この集合だけ無制限に成長する

**発生条件:** `UPDATE t SET ... WHERE x IN (SELECT ...)` のようにPrepare時に畳み込みが走るDMLを、リテラル違いで多数発行する長時間稼働ワーカーで、distinct fingerprint数分の文字列がスレッド毎に解放されず蓄積し続ける

**確認・対応記録:**

- Volatile fingerprint set capped at 1024 per thread/session. Saturation uses a sticky constant-space disable-reuse flag instead of evicting marks that might revive stale global plans. Session isolation and demotion/saturation regression pass in plan_cache_unit_test batch60.
- Batch66 ASan/UBSan plan_cache_unit suite passes saturation/demotion/session isolation tests.

### low-108

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `query/sql_engine.cpp:219`

**指摘:** TINYLAMB_FUZZ_TRACE_SQLをコードが読むが、docs/environment_variables.mdに記載がない。同doc冒頭(3行目)は「Every TINYLAMB_* environment variable read by the code, in one place.」と網羅を謳っている

**発生条件:** fuzz再現時にSQLトレースを有効化しようとdocを引き当てても変数が見つからない。両ファイルのTINYLAMB_*をgrep比較し、コード側のみに存在することを確認済み

**確認・対応記録:**

- medium-9 FUZZ_TRACE_SQL inventory.

### low-109

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `query/sql_engine.cpp:3777`

**指摘:** TINYLAMB_GROUPED_ROUTEの文档極性が逆。docs/environment_variables.md:101は「`=relational` prefers the hand-rolled relational route ... the default keeps the Cascades-first order」と書くが、コードは未設定時がrelationalルートで、`=cascades`のときだけCascades優先に戻る

**発生条件:** ベンチ/AB比較で「デフォルト=Cascades」を前提に `TINYLAMB_GROUPED_ROUTE=relational` を設定しても動作はデフォルトと同じで、測定結論が反対になる。cascades優先にしたければ `=cascades` を設定する必要がある

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-110

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_engine.cpp:907`

**指摘:** NarrowIntegerFitsがカラム名の部分文字列(int8/int16/int32/uint*/int64は対象外)で窄幅整数を推定するため、偶然名前にこれらを含む普通のカラム(例: point32, joint8count)が誤った範囲検査/強制CASTを受け、正当なINSERTが範囲エラーで拒否される

**発生条件:** カラム名 point32 のテーブルへ `INSERT INTO geo VALUES (100000)` → "assignment out of range for column point32" で拒否(5052-5067行)。同カラムへのUPDATEは6776-6793行で式がUINT32/INT32等へのCastExpressionで包まれる

**確認・対応記録:**

- Declared 8/16/32/64-bit integer metadata now survives schema encoding and constraint reconstruction. INSERT and table UPDATE validate declared bounds, never column-name substrings. Column, schema, table, SQL audit, AST and expression matrix suites pass (batch59).
- Full-suite follow-up found CTAS lost declared INT32/UINT32 widths; projection/CTAS now preserve declared domains. CtasPreservesDeclaredIntegerAndIntervalDomains and dml_insert compliance pass batch67.

### low-111

**判定:** 既存の方言差を明記（実装未変更） (`documented-compatibility-limitation`)

**元の対象:** `query/sql_expression_matrix_test.cpp:120`

**指摘:** INT64/FLOAT64 と BOOL の混在比較・算術を bool→1/0 の暗黙強制つきで成功値としてピン留め(SELECT 1 = true → "1" (120), 1.5 = true → "0" (124), true < 1.5 → "1" (195), CAST(1 AS BOOL) → "1" (424) 等)。GoogleSQL は INT64 = BOOL にマッチするシグネチャがなく型エラーとする。同一テストが 1 = 'a' (685) をエラーにピン留めしており、「文字列とは比較不可だが BOOL とは暗黙比較可」という GoogleSQL に反する非対称が、逸脱である旨の注記なしに正の期待値になっている。DeviationSweep (819-831) は 1 + true 群を「behavior under review」と未ピンにしているのに、比較側は確定値として凍結しており、何が確定挙動かの基準もファイル内で一貫しない。

**発生条件:** SELECT 1 = true → 1 が期待値。GoogleSQL 準拠を目指す修正(NO MATCHING SIGNATURE 相当のエラー化)を入れるとこの行列全体が失敗する。

**確認・対応記録:**

- BOOL remains represented as INT64; mixed numeric comparisons are a known dialect deviation, not GoogleSQL conformance. Matrix and arithmetic sweep explicitly document this; CAST(INT64 AS BOOL) is distinct and valid. Strict semantic BOOL typing is not implemented by this audit. Matrix suite passes batch63.

### low-112

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_expression_matrix_test.cpp:501`

**指摘:** TIMESTAMP_TRUNC(TIMESTAMP '2026-05-04 03:04:05+00', DAY) → "2026-05-03 07:00:00+00" というピンは、タイムゾーン未指定時のデフォルトが America/Los_Angeles であることを暗黙に固定している。GoogleSQL/BigQuery のデフォルトタイムゾーンは UTC であり、同一文は "2026-05-04 00:00:00+00" を返すべき。エンジンのデフォルトゾーンは type/date.cpp:190 で LA にハードコードされており、GoogleSQL 意味論との乖離が期待値側に埋め込まれている。

**発生条件:** SELECT TIMESTAMP_TRUNC(TIMESTAMP '2026-05-04 03:04:05+00', DAY) → 05-03 07:00+00 (PDT 深夜0時基準)。UTC 既定の GoogleSQL では 05-04 00:00+00。DAY/WEEK 系の未指定ゾーン演算すべてが 7 時間ずれた日付境界を正として固定される。

**確認・対応記録:**

- Session default and empty reset now use UTC. Explicit Los Angeles remains available. UTC TIMESTAMP_TRUNC and default reset regressions pass; matrix expected UTC updated. Batches57/59.
- UTC default changes legacy formatting/date pins; explicit -08:00 input retains its UTC conversion.

### low-113

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/sql_statement_sweep_test.cpp:3`

**指摘:** ファイル先頭コメント(3-6)は「DML including the struct-INSERT expansion」をスイープ対象と謳うが、ErrorTable (3271-3274) は struct を含む INSERT 3 形態をすべて必ず raise にピンし、EXPECT_THROW にメッセージ検査がない(3288-3289)ため、原因が何であれ通る。struct UPDATE のみが実行されるが空テストーブルで 0 行(sql_engine.cpp の ExpandStructInsertValue (972) と 5035-5040 の展開経路は実データで一度も成功経路を通らない)。特に INSERT INTO st_t VALUES (2, NULL) は私が読んだ実行時型検査では NULL を通す (sql_engine.cpp:5075 row[i].IsNull() → continue) ため、読みだけでは拒否理由を特定できず、ピンが無関係な失敗を「期待エラー」として祝福している可能性を排除できない。

**発生条件:** ErrorTable 実行時、INSERT INTO st_t VALUES (1, STRUCT(10 AS x, 'hello' AS y)) / VALUES (2, NULL) / (id, s) VALUES (3, STRUCT(...)) がすべて EXPECT_THROW で合格。どのエラー文面でもよいため、外部パーサの一時障害由来のエラーでも合格し得る。

**確認・対応記録:**

- Added successful whole-row STRUCT/NULL expansion, stored STRUCT cells and nonempty dotted UPDATE regression. Existing failing st_t INSERTs now assert the actual cause: empty CTAS inferred id as STRING, so INSERT type mismatch for column id. Removed blanket EXPECT_THROW entries and corrected sweep coverage documentation; SQL audit and statement sweep pass batches63/64.

### low-114

**判定:** 既存の方言差を明記（実装未変更） (`documented-compatibility-limitation`)

**元の対象:** `query/sql_statement_sweep_test.cpp:3245`

**指摘:** 疎 GROUP BY(GoogleSQL では SELECT/HAVING に非グループ化・非集約列を置くとエラー)を、代表行の値つきで平然たる成功ピンにしている: SELECT a, b FROM t GROUP BY a → 5行 (3245)、SELECT a FROM t GROUP BY b HAVING a > 1 → 8行 (3243)。ピン値自体は PinnedDumpTable 途中の UPDATE (3232) 後のフィクスチャに対し各グループの先頭行 representative として数値的には整合するが、非キー列にどの行の値を採るかは経路依存で、リレーショナル経路の設計(代表は ungrouped_slots の canonical minimum, NULLS LAST — executor/relational.cpp:613-618)とは食い違う値(a=4 グループで先頭行の NULL がピンされ、canonical min の 4.0 ではない)が答えとして凍結されている。逸脱である旨の注記がない。

**発生条件:** SELECT a, b FROM t GROUP BY a (UPDATE 後の t) → "5 rows | [2, 1.5] | [4, NULL] | ..."。GoogleSQL は "Column name b ... is not grouped" 相当のエラー。代表選択は実行経路ごとに異なり得るため、どちらか一方の代表選択が SQL の正として固定される。

**確認・対応記録:**

- Sparse GROUP BY remains a permissive engine dialect behavior, not GoogleSQL conformance; representative-row values are not promised across plans. The characterization sweep now states that limitation explicitly instead of implying these pins define valid GoogleSQL. Strict grouped-column semantic validation is not implemented in this audit.

### low-115

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `query/statement.hpp:839`

**指摘:** SelectStatement::Fingerprint()のメモ化が死んでいる。呼び出しごとに全木を再帰再シリアライズしてmutableなcached_fingerprint_(927行)に書くが、早期リターンの読み出しがなく参照されない。加えて728行(集約/ウィンドウ節点の生ポインタ)と838行(set_operation_tree_の生ポインタ)がfingerprintに混入する

**発生条件:** subquery_runtimeの非相関サブクエリCSE(2899-2901行「Use a structural key ... separately allocated but identical」が目的)がUncorrelatedCacheKey(2752-2756行)でこのFingerprintを使うため、集約やセット演算を含む同一サブクエリが別々に確保されると鍵が一致せずCSEが効かない。また共有Statementを複数スレッドからfingerprintすると非アトミックなstd::stringへの並行書き込みになる

**確認・対応記録:**

- Fingerprint recomputation is required for shared child mutation; renamed cached_fingerprint_ to fingerprint_buffer_ and documented process-local identity components and returned-reference lifetime. Three fingerprint tests pass incl mutation of shared subquery without touching parent (batch51). This does not claim structural CSE across separately allocated opaque aggregates or concurrent calls on same mutable statement.
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.
- Fingerprint recomputes shared-child mutations and returns an owned string, removing mutable buffer writes from const readers. Pointer-based opaque identity is documented as process-local. Three fingerprint regressions pass batch59; earlier recomputation regressions also passed ASAN batch55.
- All Fingerprint call sites rechecked batch66: no stored string_view to the newly returned owned string.

### low-116

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/README.md:12`

**指摘:** recovery/README.md:11-13はCheckpointManagerが「buffer-poolエントリをピン留めしRecoveryLSNからDPTを導出し、CHECKPOINT_BEGIN/CHECKPOINT_ENDレコードをWALに書き、dirty pageを強制フラッシュしない」と記述するが、実装はWALにチェックポイントレコードを一切書かず(checkpoint_manager.cpp:102-138、kBeginCheckpoint/kEndCheckpointはどこからも発行されない。checkpoint_manager_test.cpp:180-184が『チェックポイント前後でBufferedLSN不変・サイドカー不存在』を検証)、バッファプールピン留めもdirty pageフラッシュもない。またdocs/recovery_invariants.md:136-173「Testing」節が列挙する約20個のテスト名のうち実在するのはCommittedRowsSurviveCrashDuringCheckpointStormのみで、recovery_extra_test一式・torn tail分類(HalfWrittenRecordAutoTruncatesWithoutForce等)・アロケーションundo・WAL CRC(MidRecordBitFlipStopsScan等)・CheckpointTest.WorkerStaysParkedUntilStart等は木に存在しない(全名をgrepして確認、1件のみヒット)。

**発生条件:** READMEの記述を信じてkBeginCheckpoint/kEndCheckpointレコードのパース・実装に手を入れたり、doc列挙テストの存在を前提にリグレッション範囲を判断すると誤る。列挙されたテスト群が存在しないため、docに記載の回帰保証(torn tail分類やWAL CRCなど)は現状どこでも検証されていない。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-117

**判定:** 現行保存形式の制約を明記（互換性の機能追加なし） (`documented-format-limitation`)

**元の対象:** `recovery/log_record.cpp:231`

**指摘:** WALレコード直列化がエンディアン混在。magic/version/total長/文字列長プレフィックス/CRCはホストエンディアンの生memcpy(log_record.cpp:36-38, 42-50, 225-232)、prev_lsn/owner/pid/slotはserdesのBSWAPによるビッグエンディアン(serdes.hpp:61-95, 104-105)。

**発生条件:** ビッグエンディアン環境で書いたWAL/.dbをリトルエンディアン環境で(またはその逆に)リカバリすると、magic/version/total長の解釈が崩れ全レコードが破損扱いになる。サポートOS(macOS/Windows/Linux)が現状LEのみのため顕在化しない。wal_format.md:134の「All fixed-width fields use big-endian order」もこのため不正確。

**確認・対応記録:**

- Confirmed v7 mixes native-endian header/string lengths/CRC with big-endian IDs. Documented same-endian persistence restriction and required versioned migration for portability; no supported cross-endian runtime is being claimed. No silent wire reinterpretation introduced.

### low-118

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `recovery/logger.cpp:1067`

**指摘:** legacy(非fixed)セグメントのRollToLockedは、短い現セグメントを次セグメント生成(:1110 CreateFile)より先にsegment_size満杯までゼロ拡張する(:1066-1074)。この2処理の間でクラッシュすると「最新セグメント=満杯・末尾は未使用ゼロ・後続セグメント未作成」という無害な状態で再開するが、再開時ValidLogEndStatusはPreRollPaddingEndでパディングを検証した後SegmentFdFor(boundary)がnullになりmissing_successorを立ててkIOErrorで起動失敗する(recovery_manager.cpp:290-295, 374-385)。仮に到達してもClassifyTailDamageは on_disk >= segment_end のゼロ連続をkSuspect(自動切断対象外)と判定する(:433-440)。wal_format.md:114-118の「kZeroTailは常に自動切断」記述と矛盾し、--force後の再起動+ロールという狭い窓だが、ベンイング状態で無人再起動が楔止まりし、エラーメッセージも実態(未生成)と異なる「missing WAL segment」と表示される。

**発生条件:** (1) --forceでWAL中途までTruncateTo→再起動→(2) 大きいレコードでprerollが発生しRollToLockedが短い末尾セグメントを拡張→(3) 次セグメント生成前にクラッシュ→(4) 再起動がRecoverFromでkIOError 'missing WAL segment after pre-roll padding' となり開けない。

**確認・対応記録:**

- Legacy final zero padding up to the segment boundary is recoverable after a roll crash before successor creation. Actual middle holes still fail and nonzero damage remains corrupt. Logger segment and recovery suites pass batch60; WAL format updated.
- Batch66 ASan/UBSan logger_segment and recovery_manager suites pass legacy pre-roll zero-tail classification test.

### low-119

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/logger.cpp:1473`

**指摘:** docs/wal_format.md:13-16は「レコードはセグメント境界を跨いでもよい(バイトが2ファイルに分割され、writerはwrite()を境界で切り詰めてロールする)」と記述する。実際のwriterは境界跨ぎを書かず、recordが残り領域に収まらない場合は旧セグメント末尾をゼロ埋めして次セグメント先頭にprerollする(logger.cpp:1473-1506)。境界跨ぎハンドリング(ReadAtDetailedの継続読み、RecycleUpToのboundary validator)は移行済みlegacyログ用に残存するが、新規WALではゼロパッド+PreRollPaddingEnd経由のスキップ(recovery_manager.cpp:245-297)が正経路で、writer動作の記述として不一致。logger_segment_test.cpp:290 RecordsNeverStraddleAndRecycleKeepsMovingがこの挙動を固定している。

**発生条件:** wal_format.mdの記述を前提にしたWALツール(境界跨ぎレコードの連結読みだけを実装しゼロパッドスキップを持たないスキャナ)が、新形式WALでパディング以降を誤って破損と判定する。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-120

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `recovery/logger_fuzzer.hpp:65`

**指摘:** loggerファズハーネスがAddLogのStatusOr戻り値を無視し(:58)、その後 `while (logger->CommittedLSN() < total) {}` で無限ビジーウェイトする。AddLogが1度でも失敗(Logger::Failed()==true。ENOSPC/EDQUOT/EIOやSetFailed後の全AddLog拒否、logger.cpp:1448, 1453-1464)するとCommittedLSNはtotalに届かずループが永久に回り、ファズ実行がハングする。検証で刺さったとき失敗原因(どのレコードで失敗したか)も表示されない。

**発生条件:** ディスク残量が少ない環境(AGENTS.mdが想定するdisk-quotaガードが必要な長時間ファズスイープ)でENOSPCが起きると、libFuzzerプロセスがタイムアウトまで回り続け、真の発見が requester 側から見えなくなる。

**確認・対応記録:**

- Logger fuzzer now checks Create/AddLog and WaitForDurable instead of busy polling CommittedLSN; acknowledged LSN determines barrier. Per-input owned directory removes sidecars on all exits. LoggerTest full suite passes incl stopped logger rejection and payload durability (batch52).
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### low-121

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `recovery/recovery_manager.cpp:517`

**指摘:** docs/recovery_invariants.md:8-15とrecovery/README.md:8は「実行時アボートはCLRで連鎖を補償した後、リカバリがコミット判定に使うのと同じ(LogType::kCommit)終端レコードを追記する」「kAbortレコードは存在しない」「各CLRは元レコードのprev_lsnをundo-nextに持つ」と記述する。実際はTransactionManager::AbortがLogType::kAbortを追記し(transaction/transaction_manager.cpp:702 AbortLogRecord)、リカバリはkCommitオーナーのみをcommitted集合に入れkAbortトランザクションは単に無視する。コミットフィルタ方式では結果は等価だが、ドキュメントが記述する終端レコード契約とCLR再開機構は実在しない。

**発生条件:** ドキュメント記述に基づき「abort終端はkCommit互換である」ことを前提にしたリカバリ解析・ツール(WALスキャナ等)を書くと、kAbortレコードを見て誤動作する。recovery_invariants.md §1のクラッシュ中アボート連鎖の記述(CLR再開・UndoLoserChainsのpage_lsnガード)も実装に対応しない。

**確認・対応記録:**

- Rewrote recovery/WAL specifications from current source: v7 actual wire layout, committed-state snapshots and 2-pass commit-filtered replay, staged in-memory abort plus kAbort, actual anchor/recycle policy and tail handling. Removed nonexistent DPT/ATT/FPI/CLR/parallel-replay guarantees. See /tmp/patch-audit-recovery-docs.py.

### low-122

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `scripts/check_layering.py:149`

**指摘:** `--root DIR` オプション(ARCHITECTURE.md:107「--root DIR: リポジトリルート指定」、scripts/AGENTS.md も同記載)が走査対象を切り替えない。source_files() は常にスクリプト位置由来の ROOT(scripts/check_layering.py:47)を rglob し、--root は :188 の読み込みパスにのみ使われる。別ツリーを指定すると、走査リストはスクリプト側リポジトリのまま別ルートから内容を読むため、指定ルートにしかないファイルは検査されず、逆にこちらにしかないファイルは読み込み例外になる。

**発生条件:** CIやローカルで別チェックアウトに対して `python3 scripts/check_layering.py --root /other/checkout` を実行すると、/other/checkout 固有の新規ソースが検査対象から漏れ、違反を見逃す(またはファイル不在で例外終了する)。

**確認・対応記録:**

- source_files uses supplied --root for inventory and relative paths. Temporary independent tree regression failedbefore/passesafter; realrepo lintpass.
- source_files now inventories the root argument rather than global ROOT. Temporary-root test failed before/passes after, real layering lint also passes (78 accepted edges).

### low-123

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `server/AGENTS.md:3`

**指摘:** server/AGENTS.md 冒頭の「Linux-only runtime (epoll/eventfd/accept4) — tinylamb_server binary is not built on macOS」が現在のコードと不一致。CMake は tinylamb_server を OS 条件なしでビルドし、server 層は epoll/eventfd を直接参照せず platform:: 抽象化(epoll/kqueue/Windows)のみを使う。

**発生条件:** macOS で cmake -S . -B build && cmake --build build を実行すると tinylamb_server が生成される(ターゲットに OS ガードがないため)。server/postgres_server.cpp 内の epoll/eventfd への言及はコメントのみで、実参照は common/platform/event_poller.cpp の実装に集約されており、そちらは kqueue(:396 付近)と Windows(:37, :193)の分岐を持つ。直近のコミット「MacOS ready」「support windows」「Add common/platform OS abstraction」とも整合する。

**確認・対応記録:**

- Cross-checked current implementations; corrected retired classes, actual APIs, null/NaN/order/error behavior, lock names, platform support, and storage limits. Exact changes recorded by /tmp/patch-audit-docs3.py and /tmp/audit-batch13 production changes.

### low-124

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `server/postgres_server.cpp:1066`

**指摘:** 起動時に ParameterStatus で standard_conforming_strings=on(バックスラッシュは通常文字)をクライアントに宣言しながら、実際の文分割(SplitSqlStatements)とフロントエンドは '\''/'\"' をエスケープ対として解釈する。ワイヤで広告した文字列意味論と実動作が乖離しており、標準準拠を前提とするクライアントの文分割とサーバ側の分割がずれる。

**発生条件:** standard_conforming_strings=on を前提とするクライアントが `SELECT '\'; SELECT 2;` を送る。PostgreSQL 意味論では '\'' で文字列が閉じて2文に分割されるはずが、postgres_protocol.cpp:404-410 が \+次文字をペアで消費してクォートトグルを防ぐため1文に結合され、フロントエンドもエスケープ解釈するため未終端文字列エラーになる。SQLSTATE や ErrorResponse の形では fail するが、分割契約(SplitSqlStatements は CLI と共有、server/AGENTS.md:10-11)として standard_conforming_strings の広告と逆の挙動になる。

**確認・対応記録:**

- Startup now advertises standard_conforming_strings=off, matching GoogleSQL backslash escape parsing. TCP startup packet and escaped newline query regression pass batch38; no claim of PostgreSQL full string-syntax compatibility.

### low-125

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `server/postgres_server.cpp:181`

**指摘:** StripLeadingComments がブロックコメントのネスト深度を追跡しないため、正しくネストしたブロックコメントの後に続くキーワードを取りこぼす。SplitSqlStatements 側はネスト対応済み(postgres_protocol.cpp:328-335)で、同じインプットに対してレイヤ間で挙動が分岐する。

**発生条件:** `/* outer /* inner */ */ BEGIN;` を送ると :179-185 は最初の */ でコメント終端と誤認し、残り `*/ BEGIN` の先頭語 `*/` がコマンドになる。BEGIN の高速経路(:1142-1151)とトランザクション制御分岐(:1313)に乗らずフロントエンドに渡ってエラーになり、トランザクションステータスは 'I' のまま(PostgreSQLは 'T')。同根の問題として `/* /* */ */ CREATE TEMP VIEW ...` は RegistersTempObject(:1565-1607)が検出できず session_has_temp_objects が立たないため、後続の全SELECTメッセージが read ワーカーにオフロードされ、スレッドローカルのフロントエンドレジストリに存在しないTEMPオブジェクトを参照して失敗する(:1559-1565 のコメントが警告する故障そのもの)。

**確認・対応記録:**

- Leading comment scanner now tracks nested block-comment depth consistently with statement splitter. Real TCP nested-comment BEGIN returned idle before, transaction after; batch38 server suites pass.

### low-126

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/fragment/fragment.cpp:1119`

**指摘:** kSum集計の累積が符号付きint64の乗算・加算を素通しし、オーバーフロー時のUB。単項は`agg.coeff * value_of(agg.col)`と`acc->sum +=`(1119)、多項式は`__int128 product`への累乗積(1124-1131)も同様にオーバーフローが未定義(1136のstatic_cast<int64_t>への切詰めも実装定義動作に依存)。

**発生条件:** aggregate.kind=kSumでINT64_MAX近傍の値(schema_fuzzer.cpp:106-108がchunk系で生成する極値データと同級)をcoeff=1で累積するとUB。UBSan有効のファズ/テストジョブで検違反になりうる。

**確認・対応記録:**

- N-key GROUP BY and fragment scalar/polynomial SUM use explicit unsigned modular arithmetic and bit_cast to int64, preserving storage kernel truncation contract without signed overflow (including >128-bit monomials). Boundary regressions pass batch37; sanitizer validation pending.

### low-127

**判定:** 修正＋コード確認（専用再現試験なし） (`fixed-code-reviewed`)

**元の対象:** `table/fragment/fragment.cpp:1678`

**指摘:** group_pushedはCapabilities::group_by_single_columnビットしか参照せず、Capabilities::group_by_multi_column(fragment.hpp:89「enabled by the N-key group-by work」)はコードのどこからも読まれない。複数group_colsのフラグメントはmulti_columnビットがoffでも実行され、capabilityゲートとして無効(現行GetCapabilitiesが常にtrue=1336のため今日は発火しない潜在契約違反)。

**発生条件:** group_by_multi_column=falseを返すランタイム(WASM等の将来実装)でgroup_cols=2列のフラグメントをExecuteすると、N-key GROUP BYがゲート無しで実行される。

**確認・対応記録:**

- Group capability selects single vs multi-column bit by key count. Current runtimes advertise both, so no runtime behavior change today.

### low-128

**判定:** 修正＋コード確認（専用再現試験なし） (`fixed-code-reviewed`)

**元の対象:** `table/fragment/fragment.cpp:870`

**指摘:** ScanRunのevalループで`if (!v.IsNull() && v.type == ValueType::kInt64)`が同一条件で2連続(859と870)に現れ、2つ目は到達不能デッドコード。NULLと非int64は両方フォールスルー(874-876)で拒否されており動作は正しいが、意図(非int64のみ拒否のガード)とコード形状が不一致で、将来のNULL経路変更時に誤編集を誘発する。

**発生条件:** 機能影響は現在なし。859側のブロックはint64非NULLを全消費(breakまたはcontinue)するため870は常に偽経路で到達不能。

**確認・対応記録:**

- Removed unreachable duplicate nonnull-int branch in run filter.

### low-129

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `table/fragment/fragment.hpp:39`

**指摘:** fragment.hppの7箇所とfragment.cppの1箇所が存在しない文書を契約の真の源として参照している。「Interface.md §1.2/§3/§3.2/§4/§6.1/§6.2/§6.3」(fragment.hpp:39, 53, 67, 101, 177, 260, 283)と「docs/08-wasm-pushdown.md」(fragment.cpp:1317)はいずれもリポジトリ内に存在しない。IR契約(Estimate/Execute一致、capability意味論、価格モデル)の根拠文書が辿れない。

**発生条件:** fragments/capability契約をInterface.mdに従って実装・レビューしようとすると参照不能。`find -iname "*interface*" -name "*.md"`と`ls docs/08-wasm-pushdown.md`がともに0件であることを確認済み。

**確認・対応記録:**

- Removed references to missing Interface.md and 08-wasm-pushdown.md from fragment contracts; retained actual IR/coverage/cost/TopK contracts in the header.

### low-130

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/full_scan_iterator.cpp:997`

**指摘:** SetRuntimeFilterがグループをAcceptsColumnで拒否した(またはnullptr渡しの)場合、runtime_filter_column_をnulloptにしてkey_column_を上書きするが、key_filter_はnullにしない(:998の`if (runtime_filter_ != nullptr)`が偽のため残る)。結果、構築時に設定済みのレガシーkey_filter(key_column_必須)がNextVisibleImage:440の`... && key_column_.has_value()`で沈黙的に無効化される。

**発生条件:** BeginMorselScan/BeginFullScanでkey_filter+key_columnを設定したスキャンに対し、後からSetRuntimeFilterを(NULLまたは宣言型不一致のグループ、例: VARCHAR列に向けたINT64ドメインのフィルタ)で呼ぶと、key_filterが一切適用されず全行がデコード・後段joinへ流れる。IteratorBase::SetRuntimeFilter契約(iterator_base.hpp:57-66「no-op for iterators that cannot prune」)に反し、フィルタ前提の計画では顕著な性能崩壊。現行の唯一の呼び出し元executor/full_scan.cpp:70-86が型不一致を事前ゲートするため今日は到達しないが、table/単体では契約違反の地雷として残る。

**確認・対応記録:**

- Rejected/null runtime-filter installation retains legacy key column/filter. Previously selected IDs1,2,3 instead of1,3; regression passes after. Also fixed cached peek plan reuse across accepted column changes: reproduced ID3 instead ofID2, now resets peek plan. Fullscan15/MVCC17 pass batch41.

### low-131

**判定:** 既に対処済み＋関連テスト成功 (`already-fixed-tested`)

**元の対象:** `table/fuzz/lsm_merge_fuzzer.cpp:80`

**指摘:** LSMマージ系fuzzオラクルが「同キー1行・昇順・件数上限」しか検証せず、generation順位(最新win)の当たり外れを検出できない。lsm_merge_fuzzerは全runがキーごとに同一ペイロード("x"+id, 80行)のため勝者の識別が原理的に不可能で、schema_fuzzerのマージ検証も件数比較のみ(418-431)。MergeAllSortedRunsの文書化された「latest generation wins on key tie」(fuzz_chunk_common.hpp:131-133, 159-163)が壊れてもこのfuzz群は全部通る。

**発生条件:** MergeAllSortedRunsのタイブレークが誤って旧generationの値を採用する回帰が入っても、lsm_merge/schema両fuzzとpax_storage_fuzzer_testのシード再生は成功し続ける。期待値(ペイロード差+当該キーの勝者一致)の追加が必須。

**確認・対応記録:**

- Concurrent work already made LSM merge oracle generation-specific, tombstone-aware and exact-value checked. Preserved those edits; rebuilt and ran all 11 PaxStorageFuzzerTest cases (batch50-tests2).

### low-132

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk.cpp:300`

**指摘:** varchar kOpDict の compressed-domain カウント折り畳みにおけるクランプコード(デコード値 "")の参加判定 `clamp_pass = empty_rank >= wlo && empty_rank < whi` が、文件一貫のクランプ規約(「corrupt high codes はデコード値でpredicateを評価し、その述語が真のときだけ参加する」— pax_chunk.hpp:2154 の OpDictRegionCountSum コメント等と同一規約)を実装していない。空文字の「挿入ランクがコード窓に入るか」という判定は述語そのものではないため、(a) needle=="" の kEq で辞書に空エントリが無い場合(窓が [c,c) に潰れ clamp_pass=false)、""=="" が真でもクランプ行を数えない(過小計数)、(b) kGt で needle が全辞書エントリ未満の場合(ub(needle)==0 → wlo==0 → clamp_pass=true)、"" > needle が偽なのにクランプ行を数える(過大計数)。kLt と kGe については規約どおりの挙動になることを確認済み。なおエンコーダ出力(ValidateWalk 済みチャンク)では全コード < ncodes でクランプバケットが発生しないため、影響は検証を経ていない領域(legacy/改変フッタ)に限られる。

**発生条件:** code space が 2^cw に満たない(クランプコードを含みうる)kOpDict varchar 領域に対し、(a) VarcharFilterCount(kEq, needle="") を発行して辞書に "" エントリが無い → クランプ行が数えられない、または (b) VarcharFilterCount(kGt, needle=最小心値より小さい文字列) を発行 → クランプ行(""と同値)が誤って加算される。行評価経路(VarcharRegionMatchCountGeneric/VarcharRegionAt は "" として述語評価)と結果が乖離する。

**確認・対応記録:**

- OpDict clamp predicate now evaluates decoded empty string directly, not its dictionary insertion rank. On dictionary without empty entry, high code was wrongly counted as abc and not empty before; all predicate kinds agree with selection after. Initial adapter fixture did not support varchar OpDict; corrected to EncOpDictStr before reproduction (batch40-before4).

### low-133

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk.hpp:1795`

**指摘:** VarcharGroupCache::SelectColumns の投影マスクは std::array<uint64_t,16>(=1024 列)で、column >= 1024 の列は無言でマスクから落ちる。IsColumnSelected が false を返し、消費側はその列に NULL(既定 Value)を格納するため、1025 列以上のスキーマで投影すると見た目上「データ欠落(誤 NULL 結果)」になる。

**発生条件:** 1025 列以上のテーブルで列 1024 以降を含む投影を付けた逐次スキャン(ReadRowIntoCached 経路)を実行すると、列 1024+ のセルが値ではなく NULL で返る。現状 SelectColumns の呼び出し元は fuzzer のみ(table/fuzz/fuzz_chunk_common.hpp:89)で本番 scan は projection_active=false のため潜在的だが、公開 API の誤結果経路である。

**確認・対応記録:**

- Kept inline1024-column mask for common schemas, added lazily allocated overflow words for wider projections. Selections1024/65534 failed before and now pass; re-selection clears old wide bits (batch37).

### low-134

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `table/pax_chunk/pax_chunk.hpp:1889`

**指摘:** PaxChunk::Deserialize の契約コメント「This is the persistence entry point used by pack files」に対し、このツリー内に pack file の読み書き経路が存在せず、Deserialize の呼び出し元は fuzz ハーネスのみ。また参考ドキュメント docs/page_format.md は「rows in RowPage blocks」のみを記載し、PAX チャンク(カラムナ格納)の形式・永続化に一切触れていないため、永続化フォーマット文書として実態とずれている。

**発生条件:** pack file による永続化を前提に「Footer を外持ちしなくてよい」設計(hpp:1890)に依存する呼び出し側を書いても、実際には pack 書き出し側が存在しないため接続先がない。逆に page_format.md だけ読んだ読者は、テーブル行が PAX チャンクにも格納される(lstable_bridge が SortedRun から BuildFromSortedRun で構築)ことを知らずに永続化/リカバリ設計をする。

**確認・対応記録:**

- PaxChunk::Deserialize is documented as standalone wire decoder exercised by fuzz harnesses, not a currently connected database pack-file persistence path.

### low-135

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `table/pax_chunk/pax_chunk.hpp:759`

**指摘:** hpp の kTopk ワイヤ形式コメントは [d u16][hot 15x8B][freq 15x2B][nexc u16][nibbles][inner](ischeme/iparam フィールドの記載なし、nexc は freq の後)と記すが、実際のワイヤはエンコーダもデコーダも [d u16][nexc u16][hot][freq][ischeme u8][iparam u32][nibbles][inner] である。

**発生条件:** kTopk リージョンを hpp コメントに従って自作パーサ/WASM リーダで読むと、nexc を freq テーブル直後に読み違え、5バイトの ischeme/iparam の存在も見落として全フィールドがずれる。

**確認・対応記録:**

- TopK wire comment now matches encoder/decoder: d,nexc,hot,freq,inner-scheme,inner-param,nibbles,inner-region.

### low-136

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:1997`

**指摘:** Chimp128ColumnCountSum / AlpRdTansColumnCountSum / PatasColumnCountSum の非対象スキーム フォールバックが、rc ≤ kRowGroupSize の検証なしに std::array<int64_t, kRowGroupSize>(8KB スタック)へ DecodeInt64ColumnRg する。デコーダは footer_.row_counts[g](u16、最大65535)個を書くため、rc>1024 の footer でスタックバッファオーバーフロー(SEGV/スタック破壊)。

**発生条件:** Deserialize 経由なら ValidateWalk が rc≤1024 を強制する(table/pax_chunk/pax_chunk_scan.cpp:5700「kRowGroupSize contract」)ため現行エントリからは到達不能だが、これらは公開 API(pax_chunk.hpp:2121-2131)で、メモリ上で組んだ hand-built Footer を直接渡す新規呼び出し側が即座にスタックを壊す。同ファイルの他の全デコード呼び出しは rc>1024 をヒープへ落とす(gate: :3688 ReduceColumn、:4379 VectorizedScanColumn、:7438 BuildSelectionVector、:5927 kWaveletR)ため、この3箇所だけが規約違反。fuzz_chunk_common.hpp:608-612 がこの3関数を呼ぶ巡回カーネルである点でも一貫性維持が要る。

**確認・対応記録:**

- Chimp128/AlpRdTans/Patas column aggregate helpers reject row groups larger than stack fallback capacity before decoding. Oversized constant fallback tests return false without modifying outputs (batch37); ASan pending.

### low-137

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:2424`

**指摘:** pax_chunk.hpp の ReduceColumn 契約コメント(:2093「COUNT needs no decode, APPROX_COUNT_DISTINCT reads the per-(row-group,column) HLL sketch」、:2088「kApproxDistinct (HLL estimate)」)に対し、実装は HLL を preds.empty() のときしか読まない。述語付き plan の APPROX_COUNT_DISTINCT は HLL を一切使わず、decode した選択行の正確な distinct 数(unordered_set)を返す。estimate ではなく exact であり契約記述と不一致。

**発生条件:** 述語付きで ReduceColumn(kApproxDistinct)を呼ぶと HLL estimate ではなく exact 値が返る(ドキュメント読みの呼び出し側は誤差の存在を前提にする)。コード側コメント(:2446-2447「Predicated APPROX_COUNT_DISTINCT falls back to an EXACT distinct count over the decoded selected values (the footer HLL cannot be filtered)」)が正しい現挙動を文書化しており、ヘッダ側が未更新。

**確認・対応記録:**

- ReduceColumn docs distinguish unpredicated footer HLL/COUNT from predicated selection masks and exact decoded DISTINCT. No false claim that predicated COUNT avoids decoding.

### low-138

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:3146`

**指摘:** kConst 圧縮実行 MIN/MAX arm が v != 0 を「非 NULL 行の実在」と無関係に consider(v) する。全セル NULL の kConst セル(null_present 付き hand-built footer)では compressed fold が min/max=v を valid で返し、同一チャンクの汎用デコード経路(NULL を :3707-3710 でマスクし minmax_seen=false → valid=false)と矛盾する。SUM サブ arm(:3143 の SelNonNullCount)は NULL を正しく除外しており、同 arm 内でも契約が不揃い。

**発生条件:** null_present=true かつ encodings=kConst で値が非 0 の footer を直接構築して ReduceColumn(kMin/kMax, preds 空)を呼ぶと、zone-map fast path が失敗した場合に compressed arm へ落ち、MIN/MAX が NULL ではなく v を返す。コメント自身が「unreachable from the builder ... hand-built footer」と認めている builder 非到達ケースだが、ファイルの照合対象(差分ファズの期待値)は汎用デコード側の挙動。

**確認・対応記録:**

- CONST MIN/MAX now requires a selected non-NULL cell even when encoded value is nonzero. Hand-built all-NULL nonzeroCONST regression failed before, passes after batch37.

### low-139

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_aggregate.cpp:4916`

**指摘:** PaxChunk::ColumnMinMax が zone 配列が短い行グループを continue で無言スキップし、そのグループの実 min/max を欠いた部分範囲を kSuccess で返す。ReduceColumn の未述語 MIN/MAX メタデータ fast path(:2431-2442)は kSuccess を信じて res.valid=true で即returnするため、スキップされたグループに真の極値があるとき、decode する汎用経路と異なる誤った MIN/MAX が「成功」として確定する。

**発生条件:** all_zm_present=true だが all_zone_mins/maxs が rg*cc に満たない hand-built footer で、欠落セルに列全体の真の MAX が属する場合: SELECT MAX(col) の metadata fast path が部分範囲の max を返す(汎用経路なら全セル decode で真値)。fail-closed な代替(kNotExists を返して汎用 decode へ)が同関数の他の gate(:4906-4910)の作法。

**確認・対応記録:**

- ColumnMinMax declines incomplete zone arrays instead of publishing partial bounds. Missing second group regression failed before, now returns no proof and leaves outputs untouched (batch37).

### low-140

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `table/pax_chunk/pax_chunk_encode.cpp:9328`

**指摘:** kPostings 強制パスのインラインコメントが構造ゲートを「4 <= d <= 128」と記すが、エンコーダの実ゲートは d <= 64 であり、hpp の列挙体コメント(4 <= d <= 64)とも不一致。128 はデコード側容量 kPostingsMaxLists=128 との混同。

**発生条件:** TINYLAMB_FORCE_SCHEME=POSTINGS で distinct 65〜128 の列を強制したとき、コメントを信じた開発者/ベンチ担当は「ゲート違反で落ちた」と誤認する(実際は 64 で門払い)。強制パスの測定契約(the measurement contract)の docs として誤解を生む。

**確認・対応記録:**

- Forced postings encoder comment corrected to actual d<=64 gate, distinct from decoder capacity128.

### low-141

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_encode.cpp:9957`

**指摘:** codec_benchmark::EncodeInt64Block は 2^32 行までの入力を受け付けるが、複数のスキームのリージョンヘッダが u16 であり、巨大入力で static_cast<uint16_t> が無言で切り詰められ、自己無整合なリージョンが生成される。本番 Build 経路は 1 行グループ <= kRowGroupSize(1024)のため不発で、ベンチ/ブロック API 専用の問題。

**発生条件:** 例えば kInterp で ~840 万行超(ncy=ceil(n/128)>65535)の単調列を渡すと AppendBE16(ncp) が下位16ビットのみ書き、チェックポイント数がヘッダと実体で不一致な壊れたリージョンを返す。kDdRice/kDeltaVarint/kXorForC/kGorillaC/kChimpC の ncp、EncAlpF64 の nexc、EncHuffDictI64/EncFreqI64 の freq(行数>65535 で頻度値そのものを切り詰め)も同型。

**確認・対応記録:**

- 8,388,608-row INTERP block reproduced truncated uint16 checkpoint count before fix (/tmp/audit-batch42-before-tests.log).
- Benchmark block encoders conservatively reject >65535 rows for schemes with narrow metadata; documented PLAIN/CONST/integer RLE exceptions retained. Physical 1024-row groups unchanged.
- pax_chunk_codecs_test: all 18 pass; integer/double/string oversized blocks and allowed PLAIN/RLE covered (/tmp/audit-batch42-tests.log).

### low-142

**判定:** 他項目と重複・対応箇所を修正 (`duplicate-fixed`)

**元の対象:** `table/pax_chunk/pax_chunk_scan.cpp:2314`

**指摘:** 二次索引オプトアウト(TINYLAMB_NO_INV_IDX / TINYLAMB_NO_XOR8)の挙動コメントが「Documented in docs/05-pax-chunk.md」と参照するが、そのファイルはリポジトリのどこにも存在しない。また TINYLAMB_NO_INV_IDX / TINYLAMB_NO_XOR8 / TINYLAMB_NO_HASH_IDX のいずれも docs/ 配下のどの文書にも記載がなく、「文書化済み」という主張自体が偽である。セクション欠損時の契約(「absent section は scan で答えるので決して誤りにならない、遅いだけ」)がこのコメント以外に一次資料を持たない。

**発生条件:** 読み手が参照先 docs/05-pax-chunk.md を開こうとするとファイルが無い。`find docs -iname '*pax*'` も空で、PAX チャンク形式の設計文書自体がリポジトリから失われている(コメント末尾の WASM reader との契約記述も行き場がない)。

**確認・対応記録:**

- Covered by medium-9 environment inventory update.

### low-143

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_scan.cpp:2548`

**指摘:** 全列ゾーンマップ(all_zone_mins/all_zone_maxs)は int64 以外の列(DOUBLE/VARCHAR)の統計 min/max を常に [0,0] に折りたたんだ値(pax_chunk_encode.cpp:7682-7684 SOA 経路 / 7735-7738 行プール経路の「bounds fold toward 0」)を列型に関係なくそのまま格納する(2548-2549)。一方 RGOverlapsCol(4978-4995)は列型を一切ゲートせずゾーン比較だけを行うため、DOUBLE 列で呼ぶと 0 を含まない範囲の全グループが「非重複=空」と判定され、誤った行グループ枝刈り(空結果)に直結する。現行の唯一の呼び出し元 fragment.cpp:328 は ConjunctSupported→IsInt64Column(fragment.cpp:71)で int64 に限定しているため潜在バグだが、RGOverlapsCol のヘッダコメント(pax_chunk.hpp:1515-1520)にも int64 専用の前提は書かれておらず、同一チャンクの int64 列では正しく動くだけに罠になっている。

**発生条件:** 将来の呼び出し元が DOUBLE 列の述語(col > 100 など)のグループ枝刈りに RGOverlapsCol を使うと、ゾーン [0,0] のため 0 を含まない [101, INT64_MAX] と全グループが非重複になり、実データがあるのに空結果を返す。bloom も同経路で raw 0 のみ立つ(XorMix64(0))ため、bloom 枝刈りでも同形の誤りが起き得る。

**確認・対応記録:**

- Reproduced false pruning for VARCHAR/DOUBLE zone maps in compressed and uncompressed chunks. Encode unbounded integer intervals for noninteger profiles. All 19 codec tests pass (batch51).
- Batch55 ASAN/UBSAN: 784 tests across17 binaries pass with halt_on_error=1; fault-injection/platform gaps above are unchanged.

### low-144

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/pax_chunk/pax_chunk_scan.cpp:2986`

**指摘:** キーゾーンマップ(group_min_keys/group_max_keys)は「入力 run がキー昇順」であるという前提を検証せずに構築・使用される。同ファイルの PGM 学習索引は昇順を実検証してから pgm_present を立てる(3009-3013)のに、ゾーンマップには同等の防御がなく、PointLookup(4900-4916)と LowerBoundIndex(5419-5432)のグループ二分探索が「groups are ascending and disjoint」(pax_chunk.hpp:1401-1404 の文書化済み前提)を無検証で信じて候補グループを1つに絞り、他グループを再探索しない。ソート契約に違反する run が渡ると、実在するキーに対してエラーではなく kNotExists を静かに返す(誤結果)。NULL キーの扱い自体(非NULLのみで min/max、全NULLグループでマップ全体を無効化、2989-2997)と RGOverlapsCol の NULL→raw 0 補正(4989-4994)は検証済みで正しい。

**発生条件:** 将来の呼び出し側が昇順でない SortedRun を BuildFromSortedRun に渡した場合(ヘッダ契約違反だが PGM 経路が自己検証を持つこと自体がその懸念の証拠)、group_max_keys の二分探索が誤ったグループを選び、キーがチャンク内に存在しても PointLookup/LowerBoundIndex が kNotExists を返す。CRC は通るため破損検知もない。

**確認・対応記録:**

- PAX builder verifies bounded key lengths and ascending order before creating searchable zone maps; rejects unsorted runs with kCorrupt. Both row/SOA schemas and compressed/uncompressed regressions pass batch61.

### low-145

**判定:** 到達不能コードを削除 (`dead-code-removed`)

**元の対象:** `table/sorted_run/sorted_run.hpp:37`

**指摘:** `class BlobFile;`前方宣言がsorted_run.hppに残存するが、BlobFile型はリポジトリのどこにも定義・使用されていない(宣言1行のみ)。design.md:409の「BlobFileとSortedRunの対応が復元後も一致する」という不変条件が参照する抽象が退隠しており、現行SortedRunはBlobFileを持たない。

**発生条件:** BlobFile前提のレイヤー理解(design.md D10)でsorted_runを読むと実装と不一致。デッド宣言のみが残る。

**確認・対応記録:**

- Removed unused BlobFile forward declaration. design.md already labels old BlobFile recovery acceptance conditions historical (batch19); current SortedRun owns vector<char> payloads.

### low-146

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `table/table.cpp:1243`

**指摘:** BuildScanMorselsがAcquireLease失敗(DropPending/Retired)時に空morselsを正常値として返し、txnをMarkFailedしない。チェーンwalk経路のエラー(:1280-1288)はMarkFailed+空返却なのと非対称で、リース競合が「空表」としてクエリ結果に化ける。

**発生条件:** トランザクションAがDROP TABLEでBeginDropした瞬間に並行SELECTがBuildScanMorsels→AcquireLease失敗(kConflicts)→morsels空→morsel駆動スキャンが0行を返し成功ステータス。エラーでも待機でもなく「抹消済みのように見える空結果」を返す。dropがアボートして表が復活した場合、同一SELECTの再実行で行が忽然と出現する。

**確認・対応記録:**

- Dropping and retired incarnations returned empty morsels without txn failure; regression failed both cases before fix.
- BuildScanMorsels now marks transaction failed when AcquireLease fails. table_test 53/catalog_test 39 pass.

### low-147

**判定:** 修正済み・専用の障害注入テスト未実施 (`fixed-needs-fault-test`)

**元の対象:** `table/table.cpp:85`

**指摘:** CreateIndexのバックフィル失敗時に半分構築のLSM索引を`std::ignore = LsmIndex(root_pid).Destroy(txn);`で破棄し、その失敗を無通知で握りつぶす。同一ファイル:93-102の規約(「補償の失敗はstd::ignoreに消えてはならない」、LogCompensationFailure)に自ら違反し、破棄失敗時の孤児ページ/ツリーが診断不能になる。

**発生条件:** 重複キーを含む表へのCREATE INDEXでバックフィル中にkDuplicates(table_test.cpp:796-822がこの経路を踏む)→indexes_.pop_back後のDestroyがWAL書き込み失敗等で失敗してもログゼロ。確保したroot_pidのページが回収されず、以降の障害調査で「どのツリーが漏出したか」が追えない。

**確認・対応記録:**

- Both unique/multi-index backfill cleanup branches now log LsmIndex::Destroy failure and mark transaction failed, preserving original failure status. Successful compensation keeps duplicate-index failure recoverable.
- Existing CreateIndex_WhenBackfillFails_CleansUpHalfBuiltIndex and all53 table tests pass; secondary cleanup WAL failure not yet fault-injected.

### low-148

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `transaction/transaction.cpp:306`

**指摘:** AddWriteSet(rp, before)の再取得失敗パスがwrite_set_にrpを残し、後続のAddWriteSet(rp)が実intentなしでtrueを返す穴。transaction.cpp:310は再取得がfalseを返してもwrite_set_.erase(rp)しない(296-298行や321-323行の失敗パスと不对称)。以後、単引数版AddWriteSetは284-286行のwrite_set_メンバ判定で短絡trueを返し、intentが存在しないままRegisterVersionWriteへ到達しうる。RegisterVersionWriteは「without reserved write intent」をLOG(ERROR)してassert(false)後、NDEBUGビルドでは無音returnする(transaction_manager.cpp:1817-1821)ため、呼び出し側は書き込み成功を信じたまま値が欠落する(誤結果/データ欠落)。

**発生条件:** RowPage::Insertの穴プローブでTryAddWriteSetがintentを確保した直後(row_page.cpp:50)、before画像を供給する再取得の前にそのトランザクションがWound()されるかfail-stopする → 再取得がfalse・write_set_にrpが残留。現行コードは「falseなら必ずabort」という呼び出し側契約で守られているが、契約違反する呼び出しや同一positionへの再書き込みで暗黩の書き込み欠落に転化する。

**確認・対応記録:**

- Write-set cache hits now reject wounded/fail-stopped/failed transactions; failed before-image reacquisition marks transaction failed while retaining intents until abort. Reproduced cached success after wound before fix; batch35/36/37 transaction suites pass.

### low-149

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `transaction/transaction.cpp:348`

**指摘:** nullガードの不在がAPI間で不对称。デフォルト構築Transaction(「For test purpose only」, transaction.hpp:270)に対し、ReadVersion(409-413)/RegisterVersionWrite(495-497)/CancelStagedVersion(515-518)/HasForeignWriteIntentやWaitForRowWriteIntent(transaction.hpp:436-445)はnullを弾くのに、SnapshotSeesRow(transaction.cpp:348-350)・ReleaseWriteIntent(352-353)・AddWriteSet/TryAddWriteSet(289/336のtransaction_manager_->AcquireWriteIntent)・AcquireIndexReadGuard(transaction.hpp:463-466)・GetPageManager(transaction.hpp:706-708)は参照外しでSEGVする。

**発生条件:** テストや組込み用途でmanagerなしTransactionにAddWriteSetを呼ぶと即SEGV。同じ「テスト用オブジェクト」でも入口ごとにセンチネルreturnとNULL参照外しが混在しており、DefaultTransaction*系テスト(transaction_test.cpp:453-474)が通る入口だけが堅牢化されている。

**確認・対応記録:**

- Default transaction SnapshotSeesRow and ReleaseWriteIntent guard missing manager; CommitWait returns success for default object. Default-object regression segfaulted before fix; passes after.

### low-150

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `transaction/transaction.cpp:718`

**指摘:** CommitWaitの二重コーナーケース。(1) transaction_manager_がnullのデフォルト構築Transactionで呼ぶと即NULL参照外し。(2) WALフラッシュワーカ死亡後(logger Failedラッチ)、CommittedLSN()がprev_lsn_に到達できずwhileスピンが永久に回る(1ms sleepのビジー待ち)。ヘッダコメント(transaction.hpp:699-704)は「visibility barrier」とのみ述べ、デッドWAL時の振る舞いに言及しない。現状の呼び出し元はテストのみ(grepで本番路径なし)だがpublic API。

**発生条件:** transaction_coverage_test.cpp:1394-1412のようなWAL全損後の状態でCommitWaitを呼ぶ利用者がいると、CommittedLSNが永久に停止しているためスピンから帰らない。

**確認・対応記録:**

- CommitWait returns Status and waits for max(record-end LSN, dependency LSN) through WAL durability barrier. Real /dev/full WAL failure regression verifies error propagation rather than unbounded polling. Batch35/36 coverage67 pass.

### low-151

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `transaction/transaction_manager.cpp:1193`

**指摘:** WaitForRowWriteIntentの早期失敗3路径(fail-stop 1189-1192、IsFinished 1193-1195、IsWounded 1196-1198、kNoWait 1199-1201)がIntentWaitFailureを記録せずkConflictsを返すため、lock_timeout.md:34-40の「Every failed write-intent wait records why it gave up」(5分類を全待機が記録する)と不一致。LastIntentWaitFailure()に前回失敗のゴミが残る。さらに待機ループ(1249-1270)にはAcquireWriteIntentの各ポリシーブランチが持つholder-abort即時ギブアップ(IsAborting、transaction_manager.cpp:958/1036/1099)がなく、abort中holderの裏でフルの待上限(既定5秒)を乗り切る。

**発生条件:** Table::IndexInsertのunique検査待ち(table.cpp:997)でWaitForRowWriteIntentに泊まった待機者のholderがabort開始(latch-cycle成立)しても、この入口では~10msTickのkHolderAborting即時撤退が働かず5秒乗る。ホルダーアボートgrace(250ms)が効くのはAcquireWriteIntent経由の待ちのみで、失敗分類もタイムアウト時(kTimeout, 1266行)しか記録されない。

**確認・対応記録:**

- WaitForRowWriteIntent clears stale cause on entry and records fail-stop, finished/conflict, wounded and no-wait/conflict early exits. Tests assert distinct diagnostics including real WAL fail-stop; batch36 transaction40/coverage67 pass.

### low-152

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `transaction/transaction_manager.cpp:2374`

**指摘:** ReservationBisectModeのモード4がコメントどおり動かない。transaction_manager.cpp:2332-2334は「4=registration on but admission discount off」と定義するが、IndexUndoReservedBytesのガードは`bisect != 0 && bisect != 4 && bisect != 5`でモード4を通すため、モード4はモード0と全く同じ(admission割引が有効のまま)になり、「割引OFF」のA/B比較が不可能。診断ツールの挙動が自己文書と不一致。

**発生条件:** TINYLAMB_DISABLE_INDEX_RESERVATION=4で「予約登録はあるがadmission割引なし」の実験を意図しても、実割引が計算されて返り(2385-2391)、対照実験として無効。モード4を割引OFFにするならここで0を返す必要がある。

**確認・対応記録:**

- Mode4 retains shared reservation registration while disabling admission discount. Fresh-process test additionally exposed Transaction::ReserveIndexUndoBytes treating any env presence as disabled; removed duplicate env gate so manager is authoritative. Fresh-process modes0/4 and default transaction suite pass batch37.

### low-153

**判定:** 到達不能コードを削除 (`dead-code-removed`)

**元の対象:** `transaction/transaction_manager.hpp:562`

**指摘:** コメント契約上はTransactionデストラクタから呼ばれるはずのReleaseActiveTransaction(hpp:559-562)とUnregisterActiveTransaction(hpp:587-588)がどこからも呼ばれていない死コード。デストラクタ(transaction.cpp:64)はFinishRunningNoexceptのみを呼び、全終了路径はForgetTransactionで登録抹消する。またShouldAbortOnWait(hpp:856-859)は宣言のみで定義が存在しない(呼ぶとリンクエラー)。ReleaseActiveTransactionのコメント「Called from the Transaction destructor」は虚偽で、後続のレジストリ整理改修者が二重解放/GCカウント二重加算を警戒する根拠になる。

**発生条件:** ForgetTransactionとReleaseActiveTransactionの役割分担を誤解したまま登録抹消路径を変更すると、実装されていない「デストラクタ経由の解放」を前提にし、ForgetTransactionのerase(ポインタ同一性チェックなし、transaction_manager.cpp:2604-2610)と 새路径の二重化で壊れる。grep確認: 両関数およびShouldAbortOnWaitの使用箇所は定義部のみ。

**確認・対応記録:**

- Removed unreferenced UnregisterActiveTransaction/ReleaseActiveTransaction declarations and definitions; retained actual ForgetTransaction and move tracking. Transaction lifecycle suites pass batches35-37.

### low-154

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/constraint.cpp:162`

**指摘:** Constraint と Type のデコーダがストリームのタグバイトを無検証でenumに静的キャストする。同じ層の Value デコーダ(value.cpp:1702-1707)と Column デコーダ(column.cpp:73-79)は不正タグで e.Fail() によるsticky failureを起こすのに、この2つは破損カタログから範囲外の ctype/TypeTag を黙って受入れ、以降のswitch(ostream出力 constraint.cpp:57-85、Type::Size type.cpp:27-45 など)がどのcaseにも当らず黙って通過する。

**発生条件:** 破損したカタログ/WALイメージから Constraint をデコードし ctype=200 の Constraint が生成→IsUnique()等の判定がすべてfalse、operator<< は空文字列で出力。Column の Type が type_=0xFF の Type になる→IsVariableLength() false、Size() は default で CHECK_MSG(false) による異常系。破損データの検出が層内で一貫しない。

**確認・対応記録:**

- Constraint and Type stream decoders reject unknown enum bytes through sticky Decoder::Fail; corrupt-tag regression failed before and passes after. All known TypeTag bytes remain accepted. constraint11 pass batch34.

### low-155

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/row.cpp:572`

**指摘:** Row::TryPeekInteger は optional<int64_t> を返す無投入プローブAPIとして宣言されている(row.hpp:103-105、例外に言及なし)が、実装は corrupt イメージのスキップ失敗で detail::ExcShimThrow で例外を投げる。"Try* はStatus/optionalで返す"という層の命名規約とヘッダ契約に反し、スキャンループ内の呼び出し元が例外経路を想定していないと破綻する。

**発生条件:** 破損ページ上の行で TryPeekInteger を呼ぶと、可変長列のスキップ失敗(kCorrupt)が std::runtime_error として full_scan_iterator.cpp:318/324/459/465 など呼び出し元へ例外で飛ぶ。boundary catchがあれば回収されるが、optional を返す関数から例外は呼び出し側から不可視。

**確認・対応記録:**

- Row::TryPeekInteger now returns StatusOr<optional<int64_t>>: NULL/non-integer stays nullopt; array skip corruption, schema mismatch and invalid column return error. Full/index scans propagate Status rather than dropping failed peeks. Corrupt ARRAY prefix threw before fix; regression passes after. Updated row-codec fuzzer to inspect Status. Batch39 row14/fullscan13/index31 and storage fuzzer11 pass.

### low-156

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/row.cpp:750`

**指摘:** Row::Extract がオフセット範囲外のとき Status エラーではなく空Rowを返してエラーを飲み込む。投影バグやスキーマ不一致が遠くの別エラー(空キーの "cannot encode key of empty row" throw、row.cpp:611)に化けて原因箇所から離れた場所で表面化する。

**発生条件:** schemaより広い添字のelms(例: 3列Rowにoffset=5のExtract)を渡す→エラー無しの空Row。これをキー構築に流すと EncodeKeyMemcomparableFormat が "cannot encode key of empty row" でthrowし、真因(範囲外オフセット)は失われる。

**確認・対応記録:**

- Added Row::TryExtract with explicit out-of-range Status; legacy Extract now documented throwing wrapper instead of silent empty row. Lookup/late-materialization joins use checked projection and propagate failures. Row projection and invalid lookup-column regressions pass; executor227/late9/row-schema5 pass batch39. First lookup test fixture selected pre-existing unrelated index; corrected to newly created index before successful verification.

### low-157

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/schema.cpp:134`

**指摘:** Schema::WithName が新規デフォルト構築に列と名前だけをコピーし key_column_index_/schema_version_ を引き継がない。Schema の Encoder/Decoder(schema.cpp:212-224)もこの2フィールドを直列化しない。非ゼロの key_column_index を持つスキーマは RENAME TO(database.cpp:843 で使用)やカタログ復元でキー列インデックスが0に静かに戻る。

**発生条件:** Schema(name, columns, /*key_column_index=*/1) で構築→WithName("t2")→KeyColumnIndex()==0。EncodeMemcomparableFormatのキー構築(row.cpp:609-629)やPAXフッター(table/pax_chunk/pax_chunk_scan.cpp:2305)が別列をキーとして扱う。なお現ツリー内に非ゼロkey_column_indexの構築箇所を発見できておらず(4-引数ctorの呼び出し探索まで)、潜在リスクとしての報告。

**確認・対応記録:**

- Schema::WithName starts from a copy, preserving key column, version and derived layout while assigning fresh lookup identity. Nonzero-key/version regression failed before; schema12 pass batch34.

### low-158

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/value.cpp:1037`

**指摘:** 対になっているエンコードAPIのNULL/配列扱いが不一致: TryEncodeMemcomparableFormat は kNull を1バイト正当キーとしてエンコードし(value.cpp:925-930「NULL keys are real index keys」)、kArrayも対応するが、非alloc版 EncodeMemcomparableFormatTo は kNull(1037行)と kArray(1055行)で例外送出。デコード側 DecodeMemcomparableFormatBounded は kNull を受ける(1063-1065行)ため、エンコードだけが投げる非対称。

**発生条件:** NULLキー列を EncodeKeyMemcomparableFormatTo(row.cpp:620-629)経由でエンコードすると"Cannot encode unknown type."でthrow。現行の呼び出し元は memtable.cpp:300-321 が NULL/VarChar を事前に排除し、汎用パス(row.cpp:332)は Try 系を使うため今日の被害経路は未確認だが、API契約として潜在不一致。

**確認・対応記録:**

- Buffer memcomparable encoding now supports NULL and recursive ARRAY with the same wire bytes as allocating encoding. Nested/NULL elements regression failed before; value90/row13/data_chunk35/differential17 pass batch34.

### low-159

**判定:** 既に対処済み＋関連テスト成功 (`already-fixed-tested`)

**元の対象:** `type/value.cpp:518`

**指摘:** Value::DeserializeInto の kArray 経路がストリーム読みの count(最大4G)を検証なしで elements.reserve(count) し、要素の再帰デコード(528行)・SkipSerializedInto の再帰(583行)・Row::PeekValueSerializedSize の再帰(row.cpp:678)・TryDecodeMemcomparableFormat の再帰(1215行)にネスト深さ上限がない。memcomparable側には要素数上限(kMaxEncodedArrayElements=2^20、1191行)があるが深さ上限はなく、行イメージ経路の保護は呼び出し側が先に Row::PeekSerializedSize で境界検証することに全て依存している。

**発生条件:** (a) 境界検証なしで Value::TryDeserialize を呼ぶ任意の経路で count=0xFFFFFFFF の corrupt 配列イメージを与える→巨大reserve→bad_alloc が Try* 関数から漏出。(b) ネスト配列の memcomparable キーはページサイズに縛られないため、1MB級の深くネストした ARRAY キー(~7バイト/深さ)で再帰が既定スタックを超え得る(SEGV)。現行の行デコード呼び出し元は DeserializeBounded→Peek で防がれており、直接被害経路は未確認。

**確認・対応記録:**

- Current Value deserialize/skip code already bounds array count at 1<<20 and nesting at 32. Added huge-count/deep-array rejection and subsequent shallow decode test; passed before additional changes and in value90 batch34. Raw-pointer codec still relies on caller image-length validation.

### low-160

**判定:** 修正＋関連テスト成功 (`fixed-tested`)

**元の対象:** `type/value.hpp:534`

**指摘:** MoveString() が owned_data をムーブアウトした後も type は kVarChar のまま、value.varchar_value はムーブ元バッファを指し続ける。ムーブコンストラクタ/ムーブ代入は o.type=kNull に落す(value.hpp:217, 251, value.cpp:348, 382)のに、MoveString だけ退避処理がなく、呼び出し後に同一Valueへ == や AsStringView 等を適用すると解放済み/未規定バッファの string_view を読むUB。

**発生条件:** Value v("abc"); std::string s = v.MoveString(); v.AsStringView() → owned_data はムーブ後未規定状態で、SSO解放/長文字列解放済みバッファ参照。現行の呼び出し元(table/pax_chunk/pax_chunk_scan.cpp:3896, 4166, 4362)は直ちに捨てるか SetString に流し込むため生被害なしの設計上の足穴。

**確認・対応記録:**

- MoveString transfers exactly the visible string and leaves source NULL with cleared scalar tag, preserving shared owners and borrowed/sliced strings. Short/long unique/shared/borrowed/slice regression failed before; value90/data_chunk35 pass batch34.

### low-161

**判定:** 説明・仕様記述の訂正 (`docs-corrected`)

**元の対象:** `type/value.hpp:564`

**指摘:** value_storage.md「Current model」(kVarCharはstd::stringメンバ/Targetはoffset-length化)は陳腐。実装はunion内にstd::string_view varchar_value + 別メンバstd::string owned_data(docの言うTargetが既に実装済み)で、さらにkArray(shared_ptr<ArrayPayload>)も持つ。

**発生条件:** Valueのライフタイム契約(varchar viewがowned_dataを指す)をdocsの「所有するstd::string」前提で扱うと、string_viewのダングリング可能性について誤った安心感を得る。

**確認・対応記録:**

- Corrected retired TSAN justification (deadlock checking still explicitly disabled, not claimed covered), Value owned/borrowed payload model, nested-loop error propagation and malformed query guide formatting; source reviewed (batches20-27).

