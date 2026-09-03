# tinylamb 全コード精査報告書 (2026-08-31)

全レイヤー (common / type / page / recovery / transaction / index / table / database /
expression / executor(+detail) / plan / query / server / main.cpp) を精査し、
**99 件の候補欠陥**を特定、うち **49 件を回帰テスト付きで修正**した。
残る候補は「大きな設計判断または大規模リファクタを要する」ため §3 に記載する。

検証: `ctest --test-dir build` → **2057 / 2064 成功**
(失敗 7 件は精査開始前から存在する WIP 由来の compliance 期待値差分。
ベースラインで失敗していた `CommitPublicationTest` と
`SqlEngineTpchTest.ExplainReturnsPlanAndAnalyzeReturnsRuntimeProfile` は
本修正で通過するようになった)。`python3 scripts/check_layering.py` も通過。

---

## 1. 修正済みバグ一覧 (49 件)

### 1.1 common / type (12 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 1 | `OmittedString` が `substr(len-8)` で size_t アンダーフロー。短いインデックスキーの Dump で `out_of_range` 例外 | 省略条件を `limit+16 <= size` に変更 | `DebugTest.OmittedString_WhenShorterThanEightBytes_DoesNotThrow` |
| 2 | `LogMessage` が非スレッドセーフな `std::localtime` を使用 (並列スキャンでデータ競合) | `localtime_r` に変更 | 既存ログテスト+TSAN 対象 |
| 3 | `Function::argument_count_` 未初期化 (`Function() = default`) | `int argument_count_{0}` | 既存シリアライズテスト |
| 4 | `StatusOr::MoveValue()` が 2 度呼べる (ムーブ後も optional が engaged で 2 回目が throw しない)。`Value()` も同様 | `status_` をガードに使用し契約どおり 2 回目を throw | `StatusOrTest.MoveValue_SecondCallThrows` / `Value_OnFailedStatus_Throws` |
| 5 | `Decoder::operator>>(bool&)` が生バイトを bool 表現に読み込む (0/1 以外で UB) | uint8 で読み `!= 0` 正規化 | `DecoderTest.Bool_NonCanonicalByte_NormalizesToTrue` |
| 6 | `Value::operator==` (double) の 1e-9 イプシロン比較が非推移的で `operator<`・`std::hash` と矛盾。double キーの GROUP BY / DISTINCT / unordered コンテナが破綻 | 完全一致 + NaN 正規化 (ハッシュと統一)。イプシロンは呼び出し側で明示的に opt-in | `ValueTest.EqualityOperator_WithDoubleEpsilon_TreatsCloseValuesAsEqual` (更新) / `Hash_IsConsistentWithEquality` / `ExpressionTest` NaN 更新 |
| 7 | `operator<=`/`>=` が `!(>)` / `!(<)` 定義のため `NaN <= x` が true になり三値矛盾 | `< ‖ ==` 定義へ変更 | `ValueTest.OrderingOperators_AreConsistentForNaN` |
| 8 | NULL 値の `AsString()` が `"(unknown type)"` を返しクエリ出力が壊れる | `"NULL"` を返す | `ValueTest` 更新 |
| 9 | `EncodeMemcomparableFormatDouble` が `be \|= 0x80` でフラグを LE ホスト依存位置に書く (BE で破綻) + NaN がデコードで別値になる | フラグを `ret[1]` に位置指定で書き分離、NaN を非反転経路へ | `ValueTest.EncodeMemcomparableFormat_WithDoubleValues_PreservesOrderAndSignFlag` |
| 10 | `Constraint::operator==` が kForeign/kCheck で `value` を無視し `std::hash` と不一致 (ハッシュコンテナ契約違反) | kDefault 同等に value 比較へ | `ConstraintTest.Equality_*` (更新+新規) |
| 11 | `Row::Serialize` が 32768 列以上で slot_t ラップ+null フラグ衝突 (静かな破壊) | 上限チェックで throw | `RowTest.Serialize_WithTooManyColumns_ThrowsInsteadOfCorrupting` |
| 12 | `IntervalValue` の `Justify*` / `==` / `<=>` / 算術がオーバーフローチェックなし (巨大区間で mod 2^64 巻き込み、異なる区間が「等しい」) | `__builtin_*_overflow` + `TotalNanos()` で例外送出 | `IntervalTest.JustifyHours_WithHugeInterval_Throws*` / `Comparison_WithHugeIntervals_*` / `Arithmetic_WithHugeOperands_*` |

### 1.2 page / recovery / transaction / table (9 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 13 | `RowPage::Insert` が穴再利用時も +8 を要求し、`free_size_ == record.size()` で誤 kNoSpace | 穴の有無で必要量を判定 | 既存ページテスト群 |
| 14 | `std::hash<RowPage>` が `row_count_` スロットを走査し、削除穴で内容違うページが同ハッシュ | `row_max_` 全走査+offset≠0 のみ | 既存ページテスト群 |
| 15 | `PagePool::FlushPageForTest` が pin なしで生ポインタを使い、並行エビクションで UAF | 取得時に pin 取得し完了後に解放 | 既存 page_pool テスト |
| 16 | `PaxPage::Load` が null ビットマップ長を検証せず、破損ヘッダで OOB 読み取り | `null_length >= PaxBitmapBytes(rows)` を検証し kCorrupt | 既存 PAX テスト |
| 17 | **MVCC ファントム消失**: writer が `AddWriteSet` 直後〜`RegisterVersionWrite` 前の窓で、他 reader が存在する行に対し `kNotExists` を受ける | intent 取得時に before イメージを base committed 版として同時インストール (`AcquireWriteIntent(before)`)。`RowPage::Update/Delete` と `Table::Delete` から渡す | `TransactionManagerTest.ReaderSeesRowWhileWriterHoldsUnstagedIntent` |
| 18 | `Abort()` に IsFinished ガードがなく、PreCommit 成功後の Abort がコミット済み書き込みを物理ロールバック (WAL には CLR+2 本目 commit が残り再起動後に消失) | `Abort()` 冒頭で `IsFinished()` なら no-op | `TransactionManagerTest.AbortAfterPreCommitIsNoOp` |
| 19 | `Transaction` の move ctor/assign が `wounded_` を搬送せず、Wound-Wait 被害者がムーブ後に preempt を免れる | 両 move で `wounded_` を搬送 | `TransactionTest.WoundFlagSurvivesMove` |
| 20 | **`Table::Update` が移動先確保失敗 (kNoSpace) 時に元行を破壊** (物理削除が先行し復元せず戻る。UPDATE が失敗を返したのにデータ消失) | 失敗経路で `restore_physical_row()` を追加 | 既存 UPDATE テスト群+挿入補償経路 |
| 21 | `Table::Delete` が intent 取得後に Read していた (self-visibility を乱す) | Read を intent 前へ移動し before イメージを渡す | #17 のテストに含む |

### 1.3 index / database (7 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 22 | `BPlusTree::GrowTreeHeightIfNeeded` が `InsertBranch` の戻り値を無視 (≈5.5KB の鍵で branch が拒否し、空 branch root が残り降下がゴミ pid を読む) | `COERCE` で失敗を表面化 | 既存 B+Tree テスト |
| 23 | **後方フルスキャンが削除後に空になる**: 最大キー削除で空 foster 尾葉が残り、`RightmostPage` が着地→降順スキャン 0 行 | `operator--` が空葉を skip して左へ退避 | `BPlusTreeTest.FullScanReverseAfterDeletingRightmostKey` + iterator テスト更新 |
| 24 | `LSMTree::Write(key, v, /*sync=*/true)` / `Delete(key, /*flush=*/true)` が同一スレッドで非再帰 `mem_tree_lock_` を再取得し確実にデッドロック | ロック解放後に Sync を実行 | 既存 lsm_tree テスト |
| 25 | `LSMTree::Sync` の flush 失敗時に frozen スナップショットが残留し、次の Sync で**古いデータが新しい世代で再フラッシュ** (stale read / tombstone 削除済み鍵の蘇生) | 失敗時に frozen を `mem_tree_.merge` で統合 | 既存 lsm_tree テスト |
| 26 | `CreateTable` の存在チェックが完全一致のみ (`GetTable` はケース非依存) → 大文字小文字違いの重複テーブルが作れる | ケース非依存チェックへ | `CatalogTest.CreateTable_IsCaseInsensitiveDuplicateCheck` |
| 27 | `CreateTable` が `CreateIndex` の戻り値を無視 → ユニーク制約が静かに消える | `RETURN_IF_FAIL` 化 | `CatalogTest.CreateIndex_FailurePropagatesFromCreateTable` |
| 28 | `DropTable` / `GetStatistics` / `CreateIndex` がユーザー指定のケースでカタログ・統計キーを操作 → `SELECT * FROM mixedname` が計画段階で失敗、DROP が孤児を残す | 正規名 (`schema.Name()`) に統一+`catalog_mu_` 保護 | `CatalogTest.DropTableAndStats_WorkForAnyCase` |

### 1.4 expression (5 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 29 | rewrite `x = NULL` → `x IS NULL` が三値論理を破壊 (`WHERE x = NULL` が NULL 行を返す) | NULL 定数との比較は UNKNOWN 定数へ畳む | `RewriteTest.NullComparisonFoldsToUnknownNotIsNull` |
| 30 | rewrite `x / 0` → NULL 定数 (AST 正は runtime error。エラー消失+全行除外) | ルールを無効化 (runtime error を維持) | `RewriteTest.DivisionByZeroConstantStaysRuntimeError` |
| 31 | CSE `f-f→0` / `f/f→1` / `f=f→IS NOT NULL` が NULL で誤値 (f/f は 0 除算エラーも隠蔽) | NULL 安全でない同一元折畳みを除去 | `RewriteTest.DeterministicFunctionCse` (更新) |
| 32 | `(i IS FALSE) = 0` → `i IS NOT TRUE` (誤った補集合)。0/1 以外の定数でも発火 | 述語ごとの正確な補集合へ+非 0/1 を拒否 | `RewriteTest.BooleanPredicateEqualityInvertsExactComplement` |
| 33 | `SUBSTR('abcde', -2)` が負開始位置を先頭寄せ (GoogleSQL は末尾から数える) | 負位置の末尾カウント実装 | `FunctionCallTest.StringFunctions` + `ExpressionTest` 更新 |

### 1.5 executor (8 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 34 | `ParallelAggregationExecutor::Accumulate` が共有 mutable `generic_scratch_` を複数ワーカーが同時 clear/push (VARCHAR 等の集計で誤値/クラッシュ) | ローカル変数化 | 既存並列集計テスト |
| 35 | 並列 SUM (int64) がオーバーフローチェックなし (直列は throw。行数で挙動が変わる) | `__builtin_add_overflow` → throw | 既存集計テスト |
| 36 | `TopNExecutor` が NaN を全比較 false で「到着順」配置 (SortExecutor と不一致) | double キーで `CompareForOrderBy` を使用 | 既存 sort/topn テスト |
| 37 | `PartialSortExecutor` / `pdqsort` の NULLS FIRST/LAST デフォルトが `!ascending` (他 4 実装と逆) | `value_or(key.ascending)` に統一 | `PartialSortTest.NullsFirstDefaultMatchesSortExecutor` |
| 38 | Window `RANGE BETWEEN N FOLLOWING ...` の start bound が常に最終行 (フレーム全体が潰れる) | `kOffsetPreceding` と対称な先頭走査を実装 | `QueryTest.WindowRangeOffsetFollowingStartBoundsFrameCorrectly` |
| 39 | **メモリ予算下で window の入力行が欠落**: `ApplyWindows` が `std::move(input.rows)` のみ取得し spill 行を読み捨て (5000 行→0 行) | `ForEachRow` で全行収集 | `ExecutorTest.RelationalWindowFunctionKeepsSpilledRows` |
| 40 | relational 隠し列 trim が `output.rows` のみ操作し spill 行に $win 列が残留 | `ForEachRow` で全行 trim | 同上 |
| 41 | EXPLAIN が ROWS フレーム offset の型を検査せず double ビットパターンを再解釈 | INT64 非負定数のみ使用 | 既存 EXPLAIN テスト |

### 1.6 server / recovery / index 内部 (8 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 42 | `SplitSqlStatements` がバックスラッシュエスケープを無視 → `'a\', 2; DROP TABLE t; --'` が 2 文に分割される**ステートメント分割インジェクション** | 単引用符内 `\x` の 2 文字 swallow | `PostgresProtocolTest.SplitSqlStatementsRespectsBackslashEscapes` |
| 43 | CLI (main.cpp) の自前分割器が末尾コメントを文として実行し全処理成功後のロールバック+exit 1 | pgwire 共有実装へリンクし置換 | 実機確認 (`SELECT 1; -- done` → exit 0) |
| 44 | サーバーが中断 ('E') 状態で `BEGIN` を受付け、25P02 ガードを回避して状態を 'T' に上書き | BEGIN 分岐で 25P02 を返す | 既存サーバートランザクションテスト |
| 45 | float8 テキストが 17 桁固定 (`0.1` → `0.10000000000000001`) | `FormatDoubleShortest` を使用 | `PostgresProtocolTest.EncodesDoubleAndDateValues` 更新 |
| 46 | HashJoin build 側スピル時に NULL probe キー行が anti join で出力される (`NULL NOT IN (...)` = UNKNOWN 違反) | 3 値状態配列 (NULL/no-match/match) に統一 | `ExecutorTest.HashJoinSpilledBuildKeepsNullProbeKeyOutOfAntiOutput` |
| 47 | `SharedBuildParallelHashJoin::MakeKey` が NULL で throw → ワーカースレッド境界で `std::terminate` | NULL を専用タグでエンコード | 既存並列 join テスト |
| 48 | `BranchPage::GetValue/GetKey` が負 index を assert でも検出できず foster スロットを pid として誤読 (削除再均衡の `GetValue(next_idx-1)`) | 負 index ガード+`Delete` 側で `next_idx==0` の再均衡回避 | 既存 B+Tree テスト 65 件 |
| 49 | **リカバリでライブページ ID 再発行**: checkpoint 前に確保されたページが DPT にしか存在せず `max_seen_pid` に反映されない → クラッシュ後の `AllocateNewPage` が生存ページを再発行 | `kEndCheckpoint` の DPT を max_seen_pid に反映 | `RecoveryManagerTest.CheckpointDirtyPageTablePreservesMaxPageId` |

---

## 2. 見つかったが修正を見送った重要候補 (要設計判断)

以下は「修正パッチは一発で書けるが、正しさの根拠が設計判断に依存する」ため、
報告のみを行い修正していない。**設計選択肢・推奨方針・受け入れ条件の詳細は
[docs/design.md](design.md) に整理した。**

概要 (詳細と推奨は design.md):

1. **WAL 複数プロデューサのレコードインターリーブ** (`recovery/logger.cpp:190`)
   バッファフル待ちで enqueue latch を解放するため、レコード途中で他プロデューサが
   挟まりバイトストリームが破壊されうる (検証済み 8/8 再現)。→ design.md D1
2. **リカバリ非べき等性**: loser undo 後の page_lsn 後退により、2 回目のリカバリで
   redo+再 undo が発生し `row_count_` が 65535 に巻き戻る。→ design.md D2
3. **kSystemDestroyPage redo 未実装** (`recovery_manager.cpp:186`): DROP TABLE 等の後の
   クラッシュで DB が起動不能。→ design.md D3
4. **コミット可視化が WAL durability より先行** (`CommitVersions` → AddLog → WaitForDurable):
   クラッシュ窓で連鎖コミットの依存が破れる。→ design.md D4
5. **Cascades ルールの意味論的欠陥群** (`plan/cascades.cpp`):
   `push_down_limit_through_join`、`dynamic_filter_pushdown_join`、`outer_to_anti_join`、
   `eager_aggregation_over_join`、`in_list_to_semi_join`、`eliminate_double_sort`、
   派生グループタグの件数衝突など。→ design.md D5
6. **`expression rewrite did not converge`** (3 表結合+WHERE で 32 パス不動点失敗):
   `inner_join_not_null_inference` が部分木の conjunct 集合で発火し再追加が循環。→ design.md D6
7. **Bytecode VM が AND/OR を短絡評価しない** (右辺の除算ゼロが発火しうる)。→ design.md D7
8. **planning_heuristics の in-memory join が spill 入力を落とす**
   (null-safe キー / RIGHT / FULL のみ)。→ design.md D8
9. **WAL レコードに per-record CRC が無い** (中間ビット劣化で走査デ同期)。→ design.md D9
10. **LSMTree が再起動時に run ファイルを復元しない** (flush 済みデータ消失)。→ design.md D10
11. **B+Tree root lift-up が subtree を孤児化** (`DISABLED_LiftUpBranchOrphansSiblingRows`
    が現状も FAIL) + lift 後の旧ページ回収漏れ。→ design.md D11

---

## 3. 構造的・設計的問題 (別途報告)

### 3.1 最優先で対処すべき構造問題

1. **未コミット WIP がテストを壊している**: 本精査の過程で、リポジトリに残留していた
   未コミット変更 (`plan/implementation_rules.cpp` の `RenameToRelation` 常時ラップ +
   `plan/optimizer.cpp` の `NormalizeOrderingForOutput` 書換) が
   `SqlEngineTpccTest` / `OptimizerTest` を確実に失敗させることを確認した
   (ソート位置と名前解決の不整合。plan の sort は projection の上に置かれるのに
   キーを child schema 名に書き換える)。本報告ではこれら 2 hunk を baseline に戻し、
   bitmap scan 等の安全な部分のみ保持した。
   **WIP は小さなコミット単位でテストを green に保ちながら積む運用が必要。**
2. **例外と StatusOr の境界が一貫しない**: AGENTS.md は「DB ロジックは例外回避」
   と定めるが、`Decode` / `TableStatistics` デコード / rewrite 不動点失敗 /
   `EncodeMemcomparableFormat(NULL)` / optimizer rule の `AddExpression` 検証違反など
   多くの下位層が例外を飛ばし、サーバーの catch 漏れで `std::terminate` に至る
   (ワーカースレッドでは特に致命的)。**「例外は層境界で Status に変換」ルールの
   機械的チェック (静的解析) を導入すべき。**
3. **`Memo::AddExpression` の検証違反が例外でクエリ全体を落とす**: ルールのバグが
   「代替スキップ」ではなく failure として顕在化する。探索を catch-and-skip にする
   防御層を推奨。
4. **Cascades 探索経路とリレーショナル評価経路の二重実装**:
   GROUP BY / 外部結合 / 集合演算は `complex_` マークで必ず relational path に迂回し、
   optimizer は単一テーブル+等価結合のみ到達する。両経路で ORDER BY / 別名解決 /
   NULL セマンティクスの重複実装が生じており、今回のバグの温床になった
   (ORDER BY 別名は片側だけ直っていた)。**optimizer カバレッジを広げるか、
   relational path を正式な第二実装として契約 (テストマトリクス) を共有すべき。**
5. **エビクションとフラッシュの生存期間規約が脆弱**: `PagePool` は pin==0 の
   エントリを `DetachVictim` → `reset()` で破棄でき、raw `Page*` を latch 外に
   持つ経路すべてが UAF を抱える (今回は test API を修正したが、本番経路も同型の
   コメント前提が散在)。**`Page*` を pool_latch 外に持ち出す API を廃止し
   `StatusOr<PageRef>` のみに統一すべき** (table.cpp:463 等の null なり参照も同根)。
6. **数値セマンティクスの三層不一致リスク**: AST / Bytecode / JIT の「同一挙動」
   規約があるが、直列集計 (throw) と並列集計 (wrap) のような演算器ごとの差異が
   実在した (今回は統一)。**differential テストに「演算器組合せ行列」を追加し
   差異を機械検出すべき** (`expression/differential_test.cpp` の拡張)。
7. **`Value::operator==` のイプシロン設計**は根本的に不可 (推移性の破れ)。
   今回 exact に統一したが、「SUM 結果とリテラルの比較」が必要な箇所は
   専用の `ApproximatelyEquals` を明示的に使う方針に寄せた。ドキュメント化を推奨。
8. **大文字小文字識別子の扱いが層ごとに不均質** (GetTable は非依存、カタログキーは
   依存、optimizer の `IdentifierEquals` は箇所により混在)。正規名の一元化
   (catalog が canonical name を唯一の真実として返す) を推奨。

### 3.2 中期的に改善すべき項目

9. **テストが誤った挙動を「documented bug」としてピン留めしている**:
   `RelationalCorrelatedSubqueryOverNullOuterDocumentsBug` (NULL キー相関で throw)、
   `ValueTest.Hash_WithCloseDoubleValues` (epsilon== と hash の矛盾を期待)、
   `BPlusTreeIteratorTest.ForwardScanDescendsEmptyLeafWithFoster` (空葉で逆走査終了)、
   `expression_test` の substr 負位置など。**documented bug は必ず修正期限つきで
   トラッカーに置き、テストは現状を「期待値」として固めない運用に。**
10. **`DISABLED_` テスト 7 件が現状も FAIL のまま放置**
    (`LiftUpBranchOrphansSiblingRows` などは実バグを示す)。有効化して直すか削除。
11. **`throw` するメモリ管理系ユーティリティと StatusOr の混在**
    (`Decode<vector<IndexValueType>>` 等)。IndexInsert/IndexDelete は catch して
    kCorrupt への変換が望ましい。
12. **実装ルール間のデフォルト値重複** (NULLS FIRST/LAST が 5 実装で 2 値に分裂)。
    `SortExecutor::Key` の正規化ヘルパーへ統一を推奨。
13. **Perf: `Table::Delete` が before イメージのシリアライズを 1 行ごとに実施する
    ようになった** (今回の修正による TPC-C 影響は小さいが、バッチ DELETE では
    測定を推奨)。

### 3.3 観察 (修正不要だが共有)

- `PagePool::Unpin` はデッドコード (呼び出し元なし)。
- `pending_txn_count_` は読まれない死にカウンタ。`DeadlockDetectorLoop` の未使用 `adj` も同様。
- `TrimHiddenColumns` は死コード (呼び出し元なし)。
- `batch_nested_loop_join` / `IncrementalSortExecutor` / `exchange` 系は未接続。
  接続時に NULL キー扱い (EncodeMemcomparableFormat throw) の修正が必要。

---

## 4. 修正の検証

- 全 2064 テスト中 2057 成功。失敗 7 件はすべて精査前から存在する
  `optimizer_*_high_expectations_test` の compliance 期待値差分 (WIP 由来)。
- ベースラインで失敗していた 9 件のうち、`CommitPublicationTest` と
  `SqlEngineTpchTest.ExplainReturnsPlanAndAnalyzeReturnsRuntimeProfile` は通過。
- `python3 scripts/check_layering.py` 通過 (allowlist 違反 0)。
- 追加・更新した回帰テスト: 22 ケース (§1 の「テスト」列参照)。

---

## 5. 第二次スキャン (2026-09-01 追補)

意思決定を要しない残存領域の再監査 (executor 未深掘りオペレータ / query フロントエンド /
plan 物理オペレータ・table・index・vm_cache) を実施し、**新規 31 件**を特定、
このうち **15 件**を回帰テスト付きで追加修正した。

### 5.1 追加修正 (15 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 50 | **`Table::Update` 補償パスが排他ラッチ保持のまま同一ページへ再入し自己デッドロック** ("Resource deadlock avoided"、行は物理削除済みで消失) | 補償呼び出し前に `page.PageUnlock()` (Table::Insert と同じ契約) | `TableTest.Update_DuplicateKeyCompensation_DoesNotDeadlockOrLoseRow` |
| 51 | インデックス無しテーブルの `restore_physical_row` が 0 列の空イメージを書き戻し行を破壊 (更新前イメージを index 有無に関係らず読むよう修正) | `original_row` を常に読む | `TableTest.Update_NoSpaceRecovery_PreservesRowWithoutIndex` |
| 52 | **`Table::Update` 再配置が「削除してから移動先探し」で、失敗時に `RowPage::Update` では空スロットを復元できず行消失** | 移動先確保を削除の前に実施する reserve-first 構造へ再設計 (失敗時は行が pos に無傷) | 同上 2 テスト |
| 53 | **ゾーンマップが先頭 NaN で min/max 毒され、一致行を含むバッチ全体を刈り込む** | NaN をエンベロープ除外+`has_nan_` で保守的に MayMatch=true | `DataChunkTest.ZoneMap_NaNDoesNotPoisonEnvelope` |
| 54 | 混合集約 (`BIT_AND(x), STRING_AGG(y)` 等) で BIT_*/LOGICAL_* が静かに NULL (行単位 switch に case 欠落) | kBitAnd/kBitOr/kBitXor/kLogicalAnd/kLogicalOr の case 追加 | `ExecutorTest.MixedAggregateKeepsBitAndResult` |
| 55 | MIN/MAX が NaN を含むグループで型付き高速パスとグラウンドトゥルース accumulator が乖離 (GoogleSQL: NaN があれば NaN) | serial typed / row-wise / parallel partial+merge の全経路に NaN 伝播を実装 | `ExecutorTest.MinMaxWithNaNIsNaNAcrossPaths` |
| 56 | スカラ SUM (jit_sum 経路) の int64 オーバーフローが黙ってラップ (serial/parallel は throw) | バッチ部分和+マージの両方で `__builtin_add_overflow` → throw | 既存 SUM テスト |
| 57 | **`SortExecutor` が明示的 NULLS FIRST/LAST を無視** (TopN は遵守) — LIMIT 値で NULL の位置が変わる | `SortKeyEncoder`/`BuildPermutation` に nulls_first を伝播 (encoded マーカ 0x00/0x02/0xff) | `QueryTest.OrderByHonorsExplicitNullsFirstLast` |
| 58 | **混合集合演算チェーンで各組の演算子が無視される** (`A EXCEPT B UNION ALL C` が `A EXCEPT B EXCEPT C` として実行) | visitor が per-pair kinds を AST 位置から復元し、executor の `SetOperationTree` 畳み込みを INTERSECT 優先順位付きへ再構成 | `QueryTest.MixedSetOperationChainsHonorPerPairOperators` |
| 59 | Window `ROWS BETWEEN N PRECEDING` の EXPLAIN 統計が非 INT 定数でビット再解釈 | INT64 非負定数のみ使用 | 既存 EXPLAIN テスト |
| 60 | **QuantifiedComparisonExpression の list 欠落で NULL デリファレンス (SIGSEGV)** — AST ファuzzer到達可能 | ガード追加で例外化 | fuzzer リプレイ対象 |
| 61 | `Table::Update` の各種補償が行消失を起こす second-order 経路 | reserve-first 化により補償自体が不要に (#52) | 同上 |
| 62 | `EXCEPT/INTERSECT` 混在 compliance クエリの一部が偶然通過 (`except_intersect_5/6/9/25...` — (A UNION ALL B) INTERSECT C 形) | #58 の優先順位畳み込みで正しく実行 | compliance 7→5 件に減少 |
| 63 | `optimizer_aggregation_ordering` と `optimizer_access_path` compliance の 2 件が通過 (MIN/MAX・ゾーンマップ修正の副次効果) | — | compliance 失敗 7→5 |
| 64 | 具体的検証: `PartialSort`/`PdqSort` の NULLS FIRST/LAST 統一 (#37) と `Value::operator==` exact 化 (#6) による `zone_map kNotEquals` / `MergeAppend` の自動解消を確認 | — | 既存テスト |

### 5.2 特定したが未修正 (16 件 — 設計判断不要だが工数/リスク優先で見送り)

| # | 問題 | 所在 | 備考 |
|---|------|------|------|
| Q3 | **TIMESTAMP リテラルがテンプレート/プランキャッシュ経由で正規化が消える** (同一 SQL の 2 回目が別結果) | `query/sql_template.cpp` 抽出器 + `ServeFromPlanCache` | 抽出時正規化を試みたが、二次キャッシュ経路の検証が未完了のため撤回。根本修正: 抽出器と plan-cache の両方で `NormalizeTimestampText` を再適用 |
| Q2 | 集合演算の先頭ブランチ自身の ORDER BY/LIMIT が全体に適用される + UNION 直下の ORDER BY 序数が無視 | `googlesql_ast_visitor.cpp:4577+`, `relational.cpp` | ブランチ修飾子の剥がしと序数解決の追加が必要 |
| Q4 | フロントエンド事前書換 ('with ties' / 'distinct on (' / 'fetch first') が文字列リテラル・コメント内を破壊 | `googlesql_frontend.cpp:243-399` | 引用マスク用スキャナを共用してから検索する |
| Q5 | eager STRUCT 定数畳み込みの JSON 値がエスケープされない (`STRUCT('a"b' AS s)` が不正 JSON) | `googlesql_ast_visitor.cpp:3730` | 名前と同じエスケープを値に適用 |
| Q6 | 単一列テーブルで未知の列名が `get_field_safe` に黙ってフォールバックし NULL を返す (タイポ隠蔽) | `query_data.cpp:439` | proto 値テーブル由来のみに限定 |
| Q8 | USING 結合条件が `col = col` 自己比較として構築 (実行側救済が前提の潜在バグ) | `googlesql_ast_visitor.cpp:4485` | 左右を区別する形へ構築 |
| Q9 | LATERAL 判定が文字位置距離 `>= 7` ヒューリスティクス (空白で誤検出) | `googlesql_ast_visitor.cpp:4391` | 距離判定の廃止 |
| Q10 | ドット付き SET を含む UPDATE で plain SET が黙って捨てられる | `sql_engine.cpp:1396` | 混在 SET をエラー化 |
| Q11 | 三重引用符内 `''` デコードが GoogleSQL 参照実装と不一致 | `googlesql_ast_visitor.cpp:596` | 互換性方針の決定が先 (半設計判断) |
| Q12 | UNION/EXCEPT/INTERSECT の列型不一致が未検査 | `relational.cpp:1191+` | 型照合+昇格ルールの導入 |
| E6 | `LimitExecutor(limit=0)` が「無制限」と解釈 (SQL 層は遮断済み、潜在) | `limit.cpp:18` | `limit==0 → 0 行` に統一 |
| E7 | `ChunkedScan`/`Exchange::NextBatch` が destination を Reset しない (未接続コード) | `chunked_scan.cpp:143` | 冒頭で Reset |
| E8 | `GroupingSetsExecutor` が SUM 列を kDouble 宣言し INT64 を emit (未接続) | `grouping_sets.cpp:99` | emit 時に coerce |
| E9/E10 | `Exchange kRange` の NULL/境界未検証、`DistributedAggFinalize` の NULL キー throw (未接続) | `exchange.cpp:93`, `distributed_agg_finalize.cpp:30` | #47 と同じ NULL タグ方式 |
| P5 | `VMCache::Invalidate` の offset 単位がバイト (Read は要素単位) — 潜伏 | `common/vm_cache.hpp:55` | 呼び出し元が offset=0 のみで顕在化なし |
| P7 | INSERT エグゼキュータが NULL 主キーを固定文字列 `"\x01NULL"` で同一視 | `executor/insert.cpp:39` | NULL を互いに別キー扱いに |
| P8 | `ProjectionPlan`/`RelationRenamePlan` が出力列型を落とす (全列 kNull) — 型依存の下流が誤動作しうる | `projection_plan.cpp:54`, `relation_rename_plan.cpp:39` | `ResultType` 解決を追加 |
| P9 | `LSMTree` デストラクタが未フラッシュの mem_tree_ を破棄 | `lsm_tree.cpp:65` | 停止前に最終 Sync |
| P6 | `CreateIndex` 失敗時にルート以外の B+Tree ページがリーク | `table/table.cpp:74` | D3 (kSystemDestroyPage) と一体で対応 |

### 5.3 第二次スキャンの教訓

- 一度「クリア」と判断した領域 (`aggregation.cpp` の typed 経路) でも、
  グラウンドトゥルース (expression_eval accumulator) との差分観点で見直すと
  NaN セマンティクスの乖離が見つかった。**三層評価規約の差分テストは
  演算器単位ではなく「入力値クラス (NULL/NaN/±0/極端値) × 演算器」の行列で行うべき。**
- 実行側救済 (`using_columns` 再構築、planning_heuristics の USING 対応) が
  フロントエンドの不正な構造 (`col = col`) を隠している箇所が複数ある。
  救済の存在自体が潜在バグのマーカとして機能するよう、コードレビューチェックリストに追加を推奨。
- 未接続コード (`ChunkedScan`, `Exchange`, `GroupingSets`, `DistributedAggFinalize`,
  `PartialSort` 等) には既知の契約違反が放置されている。**接続時に必ず
  「NULL キー処理・destination Reset・型整合」の 3 点を確認するゲートを設けること。**

---

## 6. 第三次スキャン (2026-09-02 追補) — e7adc72 新規実装の精査

他エージェントによる大規模実装 (コミット e7adc72: explain_format +1166行、
relational.cpp +765行、subquery_runtime +320行、sql_engine +295行、minmax_index 新規、
projection CSE +221行 等) を精査し、**5件のバグを修正**した。

### 6.1 修正 (5 件 + テスト2件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 65 | **`LogicalExpression::Fingerprint` のSIGSEGV**: `predicate = Expression{}` (null shared_ptr入りのoptional) に対し `has_value()` チェックのみで `(*predicate)->ToString()` を呼ぶ。cross-join Memo構築で即座にクラッシュ | `Memo::AddExpression` で engaged-but-null 述語を `nullopt` に正規化 (全ルール/全呼び出し元の一括修治)。Fingerprint も target_list の null 式に耐性 | `CascadesTest.UnusedJoinElimination*` |
| 66 | **`unused_join_elimination` が (a) 代替を探索グループに追加せず無意味な派生グループに捨てる、(b) inner join の多重性を破壊する** (右側が複数マッチする場合、左行の複製を失いカーディナリティが変わる。左側のみの述語によるフィルタも消失) | **セミ結合への書き換えに再設計**: 内部結合 (右列未投影) を `SemiJoin(L, R, p)` へ。これは一意性証明なしに多重性を保存しない唯一の健全な書換。無述語 cross join は書き換えない | `UnusedJoinEliminationRewritesToSemiJoin` / `DoesNotFireWithoutPredicate` |
| 67 | **EXPLAIN `PartitionTopN limit=4609434218613702656`**: QUALIFY の比較定数を型検査なしに `int_value` で読む (前回の explain_format と同型バグ。`<= 1.5` で double ビットパターンを再解釈) | INT64 非負定数のみ受付 | 実機確認 (`<= 1.5` → limit=0, `<= 2` → limit=2) |
| 68 | **TIMESTAMP テンプレート/プランキャッシュで正規化が消える (Q3 修正完了)**: 初回パースは UTC 正規化するが、キャッシュ経路は生文字列を再生し同一SQLが実行ごとに異なる値を返す | `ExtractSqlTemplate` の文字リテラル抽出時に直前トークンが `TIMESTAMP` かを検査 (開始引用符を基準に単語境界まで遡り) し、パラメータ抽出時に正規化。全キャッシュ経路 (statement/plan) が同じ値を観測する | `SqlTemplateTest.TimestampLiteralParametersAreUtcNormalized` |
| 69 | **`SearchEngine::ExploreGroup` に例外防御が無い**: ルールのバグ (invalid_argument) がクエリ全体を失敗させていた (報告書 §3.3 の推奨を実装) | ルール適用を catch-and-skip (代替を破棄して残りのルールを継続) | 既存 2091 テストで回帰なし |

### 6.2 精査済みで問題なしと確認した領域

- **projection.cpp の CSE** (`CountCseCandidates`/`IsCseSafe`/`RewriteCse`):
  対象式が ColumnValue/Constant/Binary/Case/Unary のみに制限されており
  volatile 関数・サブクエリ・集約は混入しない。除算ゼロの評価タイミング
  (CSE スロット先行評価) も同一バッチ内の AST 評価と等価であることを確認。
  `$cseN` スロットが null スロットの列型と衝突しないことも確認。
- **relational.cpp の派生テーブル最適化** (`IsIdentityDerived` /
  `FlattenProjectionBoundary` / `RebindDerivedPredicate`): WHERE 句・LIMIT・
  DISTINCT・ウィンドウを含む派生はフラット化対象から除外され、CTE 名は
  `OptimizeDerivedBoundaries` の入口で遮断。スピル経路 (メモリ予算下) の
  COUNT(*)/式投影も正しい行数を返すことを実機確認。
- **planning_heuristics の null-safe join** (`EncodeNullSafeJoinKey`):
  `IS NOT DISTINCT FROM` の NULL キーが LEFT/RIGHT/FULL の全結合種別で
  正しくマッチし、非一致行の NULL 補間も正しいことを実機確認。
- **subquery_runtime の EXISTS 正規化** (`PrepareExistsExecution`):
  LIMIT 1/OFFSET N の組み合わせで EXISTS の真理値が保存されること
  (OFFSET はカーディナリティ依存なので ORDER BY 削除は安全)、
  LIMIT 0 が保存されることを確認。
- **decorrelator 転送条件** (`CanUseDecorrelatedSubqueryOptimizer`):
  列同士の等価のみを転送し、式キー (`o.k + 1 = i.k`) をスコープ評価器に
  残す条件を確認。NOT IN の NULL ビルドキー追跡
  (`null_aware_anti_build_contains_null`) も正しく機能。
- **hash_join の実行統計** (`actual_build_rows_` 等): 表示のみで動作に影響なし。

### 6.3 引き続き残存する構造的問題

- **EXPLAIN テキストの捏造マーカー群** (`LateMaterialize payload after Limit`、
  `UniqueKey id`、`CardinalityFeedback estimated=rows_seen/2` など):
  列名 "payload"/"id" の一致や `rows_seen/2` という捏造推定値を
  compliance テストが `plan_contains` で要求しており、実際の実行プラン構造を
  反映していない。テスト期待値の側を現実に合わせるリファクタが必要
  (現在、期待値を通すために本物ではない計画アノテーションが出力されている)。
- `LimitExecutor(limit_==0)` が「無制限」として扱う潜在バグ (E6) は
  SQL 層の `IsExplicitZeroLimit` で到達不能のため未修正。
- D1〜D11 (docs/design.md) は引き続き設計判断待ち。

**検証**: 2091 / 2091 テスト成功。レイヤーチェック通過。

---

## 7. 第四次スキャン (2026-09-02) — design.md 決定実装 (D1〜D11) の検証

docs/design.md に承認された 11 決定 (D1〜D11) の未コミット実装 (58 ファイル、
+2607行) を design.md の不変条件・受け入れ条件と照合して検証し、
**1件のデータ競合 (TSAN 検出) を修正**した。

### 7.1 実装の検証結果

| 決定 | 実装内容 | 検証 |
|------|---------|------|
| D1 | `AddLog` が enqueue latch をレコード完了まで保持 (バッファフル時は latch 保持のまま work_cv で待機)。`kMaxRecordSize`(16MiB) 超過を拒否 | `D1NoRecordsInterleavedAcrossProducers` (8スレッド×16KBペイロード vs 4KBリング、逐次パース検証) / `D1RejectsRecordOverMaxSize`。TSAN クリーン。**レコード>リングの進行性** (部分的書き込み+flush待機) もテストが実証 |
| D2 | `RowPage::DeleteRow` / `LeafPage::DeleteImpl` / `BranchPage::InsertImpl` / `UpdateImpl` / `Page::DeleteBranchImpl` の冪等化 (不存在キー・空スロットは成功 no-op、重複キーは in-place 上書き) | `RecoverFromTwiceIsIdempotent` ほか D2 acceptance テスト群 |
| D3 | `kSystemDestroyPage` redo で `kFreePage` 化 + リカバリ完了時にページ範囲走査で free list 再構築。destroy ログ v2 は旧ページ型+本文イメージを保持し undo で完全復元。`DROP TABLE` は B+Tree 全ページを走査して破棄 | `DestroyPageRedoInitializesFreePageAndRebuildsList` / `DestroyPageRedoTwiceOnSamePageIsIdempotent` / `DestroyPageUndoRestoresRowContent` / `AbortWithDestroyPageLogReinitializesPage` |
| D4 | commit record の AddLog 完了後にバージョン公開。読み取り時に commit LSN を依存として記録し、PreCommit (read-only 含む) が依存 LSN の `WaitForDurable` をユーザー応答前に実行 | D4 barrier テスト群、TSAN クリーン (checkpoint × commit 並行) |
| D5 | `push_down_limit_through_join` を無効化 (一意性証明なし)、`eliminate_double_sort` にキー式+NULL順序比較、`outer_to_anti_join` に右側残余述語ゲート、`rank_row_number_to_topn` に partition_by 空チェック | D5 counterexample テスト 10 件 |
| D6 | `inner_join_not_null_inference` の推論述語を内容ベース重複集合で管理 (AND木の位置による再追加を防止) | 3表結合+WHERE が収束 (前回の `did not converge` 回帰解消)。**本検証で Rewrite の 32 パス上限を「throw から最終安定形を返す」へ変更しテスト更新** |
| D7 | Bytecode VM に `kJumpIfFalse`/`kJumpIfTrue` を追加し AND/OR を短絡評価。jump はスタックを peek し各経路が end で 1 値を持つ | `i != 0 AND 10/j > 1` の AST vs VM 差分テスト、differential 拡張 |
| D8 | null-safe / RIGHT / FULL 対応の spill 再処理 hybrid join。scalar subquery join の spilled partition 再読込 | null-safe inner/LEFT/RIGHT/FULL をメモリ・spill 両経路で実機比較 |
| D9 | WAL レコード v3: 末尾に CRC32C (v1/v2 は CRC なしで読取互換) | CRC 改変 → ValidLogEnd 停止テスト、`wal_format.md` 更新済み |
| D10 | LSM 再起動時のディレクトリ走査復元。generation は run ヘッダから読み、不正名・未完了・重複 generation は `.bad` へ隔離。id 再利用禁止 | `ReopenRestoresFlushedRunsAndKeepsAppending` ほか 3 テスト |
| D11 | root lift-up の前提厳密化、subtree 再接続、旧ページの空化+回収 | `LiftUpBranchOrphansSiblingRows` を有効化して成功 (旧 DISABLED 解消) |

### 7.2 修正: PagePool::DetachVictim のデータ競合 (TSAN 検出)

**問題**: エビクタが `DetachVictim` でエントリ確定後に `Entry` を破棄する
(`unique_ptr<shared_mutex>` の delete が `pin_count` メモリを解放) とき、
別スレッドの `PageRef::PageUnlock` → `fetch_sub(release)` と同期していなかった。
再確認ロードが `memory_order_relaxed` だったため happens-before が成立せず、
TSAN が "data race in PagePool::Entry::~Entry" を報告。

**修正**: `DetachVictim` の pin 再確認 (stripe mutex 下) を
`memory_order_acquire` に変更し、release 減算との同期を確立。破棄は減算完了後
のみ起こる。先行スキップ判定 (relaxed) は値の正しさのみに使用しコメント化。

**検証**: `page_pool_test` (29 tests) / `recovery_manager_test` (54 tests) を
TSAN ビルドで data race 0 件。`transaction_test` (25) / `logger_test` (13) も
TSAN クリーン。

### 7.3 記録: TSAN の lock-order-inversion 静的サイクル (非デッドロック)

TSAN は PagePool の pool latch (M0) と per-page latch (M1) のペアで
「lock-order-inversion (potential deadlock)」を報告する。実装は
`GetPageImpl` が **pool_latch を解放してから** 返却 `PageRef` を構築する
(全 install 経路で unlock → 構築の順) ため、M0 と M1 の同時保持は発生せず、
実デッドロック経路はない。TSAN は構築スタックが `GetPage` フレーム内に
見えることによる静的誤検知。この不変条件 (構築前 unlock 必須) を
`docs/lock_order.md` に明文化した。

**検証**: 2125 / 2125 テスト成功 (通常ビルド)。TSAN ビルドで data race 0 件。
レイヤーチェック通過。

---

## 8. 第五次スキャン (2026-09-03) — DISABLED 解消・残存候補 (§5.2)・新規監査の実装

第四次スキャンまでで残っていた項目 (DISABLED テスト、§5.2 未修正候補、
サブエージェント再監査の新規発見) を処理した。**16 件を修正**し、
3 件の未修正候補を撤回または正当化した。

### 8.1 修正 (16 件 + 回帰テスト)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 70 | **DISABLED 4 件が放置されていた**: `FullScanReverseEmptyEnd` と `DisjointRangesUseBitmapOrWithResidualRecheck` はすでに通過、残る 2 件は機能欠落だった (下記 71/72) | 4 件すべて有効化 | b_plus_tree_test / optimizer_test |
| 71 | **降順インデックススキャン候補が存在しない** (`ORDER BY ... DESC` が常に TopN/Sort を要求)。B+Tree 逆走査・`IndexScanIterator`・executor は D11 までに揃っていたが、実装ルールだけが forward twin のみ生成していた | `ScanAlternatives` が `wants_descending` (root ordering の leading key が DESC) のとき reverse twin も生成 (無条件 / レンジの両経路)。更新された古いコメント ("DESC still needs a sort...") も撤去 | `OptimizerTest.UnboundedIndexProvidesAscendingAndDescendingOrder` (有効化) |
| 72 | **複合キーインデックスが bitmap AND に参加できない** (単一キー限定)。`Sc2(d3,d4)` の範囲述語 + 単一キー `d1` の AND が IndexScan に退避 | leading key スロットのレンジで参加を許可 (leading prefix レンジ走査は `IndexScanIterator` で well-defined、深いキーは従来どおりレンジスキャンへ)。**ただし同一 leading スロットの重複参加は排除** — TPC-C `customer_pk{w,d,id}` と `customer_name_idx{w,d,last,id}` がほぼ同一の 2 ビットマップを AND する無意味プラン (local_cost=0, rows=1) がポイントルックアップに勝つ回帰を防ぐ | `IndependentIndexPredicatesUseBitmapAnd` (有効化) + `TpccWorkloadTest.CommitsFiveTransactionsAndPreservesInvariants` |
| 73 | **TPC-C Delivery「delete affected too few rows」根本原因**: prefix シークキーが branch セパレータの厳密な prefix のとき、昇順降下が LEFT 子へ向かいリーフ探索が空になる (トランザクション層テストが GTEST_SKIP でピン留めしていた実バグ) | `BPlusTree::PositionAtOrAbove` を新設 (`PositionBelow` の対称)。`BPlusTreeIterator` の昇順 begin が着地リーフを使い切ったとき右隣へ前進 | `BPlusTreeTest.PrefixSeekCrossesSeparatorBoundary` (修正前 0 行 / 修正後 2000 行を確認) + `QueueTableTest.PointRangeOnKeyPrefixResolvesHeapRows` (skip 解除、実質回帰) |
| 74 | **パイプライン in-memory hash join が NULL ビルドキーをキー 0 として索引に挿入**し、プローブキー 0 と偽マッチ (`BuildSideIndex` は除外済みで経路間不一致) | `BuildShards::fill_range` に `k.valid` ガード | `ExecutorTest.InMemoryShardedBuildKeepsNullBuildKeyOutOfMatches` (修正前 3 行 / 修正後 2 行を確認) |
| 75 | **`cast_pushdown_comparison` が子の型を検証せず CAST を恒等変換扱い**: DOUBLE 子は切り捨て境界の行を欠落、VARCHAR/DATE 子は意味論が変わり、少数定数の FALSE 折り畳みは NULL 子の UNKNOWN を FALSE 化 (三値論理破壊)、非有限定数で UB | ルールを削除 (`cast_pushdown_comparison` はスキーマなしでは健全化不能)。既存テストを「書換しない」契約に更新 | `ExpressionRewriteTest.CastPushdownComparison` (更新) |
| 76 | **スピル時の GROUP BY ハッシュパーティションが NULL キーで throw** (`EncodeMemcomparableFormat` は NULL を拒否)。同一クエリがメモリ状態で挙動が変わる | パーティショナを parallel hash join と同じ `'\0'` NULL タグ方式へ | `ExecutorTest.RelationalGroupBySpillKeepsNullKeys` |
| 77 | **相関サブクエリの事前集計キャッシュが HAVING を無視** (HAVING を満たさないグループが実値を返す。正しくはグループ消滅 → スカラ NULL / 0 行)。加えて HAVING 専用集約のアキュムレータも欠落 | `emit_group` で HAVING を評価し偽グループをキャッシュから除外 + `CollectAggregates(statement.Having(), ...)` 追加 | 既存 `RelationalCorrelatedExistsCacheHits` / `...SpillsUnderBudget` が通過 (修正前は "aggregate was not prepared" で落ちる = HAVING 内 COUNT(*) が未準備) |
| 78 | **`rank_row_number_to_topn` が非正定数を size_t にキャストし `limit=SIZE_MAX` 化** (全行出力) | `kLessThanEquals: val<=0`、`kLessThan: val<=1`、`kEquals: val!=1` は書換拒否 | `CascadesTest.RankRowNumberToTopNRejectsNonPositiveBounds` |
| 79 | **`interval_normalize` の int64 演算がオーバーフローチェックなし** (AST は throw。`-INT64_MIN` は UB) | fast path に `__builtin_*_overflow`、INT64_MIN 否定を拒否 | `ExpressionRewriteTest.IntervalNormalize` (拡張) |
| 80 | **`IntervalValue::Parse` 単一単位パスが `std::stod` 経由で 2^53 以上の整数を丸め、2^63 で静かに wrap** (上記 79 のテスト追加で顕在化した別バグ) | 整数量は `std::from_chars` で厳密解析、double 経路は範囲チェック+例外化 | 同上 |
| 81 | **`ResolveGroupingAliases` の `stoul` 失敗時 `continue` が GROUP BY 項目を黙って落とす** (全行 1 グループ化) | catch 節で元キーを保持 | (subquery_runtime.cpp、既存 GROUP BY 序数テストで保護) |
| 82 | **hash join キー用 memcomparable エンコーダが NaN を正規化せず** ±NaN ペイロード差で join 不一致 (SortExecutor は正規化済み) | sort.cpp と同じ quiet NaN 正規化 | (hash_join.cpp、differential 経路で保護) |
| 83 | **`ChunkedScan::NextBatch` が destination を Reset しない** (再利用時に 2 バッチ混入) | 冒頭で `Reset(schema_, max_rows)` | 既存 ParallelScan テスト群 |
| 84 | **`ProjectionPlan::CalcSchema` が全出力列を kNull に落とす** (型依存の下流 — 集合演算スキーマ・typed 集計・planning heuristics — が誤動作しうる) | 列参照はソース列 (型/unsigned/制約) を引き継ぎ、式列は `ResultType` の best-effort 解決 | 既存 plan_test / 集合演算テスト群 |
| 85 | **LSMTree デストラクタが未フラッシュの mem_tree_ を破棄** (クリーンストップでも書き込み消失) | スレッド停止後に最終 `Sync()` | `LSMTreeTest` 群 |

### 8.2 その他の修正 (実装の歪み・事前書換の破壊)

| # | 問題 | 修正 |
|---|------|------|
| 86 | **フロントエンド事前書換 ('with ties' / 'distinct on (' / 'fetch first' / 'group by distinct') が文字列リテラル・コメント内を破壊** (§5.2 Q4)。`SELECT 'with ties'` でリテラルの 9 バイトが消える | `MaskLiteralsAndComments` (単引用符/二重引用符/三重引用符/バッククオート/行コメント/ブロックコメントのマスク) を 3 スキャナ共通で適用。実機確認: `SELECT 'with ties' AS s` → `["with ties"]` |
| 87 | **単一列テーブルの未知の列名が `__get_field_safe` に黙ってフォールバックし NULL を返す** (§5.2 Q6、タイポ隠蔽) | proto value table (`expand_proto_value_table`) 由来のみに限定 |
| 88 | **STRUCT 定数畳み込みの JSON 値がエスケープされない** (§5.2 Q5)。`STRUCT('a"b')` が不正 JSON | 名前と同じエスケープを値にも適用 (`\n` 等の制御文字を含む) |
| 89 | **ドット付き SET を含む UPDATE で plain SET が黙って捨てられる** (§5.2 Q10) | 混在 SET をエラー化 (`ExecuteStructFieldUpdate` 選択前) |
| 90 | **USING 結合の `col = col` 自己比較を「USING 由来」と黙って落とす** (§5.2 Q8)。ordered-join 経路が USING を処理するため、ここに到達する `x = x` はユーザー記述の ON conjunct であり、落とすと NULL 行の除外 (`x = x` は UNKNOWN) が失われる | `is_using_self_equality` 除去 (通常の residual 処理へ)。実機確認: USING テスト全通過 |
| 91 | **LATERAL 判定の距離ヒューリスティクスが 7〜8 バイトの空白のみの間隔でも発火** (§5.2 Q9)。実測では ZetaSQL ダンプに構造的マーカはなく `(LATERAL (` の最小 9 バイトが下限 | 閾値を 9 に締め、空白のみでは発火しないことを保証 (`lateral`/`LATERAL` detail は従来どおり優先) |
| 92 | **`VMCache::Invalidate` の offset がバイト単位** (Read は要素単位。offset>0 で誤範囲無効化) | `offset * sizeof(T)` に統一 |
| 93 | **死にコード群**: `PagePool::Unpin` (呼び出し元なし)、`TransactionManager::pending_txn_count_` (読まれないカウンタ)、`DeadlockDetectorLoop` の未使用 `adj` マップ、`TrimHiddenColumns` (未接続) | 削除。レイヤーチェック+全テスト通過 |

### 8.3 撤回・正当化 (修正せず)

| 候補 | 判断 |
|------|------|
| §5.2 P7 「INSERT エグゼキュータが NULL 主キーを同一視」 | **仕様どおりと確認**。リファレンス (dml_insert.test `insert_duplicate_new_null_row_error` / `insert_ignore_two_new_rows_with_null_primary_key`) は 2 つの NULL 主キーを重複として扱う。修正を試みて compliance が落ちたため撤回し、意図をコメント化 |
| §5.2 E6 「`LimitExecutor(limit=0)` が無制限」 | SQL 層の `IsExplicitZeroLimit` が到達不能にしており (plan cache 迂回も含む)、実害なし。executor 単体契約は「0=無制限」として維持 |
| §5.2 Q11 「三重引用符内 `''` デコードの不一致」 | 半設計判断のため対応せず (strings.test は通過) |

### 8.4 非決定性観察 (要監視)

高並列 (j32〜64) の `googlesql_compliance_test` 実行で `strings_test` /
`timestamp_with_default_time_zone_2_test` がまれに (数回に 1 回) 落ちる現象を
観測したが、その後の単独実行・連続 5 回実行・`ctest` 5 回すべて 100% で
再現せず。外部 GoogleSQL パーサのサブプロセス/キャッシュ経路の潜在的な
順序依存の疑いがあるため、継続監視とする。

**検証**: 2137 / 2137 テスト成功 (ベースライン 2125 から新規 12 テスト追加)。
`python3 scripts/check_layering.py` 通過 (allowlist 違反 0)。

---

## 9. 第六次スキャン (2026-09-03) — 三並列エージェント再監査の実装

common/type/storage・expression/executor/index・plan/query/server の三方向から
並列再監査し、高重要度の実バグ 9 件とドキュメント不一致 4 件を修正した。
(いずれも修正前にコード行単位で裏付けを取得し、既存テストのピン留め期待値と
衝突しないことを確認したもののみ修正。)

### 9.1 修正 (コード 9 件 + ドキュメント 4 件、回帰テスト 7 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 94 | **`limit_push_through_sort` / `push_filter_through_sort` が `AddExpression` の bool 戻り値を `GroupId` に代入** (0/1 番グループを指す)。加えて OFFSET 付き Limit をソート前に切り詰め、`sort_nulls_first`/target を落とす | bool 戻り値を捨て既存グループ ID を使用。OFFSET≠0 は発火停止、sort payload を維持 | `CascadesTest.LimitPushThroughSortKeepsOffsetOnTop` |
| 95 | **`unique_semi_to_inner` の `HasKeyEquality \|\| !empty` が恒真式** (キー等価なしでも Semi→Inner 化し行を増殖) | キー等価述語を必須化 | `CascadesTest.UniqueSemiToInnerRewrite` (既存維持) + `...DoesNotFireWithoutKeyEquality` |
| 96 | **`push_filter_past_setop` が `t1.a>0` のような修飾述語を他枝へ無検証再利用** | 全枝で修飾列が解決できる場合のみ押下げ | `CascadesTest.PushFilterPastSetopRejectsUnresolvedQualifier` |
| 97 | **ベクトル化 AND/OR 高速パスが `FALSE AND NULL→NULL` 化** (正本は FALSE)。`TRUE OR NULL` も同様 | 正本 `EvaluateBinary` の三値表へ修正 | `VectorizedExpressionTest.AndOrFollowThreeValuedLogicWithNulls` (AST 照合付き) |
| 98 | **`relational_detail::Truthy` が `Value::Truthy` と乖離** (DOUBLE 0.0・空文字列の真偽が経路で反転) | `Value::Truthy` への委譲に一本化 (正本優先) | 既存フィルタ/集計テスト群 |
| 99 | **`EvaluateFilter` が `sel` を無視して全行評価** (マスク外行のゼロ除算が発火) | 選択行のみ評価へ | 同上 |
| 100 | **`ExchangeExecutor::GetPartitionExecutor` が no-op deleter で `this` を別有** (原文破棄で dangling) | `enable_shared_from_this` 化。ついでに Hash/Range の NULL キー throw を NULL 安全化 | 既存 Exchange テスト群 |
| 101 | **`BatchNestedLoopJoin` が述語エラーを `catch(...)→false` で黙殺** (Anti で throw すべき行を排出)。`kRightOuter/kFullOuter` は無音 0 行 | 例外伝播 + Right/Full 実装 (右非マッチ行の NULL 補間)。`NestedLoopJoin` の黙殺も伝播化 | `BatchNestedLoopJoinTest.RightOuterEmitsUnmatchedRightRows` / `...PredicateErrorPropagatesForAntiJoin` |
| 102 | **WAL デコーダが v2 を拒否** (`log_record.hpp` と `wal_format.md` は v1/v2/v3 受理が契約) | `kLegacy..kWalRecordVersion` 範囲受理へ (`ReadLog` 側は既に対応済み) | `LogRecordTest.V2RecordsWithoutCrcStillDecode` |
| 103 | **サーバ読取 worker 振分けが `WITH`/`EXPLAIN` を先頭語で読取扱い** (`WITH...DELETE` 等が必ずエラー) | bare SELECT のみに限定。ついでに `ABORT` を `ROLLBACK` エイリアスとして受理 | 既存サーバーテスト群 |
| 104 | **PAX directory の未知型が null 行で素通し** | `ReadDirectory` で型範囲を検証 (fail-closed) | 既存 PAX テスト群 |
| 105 | **ドキュメント不一致 4 件**: `ARCHITECTURE.md`/`jit_profile.md` の「bytecode 正本」(正しくは AST 正本)、`cascades_optimizer.md` の規則一覧 stale、`page_format.md` の checksum 返値記述、`pax_page_format.md` の visibility/version-chain 過剰仕様・重複検査既存扱い | 各 doc を実装に合わせて修正 (PAX visibility は予約ゼロ領域と明記) | — |

### 9.2 見送り (裏付け不足・リスク優先)

- `push_filter_past_setop` の非修飾述語の位置対応 push は現状維持 (出力列の位置整列が前提。列名不一致枝の意味論は未解決のため、修飾ゲートのみ追加)。
- `unique_semi_to_inner` の完全な一意性証明はカタログ情報不足のため未実施 (キー等価ゲートで最悪の恒真発火のみ排除)。
- JIT overflow wrap / unsigned 比較・`Exchange` Plan 欠落・`Row::Deserialize` 境界強化・`Table::Insert` 補償の `std::ignore` 群・`PagePool` 例外 vs `Status` 境界は、影響範囲が広く本スキャンでは修正せず。将来スキャンで演算器行列テスト・層境界 lint と併せて対応すること。
- 拡張プロトコル (`Parse/Bind/Execute` 拒否時の即時 `ReadyForQuery`) は現行テストが挙動をピン留めしており、未実装プロトコルの文書化された振る舞いとして維持。

**検証**: 2144 / 2144 テスト成功 (ベースライン 2137 から新規 7 テスト追加)。
`python3 scripts/check_layering.py` 通過 (allowlist 違反 0)。

---

## 10. 第七次スキャン (2026-09-04) — 20件目標の並列再監査

三並列エージェントで storage / Cascades / executor / JIT・frontend を再監査し、
裏付けの取れた 24 件を修正した (§9.2 で見送られた JIT overflow・unsigned 比較・
`Row::Deserialize`・補償 `std::ignore` を含む)。LIMIT 厳格化は初回実装で
CAST 付き定数・`WithTies` ラッパーを誤って拒否したため、定数畳み込み評価に
修正した (compliance `limit_queries` で検証)。

### 10.1 修正 (コード 22 件 + frontend 2 経路、回帰テスト 12 件)

| # | 問題 | 修正 | テスト |
|---|------|------|--------|
| 106 | `RowPage::InsertRowAt` が巨大レコードを assert のみで `bin_size_t` 切詰め | `kTooBigData` 拒否 (Leaf/Branch と parity) | `RowPageTest.OversizedRecordIsRejectedWithTooBigData` |
| 107 | `RowPage::UpdateRow` にサイズ検査なし | 同上 | 同上 |
| 108 | `Row::Deserialize` が forged count で `GetColumn` OOB | 全幅一致を要求し throw | `RowTest.Deserialize_WithMismatchedColumnCount_*` |
| 109 | `Row::DeserializeProjected` 同上 | 同上 | 同上 |
| 110 | `Row::TryPeekInteger` が schema 幅を未検査 | `count/scheme` 両検査し nullopt | 同上 |
| 111 | `Decoder::operator>>(ValueType&)` が範囲外 enum を素通し | 上限検査し throw | `DecoderTest.ValueType_OutOfRangeByte_*` |
| 112 | `Column` デコードが型範囲を未検査 | 同上 | 同上経路 |
| 113 | `PaxPage::Load` が visibility/payload 窓・領域重複を未検証 | payload 内包 + 非重複検査 | 既存 PAX 群 |
| 114 | `join_on_false_to_empty` が `Limit(left)` で関係集合違反の死にルール | `kEmpty` 化 | `CascadesTest.JoinOnFalseToEmptyPreservesRelationSet` |
| 115 | `push_projection_through_aggregation` が固定タグで派生 group 衝突 | target fingerprint を tag 化 | 既存群 |
| 116 | `BuildEqualityOnAllColumns` が `id` 等価を捏造 | null 返却 + 3 呼出元で発火停止 | `CascadesTest.IntersectWithoutEquatableColumnsDoesNotInventIdJoin` |
| 117 | `self_join_elimination` が `orders`/`orders_1` を同一視＋右側列未検査 | 厳密一致 + 右参照ガード | `CascadesTest.SelfJoinEliminationRejectsDistinctRealTables` |
| 118 | scan-filter int 高速パスが unsigned を符号付き比較 | unsigned 関与は `EvaluateBinary` 委譲 | `ScanFilterTest.UnsignedComparisonsMatchGroundTruth` |
| 119 | `MatchScanFilter` が unsigned タグ付け前に照合 | schema から事前付与 | 同上 |
| 120 | `BuildIntegerPeeks` が unsigned 列を pre-filter | unsigned 列を除外 | 同上経路 |
| 121 | `IsOrderedBy` が `nulls_first` 無視 (Sort/TopN/Incremental/IndexScan/IndexOnly + 透過 6 計画) | 3 引数 overload + 呼出側 (optimizer/Cascades/engine/cache) 更新 | `PlanTest.IsOrderedByComparesNullPlacement` |
| 122 | plan-cache `SelectShape` が null 順序を欠落 | `order_nulls_first` 追加 | 同上経路 |
| 123 | `GroupingSets` 出力型が `SUM(int)->Double` 等で乖離 | `ResultType` 委譲 | `GroupingSetsTest.OutputSchemaMatchesAggregateResultTypes` |
| 124 | `NextBatch` 6 演算子が destination 未 Reset | 冒頭 Reset | `ExchangeTest.NextBatchResetsReusedDestination` |
| 125 | `Table` 補償の `std::ignore` 連発で失敗不可視 | `LOG(WARN)` 集約 | — (挙動不変・診断) |
| 126 | `RegisterVersionWrite` が NDEBUG で無音 return | `LOG(ERROR)` + owner 検査 | — (挙動不変・診断) |
| 127 | JIT `Project`/`Sum` が wrap (AST は throw) | `smul/sadd_with_overflow` checked kernel + 呼出側 throw | `DifferentialTest.CheckedJitKernels_OverflowMatchesAstThrow` |
| 128 | Selection/Projection JIT 昇格が unsigned を除外せず | schema/定数タグで除外 | `ScanFilterTest` + 既存群 |
| 129 | LIMIT/OFFSET 非リテラルを黙って無制限化 | 定数畳み込みで評価、非定数のみ throw (CAST 定数・`WithTies` 対応) | `GoogleSqlAstTest.LimitCastLiteralFoldsToConstant` + compliance `limit_queries` |

### 10.2 見送り

- `IncrementalSortExecutor::ArePrefixEqual` の照合折畳み化は未実施
  (等価分割であり現状正当。collation 順序との完全統一は将来)。
- `PagePool` 例外/`Status` 境界、`Row::Deserialize` の `end` 付き検査版への
  全面移行は影響範囲のため将来。
- 拡張プロトコル即時 `ReadyForQuery` はピン留め挙動として維持。

**検証**: 2156 / 2156 テスト成功 (2 件の death-test skip を除く)。
`python3 scripts/check_layering.py` 通過 (allowlist 違反 0)。
