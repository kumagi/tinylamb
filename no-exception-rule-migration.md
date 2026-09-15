# No-Exception Rule Migration

Google C++ Style Guide に沿い、DB ロジック（ライブラリ層）から例外を全排除し、
`Status` / `StatusOr<T>` による直和型で伝播させる_migration_記録。

## 方針

1. **`Status` にメッセージを持たせる**（現状は `uint8_t` enum で、深い層の
   エラーメッセージは boundary の `catch` で `e.what()` を拾うサイドチャネルに
   依存していた）。enum は `StatusCode` にrenameし、`Status` は code +
   任意メッセージを持つ軽量クラスにする。`Status::kXxx` の綴りは維持。
2. **throw 箇所は 3 分類して処分する**:
   - (a) プログラマミス / 到達不能（`undefined type`、switch default、
     `operator--` 未対応、"not prepared" 等）→ `CHECK` / `ASSIGN_OR_CRASH`
     （Google スタイル的に "fast failure"。DB ロジックの例外ではない）
   - (b) ユーザーデータ起因の実行時エラー（ゼロ除算、整数オーバーフロー、
     cast/DATE/INTERVAL パース失敗、壊れたデータ）→ `StatusOr<>` で伝播
   - (c) 標準ライブラリ由来（`std::get` / `.at()` / `std::stod`）→
     ノースロー版に置換
3. **伝播は bottom-up**: `common -> type -> page -> index -> table -> database
   -> expression -> plan/executor -> query -> server`。レイヤーを跨ぐシグネチャ
   変更はコンパイルエラーを辿って機械lyに潰す。
4. **boundary の try/catch は最後に削除**する（`SqlEngine::Execute/Prepare`、
   postgres_server）。削除後は Status のメッセージがそのまま ErrorResponse に
   乗る。`main.cpp` / `postgres_server_main.cpp` のトップレベル catch は
   Google スタイルでも許容されるため残す。
5. **スコープ外**: `*_test.cpp`、`*_fuzzer*`、`benchmark/`、`main.cpp` は
   例外を使ってよい（gtest 等）。
6. 各フェーズ末に `cmake --build build -j && ctest --test-dir build -j` と
   `python3 scripts/check_layering.py` を通す。
   `expression/differential_test` は AST 意味論の正誤なので特に重要。

## TODO

### Phase 0: 準備
- [x] 現状棚卸し（非テストコード throw ~1064 箇所 / 93 ファイル）
- [x] このドキュメント作成

### Phase 1: Status のメッセージ保持化 (common)
- [x] `StatusCode` enum + `Status` クラス化（`Status::kXxx` 互換維持、
      `ToString`/`operator<<`、message 保持）
- [x] `StatusOr<T>::Value()` / `MoveValue()` の throw を abort 化
- [x] 全コンパイルエラーFix + ctest

### Phase 2: 深層レイヤー
- [x] `common/`（vm_cache 12 他）
- [x] `page/`（page.cpp 22, page_pool.cpp 13 他）
- [x] `recovery/`（logger 6, log_record 6 他）
- [x] `transaction/`（transaction_manager 6）
- [x] `index/`（lsm_detail/sorted_run 10 他）

### Phase 3: type レイヤー
- [x] `type/value.{hpp,cpp}`（55 — 演算子は StatusOr 化 or CHECK 選別）
- [x] `type/date.{hpp,cpp}`（13）
- [x] `type/interval.{hpp,cpp}`（23+5）
- [x] `type/row.cpp`, `type/column.cpp`, `type/type.cpp`, `type/digest` 他

### Phase 4: table / database レイヤー
- [x] `table/table_statistics.cpp`（6 — Add/EstimateCount → CHECK、
      legacy/version デコード → `Decoder::Fail()`。Evaluate const-fold の
      `catch` は Phase 5 で削除予定）
- [x] `table/full_scan_iterator.cpp`（logic_error → sticky GetStatus、
      Phase 2 の ripple で一緒に変換済み）
- [x] `database/`（Phase 2 ripple で変換済み。throw std:: 残存 0）

### Phase 5: expression レイヤー ✅ 完了（2208/2208 green）
- [x] `ExpressionBase::TryEvaluate`（3 オーバーロード）追加、変換済み
      ノードは本体を `TryEvaluate*` に移し `Evaluate` は EXC-SHIM。
- [x] `expression/function_call_expression.cpp`（100）
- [x] `expression/cast_expression.cpp`（SAFE_CAST を TryCastValue に集約）
- [x] `expression/binary_expression.cpp` / unary / in / case / array /
      query / aggregate / column_value / interval / lambda / window
- [x] `expression/rewrite.cpp`（TryRewrite/TryRewriteOnce + arity→CHECK）
- [x] `expression/proto_text.cpp`（Try* 一式 + 旧名 shim）
- [x] `expression/sql_udf.cpp`（Bind→StatusOr, 深度 guard は CheckAvailable）
- [x] `expression/jit.cpp`（kernel 誤用は CHECK、jit_test は EXPECT_DEATH 化）

- **Phase 5 完了時の副次修正（重要）**: `query/sql_engine.cpp` の
  **template cache が nested-DML UPDATE を誤バインド**していた潜在バグを
  修正。`Templatize` はリテラルを出現順（SET の中身が先）で抽出するが
  `BindStatementLiterals` は構造順（SET→WHERE→NestedItems）で戻すため、
  `two_overlapping_updates` の外側 `WHERE primary_key = 10` が最初の nested
  リテラル `1` を受け取ってしまい pk=1 に化けて 0 行 → 期待エラーが
  Success になっていた。AST 変換で語彙が踏火して顕在化。nested-DML を
  template cache から除外（`templatable=false`）して解決。dml_nested 復帰。

### Phase 6: plan / executor レイヤー ✅ 完了
- [x] `executor/detail/expression_eval.cpp`（338）
- [x] `plan/cascades.cpp`（26）他 plan/
- [x] `executor/` 残り（data_chunk 17, spill_file 16, window_eval 14 他）
- [x] 並列 executors の `catch(...)` 集約を Status 伝播に置換

### Phase 7: query / server boundary
- [x] `query/googlesql_ast_visitor.cpp`（122）
- [ ] `query/sql_engine.cpp`（throw 変換済み。残: boundary catch 削除、
       `last_error_` は Status.message へ移行済みか最終確認）
- [x] `query/sql_template.cpp`（6）
- [ ] `server/postgres_server.cpp`（throw 変換済み。残: boundary catch 削除、
       ErrorResponse は Status.message を使用）

### Phase 8: 最終検証
- [x] ctest 全件（2026-09-15: 2404/2404 green、gcc/clang -Wconversion クリーン）
- [x] `python3 scripts/check_layering.py`
- [x] フォーマット（clang-format、2026-09-15: リポジトリ全体で未適用 0 件）
- [x] 残存 throw の棚卸し: ライブラリ層の実 throw は `common/exc_shim.hpp`
      の shim 機構のみ。`recovery/log_record_oracle.cpp` は PBT サポート
      （oracle 内 catch で完結、スコープ外扱い）。テスト/fuzzer/benchmark/
      main.cpp はスコープ外。
- [ ] ドキュメント更新（common/AGENTS.md の "never throws" 記述の実態化）

### Phase 8 メモ（2026-09-15 健全化パス）
- boundary catch（sql_engine / postgres_server）は残置 = Phase 7 未完のまま。
  ExcShimUnwrap 呼び出し（type/expression/plan + executor/detail の
  const-fold 経路）が依然例外を投げるため、shim 全廃までの正常系。
  完了条件は exc_shim.hpp 冒頭コメントの grep 監査を参照。
- `Transaction::CommitWait` は write バリア（fsync ではない）とヘッダに明記。

## 進捗メモ

- **Phase 1 完了**: `Status` を `enum class StatusCode` + メッセージ保持クラスに
  分離。`Status::kXxx` は `static const inline StatusCode` にして既存の
  `Status::kSuccess` 比較を維持（Status と StatusCode の比較は暗黙変換で成立）。
  `StatusError(code, msg)` ファクトリ追加。`ToString(StatusCode)` は
  string_view、`ToString(Status)` はコード + ": メッセージ" を返す。
  `StatusOr::Value()/MoveValue()` の throw を `LOG(FATAL)+abort` に変更
  （debug_test の該当 EXPECT_THROW は EXPECT_DEATH に更新）。
  新規 StatusCode: `kIOError, kInvalidArgument, kRuntimeError`。
- **`common/log_message.hpp`**: Google スタイルの `CHECK(cond)` /
  `CHECK_MSG(cond, msg)` マクロ追加（到達不能/前提崩壊用の abort 経路）。
  これが "例外の代わりに使う fast-failure" の受け皿。
- **Decoder**: sticky な `Fail()/Failed()`（`is_->setstate(failbit)`）追加。
  `Decode<T>()` は `StatusOr<T>` を返す（kCorrupt）。unknown discriminant
  （ValueType/PageType/IndexKey/LogRecord）は throw ではなく failbit に統一。
- **Encoder/SerializeStringView**: サイズ超過は上流のページサイズ/kTooBigData
  チェックが保証する不変条件として `LOG(FATAL)+abort`。
- **Phase 3/4 完了（type 層 + table_statistics, 2208/2208 green）**:
  - 新方針: 失敗しうる型層ロジックはすべて `Try*`（`StatusOr`/`Status`）へ
    移動し、旧 throw API は **EXC-SHIM**（ヘッダ inline +
    `common/exc_shim.hpp::ExcShimUnwrap`）が「失敗→例外」に変換して
    当面の挙動（throw → boundary catch）を維持。Phase 7 で shim を
    grep して全削除する。`.cpp` 内の実 throw は 0（コメントのみ）。
  - `date`: `TryParseDateDays/TryFormatDateDays/TryAddDateIntervalDays`
    (kInvalidArgument)。`interval`: `TryParse/TryToString/TryPlus/TryMinus/
    TryNegate/TryMultiply/TryTotalNanos/TryJustify*`（overflow フラグ方式の
    checked_mul/add で Parse のネストを保ったまま変換）。
  - `value`: `TryArithmetic(BinaryOperation)` に 8 演算を集約
    （div/mod by zero は kIsInfinity、他 kInvalidArgument）、
    `TryDeserialize/TrySkipSerialized/TryEncode/TryDecodeMemcomparableFormat`
    （破損データは kCorrupt）、`TryLess/TryGreater`。内部型タグ不正
    （Size/Serialize/AsString/==/hash/Encode の "undefined type"）と
    `DateDays` の型不一致は CHECK（=aborts）に格上げ。
  - 新規 `Value::CheckSerializable()/Row::CheckSerializable()`
    （varchar>65535 → kTooBigData）。`Table::Insert/Update` が
    シリアライズ前に検証（serdes の FATAL は真に到達不能な不変条件化）。
  - `Row::Deserialize/DeserializeProjected/DecodeMemcomparableFormat` は
    `Try*` 版を新設し旧名は shim。`table_statistics` の legacy/version
    デコードは `Decoder::Fail()` へ（呼び出し側 `Deserialize()` は
    既に failbit を kCorrupt に変換）。`Type::Size` default と
    `Column` デコーダの不正型は CHECK/Fail。
  - 呼び出し側の一時修正: `expression/binary_expression.cpp` の
    "unsupported binary operation" 変換は shim のメッセージ接頭辞で
    starts_with が壊れたため find() に変更（Phase 5 で `TryArithmetic`
    直接呼びに置換予定）。
  - 教訓: **ASSIGN_OR_RETURN を推論 return 型の lambda 内で使うと
    「non-void 関数の末尾到達」で ud2 / stack smashing になる**
    （table.cpp Update 補償 lambda で発生。明示的 Status 分岐に修正）。
    同種パターンは `grep 'ASSIGN_OR_RETURN' 内 lambda` ではなく
    `-Wreturn-type` 警告をビルドログで確認すること。
- **Phase 5 進行中（expression 層）**: `ExpressionBase` に `TryEvaluate`
  3 オーバーロード（`StatusOr<Value>`、デフォルトは旧 `Evaluate` へ
  フォールバック）を追加。変換済みノード: ColumnValue / Unary /
  Interval / WindowFunctionCall / Lambda / Binary / In / Case / Array /
  Query / Aggregate。パターン: 本体は `TryEvaluate*` に移し `Evaluate` は
  EXC-SHIM 化。共有本体は可変長テンプレートヘルパーで 3 オーバーロードの
  重複を解消（`EvaluateMaybeShortCircuited` / `TryMembership` /
  `TryPickBranch` / `TryBuildArray`）。`EvaluateBinary/Unary/
  QuantifiedComparison` は `Try*` 版が本体、旧名は planner/executor の
  const-folding 用 shim。
  - 重要: shim の例外メッセージは **原文ママ**（接頭辞を付けると
    differential_test のメッセージ一致が壊れる。`d + d` の
    "unsupported binary operation" 変換は `TryArithmetic` の
    `GetMessage().starts_with("Cannot do ")` で判定する形にした）。
  - 完了: `cast_expression.cpp`（TryCastValueCore+`TryCastValue` wrapper
    が SAFE_CAST を一箇所で実装、`ValidateIntWidth`→Status、外部
    `CastValue` 名前はない）、`function_call_expression.cpp`（100 throw
    →`ExecuteFunction/FormatFunction/AddOrSubInterval/TryProtoTextScalar/
    TryStructSetField/TryEncodeStructMemberJson/ProtoTextExtractFieldShim`
    を StatusOr 化、旧エクスポート名 `StructSetField/
    EncodeStructMemberJson` は shim、Evaluate x3 → TryEvaluate x3 + shim、
    IFERROR/ISERROR/NULLIFERROR の try/catch は Status 判定に変換）。
  - 残: `rewrite.cpp`(10: "expression too deep"/proto arity —
    StatusOr 再帰化が必要), `proto_text.cpp`(10), `bytecode.cpp`(4),
    `jit.cpp`(5), `sql_udf.cpp`(2), 各ノードの ResultType/Validate の
    throw（planning 経路、Phase 6 でまとめて StatusOr 化予定）。
- **Phase 2 変換済み API（呼び出し側 ripple の発生源）**：
  - `Logger::Create()`（ ctor は private、`std::thread` 失敗だけ例外→Status）,
    `AddLog→StatusOr<lsn_t>`, `WaitForDurable→Status`, `Finish→Status`,
    `RaiseIfFailed→CheckFailed→Status`.
  - `PagePool::Create()`, `GetPage/GetPageForRecovery→StatusOr<PageRef>`,
    `WriteBack/ReadFrom→Status`, durability gate は `Status(lsn_t)`.
  - `PageManager::Create()`, `GetPage/AllocateNewPage/GetMetaPage→
    StatusOr<PageRef>`, `DestroyPage→Status`.
  - `Page::DecodeDisk→Status`（magic/version は kCorrupt）, ページ型ディスパッチ
    の "invalid page type" は CHECK 化, `SetLowestValue/PageTypeChange/
    AllocateNewPage→Status/StatusOr`.
  - `Transaction::{AppendLog, Insert*Log, Update*Log, Delete*Log, Set*Log,
    AllocatePageLog, DestroyPageLog, SetLowFence/HighFence/Foster}→
    StatusOr<lsn_t>`, `Abort→Status`.
  - `TransactionManager::{Compensate*Log, AddLog}→StatusOr<lsn_t>`,
    `Abort→Status`. PreCommit/Abort の try/catch を Status フローに置換.
  - `RecoveryManager::{RecoverFrom, SinglePageRecovery, LogUndoWithPage}→
    Status`（内部 LogRedo→Status, LogUndo→StatusOr<lsn_t>, PageReplay/
    ReplayPagesInParallel→Status）. ReadLog は bool のまま（デコード失敗は
    `dec.Failed()` で torn tail として扱う）。
  - `CheckpointManager::WriteCheckpoint→StatusOr<lsn_t>`,
    `WriteMasterRecord→Status`.
  - `BPlusTree::Open(txn, root)→StatusOr<BPlusTree>`（旧 ctor）,
    FindLeaf/FindLeafFromHint/FindLeafReadOnly/Leftmost/RightmostPage→
    `StatusOr<PageRef>`, FollowFosterChain/ReclaimIfOrphaned/GrowTree→Status,
    PositionAtOrAbove/PositionBelow→`StatusOr<bool>`.
  - `BPlusTreeIterator`: ページ取得失敗を sticky `GetStatus()` で保持、
    IsValid() は invalid に、Key/Value は失敗時 {}。ループ後に GetStatus() で
    "枯渇" と "IO 破損" を区別する契約（ヘッダコメント記載）。
  - `MetaPage::{AllocateNewPage→StatusOr<PageRef>, DestroyPage→Status}`.
  - VMCache/VMCacheImpl/Cache/BlobFile: `Create()→StatusOr<unique_ptr>`,
    Read/Copy/ReadAt→`Status`/`StatusOr`, pread/mmap/fstat 失敗は Status。
    未知ページ状態/UnfixPage/SanityCheck は CHECK。
  - LSM: `LSMTree::Create→StatusOr<unique_ptr>`, `Restore→StatusOr<SortedRun>`,
    BlobFile/SortedRun/LSMView の Read/Find/Iterate/Write/Sync/MergeAll/
    CreateSingleRun → Status/StatusOr。LSMView::Iterator::Begin→
    StatusOr<Iterator>。std::thread 起動失敗のみ例外→Status。
  - `PageStorage::Create / Database::Create → StatusOr<unique_ptr>`（旧 ctor）。
    Database は `std::unique_ptr<PageStorage> storage_` を持つ。
- **std::thread / std::bad_alloc について**: `std::thread` 起動と
  `list::emplace_back`（ページプール）は OS/C++ランタイム由来の例外だけが
  残る。これらは境界で catch して Status にマップしている（ロジック例外は
  廃止）。デストラクタ内の書き戻し失敗は従来通りログのみ。
- **Phase 5 完了（expression 層）**: AST `TryEvaluate`（3 オーバロード）を
  基準実装化、Cast/FunctionCall/Binary/... ノードを `Try*` 本体 +
  `Evaluate` EXC-SHIM に分割。bytecode/jit/sql_udf/rewrite/proto_text も
  `Try*` 化（jit カーネル誤用は CHECK）。differential_test 基準のメッセージ
  文字列は shim で verbatim 維持。

- **Phase 6 完了（plan / executor 層, 2214/2214 green）**:
  - `expression_eval.cpp`（335 throw）: インタプリタ `Evaluate`/
    `EvaluateFunction` を `TryEvaluate`/`TryEvaluateFunction`
    （`StatusOr<Value>`）へ。`common/exc_shim.hpp` の `Evaluate` shim は
    旧名前を維持。意味的 try/catch（IFERROR/ISERROR/NULLIFERROR/
    非比較要素の重複検出）は `HasValue()` 判定に置換。日付キーワード
    （EXTRACT の `DAY` 等）フォールバックは `TryLookup` 失敗時の
    HasValue チェックへ。`TryEvaluateProtected`（EXC-SHIM bridge）で
    サブクエリ/集約/lambda 内部の throw を IFERROR 系が Status として
    観測できるようにした（Phase 7 で shim 除去と同時に不要化）。
  - **`ExecutorBase` に sticky Status 追加**: `GetStatus()/ok()` +
    protected `FailWith(Status)->false` / `FailWithChildOf(child)`。
    `Next()` は EOF と失敗の両方で false を返し、呼び出し側は枯渇後に
    `GetStatus()` で判別（cursor の fail-fast 意味論）。compliance test の
    `Drain` ヘルパもこの契約に合わせて更新。
  - 変換した演算子: sort（RunReader→sticky, encode 未知型→CHECK,
    Materialize→Status, 外部マージ reader 伝播）, hash_join
    （MaterializeOrThrow→bool+sticky, SingleJoin 違反→Status, BuildShards
    未設定→CHECK, 子 sticky 転送）, nested_loop_join（Materialize→Status,
    EvaluatePredicate→StatusOr, 子 sticky 転送）, set_operation
    （schema/型変換→StatusOr<Relation>）, merge_append
    （CoerceTo/KeyValue/Initialize→StatusOr, Before は sticky）,
    aggregation（SUM/AVG オーバーフロー→FailWith, CheckedAdd→StatusOr,
    layout 不整合→CHECK）, projection（int64 演算 overflow→FailWith）,
    DML（insert/update/delete/merge の行 insert 失敗→kConflicts, 主キー
    重複→kDuplicates, ASSERT_ROWS_MODIFIED→kInvalidArgument）,
    max1_row/index_scan/nl_join/generate_series 等。
  - **`Relation` パイプライン → `StatusOr<Relation>`**: LoadSource,
    BuildInput, Join, InnerJoin, HybridHashJoin, LateralExpandRelation,
    Project, FinishQuery, ExecuteQuery, ExecuteRecursiveCte, ApplyWindows,
    AggregateOverFrame/ResolveFrame/EvalAt/BuildWindowOrderLayout/
    ComputeOneWindow（window_eval）。table not found / 再帰 CTE budget /
    未対応ウィンドウ集約などユーザクエリ誤りを Status で伝播。
    空/非対応 schema は実行時検証へ延期（set_operation_plan は最善努力
    schema + 実行時拒否）。境界（RelationalExecutor/GroupedFinish/
    RecursiveCte::Initialize）は一旦 `throw runtime_error(GetMessage())`
    して従来挙動維持 → Phase 7 の boundary catch 削除時に置換。
  - **注意（再発防止）**: `ASSIGN_OR_RETURN` を `ForEachRow` のような
    void ラムダ内で使うと戻り値型衝突で無音 `ud2`（control-reaches-end）に
    なる。VOID ラムダ内では明示 `if (!x.HasValue()){ err=x.GetStatus();
    return; }` を使う。
- **未着手**: query / server 層の throw（googlesql_ast_visitor 117 他）と
  boundary catch 削除、全 EXC-SHIM 除去。

- **Phase 7 完了（query / server / executor 残りを Status・sticky 化, 2218/2218 green）**:
  - `googlesql_ast_visitor.cpp`（throw 117→0: `AstError/AstStatus` +
    `StatusOr` 再帰, `Visit` は `StatusOr<unique_ptr<Statement>>`）。
    `sql_engine.cpp` / `sql_template.cpp`（`Bind*` 群を `StatusOr` 化）/
    `postgres_protocol.cpp`（`ReadUint*`→`StatusOr`, wire overflow→CHECK）/
    `postgres_server.cpp`（output_overflow スティッキー化,
    `StreamSelectResult`→`StatusOr`, 行上限→kTooBigData）。
  - executor 残 `Evaluate` shim 呼び出しを全滅: relational / hash_join /
    window_eval / subquery_runtime / planning_heuristics / scan_filter /
    aggregation / parallel_aggregation / merge / vectorized_expression /
    chunked_scan / topn / sort（`SortKeyEncoder::AppendEncoded` は
    `Status*` 出パラメータ）/ pdqsort・partial_sort・incremental_sort
    （比較関数 = `Status*` 記録 + `Next()` で `FailWith`）/ returning /
    merge_join・parallel_merge_join（`mutable residual_error_`）/
    batch_nested_loop_join（`predicate_error_`）/ apply / index_scan /
    index_only_scan / bitmap_scan / skip_scan_distinct / selection /
    distinct / projection / partial_aggregate・grouping_sets・
    two_phase_distinct_agg（`Materialize`→`Status`）。
  - **スティッキー EOF 転送の穴を塞いだ**: 子が sticky error で枯渇した時
    root まで届くよう `Projection/Selection::NextBatch` で子状態転送、
    `Insert/Update/Delete/Merge` はドレイン後に `GetStatus()` 確認、
    `QueryResult::GetStatus()` を追加し server が SELECT 枯渇後に参照。
  - `expression/function_call_expression.cpp` の lazy 系（IF/IFERROR/
    COALESCE/IFNULL/DATE_ADD）pair/context オーバーロードを `TryEvaluate` 化。
  - `expression/bytecode.*`: `TryEvaluateBatch/TryEvaluateRow`
    （`StatusOr`）を本体化、`EvaluateBatch/EvaluateRow` は EXC-SHIM。
    VM 内算術は `TryEvaluateBinary/Unary`、不正スタックは
    `StatusError(kRuntimeError)`、`AddConstant` 上限・型なしオペランドは
    コンパイル失敗（`nullopt` フォールバック）に変更。`Compile` は
    `TryRewrite` を使用。
  - `plan/optimizer.cpp`: `Rewrite`→`TryRewrite`、
    `EvaluateConstantPredicate`→`TryEvaluateBinary`。
  - `executor/parallel_hash_join.cpp`: worker の `exception_ptr` ブリッジを
    `Status`（kRuntimeError）ラッチに変更、`BuildSharedHashTable/
    ParallelProbe/EnsureMaterialized`→`Status`。
  - `expression_eval.hpp` の `Lookup/Binary/Evaluate` shim とノードの
    `Evaluate` shim の**ライブラリ内呼び出し元はゼロ**（残る呼び出しは
    `*_test`/`*_fuzzer`/benchmark/main のみ。これらの try/catch は規則対象外）。
  - differential_test の `NOT(順序比較)` NaN 不整合は、`Compile` が
    スキーマ認識で `NotComparisonFreeRules` に切替える既存設計を生 AST 側で
    再現する形で解消（書き換え非保存は `ContainsNotOfOrderedDoubleComparison`
    が抑止、二重化していた test 側 fold は撤去）。
- **残存する意図的な throw（方針）**:
  1. `ExpressionBase::ResultType`（2 オーバーロード）と
     `ColumnValue::ResultType` の "column type not found"。
     planning 系約 200 呼び出しの波及が大きく `StatusOr<Type>` 化は別作業へ
     延期。境界（`StaticallyDouble/SideCanBeDouble/BytecodeCompiler::Compile`
     のフォールバック catch 等）で catch して意味決定している。
  2. `common/exc_shim.hpp::ExcShimUnwrap` 自体とノード `Evaluate` shim —
     テスト/ベンチ/main 向けアダプタとして保持（ライブラリ内部からは不使用）。
  3. `std::stoll` 等の C++ ランタイム例外由来 catch（`cast_expression.cpp`,
     `type/interval.cpp`）と raw thread 境界の安全網 catch は許容。
     `*_oracle.cpp` はテストハーネスとして除外扱い。
- **2026-09-13: ユーザーデータ起因の無防護 `std::sto*` を解消（(b) 分類）**:
  `type/interval.cpp` の INTERVAL 数値成分、`expression_eval.cpp` の
  DATETIME/TIME コンストラクタ・ADD_MONTHS・小数秒 INTERVAL を
  `std::from_chars` ベースの strict パース（`ParseI64Strict` /
  `ParseDoubleStrict`）へ置換し、`StatusError(kInvalidArgument)` で伝播する
  ようにした。scan accepted 入力（`"."`、桁過多）でも例外は上がらない。
- **Phase 8 検証手順**: `cmake --build build -j32` → 
  `ctest --test-dir build -j16 --timeout 120`（2218/2218）→
  `python3 scripts/check_layering.py`（exit 0, allowlisted=63）→
  触れた境界で `clang-format -i`。
