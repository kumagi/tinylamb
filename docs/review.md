# コードベース全体レビュー

2026-08-25 時点。ストレージ / index / table / expression / plan / executor /
query+server の各層を並行レビューし、発見した改善点を重要度順にまとめた。
一部の指摘は実機検証済み(LSM 自己デッドロックの最小再現、
`differential_test` の実行など)。既に [`next-actions.md`](next-actions.md),
[`executor_todo.md`](executor_todo.md), [`optimizer_todo.md`](optimizer_todo.md)
に記載済みの意図的先送り項目は原則除外している。

マーク:

- **[高]** 正確性・クラッシュセーフティ・可用性に直結。早期対処推奨。
- **[中]** 潜在バグ、性能劣化、契約違反。計画的に対処。
- **[低]** 保守性・文書・細粒度な性能改善。

---

## 総評

- **強み**: S3-FIFO/PageRef/ページラッチの並行設計、durability gate による
  WAL 先行書込みの強制、fuzz・クラッシュエミュレートテスト、Cascades memo の
  設計規約(D1〜D6)とガードレールコメント、DataChunk の型付き列ゼロコピー
  gather など土台の品質は高い。AST 評価器は意味論の正本として堅牢で、
  リライト規則の三値論理への配慮も行き届いている。
- **弱み**: (1) ARIES の古典的な綻び(abort 経路の冪等性・ログ→変更の順序・
  終端マーカ)、(2) 「高速パスが正本実装から意味的に漂う」構造リスク
  (直列/並列集約の不一致、Bytecode の短絡評価喪失、JIT のオーバーフロー wrap)、
  (3) 文字列 sniffing に依存する AST 解釈とグローバルなセッション状態、
  (4) スピル予算が入力側のみで出力側が無制限、という4系統に集中している。

---

## 最優先修正候補(横断サマリ)

| # | 領域 | 概要 | 重要度 |
|---|------|------|--------|
| 1 | index | LSM `Write(sync=true)`/`Delete(flush=true)` が自己デッドロック(再現済み) | 高 |
| 2 | recovery | Abort 終端マーカに `kCommit` を流用、補償後クラッシュで二重 undo → ページ破損 | 高 |
| 3 | page | Insert/Update が「ページ改変→WAL 追記」の順で、logger 失敗時にファントム行が永続化 | 高 |
| 4 | recovery | `kSystemDestroyPage` の redo 未実装 → DROP 含む WAL で起動不能 | 高 |
| 5 | table | フルスキャン MVCC 高速パスにダーティリード窓口 | 高 |
| 6 | query | `JOIN ... USING` が `col = col`(自明真)を生成し結合が壊れる | 高 |
| 7 | query | 集合演算が種別に関係なく常に UNION ALL(`UNION DISTINCT`/`INTERSECT`/`EXCEPT` が誤結果) | 高 |
| 8 | query | セッション定数が thread_local で接続間リーク | 高 |
| 9 | executor | ParallelAggregation の `generic_scratch_` データレース | 高 |
| 10 | executor | 直列集約に LOGICAL_AND/OR 未実装・並列 SUM が overflow wrap で DOP により結果が変わる | 高 |
| 11 | expression | `differential_test` が現在赤(int 除算セマンティクスの三者不一致) | 高 |
| 12 | plan | 存在しないテーブルへの SELECT * で `LOG(FATAL)` → サーバ全体が落ちる | 高 |
| 13 | expression | Bytecode/JIT が AND/OR 短絡評価を失い例外抑制が効かない | 高 |
| 14 | plan | インデックスフィルタ経路で選択率を二重計上しプラン品質を系統的に歪める | 高 |
| 15 | 全体 | ルートに 277 個のテストログ散布・`.gitignore` 未対応、legacy parser への全テスト過剰リンク | 中 |

---

## 1. ストレージ層(page/ recovery/ transaction/)

### [高] Abort 終端マーカに `kCommit` を流用しており、マーカ喪失時に二重 undo でページ破損
- 場所: `transaction/transaction_manager.cpp:189`(abort なのに `LogType::kCommit`)、
  `recovery/recovery_manager.cpp:145,219-221`(`kCompensateInsertRow` の redo も
  `kInsertRow` の undo も両方 `DeleteImpl`)、`page/row_page.cpp:235-238`
- 問題: Abort は undowalk 後に `kCommit` マーカを書くだけで `WaitForDurable`
  しないため、「補償レコード flush 済み・マーカ未 flush」の窓(~1ms)でクラッシュすると
  loser 判定され、redo の補償適用後に per-page undo が再度 `DeleteImpl` を実行する。
  `row_count_` アンダーフローと `free_size_` 二重加算がチェックサム付きでフラッシュされる。
- 提案: (a) Abort の最後でマーカを `WaitForDurable`、(b) `RowPage::DeleteRow` を
  offset==0 で no-op にして冪等化、(c) 専用 `kAbort` 型の導入。最低限 (b)+(c) で
  二重適用を構造的に排除。

### [高] `kSystemDestroyPage` の redo が未実装で例外 — DROP 系操作を含む WAL はクラッシュ後必ずリカバリ失敗
- 場所: `recovery/recovery_manager.cpp:186-194`(redo で throw)、`:231-239`(undo も lossy)。
  コメントが参照する `recovery/CODE_REVIEW.md` は存在しない
- 提案: destroy レコードに旧ページタイプ + free-list 差分を持たせるか、redo を
  「free-list push のみ」と定義して最小実装。未実装の間は DDL 経由の `DestroyPage`
  呼び出しをガード。

### [高] Insert/Update 系が「ページ改変→WAL 追記」の順で、logger 失敗時に WAL 無しの改変がフラッシュされる
- 場所: `page/row_page.cpp:86-92`(Insert)、`:171-173`(Update)、`page/leaf_page.cpp:81-82`、
  `page/branch_page.cpp:77-78`、`page/meta_page.cpp:49-50`
- 問題: 変更後に `AddLog` が失敗(ENOSPC 等)すると dirty ページが後日 write-back され、
  WAL に一切記録のない行がリスタート後も残る(MVCC チェーンは消滅済み)。Delete のみ正しい。
- 提案: スペース確保のみ先行し `log → mutate` の順へ統一。mutate 先の場合は
  AddLog 失敗時に旧値復元の補償を書く。

### [中] PreCommit がコミットレコード durability 前にバージョンを公開
- 場所: `transaction/transaction_manager.cpp:108-124`
- 問題: `synchronous_commit=true` でも公開から fsync 完了まで他 txn がコミット済みデータを
  読める。ここでクラッシュすると観測された「コミット」が消失。`AddLog` 例外時も
  `CommitVersions` がロールバックされない。[docs/commit_durability.md](commit_durability.md)
  に記載はあるが sync commit の意味論としては脆弱。
- 提案: `AddLog(kCommit)` → (sync 時) `WaitForDurable` → `CommitVersions` の順へ変更。

### [中] WAL レコード本体にチェックサムが無く、中間ビット反転で以降の全レコードが失われる
- 場所: `recovery/log_record.cpp:706-780`、[wal_format.md](wal_format.md) はページ画像のみ
- 提案: レコード尾部に CRC32C(v2 フォーマット)。`common/crc32c.hpp` を流用可能。

### [中] 部分読み(torn write)時に `validate=true` でも fail-open
- 場所: `page/page_pool.cpp:506-517`(短読み→黙って `PageInit(kFreePage)`)
  [recovery_invariants.md](recovery_invariants.md) の「fail closed」と矛盾
- 提案: `validate=true` かつ `0 < nread < kPageSize` は `kCorrupt` に。EOF 区別は
  `offset+kPageSize <= filesize` で判定。

### [中] CheckpointManager が Transaction フィールドを同期化なしで読む(data race)
- 場所: `recovery/checkpoint_manager.cpp:134-140`
- 問題: `prev_lsn_`/`status_` はワーカスレッドがロックなし更新 → TSAN 違反。
  ATT の last_lsn が途中までしか進まないと undo 漏れのリスク。
- 提案: atomic 化したスナップショット API を用意、または登録時に pair を複製して読む。

### [中] LeafPage::Split が途中失敗すると左右両方に同一キーが残る
- 場所: `page/leaf_page.cpp:271-280`
- 提案: 失敗時に右ページ転記分を補償で消すか、split 全体やり直しプロトコルを
  b_plus_tree 側と明示契約。

### [中] マスターレコード tmp を fsync せず rename(コメントと実装不一致)
- 場所: `recovery/checkpoint_manager.cpp:53-79`
- 提案: open → `::fsync(fd)` → close → rename → dir fsync の順に修正。

### [低] チェックポイントが実質停止 + リカバリが O(ページ数×WAL長)
- 場所: `database/page_storage.cpp:77,98`(`cm_.Start()` 未呼出)、
  `recovery/recovery_manager.cpp:474-502`(壊れたページごとに WAL 全走査)、
  `:751-774`(`ReadLog` が毎レコード 4KB 文字列 + `istringstream`)
- 提案: 起動時 `cm_.Start()`、SPR に `[rec_lsn, valid_end)` を渡す、`ReadLog` の
  バッファ再利用化。

### [低] GetMetaPage が異型メタページを WAL 無しで黙って初期化し直す
- 場所: `page/page_manager.cpp:76-79`
- 提案: type 異常時は `kCorrupt` で fail-closed に。修復はリカバリ経由のみ。

### [低] ドキュメントと実装のドリフト
- 場所: [lock_order.md](lock_order.md)(識別子が実装と不一致)、
  [wal_format.md](wal_format.md)(kAbort 言語 vs kCommit 流用)、
  `recovery/recovery_manager.cpp:191,237`(存在しない CODE_REVIEW.md 参照)
- 提案: 各 doc を実装に合わせ更新し、doc 内識別子を CI grep 検証。

### [低] Logger 終了後のブロック・~PagePool の retired 解放で UAF 可能
- 場所: `recovery/logger.cpp:134-155`(`Finish()` 後も永久待ち)、
  `page/page_pool.cpp:303-315,434`
- 提案: `Finish()` 後は closed 状態で即例外化。~PagePool は pin_count>0 を検出したら
  解放延期。

### [低] RowPage::Insert の空きスロット走査が競合時にスロット数×1ms ブロック
- 場所: `page/row_page.cpp:75-85`
- 提案: 最初は `TryAddWriteSet`(wait=false)で通し、全滅時のみ wait=true。

### [低] BranchPage::InsertImpl が空き容量検証前に `free_size_` を減算
- 場所: `page/branch_page.cpp:82-103`(LeafPage とは不揃い)
- 提案: LeafPage と同じ順序(計算→上限チェック→DeFragment→減算)に揃える。

---

## 2. index / table

### [高] LSM `Write(sync=true)`/`Delete(flush=true)` がセルフデッドロック(実機で確認済み)
- 場所: `index/lsm_tree.cpp:183-191` → `:212`(`mem_tree_lock_` を保持中に同一スレッドが
  `Sync()` で二重ロック。非リエントラントな `std::timed_mutex`)
- 問題: 最小再現コードで 3 秒以上ブロックを確認。fuzzer(lsm_tree_fuzzer.hpp:69,77)は
  50% の確率で踏む経路だが通常テストはデフォルト引数のため潜在化。
- 提案: ロック解放後に `Sync()` を呼ぶようスコープ分離、または `SyncLocked()` 内部関数に分離。

### [高] フルスキャン MVCC 高速パスにダーティリード窓口
- 場所: `table/full_scan_iterator.cpp:65-74`
- 問題: 未コミットのまま停滞したページ変更(PageLSN=X)より新しいコミットが閾値を押し上げると、
  RO トランザクションが version store を経ずに物理行を読む(分離性違反)。
- 提案: ページに最終コミッタ timestamp を別スタンプ、または高速パス前に write intent 登録簿を
  照会して fallback。

### [高] LSM にクラッシュ時リカバリが存在しない(manifest 無し・dir fsync 無し・起動時スキャン無し)
- 場所: `index/lsm_tree.cpp:47-63,237-241`、`sorted_run.cpp:380-384`
- 問題: 再起動で fsync 済み run ファイルも孤児化。memtable WAL も無く Sync 前の書き込みは消失。
- 提案: manifest ファイル(原子置換 + 親ディレクトリ fsync)か起動時 root_dir 走査で run を復元。
  DB 本体統合前の必須作業。

### [中] `SortedRun` コンストラクタの例外が Merger スレッドを `std::terminate`
- 場所: `sorted_run.cpp:229-234`(StatusOr 方針に反して throw)、
  `lsm_tree.cpp:108-117,298`(catch 無し)
- 提案: `static StatusOr<SortedRun> Open()` ファクトリ化 + Merger スレッドに最外殻 catch。

### [中] `SortedRun` が fd を決して close しない(fd リーク)
- 場所: `sorted_run.cpp:229`(close はエラー経路のみ :251-277)。merge/pop 後も開放されず、
  長時間稼働で fd 枯渇
- 提案: RAII fd ラッパまたは参照カウント末尾での close。

### [中] WiscKey 形式なのに blob GC が存在せず blob.db が単調増加
- 場所: `index/lsm_tree.cpp:291-300`、`blob_file.cpp:55-61`
- 提案: マージ時の生存 blob copy-out 型 GC、最低限「最古 2 run の tombstone drop + live-set 再配置」。

### [中] `Read`/`Contains` が mem→disk 遷移と競合しスナップショット不整合 + 三重実装
- 場所: `index/lsm_tree.cpp:119-181`、`lsm_view.cpp:54-65`
- 提案: mem ロック保持中に generation を撮影し新世代 run をスキップ。`Contains` は `Read` で
  実装し `LSMView::Find` に集約。

### [中] `Table::Update` の kNoSpace 経路: 移設失敗時に行が消えたままエラー返却
- 場所: `table/table.cpp:377,395-400,406-409`(`restore_physical_row()`:315-322 があるのに
  失敗枝で未使用。table.cpp:465-484 の TPC-C Delivery 障害コメントと同型の地雷)
- 提案: 移設前に original_image を構築し、全失敗枝で `restore_physical_row()` を呼ぶ。

### [中] BPlusTree::Delete 再平衡で内部メタ領域を誤読する可能性
- 場所: `index/b_plus_tree.cpp:741-749`(`next_idx==0 && RowCount()==1` で
  `size_t(-1)+kExtraIdx=rows_[2]` を pid として解釈)
- 提案: `next_idx == 0` 時は `lowest_page_` 側を foster 親候补に、または branch RowCount()==1 を
  root 折り畳みで潰す不変条件を assert 明文化。

### [低] BPlusTreeIterator の前進停止条件と operator-- の非対称処理
- 場所: `b_plus_tree_iterator.cpp:117-121,153-156`
- 提案: 空ページ時は foster を辿るループ化、`--` 側にも `return *this`。

### [低] 死んだコード・陳腐コメント・統計まわりの細粒度改善
- 場所: `lsm_tree.hpp:89-98`(`FileAndIndex` 未使用)、`lsm_tree_test.cpp:463-471`
  (「DISABLED」だが有効)、`sorted_run.cpp:59-70`(memcmp 逆符号の `MemoryCompare`)、
  `lsm_tree.cpp:218-221`(ファイル名と内部 generation の 1 ずれ)、
  `index_scan_iterator.cpp:136-158,213-220`(value を 2 回 Decode)、
  `table_statistics.cpp:661-690,823-830`(ANALYZE 常時フルスキャン・ヒストグラム線形走査)
- 提案: 削除/改名/Decode 結果の引き回し/ページ単位サンプリング導入/bound 探索化。

---

## 3. expression(三層式エンジン)

### [高] differential_test が現在失敗中 — 整数除算セマンティクスの食い違いが未解決
- 場所: `expression/differential_test.cpp:979-1016` 対 `binary_expression.cpp:167-185`
- 問題: `DetailPathNumericEdgeCasesMatchCanonical` が FAIL を確認。テストは int/int 切り捨てを
  主張するが正規 `EvaluateBinary` は double 昇格(`BinaryResultType` :383-385 も昇格側)。
  コメントの "now resolved" は虚偽。
- 提案: GoogleSQL 準拠(FLOAT64 昇格)を正とし relational_detail 側を委譲 + 期待値修正。
  切り捨てを正とするなら `EvaluateBinary` と `BinaryResultType` を同時変更して三層で再差分。

### [高] Bytecode/JIT が AND/OR 短絡評価による例外抑制を失う
- 場所: `binary_expression.cpp:324-336`(AST は短絡)、`bytecode.cpp:156-169`(両辺評価後に適用)、
  `executor/selection.cpp:171`
- 問題: `i = 5 OR 1/(j-5) > 0` のような行で AST は true を返すが Bytecode は右辺必須評価で
  0 除算例外 → バッチ全体が失敗。differential の論理マトリクスは例外セルを含まず未検出。
- 提案: jump 系オペコードで制御フロー化、または短絡が必要な述語は AST フォールバック。
  「左辺確定 + 右辺 throw」セルを差分テストへ追加。

### [中] JIT projection カーネルがオーバーフローを静かにラップ
- 場所: `jit.cpp:233`(CreateMul/CreateAdd)対 `type/value.cpp:701-717`(AST は builtin_overflow で throw)
- 提案: `llvm.SAddWithOverflow` 系でセンチネルフラグ → AST 再評価フォールバック、または
  eligibility 判定で静的に除外。

### [中] `identity_divide_one` 書き換えが型と精度を変える
- 場所: `rewrite.cpp:530-537`。`x / 1` が INT64 の `x` になり結果型が変わる。
  |x| > 2^53 では丸め落ちで AST/Bytecode 分岐の具体経路
- 提案: 左辺が静的 double の場合のみ適用、differential に `col / 1` セル追加。

### [中] `like_equality` 書き換え × Value 層の「魔法の等価比較」相互作用
- 場所: `rewrite.cpp:625-656` 対 `binary_expression.cpp:292-303`(struct JSON 順不同一致)、
  `type/value.cpp:393-401`(interval 風文字列の正規化一致)
- 問題: LIKE はバイト一致だが `=` に書き換えると特殊等価が発火し真偽が変わる。
- 提案: varchar の意味的等価を `operator==` から分離。応急として JSON/interval 形パターンは
  書き換え対象外に。

### [中] `fold_function` が非決定的関数(CURRENT_TIMESTAMP 等)を畳み込む
- 場所: `rewrite.cpp:282-297`。plan 時と演算子構築時で固定時刻が異なり層間不一致の元。
- 提案: volatile 関数ブラックリスト(current_* )導入。将来的に purity をカタログ管理。

### [中] リライトのホットパスコスト(Match 毎 bindings コピー、ToString 同一性判定)
- 場所: `rewrite.cpp:38-41`、`rewrite.hpp:207`、`rewrite.cpp:789-822`
  (最大32パス×~35ルール×全ノード、`Same` が O(サイズ) 文字列比較)
- 提案: 式ノードに構造ハッシュ導入で `Same` O(1) 化、bindings のロールバック方式化、
  コンパイル済みプログラムのキャッシュ。

### [中] Bytecode VM の型別オペコードが実質未使用・行毎インタプリタの割高
- 場所: `bytecode.cpp:146-179`(全バイナリオペコードが汎用 `EvaluateBinary` 呼ぶだけ)、
  行毎 Value ボックス化(varchar は行毎 `std::string` 新規確保: data_chunk.cpp:218)
- 提案: NULL-free int64 列の生配列直接演算など型特化ループ、ゾーンマップ NULL-free 先解決。

### [低] リライタの throw がクエリ全体を落とし得る
- 場所: `rewrite.cpp:797,804-806`(plan/optimizer.cpp:543 と scan_filter.cpp:219 は catch しない)
- 提案: 非収束・深さ超過時は元の式を返す縮退動作(bytecode.cpp:135-139 と同様)。

### [低] differential_test の網羅性ギャップ
- CAST/SAFE_CAST 全般、JIT SUM カーネル、COALESCE/NULLIF/文字列関数、date±interval、
  AND/OR 短絡×例外セル、IS TRUE 等の単項述語×列参照
- 提案: まず CAST マトリクス(型×NULL×エラーセル)と CompileSum 差分を追加。

### [低] 日時パーサが cast_expression.cpp と function_call_expression.cpp で重複かつ非互換
- 場所: `cast_expression.cpp:44-115`(トリム有り)対 `function_call_expression.cpp:58-119`(トリム無し)
- 提案: 共通ユーティリティへ抽出、CAST vs PARSE_TIMESTAMP 一致セルを差分テストへ。

### [低] LIKE の `_` がバイト単位・double 等価のイプシロン比較(推移律破壊)
- 場所: `binary_expression.cpp:100-125`、`type/value.cpp:403-406`
- 提案: LIKE は UTF-8 文字単位照合へ。イプシロン比較は `ApproxEquals` に分離し
  `operator==` は厳密比較に(GROUP BY/HASH 整合のため)。

---

## 4. plan(Cascades オプティマイザ)

### [高] フィルタ選択率のコスト二重計上(推定行数の過小評価)
- 場所: `plan/implementation_rules.cpp:605-620,970-976`、`plan/cascades.cpp:816-847`
- 問題: index scan 経路で `SelectionPlan`(ctor 内 `TableStatistics::Filter()` で選択率適用済み:
  table_statistics.cpp:892-898)を被せた上にさらに `EmitRowCount()*selectivity` を乗算。
  full scan 側は乗算なし。pushdown ルールは Selection 式を取り除かないため
  selection 実装ルールも再度フル述語選択率を掛ける。index+filter 系代替が体系的に過小評価され
  プラン選択が歪む。
- 提案: covered==false 時は `SelectionPlan` の EmitRowCount をそのまま使用。
  pushdown 後の残余 Selection は「pushed 済 conjunct を除いた述語」にするか、
  子の estimated_rows を上限に clamp。

### [高] 存在しないテーブルへの SELECT * でプロセス異常終了
- 場所: `plan/optimizer.cpp:76-80`(`LOG(FATAL)`)
- 提案: `ASSIGN_OR_RETURN` で Status を返す。入力起因のエラーを FATAL にしない。

### [中] Fingerprint 再計算の O(k²) 直列化と join_enumeration の出現ごと再走査
- 場所: `plan/cascades.cpp:518-522,1152-1167,729-744`
- 問題: 重複検出が既存全式の `Fingerprint()`(AST 全体 ToString)を都度再計算する線形走査。
  join_enumeration は join 式の出現ごとに 2^n ビットマスク全走査 + `CutConnected`。
  n≈10〜16 でプランニング時間が爆発(cap 到達まで悪化)。
- 提案: 式に構築時ハッシュを持たせ二段判定化。join_enumeration をグループ先頭 1 回に限定。

### [中] デコリレーション深さガードが機能していない(デッドコード)
- 場所: `plan/optimizer.cpp:572-577`(インクリメントが即時ラムダ内、実際の再帰 :783 はガード外)
- 提案: インクリメントとガードを再帰呼び出し箇所へ移動。

### [中] PhysicalProperties 派生で wait_for_write_intent が黙って落とされる
- 場所: `plan/cascades.cpp:1207-1227`(kJoin/kAggregation がデフォルト props を返す)
- 提案: 子へ `wait_for_write_intent`/`access_method` は転送し、row_position/ordering/limit_hint
  のみ演算子特性に応じて落とす。

### [中] push_selection_through_join は split_selection_over_join に完全包含され冗長
- 場所: `plan/cascades.cpp:816-835` 対 `:838-847`(適用結果が常に部分集合)
- 提案: 削除するか左側制限の意味を doc/comment 化。

### [中] Pattern マッチ失敗時に capture バインディングが汚染され得る
- 場所: `plan/cascades.cpp:593-612`(any_of 全敗時の挿入取消なし → false negative)
- 提案: capture 検査を読み取り専用にし、挿入は成功確定時に commit。

### [中] AggregationPlan::GetStats が子(集約前)統計を返す
- 場所: `plan/aggregation_plan.cpp:55-57`(出力は最大 1 行なのに数十万行の統計)
- 問題: Aggregation 上位の Selection がこの母数で Filter() を掛け残余 HAVING 付きプランの
  行数推定が桁違いに膨らむ。
- 提案: 自身の EmitRowCount に合わせ ScaleToRows(1)。GROUP BY 対応時に NDV ベース推定へ。

### [低] 空 relations コンジュンクト(mask=0)がメモ内で適用先を失い述語消滅
- 場所: `plan/cascades.cpp:277-292,309-339`(現在は optimizer.cpp:653-662 のガードで回避)
- 提案: Build 内で検知したら throw または root Selection へ自動昇格。

### [低] IndexScan の provided_order が等値固定列を含まず ORDER BY を取りこぼす
- 場所: `plan/implementation_rules.cpp:581-585`、`index_scan_plan.cpp:30-49`
- 問題: `WHERE a=5 ORDER BY a,b`(索引 (a,b))で不要なソート課金。
- 提案: OrderMatches に「等値固定列スキップ可」規則を追加。

### [低] ProjectionPlan::IsOrderedBy の盲目的転送による偽陽性リスク
- 場所: `plan/projection_plan.hpp:52-55`
- 提案: 恒等写像の場合のみ転送。

### [低] Memo::Build 再呼び出しで状態破壊・kRelational のポインタ指纹
- 場所: `plan/cascades.cpp:270-276,239-241`
- 提案: Build 済みフラグで禁止 or 差分更新。指纹にはステートメント通番を使用。

---

## 5. executor

### [高] ParallelAggregation の `generic_scratch_` がワーカースレッド間で競合(データレース)
- 場所: `executor/parallel_aggregation.cpp:159-180,475`(`parallel_aggregation.hpp:80`)
- 問題: `Accumulate()` は input mutex の外で並行呼び出しされるが、共有メンバー vector に
  複数スレッドが同時 clear/push_back → UB/集計破壊。式引数・DISTINCT・実行時型不一致の
  フォールバック集計が 1 つでもあると発火。
- 提案: ローカルスクラッチ(ワーカー毎 1 回アロケーション)へ。TSAN で並列集約テストを回す。

### [高] TableKeyFilter スタッシュがテーブル名単位で自己結合の別エイリアスにも適用され誤結果
- 場所: `executor/detail/planning_heuristics.cpp:884-891,1027-1051`
- 問題: `FROM t AS t1 JOIN t AS t2 ... WHERE t1.x IN (...)` のキー集合が t2 のスキャンにも
  t1 の列で適用され過剰フィルタ。
- 提案: スタッシュキーに alias/qualifier を含める、または同一述語由来のスキャンのみに制限。

### [高] LOGICAL_AND/OR 集約が直列実行で無視され、DOP で結果が変わる
- 場所: `executor/aggregation.cpp:338-351,402-424`(ケース不在)/ `relational_factory.cpp:110-123`
- 問題: visitor(:633)は parse し、ParallelAggregation と AST 基準は実装するが直列 switch に
  ケースがなく常に NULL。行数しきい値前後で同じクエリの答えが変わる(三層一致規則違反)。
- 提案: 直列パスに追加し、NULL 入力の扱いを差分テストで固定。

### [高] 並列 SUM(int64) が overflow を黙って wrap、直列は例外という非対称
- 場所: `parallel_aggregation.cpp:243-257` 対 `type/value.cpp:666-668`
- 提案: バッチ確定时マージを CheckedAdd 相当に統一。

### [中] グローバル予算の CanReserve→Add が TOCTOU、プロセス単位で全クエリ共有
- 場所: `hash_join.cpp:419-432` / `query_memory.cpp:61-76`
- 提案: CAS ベース TryReserve へ。中長期はクエリごと budget lease へ移行。

### [中] ハッシュジョインのスピルが片レベルのみで、パーティション読み出し時のメモリ上限がない
- 場所: `hash_join.cpp:846-884,903-905,1049-1126`
- 問題: Flush 後もパーティションサイズ無検証。スキューで予算無視の OOM。出力も全量常駐。
- 提案: 閾値超過で再分割(再帰的スピル)or ストリーミング読み。キーヒストグラム分割。

### [中] 外部ソートがマージ最終段で全行を再常駐させ、スピルの意味を消す
- 場所: `sort.cpp:517-557`
- 提案: マージ結果のストリーミング返却 or 最終 run 1 つになった時点でファイル直接読み。

### [中] オペレータごとの即席 jthread 生成が QueryScheduler を迂回しネストで過剰購読
- 場所: `hash_join.cpp:746-766,894-914,931-969` / `sort.cpp:313-339` /
  `detail/scan_filter.cpp:343-388`(ParallelAggregation×HashJoin×…で DOP 乗算)
- 提案: QueryScheduler の CPU slot を消費するワーカープールへ統一、DOP 予算の子伝搬。

### [中] 相関サブクエリ preaggregate 経路が空 Row を代表行として評価し範囲外参照の恐れ
- 場所: `detail/subquery_runtime.cpp:397-404`(`SUM(x)+y` のような混在 select 項が通過)
- 提案: aggregate_only 判定にローカル非集約列の存在チェックを追加。

### [中] UNNEST(PROTO) の行幅が行ごとに変わりスキーマと不整合
- 場所: `detail/scan_filter.cpp:458-492`(幅正規化なし、`std::stoll` 失敗は catch(...) 黙殺で 0 埋め)
- 提案: 1 行目スキーマ準拠で幅正規化、パース失敗は警告 or 明示エラー。

### [中] IndexJoin が複合キーの先頭列しか使わず過剰マッチ
- 場所: `index_join.cpp:53-56`(残列の等価検証なし)
- 提案: 残り列の等価チェック追加、または BuildKeyOffsets で複合を禁止して assert。

### [低] EstimateJoinRows が候補ごとに全行 memcomparable エンコードをやり直す
- 場所: `planning_heuristics.cpp:179-215,1088-1089`
- 提案: キー頻度表キャッシュ or NDV ベース閉式估算(table_statistics を利用)。

### [低] ParallelScan の pending 分割コピー・Selection のチャンク再初期化
- 場所: `parallel_scan.cpp:158-167,190-196` / `selection.cpp:124`(`Reset(schema_,max_rows)`
  で確保済み容量を毎バッチ廃棄。Projection は Reset()+Reserve で回避済み)
- 提案: AppendGather 使用、Selection も同様に容量再利用。

### [低] DistinctExecutor の重複集合がメモリ予算に無勘定で無限成長
- 場所: `distinct.cpp:11-23`
- 提案: insert 時課金、超過で sort-based unique へフォールバック(executor_todo の SortDistinct と接続)。

### [低] Window の SUM(int) が uint64 ラップ・分割は O(n²)
- 場所: `detail/window_eval.cpp:393-401,489-499`
- 提案: int64 チェック付き統一、grouping をハッシュマップ化。

---

## 6. query / server

### [高] JOIN ... USING が `col = col` という自明な条件になる
- 場所: `query/googlesql_ast_visitor.cpp:1704-1707`(左右両辺に同一 ColumnValueExp)。
  コメント(:1699-1701)は「shared columns の等価結合」を意図しているが実装が伴っていない
- 問題: `t1 JOIN t2 USING(id)` が「id 非 NULL 行のクロス積」になり結合が壊れる。
- 提案: 左右ソースの修飾子付き `l.id = r.id` を構成。USING 専用中間表現を SelectSource に
  持たせるのが確実。

### [高] CREATE CONSTANT のセッション定数がスレッド生存期間で共有され接続間リーク
- 場所: `type/interval.cpp:19,41-53`(tls_session_constants)/
  `query/googlesql_ast_visitor.cpp:886,2061`
- 問題: サーバはワーカースレッドを使い回すため、ある接続の定数が別接続で参照され
  PathExpression が静かに置換される。同名 `t_named_windows`(:1675-1678)はスコープ復元していて
  対照的。
- 提案: 定数を TransactionContext/セッションオブジェクトへ。最低限ステートメント終了時にクリア。

### [高] サーバの SplitSqlStatements がドル引用符($$/$tag$)非対応
- 場所: `server/postgres_protocol.cpp:264-366` 対 `main.cpp:69-85`(main 側は対応済みで不統一)
- 問題: psql 標準の `SELECT $$a;b$$` で文字列内部分割され断片が別文として実行。
  ブロックコメントネストも未対応。
- 提案: pgwire 版にドルタグ追跡を実装、または main.cpp 側を pgwire ライブラリへ一本化。

### [高] 集合演算が種別に関係なく常に UNION ALL として連結
- 場所: `query/googlesql_ast_visitor.cpp:1652-1665` / `executor/relational.cpp:766-771`
- 問題: `UNION`(DISTINCT)は重複排除されず、`INTERSECT`/`EXCEPT` も和集合。compliance テストに
  UNION 系が見当たらず未検出。
- 提案: op 種別を保持し UNION DISTINCT は Distinct 相当、INTERSECT/EXCEPT は明示的非対応エラーに。

### [中] VARCHAR 中の NUL バイトがワイヤフレーミングを壊す
- 場所: `server/postgres_protocol.cpp:45-65,228-244`(SanitizeField は ErrorResponse のみ)
- 提案: DataRow 送出時の NUL 検知で 22021 エラー応答 or サニタイズ。

### [中] 単独の `9223372036854775808` リテラルが INT64_MIN に化ける
- 場所: `query/googlesql_ast_visitor.cpp:150-152`(-9223372036854775808 用の特別扱いが
  ParseIntLiteral 自体に入っている)
- 提案: 特別扱いを UnaryExpression の "-" 分岐に限定し、ParseIntLiteral は範囲超過エラー。

### [中] クエリ結果を全行 materialize してから送信、backpressure 不成立
- 場所: `server/postgres_server.cpp:800-822,471-478`(Queue が毎回 erase(0,offset) で O(n))
- 提案: RowDescription 先送り + DataRows をチャンク単位で EPOLLOUT backpressure に乗せる。
  Queue は deque<string>/リングへ。

### [中] 認証後入力上限 16 MiB/接続 × max_connections=1024 が DoS 面
- 場所: `server/postgres_server.cpp:61,406-414` / `postgres_server.hpp:18`
- 提案: 接続あたり入力バッファ合計の上限(64 KiB〜1 MiB)を別途設定し超過切断。

### [中] NULL ソート順が PostgreSQL 既定と逆(GoogleSQL 式)
- 場所: `executor/sort.cpp:163-181` / `relational.cpp:290-291,374-376`
  (ASC=NULLS FIRST。pgwire 互換掲げなら PG 既定 ASC=NULLS LAST と逆向け)
- 提案: 規約を明示・文書化。pgwire 互換重視なら NULLS FIRST/LAST パースと共に PG 既定へ。

### [中] 型推論・暗黙キャストの穴
- 場所: `googlesql_ast_visitor.cpp:672-710`(配列要素型が PathExpression 等で INT64 フォールバック)/
  `sql_engine.cpp:752-770` vs `1055-1068`(INSERT は coercion 有り、UPDATE SET は列型検査なし)
- 提案: 要素型は実行時 Value から決めるか非対応エラー。UPDATE も INSERT の coercion を共用。

### [中] 日時・タイムゾーンパースのエラー黙認と三系統重複
- 場所: `googlesql_ast_visitor.cpp:49-99`(sscanf 戻り値未確認、不正 TZ は既定 -8h へ黙って
  フォールバック)、`904-1020`。executor/detail/expression_eval.cpp:644 以降や
  function_call_expression.cpp にも別実装(AST/Bytecode/JIT 差異リスク)
- 提案: パース層(type/date.cpp 等)に一元化、失敗は可視化。タイムスタンプリテラルの差分ケース追加。

### [低] sql_engine.hpp の #endif 後に stray include
- 場所: `query/sql_engine.hpp:126-127`(`<functional>` がガード外)
- 提案: ガード内へ移動。

### [低] VisitExpression 約 630 行の単一関数
- 場所: `query/googlesql_ast_visitor.cpp:881-1514`(リテラル/日時/STRUCT-JSON/区間が混在)
- 提案: 「リテラル系」「日時系」「コンストラクタ系」関数群に分離、JSON エスケープ共通ヘルパ化。

---

## 7. ビルド・リポジトリ衛生・テスト基盤

### [中] リポジトリルートに 277 個のテストログが散布
- 場所: ルート直下の `transaction_test-*.log`(267 件)・`page_pool_test-*.log` 等。
  untracked だが `.gitignore` に `*.log` がなく `git status` が 369 エントリで汚染されている。
- 提案: `.gitignore` に `*.log`(少なくとも `*_test-*.log`)を追加し、既存ログを削除。
  テスト側でログ出力先を build ディレクトリに寄せる。

### [中] 全テストが legacy parser + sql 層へ過剰リンク
- 場所: `CMakeLists.txt:557-570`(add_simple_test)、`:153-162`(`tinylamb_parser` の正体は
  `legacy/parser/*.cpp`)、`:418-423`(test_util が `type/row.cpp` を独自コンパイル →
  `tinylamb_type` アーカイブ内同一 TU とシンボル競合の ODR ハザード)
- 問題: page/recovery 等の下位レイヤーテストまで legacy parser/sql/test_util にリンクされ、
  ビルド時間増とレイヤ違反の隠蔽要因になる。AGENTS.md の「legacy は canonical 実行に不使用」
  と、top-level `parser/` ディレクトリ(legacy parser のテスト置き場)の位置づけも不明瞭。
- 提案: add_simple_test に必要ライブラリ上書き引数を追加。test_util は row.cpp を持たず
  `tinylamb::type` に依存。parser ディレクトリの役割を AGENTS.md/CMake コメントで明文化。

### [低] main.cpp のステートメント分割器が pgwire 版と二重実装
- 場所: `main.cpp:42-101`(自認コメントあり。ドル引用符対応差が既に発生)、stdin 一括読込は
  サイズ無制限
- 提案: pgwire ライブラリへ統合、読み込みはチャンク処理 or 上限付き。

### [低] differential_test / TSAN / fuzz の CI 組み込み
- 本レビューで `differential_test` が赤であることが実機確認できたが、CI がこれを拾っていない。
  §3 の三層一致性問題は TSAN + differential の定期実行で大半が早期検出可能。
- 提案: differential_test・ASAN/TSAN・lsm_tree_fuzzer(sync=true 経路)を CI の定期ジョブへ。

---

## 既知の設計バックログとの関係

本レビューで指摘した「スピル予算の統一プロトコル」(§5)、「Window の正式化」(§5 低)、
「SortDistinct フォールバック」(§5 低)は [`executor_todo.md`](executor_todo.md) の項目と
接続する。一方で:

- Abort マーカ問題(§1)、destroy redo 未実装(§1)、LSM リカバリ不在(§2)、
  USING/集合演算(§6)はバックログ未記載の**新規発見**であり、優先度判断が必要。
- [`optimizer_todo.md`](optimizer_todo.md) のコストモデル改善には §4 の選択率二重計上が
  前提知識として有用。

推奨着手順: (1) §2-1 デッドロックのような即修正可能なもの → (2) differential_test を緑に戻し
例外セルを追加(§3) → (3) WAL 順序・abort 冪等性(§1 高) → (4) USING/集合演算/セッション定数
(§6 高) → (5) 並列集約レースと直列/並列不一致(§5 高) → (6) コストモデル二重計上(§4 高)。
