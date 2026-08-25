# コードベース全体レビュー

- **v1**: 2026-08-25 初版。
- **v2**: 2026-08-26 全面再監査(HEAD `3a945e7` 時点)。コードベースの大幅刷新
  (TPC-C 高速化 `97ea1f8`、clang-tidy/transaction 強化 `0cd8662`、新機能
  `bcc0303`(kArray 型・window 式)、カバレッジ向上)を受けて、v1 の全指摘を
  現行コードと再照合し、新規コードへのレビューを拡充した。

**監査対象に関する重要な注記**: 再監査の実施中にワーキングツリーが変動し、
集合演算(`common/set_operation.hpp`, `executor/set_operation.*`,
`plan/set_operation_plan.*`)・MergeJoin・TopN・MergeAppend・Max1Row・Values
などの新規モジュール群が現 HEAD には含まれなくなっている(reflog 上の
`9423511` および `../tinylamb_tmp` には存在)。これらに対する指摘は
「**§8 復元時必修事項**」に分離して記載する。現 HEAD にのみ存在するコード
(query/plan_cache.hpp, sql_template.cpp 等)の指摘は通常の節に含む。

マーク:

- **[高]** 正確性・クラッシュセーフティ・可用性に直結。早期対処推奨。
- **[中]** 潜在バグ、性能劣化、契約違反。計画的に対処。
- **[低]** 保守性・文書・細粒度な性能改善。

---

## 総論(v2 更新)

- **強み**: v1 で指摘した問題の一部は着実に解消されている(differential_test
  の緑化、fd リーク、Table::Update の補償、B+Tree イテレータ、Cascades の
  capture/predicate 管理、サーバ入力上限、.gitignore 整備など)。新規実行器群
  (TopN・MergeAppend・EmptyPlan/ValuesPlan)の意味論報告契約は概ね堅実で、
  Cascades の provided-order 活用とも整合。MVCC intent/stable timestamp/GC
  並列化の方向性も妥当。
- **弱み**: ARIES 系の構造問題(abort 冪等性・destroy redo・WAL 順序)は
  **15 件中 0 件しか解消しておらず**、さらに `97ea1f8` の高速化が新経路
  (ランタイム undo の page_lsn 回帰、undo 中断時のマーカ書き込み)で同じ根を
  踏んでいる。新機能では window frame の frame_unit 未実装と interval 文字列
  sniffing の比較器侵入が即時の正確性リスク。クエリキャッシュ/テンプレート
  再バインド層には「同一クエリの 2 回目以降で結果が変わる」系の無言バグが
  実機再現済み。集合演算は一時的に正しく実装されたが現 HEAD で失われ
  (= UNION ALL 回帰)、ORDER BY/LIMIT も無言で捨てられる状態に戻っている。

---

## 最優先修正候補(横断サマリ v2)

| # | 領域 | 概要 | 状態 | 重要度 |
|---|------|------|------|--------|
| 1 | recovery/tx | ランタイム Abort の undo が page_lsn を過去へ回帰させ、再起動 redo が適用済み変更を再適用(**新規**) | 新規発見 | 高 |
| 2 | tx/recovery | undo 中断時でも `kCommit` 型マーカを書き、不完全 txn が恒久的 committed 扱い(**新規**) | 新規発見 | 高 |
| 3 | tx | Abort 終端マーカに `kCommit` 流用・補償後クラッシュで二重 undo(v1 #2) | 未修正 | 高 |
| 4 | page | Insert/Update の「ページ改変→WAL」順・logger 失敗時にファントム行(v1 #3) | 未修正 | 高 |
| 5 | recovery | `kSystemDestroyPage` redo 未実装 → DROP 含む WAL で起動不能(v1 #4) | 未修正 | 高 |
| 6 | expression | window frame_unit 未実装: ROWS/GROUPS が RANGE 扱いになり結果が根本的に誤る(**新規**) | 新規発見 | 高 |
| 7 | type/value | interval 文字列 sniffing が比較器・hash に侵入、誤同値/GROUP BY 併合(**新規・悪化**) | 新規発見 | 高 |
| 8 | index | LSM `Write(sync=true)`/`Delete(flush=true)` 自己デッドロック(v1 #1。Flusher 側のみ修正) | 未修正 | 高 |
| 9 | query | 集合演算が UNION ALL に回帰 + ORDER BY/LIMIT/OFFSET/WITH を無言廃棄(**回帰**) | 回帰 | 高 |
| 10 | query | sql_template 再バインドで `union_all_`/`has_limit_` が喪失 → 2 回目実行で枝欠落・LIMIT 無視(実機再現)(**新規**) | 新規発見 | 高 |
| 11 | executor | ParallelAggregation `generic_scratch_` データレース(v1 #9) | 未修正 | 高 |
| 12 | executor | 直列集約 LOGICAL_AND/OR 不在・並列 SUM overflow wrap で DOP により結果変更(v1 #10) | 未修正 | 高 |
| 13 | plan | インデックスフィルタ選択率の二重計上(v1 #14) | 未修正 | 高 |
| 14 | plan | 存在しないテーブル SELECT * で LOG(FATAL)(v1 #12) | 未修正 | 高 |
| 15 | expression | Bytecode/JIT が AND/OR 短絡評価を失う(v1 #13) | 未修正 | 高 |
| 16 | §8 | EXCEPT DISTINCT 多重度誤り・UNION ALL 全 materialize(復元時必修) | 復元時 | 高 |
| 17 | query | `UPDATE t SET a='not-an-int'` が成功しゴミ値を永続化(実機確認)(**新規実害確認**) | 未修正 | 中 |

---

## 前回(v1)からの解決済み項目

再監査で修正を確認できたもの。回帰に注意しつつ、同種の実装を行う際の参照に。

| 分野 | 項目 | 根拠 |
|------|------|------|
| expression | differential_test が赤 → int 除算を FLOAT64 昇格に統一し 13/13 パス(実機確認) | `binary_expression.cpp:167-185` |
| index | `SortedRun` fd リーク → `shared_ptr<VMCache>` own_fd 化で close | `sorted_run.hpp:233`, `common/vm_cache_impl.hpp:100-105` |
| index | B+Tree イテレータの foster 前進停止・`operator--` 非対称 | `b_plus_tree_iterator.cpp:96-150` |
| index | マージファイルパス衝突 → `"merged-"+generation` 化 | `lsm_tree.cpp:266-269` |
| index | IndexScanIterator の value 二重 Decode | `index_scan_iterator.cpp:95-140` |
| table | `Table::Update` kNoSpace 時の行消失 → 全失敗枝で `restore_physical_row()` | `table/table.cpp:315-322,342-408` |
| table | ANALYZE 常時フルスキャン → リザーバサンプリング導入 | `table_statistics.cpp:58-160` |
| storage | WAL に magic/version ヘッダ追加(torn tail 検知向上。payload CRC は未導入) | `log_record.cpp:789-800`, `recovery_manager.cpp:729-747` |
| storage | `UndoLoserChains` に page_lsn ガード・循環ガード(ページ毎 undo は無ガード) | `recovery_manager.cpp:783-801` |
| page | `~PagePool` が pin>0 ページを保護(retired splice) | `page_pool.cpp:420-434` |
| index | LSM Flusher がロック解放後に Sync、CV 待機導入 | `lsm_tree.cpp:87-101` |
| plan | Pattern capture バインディング汚染 → local bindings + 成功時 commit | `cascades.cpp:683-733` |
| plan | mask=0 コンジュンクトの述語消滅 → root 保持(`~uint64_t{0}`) | `cascades.cpp:309-315` |
| plan | ProjectionPlan::IsOrderedBy の盲目的転送 → 列名翻訳付き委譲 | `projection_plan.cpp:82-103` |
| plan | join_enumeration に relation 数 cap(kMaxJoinEnumerationRelations=16)新設 | `cascades.cpp:42-48,792` |
| executor | IndexJoin が複合キーの残列等価を検証 | `index_join.cpp:70-88` |
| executor | ParallelScan の pending 分割コピー → pending split 方式 | `parallel_scan.cpp:157-196` |
| executor | 外部ソート最終マージを RunReader+ヒープでストリーミング化(`rows_` 全蓄積は残存) | `sort.cpp:504-560` |
| executor | memory budget Release の CAS 化 | `query_memory.cpp:80-91` |
| server | 認証前 1KiB + 認証後も累積入力上限で切断 | `postgres_server.cpp:61,407-414` |
| server | 結果行数上限 `kMaxServerResultRows` 新設(単一 string 連結は残存) | `postgres_server.cpp:56,806` |
| server | SplitSqlStatements が行/ブロックコメント・`''` 対応($$/$tag$・ネストは未対応) | `postgres_protocol.cpp:264-366` |
| query | 単独 INT64_MIN リテラル問題の部分修正(UnaryExpression "-" 分岐新設) | `googlesql_ast_visitor.cpp:1219-1223` |
| query | NULLS FIRST/LAST パース導入・既定規約を明記(PG 既定とは不一致のまま) | `statement.hpp:169-171` |
| query | named timezone を `locate_zone` で解決(sscanf 未確認・未知 TZ 黙認は残存) | `googlesql_ast_visitor.cpp:49-99` |
| 衛生 | `.gitignore` に `*.log`/`*.db` 追加、ルートはほぼクリーン | `.gitignore` |
| 衛生 | CI workflows 新設(tsan/fuzzer-nightly 等。differential_test は未組み込み) | `.github/workflows/` |

---

## 1. ストレージ層(page/ recovery/ transaction/)

### 1.1 v1 指摘の状況

**未修正**(行番号は現行):

- **[高] Abort 終端マーカに `kCommit` 流用**: `transaction/transaction_manager.cpp:189`、
  `recovery/recovery_manager.cpp:145,221`、`row_page.cpp:235-242`(DeleteRow 非冪等)。
  `UndoLoserChains` 側のガード追加(:783-801)があるが、ページ毎 undo(`PageReplay`
  :341-352)は無ガードで核心リスクは残存。→ 提案は v1 同様(冪等化 + 専用 `kAbort`)。
- **[高] `kSystemDestroyPage` redo 未実装で throw**: `recovery_manager.cpp:186-194,231-239`。
  参照先 `recovery/CODE_REVIEW.md` は現存しない。
- **[高] 「ページ改変→WAL 追記」順**: `row_page.cpp:86-92,171-173`、
  `leaf_page.cpp:81-82`、`branch_page.cpp:77-78`、`meta_page.cpp:49-64`。
  Leaf/Branch の Update のみ log 先行で不統一。
- **[中] PreCommit が durability 前にバージョン公開**: `transaction_manager.cpp:110` 対
  `:114`。catch(:136-144)でも公開済み version は放置。stable_timestamp 新設後も順序不変。
- **[中] WAL レコード本体に payload チェックサム無し**: magic/version 強化はあったが
  CRC は未使用(`log_record.cpp:789-800`)。
- **[中] torn write 時 fail-open**: `page_pool.cpp:510-517`(ERROR ログ追加のみ、
  validate=true でも `kCorrupt` にならず)。
- **[中] CheckpointManager が Transaction フィールドを無同期読み**:
  `checkpoint_manager.cpp:132-140`。字段更新は所有スレッドがロック外(`transaction.cpp:268`)。
- **[中] LeafPage::Split 失敗時の左右重複キー**: **部分修正**
  (`RETURN_IF_FAIL` 伝播は追加: `leaf_page.cpp:271-280`)。右転記済みキーの補償削除はなく
  txn abort undo 依存のまま。
- **[中] master レコード tmp を fsync せず rename**: `checkpoint_manager.cpp:48-79`
  (:49 のコメントと不一致のまま)。
- **[低] checkpoint 実質停止 + SPR が O(ページ×WAL)**: `page_storage.cpp:71-99`
  (`cm_.Start()` 未呼出)、`recovery_manager.cpp:474-502,751-774`。
- **[低] GetMetaPage が異型メタページを黙って初期化**: `page_manager.cpp:76-78`。
- **[低] doc drift**: `docs/wal_format.md:26`(kAbort 言語)、`docs/lock_order.md:10,20`
  (識別子不整合。下記の LockManager 死蔵でさらに拡大)、
  `recovery_manager.cpp:191,237`(不存在 CODE_REVIEW.md 参照)。
- **[低] Logger `Finish()` 後の永久ブロック**: **部分修正**(pool 側は改善、
  logger 側は残存: `logger.cpp:134-155,157-202`)。
- **[低] RowPage::Insert 空きスロット走査 ×1ms ブロック**: `row_page.cpp:76-84`。
  `TryAddWriteSet` は index_scan(`executor/index_scan.cpp:111`)でしか未使用。
- **[低] BranchPage::InsertImpl が容量検証前に減算**: `branch_page.cpp:88`。

### 1.2 新規発見(97ea1f8/0cd8662 の変更に由来)

- **[高] ランタイム Abort の undo が page_lsn を過去へ回帰させ、再起動 redo が適用済み変更を再適用する**
  - 場所: `recovery/recovery_manager.cpp:309`(`LogUndo` 末尾の無条件 `SetPageLSN(lsn)`)、
    経路 `transaction/transaction_manager.cpp:174-185`
  - 問題: 実行時 Abort は新旧混在ページを undo するたび page_lsn を「undone レコードの
    LSN」へ上書きする。他 txn のコミット済み変更(より新しい LSN)を含むページで flush
    されると、再起動 redo 条件 `page_lsn < lsn` が既適用レコードを全部再適用する。
    (a) コミット済み DELETE 再適用で `row_count_--` がアンダーフロー、(b) leaf INSERT
    再適用で重複キー挿入(重複チェック無し: leaf_page.cpp:86-114)、(c) abort マーカが
    flush 済みなら loser 扱いされずアボート済み行が物理復活し恒久化。
    `full_scan_iterator.cpp:71` の物理直読み前提も崩す。
  - 提案: 実行時 undo では page_lsn を回帰させない(現 page_lsn 維持または補償 LSN で前進)。
    `DeleteImpl` の offset==0 no-op 化で redo 冪等性を確保。
- **[高] undo 中断時でも `kCommit` 型マーカを書き、不完全 txn が恒久的に committed 扱いになる**
  - 場所: `transaction/transaction_manager.cpp:177-181`(ReadLog 失敗で break)+
    `:186-196`(マーカ書き込み)。この break 経路は 97ea1f8 で新設
  - 問題: 「中途半端に undo された txn」が WAL 上は完全コミットに変わり、以後のリカバリで
    修正不能。v1 の「マーカ喪失」とは逆方向の新しい破綻経路。
  - 提案: 専用 `kAbort` 型を導入し、undo 完了の場合のみマーカを書く。中断時は何も書かず
    例外報告。(1.1 の abort マーカ問題と同じ修復筋で解消する)
- **[中] commit ts 割当と RegisterPendingCommit の間の窓で stable_timestamp が未公開 commit を追い越す**
  - 場所: `transaction_manager.cpp:384-385`(fetch_add 後に登録)対 `:355-373`
  - 問題: T1 が ts 取得直後(未登録)、T2 が登録・公開すると unpublished が空になり
    stable が T1 の未公開 ts 込みまで跳ぶ。同一 snapshot 値で可視状態が不一致になる。
  - 提案: fetch_add+登録を `pending_commits_mutex_` 臨界圏内で原子化。
- **[中] AcquireWriteIntent の待機が「1 回限り・1ms」で strict locking のコメントと矛盾**
  - 場所: `transaction_manager.cpp:217-229`
  - 問題: ホルダが複数行更新や fsync で 1ms 超えると競合相手は即 conflict。
    TPC-C 系ワークロードで spurious abort を量産する。
  - 提案: 待ちループ化(デッドライン引数化)か、待機/即断念の期待を呼出側で明示分離。
- **[中] Transaction 放棄時の登録リーク(GC 停滅・intent 滞留)**
  - 場所: `transaction/transaction.hpp:94`(`~Transaction() = default`)、
    `database/transaction_context.hpp:62`
  - 問題: PreCommit/Abort を経ずに破棄されると active_transactions_/write intent が
    永遠残留。oldest_snapshot 固定でバージョンストア単調増加、当該行は全 writer から
    1ms conflict を繰り返す。
  - 提案: ~Transaction で未終了なら Abort 相当を実行、または assert+LOG(FATAL) で早期発見。
- **[低] LockManager が本番コードから完全未使用化(doc ドリフト拡大)**
  - 場所: `transaction/lock_manager.hpp`(参照は fuzzer/test のみ)
  - 問題: 97ea1f8 で TM の lock 呼出が削除。`release_epoch_` の最大 60 秒待機延長は
    自己再入をも 60s ブロックさせる地雷だが死蔵コード化。
  - 提案: 削除 or 「テスト専用」明記 + lock_order.md 更新。
- **[低] Transaction::CommitWait が 1ms ポーリングのまま**
  - 場所: `transaction/transaction.cpp:381-386`。`Logger::WaitForDurable` への委譲へ置換。
- **[低] unstaged intent 保持 txn の同一行読みが snapshot を無視**
  - 場所: `transaction_manager.cpp:284-291`。意図的な仕様(コメントあり)なら doc 化 +
    差分テストで固定。そうでなければ staged 自身の値のみ返す。

---

## 2. index / table

### 2.1 v1 指摘の状況

- **[高] LSM `Write(sync=true)`/`Delete(flush=true)` 自己デッドロック**: **未修正**
  (`lsm_tree.cpp:183-191,193-201` → `Sync()` 内 `:212` で `mem_tree_lock_` 再取得。
  非リエントラント `std::timed_mutex`: lsm_tree.hpp:125)。Flusher 側のみ修正済み(:87-101)。
  fuzzer が踏む経路のまま。
- **[高] フルスキャン MVCC 高速パスのダーティリード窓口**: **未修正(意図的受容を文書化)**
  (`full_scan_iterator.cpp:65-74`)。境界条件の詳細コメント追加。ただし §1.2 の
  page_lsn 回帰バグがこの前提を崩すため、受容判断の再評価が必要。
- **[高] LSM クラッシュリカバリ不在(manifest 無し・dir fsync 無し・起動時スキャン無し)**:
  未修正(`lsm_tree.cpp:47-63`)。
- **[中] `SortedRun` コンストラクタ例外**: **部分修正**(書き込み側は
  `SortedRun::Construct` Status 返却化: sorted_run.cpp:284-384。読み込み用 ctor は
  throw のまま(sorted_run.cpp:228-234)で、catch 無しバックグラウンドスレッドから
  emplace される経路が残存: `lsm_tree.cpp:239,298`)。
- **[中] fd リーク**: **修正済み**(解決済み表参照)。
- **[中] blob GC 不在(WiscKey なのに blob.db 単調増加)**: 未修正(`lsm_tree.cpp:244-301`)。
- **[中] Read/Contains の mem→disk 遷移競合 + 三重実装**: 未修正・軽減のみ
  (`lsm_tree.cpp:119-181`, `lsm_view.cpp:54-65`)。
- **[中] B+Tree 再平衡の内部メタ誤読**: 未修正(`b_plus_tree.cpp:741-752`、
  `GetValue(next_idx-1)` が `next_idx==0 && RowCount()==1` で rows_[-1+kExtraIdx] 解釈)。
- **[低] イテレータ前進/--**: **ほぼ修正済み**。**[低] 死んだコード**:
  部分修正(`FileAndIndex` 未使用 lsm_tree.hpp:89-98、`MemoryCompare` 逆符号
  sorted_run.cpp:59-70 は残存。「DISABLED」陳腐コメント lsm_tree_test.cpp:463-471 も残存)。

---

## 3. expression / type(新機能 kArray・window 式を含む)

### 3.1 v1 指摘の状況

- 除算セマンティクス不一致: **解決済み**(解決済み表参照)。
- **[高] Bytecode/JIT が AND/OR 短絡評価を失い例外抑制が効かない**: 未修正
  (`binary_expression.cpp:324-333` AST は短絡 vs `bytecode.cpp:156-169` 両辺評価、
  `selection.cpp:170-171`)。
- **[中] JIT projection overflow wrap**: 未修正(`jit.cpp:233`)。
- **[中] `identity_divide_one` 書き換えが型を変える**: 未修正(`rewrite.cpp:531-538`)。
- **[中] like_equality × Value 層の魔法の等価**: **悪化**(§3.2 参照。ルール自体は
  `rewrite.cpp:626-656` に残存)。
- **[中] `fold_function` が非決定的関数を畳む**: 未修正(`rewrite.cpp:283-297`)。
- **[中] リライトホットパス(ToString 同一性・32 パス全走査)**: 未修正
  (`rewrite.cpp:37-40,789-798`)。
- **[中] Bytecode 型別オペコード形骸化**: 未修正(`bytecode.cpp:156-169`)。
- **[低] リライタ throw がクエリ落とす**: 未修正(`rewrite.cpp:797,805`;
  `optimizer.cpp:543`・`scan_filter.cpp:219` は catch 無し)。
- **[低] differential 網羅性ギャップ(例外セル・CAST マトリクス)**: 未修正
  (13 TEST 構成のまま)。
- **[低] 日時パーサ重複非互換**: 未修正(`cast_expression.cpp:37-115`(トリム有)vs
  `function_call_expression.cpp:58-119`(トリム無))。
- **[低] LIKE `_` バイト単位・double イプシロン比較**: 未修正
  (`binary_expression.cpp:100-125`, `value.cpp:407-409`)。

### 3.2 新規発見(bcc0303 由来)

- **[高] interval 文字列 sniffing の比較器・hash 侵入で誤同値・順序破壊**
  - 場所: `type/value.cpp:396-406`(==)、`:615-625`(<)、`:646-656`(>)、
    `:861-875`(std::hash)、`type/interval.cpp:240-300`(composite パース)
  - 問題: 「`-` と空白を含み `-` が先」だけの弱ヒューリスティックで、タイムスタンプ
    `"2024-01-15 10:00:00"` 等の通常文字列が INTERVAL 判定される。composite パーサは
    不正入力でも**例外ではなく無言成功**(stoll 失敗 catch して 0 加算)のため、
    `"AA-BB CC"` と `"XX-YY ZZ"` が共にゼロ間隔に正規化され `==` 真・hash 一致 →
    GROUP BY/DISTINCT が異なる値を併合、ソート順も間隔値順に化ける。ISO 'P...' 形式は
    今度は Parse が throw(`interval.cpp:179-202`)。v1 の like_equality 問題が
    bcc0303 で Value 層全体へ拡大した形。
  - 提案: Value 層の sniffing を撤去し INTERVAL 比較は明示キャスト経由に限定。
    応急として Parse 成功時も「区間らしい形式」検証を追加し失敗時はバイト比較へ。
- **[中] ArrayExpression の型強制が宣言型を強制しない**
  - 場所: `expression/array_expression.cpp:29-46`(CoerceArrayElement)、`:56-64`
  - 問題: BOOL 宣言配列に INT64 要素が変換無しで混入。対応表に無い組合せは素通しされ
    混合配列が生成。推論も最初の非 NULL 要素のみ駆動。
  - 提案: default 経路を「キャスト or 型不一致エラー」に。推論は全要素合意方式へ。
- **[中] WindowFunctionCallExpression の ResultType 固定 VarChar と常時 throw**
  - 場所: `expression/window_function_expression.cpp:43-53`
  - 問題: 全ての結果型が kVarChar 固定のためオプティマイザの見積り・型検査・EXPLAIN が
    信じると誤動作。`Evaluate` は常に runtime_error(Status 経路無し)。
  - 提案: 関数種別→結果型マップを用意し、Evaluate は planner が保証した経路でのみ到達。
- **[中] kArray の等価と hash が不一致**
  - 場所: `type/value.cpp:410-412`(==)、`:880-884`(hash)
  - 問題: == は要素の double に 1e-9 イプシロン比較・varchar に sniffing が効く一方、
    hash はビットベース。「等価だが異なるバケット」が GROUP BY/HASH JOIN で重複グループ化。
  - 提案: kArray 専用の正規化比較(厳密比較)を定義し hash と一致させる。
- **[中] kArray 逆シリアルの型バイト無検証と NUL 走査の無制限読み**
  - 場所: `type/value.cpp:303`(Deserialize)、`:340`(SkipSerialized)、`:586` 付近
  - 問題: 生バイトを `static_cast<ValueType>` し破損データで未定義型が得ても検出されない。
    NUL 探し・要素走査にバッファ末端チェックが無く範囲外読み/暴走し得る。
  - 提案: enum 妥当性検証で `kCorrupt` 系エラーに。残バッファ長を受け取る overload 追加。
- **[中] IndexScanIterator::ResolveRow がエラーを行「無し」に変換**
  - 場所: `index/index_scan_iterator.cpp:145-158`(0c1f397 で新設)
  - 問題: 行読み取り失敗(ロック競合等の一時エラー含む)で `Clear()` して反復終了 →
    結果行が静かに欠落し通知されない。
  - 提案: kNotExists 以外は伝播。一時エラーはリトライまたはエラー返却。
- **[低] 複合キー range の NULL 打ち切り契約が不明瞭**: `index_scan_iterator.cpp:38-51`。
  契約コメント + 切断時 DEBUG ログ。
- **[低] ArrayExpression の 3 overload がコピー貼り付け**: `array_expression.cpp:50-114`。
  共通ヘルパへ集約(AST 正本規約上、改修漏れ温床)。
- **[低] LAG/LEAD/NTILE 引数の型無検証**: `window_eval.cpp:663-700`。
  INT64 定数検査+明示エラー。

---

## 4. plan(Cascades オプティマイザ)

### 4.1 v1 指摘の状況

- **[高] インデックスフィルタ選択率の二重計上**: 未修正
  (`implementation_rules.cpp:766-767`(SelectionPlan ctor が Filter() 適用)+
   `:779-781` で `EmitRowCount()*filter_selectivity` 再乗算。full scan 経路 :797-812 は単回)。
  index+filter 系が sel² で系統的に過小評価される。
- **[高] 存在しないテーブル SELECT * で LOG(FATAL)**: 未修正(`optimizer.cpp:84-86`、
  コメント付きで意図的に継続)。入力起因エラーでプロセスごと落ちるのは DoS 相当。
- **[中] Fingerprint O(k²)**: 部分(join cap 新設。文字列再構築は継続:
  `cascades.cpp:225-244,589-592`)。
- **[中] デコリレーション深さガード**: 部分(RAII `DepthGuard` 化: optimizer.cpp:158,649-653
  だがインクリメント位置は旧指摘と同型で semi-join 構築(:897-910)を跨がない疑い)。
- **[中] PhysicalProperties で wait_for_write_intent 落下**: 部分(TopN/pass-through は
  転送: cascades.cpp:1896-1921。Join/Aggregation はデフォルト props のまま :1873-1883)。
- **[中] push_selection_through_join 冗長**: 未修正(`cascades.cpp:917-948`)。
- **[中] AggregationPlan::GetStats が子統計**: 未修正(`aggregation_plan.cpp:56-60`)。
- **[低] IndexScan provided_order が等値固定列を含まず**: 未修正(`index_scan_plan.cpp:30-52`)。
- mask=0 述語消滅 / capture 汚染 / Projection IsOrderedBy: **修正済み**(解決済み表)。

---

## 5. executor

### 5.1 v1 指摘の状況

- **[高] ParallelAggregation `generic_scratch_` データレース**: 未修正
  (`parallel_aggregation.cpp:159-180`、共有メンバを mutex 外で clear/push_back)。
- **[高] TableKeyFilter スタッシュが自己結合の別エイリアスにも適用**: **部分修正**
  (文間漏れは防止: planning_heuristics.cpp:1009-1057,1133-1147。キーは table 名のまま
  (:986,1134)で同文内 t1/t2 衝突は残存)。
- **[高] 直列集約に LOGICAL_AND/OR ケース不在(DOP で結果が変わる)**: 未修正
  (kLogicalAnd/Or は並列のみ: parallel_aggregation.cpp:138,145,414,423)。
- **[高] 並列 SUM(int64) overflow wrap**: 未修正(`parallel_aggregation.cpp:253-267`)。
- **[中] CanReserve→Add TOCTOU**: 部分(Release は CAS 化。Reserve 経路は非原子対のまま:
  query_memory.hpp:36-39, hash_join.cpp:430-441)。
- **[中] ハッシュジョインのスピル**: 部分(part==0 resident 判定に CanReserve:
  hash_join.cpp:1188-1196。非 resident 読み出しと出力全量常駐(:1213)は予算外)。
- **[中] 外部ソート最終段**: 部分(ストリーミング化。最終 `rows_` 全蓄積は継続)。
- **[中] 即席 jthread で QueryScheduler 迂回**: 未修正
  (hash_join.cpp:780,871,1017,1056 / sort.cpp:321 / scan_filter.cpp:345 /
   parallel_aggregation.cpp:468。scheduler 未接続、worker cap 16 のみ: relational_factory.cpp:203-206)。
- **[中] preaggregate が混在 select 項(SUM(x)+y)を通す**: 未修正
  (`subquery_runtime.cpp:292-299`)。
- **[中] UNNEST(PROTO) の stoll 黙殺**: 未修正(`detail/scan_filter.cpp:464,483`)。
- **[中] IndexJoin 複合キー**: **修正済み**。
- **[低] EstimateJoinRows 再エンコード**: 未修正(`planning_heuristics.cpp:180-209`)。
- **[低] Selection::NextBatch の Reset で容量廃棄**: 未修正(`selection.cpp:124`)。
  ParallelScan 側は修正済み。
- **[低] Distinct 重複集合の無課金**: 部分(SortDistinctPlan/Executor 新設:
  implementation_rules.cpp:1540-1572。ただし cost にメモリ勘定がなく hash が選ばれ続ける)。
- **[低] Window SUM uint64 wrap / O(n²) 分割**: 未修正(`detail/window_eval.cpp:555-557`)。

---

## 6. query / server

### 6.1 v1 指摘の状況

- **[高] JOIN ... USING が `col = col` 自明真を生成**: **未修正**
  (`googlesql_ast_visitor.cpp:1930-1946`。移動のみ)。
- **[高] CREATE CONSTANT が thread_local で接続間リーク**: 未修正
  (`type/interval.cpp:19,41-53`、visitor `:2375`)。
- **[高] 集合演算**: **回帰**(§6.2 参照)。
- **[中] VARCHAR の NUL バイトがフレーミングを壊す**: 未修正
  (`postgres_protocol.cpp:45-60,228-`)。
- **[中] 全行 materialize/backpressure**: 部分(kMaxServerResultRows 新設。
  単一 string 連結と Queue erase O(n) は維持)。
- **[中] UPDATE SET の型無検査**: **未修正 + 実害確認**
  - `UPDATE t SET a='not-an-int'`(INT64 列)が成功し、生バイト再解釈値
    `2936174736662894` を永続化することを実機確認(executor/update.cpp に coercion 無し。
    INSERT は有り)。
  - 提案: INSERT の coercion ヘルパを共用。
- **[中] NULL ソート順**: 変化(NULLS FIRST/LAST パース導入、既定規約を statement.hpp に
  明記。PG 既定 ASC=NULLS LAST とは不一致のまま)。
- **[中] 日時パースの黙認**: 部分(named zone 解決追加。sscanf 未確認・未知 TZ の
  既定オフセットフォールバックは残存: visitor `:49-99`)。
- **[低] sql_engine.hpp stray include / VisitExpression ~630 行**: 未修正。
- **[低] pgwire $$ 非対応 + main.cpp 二重実装**: 部分(コメント対応済み。
  $$/$tag$・ネストコメントは未対応。main.cpp:44-101 との不統一継続)。

### 6.2 新規発見(現 HEAD での回帰・新バグ)

- **[高] 集合演算の ORDER BY/LIMIT/OFFSET/WITH が無言で捨てられる(現 HEAD 回帰)**
  - 場所: `query/googlesql_ast_visitor.cpp:1968-1981`(SetOperation 早抜けが
    OrderBy/LimitOffset/WithClause 処理より前に return)
  - 実機再現: `SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a DESC LIMIT 2`
    → ソートも制限も適用されない。
  - 提案: reflog `9423511` が持っていた head への句保存コード(同 rev :2062-2115)を復元。
- **[中] 集合演算種別が常に UNION ALL(v1 #7 への逆戻り回帰)**
  - 場所: visitor `:1977-1978`(全オペランド `AddUnionAll`)+
    `executor/relational.cpp:839`(種別無視の追記)
  - 実機再現: `UNION DISTINCT` が重複保持、`INTERSECT DISTINCT` が連結を返す。
  - 提案: 9423511 の `SetOperationKind` パイプライン(visitor の MetadataList 解析、
    common/set_operation.hpp、executor/set_operation.*)を丸ごと復元(§8 参照)。
- **[高] sql_template 再バインドが `union_all_` 枝を無言で喪失(実機再現)**
  - 場所: `query/sql_template.cpp:325-414`(BindSelect が union_all_/Qualify を
    コピーしない)、`sql_engine.cpp:641-647`(fill 時 RememberTemplate)、`:569-579`(再生)
  - 問題: リテラルが head 側だけの集合演算は、枝落ちした文がそのままテンプレート
    キャッシュに保存され、同一形状の次回実行は **head のみ実行され行が欠落**。
    枝側にもリテラルがある場合のみパラメタ数不一致 throw が偶然救済する(設計ではない)。
  - 再現: `SELECT a FROM t1 WHERE a=1 UNION ALL SELECT a FROM t2;` →
    次に `...WHERE a=2...` で t2 側が消える。
  - 提案: BindSelect に union_all_ の再帰コピー(+SetQualify)追加。
    または `RequiresRelationalEvaluation()` の文をテンプレート対象外に。
- **[高] 同じ再バインドで `has_limit_` が喪失し DISTINCT+LIMIT が無視される(実機再現)**
  - 場所: `query/sql_template.cpp:398-400`(ctor に limit 値を渡すのみで SetLimit せず)、
    `sql_engine.cpp:976,994`
  - 再現: `SELECT DISTINCT a FROM t WHERE a<9 LIMIT 2;` の次に `a<99` 版 → LIMIT 無視。
  - 提案: `result->SetLimit(select.HasLimit() ? ... : nullopt)` を追加。
    SelectStatement の全フィールドを運ぶコピーヘルパ化が根本策。
- **[中] CollectStatementColumns が union_all_ 枝を走査しない**
  - 場所: `executor/detail/expression_eval.cpp:4359-4381`
  - 問題: reusable_projections が root 文の必須列だけで初期化され table 名単位で共有。
    自己 UNION の異列枝や集約+同名表の組合せで過小射影(column X not found)が発火し得る。
  - 提案: UnionAll 枝(と Qualify)の走査追加 + 共有キーに statement 識別子を含める。
- **[中] plan_cache: エポック無効化は同一 fingerprint の再照会時しか起きない**
  - 場所: `query/plan_cache.hpp:624-643`(lazy drop)、`:578-591`、`sql_engine.cpp:1035-1038`
  - 問題: DROP TABLE 後に二度と照会されない fingerprint は Table/Schema を pin 継続。
    `Find()` は parameter mismatch 判定前に hits++(:589,641)しヒット率指標が膨張。
    thread(:583)と global(:632)で epoch 参照経路が非対称。
  - 提案: BumpSchemaEpoch 時の世代スイープ or TTL。カウンタ計上位置の整理。
- **[低] ParameterSlot が `Type()==kConstantValue` と偽装する UB 地雷**:
  `plan_cache.hpp:114-131`。将来 `AsConstantValue()` 呼び出しで downcast UB。
  debug assert か専用 TypeTag。
- **[低] evaluation_context_impl.hpp が本番未接続(コメントは接続済みと主張)**:
  `evaluation_context_impl.hpp:14-16`。include は differential_test のみ。
  接続するか削除。
- **[低] ORDER BY 序数が静かに無視(実機再現)**: `SELECT a FROM t ORDER BY 1 DESC` が
  無ソート。ParseOrderingTerm が式を返さず term ごと破棄。序数解決 or 明示エラー。

---

## 7. ビルド・リポジトリ衛生・テスト基盤

- **[中] 全テストが legacy parser + sql 層へ過剰リンク(test_util ODR ハザード)**:
  未修正(`CMakeLists.txt:559-572`、test_util が `type/row.cpp` を独自コンパイル)。
- **[中] CI への differential_test 未組み込み**: TSAN/fuzzer-nightly workflows は新設
  されたが、三層一致性の要である differential_test は未登録。§3 の問題群の早期検出に必須。
- **[低] compliance testdata の実行残骸**: `query/testdata/googlesql_compliance/` 配下に
  `ironlamb-compliance-*` の .db/.log が多数。掃除 + ignore。
- **[低] main.cpp の分割器二重実装**: 未修正(`main.cpp:44-101`。pgwire 版との差異継続)。

(v1 の「ルートログ散布」「サーバ入力上限」は解決済み表参照)

---

## 8. 新規モジュール(現 HEAD 未収録 — reflog `9423511` / `../tinylamb_tmp`)復元時必修事項

集合演算・MergeJoin・TopN 等は一時的に実装されたが現作業木から外れている。
品質自体は概ね妥当(種別 6 種・列数/型不一致の明示エラー・共通型昇格・スピル・
CTE 内集合演算まで整備)だが、以下を修正してから復元すること。

- **[高] EXCEPT DISTINCT が多重度を数えてしまい、B 側に存在する行を出力する**
  - 場所(tmp 木): `executor/set_operation.cpp:234-241`(AppendExcept の all=false 経路)
  - 問題: `removed` を multiset カウント減算に使うため `A={5,5,5}, B={5}` で 1 行出力。
    SQL の EXCEPT DISTINCT は「B に一度でも出現した行は除外」(正解は空集合)。
    EXCEPT ALL としては正しいので分岐構造の誤り。既存テストは count_A==count_B のみで未検出。
  - 提案: distinct 時は「存在すれば無条件スキップ」、all 時のみカウント減算。
    `count_A > count_B` の差分テスト追加。
- **[高] SetOperationExecutor が UNION ALL まで全入力をメモリ materialize(予算課金・スピル対象外)**
  - 場所(tmp 木): `executor/set_operation.cpp:284-307`(spill 条件が
    `operation != kUnionAll` のため)、`:84-85,169-171`
  - 問題: UNION ALL は本来逐次 drain のストリーミング演算なのに全ソース全行を蓄えてから
    最初の 1 行を返す。QueryMemoryCharge も無しで大入力 OOM。旧 relational.cpp 実装より劣化。
  - 提案: kUnionAll はソース順次連結(MergeAppend に寄せる)へ。課金は全種別に適用。
- **[中] MergeJoin が両入力+出力を 3 重に完全 materialize、入力ソート済み契約を検証しない**
  - 場所(tmp 木): `executor/merge_join.cpp:45-61,14-27`
  - 問題: マージジョインの存在意義(ストリーミング合成)を果たしておらず、スピルも無い。
    ルール側の IsOrderedBy 判定が誤ると黙って欠損マッチ(誤結果)。
  - 提案: ストリーミング本体へ書き換え + debug ビルドで隣接キー順序 assert。
- **[中] MergeJoinPlan が semi/anti でも右側統計を Concat し schema と列数不整合**:
  `merge_join_plan.cpp:21-29`。semi/anti では左 stats のみに。
- **[中] TopN の LIMIT 0 / WITH TIES 契約が LimitPlan・LimitExecutor と正反対**:
  `topn.cpp:19-22`(limit 0 = 空)対 `limit_plan.cpp:29` 等(limit 0 = 無制限)。
  現状はルールが limit_count != 0 でガードし不発だが地雷。with_ties 時 EnforcesLimit=false
  だと engine が LimitExecutor を上乗せし ties を切り捨てる。
  「limit 0 = 無制限」へ統一し、with_ties 用の専用フラグを engine と合意すること。
- **[中] 集合演算実装ルールが ORDER BY の NULLS FIRST/LAST 指定を落とす**:
  implementation_rules.cpp:1635-1638(`SortKey{..., nullopt}` 固定)。
  required.ordering に方向情報を持たせるのが根本対応。
- **[中] 混合連鎖の種別決めが「メタデータ欠落時に最終種別を複製」に依存**:
  9423511 visitor:2038-2055。空 op を skip → padding で back() 複製。欠落は throw に。
  SqlEngine 経由の UNION DISTINCT/INTERSECT/EXCEPT(ALL) e2e テスト追加。
- **[中] スピル時も出力が全量メモリ常駐**: set_operation.cpp:148-166(`accumulated` に
  課金・再分割なし)。charge 課金 + 閾値超過で再分割。
- **[低]** SetOperationPlan の EmitRowCount/GetStats が常に先頭子基準(hpp:30-32) /
  MergeAppendExecutor::Before が比較のたび KeyValue 再計算(merge_append.cpp:61-98) /
  Max1RowExecutor が std::runtime_error throw(max1_row.cpp:18-20) /
  ValuesExecutor の「replayable」コメントと move 実装の不一致(values.hpp:13-16) /
  RelationRenamePlan::GetStats が改名前修飾子のまま(relation_rename_plan.hpp:37-39)。

---

## 推奨着手順(v2)

1. **ワーキングツリーの安定化**: §8 のモジュール群は reflog `9423511` /
   `../tinylamb_tmp` にのみ存在。意図した状態か確認し、戻す場合は本節の必修修正を
   施してから取り込む。
2. **differential/回帰テスト網の整備**: CI に differential_test を追加し、
   集合演算・LIMIT・テンプレート再バインドの e2e テスト(§6.2 の再現クエリ)を固定化。
   「2 回目の実行で結果が変わる」系は回帰として最悪。
3. **ARIES 系の構造修复(§1)**: 専用 `kAbort` 型導入 + マーカ durable 化 +
   DeleteRow 冪等化で、v1 からの abort マーカ問題と v2 新発見(undo 中断マーカ、
   page_lsn 回帰)を同一の修復筋で解消。`log → mutate` 順序統一と destroy redo も併せて。
4. **新機能の正確性(§3.2)**: window frame_unit 実装と interval sniffing 撤去。
   どちらも「静かに誤った結果を返す」タイプ。
5. **query 層の高**(USING・UPDATE 型検査・NUL バイト・session constants)。
6. **並列的正確性(§5)**: generic_scratch_ レース、直列/並列集約の不一致、SUM wrap。
   TSAN + 直列/並列/AST 差分テストで固定。
7. **コストモデル(§4)**: 選択率二重計上の解消(pushdown 後残余述語の整理)。
