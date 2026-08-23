# docs/commit_durability.md レビュー指摘事項

## サマリー

中核の主張(デフォルト同期コミット、`Logger::WaitForDurable` による待ち合わせ、グループコミット約10ms、`fdatasync`)は実装と一致する。`TransactionManager::PreCommit`(transaction/transaction_manager.cpp:57-73)が commit レコードを `AddLog` し、`synchronous_commit_`(既定値 true, transaction_manager.hpp:169)なら durable 待ちをする記述は正確である。一方で、(1) 待ち対象 LSN の精密さ、(2) read-only トランザクションの扱い、(3) チェックポイント機構の現状(実質無効化されている)、の3点で記述が不足しており、「commit 成功 = durable」の境界条件を実装者が誤解する恐れがある。

## 指摘一覧

### U-1: 「その LSN が durable になるまで待つ」は正確でない
- 区分: 粒度不足
- 対象: docs/commit_durability.md:5-6 「appends a commit log record via `Logger::AddLog`, then by default **waits until that LSN is durable**」
- 問題: `AddLog` の戻り値はペイロード先頭の LSN であり、実際に待つのは `BufferedLSN()`(= commit レコードの終端)。文書どおり戻り値 LSN を待つ実装を書くと、レコード末尾が durable にならない前に成功を返しかねない。
- 根拠: transaction/transaction_manager.cpp:62-69 —
  ```cpp
  txn.prev_lsn_ = logger_->AddLog(commit_log.Serialize());
  // AddLog returns the LSN *before* the payload; durable point is end of
  // the buffered commit record.
  const lsn_t commit_end = logger_->BufferedLSN();
  if (synchronous_commit_) { logger_->WaitForDurable(commit_end); }
  ```
  recovery/logger.cpp:108-143(`AddLog` は書き込み前の `buffered_lsn_` を返す)
- 提案: 「`WaitForDurable(BufferedLSN())`、すなわち commit レコード全体の終端が durable になるまで待つ」と明記する。

### U-2: read-only トランザクションはログも待ちも発生しない
- 区分: 粒度不足
- 対象: docs/commit_durability.md:3-17(Policy 全体)
- 問題: Policy は全コミットに共通の挙動のように読めるが、read-only トランザクションは commit レコードの追加も durability 待ちも一切行わず即座に完了する。境界条件として明記がないため、read-only パスの性能特性やクラッシュ時挙動を誤解する。
- 根拠: transaction/transaction_manager.cpp:61-70 — `if (!txn.IsReadOnly()) { LogRecord commit_log(...); ... WaitForDurable ... }`。また Begin も read-only ではログを書かない(:41-44)
- 提案: 「read-only トランザクションには commit レコードも待ち時間も存在しない」ことを追記する。

### U-3: 可視化(バージョン公開)は durability 待ちより前に起きる
- 区分: 粒度不足
- 対象: docs/commit_durability.md:8-10 「commit success means the commit record has been written and `fdatasync`'d」
- 問題: 「durable になってから見える」とは書かれていないが、順序の説明がなく誤解を招く。実際は CommitVersions によるバージョン公開とステータス更新が WAL 書き込み・fsync 待ちより先行し、他セッションは当該データを fsync 前に読み得る(プロセスクラッシュ時はロールバック相当になる)。分散/レプリケーション拡張(distributed.md)を検討する際に重要な順序制約。
- 根拠: transaction/transaction_manager.cpp:59-63 — `CommitVersions(txn)` → `SetStatus(kCommitted)` → `AddLog(commit)` → :67-68 で初めて durability 待ち
- 提案: 「可視化はメモリ上で即時、durability 確定はその後(既定で最大~10ms後)。クラッシュ時は未 flush コミットは消失し可視状態と整合する」と順序を明記する。

### U-4: 設定方法の文が壊れており、フラグの実態も半分だけ
- 区分: 不明瞭
- 対象: docs/commit_durability.md:16-17 「Set via `TransactionManager::SetSynchronousCommit(bool)` / `Logger` is shared; the wait uses `Logger::WaitForDurable(lsn)`.」
- 問題: (a) 文が途中で切断され「/ `Logger` is shared」の意味(Logger が何と共有なのか)が不明。(b) フラグは TransactionManager インスタンス単位(DB単位)であり SQL/セッション単位の切替ではないことが書かれていない。(c) 「benchmarks で使え」という趣旨だが、リポジトリ内のどこからも `SetSynchronousCommit(false)` は呼ばれておらず、async モードは未接続の機能である。
- 根拠: transaction/transaction_manager.hpp:59-60(インライン setter/getter のみ)/ `grep -rn SetSynchronousCommit --include=*.cpp .` では定義以外の呼び出しなし(benchmark/ 配下にもなし)
- 提案: 「`PageStorage` 毎の `TransactionManager` に対してプログラム的に設定する。現状リポジトリ内に無効化の呼び出し箇所はない」と文を修整・補完する。

### U-5: チェックポイントのタイミングが文書化されず、実装側も実質停止している
- 区分: 実態との乖離
- 対象: docs/commit_durability.md 全体(タイトルに「durability」を掲げながら checkpoint に触れない)、同:29 参照先
- 問題: crash safety の裏付けとしてチェックポイント挙動の説明が必要だが、現状 (1) 周期チェックポイントは `Start()` が呼ばれないため起動しない、(2) マスタレコード(`.last_checkpoint`)は書くだけで誰も読まない、(3) 起動時は常に WAL 先頭からフルスキャン——という状態である。「commit survives process crash」自体は正しい(WAL 全量リプレイのため)が、recovery 時間がチェックポイントで短縮されない事実とセットで文書化されていない。
- 根拠: recovery/checkpoint_manager.hpp:38 `size_t interval = 60`(既定60秒)/ recovery/checkpoint_manager.cpp:42-44 `while (!start_ && !stop_) { sleep(10ms); }` — `Start()` 未呼出なら周期実行に進まず、database/page_storage.cpp:51 の構築では `cm_(MasterRecordName(), &tm_, pm_.GetPool())` のみ / `Start()` の呼び出し箇所は本番コードに存在しない(grep では宣言のみ)/ database/page_storage.cpp:52 `rm_.RecoverFrom(0, &tm_);` — checkpoint LSN を渡していない / recovery/checkpoint_manager.cpp:90-93 マスタレコードは `ofstream` に書くのみ(fsync なし)。`.last_checkpoint` を読む実装は存在しない(tpch_benchmark.cpp:636,643 はファイル存在確認・削除のみ)
- 提案: 「チェックポイントは60秒周期設計だが現行の起動パスでは無効、リカバリは常に WAL 全スキャン」と現状を明記するか、この文書のスコープ外として別節に切り出す。

### U-6: 「~10ms」は固定値であり、調整用パラメータは死んでいる
- 区分: 粒度不足
- 対象: docs/commit_durability.md:10 「the logger worker batches syncs up to ~10ms」
- 問題: 10ms は `constexpr` 固定で設定不能。一方 Logger のコンストラクタ引数 `every_ms` は `every_us_` に保存されるだけで一度も読まれず、`PageStorage` は 1000 を渡している——「sync間隔を変えられる」という偽の調整面がある。また Linux は `fdatasync`、macOS は `F_FULLFSYNC` であり、単に "fdatasync" と書くのは不正確。
- 根拠: recovery/logger.cpp:149 `constexpr auto kSyncInterval = std::chrono::milliseconds(10);` / recovery/logger.hpp:76 `const size_t every_us_;`(宣言のみ、読み取り箇所なし)+ recovery/logger.cpp:58 初期化のみ / database/page_storage.cpp:47 `logger_(LogName(), static_cast<size_t>(8 * 1024 * 1024), 1000)` / recovery/logger.cpp:46-52 `FdataSync` は `#if defined(__APPLE__) F_FULLFSYNC #else fdatasync`
- 提案: 「sync 間隔は固定 10ms(constexpr)、変更不可。macOS では F_FULLFSYNC」を追記し、`every_ms` 引数は削除するか無意味である旨を注記する。

### U-7: CommittedLSN / DurableLSN / BufferedLSN の混同リスク
- 区分: 不明瞭
- 対象: docs/commit_durability.md:6,17 「`Logger::WaitForDurable`」「the wait uses `Logger::WaitForDurable(lsn)`」、同:29 参照先
- 問題: Logger には3つの LSN 系統があり、特に `CommittedLSN()` は「write() 済みだが fsync 未」のバイト数を返す名前である。参照先 improvement.md §6.1 の対策案 (A) が「PreCommit が `CommittedLSN` 待ち」と表現しており、本ドキュメントだけ読むと `CommittedLSN` を durable 点と誤認しかねない。
- 根拠: recovery/logger.hpp:45-51 — `CommittedLSN()`: "Bytes written to the log file (may not be fsynced yet)", `DurableLSN()`: "Bytes that have survived fdatasync", `BufferedLSN()`。improvement.md:119-124 §6.1(ギャップ指摘は [高] のまま残置)/ improvement.md:357-358 M2.3-M2.4([x] 完了)
- 提案: 3つの LSN の対比表を追加し、「durable 判定に使うのは DurableLSN のみ」を明示する。§6.1 が修正前の問題提起であることも一言添える。

## 未検証事項

- Trade-off 表(docs/commit_durability.md:21-25)の「Lower p99 / higher TPC-C tps」「TPC-H read-mostly workloads are barely affected」は、BenchmarkHistory.md に sync on/off 比較の記録がなく実測根拠を確認できなかった。
- ディスク満杯・write 失失敗時の挙動(logger.cpp:191-194 は LOG(FATAL) 後 return)における `DurableLSN` の整合性は本レビューでは検証していない。
