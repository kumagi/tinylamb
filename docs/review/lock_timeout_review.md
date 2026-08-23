# docs/lock_timeout.md レビュー指摘事項

## サマリー

定数値・シグネチャ・基本動作の主張は実装と一致している。「5 seconds」は `LockManager::kExclusiveWaitTimeout{5}`(transaction/lock_manager.hpp:22)と正確に対応し、`wait=false` の try-lock 意味論、タイムアウト後の `false` 返却、`GetSharedLock` のノンブロッキング性、wait-for graph 不在もすべてコードどおりである。参照先 improvement.md §7.1 / §M2.5 も実在する。一方で、「caller should abort / retry」の retry は Insert 経路では危険であること、読者パスはそもそも共有ロックを取らないこと、ライタ飢餓の可能性など、呼び出し元契約の重要な側面が文書化されていない。

## 指摘一覧

### L-1: 「abort / retry」の retry が Insert 経路で安全でない
- 区分: 実態との乖離
- 対象: docs/lock_timeout.md:4-5 「then returns `false` (caller should abort / retry)」
- 問題: Update/Delete と異なり、Insert はロック取得前に物理挿入(`InsertRow`)を先に行う。ロック競合で `false`(→`Status::kConflicts`)が返った時点でスロットは消費済みであり、WAL レコードもバージョン登録もない状態の穴がページに残る。この状態で「retry」すると別スロットへの二重挿入になり得るため、retry を対称的な選択肢として提示するのは誤り。実際、コードベース上 kConflicts をリトライする呼び出し箇所は存在しない。
- 根拠: page/row_page.cpp:70-73 — `slot_t result = InsertRow(record); if (!txn.AddWriteSet(RowPosition(page_id, result))) { return Status::kConflicts; }`(物理挿入がロック取得に先行)/ 同じく :157-160(Update はロック取得が先行)/ 呼び出し側 table/table.cpp:136-137, 202-203 は `return Status::kConflicts;` して伝播するのみ
- 提案: 「false 戻り時はトランザクションを abort すること。Insert 経路では物理挿入が先行するため単純な再試行は不可」と契約を限定して書き直す。

### L-2: GetSharedLock の記述は LockManager API としては正しいが、エンジンの読者経路では未使用
- 区分: 粒度不足
- 対象: docs/lock_timeout.md:7-8 「`GetSharedLock` remains non-blocking: if an exclusive lock is held it returns `false` immediately (readers do not queue behind writers).」
- 問題: LockManager 単体の意味論としては正しい(cpp:15-27 で排他保有時即 false)。しかしトランザクション層の読み出しは MV2PL 方式で共有ロックを一切取得しない(`AddReadSet` は read_set_ への登録のみ)。つまり「readers do not queue behind writers」の真の理由は共有ロックの非ブロッキング性ではなく「読者はロックを取らない」ことであり、スキャン実装者がここから共有ロック利用を連想すると設計と乖離する。
- 根拠: transaction/transaction.cpp:70-76 — `bool Transaction::AddReadSet(const RowPosition& rp) { ... read_set_.insert(rp); return true; }` コメント「MV2PL readers use snapshot-visible row versions and therefore never take a lock that conflicts with a writer's exclusive lock」/ index/index_scan_iterator.cpp:151 `txn_.AddReadSet(pos_);` / 本番コードでの `GetSharedLock` 呼び出しは TransactionManager の転送(transaction/transaction_manager.cpp:381-383)のみでテスト以外に使用者なし
- 提案: 「現行の読み出し経路はスナップショット分離により共有ロックを使用しない。GetSharedLock は補助 API」と立場を明記する。

### L-3: ライタ飢餓(継続読者による排他待ちの無期限遅延)が文書化されていない
- 区分: 粒度不足
- 対象: docs/lock_timeout.md:3-5 「waits up to **5 seconds** for conflicting shared/exclusive holders to release」
- 問題: 待機条件は「現在の保持者なし」のみで待機者キューを考慮しないため、`GetSharedLock` は排他待ちが存在しても新規取得でき、読者が途切れない限り排他要求は5秒ごとに失敗し続ける(飢餓)。また待ち行列の FIFO 性・割込み(barging)の有無も文書になく、タイムアウト=「5秒以内に取得できる/できない」と読むと誤解がある(5秒は各試行の上限であって公平性の保証ではない)。
- 根拠: transaction/lock_manager.cpp:53-56 — blocked 判定は `shared_locks_` / `exclusive_locks_` の現存チェックのみ(待機者は記録されない)/ transaction/lock_manager.cpp:15-27 — GetSharedLock は排他保有時しか拒否しない / transaction/lock_manager.hpp:41-44 — 待機キューや waiter 数のフィールドは存在しない
- 提案: 「タイムアウトは個々の取得試行に対する上限。継続的な読者トラフィック下では排他取得が繰り返し失敗し得る(待機者優先制御なし)」を追記する。

### L-4: ミリ秒オーバーロードと TryUpgradeLock の意味論が未記載
- 区分: 粒度不足
- 対象: docs/lock_timeout.md:3, 5(wait=true / wait=false の二択としての記述)
- 問題: 公開 API には任意タイムアウトを取るオーバーロードがあり、テストもこれを使用しているが文書が言及しない。また `TryUpgradeLock` は「共有保持者がちょうど1かつ排他保持者なし」でのみ成功するという条件(複数読者いるアップグレードは常に即失敗)が文書化されておらず、呼び出し側がリトライ戦略を立てられない。
- 根拠: transaction/lock_manager.hpp:28-29 `bool GetExclusiveLock(const RowPosition& row, std::chrono::milliseconds timeout);` / transaction/lock_manager_test.cpp:35,50 `EXPECT_FALSE(lm.GetExclusiveLock(row, std::chrono::milliseconds(50)));` 等 / transaction/lock_manager.cpp:77-85 `TryUpgradeLock` — 排他保有時 false、`shared->second != 1` でも false
- 提案: オーバーロードと TryUpgradeLock の成立条件(共有カウント==1 のみ)を追記する。

### L-5: 参照先 improvement.md §7.1 が修正前の状態を述べており混乱を招く
- 区分: 不明瞭
- 対象: docs/lock_timeout.md:14 「See `improvement.md` §7.1 / §M2.5.」
- 問題: 参照自体は妥当(§7.1 = improvement.md:134、M2.5 = :359 とも実在)。ただし §7.1 本体には「デッドロック検知なし(wait-for graph も timeout も wait-die もない)。2txnが交差待ちすると永久停止。」と現行実装(timeout 実装済み、M2.5 は [x] 完了)と矛盾する記述が残っており、参照だけだと現状と過去のどちらを見ているのか判別できない。
- 根拠: improvement.md:136 「デッドロック検知なし(wait-for graph も timeout も wait-die もない)。...timeout付き待機+エラー返却の最小実装を推奨。」/ improvement.md:359 `- [x] M2.5: LockManager に待機タイムアウト(最低限)+ エラー返却。...` / 実装は transaction/lock_manager.cpp:50-66(timeout付き待機+false返却)
- 提案: 「§7.1 は本機能追加前の問題提起、M2.5 が実施記録」という注釈を参照箇所に添える。

## 未検証事項

- タイムアウト値 5 秒の妥当性(TPC-C/TATP レイテンシへの影響)についてはベンチマーク記録がなく評価していない(improvement.md §7.1 の「行シャード化」提案との絡みも含む)。
- `available_.notify_all()` 全員起こし方式のスループット特性(thundering herd)は測定していない(recovery/logger.cpp の condvar 利用とは別箇所)。
