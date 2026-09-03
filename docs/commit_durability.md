# Commit durability

本方針は docs/design.md **D4**（WAL順序と外部可視化の分離）の実装記録。

## Protocol (D4)

`TransactionManager::PreCommit` は次の順序で進行する:

1. データ WAL に続いて commit レコードを `Logger::AddLog` する。
   `AddLog` 完了が WAL 上の順序確定であり、この完了前に可視化は行わない
   （「AddLog 完了前の version を別トランザクションへ可視化しない」）。
2. commit レコードの `AddLog` 完了後、`CommitVersions(txn, commit_end)` が
   版を公開し、内部の write intent を解放する。ここは fsync を待たない。
   各 `CommittedVersion` は自身の恒久化地点 `commit_lsn`
   （= commit レコード終端、AddLog 直後の `BufferedLSN()`）を持つ。
3. 版を読んだトランザクション（T2）は、観測した版の `commit_lsn` を
   依存 LSN（`Transaction::RecordDurabilityDependence`）として記録する。
   T2 の内部処理は先行してよい。
4. T2 は結果をユーザーへ返す直前（`PreCommit` の返し際）に、
   依存 LSN すべて（＝観測した最大値）の恒久化を
   `Logger::WaitForDurable` で待つ。read-only トランザクションでも省略しない。
5. 書き込み T2 自身も、自身の commit 待機とは独立に依存 LSN の待機を満たす。
   「自分の commit LSN が依存先より後に並ぶ」ことだけを根拠にしない。

## synchronous_commit との関係

- **`synchronous_commit = true`（既定）**: 自身の commit レコード終端の
  恒久化を待つ（グループコミット、既定 1ms 間隔）。
- **`synchronous_commit = false`**: 自身の commit の fsync は待たない。
  ただし **D4 の依存 LSN バリアは無効化されない**。設定が制御するのは
  自身のコミット待機だけである。

| Mode | 自身の commit | 観測した依存 commit |
|------|--------------|--------------------|
| sync on | fsync 待ち | fsync 待ち |
| sync off | 待たない（喪失しうる） | fsync 待ち |

## 実装上の注意

- 待機点: `WaitForDurable(レコード終端)`。AddLog 戻り値はレコード先頭 LSN
  であり恒久化地点ではない。Abort 経路も終端待ち。
- read-only txn: 自前のログ書込はないが、観測版があれば右端の依存 LSN 待機が発生する。
  何も観測しなければ待機しない（依存 0）。
- チェックポイント: transaction status / prev LSN は `transaction_table_lock`
  下で読み取られる。依存 LSN はトランザクション毎の atomic で、恒久化
  バリアのみが読む（WAL には記録しない）。
- sync 間隔は `Logger::every_ms`（既定 1ms、0 は 1ms へ clamp）。macOS は
  F_FULLFSYNC。
- LSN 3種: CommittedLSN=write済み(fsync未)、DurableLSN=fdatasync済み、
  BufferedLSN=バッファ末尾。durable 判定に使うのは DurableLSN/WaitForDurable のみ。
- 受け入れテスト: `DurabilityBarrierTest.*`
  (transaction/transaction_extra_test.cpp)。
