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
  であり恒久化地点ではない。
- Abort 経路は恒久化を待たない。`staged_records_` を逆順にメモリへ適用し、
  未コミット MVCC 版を破棄して、ログを書いた transaction には `kAbort` を追記する。
  WAL の読み戻しや `read_undo_record` の待機はない。再起動は `kCommit` のある
  transaction だけを再生するため、揮発した Abort レコードは loser の復元を招かない。
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

## サーバの遅延応答

`PrepareCommitForDurableReply()` は公開までを行い、own commit（sync on時）と
観測依存の最大LSNを返す。通常の `PreCommit()` の同期動作は変えない。
単文のworker経路はSQLワーカを解放し、耐久化coordinatorが
`Database::WaitForCommitDurable(target)` の成功後に応答全体をepollへ渡す。
送信時の固定費を減らすため、前の出力が残っていない接続の4KiB以下の応答は、
この耐久化判定後にworkerからnonblockingで送信できる。sendとcloseを共有mutexで
排他し、切断後のfd再利用へ書き込まない。一部だけ送れた時やEAGAIN時は残りを
epollへ渡す。completionの処理まで次のQueryを実行せず、応答順序を保つ。
literal返信cacheの登録用frameと、output上限超過の応答はこの経路から除外する。
複数応答の最大targetをまとめて待っても、各応答のown／依存条件は弱めない。
単独writerには追加のbatch待機を置かない。新しいcommitを作り得る保留SQL task（autocommit DML、COMMIT/END）がある時だけ、直近の同期時間に応じて100–1000µsを上限にまとめ、対象が処理済みなら期限前でも同期を始める。SELECTと明示block中のDMLは、このメッセージで新しいcommitを作らないため待ち合わせない。観測依存のLSN gateは省略しない。

公開済みのtransactionはregistryから外せるが、GCは最新commit_lsnが
DurableLSN以下になるまでversion chainを消さない。依存を失ってheapから
未耐久値を外部公開することを防ぐ。明示BEGIN中のSELECTも、COMMIT前に
観測した他transactionの依存LSNを満たしてから応答する。

待機失敗時は保留応答をErrorResponseに置き換え、COMMIT成功を返さない。
公開済みcommitは取り消さない。切断した保留commitも耐久化まで処理し、
未完了の明示blockはabortする。停止時はSQL producerをjoinしてから
耐久化queueをdrainし、loggerの寿命内に終了する。
複文DML／トランザクション制御Queryとevent-loop専用TEMP/UDF経路は同期commitを継続する。
SELECTだけの複文は、同じread-only snapshotの応答全体をcoordinatorへ渡せる。

`Database::DurableWALLSN()` はLoggerがfdatasync成功後に公開した実際のDurableLSNを読む。
own／観測依存のtargetがその境界以下なら、応答はqueueを経由せずに返せる。
1回の同期はcoordinatorが要求したtargetより先のWALまで耐久化することがあるため、
要求値のcacheではなく実際の証明済み境界を使う。BufferedLSNやwrite完了は
この判定に使わない。未到達targetは従来どおり待ち、同期失敗はエラーにする。

## 変更のないコンテキスト

更新可能なコンテキストでも、WAL記録、write set、索引変更、recycled allocationの
すべてが空ならown commit記録は不要である。WITHなどの保守的なサーバ経路でも
読取だけならWALを書かずに終える。版の観測依存はread-only経路と同じく必ず待つ。
ログを直接書かない埋込みテストのversion writeもwrite setがあれば従来通りcommitする。

複文の後半が失敗しても、先行SELECTの応答が観測した依存を失わない。
abort済みのコンテキストを破棄する前に依存を保持し、部分応答とエラーの送信も
耐久化成功まで待つ。待機失敗時は当該Queryの未送信結果をエラーへ置き換え、
それより前のQueryの出力は保持する。

## commit要求を中心にしたWAL同期

サーバーの耐久化coordinatorが有効な場合、Loggerは要求のないdirty WALの周期同期に
200 msのbackstopを使う。トランザクション途中のWALを短い周期で同期し始め、直後に
到着したCOMMITのためにもう一度同期する状況を減らす。`WaitForDurable` の要求、
セグメント切替、`Finish` は周期を待たず、従来どおり書き込み・同期・DurableLSN公開の
順で処理する。成功応答のown/dependency barrierとWAL-before-dataは変わらない。

この方針は同期commitを強制するサーバー向けであり、非同期commitの耐久性を保証する
ものではない。埋込みDatabaseと単体Loggerは明示的に有効化しない限り、指定された
`wal_sync_ms` / `every_ms` の周期を保つ。この同期方針自体はWAL形式・論理終端・再起動時の末尾破損判定を
変更しない。固定長方式の事前確保と形式は次節で区別する。


## 固定長WALの同期バリア

metadata v2のWALでは、同期前にデータ領域のwritten endをCRC付き終端スロットへ
書き、同じfdのfdatasync成功後だけDurableLSNを進める。WAL-before-dataのgateと
遅延応答coordinatorは従来と同じDurableLSNを使用する。物理サイズ、preallocation完了、
終端スロットのpwrite完了はcommit成功の証明にしない。
新規server DBへの採用範囲・検証・旧形式の維持は[ADR 0001](adr/0001-fixed-wal-logical-end.md)に記録する。
