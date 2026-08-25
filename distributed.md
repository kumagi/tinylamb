# tinylamb 分散アーキテクチャ設計(改訂版)

この文書は分散版 tinylamb の**親設計書**である。実装は複数のエージェントが並行して
行うことを想定しており、用語・不変条件・プロトコル手順を厳密に記述する。
本書だけでも全体像と不変条件は把握できるが、次の 3 冊の詳細仕様を子文書として持つ:

| 子文書 | 内容 | 対応実装者 |
|---|---|---|
| `docs/distributed_raft.md` | Raft の完全実装仕様(Raft の先行知識不要) | 制御プレーン担当 |
| `docs/distributed_membership.md` | メンバシップ管理(single-server change / joint consensus / learner / 障害回復) | 制御プレーン・運用ツール担当 |
| `docs/distributed_migration.md` | 稼働系からの移行運用計画(M0〜M7、ロールバック込み) | 移行・運用担当 |

実装者は本書の不変条件(I-1〜I-10)と受け入れ基準(Phase 0〜5)に従うこと。

## 0. 前提

- 故障モデル: **crash-recovery(クラッシュストップ)**。ノードは任意時刻でプロセス
  停止し、ディスク状態は保存される。再起動後は WAL からのリカバリを経て復帰する。
  ビザンチン故障・悪意あるノードは扱わない(§14)。
- 既存の単一ノード tinylamb(ARIES 風 WAL、Strict 2PL + MVCC、epoll 製 PostgreSQL
  ワイヤサーバ)を土台にする。**ネットワーク関連のコードは現在ワイヤサーバしか
  存在しない**。配送・制御の通信基盤は本計画で新設する。
- 確認済みの既存資産(2026-08 時点、ファイル:行は現行コード対応):
  - `recovery/logger.hpp:33` — 単一ログファイルへの `Logger`。`AddLog`(LSN は
    ファイル先頭からのバイトオフセット)、`WaitForDurable(lsn)`(group commit 待ち)、
    `DurableLSN()/BufferedLSN()`。fsync は最大 10ms バッチ。
  - `recovery/log_record.hpp:31` — `LogType` 32 種。txn 毎の `prev_lsn` 逆連鎖。
    **レコード/WAL にチェックサム・バージョン・長さ枠は無い**(§5.2 で対策)。
  - `recovery/recovery_manager.hpp` — `RecoverFrom` / `SinglePageRecovery`
    (CRC 不良ページのログ再構築)。page_lsn ガードで REDO 冪等
    (recovery_manager.cpp:74)。
  - `recovery/checkpoint_manager.hpp` — fuzzy checkpoint。ただし
    **master record は書かれるが読まれず、常に offset 0 から全走査する**
    (page_storage.cpp:52)。WAL 切り詰めも存在しない(§8)。
  - `transaction/transaction_manager.cpp:57` — `PreCommit` は
    ①`CommitVersions`(MVCC 可視化、:59)→ ②kCommit 記録(:62)→ ③`WaitForDurable`
    の順。**可視化がログ追記より前**である点は §5.4 で再構成する。
    また `Abort` は終端に `kCommit` **型**のレコードを書く(:97)。undo は online CLR
    完了済みのため物理的には無害だが、複製・回復の意味論で明示的に扱う(§5.4)。
  - `transaction/transaction_manager.cpp:184` — snapshot 取得(Begin)と
    commit_timestamp 出版(CommitVersions)は同一 `transaction_table_lock` で
    原子化。この規律は §5.6 で保持する。
  - `database/page_storage.hpp:51` — PageStorage が Logger/Recovery/Transaction/
    Checkpoint 各 manager を所有。`EmulateCrash()` あり。
  - `server/postgres_server.cpp` — 単一 epoll スレッド + 読み取り専用 worker pool。
    コミットの durability 待ちは epoll スレッド上で同期(`WaitForDurable`)。
    seal 待ち化には callback 完了への改修が要る(§9)。
- コンパイル規約・スタイルはリポジトリ従来通り(Apache 2.0 ヘッダ、clang-format、
  コメント最小限)。CMake は `add_simple_test`(CMakeLists.txt:382)と
  `tinylamb::*` エイリアスの慣例に従う。**fuzzer セクションは現状コメントアウト
  されている**(CMakeLists.txt:472-524)。Phase 0 で harness を復活させる(§12)。

## 1. 設計の核心(要約)

1. **合意は制御プレーンのみ**。Raft グループ(3 または 5 ノード)はメタデータ——
   ノードレジストリ、クラスタ構成(L1)、シャード→レプリカセット割当と view(L2)、
   sealed_epoch——の合意だけに使う。トランザクションデータは Raft を経由しない。
   **制御プレーンの状態はシャードノードの WAL から再構築可能な鏡像である**
   (詳細は `distributed_membership.md` §1, §8.3)。これにより制御プレーン全損が
   データ損失にならない。
2. **データパスは primary-backup の WAL ログシッピング**。primary が自ノードの
   WAL をバックアップへ転送し、quorum が fsync した時点で seal 可能になる。
3. **グローバル LSN を廃止し、並列ログストリーム+(epoch, stream LSN)に置き換える**
   (FOEDUS 方)。ページ所有権はストリームに静的に割り当て(I-9)、REDO 順序は
   「エポック昇順 → ストリーム内連続 prefix」で再現できる。
4. **コミット応答は自分の属するエポックの seal 完了まで待つ**(durability=
   kQuorum 時)。Silo の EPC の弱点(ack 直後 ~40ms の消失)を修正し、
   「ack 済みコミットは必ずフェイルオーバーを跨いで生存」を成立させる。
   移行途中は durability=kLocal(従来の自ノード fsync)を使い、切替は
   運用操作として行う(`distributed_migration.md` §M5)。
5. sealed_epoch は制御プレーンの Raft に 1 エポック 1 レコードで複製され、
   ハートビートで全ノードに配布される。エポック進行そのものは合意せず、
   **完了(seal)のみを合意**する。
6. **構成変更はすべてメンバシップ層の小さなコマンド集合として表現される**
   (L1 single-server change / joint consensus、L2 learner→promote→remove)。
   failover・ノード交換・レプリカ数変更・移行は同じプロトコルの組み合わせで
   ある(`distributed_membership.md`)。

## 2. 用語と型

```
nodeid_t     = uint32_t   // 登録時に固定。アドレス変更で不変
epoch_t      = uint64_t   // データエポック番号。シャード毎に primary が割り当て
view_t       = uint64_t   // config epoch。fencing 用(§5.7 I-6)
stream_id    = uint32_t   // ログストリーム識別子(ノード内 worker)
stream_lsn   = uint64_t   // ストリーム内バイトオフセット(Logger の lsn_t を流用)
shard_id     = uint32_t   // Phase 4 まで常に 0
sealed_epoch             // 「epoch <= e の全レコードが primary+quorum で fsync 済」
                         // が Raft により合意された最大の e
DurabilityMode           // kLocal: 従来通り自ノード fsync で ack
                         // kQuorum: seal 完了で ack(移行の切替点)
```

新規型は `common/constants.hpp` に追加する。

## 3. 全体アーキテクチャ

```
                 ┌──────────────────────────────┐
                 │  Control Plane: Raft x3/x5   │
                 │  メタデータ FSM:              │
                 │   node registry              │
                 │   L1: 制御グループ構成        │
                 │   L2: shard configs          │
                 │     (voters/learners/primary,│
                 │      view, sealed_epoch)     │
                 │  実装仕様: distributed_raft.md│
                 └──────┬───────────────────────┘
                        │ 構成配布 Heartbeat{view, shard_configs}(10ms、TCP unicast)
                        │ SealRequest(e) / SealReply
        ┌───────────────┼──────────────────┐
        ▼               ▼                  ▼
 ┌────────────┐  ┌────────────┐    ┌────────────┐
 │ Node A     │  │ Node B     │    │ Node C     │
 │ [primary]  │  │ [backup]   │    │ [backup]   │
 │ EpochMgr   │  │ Receiver   │    │ Receiver   │
 │ Stream x4  │→ │ Stream x4  │    │ Stream x4  │
 │ WAL fsync  │  │ WAL fsync  │    │ WAL fsync  │
 │ Engine     │  │ (redo only)│    │ (redo only)│
 └────────────┘  └────────────┘    └────────────┘
        ▲ SQL クライアント(PostgreSQL プロトコル)は primary へ(Phase 4 から router)
```

- 各ノードは制御プレーンクライアント(Heartbeat 受信、SealRequest 送信)と
  データプレーン役割(primary/backup/learner)を両方持つ。
- 通信は 2 系統ある。混同注意:
  - **Raft heartbeat**(制御ノード間、50ms、`distributed_raft.md` §13)
  - **構成配布 Heartbeat**(制御 leader → データノード、10ms、本書 §4.3)
- シャードは Phase 4 まで 1 個(cluster 全体が 1 レプリカセット)。シャード分割は
  同一プロトコルの繰り返しとして拡張する。

## 4. 制御プレーン

### 4.1 Raft

- `distributed/raft/` に自己完結実装。外部依存(ETCD 等)は持ち込まない。
  **アルゴリズムの全規定(状態・メッセージ・各ハンドラの疑似コード・永続化形式・
  落とし穴・テストマトリクス)は `docs/distributed_raft.md` にあり、Raft の
  先行知識なしで実装できる粒度にしている。本書では再述しない。**
- 要点のみ: 永続化対象は currentTerm/votedFor/ログで、fsync は「投票応答前」と
  「複製成功応答前」の 2 箇所。pre-vote 必須。コミット判定の現 term 条件必須。
  読み取りは ReadIndex で線形化。
- メンバシップ変更(制御グループ自体の過半構成)は構成エントリで表現し、
  single-server change を既定、joint consensus を `--membership-mode=joint` で
  提供する。プロトコル詳細は `distributed_membership.md` §4。

### 4.2 メタデータスキーマ(FSM 状態)

コマンド体系と状態は `distributed_membership.md` §5/§6 が正である。概要:

```cpp
struct ShardConfig {
  shard_id shard;
  view_t view;                      // 単調 +1(fencing)
  std::vector<nodeid_t> voters;     // primary 含む。quorum = size/2+1
  std::vector<nodeid_t> learners;   // 追従中。seal 過半に数えない
  nodeid_t primary;
  epoch_t sealed_epoch;
  std::map<stream_id, stream_lsn> sealed_watermarks;
  DurabilityMode durability;        // kLocal / kQuorum
};
```

- `kSealEpoch` は「shard s の epoch e 以下の全レコードが、primary および
  quorum-1 個の **voter** backup で fsync 済であり、各ストリームの末端 LSN は
  sealed_watermarks の通り」という主張を合意する。
- FSM 適用は決定的(提案時検査と適用時検査の分離は `distributed_membership.md` §3)。

### 4.3 構成配布ハートビート

- 制御プレーンの Raft leader が 10ms 間隔(設定可 `--control-heartbeat-ms`)で
  全データノードへ `Heartbeat{leader_term, view, shard_configs[]}` をユニキャスト
  (TCP、制御ポート 9100 で受信。epoll インフラのパターンを流用した新規実装)。
- データノードは受信 view が自ノード保持値未満なら破棄、以上なら採用
  (view は単調にしか採用しない)。leader_term で古い leader の配布を切り捨てる。
- ハートビートは fencing token(view)の配布経路でもある(§5.7)。

### 4.4 seal 合意フロー

```
primary                          control plane (Raft leader)
  │  epoch e close                     │
  │  自ストリーム flush watermark 収集   │
  │  voter backups の FlushAck(e, wm) │
  │  quorum(primary + quorum-1)成立    │
  ├─ SealRequest{s, view, e, wms} ────▶│
  │                                    ├─ view 検査(自分の FSM view と一致)
  │                                    ├─ kSealEpoch を Raft ログに append+fsync
  │                                    ◀─ quorum commit
  │ ◀── SealGranted{s, e} (応答兼通知) ─┤
  │ sealed_epoch 更新、commit 待ち解放   │
```

- primary の view が古い場合(昇格済み新 primary がいる等)は
  SealRequest を拒否する。これが I-6 の実装点の一つ。
- 制御プレーン到達不能時、primary は seal を進められない → kQuorum モードの
  コミット応答が止まる(可用性より整合性。仕様)。kLocal モードは影響なし。

### 4.5 メタデータ問い合わせ

router や admin からの構成読み取りは制御ノードの ReadIndex
(`distributed_raft.md` §12)で線形化して返す。

## 5. データプレーン

### 5.1 並列ログストリームとページ所有権

- 書き込みワーカ数(初期値 4、`--wal-streams`)だけ `Logger` を生成し、各々独立
  ファイル `{dbdir}/wal/stream_{id}.log` を持つ。`--wal-streams=1 --epochs=off`
  (compat モード)では従来通りの単一 `<dbname>.log` で新レコード種を 1 本も
  書かない(移行互換性。`distributed_migration.md` §2)。
- **ページ所有権**: page_id → stream の写像は `page_id % num_streams` で静的に
  決める。ページを変更するログは必ずそのページの所有ストリームへ書く(I-9)。
- **トランザクションはストリームを跨ぎ得る**(修正: 旧版は「1 txn = 1 stream」と
  記していたが、複数ページに触る txn は所有権写像上必ず複数ストリームに分散する)。
  扱いは次の通り:
  - `kBegin` / `kCommit` は txn の**ホームストリーム**(`txn_id % num_streams`)へ。
  - 各変更レコードと CLR は対象ページの所有ストリームへ。
  - `prev_lsn` は `(stream_id, stream_lsn)` の組に拡張する。
  - コミット判定はホームストリームの kCommit の durability + その epoch の seal。
    txn の全レコードは kCommit 以前の epoch に書かれているため、kCommit の epoch
    が seal されれば txn 全体が確定する(seal は全ストリームの累積 watermark)。
- ストリーム内では従来どおり txn の `prev_lsn` 逆連鎖を保つ。

### 5.2 LogRecord 拡張と配送の整合性

- `LogType` に追加:
  - `kEpochBegin` — payload に epoch_t。ストリーム内で epoch 境界を刻むマーカ
  - `kShardHeader` — ファイル先頭固定位置に書く {shard_id, stream_id, creation_view}
- 既存 WAL にはチェックサム・長さ枠が無い。単一ノードでは「解析失敗=末尾」規約で
  成立しているが、配送では**バッチヘッダ
  `{stream_id, start_lsn, end_lsn, first_epoch, last_epoch, view, crc32c}`** で
  バッチ単位の完全性を検証する(`common/crc32c.hpp` を使用)。
- `LogRecord` 本体への epoch 埋め込みはしない。エポック帰属は「直前の
  kEpochBegin 以降」として回復時に決定する。シッピングバッチヘッダの epoch 区間で
  二重化する。

### 5.3 EpochManager(primary のみ)

```cpp
class EpochManager {
 public:
  epoch_t AssignEpoch();          // 変更レコード/kCommit 追記時に呼ぶ。open epoch を返す
  void CloseAndAdvance();         // タイマ(既定 5ms)or バイト閾値(既定 256KB)
  epoch_t sealed_epoch() const;   // 制御プレーンから通知された値
 private:
  epoch_t open_epoch_{1};         // 起動時は max(sealed, log 内最大)+1
};
```

- epoch はシャード内で厳密単調増加。failover 後の新 primary は
  `max(sealed_epoch, 自ログ走査で見つけた最大 epoch) + 1` から再開する
  (**廃棄した末尾エポックの再利用禁止**、I-3)。
- CloseAndAdvance は「該当 epoch のレコードを含む全ストリームの flush 完了」と
  「voter backup の FlushAck 到着」を待ってから SealRequest を出す。

### 5.4 コミットプロトコル(最重要)

現行 `PreCommit`(transaction_manager.cpp:57-73)は ①CommitVersions(MVCC 可視化)
→ ②kCommit 記録 → ③durability 待ち の順である。**分散版は順序を入れ替え、
3 段階に分割する**:

```
// Phase 0 で導入する分割 API(§9 変更表)
Status LogCommit(Transaction&);     // ①kCommit をホームストリームへ AddLog
                                    //   ②epoch_t my_ep = epoch_mgr_.AssignEpoch()
                                    //   ③durability==kLocal なら WaitForDurable して即応答
void FinalizeCommit(Transaction&);  // seal 待ち解放後に呼ぶ:
                                    //   CommitVersions(MVCC 可視化)+ status 更新
                                    //   + ReleaseLocks(ロック解放は即時でよい、§5.6)
```

- kQuorum モードでは、LogCommit の後 `my_ep <= sealed_epoch` になるまで待機
  (条件変数/コールバック)し、解放されたら FinalizeCommit してクライアントへ応答。
  サーバは epoll スレッドを塞がないため callback 完了形式に改修する(§9)。
- **ack 済みコミットの消失は許されない**(I-2)。crash 時に sealed 未満だった txn は
  ack 前なのでクライアントには不定(rollback 相当)と見なせる。
- 読み取り専用 txn は seal 待ち不要。
- **Abort の扱い**: Abort(transaction_manager.cpp:75-100)は online CLR で物理
  undo した後、終端として `kCommit` **型**のレコードを書く(現行の性質)。
  分散版でもこの形式を維持する(フォーマット変更を避ける)。abort のレコード群は
  seal を待たず複製される(best effort)。あるエポック境界で未 seal 末尾が破棄
  されても、「その位置でクラッシュした」状態と等しいため、既存の ARIES 回復が
  そのまま正しさを担う(§7)。回復側は「kCommit 型終端 = 物理 undo 済みの
  確定」として扱う(現行と同一)。
- レイテンシ見積: 平均 epoch 間隔/2 + seal RTT。既定 5ms 間隔なら p50 ~3ms。

### 5.5 レプリケーション(shipper/receiver)

- Shipper(primary): ストリーム毎に、flush 済み区間を 64KB バッチで並列送信
  (9200/tcp。Nagle 無効)。
- Receiver(backup/learner): バッチを受信したら自ストリーム WAL に追記して
  fsync し、`FlushAck{view, stream_id, durable_lsn}` を返す。
- **連続 prefix 規則(I-4)**: receiver が ack するのは自保持の連続 prefix の末端
  のみ。ギャップ検出時は該当区間の再送を要求する。「durable_lsn 以下は完全」が
  保証され、failover 時の最新性比較が単純になる。
- backup は受信ログに対し `SinglePageRecovery` 相当の即時 REDO を行い
  ホットスタンバイとして振る舞う(適用 watermark は読み出し提供時に使用、§5.7)。
- **背圧と停滞 replica の扱い**: voter backup 向けの送信キューは 1GB 上限。
  超過したら制御プレーンへ報告し、当該 replica を「要再同期」状態へ自動遷移
  (`distributed_membership.md` §6.3 で remove-replica + 再 base backup)。
  quorum 内の他 voter が健在なら primary の書き込みは継続する。
  learner の停滞は単なる警報。
- **catch-up の初期化**: 新 learner は base backup(ファイルコピー + tail 追従)で
  追いつく。手順と安全性根拠(WAL 冪等 REDO・torn page の SinglePageRecovery・
  append-only による単調 prefix コピー)は `distributed_migration.md` §M3。

### 5.6 可視化とロック

- ロック解放は LogCommit 直後(現行の Strict 2PL から変えない)。
- MVCC 可視化(`CommitVersions`)のみ FinalizeCommit、すなわち seal 後とする。
  未 seal txn の更新は `VersionChain::pending` のまま残り、他 txn からは見えない。
- commit_timestamp の発行と出版は引き続き `transaction_table_lock` の下で
  行い、`Begin` の snapshot 取得との原子性(transaction_manager.cpp:46-52 の
  既存規律)を保持する。可視化の順序は seal 完了順=エポック順になるため
  commit_ts の単調性は保たれる。
- I-6/I-8 により、未 seal でもロック早期解放による矛盾は生じない
  (crash すればページ状態は REDO で巻き戻る)。

### 5.7 不変条件

```
I-1  sealed_epoch(e) が合意されている ⇒ epoch<=e の全レコードが primary と
     quorum-1 個の voter backups に fsync 済である。以後いかなる failover でも
     消えない。learner の FlushAck は決してこの過半に数えない。
I-2  クライアントへ commit 応答を返した txn の epoch は、応答時点で sealed
     である(kQuorum モード)。kLocal モードでは従来通りの自ノード fsync 保障。
I-3  エポック番号は各シャードで単調増加し、一度 sealed された範囲は再利用されない。
I-4  backup が FlushAck するのは連続 prefix のみ。
I-5  新 primary のログは sealed_watermarks 以上の連続 prefix を必ず含む
     (選出手順 §6 がこれを検証する)。
I-6  view より古い view を持つメッセージ・WAL append は全ノードが拒否する(fencing)。
I-7  旧 primary は自身より新しい view を観測した瞬間、書き込み受理を停止し
     backup へ降格する。
I-8  リカバリ/Failover 後の DB 状態は「最後の seal までの committed 履歴」と
     して一意に定まる(分岐しない)。
I-9  通常運用中、1 ページを変更するログは単一ストリームにのみ書かれる。
     違反はバグ(debug アサートで常時検証)。
I-10 txn のコミット判定はホームストリームの kCommit + その epoch の seal で
     行い、変更レコード単位の seal 確認はしない(kCommit の epoch が seal され
     ていれば先行レコードは全て seal 済み)。
```

## 6. フェイルオーバー手順

検出: backup は primary からのバッチ/ハートビート途絶を 3×heartbeat(30ms)で
検知し、制御プレーンへ `PrimarySuspect{s}` を報告。制御プレーンは primary への
疎通も確認し、`kSetPrimary`(view+1)を発行する。**view 変更の強制発行は
最低 2 秒間隔とする**(フラッピング抑制。可用性パラメータであり安全性に無関係)。

```
1. control plane: candidates = 生きている voter のうち
   durable_lsn[全 stream] >= sealed_watermarks を満たすもの(I-5)
   - 複数あれば任意(例: 最小 nodeid)。learner は候補外
   - 誰も満たさなければ、他 replica からの欠損転送を待ってから再試行
     (可用性停止。データ分岐ではない)
2. control plane: kSetPrimary{view=v+1, primary=candidate} を Raft commit
3. candidate: 構成配布 Heartbeat で新 view と primary 指名を受領
   a. 自 WAL を ARIES リカバリ(§7)し、sealed_watermarks まで確定
   b. sealed_watermarks を超える末尾(旧 primary から受けていた未 seal 分)を破棄
   c. open_epoch を再設定(§5.3)
   d. Engine を書き込み可能状態へ遷移
4. 旧 primary(生存していた場合):
   - 新 view の Heartbeat 受信時点で書き込み停止(I-7)、自 WAL の未 seal 末尾を
     破棄して backup として再参加
   - ネットワーク分離したまま書き続けようとしても、backup 側・制御プレーン側
     とも view 検査で拒否する(I-6)
5. クライアント: primary アドレスは接続時に router へ問い合わせるか、
   redirect エラー(SQLSTATE 08P01 相当 + host 情報)に従って再接続する
```

計画停止(メンテナンス)は drain(書き込み停止→open epoch の seal 完了)後に
同一コマンドで行うため、コミットロスが常にゼロになる
(`distributed_membership.md` §6.4)。

## 7. リカバリアルゴリズム(ARIES 拡張)

起動時・昇格時に実行。既存 `RecoveryManager::RecoverFrom` をストリーム対応に拡張。

```
Analysis:
  各 stream ファイルを走査し、kShardHeader/kEpochBegin を追跡しながら
  (a) 最終 sealed_epoch 以下の各ストリーム末端 LSN(watermark)を確認
  (b) checkpoint レコード(kBeginCheckpoint/kEndCheckpoint)から DPT/ATT 復元
  (c) watermark 超過のレコード(unsealed tail)は無視リストに入れる
  (d) ホームストリームの kCommit 型終端から committed 集合を構成
      (abort の終端も kCommit 型であるため物理状態は同一、§5.4)
REDO:
  epoch 昇順に、各ストリームの watermark までのレコードを適用。
  同一 epoch 内でのストリーム間順序は不要(ページ所有権分割により同一ページを
  複数ストリームが触らないため、I-9)。page_lsn ガードの冪等再適用はそのまま有効
UNDO:
  ATT に残る loser txn を既存ロジックで undo。prev_lsn は (stream, lsn) 組を
  辿る。CLR 連鎖も従来通り
```

- 追加不変条件 **I-9** の検証アサートを REDO に置く。
- 「エポック粒度の末尾破棄」は「そのバイト位置でのクラッシュ」と等価である。
  既存 ARIES は任意ログ位置のクラッシュを扱うため、新たな回復論理は不要である。
- Phase 0 で **master record(`<dbname>.last_checkpoint`)を読んで走査開始点を
  短縮する修正**を含める(現状は常に 0 から。WAL 無限増大の対処の前提、§8)。

## 8. チェックポイントと WAL サイズ

- `CheckpointManager` は「sealed_epoch 以下の範囲」でのみ checkpoint を取る。
  DPT の page_lsn は stream LSN の組で表現されるため、DPT entry に stream_id を
  追加する(現行 DPT は `pair<page_id_t, lsn_t>`)。
- checkpoint 中も書き込みは止めない(既存設計)。fuzzy checkpoint の境界は
  kBeginCheckpoint/kEndCheckpoint レコードで従来通り決まる。
- **現状 WAL 切り詰めは存在しない**(recovery が全走査するため切り詰められない)。
  Phase 0 の master record 修正の後、Phase 5 で「checkpoint LSN までの切り詰め」
  を導入する。それまで base backup は「WAL 全量コピー + 冪等 REDO」で成立する
  (`distributed_migration.md` §M3 の根拠)。

## 9. 既存コードへの変更指示

| ファイル | 変更 |
|---|---|
| `common/constants.hpp` | nodeid_t/epoch_t/view_t/stream_id/shard_id 型追加 |
| `common/crc32c.hpp` | そのまま配送バッチ・Raft フレームの CRC に使用 |
| `recovery/logger.hpp/.cpp` | `SyncUntil(lsn)`(seal 用 group flush barrier)追加。既存バックグラウンドフラッシュは温存 |
| `recovery/log_record.hpp/.cpp` | kEpochBegin/kShardHeader 追加、Serialize/Decode。`prev_lsn` の (stream, lsn) 組化は `LogRecord` の外側(txn 側)で保持する |
| `recovery/recovery_manager.*` | マルチストリーム Analysis/REDO/UNDO(§7)。コンストラクタは stream パス集合を受ける形へ。master record 参照 |
| `recovery/checkpoint_manager.*` | DPT entry へ stream_id 追加 |
| `transaction/transaction_manager.*` | PreCommit の 3 分割(LogCommit / FinalizeCommit、§5.4)。CommitVersions は FinalizeCommit 側へ移動(transaction_table_lock の規律は保持)。Abort は現状維持 |
| `database/page_storage.*` | ストリーム集合の保持、page→stream 写像、durability モードの伝播 |
| `server/postgres_server.cpp` | 書き込み txn 完了を callback で epoll ループへ復帰(既存の read completion eventfd パターンを流用)。redirect エラー応答 |

既存テストはすべて壊さないこと。compat モードで現行動作を完全に再現する
(`distributed_migration.md` §2 の互換ポリシー)。

## 10. 新規ディレクトリ構成と CMake

```
distributed/
  raft/
    raft_log.{hpp,cpp}        // 永続化付きログ(distributed_raft.md §8 形式)
    raft_node.{hpp,cpp}       // 選出/複製/適用(distributed_raft.md §6-7)
    raft_fsm.hpp              // MetaFsm インターフェース(§11)
  wire.{hpp,cpp}              // 長さ+CRC+version フレーム(distributed_raft.md §4)
  metadata.{hpp,cpp}          // FSM 状態(ShardConfig/NodeInfo/適用時検査)
  control_client.{hpp,cpp}    // Heartbeat 受信、SealRequest、ReadIndex 問い合わせ
  epoch_manager.{hpp,cpp}
  shipper.{hpp,cpp}
  receiver.{hpp,cpp}
  basebackup.{hpp,cpp}        // M3 のコピー+tail 追従ツール
  admin_main.cpp              // tinylamb_admin(distributed_membership.md §9)
  redirect_router.{hpp,cpp}   // thin TCP router(Phase 4)
tests: distributed/*_test.cpp(add_simple_test 慣例)+ 決定論シミュレータ
CMake: add_library(tinylamb_distributed ...) + tinylamb::distributed ALIAS、
       実行体 tinylamb_distributed_server(postgres_server_main 流用)、tinylamb_admin
```

## 11. 段階的実装計画と受け入れ基準

作業 packages(WP)は並行実行を想定。依存は → で示す。

### Phase 0: 単一ノード基盤(compat 完全互換)
- WP-0a master record 参照による走査開始点の短縮(§8)
- WP-0b 型追加 + `wire` フレームユーティリティ
- WP-0c StreamLogger 複数化 + kEpochBegin/kShardHeader + EpochManager
  (ローカル即時 seal)。compat モードでは新レコード 0 本
- WP-0d PreCommit 3 分割 + (stream,lsn) prev_lsn + ホームストリーム
  (最大の改修。単一ストリームでは挙動同一)
- WP-0e postgres_server の commit callback 完了化
- fuzzer harness の復活(CMakeLists.txt:472 のコメントイン)
- **受け入れ**: 既存全テスト green。compat で TPC-C 単一ノード比 ±10%。
  epoch モードで kill -9 リカバリ成功(§7 拡張)。

### Phase 1: Raft 制御プレーン(`docs/distributed_raft.md`)
- WP-1a raft_log(永続化・CRC・セグメント)
- WP-1b raft_node(選出/pre-vote/複製/コミット)→ WP-1a
- WP-1c metadata FSM + node registry + Heartbeat 配布 + ReadIndex → WP-1b
- **受け入れ**: raft テストマトリクス全 green(distributed_raft.md §15)。
  決定論シミュレーション 100 万ステップで P-1〜P-5 違反 0。
  3 ノードで leader kill 100 回、FSM 状態不一致 0。

### Phase 2: メンバシップ v1(`docs/distributed_membership.md`)
- WP-2a L1 single-server change + learner + 移譲 → WP-1c
- WP-2b L2 shard 操作コマンド + tinylamb_admin → WP-2a
- **受け入れ**: membership テストマトリクス全 green
  (distributed_membership.md §10)。負荷中の add/remove/promote で
  コミット停止 < 1 秒。

### Phase 3: ログシッピング + seal(移行 M3〜M5 相当)
- WP-3a shipper/receiver + FlushAck + prefix 規則 + 背圧 → WP-0c
- WP-3b seal 経路 + kQuorum モード + durability 切替 → WP-3a, WP-1c
- WP-3c basebackup ツール → WP-3a
- **受け入れ**: 3 voter + kQuorum で TPC-C 走行中 primary を kill -9 →
  昇格後 ack 済み全コミットが観測(Jepsen 風)。unsealed tail 破棄も検証。
  learner の FlushAck が seal 過半に数えられないことの否定的検証。

### Phase 4: フェイルオーバー完成 + クライアント切替(移行 M6)
- WP-4a PrimarySuspect/昇格/旧 primary 再参加
- WP-4b redirect_router + redirect 応答
- **受け入れ**: 旧 primary 分離中の書き込み継続 × 新 primary 昇格を同時に
  走らせ split-brain 書き込み 0 件。分離回復後の自動 backup 化。

### Phase 5: 拡張(別紙に切り出し可)
- WP-5a joint consensus(L1)
- WP-5b 制御 FSM スナップショット + WAL 切り詰め + base backup の高速化
- WP-5c シャード分割 + クロスシャード 2PC(prepare/commit を seal に乗せる)、
  backup stale read、読み出しスケール計測
- **受け入れ**: 3→5 一括拡張(joint)を負荷中に完了し中間状態を作らない。
  切り詰め後の base backup が成立。

## 12. テスト戦略

1. **決定論的シミュレーション**: 通信層を抽象化し、メッセージ消失/遅延/重複/
   順序入れ替え、プロセス kill+restart(WAL 保持)、ディスク遅延を注入。
   乱数シード固定で失敗再現可能。Raft(distributed_raft.md §15)と
   seal/failover で共用するハーネスにする。
2. **不変条件アサーション**: I-1〜I-10 は debug build で常時検証
   (特に I-4/I-6/I-9/I-10)。
3. **Jepsen 風結合テスト**: 実プロセス群 + psql クライアント。kill -9、
   ネットワーク分離(iptables/nftables)、時計なしでコミットの一意性・永続性を検査。
4. **移行ドリル**: `distributed_migration.md` の M1〜M6 を CI で縮退版実行
   (ループバック 3 ノード)、ロールバック手順を含めて自動検証。
5. **性能回帰**: TPC-C を分散クライアント対応にし、Phase 毎に
   BenchmarkHistory.md へ記録。

## 13. 性能上の指針

- fsync はエポック粒度の group commit で償却。ストリーム並列により CPU スケール。
- シッピングはストリーム単位パイプライン。64KB バッチ、Nagle 無効。
- seal 要求はバッチ化(複数エポックを 1 SealRequest にまとめてよい)。
- ハートビート 10ms、epoch 5ms、Raft heartbeat 50ms、election 400-800ms は
  初期値。計測して調整。

## 14. 非目標

- Byzantine 耐性、認証・TLS(既存サーバも未対応)
- マルチシャード分散トランザクション(Phase 5 以降)
- Chain Replication / CRAQ(読み出しスケールの将来候補として言及のみ)
- SQL レベルの分散結合、グローバルスキーマ管理
- 時計同期への依存(正しさは一切 wall clock に寄らない)
- 構成変更の完全自動化(auto-rebalance、自動ノード交換)。runbook の半自動化まで

## 15. 参考文献

- Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (Raft)
- Ongaro, "Consensus: Bridging Theory and Practice"(博士論文。§4 メンバシップ変更、
  joint consensus と single-server change の原典)
- etcd raft library ドキュメント(learner・pre-vote・joint consensus の実装例)
- Lamport, "Vertical Paxos" — 合意を設定管理に限定する方式の理論的根拠
- Tu et al., "Speedy Transactions in Multicore In-Memory Databases"(Silo,
  SOSP'13)— エポックコミット。本設計は ack 時点 durability を保証するよう修正
- Kimura, "FOEDUS"(VLDB'15)— 並列ログストリームと (epoch, ordinal) 再構成
- van Renesse & Schneider, "Chain Replication"(参考)
- Bernstein et al., "Viewstamped Replication Revisited" — failover 時の
  ログ最新性保証
