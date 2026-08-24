# 単一ノードから分散構成へのマイグレーション運用計画

> **Status(2026-08-24)**: 設計段階(未実装)。レビュー指摘(用語未定義・手順矛盾・境界未規定等)は
> git 履歴の docs/adr(旧 docs/review)に記録済み。実装着手時に必ず履歴を参照して本書を更新すること。


本書は `distributed.md` の詳細仕様書の一つであり、**稼働中の単一ノード
tinylamb を停止時間最小(計画停止は再起動のみ、データ移行は無停止)で
3 レプリカ・quorum durability 構成へ移行する手順書**である。
各手順は 前提 / 手順 / 検証 / ロールバック を持つ。段階 M0〜M7 のうち、
**M5(durability 切替)までの間、データの安全性は常に単一ノード時代と同等以上**、
がこの計画の設計原則である。

対応フェーズ: M1=Phase 0、M2=Phase 1、M3=Phase 2/3、M5=Phase 3 完了、
M6=Phase 4(distributed.md §11 改訂版)。

## 0. 前提条件チェックリスト(M0 で確認)

### ハードウェア/ネットワーク

| 項目 | 要件 |
|---|---|
| ノード | 既存 1 台(n1)+ 新規 2 台(n2, n3)。制御プレーン 3 プロセスは n1〜n3 に同居可(初期)。本格運用では別ホスト推奨 |
| 容量 | 各レプリカ: 現行 db+wal サイズ ×1.2 以上。base backup 中は n2/n3 側に一時 ×2 |
| ネットワーク | ノード間で 9100/tcp(制御)、9200/tcp(データ配送)が開いていること。NFS 上の datadir は禁止(fsync の意味が消える) |
| fsync の実効性 | 仮想化/コンテナ環境で fdatasync が恒久化されること(`tinylamb_admin` の fsync 自己診断、または既知の virtio 設定確認) |

### 時計

正確性は wall clock に一切依存しない(`distributed.md` §14)。必要なのは
「タイムアウトがまともに動く程度に時計が狂っていないこと」のみ。
NTP 等の同期を推奨するが必須ではない。

### ポート/ファイル配置(全段階を通じて)

```
n1〜n3: {datadir}/
  <dbname>.db / .log / .last_checkpoint     ← 既存形式のまま
  wal/stream_%u.log                         ← epoch モード(M2〜)
  node_id                                   ← 登録済み nodeid
{controldir}(n1〜n3): meta, log_*.seg, snap_*(Raft、§distributed_raft.md §8)
listen: 9100(制御/Raft+メタ)、9200(データ配送受信)、5432(router、M6〜)
```

## 1. 移行の全体像

```
M0 監査     M1 入替        M2 基盤ON      M3 複製    M4 シャドウ   M5 切替    M6 客切替  M7 定常
─────┼────────┼──────────────┼───────────┼──────────┼───────────┼─────────┼─────────
 旧bi n/a   旧bi→新bi       epoch ON      learner×2   計測72h     durability client→
            (compat)        制御3台       →voter×2    (ackはlocal) →quorum     router
 ロールバック容易性: ◎(双方向)   ○(単方向)      ○(learner捨て可)  △(mode戻し可) 
```

`◎/○/△` はロールバックの自由度。M5 で「分散にしかないコミット」が生まれるが、
WAL 互換性により新バイナリの単一ノードモードへはいつでも戻せる(§5)。

## 2. 互換性とバージョニングポリシー

- **WAL レコード**: `LogRecord` にバージョン欄は無い。未知の LogType に遭遇した
  古いバイナリは解析停止(=ログ末尾扱い)する。したがって:
  - compat モード(`--wal-streams=1 --epochs=off`、既定)では**新レコード種を
    1 本も書かない**。WAL バイト列は旧バイナリと完全同一。
  - epoch モードで書いた WAL(kEpochBegin 等を含む)を読めるのは新バイナリのみ。
    **M2 以降の旧バイナリへのダウングレードは不可**(§5 の代替で戻す)。
- **配送プロトコル**: フレーム先頭 version=1(`distributed_raft.md` §4 と同じ
  形式)。不一致は接続拒否。major 違いは混在禁止。
- **Raft プロトコル**: 固定 v1。FSM コマンドは先頭バイトがコマンドバージョン。
- **ローリングアップグレード規約**: (1) 制御プレーンを先に揃えてアップグレード。
  (2) データノードは 1 台ずつ、その間 his シャードの primary でないノードから。
  (3) 新コマンド種を使う機能は、全ノードが対応版になってから有効化
  (`cluster describe` の機能フラグで確認)。

## 3. M0: 事前監査

- **前提**: なし(いつでも実行可)。
- **手順**:
  1. 現行負荷・容量・WAL 増加速度を記録(WAL は現在切り詰めが存在しないため
     **無限増大する**。`RecoverFrom` が常に先頭 0 から走査する現状
     (page_storage.cpp:52)と合わせ、Phase 0 の WP-0a 修正の根拠データを取る)。
  2. クラッシュリカバリ訓練: ステージングで kill -9 → 再起動 → 整合検査。
  3. ベースライン取得: TPC-C / TPC-H を実行し BenchmarkHistory.md へ記録。
  4. n2/n3 の到達性・ポート・ディスク・fsync 診断。
- **検証**: 上記 4 点の記録が揃っていること。
- **ロールバック**: 不要。

## 4. M1〜M2: バイナリ入替と基盤 ON

### M1: Phase 0 バイナリへの入替(compat モード)

- **前提**: M0 完了。新バイナリ = Phase 0 相当(型追加、StreamLogger、
  EpochManager、PreCommit 分割 API、async commit 完了。compat では全てが
  現行挙動と同一パス)。
- **手順**: n1 のバイナリを差し替えて再起動。フラグ既定のまま。
- **検証**: 全テスト green。TPC-C がベースライン ±10%。WAL バイト列を旧
  バイナリで recovery できること(ダウングレード訓練として一度実行)。
- **ロールバック**: 旧バイナリに戻すだけ(WAL 完全互換のため常に可)。

### M2: epoch モード切替 + 制御プレーン起動

- **手順**:
  1. n1 を `--wal-streams=4 --epochs=on` で再起動(WAL は
     `wal/stream_%u.log` へ新設。旧 `<dbname>.log` は参照不要となるが
     アーカイブとして残す)。
  2. n1〜n3 で制御プレーン 3 プロセスを起動(`--control-peers=...`)。
     Raft が leader を選出(`cluster describe` で確認)。
  3. ノード登録(n1, n2, n3)+ `kAddLearner` ではなく、初期シャードとして
     `shard 0: voters={n1}, primary=n1, view=1, durability=kLocal` を投入。
  4. 制御プレーンの Heartbeat 配布開始。primary(n1)は通常稼働を継続。
     durability が kLocal なのでコミット経路は M1 と同一(自ノード fsync)。
- **検証**: `cluster describe` が期待状態。n1 の書き込み負荷で TPC-C が
  ベースライン ±10%(epoch マーカと multi-stream のオーバーヘッド測定)。
  kill -9 → リカバリが stream 対応解析で成功。
- **ロールバック**: 制御プレーン停止 + n1 を compat で再起動。
  **ただし epoch モードで書いた WAL は旧バイナリ不可**(§2)。ロールバック先は
  「新バイナリ + compat」であり「旧バイナリ」ではない。

## 5. M3: レプリカ作成(base backup + 追従)×2

- **前提**: M2 緑。n2/n3 に空の datadir。
- **手順**(n2 で説明。n3 は並行実行可):

```
1. [n1] checkpoint 要求(admin 経由)。完了を master record 更新で確認
2. [n2] base backup: n1 の <dbname>.db / wal/ 全 stream / .last_checkpoint を
   通常のファイルコピーで取得(下記の安全性根拠参照)
3. [n2] 起動: RecoveryManager が全 stream を走査し REDO/UNDO。
   recovery 完了後、receiver を listen(9200)し、
   「コピーした各 stream ファイル長」を shipper に通知して tail 追従開始
4. [n1→n2] shipper は通知位置からバッチ送信。n2 は追記+fsync+FlushAck
5. [n2] 追従基準(全 stream で durable ≥ primary の shipped - 32KiB が 60 秒)
   を満たしたら control へ報告
6. [operator] promote --shard 0 n2 (view+1, voters={n1,n2})
   同様に n3 も promote (view+1, voters={n1,n2,n3}, quorum=3)
```

- **base backup の安全性根拠**(実装者はここを読んでツールを作ること):
  - db ファイルは走査中にページ書き戻しが起き得るが、全ページ変更の REDO が
    WAL に存在し、page_lsn ガード付き再適用は冪等(recovery_manager.cpp:74)。
    どの時点のページ混在でも「WAL 全量 + この db 画像」のリカバリ結果は一意。
  - 書き戻し途中の torn page は CRC 不一致 → SinglePageRecovery が WAL から
    再構築(既存機構の再利用)。
  - WAL は append-only かつ現状切り詰めなし。逐次 copy は単調 prefix を読む
    ので、末尾の不完全レコードは「解析失敗=末尾」規約で刈られる。
  - よって **base backup = 一貫性取得のために書き込みを止める必要がない**。
    チェックポイントはリカバリ時間短縮のためだけに入れている。
- **検証**: n2/n3 で `seal status` の durable が primary と同期。n2 単独で
  リカバリ→読み取り一致のドリル(昇格はまだしない)。
- **ロールバック**: learner を remove し n2/n3 の datadir を消去。
  primary への影響なし。

## 6. M4: シャドウ運転(計測ゲート)

- **手順**: 現状(voters=3, durability=kLocal)のまま **72 時間**運転し、
  レプリケーション基盤の健全性を計測する。ack 経路はまだ local fsync。
- **計測項目とゲート**:
  - shipper lag(durable_lsn 差) p99 < 50ms
  - FlushAck RTT p99 < 5ms
  - receiver の REDO 適用遅延 p99 < 100ms
  - prefix 規則違反(I-4)0 件、バッチ CRC 不一致 0 件
  - 制御プレーン term 変動 < 1 回/日(頻発なら election timeout チューニング)
- **検証**: ゲート全項目 green。
- **ロールバック**: M3 のロールバックに同じ。

## 7. M5: durability 切替(唯一の重要な境目)

- **手順**:
  1. ステージングで事前訓練: 同構成で TPC-C 負荷中に primary を kill -9 →
     昇格後、ack 済みコミットの消失 0、ack 前コミットの消失は許容(I-2)。
     これが落ちないこと(Phase 3 の受け入れ条件と同一)。
  2. 本番: `shard set-durability --quorum`(§6.5。エポック境界で切替)。
  3. 負荷を通常運用に戻し、seal レイテンシ(p50 ~3ms, p99 < 30ms 目安)を監視。
- **検証**: 実運用負荷で ack レイテンシ劣化が許容範囲(初期目標: p99 が
  local 比 +30ms 以内)。seal スループットが epoch 間隔に律速されないこと。
- **ロールバック**: `set-durability --local` で即時戻し可。
  **戻した瞬間から durability は単一ノードに依存する**(リスク窓を明示的に
  承認の上で戻すこと)。
- **注意**: M5 以降、「quorum で fsync 済み」のコミットが生まれる。単一ノード
  旧構成への完全復帰はこの後も可能だが、手順は §9(分散化の中止)に従う。

## 8. M6〜M7: クライアント切替と定常運用

### M6: router 経由への切替

- **手順**: router を起動し、クライアントの接続先を `router:5432` へ段階的に
  切替。直接 primary 接続は並走可能(移行期間のみ)。redirect(SQLSTATE 08P01
  相当)を有効化。
- **検証**: 接続断時に redirect 追従で再接続すること。primary 計画交代で
  セッションが新 primary へ流れること。
- **ロールバック**: クライアントを旧接続先へ戻す。

### M7: 定常運用

- ローリングアップグレード(§2 の規約)
- ノード交換・レプリカ数変更: `distributed_membership.md` §7 の playbook
- バックアップ: base backup(M3 手順)を learner から取得(primary 負荷なし)
- アラート最低セット: seal レイテンシ p99 > 50ms、制御プレーン leader 不在
  > 2 秒、heartbeat staleness > 100ms、shipper lag > 1GB(§10 参照)、
  制御プレーン disk > 80%
- 四半期ドリル: failover 訓練、force-single 手順のリハーサル
  (`distributed_membership.md` §8)

## 9. 分散化の中止(M5 以降に分散構成をやめる場合)

1. `shard set-durability --local`
2. learners がいれば remove。n2/n3 のプロセス停止
3. n1 を `--distributed=off`(epoch モードのまま単一ノード運用)で継続、
   または compat モードへ再起動(WAL は互換だが、epoch モードの WAL を読む
   のは新バイナリのみ。§2)
4. **旧バイナリへの復帰は不可**(M2 で書かれた epoch マーカを含む WAL が
   生存する限り)。やむを得ない場合は、新バイナリでチェックポイント後、
   db ファイルのみを旧バイナリへ移行する再構築手順を取る
   (db 画像はログ依存しないため可)。

## 10. 移行中の障害プレイブック

| 状態 | 影響 | 処置 |
|---|---|---|
| learner が追従しない(M3/M4) | なし(primary は local durability) | base backup からやり直し。primary の shipper 負荷を 64KB バッチ上限で保護 |
| base backup 中に n1 クラッシュ | なし | n1 復旧(旧来の単一ノードリカバリ)後、再取得 |
| 制御プレーン 1 台損失(M2 以降いつでも) | seal/failover は継続可 | `distributed_membership.md` §7.3 の交換手順。急ぐ必要なし |
| 制御プレーン過半損失(M5 後) | コミット停滞(データは安全) | `distributed_membership.md` §8.1/8.3 |
| primary クラッシュ(M5 後) | 30ms 検出+昇格、ack 済み消失 0 | 自動 failover。旧 primary は I-6/I-7 で自動降格 |
| shipper backlog > 1GB(M5 後) | primary の AddLog が背圧で遅延 | 停滞 voter を remove-replica し再構築(§7.2)。quorum 内なら書き続けられる |

## 11. 成功基準のまとめ

- 計画停止は再起動のみ(データのコピーや变更はすべてオンライン)
- M5 通過後: ack 済みコミットは任意の単一ノード損失で消失しない
- 移行中のいかなる時点でも、ロールバック手順が存在し、ドリル済みであること
