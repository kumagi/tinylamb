# メンバシップ管理仕様

本書は `distributed.md` の詳細仕様書の一つであり、クラスタの構成変更——
ノード追加・削除・交換、レプリカ数変更、primary 交代、制御プレーン自体の
再構成——をすべてこの一冊で運用できる粒度で規定する。
Raft の基礎(構成エントリ R-1〜R-4)は `docs/distributed_raft.md` §10 に置く。

## 1. 2 つのメンバシップ層

構成は 2 つの独立した層で管理する。混同が旧設計の曖昧さの源泉だった。

| 層 | 何を決める | 真実の所在 | 変更手段 |
|---|---|---|---|
| L1: 制御プレーン Raft グループ | メタデータ FSM の投票者(3/5 ノード) | Raft ログ | §4(single-server / joint consensus) |
| L2: シャードレプリカセット | 各シャードの voters/learners/primary(view) | 制御プレーン FSM(真理はシャードノードの WAL) | §6 の FSM コマンド |

重要な性質: **L2 のデータ真理(どのコミットが seal 済みか)はシャードノードの WAL
に存在し、制御プレーンはその鏡像にすぎない**。したがって制御プレーンを全損して
も、シャードノードの報告から sealed_epoch 基線を再構築できる(§8.3)。この
「制御プレーンは再構築可能なキャッシュ」という構図が、本アーキテクチャの
運用性の柱である。

## 2. ノードレジストリ(FSM 状態の一部)

```cpp
struct NodeInfo {
  nodeid_t id;                    // 登録時に固定。アドレス変更でも不変
  std::string host;
  uint16_t control_port = 9100;   // Raft ピア + メタデータ問い合わせ
  uint16_t data_port = 9200;      // shipper/receiver(WAL 受信)
  std::string zone;               // ラック/ゾーン識別子(placement hint)
  uint64_t registered_seq;        // FSM 適用順(観測用)
};
```

- ノード起動時、`{datadir}/node_id` になければ id を発行してもらい書き込む。
  id は「登録順の連番」とし、発行は制御プレーンが行う(二重登録検査は host+port
  と既存 id の突合)。
- レジストリの登録/抹消は可用性にしか影響しない(§6 のシャード構成が安全性を
  持つ)。したがって冪等な再登録を常に許可する。

## 3. 決定論規約:提案時検査と適用時検査

FSM コマンドの検証を 2 段に分ける。これが `distributed_raft.md` §11 の
決定論要求を満たす唯一の書き方である。

- **提案時検査(ベストエフォート、leader 上)**: 生存性・最新性・タイムアウト
  のような、非決定的な情報への依存はここだけに置く。例: 「learner が追いついて
  から promote する」の判断。
- **適用時検査(決定的、全ノードで同一結果)**: 構造不変条件のみ。
  例: `primary ∈ voters`、`voters.size() ∈ [1,5]`、`view は単調 +1`。
  違反コマンドは適用時に fatal(バグの即時発見)。

## 4. L1: 制御プレーンのメンバシップ変更

### 4.1 single-server change(既定方式)

1 エントリで投票者を 1 つだけ増減する。C_old={A,B,C} から D を追加する例:

```
1. [operator] D のノード登録 + 制御プレーンへ AddVoter(D) を提案
2. [leader] D を learner として構成エントリ e1(C + learner D)を追記
   (R-2: 未コミット構成エントリが無いことを確認してから)
3. [leader] D への複製が進み、match_index[D] ≥ commit_index - 8 が
   30 秒以内に成立 → 構成エントリ e2(voters={A,B,C,D}, learner なし)を追記
   追いつかない場合は 60 秒で断念し、e2 の代わりに D 解除の e1' を追記して戻す
4. [leader] e2 がコミット(過半は新構成 {A,B,C,D} の 3/4)で確定。
   以後の過半計算は全ノードが R-1 により新構成で行う
```

削除は逆順に 1 エントリで行う。安全性の根拠は単純で、**1 ノード増減では任意の
新旧過半は必ず 1 ノード以上を共有する**(n と n+1 の過半 ⌈n/2⌉+⌈(n+1)/2⌉ > n)。
共有ノードは両方の構成をログ上持つため、二重コミットが起きない。

制約:
- **リーダー自身を直接削除しない**。先に `TimeoutNow` で移譲
  (`distributed_raft.md` §12)してから削除する。そうしないと旧リーダーが
  自分を含まない構成のコミットを待てず、リーダー不在期間が長引く。
- 削除されたノードは以後 AppendEntries を受けられず、選挙を起こす。
  pre-vote により過半を得られないので無害だが、運用上は当該ノードを速やかに
  停止する(§7 のプレイブック)。

### 4.2 joint consensus(複数ノード一括変更)

single-server change を 2 回繰り返す方式の中間状態(例: voters=4 の過半 3)は
可用性の谷になる。ラック移行や 3→5 一括拡張など、**中間状態を作れない一括変更**
には joint consensus(Raft 論文 §4.3)を使う。既定は single-server、
`--membership-mode=joint` でのみ有効化する。

C_old={A,B,C} から C_new={A,D,E,F} への原子迁移:

```
状態 C(old,new): 構成エントリ jc = { voters_old={A,B,C}, voters_new={A,D,E,F} }
過半判定(m): m の過半 = (旧構成の過半) ∧ (新構成の過半)
  例: 旧 3 の過半=2、新 4 の過半=3 → 承認には旧側 2 票かつ新側 3 票

手順:
1. leader: 未コミット構成エントリがないことを確認(R-2)し、jc を追記。
   ここから全ノードは R-1 により自分のログ上の jc を即座に採用する
2. leader: jc をコミットする(判定は上記の二重過半)。採用済みノードの
   選挙・複製承認はすべて二重過半で行われる
3. leader: C_new 単体の構成エントリ nc を追記し、コミットする
   (判定は新構成の過半のみ。jc のコミットにより新構成側に旧コミット済み
   全体が伝わっていることが保証されている)
4. nc コミット確定で迁移完了。旧構成ノードはもう過半に数えられない
5. 完了後、leader は最後に「C_new 確定」の構成エントリを 1 つ置いてもよい
   (観測用。必須ではない)
```

クラッシュ時の扱い(これが joint の難所。規則を機械的に適用する):
- jc 未コミットで leader が墜ちた → 新 leader のログに jc がある限り、
  その構成(jc)で選出が行われ、新 leader が手順 2 から継続する。
- 新 leader のログに jc が無い(複製前に墜落)→ C_old のまま。手順 1 からやり直し。
- jc コミット済み・nc 未追記で leader が墜ちた → 新 leader は jc 構成で選出され
  手順 3 から継続。**jc コミット後に C_old へ戻ることは決してない**。
- 迁移中に再迁移を開始しない(R-2 により機械的に防止される)。

実装メモ: 「過半判定を構成の種類で切替える」箇所は
`QuorumOf(config, acks)` の 1 関数に集約し、単体テストで
{single, joint}×{選挙, 複製承認} の 4 系統を網羅すること。

### 4.3 learner(追加入の追従者)

- learner は AppendEntries を受信し match_index を更新するが、選挙権も
  過半数カウントも持たない。構成エントリ上は `learners` リストに載る。
- promote 判定(提案時検査): `match_index ≥ commit_index - 8` が 30 秒継続。
- learner は ReadIndex の確認相手にもしない(読み取り線形化性に寄与しない)。
- 制御プレーンとシャード(L2)の両方で同じ役割名を使うが、実体は別物
  (L2 の learner は WAL 受信側。§6.2)。

## 5. L2 のコマンド体系(改訂 MetaCmdType)

```cpp
enum class MetaCmdType : uint8_t {
  kRegisterNode = 1, kDeregisterNode,     // レジストリ(§2)
  kControlConfChange = 10,                // L1 構成エントリ(§4。特殊経路)
  kAddLearner = 20, kPromoteToVoter, kRemoveReplica, kSetPrimary,
  kSetDurability,                         // kLocal / kQuorum(§6.5)
  kSealEpoch = 30,                        // distributed.md §4.4
};
```

旧設計の `kAddNode/kRemoveNode/kShardConfig` を廃止し、L1/L2 を分離した。
`kControlConfChange` は `distributed_raft.md` §10 の構成エントリとして扱われ
(FSM を経由しない特殊エントリ)、FSM 上には監査記録として写る。

## 6. L2: シャードレプリカセット管理

### 6.1 状態と不変条件

```cpp
struct ShardConfig {
  shard_id shard = 0;
  view_t view = 1;                        // fencing token(distributed.md I-6/I-7)
  std::vector<nodeid_t> voters;           // primary を含む。quorum = size/2+1
  std::vector<nodeid_t> learners;         // 追従中。seal 過半に数えない
  nodeid_t primary;                       // voters に含まれること
  epoch_t sealed_epoch = 0;
  std::map<stream_id, stream_lsn> sealed_watermarks;
  DurabilityMode durability = DurabilityMode::kLocal;
};
```

適用時検査(決定的、違反は fatal):
- M-1 `voters.size() ∈ [1,5]`、奇数を推奨(運用規約、強制しない)
- M-2 `primary ∈ voters`、`voters ∩ learners = ∅`
- M-3 `view` は同一シャードで単調 +1(kSealEpoch/kAddLearner/kSetDurability は
  view を動かさない — それらは fencing を必要としない)
- M-4 構成変更系コマンドは view を必ず +1 する
- M-5 kSealEpoch は `(shard, epoch)` について単調増加かつ
  全 stream watermark が既存値以上

### 6.2 レプリカ追加(kAddLearner → kPromoteToVoter)

```
1. [operator] kAddLearner{shard, node} を提案 → FSM に反映
   (view 不変。quorum が変わらないため fencing 不要)
2. [data path] 新 learner は base backup 取得 + WAL tail 追従
   (手順の詳細は distributed_migration.md §M3)
3. [learner] 全 stream で durable_lsn ≥ primary の shipped_lsn - 32KiB が
   60 秒継続したことを control へ報告(提案時検査の入力)
4. [operator or auto] kPromoteToVoter{shard, node} を提案
   → view+1、quorum 計算に即座に組み込まれる
5. [data path] 以後、sealed_epoch の過半判定(primary + quorum-1)に
   新 voter の FlushAck が数えられる。旧 learners の FlushAck は数えない
```

learner の FlushAck は **seal の過半に決して数えない**(未追従レプリカに
コミット耐久性を裏書きさせない)。追従状況の観測には使う。

### 6.3 レプリカ削除(kRemoveReplica)

- 提案時検査(leader、ベストエフォート): 削除後に
  「sealed_watermarks を満たす voter が少なくとも 1 つ残る」ことを確認する。
  **安全性はここに依存しない**。failover の候補選定(distributed.md §6 step 1)
  が watermarks 以上の候補を必ず検証するため、最新 voter を全部捨てていても
  結果は「昇格待ちで可用性が止まる」であって、データ分岐ではない。
  提案時検査は可用性を守るための警告であり、失敗してもコマンド自体は有効。
- view+1。削除されたノードは shipper の配信先から外れ、そのローカル WAL は
  不要となる(オペレータが消去してよい)。

### 6.4 primary 交代(kSetPrimary)

failover(distributed.md §6)と計画停止(メンテナンス)の両方で使う同一コマンド。
計画停止では旧 primary に drain をかけ(書き込み受付停止、open epoch の seal
完了)、sealed_epoch を確定させた上で交代する。これにより計画停止では
コミットロスが常にゼロになる。failover では未 seal 末尾が捨てられる
(ack 前なので許容、I-2)。

### 6.5 durability モード切替(kSetDurability)

`kLocal`(従来通り自ノード fsync で ack)/`kQuorum`(seal 完了で ack)。
マイグレーションの切替点(`distributed_migration.md` §M5)。view は動かさない
(fencing と直交する)。primary は Heartbeat でモード変更を知り、
PreCommit の待ち先を切替える。切替はエポック境界で行う
(旧モードの open epoch を seal してから新モードで開始)。

## 7. 運用プレイブック(コマンド列と期待状態)

実行主体は `tinylamb_admin`(§9)。各段階で `cluster describe` を打ち、
期待状態と突き合わせてから次へ進める手順書形式とする。

### 7.1 ノード追加(3 → 5 台への拡張、single-server path)

```
a. register-node n4 / n5                       (FSM: NodeInfo 追加)
b. add-learner --shard 0 n4 && add-learner --shard 0 n5
c. 両 learner の追従完了を確認(promote 可能表示)
d. promote --shard 0 n4   → voters=4, quorum=3   ←中間状態(過半 3/4)
e. promote --shard 0 n5   → voters=5, quorum=3
```

中間状態 d は過半 3/4(1 台まで耐障害)で、前後(3/3, 3/5)と耐障害性が同格。
危険なのは d のまま長時間運用すること。d→e を続けて実行すること。
(joint path なら a→b→c の後 C_old,new→C_new を 1 迁移で完了し、中間状態を
通らない。§4.2)

### 7.2 ノード交換(n2 故障 → n6 へ)

```
a. register-node n6
b. add-learner --shard 0 n6 → 追従 → promote      (voters=4)
c. control-add-voter n6(制御プレーン L1 へも追加)
d. remove-replica --shard 0 n2 / control-remove-voter n2(移譲後)
e. deregister-node n2。n2 のディスクは消去
```

n2 が生きていても dock してよい(§6.3)。quorum は常時 3 を保つ。

### 7.3 制御プレーンの縮退運転(voters=3 で 1 台長期停止)

1 台欠けても過半 2/3 で制御プレーンは機能する(seal・failover・構成変更とも可)。
exchange を急ぐ必要はない。2 台欠け(過半喪失)は §8.1。

## 8. 異常系と回復手順

### 8.1 制御プレーン過半喪失

seal が止まり、`kQuorum` モードのコミットは停滞する(整合性 > 可用性、仕様)。
`kLocal` モードのシャードは書き続けられる(failover だけできない)。

```
force-new-control-cluster(破壊的、最終手段):
1. 全制御ノード停止
2. ログが最長の 1 台を選ぶ(admin CLI が log 末尾 index を比較表示)
3. 選んだノードで: admin force-single --confirm-danger
   → term+1 の単独構成エントリを書き、単独リーダーとして起動
4. 他ノードを learner として再追加し、§4.1 で復元
```

危険性: 手順 2 より長いログを持つノードが実は生きていた場合、そのログ中の
コミット済み sealed_epoch は失われる。手順 1 の停止確認を怠った場合のみ起きる。
(この場合でも §8.3 の基線再構築により ack 済みコミットの**データ**は
シャードノード側に残る。失うのは制御プレーンの記録で、§8.3 で再構築する。)

### 8.2 シャード voters の過半喪失

データ損失はないが、そのシャードは read-only になる。全 voter を復旧させて
から再開する。ディスクを失った voter は §7.2 の交換手順で再構築。

### 8.3 制御プレーン FSM 全損からの基線再構築

制御プレーンの状態はシャードノードから再構築できる:

```
1. 生きている全 voter/learner に報告を求める:
   last_learned_sealed_epoch, log 末端(stream 毎)
2. 基線 sealed_epoch = min(生存 voter の last_learned_sealed_epoch)
   ← 最も学習の遅れた voter が知っている seal は、quorum で fsync 済みと
     して必ず全員の WAL に存在する(I-1)。min は保守側の安全値。
3. 新制御プレーンで kSealEpoch{epoch=基線} を再コミットし、
   watermarks は生存ノードの実際の durable_lsn の min で確定
4. 以後は通常の seal フローが前進する
```

ack 済みコミットの消失は原理的に起きない(ack には旧制御プレーンでの
コミット済み seal が必要で、それは shard quorum の fsync を意味した)。
失いうるのは「ack 直前で制御プレーンが全損した」コミットの ack のみで、
それはクライアントからは不明なコミット(許容、I-2 の対偶)。

## 9. 運用インターフェース(tinylamb_admin)

```
tinylamb_admin --control host:9100
  cluster describe                     # FSM 状態の全文書式化
  node register|deregister ...
  control add-voter|remove-voter|set-membership-mode
  shard add-learner|promote|remove-replica|set-primary|set-durability
  seal status                          # sealed_epoch, watermarks, 遅延統計
  force-single --confirm-danger        # §8.1
```

admin は任意の制御ノードに接続し、非リーダーなら leader に転送される
(ReadIndex で読み、変更は leader 経由)。**auto-rebalance / 自動ノード交換は
本計画の非目標**。すべての構成変更は明示的なオペレータ操作(または
承認済み runbook の自動化スクリプト)で行う。

## 10. テストマトリクス

| シナリオ | 期待 |
|---|---|
| single-server 追加中の leader クラッシュ | 構成整合・複製継続(R-1/R-3) |
| learner が永遠に追いつかない | 60 秒で断念、状態が元に戻る、集群への影響なし |
| リーダー直接削除(禁止操作)の拒否 | admin が拒否。移譲を案内 |
| joint 迁移中の leader クラッシュ ×3 箇所 | §4.2 の継続規則通り再開、分岐なし |
| promote 直後の即 failover | 新 voter が候補になり得る(quorum 計算の即時反映) |
| learner の FlushAck が seal に数えられること | 数えられないこと(否定的検証) |
| 削除済みノードの再接続・立候補 | 過半不可・pre-vote 不成立で無害 |
| force-single 後の再構築ドリル | §8.1 + §8.3 を通しで実行し状態一致 |
| 決定論シミュレーション | 構成変更を含む乱択で FSM 状態の全ノード一致 |
