# 制御プレーン Raft 実装仕様

> **Status(2026-08-24)**: 設計段階(未実装)。レビュー指摘(用語未定義・手順矛盾・境界未規定等)は
> docs/review/ に記録済み。実装着手時に必ず履歴を参照して本書を更新すること。


本書は `distributed.md` の詳細仕様書の一つであり、制御プレーン Raft を
**Raft の先行知識なしで実装できる**粒度で規定する。本文書単体で完結している。
メンバーシップ変更(本書 §10 の基礎の上に立つ)は `distributed_membership.md`、
運用手順は `distributed_migration.md` を参照すること。

## 1. スコープと負荷特性

Raft が複製するのは制御プレーンのメタデータ FSM のみ(クラスタ構成、シャード設定、
sealed_epoch)。トランザクションデータは一切経由しない(`distributed.md` §1)。

- エントリ生成源: seal 1 件/epoch/shard(既定 5ms 間隔で ~200 entry/sec)、
  構成変更は疎。エントリサイズは数十バイト〜数 KB。
- 読み取り: router・データノードからの構成問い合わせ(~100 QPS 程度)。
- この負荷特性より、実装は「正しく単純」を目指す。pipelining の window 管理や
  lease read 等の最適化は行わない(§13 参照)。

## 2. Raft の要点(前提知識ゼロ向け要約)

Raft は「複数ノードでログを同じ順序に複製する」問題を、**term(任期)と呼ばれる
単調増加の論理時計**と**単一リーダー**で解くアルゴリズムである。

- すべてのノードは「リーダーが決めたログの順序」に従い、同じコマンド列を同じ順番で
  適用することで同じ状態に収束する。
- term は論理的な世代番号。大きい term は常に小さい term に勝つ。
  リーダーが疑われるたびに term が増え、古いリーダーの決定は新しい term の前に
  必ず無効化される。
- リーダーは「有効投票者の過半」にエントリを複製できたとき、そのエントリを
  **コミット済み**と判定する。コミット済みエントリは以後いかなる故障でも消失しない。
- 各ノードは永続化した term と「今の term で誰に投票したか」を忘れない限り、
  同一 term で 2 人に投票しない。これが「同一 term に最大 1 リーダー」
  (選出安全性)の源泉である。

## 3. 型と状態

```cpp
using nodeid_t = uint32_t;   // common/constants.hpp に追加。登録時に固定、
                             // アドレス変更でも不変。0 は無効値。

enum class Role : uint8_t { kFollower, kCandidate, kLeader, kLearner };
// kLearner: 選挙権・過半数カウントに含まれない追従者(メンバシップ変更用)。

struct RaftEntry {
  uint64_t term;      // 追記したリーダーの term
  uint64_t index;     // 1 始まりの連続番号
  std::string cmd;    // FSM コマンドのバイト列(§11)
};

// —— 永続化必須(§8 のファイルに書く。再起動で失うと安全性が崩れる)——
uint64_t     current_term = 0;              // 最後に観測した term(自分のものも含む)
std::optional<nodeid_t> voted_for;          // 今の term で投票した相手
std::deque<RaftEntry>  log;                 // 先頭は必ず index=1 …とは限らない
                                             // (スナップショットで先頭が切れる。§9)

// —— 揮発(再起動でゼロから)——
uint64_t commit_index = 0;   // 過半複製が確定した最大 index
uint64_t last_applied = 0;   // FSM へ適用済みの最大 index
Role     role = Role::kFollower;
// リーダーのみ(ピア毎):
std::unordered_map<nodeid_t, uint64_t> next_index;   // 次に送る index
std::unordered_map<nodeid_t, uint64_t> match_index;  // 複製確認済み index
```

有効投票者集合(voters)と learner 集合は「自分のログ末尾から遡って最初に現れる
構成エントリ」から決まる(§10)。過半数計算は常にこの最新の自ログ上の構成に対して
行う。

## 4. メッセージとワイヤ形式

全 RPC は 1 本の長寿命 TCP コネクション(ピア毎、双方向で 1 本)で流す。
ノード間で 9100/tcp を使用(既定、`--control-port` で変更可)。

```
フレーム: [u32 body_len][u8 version=1][u8 msg_type][u64 rpc_seq][body...][u32 crc32c]
  crc32c は body 全体(ヘッダ含む)に対して計算(common/crc32c.hpp を使用)。
  rpc_seq はピア毎に単調増加。応答は要求の rpc_seq を echo する。
  → 遅延到着した古い応答を破棄でき、べき等でない操作の二重適用を防ぐ。
```

```cpp
enum class MsgType : uint8_t {
  kRequestVote, kRequestVoteReply,
  kAppendEntries, kAppendEntriesReply,
  kInstallSnapshot, kInstallSnapshotReply,
  kTimeoutNow,                       // リーダーシップ移譲(§12)
};

struct RequestVote {
  bool pre_vote;                     // §6 の pre-vote 段
  uint64_t term;                     // pre_vote なら提案 term(current+1)
  nodeid_t candidate;
  uint64_t last_log_index, last_log_term;   // 候補者のログ末尾
};

struct AppendEntries {
  uint64_t term; nodeid_t leader;
  uint64_t prev_log_index, prev_log_term;   // 直前エントリの一貫性検査用
  std::vector<RaftEntry> entries;           // 空=heartbeat
  uint64_t leader_commit;
};

struct AppendEntriesReply {
  uint64_t term; bool success;
  uint64_t match;                    // success 時: 受信者が新たに持った末尾 index
  // success=false の衝突ヒント(fast back-search):
  uint64_t conflict_term;            // 衝突位置のエントリの term(不明なら 0)
  uint64_t conflict_index;           // その term を持つ自分のログ内最初の index
};

struct InstallSnapshot {
  uint64_t term; nodeid_t leader;
  uint64_t last_included_index, last_included_term;
  std::string fsm_bytes;             // FSM スナップショット(CRC はフレームが担保)
};
```

## 5. 共通規則(全メッセージ受信時の最初の処理)

```
if (msg.term > current_term) {
  current_term = msg.term;
  voted_for = std::nullopt;
  persist_meta();                    // fsync 後に処理を続行(§8)
  role = kFollower;                  // リーダー/候補でも即フォロワー化
}
// pre-vote の RequestVote は上記を行わない(§6)。これが pre-vote の本質。
```

## 6. リーダー選出

タイマ: 各ノードは `election_timeout` を **[400ms, 800ms] の一様乱数**で引き直し、
有効なリーダー由来の AppendEntries を受信するたびにリセットする。
learner は立候補しない。

```
on election_timeout():            // follower のみ
  // 第 1 段: pre-vote(状態を壊さない事前調査)
  proposed_term = current_term + 1
  grant = 0
  for each voter(v自ノードを含む): RequestVote{pre_vote=true, term=proposed_term, ...} を送信
        → 過半の grant が揃うまで待つ(タイムアウト=再度引き直してやり直し)
  過半に満たない場合: 何も状態変更せず待機し直す。
  // 効果: リーダーから分離されたノードが term を無闇に増やして
  //        復帰後に集群を攪乱するのを防ぐ(§14 E-2)。

  // 第 2 段: 本選挙
  current_term += 1; voted_for = self; persist_meta();   // ←fsync 必須
  role = kCandidate; votes = 1(自分)
  for each voter: RequestVote{pre_vote=false, ...} を送信

on RequestVoteReply(term, granted):
  if (role != kCandidate || term != current_term) 破棄   // 出遅れ応答
  if (granted && ++votes > voters.size()/2) become_leader();
```

```
on RequestVote(pre_vote, term, candidate, last_log_index, last_log_term):
  bool up_to_date =
      last_log_term > LogLastTerm() ||
      (last_log_term == LogLastTerm() && last_log_index >= LogLastIndex());
      // 「ログが自分以上に新しい」判定。古いログの候補者はリーダーになれない
      // (コミット済みエントリの上書き防止、選出安全性 P-2)。
  if (pre_vote) {
    grant = up_to_date && (最後の leader 通信から election_timeout 経過);
    reply(current_term, grant);                       // 永続化しない
  } else {
    grant = up_to_date &&
            (!voted_for || voted_for == candidate);
    if (grant) { voted_for = candidate; persist_meta(); }  // 応答前に fsync
    reply(current_term, grant);
  }
```

```
become_leader():
  role = kLeader
  for each peer: next_index[p] = LogLastIndex() + 1; match_index[p] = 0
  log.push_back({current_term, LogLastIndex()+1, kNoop});   // ←no-op エントリ
  persist_log(); 全ピアへ即座に AppendEntries 送信
  // no-op の意義: 選出直後、先行 term のエントリだけがログに残っている場合が
  // ある。現 term の no-op をコミットする際にそれらが間接的に確定する(§7)。
  // no-op がコミットされるまで ReadIndex(§12)の読み取りを拒否する。
```

## 7. ログ複製とコミット

### 7.1 リーダー側

```
replicate():                       // heartbeat(50ms)毎 + エントリ追記時
  for each peer p:
    if (next_index[p] <= LogFirstIndex()-1) → InstallSnapshot を送る(§9)
    else AppendEntries{
           prev_log_index = next_index[p]-1,
           prev_log_term  = log[normalize(next_index[p]-1)].term,
           entries        = log[next_index[p] .. min(next_index[p]+kMaxBatch, last)],
           leader_commit  = commit_index } を送信(kMaxBatch=64)

on AppendEntriesReply(rpc_seq, term, success, match, conflict_*):
  if (term > current_term) → §5 の共通規則へ(リーダー失任)
  if (応答の rpc_seq が最新要求と不一致) 破棄
  if (success) { match_index[p] = match; next_index[p] = match + 1; }
  else {
    next_index[p] = conflict_index > 0 ? max(conflict_index, 1)
                                       : next_index[p] - 1;   // ヒント無ければ 1 減
    // conflict_index の意味: 受信者は「自分のログで衝突 term を持つ最初の位置」を
    // 返す。リーダーはそこまで一気に戻して再送する(1 ずつ戻す線形探索を回避)。
  }
  // コミット判定:
  N = max{ n : log[n].term == current_term &&
                (1 + |{p ∈ voters, p≠self : match_index[p] ≥ n}|) > voters.size()/2 }
  if (N > commit_index) { commit_index = N; apply_to_fsm(); }
```

**コミット規則の「現 term 条件」は省略できない**。`log[n].term == current_term`
を課さずに過半複製だけでコミットすると、term 2 の一時過半が作ったエントリが、
term 3 の正リーダーによって別内容に上書きされ、既に適用済みのノードと分岐する
構成が存在する(Raft 論文 figure 8)。no-op 導入とこの条件の組み合わせにより、
「選出されたリーダーのログはコミット済みログ全体を必ず含む」(P-3)が成立する。

### 7.2 受信者側(フォロワー/learner)

```
on AppendEntries(term, leader, prev_log_index, prev_log_term, entries, leader_commit):
  if (term < current_term) reply(false);
  role = kFollower(leader 由来の通信として election タイマをリセット)
  if (LogLastIndex() < prev_log_index)
    reply(false, conflict_term=0, conflict_index=LogLastIndex()+1);
  if (log[normalize(prev_log_index)].term != prev_log_term) {
    t = log[normalize(prev_log_index)].term;
    reply(false, conflict_term=t, conflict_index=自分のログで term t の最初の index);
  }
  for e in entries:
    if (自分のログに index(e) がある) {
      if (log[e.index].term != e.term)
        { log を index(e) 以降で切り詰め; 以降の e を追記; }
      // term が一致する既存エントリは内容も一致する(ログ整合性 P-4)。スキップ。
    } else log.push_back(e);
  persist_log();                    // ←fsync。これが成功応答の耐久性の本体。
  if (leader_commit > commit_index) {
    commit_index = min(leader_commit, 受信により新たに持った末尾 index);
    apply_to_fsm();
  }
  reply(success, match=新末尾 index);
```

切り詰めは「リーダーが決めた正史への服従」である。分離していた旧リーダーの
未コミット末尾はここで破棄される。

### 7.3 FSM 適用

```
apply_to_fsm():                    // Raft グループ内で必ず単一スレッドから呼ぶ
  while (last_applied < commit_index) {
    ++last_applied;
    fsm_->Apply(log[normalize(last_applied)].cmd);   // 決定的であること(§11)
    apply_cv_.notify_all();       // ReadIndex の待ち人を解放
  }
```

## 8. 永続化形式

```
{controldir}/meta          … 行形式 "term voted_for" を追記。
                             起動時に最終の完全行を採用(途中の破損行は無視)。
                             追記+fsync なのでクラッシュ安全性が自明。
{controldir}/snap_%020u    … FSM スナップショット(§9)
{controldir}/log_%020u.seg … エントリ列:
  [u32 body_len][u64 term][u64 index][bytes cmd][u32 crc32c]
  セグメントは 4MiB 毎に分割。スナップショット済み先頭側から丸ごと削除できる。
  末尾切り詰め(衝突時)は fsync 後に ftruncate してから再度 fsync。
```

fsync すべき瞬間は正確に 2 つ:「投票の応答前」(§6)と「AppendEntries の成功応答前」
(§7.2)。この 2 点を守れば、応答済みの投票・応答済みの複製は再起動後も再現される。
fsync はエントリ到着群をまとめて 1 回行ってよい(group commit)。
CRC 不一致・長さ不一致の末尾エントリは破棄する(応答前のエントリは fsync 済みで
あるから、破棄してよいのは未応答分のみであり安全性に影響しない)。

## 9. スナップショットと InstallSnapshot

制御 FSM の状態は小さい(シャード数×設定)。`last_applied - LogFirstIndex()`
が 100,000 エントリまたは log サイズ 64MiB を超えたらスナップショットを取得する。

```
leader: peer p の next_index[p] < LogFirstIndex() のとき
  InstallSnapshot{last_included_index/term, fsm->Snapshot()} を送る。
  以後 next_index[p] = last_included_index + 1 から再開。

receiver on InstallSnapshot:
  tmp ファイルへ書き出し → crc 検査 → rename(snap_%020u)
  log を空にし、先頭を last_included_index とみなすダミー境界を置く
  fsm_->Restore(bytes)
  commit_index = last_applied = max(両者, last_included_index)
  永続化後、success reply
```

スナップショットには「適用時点の有効構成」を含めること(§10 の構成決定規則が
スナップショット復元後も成立する必要がある)。

## 10. 構成エントリ(メンバシップ変更の基礎)

構成変更は FSM コマンドではなく **Raft ログへの特殊エントリ**として表現する
(FSM を経由しない)。規則:

- R-1: サーバの有効構成は「**自分のログを末尾から遡って最初に見つかった構成
  エントリ**」。コミット済みかどうかによらない。AppendEntries の過半判定・
  投票の付与・過半計算はすべてこの構成で行う。
- R-2: リーダーは **未コミットの構成エントリがログ内に存在する間、次の構成
  エントリを追記できない**(1 度に 1 変更)。
- R-3: リーダーは自身の構成エントリのコミットを `match_index` の過半(新構成に
  おいて)で確認し、確定を待ってから次の操作に出る。
- R-4: 構成エントリも通常エントリと同様に複製・コミットされる(通常エントリの
  保存と同じファイル・同じ fsync 規則)。

この基盤の上の single-server change と joint consensus のプロトコル詳細は
`distributed_membership.md` に置く。本書の実装者は R-1〜R-4 と「過半計算を
メッセージ受信時点の自ログ構成に対して行う」ことだけを守ればよい。

## 11. FSM インターフェースと決定論規約

```cpp
class MetaFsm {
 public:
  void Apply(std::string_view cmd);   // 同一 cmd 列 → 全ノード同一状態
  std::string Snapshot() const;       // 状態全体のバイト列(crc は呼び出し側)
  void Restore(std::string_view snapshot);
};
```

- Apply は純粋にコマンド列から状態を決定論的に導くこと。壁時計・乱数・ネット
  状況・ローカルの健全性情報を Apply の結果に混ぜてはならない(それらは
  **提案側の事前検査**にのみ使う。`distributed_membership.md` §3)。
- コマンドの先頭バイトはバージョン。FSM は未知バージョンを拒否(fatal)する。
  ローリングアップグレード規約は `distributed_migration.md` §2。

## 12. ReadIndex(線形化可能読み取り)とリーダー移譲

**ReadIndex**: 構成問い合わせ(router、データノード)をログ追記なしで線形化
可能に読む手順。

```
on read request at leader:
  if (no-op が未コミット) → 「リーダー確定待ち」エラーで拒否
  read_index = commit_index
  空の AppendEntries(current_term)を全 voter に送り、current_term のまま
  過半の成功応答を得る(100ms で揃わなければ失敗応答)
  last_applied >= read_index まで待ってから FSM 状態から応答
```

過半確認により「自分が今も現 term のリーダーである」ことを保証してから読む。
これを省略すると分離した旧リーダーが古い構成を配布し得る(可用性問題にはなるが
view fencing により安全性は保たれる。それでも本実装では毎回確認を課す)。

**リーダー移譲**(`distributed_membership.md` の削除手順が使用):
リーダーは `TimeoutNow` を対象ノードに送り、自身は新エントリ受付を停止する。
受領者は即座に選出タイマを満了扱いにして立候補する。タイムアウト後は移譲失敗
として通常選出に戻ってよい。

## 13. タイムパラメータ(初期値)

| パラメータ | 初期値 | 備考 |
|---|---|---|
| Raft heartbeat | 50ms | AppendEntries 空 送信間隔 |
| election timeout | 400–800ms 一様乱数 | 選出のたびに引き直し |
| ReadIndex 確認タイムアウト | 100ms | 揃わなければ読み取り失敗 |
| batch 最大エントリ | 64 | |
| スナップショット閾値 | 100k entry / 64MiB | |
| 構成配布ハートビート(データ面) | 10ms | 本書の RPC とは別物(`distributed.md` §4.3)|

注意: 「Raft heartbeat(ピア間、50ms)」と「制御プレーン→データノードへの構成
配布ハートビート(10ms)」は別の通信である。後者は Raft リーダーの FSM 状態を
読んで(ReadIndex 相当の確認付きで)ユニキャスト配布するだけのアプリ層処理で
ある。

## 14. 落とし穴チェックリスト(実装レビューで確認)

- E-1: 応答前に fsync を忘れる(投票・複製の 2 箇所)。最悪のバグ。
- E-2: pre-vote 未実装。分離ノードの term 暴走で正常リーダーが延々失任する。
- E-3: コミット判定の現 term 条件の省略(fig.8 分岐)。
- E-4: 出遅れた AppendEntriesReply を rpc_seq 無しで処理し、match_index を
  巻き戻す。
- E-5: 構成エントリを「コミットされたら有効」にしてしまう(R-1 違反)。
  選出中の構成遷移が停止する。
- E-6: FSM Apply を複数スレッドから呼ぶ(適用順が分岐する)。
- E-7: 選出直後の no-op 未コミット状態で読み取り/構成変更を許可する。
- E-8: ログ切り詰めを「追記だけ」で実装し、衝突末尾が残る。
- E-9: スナップショットに有効構成を含めない(復元後の過半計算が崩れる)。
- E-10: rpc_seq・term の等値検査を忘れ、旧 term の自分への応答を処理する。

## 15. テストマトリクス

| シナリオ | 期待 |
|---|---|
| リーダー kill → 選出 | 400–800ms 以内に新リーダー。コミット済みは全ノード保持 |
| 分離旧リーダー復帰 | 末尾を切り詰めて追従。term は有情報で吸収 |
| pre-vote: リーダー生存下の分離ノード | term 増加なし・選出攪乱なし |
| 衝突ログの再送(conflict hint) | 一致位置まで一気に後退し再送で収束 |
| コミット直前クラッシュのエントリ | 過半未到達なら上書きされ得る(仕様)。応答済みは不滅 |
| ReadIndex 中の分離 | 過半確認失敗→読み取り失敗。古い構成の配布なし |
| InstallSnapshot 復元 | 全状態一致・以降の複製継続 |
| rpc_seq 入れ替え・重複配送 | 出遅れ応答の破棄・べき等な再処理 |
| 決定論的シミュレーション乱択 | 100万ステップ、P-1〜P-5 不変表明違反 0 |

安全性性質(テストの表明に使う):
P-1 選出安全性: 同一 term に高々 1 リーダー。
P-2 リーダー完全性: 選出されたリーダーのログはコミット済み全エントリを含む。
P-3 ログ整合性: index と term が一致する 2 エントリの内容は一致し、前置も一致。
P-4 機械安全性: FSM 適用順は全ノード同一。
P-5 リーダー付加性: コミットは常に現 term の過半複製を経由する。
