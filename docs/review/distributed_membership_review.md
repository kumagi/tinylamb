# docs/distributed_membership.md レビュー指摘事項

## サマリー

本書に対応する実装(L1/L2 メンバシップ管理、FSM コマンド体系、tinylamb_admin)はリポジトリに一切存在しない。`grep` で MetaCmdType / ShardConfig / NodeInfo / kSealEpoch / DurabilityMode / tinylamb_admin のいずれも 0 ヒットであり、「旧設計の kAddNode/kRemoveNode を廃止」(:143)の廃止対象自体がコード上に存在しない。したがって本書は純粋な設計提案であり、§7 プレイブック・§8 障害回復手順は現状いかなる部分も実行できない。文書内部の品質としては 2 層分離(L1/L2)の方針と joint consensus のクラッシュ時規則(:110-116)は Raft 論文と整合しており明快だが、promote 判定の時間条件が §4.1 と §4.3 で食い違い、quorum 記述に式と矛盾する箇所があり、シャード初期作成コマンドと L2 primary 削除手順が欠落しているなど、運用者が事故りやすい穴が複数ある。

## 指摘一覧

### M-1: 「改訂 MetaCmdType」とされるが改訂元も新体系もコード上に存在しない
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:131-145(§5 コマンド体系)、:143(「旧設計の kAddNode/kRemoveNode/kShardConfig を廃止」)
- 問題: 文書は既存の MetaCmdType を改訂する体裁だが、その列挙型はリポジトリのどこにも存在せず、「廃止」の対象も不在。実装状態の表示がないため、読者は FSM コマンド処理が既にあると思い込む。
- 根拠: `grep -rn "MetaCmdType\|ShardConfig\|kSealEpoch\|DurabilityMode\|kRegisterNode" --include="*.hpp" --include="*.cpp"` → build 除き 0 件。nodeid_t も common/constants.hpp:147-151 の型エイリアス群(lsn_t/txn_id_t/page_id_t/slot_t/bin_size_t)に存在しない。
- 提案: 冒頭に「本書は設計仕様。対応実装は distributed.md §11 Phase 2(WP-2a/2b)で未着手」の注記を追加し、§5 を「新設する enum 定義」と明示する。

### M-2: 運用インターフェース(tinylamb_admin)全体が未実装で、§7/§8/§9 の手順がどれも実行不可能
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:218-253(§7 プレイブック)、:255-299(§8 障害回復)、:301-316(§9 CLI)
- 問題: register-node、control add-voter、shard promote、seal status、force-single、cluster describe のすべてに対応する実装・CLI・バイナリが存在しない。「各段階で cluster describe を打ち」(:221)という手順の前提が成立しない。
- 根拠: `grep -rn "tinylamb_admin\|cluster describe\|force-single\|control add-voter" --include="*.cpp" --include="*.hpp" --include="*.txt" . | grep -v build` → 0 件。CMakeLists.txt の実行体ターゲットは tinylamb、tinylamb_server、ベンチマーク類のみ(distributed.md:447-451 の admin_main.cpp 計画も未実装)。
- 提案: 実装不在の注記を追加するとともに、Phase 1〜2 完了まで §7 以降を「ドラフト手順」と明示する。

### M-3: promote 判定の時間条件が §4.1 と §4.3 で食い違い、マジック定数の根拠もない
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:64-66(§4.1 手順 3「match_index[D] ≥ commit_index - 8 が 30 秒以内に成立 → e2 追記」「追いつかない場合は 60 秒で断念」)、:126(§4.3「match_index ≥ commit_index - 8 が 30 秒継続」)
- 問題: 同一の promote 条件が 2 通りに書かれている。「30 秒以内に条件成立したら即 e2」(:64-65)と「条件が 30 秒継続したら promote 可能」(:126)では到達時刻が最大 30 秒ずれ、実装・テストの期待値が一意に定まらない。また :66「60 秒で断念」との関係(30〜60 秒間は何を待つのか、断念判定の開始点)も不明。定数「−8」の単位(Raft エントリ数)と選択根拠、チューニング時の変更基準も説明がない。
- 根拠: 文書内に定数の定義・根拠や両者の整合規定なし。§10 テストマトリクス :323 は「60 秒で断念」としか書かず 30 秒の意味がさらに不明瞭。
- 提案: 条件を 1 文に統一し(例: 「条件成立後 30 秒継続で promote 可、開始から 60 秒で断念」)、タイムライン図を添える。「−8」の意図(heartbeat 50ms × batch 64 を考慮した許容遅延量等)を注記する。

### M-4: 断念時の「e1' を追記して戻す」後の再試行手順と「状態が元に戻る」の意味が未定義
- 区分: 不明瞭
- 対象: docs/distributed_membership.md:66(e1' 追記して戻す)、:323(テスト期待「60 秒で断念、状態が元に戻る、集群への影響なし」)
- 問題: 構成エントリはログに残り続けるため、「戻す」は learner D を外す新エントリ e1' の追記であり、e1 が消えるわけではない。「状態が元に戻る」の正確な意味(有効構成が C + learner D から C へ変わるだけ)と、断念後に D を再度追加する場合の手順(e1' の次にまた e1 相当を追記してよいのか、R-2 の 1 変更制約との絡み)が未規定。
- 根拠: 文書内に再試行手順の記載なし。
- 提案: 「有効構成が C に戻るのみでログは伸び続ける」ことを明記し、断念後の再試行(同じ手順のやり直しで可、R-2 により常に直前構成からの 1 変更)を追記する。

### M-5: ノード登録の二重登録検査と冪等再登録許可の衝突規則が未定義
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:36-40(host+port と既存 id の突合、冪等な再登録を常に許可)
- 問題: (a) 同一 host+port で別ノードが登録しようとした場合(故障交換でのディスク流用等)に拒否するのか上書きするのか不明。(b) 「二重登録検査あり」と「冪等な再登録を常に許可」が競合する条件(同一 id + 変更後 host/port での再登録は可?不可?)が曖昧。(c) deregister 後の id 再利用可否が未定義(id は「登録時に固定、アドレス変更でも不変」(:27)のため再利用禁止のはずだが明文化されていない)。
- 根拠: 文書内に競合時の規則表なし。
- 提案: (id, host+port) の組合せごとの許否表を追加し、id は再利用しない方針を明記する。

### M-6: シャード初期作成のコマンドが L2 コマンド体系に存在しない(migration 文書とも矛盾)
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:134-140(MetaCmdType 列挙)、:151-162(ShardConfig 初期値 view=1)
- 問題: kAddLearner / kPromoteToVoter は既存シャードへの追加を前提とする列挙値で、シャードそのもの(ShardConfig の初期生成)を行うコマンドがない。distributed_migration.md §M2(:103-104)は「初期シャードとして shard 0: voters={n1}, primary=n1, view=1 … を投入」とだけ書き、どのコマンド経路で投入するのかがどちらの文書にもない。
- 根拠: §5 の列挙(:134-140)にシャード作成系の値なし(kRegisterNode はノードレジストリ操作)。適用時検査 M-2(primary ∈ voters)は初期生成時にも課されるはずで、その経路の規定が必要。
- 提案: kCreateShard 相当のコマンドを追加するか、「制御プレーン初回起動時の特殊経路(kControlConfChange 同様、監査記録として FSM に写る)」と規定する。

### M-7: quorum 記述が自文書の計算式と矛盾する(voters=3 時の過半)
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:155(quorum = size/2+1)、:248(§7.2「quorum は常時 3 を保つ」)
- 問題: 式に従えば voters=3 のとき quorum=2、voters=4 のとき 3。しかし §7.2 はノード交換中「quorum は常時 3 を保つ」と述べる。実際は voters 3→4→3 に対し quorum は 2→3→2 と変化し、不変なのは耐障害性(1 台)である。distributed_migration.md §M3(:130)の「voters={n1,n2,n3}, quorum=3」も同様に式から導けない数値であり、seal 過半(I-1)を実装・テストする際に誤った閾値を固定しかねない。
- 根拠: distributed.md:155(§4.2 ShardConfig の「quorum = size/2+1」)。§7.1 d(:229「promote --shard 0 n4 → voters=4, quorum=3」)は式通りであり、同文書内でも基準が揺れている。
- 提案: 「quorum は常時 3」を「voter 数 3 台・耐障害性 1 台を保つ(quorum は 2↔3 で変動)」に修正し、migration 文書側の「quorum=3」表記も式に合わせる。

### M-8: kRemoveReplica を primary に対して実行する場合の扱いが未定義
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:191-200(§6.3 レプリカ削除)
- 問題: L1(single-server change, :76-78)では「リーダー自身を直接削除しない。先に TimeoutNow で移譲」と制約があるのに、L2 の kRemoveReplica には削除対象が現 primary だった場合の手順(kSetPrimary を先行させる等)がない。primary ∈ voters(M-2 適用時検査)を維持したまま primary を外す操作は構造的に不可能であり、適用時 fatal(:50-52)になるか、事前の kSetPrimary 必須という運用制約が必要。
- 根拠: 文書内に L2 primary 削除の規定なし。M-2 検査(:166)との矛盾が生じる。
- 提案: 「削除対象が primary の場合は必ず kSetPrimary を先行させる(提案時検査で拒否)」を §6.3 に追記する。

### M-9: force-single の term+1 が旧ノード生存時の term 衝突を考慮していない
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:266-267(term+1 の単独構成エントリを書き、単独リーダーとして起動)、:271-274(危険性の記述)
- 問題: 危険性として「より長いログを持つノードが生きていた場合」のみを論じるが、term の観点では、停止漏れの旧ノードが force-single 後の term より大きい term を保持していた場合、pre-vote・選出の攪乱(distributed_raft.md §14 E-2 が指摘する現象)が起きる。「term+1」の出所(誰の term に対する +1 か)も特定されない。
- 根拠: distributed_raft.md:120-128(§5 共通規則: msg.term > current_term のときのみ採用)、同 :380(E-2 pre-vote 未実装時の攪乱)。本書 :267 は term+1 の基準を書かない。
- 提案: 「観測可能な全制御ノードの term 最大値 + 1 を用いる」等の手続き化と、停止漏れ検出(pre-vote 不成立継続時の警告)を手順に追加する。

### M-10: 基線再構築(§8.3)の min 採用が安全となる条件と learner 報告の扱いが不明確
- 区分: 不明瞭
- 対象: docs/distributed_membership.md:286-294(min(生存 voter の last_learned_sealed_epoch)、watermarks を durable_lsn の min で確定)
- 問題: (a) min が過小になった場合(例: 遅延 voter が 1 台残るのみで多数が喪失)の影響(seal 前進の遅延幅)が評価されていない。(b) watermarks を「生存ノードの実際の durable_lsn の min」で確定すると、ディスク喪失から復旧済み voter がいた場合に過去に合意済みの sealed_watermarks より小さくなる可能性があり、それが許容される理由(単なる前進遅延でデータ分岐ではないこと)が読み取れない。(c) :286 は voter/learner 両方に報告を求めるが、min 計算に learner の報告を入れるのか除外するのか不明確(learner の last_learned_sealed_epoch は fsync 保証がなく I-1 の根拠が使えない)。
- 根拠: 文書内の上記箇所以外に規定なし。
- 提案: 「min は過小でも安全性を損なわない(seal は単調増加のため再進行するだけ)」「min に入れるのは voter のみ、learner の報告は観測用」等を明記する。

### M-11: 用語・表記の問題(簡体字混入、L1/L2 での learner 同名異義)
- 区分: 不明瞭
- 対象: docs/distributed_membership.md:90「原子迁移」、:105「迁移完了」、:116「迁移中に再迁移」、:235「1 迁移で完了」、:325「joint 迁移中」; :122-129(learner の二重使用)
- 問題: (a) 日本語文書に簡体字「迁移」が 5 箇所用いられており(正しくは「移行」「遷移」)、検索性・用語統一の面で問題。migration 文書にも類似の簡体字「变更」がある。(b) §1(:10)は「混同が旧設計の曖昧さの源泉だった」と述べた直後に、L1(Raft learner)と L2(WAL 受信側 learner)に同じ名前を使い続けており、§4.3(:128-129)が「同じ役割名…実体は別物」と自認する以上、文書上も区別できる呼称(例: control-learner / shard-learner)を与えるべき。
- 根拠: 上記行番号の通り。distributed_raft.md §3(:42)の Role::kLearner は L1 専用の文脈。
- 提案: 「迁移」→「移行」へ統一修正し、L2 の learner について文書内で一貫した区別表記を導入する。

## 未検証事項

- §4.2 joint consensus のクラッシュ 3 箇所継続規則(:110-116)の網羅性(jc コミット済み・nc 未コミットで新 leader のログに nc が部分的に残るケース等)はシミュレーション不存在のため検証していない。
- §10 テストマトリクス「負荷中の add/remove/promote でコミット停止 < 1 秒」(distributed.md :483 由来)は実装・計測とも不存在のため評価不能。
- promote 直後の failover 候補選定(:326「新 voter が候補になり得る」)と distributed.md §6 step 1(I-5: durable_lsn ≥ sealed_watermarks の候補検証)の整合は、I-5 を満たす候補のみ昇格する前提で読めば矛盾しないが、テスト記述としては前提が省略されており誤解を招く(本レビューでは指摘 M-7 に含めた)。
