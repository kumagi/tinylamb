# docs/distributed_raft.md レビュー指摘事項

## サマリー

本書に対応する実装(Raft 制御プレーン)はリポジトリに一切存在しない。ディレクトリにもファイルにも現れず(`find` で `distributed/` 等なし)、`grep -ril "raft\|membership\|migration\|leader"` の全ヒットが無関係(IN サブクエリの membership キャッシュ、LSM テストの key membership、テスト名の部分文字列)であることから確定した。したがって本書は純粋な設計提案であり、指摘の中心は「その旨の状態表示がないこと」と「実装者がつまずく仕様の穴」である。一方で、既存資産への参照(`common/crc32c.hpp`)だけは正確で、親文書 `distributed.md`(§4.1, §13)との相互参照も構造的には一致している。プロトコル本体(pre-vote、fsync 2 箇所、コミットの現 term 条件)は Raft 論文の規範と整合しているが、wire 形式の未定義メッセージ型、境界条件の未規定、**本文中の安全性性質 P 番号参照が §15 の定義と 3 箇所すべて食い違う**内部矛盾がある。

## 指摘一覧

### R-1: 対応実装が存在しないことが文書自体に書かれていない
- 区分: 実態との乖離
- 対象: docs/distributed_raft.md:1-6(冒頭)、全文
- 問題: 本書は「実装できる粒度で規定する」設計仕様であるが、対応する実装(distributed/raft/、distributed.md §11 の WP-1a〜WP-1c)は 1 行も存在しない。文書単体では「既に動いているものの仕様」か「これから作るものの仕様」か判別できず、コードレビュー・引き継ぎ時に誤解を生む。
- 根拠: `find . -type d -not -path "./build*" -not -path "./.git*"` に distributed/ なし。`grep -ril "raft\|membership\|migration\|leader" --include="*.cpp" --include="*.hpp" . | grep -v build` のヒットは executor/detail/subquery_runtime.hpp:53(IN サブクエリの `uncorrelated_membership`)、executor/detail/planning_heuristics.cpp:789-807(同変数)、index/lsm_tree_test.cpp:536(LSM のキー membership)、server/postgres_server_test.cpp:2230(テスト名 "Erro**raft**erCommit" 内の部分文字列一致)のみで、いずれも分散無関係。親文書 distributed.md:22「ネットワーク関連のコードは現在ワイヤサーバしか存在しない」の記述は本書に反映されていない。
- 提案: 冒頭に「本書は設計仕様。対応実装は distributed.md §11 Phase 1(WP-1a〜1c)であり 2026-08 時点で未着手」等の状態注記を追加する。

### R-2: 既存資産と新設物の区別が現在形で書かれており読み手が混同する
- 区分: 実態との乖離
- 対象: docs/distributed_raft.md:38(nodeid_t を common/constants.hpp に追加)、:72(--control-port)、:76(common/crc32c.hpp を使用)
- 問題: 同じ断定形で書かれた参照先のうち、実在するのは crc32c.hpp のみ。nodeid_t は constants.hpp に未追加(:38 自体は「に追加」と未来形なので例外的に正しい)、`--control-port` フラグはどこにも存在せず、フラグ解析基盤自体がサーバ main にしかない。「〜を使用」「〜で変更可」と読むと既存機能と誤認する。
- 根拠: common/constants.hpp:147-151 の型エイリアスは lsn_t/txn_id_t/page_id_t/slot_t/bin_size_t のみで nodeid_t/epoch_t/view_t/stream_id/shard_id は無し。server/postgres_server_main.cpp:21-68 は `--host`/`--port`/`--read-workers`/`--help` のみを解析。common/crc32c.hpp と common/crc32c_test.cpp は実在(CMakeLists.txt:561 で add_simple_test 登録済み)。
- 提案: 既存資産には「既存」、新設物には「新設(WP-0b で追加)」と明示的にラベル付けする。

### R-3: RequestVoteReply / InstallSnapshotReply / TimeoutNow のボディ形式が未定義
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:82-87(MsgType 列挙、7 型)、:152-155・:158-173(reply(current_term, grant) の使用)、:356-359(TimeoutNow 送信)
- 問題: 定義済み構造体は RequestVote / AppendEntries / AppendEntriesReply / InstallSnapshot の 4 種のみで、列挙された 7 型のうち 3 型の wire 形式がない。(a) pre-vote 要求への応答と本選挙要求への応答を候補者がどう識別するか(rpc_seq 突合で原理的には可能だが明記がなく、E-10 の「rpc_seq・term の等値検査」を実装するための対応表がない)、(b) InstallSnapshotReply のフィールド(転送確認・再送に必要)が不明。(c) InstallSnapshot は fsm_bytes を 1 フレームで送る設計だが、スナップショット閾値が log 64MiB(:283-284)である以上 FSM が数十 MiB になり得ず、u32 body_len 1 本のフレームでの受信側メモリバッファリング・分割転送の要否に言及がない。
- 根拠: 文書内に RequestVoteReply / InstallSnapshotReply / TimeoutNow の構造体定義は存在しない。
- 提案: 全 msg_type の構造体を定義する(空ボディならそう明記)。RequestVoteReply には pre_vote echo を含めることを推奨。スナップショット分割転送(chunk + reply ack)の要否を決める。

### R-4: normalize() / LogFirstIndex() 等の内部関数とスナップショット後境界の扱いが未定義
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:53-54(「先頭は必ず index=1 …とは限らない」のコメント)、:196,:230-232,:257(log[normalize(index)])、:228-229(受信側の conflict 応答)
- 問題: 疑似コードの中核を担う normalize(index)(絶対 index → コンテナ位置)が一度も定義されない。特に §7.2 受信者側で `prev_log_index < LogFirstIndex()`(自分がすでにスナップショットで compact 済みの領域をリーダーが指定してきた場合)の分岐が規定されておらず、疑似コードを素直に実装すると `log[normalize(prev_log_index)]`(:230)が範囲外アクセスになる。この分岐の返答規則を誤ると P-3 検査と InstallSnapshot フォールバックの両方を壊す。
- 根拠: 文書内に normalize / LogFirstIndex / LogLastTerm / LogLastIndex の定義箇所はない(§9 :293 の「ダミー境界を置く」が唯一の言及)。
- 提案: ダミー境界エントリ(term=last_included_term を保持する番兵等)の具体形と normalize の定義を書き、「prev_log_index < LogFirstIndex() のときは conflict_term=0・conflict_index=LogFirstIndex() で拒否し、リーダーは InstallSnapshot へフォールバック」等の境界規則を追記する。

### R-5: pre-vote の grant 条件が曖昧(タイマ値・自己票・応答対応)
- 区分: 不明瞭
- 対象: docs/distributed_raft.md:165「grant = up_to_date && (最後の leader 通信から election_timeout 経過)」、:141(v 自ノードを含む)、:152-155
- 問題: (a) 「election_timeout 経過」判定に [400ms,800ms] の乱数のどの値を使うのか(直近に引いた値か、固定値か)不明。(b) pre-vote の過半計算に自己票を無条件で数えるのか不明。(c) 「最後の leader 通信」に旧 term の AppendEntries や learner 由来の通信を含むのか不明。(d) on RequestVoteReply(:152-155)は pre-vote 応答と本選挙応答を同一ハンドラで扱っており、pre-vote 待ち中に本選挙応答が到着した場合の処理が未定義。
- 根拠: 文書内にこれらの規定はない。§13(:366)は election timeout を「400–800ms 一様乱数」としか定めない。
- 提案: 判定用タイマの値(例: 直近に引いた election_timeout 値を使用)、対象通信の範囲(term 一致の leader 由来のみ)、自己票の扱い、応答対応規則(R-3 の pre_vote echo と併せて)を明文化する。

### R-6: no-op エントリ(kNoop)の形式が FSM 適用規約と衝突する
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:179(log.push_back({current_term, ..., kNoop}))、:183(no-op コミットまで ReadIndex 拒否)、:254-260(apply_to_fsm は全エントリを Apply)、:335(FSM は未知バージョンを fatal 拒否)
- 問題: kNoop の cmd バイト列表現が定義されないまま、apply_to_fsm は全エントリを fsm_->Apply に渡す。§11 の規約(先頭バイト=バージョン、未知は fatal)に素直に従うと no-op 適用で全ノードが落ちる。no-op を Raft 層で skip する場合、last_applied の進め方と ReadIndex の待ち合わせ(:348-349)との整合(last_applied が no-op を飛び越えて進んでよいか)を決める必要がある。
- 根拠: 文書内に kNoop のバイト表現・適用可否の規定なし。
- 提案: no-op 専用コマンド形式(例: version バイト + type バイト)を定義し、Apply は no-op を状態変更なしで受理すると明記する。または apply_to_fsm での skip 規則と last_applied の進行規則を追記する。

### R-7: meta ファイルの「破損行は無視」に検出手段の規定がない
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:265-267(行形式 "term voted_for" を追記、途中の破損行は無視)
- 問題: voted_for が無投票(optional 空)のときの行表現、「完全行」の判定基準(改行終端?)、破損検出手段が未定義。クラッシュ時の torn write で数字が半壊しても CRC 等がなければ「破損行」と機械的に判定できない(log セグメントには CRC32C があるのに meta には言及すらない)。投票応答前 fsync(E-1)の安全性はこのファイルの読み込み規則に依存するため、曖昧さは最悪のバグ筋に直結する。
- 根拠: 文書内にフォーマット詳細の規定なし。
- 提案: 具体フォーマット(例: 10 進 term + 空白 + voted_for(-1 = 無投票)+ '\n'、'\n' 終端のみ完全行として採用)を定義する。CRC 付加を推奨。

### R-8: ReadIndex 手順の細部(last_applied 待ちのタイムアウト不在ほか)
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:344-350、§13 表 :367
- 問題: (a) 「last_applied >= read_index まで待つ」にタイムアウト規定がなく、commit 停滞時に読み取り要求が永久待機し得る(100ms のタイムアウトは過半確認のみに適用)。(b) 確認用の空 AppendEntries と通常 heartbeat(50ms)を受信側でどう区別するか未規定。(c) 読み取り要求と確認応答・apply 待ち解放(apply_cv_, :258)の対応付け方法が未規定。(d) 「リーダー確定待ち」「過半確認失敗」のクライアントへのエラー応答形式が未定義(router・データノードが消費するため distributed.md §4.5 との接続が必要)。
- 根拠: 文書内に上記の規定なし。
- 提案: apply 待ちタイムアウトの追加、heartbeat との識別方式(専用フラグ等)、要求 ID による対応付け、エラー応答形式を規定する。

### R-9: learner への複製経路が擬似コード上どこにも現れない
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:41-42(Role::kLearner)、:61-62(next_index/match_index は「リーダーのみ(ピア毎)」)、:192(replicate(): for each peer p)、:211-213(コミット判定は voters のみ)
- 問題: learner は「AppendEntries を受信し match_index を更新する」(distributed_membership.md §4.3 :124-125)とあるが、本書の replicate() が learner を「peer」に含むのか、become_leader(:178)の next_index/match_index 初期化が learners をカバーするのかが読み取れない。コミット判定から除外する場所は明示されているのに、複製経路への組み込み場所が抜けている。
- 根拠: 文書内に learner 向け複製の扱いの規定なし(membership 側 §4.3 も役割定義のみ)。
- 提案: 「replicate() の peer 集合 = voters ∪ learners、コミット判定の数え上げは voters のみ」と明記する。

### R-10: 安全性性質 P-1〜P-5 の番号参照が本文と §15 で 3 箇所とも食い違う
- 区分: 不明瞭
- 対象: docs/distributed_raft.md:163「(コミット済みエントリの上書き防止、選出安全性 P-2)」、:220「『選出されたリーダーのログはコミット済みログ全体を必ず含む』(P-3)」、:238「term が一致する既存エントリは内容も一致する(ログ整合性 P-4)」 vs §15 :407-409(P-1 選出安全性/P-2 リーダー完全性/P-3 ログ整合性)
- 問題: §15 の定義では P-1=選出安全性、P-2=リーダー完全性、P-3=ログ整合性である。しかし本文は (a) :163 で「選出安全性」に P-2 の番号を付けており(名前は P-1、番号は P-2 の混合)、(b) :220 は §15 の P-2 と同一文のリーダー完全性を「P-3」と引用し、(c) :238 は §15 の P-3 と同一内容のログ整合性を「P-4」と引用する。テストマトリクス(:404)や §15 の表明実装時に間違った不変条件を検証する恐れがある。
- 根拠: 上記行番号の通り、3 つの本文参照がすべて §15 の定義と一致しない。
- 提案: §15 の定義を正として本文 3 箇所の番号を修正する(:163 は趣旨的に P-2 リーダー完全性へ統一、:220 は P-2、:238 は P-3)。

### R-11: 旧 term の AppendEntries を拒否した後も election タイマをリセットする疑似コード
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:225-227
- 問題: 受信者側ハンドラは `if (term < current_term) reply(false);` の後に return せず、次行で無条件に `role = kFollower(... election タイマをリセット)` を実行する。標準的な Raft では旧 term のメッセージはタイマをリセットしてはならず、このまま実装すると「term が先行したノードが分離から復帰した直後に、旧リーダーの heartbeat だけを受け取れる部分分離」状態で選挙タイマが永久に抑止され、可用性が失われる(liveness 破壊)。§15 テストマトリクス(:397「分離旧リーダー復帰」)でも検出されにくい種類のバグである。
- 根拠: 文書内に return/早期脱出の規定がなく、:227 が無条件実行になる構造。
- 提案: 「msg.term < current_term のときは reply(false) して即 return(タイマ更新なし)」と疑似コードを修正する。

## 未検証事項

- 数値パラメータ(heartbeat 50ms、election 400–800ms、batch 64、snapshot 閾値 100k/64MiB)の妥当性は、シミュレーション実装が存在しないため検証していない。
- joint consensus 適用中の「常に最新自ログ構成で過半計算」(:65-67・:307-309)と ReadIndex(:347)の組合せ(jc 構成時の二重過半適用)について、membership 側 §4.2 から導けることは確認したが、形式的証明は行っていない。
