# docs/distributed_raft.md レビュー指摘事項

## サマリー

対応実装は存在しない（find で distributed/ なし、grep ヒットは IN サブクエリ membership キャッシュ等のみ）。既存資産(crc32c.hpp)の参照は正確だが、--control-port や nodeid_t など新設物を現在形で書くと誤認を招く。内部矛盾として、§15 の P-1〜P-5 番号定義と本文の 3 箇所の参照(:163 選出安全性P-2、:220 リーダー完全性P-3、:238 ログ整合性P-4)がすべて食い違う。さらに疑似コードの旧 term AppendEntries 拒否後の timer リセット(:225-227)は liveness 破壊筋。

## 指摘一覧

### R-1: 対応実装が存在しないことが文書自体に書かれていない
- 区分: 実態との乖離
- 対象: docs/distributed_raft.md:1-6、全文
- 問題: 「実装できる粒度」と書かれているが distributed/ 目录もコードも 0。文書単独では既存仕様か設計提案か判別不可。
- 根拠: find . -type d -not -path "./build*" に distributed/ なし。grep -ril "raft|membership|migration" --include="*.cpp" . でヒットしたのは executor/detail/subquery_runtime.hpp:53(uncorrelated_membership)、planning_heuristics.cpp:789、lsm_tree_test.cpp:536、postgres_server_test.cpp:2230 の部分文字列のみ。distributed.md:22「ネットワーク関連のコードは現在ワイヤサーバしか存在しない」と一致。
- 提案: 冒頭に「設計仕様、Phase 1 未着手」と注記。

### R-2: 既存資産と新設物の区別が現在形で混在
- 区分: 実態との乖離
- 対象: docs/distributed_raft.md:38(nodeid_t)、:72(--control-port)、:76(crc32c)
- 問題: nodeid_t は constants.hpp:147-151 に未追加、--control-port は server/postgres_server_main.cpp:21-68 に存在せず、crc32c.hpp のみ実在。現在形の記述が読者を混同させる。
- 根拠: common/constants.hpp:147-151 は lsn_t/txn_id_t/page_id_t/slot_t/bin_size_t のみ。postgres_server_main.cpp は --host/--port/--read-workers のみ解析。crc32c.hpp は実在(CMakeLists.txt:561 でテスト登録)。
- 提案: 既存は「既存」ラベル、新設は「WP-0b 追加予定」にラベル付け。

### R-3: RequestVoteReply / InstallSnapshotReply / TimeoutNow の wire 形式が未定義
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:82-87(MsgType 列挙)、:89-115(構造体定義)
- 問題: 7 型のうち 4 種のみ構造体定義。Reply の pre_vote echo、InstallSnapshot の分割転送要否、TimeoutNow のボディが不明。
- 根拠: 文書内に上記 3 型の構造体なし。
- 提案: 全 msg_type の構造体を定義(空ボディなら明記)。InstallSnapshot の chunk 転送規則を決める。

### R-4: normalize() / LogFirstIndex() 等の内部関数とスナップショット後境界未規定
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:53-54、:196、:228-232、:257
- 問題: normalize(index) が定義されず、prev_log_index < LogFirstIndex() の分岐が規定されていない。疑似コードを素直に実装すると範囲外アクセスになる。
- 根拠: 文書内に normalize / LogFirstIndex の定義なし。§9 :293 の「ダミー境界」のみ言及。
- 提案: ダミー境界エントリと normalize の定義、prev_log_index < LogFirstIndex() 時の conflict 応答規則を追記。

### R-5: pre-vote の grant 条件が曖昧
- 区分: 不明瞭
- 対象: docs/distributed_raft.md:137-155
- 問題: election_timeout の乱数値のどれを使うか、自己票の扱い、旧 term 通信を含むか、pre-vote 待ち中の本選挙応答到着処理が未定義。
- 根拠: §13 :366 は「400–800ms 一様乱数」としか定めない。
- 提案: 判定用タイマ値と対象通信範囲、自己票、応答対応を明文化。

### R-6: kNoop の形式が FSM 決定論規約と衝突
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:179、:254-260、:335
- 問題: kNoop の cmd 形式未定義のまま apply_to_fsm は全エントリを Apply に渡す。§11 は先頭バイト=バージョン、未知は fatal とするため、no-op 適用で全ノード落ちる危険性。
- 根拠: 文書内に kNoop のバイト表現なし。
- 提案: no-op 専用形式を定義し、Apply は状態変更なしで受理と明記。

### R-7: meta ファイルの「破損行は無視」に検出手段がない
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:265-267
- 問題: voted_for が無投票のときの行表現、完全行判定基準、破損検出手段(CRC 等)が無い。投票前 fsync(E-1)の安全性はこの読み取り規則に依存。
- 根拠: フォーマット詳細の規定なし。
- 提案: 具体フォーマットと CRC 付加を定義。

### R-8: ReadIndex 手順の細部(last_applied 待ちタイムアウト不在)
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:344-350、:367
- 問題: apply 待ちにタイムアウトがなく、heartbeat と確認用空 AppendEntries の区別、要求と回答の対応付け、エラー応答形式が未規定。
- 根拠: 文書内に上記規定なし。
- 提案: apply 待ちタイムアウト、heartbeat 識別方式、要求 ID 対応付け、エラー応答形式を規定。

### R-9: learner の複製経路が replicate() に現れない
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:41-42、:192、:211-213
- 問題: learner は AppendEntries を受信し match_index を更新するが、replicate() の peer 集合とコミット判定の voters だけの計算との整合が明記されていない。
- 根拠: 文書内に learner 向け複製経路の規定なし。
- 提案: replicate() の peer = voters ∪ learners、コミット判定 = voters のみと明記。

### R-10: 安全性性質 P-1〜P-5 の番号参照が §15 定義と 3 箇所すべて不一致
- 区分: 不明瞭
- 対象: docs/distributed_raft.md:163-164(選出安全性 P-2)、:220(リーダー完全性 P-3)、:238(ログ整合性 P-4) vs §15:407-409(P-1 選出安全性/P-2 リーダー完全性/P-3 ログ整合性)
- 問題: 本文は (a) 選出安全性に P-2、(b) リーダー完全性に P-3、(c) ログ整合性に P-4 を付与。§15 では P-1/P-2/P-3。テストマトリクス(:404)の不変表明検証に間違った性質を使う恐れ。
- 根拠: 上記行番号の通り。
- 提案: §15 を正として本文 3 箇所を修正(:163→P-2 リーダー完全性、:220→P-2、:238→P-3)。

### R-11: 旧 term の AppendEntries 拒否後も election タイマをリセットする疑似コード
- 区分: 粒度不足
- 対象: docs/distributed_raft.md:225-227
- 問題: `if (term < current_term) reply(false);` の後に return せず無条件で `role = kFollower(... election タイマをリセット)`。旧リーダーの heartbeat が term が進んだノードの選挙を永久抑止する liveness 破壊筋。
- 根拠: 疑似コードの構造上、reply(false) の後も次行が実行される。
- 提案: `reply(false)` 後に即 return(タイマ更新なし)と修正。

## 未検証事項

- パラメータ値(heartbeat 50ms、election 400-800ms、batch 64、snapshot 100k/64MiB)の妥当性はシミュレーション不存在のため未検証。
- joint consensus 下の ReadIndex 過半判定(二重過半)の形式的証明は行っていない。
