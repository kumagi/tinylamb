# docs/distributed_migration.md レビュー指摘事項

## サマリー

本書に対応する実装(epoch モード・制御プレーン・shipper/receiver・router・admin)はリポジトリに一切存在しない。M1 の前提となる Phase 0 相当バイナリ（StreamLogger/EpochManager/PreCommit 分割 API/async commit）もフラグ（--wal-streams/--epochs/--distributed/--control-peers）も見当たらず、親文書 distributed.md:22 が「ネットワーク関連のコードは現在ワイヤサーバのみ」と認めている。既存コードへの参照(page_storage.cpp:52、recovery_manager.cpp:74)は正確で、WAL 無限増大や torn page→SinglePageRecovery の主張も現状と整合する。ただし **base backup の安全性根拠が置く「解析失敗=末尾」規約は現行 RecoverFrom の throw 挙動と矛盾**しており、これは M3 の中核手順の根拠に関わる最重要指摘である。さらに quorum パラメータの数値不整合、フェーズ対応表の欠落、簡体字混入等がある。

## 指摘一覧

### G-1: 移行計画全体の前提となる Phase 0 相当バイナリが存在しない
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:1-11(冒頭)、:85-93(M1 前提)、:97-112(M2)、:203(--distributed=off)
- 問題: M1 は「新バイナリ = Phase 0 相当」とするが、StreamLogger/EpochManager/PreCommit 分割 API/async commit のいずれも未実装。フラグ --wal-streams/--epochs/--distributed/--control-peers も无い。手順書を実行しようとすると M1 の時点で失敗する。
- 根拠: find/grep で distributed/ なし。postgres_server_main.cpp は --host/--port/--read-workers のみ解析。common/constants.hpp に nodeid_t/epoch_t 等なし。distributed.md §11 Phase 0(WP-0a〜0e)の受入基準を満たす痕跡がない(improvement.md §19 に該当作業なし)。
- 提案: 冒頭に「本書は未着手の設計段階の運用計画。Phase 0〜4 がすべて未実装であることを前提に読む」注記を追加。

### G-2: base backup 安全性根拠の「解析失敗=末尾規約で刈られる」が現行 RecoverFrom の挙動と矛盾する
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:139-140
- 問題: 「末尾の不完全レコードは『解析失敗=末尾』規約で刈られる」と書くが、n2/n3 起動時の RecoverFrom は解析失敗時に例外を送出して起動に失敗する(recovery_manager.cpp:397-399 throw std::runtime_error)。SinglePageRecovery のみ break で末尾扱い(:356-361)であり、本体リカバリでは成立しない。torn tail を含む WAL を base backup でコピーした場合、n2 は起動時に例外で停止し得る。
- 根拠: recovery/recovery_manager.cpp:392-396(RecoverFrom の `if (!success) { throw ... }`)、:404(`case LogType::kUnknown: throw ...`)、:356-361(SPR の `break` のみ)。ReadLog(:512-521) はデコード失敗後の stream 状態を検査せず true を返すため、torn レコードが garbage 値としてデコードされる可能性もある。
- 提案: §5 の安全性根拠に「現状 RecoverFrom は解析失敗で例外を送出するため、tail 刈りの実装(Phase 7 で追加予定)または M3 手順に『コピー完了時の WAL 長を記録し、それを超える末尾を truncate してから起動』の回避手順を追加」と書き換える。または Phase 0 で tail 切り詰めを実装する WP を追加する。

### G-3: 「未知の LogType に遭遇した旧バイナリは解析停止(=ログ末尾扱い)」の主張が現行デコーダと矛盾
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:56-61(§2 互換性ポリシー)
- 問題: 旧バイナリが新レコード種を読んだ際、operator>>(Decoder&, LogRecord&) の default 句(:835-838)は LOG(ERROR)+assert のみで payload を読まず return する。これにより Size() が破綻し、以降のオフセットが payload の途中に落ちて連鎖的に誤読を生む(リリースビルドでは assert が無効なため黙って進む)。解析停止ではなく「誤読連鎖」または「assert 死」になる。
- 根拠: recovery/log_record.cpp:835-838。recovery/recovery_manager.cpp:792-795(デコード後 switch で未知値は default: break、解析失敗ではないため throw されない)。
- 提案: §2 を「旧バイナリは未知型で無害に停止しない。互換性は『compat モードで新レコード種を 1 本も書かない』ことのみで担保し、ダウングレード訓練は新バイナリの単一ノード復帰に限定する」と修正する。

### G-4: M3 手順 1 の checkpoint 要求(admin 経由)+master record 確認が実現不可能
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:120
- 問題: (a) tinylamb_admin が存在しない(Membership M-2 と同根)。(b) CheckpointManager は周期 worker による自動実行のみ(checkpoint_manager.cpp:41-55)、外部から要求する経路がない。(c) master record はリカバリで読まれず(RecoverFrom(0) で常に先頭から走査)、外部確認用ツールもない。(d) マスターレコード更新を「完了の確認」として使う前提が成立しない。
- 根拠: grep で WriteCheckpoint 呼び出し元は worker+テストのみ。database/page_storage.cpp:52 は `rm_.RecoverFrom(0, &tm_)` で master record を参照しない。分散側の admin CLI は未実装。
- 提案: M3 手順を「周期 checkpoint の完了を待つ(または Phase 0 で admin トリガと master record 読取 CLI を WP に追加する)」に改める。確認手段を distributed.md §9 変更表に明示。

### G-5: M3 手順 6 の quorum=3 がメンバシップ式と矛盾
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:129-130
- 問題: 「voters={n1,n2,n3}, quorum=3」と書くが、distributed_membership.md §6.1(:155) および distributed.md:155 の式は quorum = size/2+1 であり 3 voter のとき 2。seal 過半(I-1)の実装閾値を誤る。
- 根拠: distributed_membership.md:155「// primary 含む。quorum = size/2+1」; distributed.md:155 同式。membershhip §7.2(:248)「quorum は常時 3 を保つ」も同系の誤り。
- 提案: 「quorum=2(size/2+1)」に修正。membershhip §7.2 も揃える。

### G-6: 対応フェーズ対応表に M4/M7 がない
- 区分: 粒度不足
- 対象: docs/distributed_migration.md:10-11
- 問題: M0/M1/M2/M3/M5/M6 は Phase 0/1/2/3/4 に対応するが、M4(シャドウ運転 72h 計測)と M7(定常運用)がどの Phase/WP に属するか記載されていない。
- 提案: 「M4=Phase 3 計測ゲート(受け入れ前観測)、M7=運用(Phase 外)」を追加。

### G-7: 表記問題(英字混入、簡体字)
- 区分: 不明瞭
- 対象: docs/distributed_migration.md:66「その間 his シャードの primary でないノードから」; :224「データのコピーや变更はすべてオンライン」
- 問題: 「his シャード」は日本語文中に英語所有格が混入。:224 は簡体字「变更」(正しくは「変更」)。membership 文書にも簡体字「迁移」が 5 箇所ある。
- 提案: 「his シャード」→「そのシャード」、「变更」→「変更」に修正。membership 側の「迁移」→「移行」にも一括修正。

### G-8: M4 計測ゲートの測定手段が規定されていない
- 区分: 粒度不足
- 対象: docs/distributed_migration.md:153-157
- 問題: shipper lag p99 < 50ms、FlushAck RTT p99 < 5ms、REDO 遅延 p99 < 100ms、制御プレーン term 変動 < 1 回/日 等のゲートを満たす測定手段が未定義。tinylamb_admin が無く、メトリクス出力 API も存在しないため、72 時間運転の合否判定が主観になり得る。
- 提案: 各ゲート指標の取得元(Raft FSM 状態、shipper/receiver の統計カウンタ)と出力形式(seal status 相当 CLI)を WP に追加。

## 未検証事項

- torn page → CRC → SPR 主張(:137-138)は checksum 実装(improvement.md M2 完了、docs/page_checksum.md)と整合するが、大規模 DB で多数ページ同時 torn の SPR 全走査(recovery_manager.cpp:346-373、ページごとに WAL 先頭から全走査)の所要時間は評価していない。
- M4 ゲートの閾値(50ms 等)と M5 の seal レイテンシ目安(~3ms)は epoch 5ms(distributed.md §13)からの一貫した見積もりとして読めるが、実装不存在のため実測検証は不可能。
