# docs/distributed_migration.md レビュー指摘事項

## サマリー

本書に対応する実装(epoch モード・制御プレーン・shipper/receiver・router・admin)はリポジトリに一切存在しない。M1 の前提となる Phase 0 相当バイナリ（StreamLogger/EpochManager/PreCommit 分割 API/async commit）もフラグ（--wal-streams/--epochs/--distributed/--control-peers）も見当たらず、親文書 distributed.md:24 が「ネットワーク関連のコードは現在ワイヤサーバのみ」と認めている。既存コードへの参照(page_storage.cpp:52、recovery_manager.cpp:74)は正確で、WAL 無限増大や torn page→SinglePageRecovery の主張も現状と整合する。なお G-2（base backup 安全性根拠への指摘）と G-3（未知 LogType のデコード挙動への指摘）は無効として撤回した：G-2 は現行 RecoverFrom が解析失敗で例外を送出せず torn tail を刈って Status を返すため、G-3 は LST 移行後の境界付き Deserialize が未知 type を return 0 で綺麗に拒否し ValidLogEndStatus が末尾扱いするため、いずれも親文書の記述は実装と整合している。quorum パラメータの数値不整合、フェーズ対応表の欠落、簡体字混入等がある。

## 指摘一覧

### G-1: 移行計画全体の前提となる Phase 0 相当バイナリが存在しない
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:1-11(冒頭)、:85-93(M1 前提)、:97-112(M2)、:203(--distributed=off)
- 問題: M1 は「新バイナリ = Phase 0 相当」とするが、StreamLogger/EpochManager/PreCommit 分割 API/async commit のいずれも未実装。フラグ --wal-streams/--epochs/--distributed/--control-peers も無い。手順書を実行しようとすると M1 の時点で失敗する。
- 根拠: find/grep で distributed/ なし。postgres_server_main.cpp は --host/--port/--read-workers に加え --max-connections/--force/--help も解析。common/constants.hpp に nodeid_t/epoch_t 等なし。distributed.md §11 Phase 0(WP-0a〜0e)の受入基準を満たす痕跡がない(旧 improvement.md §19 に該当作業なし — improvement.md はリポジトリに存在せず削除済み/未収録)。
- 提案: 冒頭に「本書は未着手の設計段階の運用計画。Phase 0〜4 がすべて未実装であることを前提に読む」注記を追加。

### G-2: [撤回 — 指摘は無効] base backup 安全性根拠の「解析失敗=末尾規約で刈られる」は実装と整合している
- 区分: 撤回（実態との乖離なし）
- 対象: docs/distributed_migration.md:142-143
- 訂正: 初版は「n2/n3 起動時の RecoverFrom は解析失敗時に例外を送出して起動に失敗する(recovery_manager.cpp:397-399 throw std::runtime_error)」と書いたが、これは誤り。recovery/ 配下に throw は存在せず（logger.cpp:863 のコメントのみ）、:397-399 は ReadLogDetailed のシグネチャと kMinHeader 定義である。実際の RecoverFrom は ValidLogEndStatus(scan_start) で有効末尾を求め（recovery_manager.cpp:601）、torn tail を LOG して logger_->TruncateTo(valid_end) で刈り（:611-614）、2 パス目の読み直し失敗時のみ StatusError(kCorrupt) を返す（:688-690）。すなわち親文書の「末尾の不完全レコードは『解析失敗=末尾』規約で刈られる」は現行実装と整合しており、§5 の書き換えや tail 刈り WP の追加は不要である。
- 根拠: recovery/recovery_manager.cpp:601（ValidLogEndStatus）、:604-615（"Truncating torn WAL tail" + TruncateTo）、:688-690（StatusError 返却）。grep -rn 'throw' recovery/*.cpp で実行文 0 件。

### G-3: [撤回 — 指摘は無効] 「未知の LogType は解析停止(=ログ末尾扱い)」は現行デコーダの実挙動と一致する
- 区分: 撤回（実態との乖離なし）
- 対象: docs/distributed_migration.md:56-61(§2 互換性ポリシー)
- 訂正: 初版は「operator>> の default 句が payload を読まず return するため Size() が破綻し誤読連鎖になる(recovery/log_record.cpp:835-838、recovery/recovery_manager.cpp:792-795)」と書いたが、現行ログ形式は 405ff4f の LST 移行で境界付き `LogRecord::Deserialize` に置き換わり、旧引用行はファイル長を超える幽霊行だった。実際の Deserialize は type バイトを範囲検査(`raw_type == 0 || raw_type > kEndCheckpoint` → return 0、log_record.cpp:353-357)し、`kUnknown` も return 0(:363-365)で payload 長を信用しない。回収側の `ValidLogEndStatus` は非 kComplete 読み取りで `return offset`(=有効末尾)を返す(recovery_manager.cpp:547-567)ため、未知レコード種は torn tail と同じく綺麗に末尾扱いで刈られる。親文書 §2 の「解析停止=末尾」記述は現行実装と整合しており、本指摘は撤回する。
- 根拠: recovery/log_record.cpp:353-357, :363-365。recovery/recovery_manager.cpp:547-567。

### G-4: M3 手順 1 の checkpoint 要求(admin 経由)+master record 確認が実現不可能
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:120
- 問題: (a) tinylamb_admin が存在しない(Membership M-2 と同根)。(b) CheckpointManager の自動契機は周期 worker に加え LSM flush/compaction コールバックとコミット閾値(checkpoint_manager.cpp の ctor は :40-51、RequestCompaction は checkpoint_manager.cpp:69)に拡張済みで、「外部から要求する経路がない」はもはや正確でない — `Database::WriteCheckpoint()`(database.hpp:206、シャットダウン経路 database.cpp:1719 や回復後アンカー page_storage.cpp:216 が使用)という公開 API は存在するが、オペレータ向けの admin/CLI 経路は依然として無い。(c) master record はリカバリで読まれず(RecoverFrom(0) で常に先頭から走査)、外部確認用ツールもない。(d) マスターレコード更新を「完了の確認」として使う前提が成立しない。
- 根拠: grep で WriteCheckpoint 呼び出し元は worker+テスト+Database API 経由のみ。database/page_storage.cpp:189 は `rm_->RecoverFrom(0, storage->tm_.get())` で master record を参照しない。分散側の admin CLI は未実装。
- 提案: M3 手順を「周期 checkpoint の完了を待つ(または Phase 0 で admin トリガと master record 読取 CLI を WP に追加する)」に改める。確認手段を distributed.md §9 変更表に明示。

### G-5: M3 手順 6 の quorum=3 がメンバシップ式と矛盾
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:129-130
- 問題: 「voters={n1,n2,n3}, quorum=3」と書くが、distributed_membership.md §6.1(:155) および distributed.md:160 の式は quorum = size/2+1 であり 3 voter のとき 2。seal 過半(I-1)の実装閾値を誤る。
- 根拠: distributed_membership.md:155「// primary 含む。quorum = size/2+1」; distributed.md:160 同式。membership §7.2(:252)「quorum は常時 3 を保つ」も同系の誤り。
- 提案: 「quorum=2(size/2+1)」に修正。membership §7.2 も揃える。

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

- torn page → CRC → SPR 主張(:137-138)は checksum 実装(旧 improvement.md M2 完了の記録 — improvement.md は削除済み/未収録、docs/page_checksum.md)と整合するが、大規模 DB で多数ページ同時 torn の SPR 全走査(recovery_manager.cpp:346-373、ページごとに WAL 先頭から全走査)の所要時間は評価していない。
- M4 ゲートの閾値(50ms 等)と M5 の seal レイテンシ目安(~3ms)は epoch 5ms(distributed.md §13)からの一貫した見積もりとして読めるが、実装不存在のため実測検証は不可能。
