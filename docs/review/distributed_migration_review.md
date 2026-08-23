# docs/distributed_migration.md レビュー指摘事項

## サマリー

本書に対応する実装(epoch モード・制御プレーン・shipper/receiver・router・tinylamb_admin)はリポジトリに一切存在せず、M1〜M7 のどの段階も開始できない。`grep` で StreamLogger / EpochManager / sealed_epoch / FlushAck / tinylamb_admin がすべて 0 ヒット、`--wal-streams` `--epochs` `--distributed` 等のフラグも無く、親文書 distributed.md:22 自身が「ネットワーク関連のコードは現在ワイヤサーバしか存在しない」と認めている。したがって本書は純粋な設計提案である。一方、既存コードへの参照(page_storage.cpp:52、recovery_manager.cpp:74)は行番号込みで正確であり、WAL 無限増大や torn page→SinglePageRecovery の指摘も現状と整合する。ただし **base backup の安全性根拠が置く「解析失敗=末尾」規約は現行 `RecoverFrom` の throw 挙動と矛盾**しており、これは本計画の中核の手順(M3)の根拠に関わる最重要指摘である。

## 指摘一覧

### G-1: 移行計画全体の前提となる実装(Phase 0 相当)が存在しないことが文書に明記されていない
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:1-11(冒頭)、:85-93(M1 前提)、:97-112(M2 手順)、:203(:--distributed=off)
- 問題: 本書は手順書形式だが、M1 の前提とする「新バイナリ = Phase 0 相当(型追加、StreamLogger、EpochManager、PreCommit 分割 API、async commit 完了)」のすべてが未実装で、M2 以降が必要とする `--wal-streams` `--epochs` `--control-peers` `--membership-mode` `--distributed` の各フラグも 1 つも存在しない。文書単体では実行可能な手順書と誤認する。
- 根拠: `grep -rn "StreamLogger\|EpochManager\|wal-streams\|epochs=\|--distributed\|control-peers" --include="*.cpp" --include="*.hpp" . | grep -v build` → 0 件。server/postgres_server_main.cpp:21-68 は `--host`/`--port`/`--read-workers` のみを解析。common/constants.hpp:147-151 に nodeid_t/epoch_t/view_t/stream_id/shard_id 型なし。distributed.md §11 Phase 0(WP-0a〜0e)の受け入れ基準を満たす痕跡がない(improvement.md §19 の完了ログにも該当作業なし)。
- 提案: 冒頭に「本書は設計段階の運用計画。Phase 0〜4(distributed.md §11)がすべて未実装であることを前提に読むこと」の注記を追加する。

### G-2: base backup 安全性根拠の「解析失敗=末尾」規約が現行 RecoverFrom の挙動と矛盾する
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:139-140「WAL は append-only かつ現状切り詰めなし。逐次 copy は単調 prefix を読むので、末尾の不完全レコードは『解析失敗=末尾』規約で刈られる」
- 問題: 「解析失敗=末尾」として起動を継続できるのは `SinglePageRecovery` のみで、n2/n3 起動時に走る本体の `RecoveryManager::RecoverFrom` は解析失敗時に**例外を送出して起動が失敗する**。base backup のコピー途中に primary が追記中だった場合、コピー末尾に torn レコードが残るのが普通であり、この規約を信じてツールを作ると M3 手順 3 のリカバリが必ず失敗する筋がある。また `ReadLog` はデコード失敗後の stream 状態を検査せず true を返すため、torn レコードが garbage 値としてデコードされ `kUnknown` 扱いで throw されるか、破綻した Size() でオフセットが飛ばされる。
- 根拠: recovery/recovery_manager.cpp:397-399(`if (!success) { throw std::runtime_error("Invalid log: ..."); }`)、:404(`case LogType::kUnknown: throw ...`)。一方 recovery/recovery_manager.cpp:356-361(SinglePageRecovery 内のみ `LOG(ERROR)` + `break` で末尾扱い)、:512-521(ReadLog は seekg 成功後にデコード結果の good 状態を無視して return true)。
- 提案: (a) 本書側で「tail 刈りの実装は Phase 0/7 拡張で追加する」と現状との差分を明記するか、(b) base backup 手順に「コピー完了時点での WAL ファイル長を記録し、receiver 初期化前にその長さへ truncate してから起動する」等の回避手順を追加する。(b) は append-only 前提の現状では安全に行える。

### G-3: 「未知の LogType に遭遇した古いバイナリは解析停止(=ログ末尾扱い)する」は現行のデコーダ挙動と一致しない
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:56-61(§2 互換性ポリシー)
- 問題: 旧バイナリが新レコード種(kEpochBegin 等)を読んだ際の実際の挙動は「きれいな解析停止」ではない。`operator>>(Decoder&, LogRecord&)` の switch に未知型の case がなく default 句で LOG(ERROR)+assert の上 payload を読まないため、record Size() が実長より小さくなり、以降のオフセットが payload 中間に落ちて連鎖的に誤読する(release ビルドでは assert が無効なため黙って進む)。ダウングレード訓練(M1 検証 :91-92)の期待結果として「旧バイナリで recovery できる」を検証するなら、まず現状のこの挙動を修正・明記する必要がある。
- 根拠: recovery/log_record.cpp:835-838(default 句:`LOG(ERROR) << "unknown log type"; assert(!"unknown log");` のまま return)。RecoverFrom 側の分析 switch は kUnknown で throw(recovery_manager.cpp:404)、それ以外の未知値は default: break のため Size() 破綻が放置される。
- 提案: §2 を「旧バイナリへの見せ方は未定義動作(assert/誤読)であり、互換は『compat モードで新レコード種を書かない』ことのみで担保する」と書き換える。または旧バイナリ側に未知型で安全停止するガードの追加を Phase 0 変更表に加える。

### G-4: M3 手順 1 の「checkpoint 要求(admin 経由)」と「master record 更新で確認」が実現不可能
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:120([n1] checkpoint 要求(admin 経由)。完了を master record 更新で確認)
- 問題: (a) tinylamb_admin が存在しない(membership 文書レビュー M-2 と同根)。(b) checkpoint を外部から要求する経路がコードに存在しない——`CheckpointManager` は周期 worker による自動実行のみで、`WriteCheckpoint` の呼び出し元は worker とテストのみ。(c) 「完了を master record 更新で確認」のための観測手段(master record を読んで表示するツール)もない。そもそもリカバリは常に先頭から走査し master record を読まない(page_storage.cpp:52)ため、「更新されたこと」を外部が意味付きで確認する方法がない。
- 根拠: recovery/checkpoint_manager.cpp:41-55(worker の周期実行のみ)、:78(WriteCheckpoint 定義)。呼び出し箇所は checkpoint_manager.cpp:52(worker)とテスト(index/b_plus_tree_test.cpp:827、recovery/checkpoint_manager_test.cpp:86,109,133,157,180)のみ。database/page_storage.cpp:52(`rm_.RecoverFrom(0, &tm_)` — master record 未参照)。`grep -rn "tinylamb_admin"` → 0 件。
- 提案: 手順を「周期 checkpoint の完了を待つ(または Phase 0 で admin トリガと master record 読取 CLI を WP に追加する)」に改め、確認手段を実装タスクとして distributed.md §9 変更表に明示する。

### G-5: M3 手順 6 の「quorum=3」がメンバシップ仕様の計算式と矛盾する
- 区分: 実態との乖離
- 対象: docs/distributed_migration.md:129-130(promote --shard 0 n2 → voters={n1,n2} / n3 も promote (view+1, voters={n1,n2,n3}, quorum=3))
- 問題: distributed_membership.md §6.1(:155)および distributed.md:155(§4.2 ShardConfig コメント)の定義は「quorum = size/2+1」であり、voters={n1,n2,n3} のとき quorum は 2 である。本書の「quorum=3」は式から導けず、seal 過半(I-1)を実装する際に誤った閾値(全 voter 一致)を固定しかねない。membership 文書 §7.2(:248)の「quorum は常時 3 を保つ」も同系の誤り(voters=3 なら quorum=2)。
- 根拠: distributed.md:155「// primary 含む。quorum = size/2+1」、distributed_membership.md:155 同式。I-1(distributed.md :323-325)は「primary + quorum-1 個の voter backups」の fsync を要求し、3 voter 構成では primary+1 台で足りる。
- 提案: 「quorum=3」を削除または「quorum=2(size/2+1)」に修正する。併せて membership 文書 §7.2 の表記も揃える。

### G-6: 対応フェーズの対応表に M4/M7 が載っていない
- 区分: 粒度不足
- 対象: docs/distributed_migration.md:10-11(「対応フェーズ: M1=Phase 0、M2=Phase 1、M3=Phase 2/3、M5=Phase 3 完了、M6=Phase 4」)
- 問題: 全体図(:44-48)は M0〜M7 の 8 段階なのに、distributed.md の Phase への対応表は M1/M2/M3/M5/M6 の 5 つのみ。M4(シャドウ運転)と M7(定常運用)がどの Phase・WP に属するのか(例: M4 は Phase 3 受け入れの計測ゲート相当)が読み取れず、進捗管理と受け入れ判断の単位が曖昧になる。
- 根拠: 文書内に M4/M7 の対応記述なし。distributed.md §11 Phase 3(:485-491)には kill -9 受け入れ基準があり M4 の位置づけを導けるが、明示されていない。
- 提案: 対応表に「M4=Phase 3 計測ゲート(受け入れ前観測)、M7=運用(Phase 外)」のように全段階を列挙する。

### G-7: 表記の問題(英字混入ミス、簡体字)
- 区分: 不明瞭
- 対象: docs/distributed_migration.md:66「その間 his シャードの primary でないノードから」、:224「データのコピーや变更はすべてオンライン」
- 問題: (a) :66 の「his シャード」は日本語文中に英語所有格が混入した誤記(意図は「そのシャード」)。(b) :224 の「变更」は簡体字(正しくは「変更」)。手順書として引用・翻訳される際に検索不能になるほか、membership 文書にも同系の簡体字「迁移」が 5 箇所ある。
- 根拠: 上記行番号の通り(grep で確認)。
- 提案: 「his シャード」→「そのシャード」、「变更」→「変更」に修正し、membership 文書の「迁移」も一括置換する。

### G-8: M4 シャドウ運転の計測ゲートを満たす測定手段が規定されていない
- 区分: 粒度不足
- 対象: docs/distributed_migration.md:150-158(shipper lag p99 < 50ms、FlushAck RTT p99 < 5ms、REDO 遅延 p99 < 100ms、term 変動 < 1 回/日)
- 問題: ゲート判定に必要な各指標の取得手段(shipper/receiver からのメトリクス出力、admin での集計表示)がどこにも定義されていない。tinylamb_admin 未実装(G-4)のため「seal status」(:143)も「cluster describe」も打てず、72 時間運転の合否判定が主観になり得る。
- 根據: `grep -rn "FlushAck\|shipper\|metrics\|stats" --include="*.cpp" --include="*.hpp" . | grep -v build`(分散関連語)→ 0 件。distributed.md §10 の新規ディレクトリ計画(:434-452)にもメトリクス出力の項目がない。
- 提案: 各ゲート指標について取得元(Raft FSM 状態、shipper/receiver の統計カウンタ)と出力形式(seal status 相当の CLI サブコマンド)を WP として追加する。

## 未検証事項

- M4 ゲート閾値(lag p99 < 50ms 等)と M5 の seal レイテンシ目安(p50 ~3ms、p99 < 30ms、:168)は epoch 間隔 5ms(distributed.md §13)からの一貫した見積もりとして読めるが、実装不存在のため実測検証は不可能。
- torn page → CRC 不一致 → SinglePageRecovery 再構築(:137-138)は、ページチェックサム実装(improvement.md M2.1/M2.2 完了、docs/page_checksum.md)により現行コードと整合することを確認したが、大規模 DB で多数ページが同時に torn になった場合の SPR 全走査(recovery_manager.cpp:346-373、ページごとに WAL 先頭から全走査)の所要時間は評価していない。
