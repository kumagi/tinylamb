# docs/distributed_membership.md レビュー指摘事項

## サマリー

対応実装は存在しない（MetaCmdType/ShardConfig/NodeInfo/EpochManager/sealed_epoch/tinylamb_admin すべて 0 ヒット）。「旧設計の kAddNode/kRemoveNode が廃止」と書くが廃止対象自体が存在しないため、文書は純粋な設計提案。2 層分離(L1/L2)と joint consensus のクラッシュ時規則は Raft 論文と整合して明快だが、promote 判定の時間条件が §4.1 と §4.3 で食い違い、quorum 記述に式と矛盾、シャード初期作成コマンドと L2 primary 削除手順が欠落している。

## 指摘一覧

### M-1: 「改訂 MetaCmdType」とされるが改訂元も新体系もコード上に存在しない
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:131-145(§5)、:143(「旧設計の kAddNode/kRemoveNode/kShardConfig を廃止」)
- 問題: 列挙型自体がリポジトリに存在せず、廃止対象も不在。読者は既存 FSM があると誤認しやすい。
- 根拠: grep で MetaCmdType/ShardConfig/kSealEpoch/DurabilityMode すべて 0 件。nodeid_t も common/constants.hpp:147-151 に存在しない。
- 提案: 冒頭に「本書は設計仕様。WP-2a/2b で新設」と注記。§5 を「新設 enum 定義」と明示。

### M-2: 運用インターフェース(tinylamb_admin)全体が未実装で §7/§8/§9 の手順が実行不可能
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:218-253(§7)、:255-299(§8)、:301-316(§9)
- 問題: register-node / shard promote / seal status / force-single / cluster describe のすべて未実装。手順書の前提が成立しない。
- 根拠: grep で tinylamb_admin/cluster describe/force-single すべて 0 件。CMakeLists には admin_main.cpp ターゲットなし。
- 提案: 実装不在を注記し、Phase 1 完了まで §7 以降を「ドラフト手順」と明示。

### M-3: promote 判定の時間条件が §4.1 と §4.3 で食い違い、マジック定数の根拠がない
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:64-66(§4.1 手順 3)、:126(§4.3 promote 判定)
- 問題: §4.1 は「match_index[D] ≥ commit_index - 8 が 30 秒以内に成立 → e2 追記、60 秒で断念」。§4.3 は「30 秒継続」で promote。到達時刻が最大 30 秒ずれ、実装・テストの期待値が一意に定まらない。定数「-8」の単位とチューニング基準も不明。
- 根拠: 文書内に統合された時系列図なし。§10 テストマトリクス :323 は「60 秒で断念」とのみ書かれ 30 秒の意味不明。
- 提案: 条件を 1 文に統一しタイムライン図を追加(例: 0s 開始→30s 判定→60s 断念)。「-8」の意図(heartbeat 50ms × batch 64 の許容遅延等)を注記。

### M-4: 断念時「e1' を追記して戻す」後の再試行手順と「状態が元に戻る」意味が未定義
- 区分: 不明瞭
- 対象: docs/distributed_membership.md:66、:323
- 問題: 構成エントリはログに残り続けるため「戻す」は新構成エントリの追記のみ。再試行手順とログの伸び続ける性質を明記する必要がある。
- 根拠: 文書内に再試行規定なし。
- 提案: 「有効構成は C に戻るがログは伸び続ける」ことを明記し、断念後の再試行を R-2 に従う 1 変更として規定。

### M-5: ノード登録の二重登録検査と冪等再登録許可の衝突規則が未定義
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:36-40
- 問題: 同一 host+port で別ノード登録時の拒否/上書き、id 再利用可否が不明。冪等再登録と二重登録検査が競合する条件(同一 id で host/port 変更後の再登録)が未規定。
- 根拠: 規則表なし。
- 提案: (id, host+port) の許否表を追加し id 再利用禁止を明記。

### M-6: シャード初期作成のコマンドが L2 コマンド体系に存在しない(migration との矛盾)
- 区分: 実態との乖離
- 対象: docs/distributed_membership.md:131-140、:151-162; docs/distributed_migration.md:103-104(M2 手順)
- 問題: kAddLearner/kPromoteToVoter は既存シャード前提。migration M2 は「初期シャードとして shard 0 を投入」と書くが経路不明。kControlConfChange は構成エントリとして扱うがシャード生成ではない。
- 根拠: MetaCmdType に kCreateShard 相当なし。M-2 適用時検査(:166)も初期生成経路を要する。
- 提案: kCreateShard の追加、または制御プレーン初回起動時の特殊経路を規定する。

### M-7: quorum 記述の不整合と voters=2 窓の可用性クリフ
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:155、:229、:248; docs/distributed_migration.md:130
- 問題: 式 `quorum = size/2+1` に対し、§7.2 は「quorum は常時 3 を保つ」(:248)、migration M3 は「voters={n1,n2,n3}, quorum=3」(:130) と書くが voters=3 なら quorum=2。voters=2 の中間状態(1→2 promote 直後)では quorum=2(全員一致)となり、n2 の追従遅延で seal が停滞。§7.1 は voters=4/5 の中間状態を評価するが 3→2 窓を無視。
- 根拠: distributed.md:155 の式と§7.1 d(:233)、§7.2(:248) の記述が矛盾。M3 の promote 手順も同式に反する数値を記載。
- 提案: 「quorum は常時 3」を「voter 3 台・耐障害 1 台(=quorum は 2↔3 で変動)」に修正。M3 の quorum=3 も 2 に修正。membershhip §6.2 に voters=2 窓の評価と catch-up 制御を追記。

### M-8: kRemoveReplica を primary に対して実行する場合の扱いが未定義
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:191-200(§6.3)
- 問題: L1 ではリーダー削除制約(:76-78)があるのに L2 の削除コマンドに primary 削除の規定がない。primary ∈ voters(M-2) を維持したまま primary を削除すると適用時検査違反になる。
- 根拠: §6.3 に primary 削除の手順なし。
- 提案: 「削除対象が primary の場合は kSetPrimary を先行させる(提案時検査で拒否)」を追加。

### M-9: force-single の term+1 が旧ノード生存時の term 衝突を考慮していない
- 区分: 粒度不足
- 対象: docs/distributed_membership.md:266-267、:271-274
- 問題: 「term+1 の単独構成エントリ」と書くが、誰の term に対する +1 かで、停止漏れの旧ノードがより大きい term を持つ場合の pre-vote 攪乱(:380 E-2)を考慮していない。
- 根拠: distributed_raft.md:120-128(§5)の共通規則と :380 の E-2 参照。文書に term の出所規定なし。
- 提案: 「観測可能な全制御ノードの term 最大値 + 1」を手続き化し、停止漏れ検出(pre-vote 不成立)を手順に追加。

### M-10: 基線再構築 min 採用の条件と learner 報告の扱いが不明確
- 区分: 不明瞭
- 対象: docs/distributed_membership.md:286-294
- 問題: min は保守的だが過小になるケース(生存 voter が 1 つのみ)の前進遅延評価がない。watermarks の min が既存 sealed_watermarks を下回る可能性とその許容理由が読めない。learner の報告を min に入れるか否か(:286 は両方要求)が不明。
- 根拠: 上記箇所以外に規定なし。
- 提案: min は過小でも安全性を損なわない理由を明記。min に入れるのは voter のみ、learner は観測用と明記。

### M-11: 用語・表記の問題(簡体字混入、L1/L2 learner 同名異義)
- 区分: 不明瞭
- 対象: docs/distributed_membership.md:90,105,116,235,325(「迁移」); :122-129(learner の二重使用)
- 問題: 日本語文書に簡体字「迁移」が 5 箇所。L1(Raft)と L2(WAL 受信)の learner が同名だが実体別物(§4.3 が自認)であり、表記上の区別がない。
- 根拠: 上記行番号の通り。distributed_raft.md:42 の Role も L1 専用。
- 提案: 「迁移」→「移行」に統一。L2 の learner を文書内で一貫して区別表記(例: shard-learner)にする。

## 未検証事項

- §4.2 joint consensus のクラッシュ 3 箇所継続規則(:110-116)の網羅性はシミュレーション不存在のため検証不可。
- §10 テストマトリクス「負荷中の add/remove/promote でコミット停止 < 1 秒」(:320 程度)の測定は実装不在のため評価不能。
