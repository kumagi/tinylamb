# 性能・ストレージ・長時間運転に関する224件の研究命題

同じ SQL 結果を保ってどこまで処理を省けるか、どの情報を持てば資源不足を予測できるかを、22個の Lean モデルに分けた。すべての命題は `TinylambOpt.ResearchGoals.ResourcePolicies` 以下にある。各ファイルの `def ….Goal : Prop` に前提と結論を記載した。表の実装先は候補を検討する場所を示す。ピークやバイト数は各モデルで定めた量で、実機ベンチマークは今回の対象外。 [Lean の登録モジュール](../../TinylambOpt/ResearchGoals/ResourcePolicies.lean)

## 小さな入力で観察できる境界

| 判断したいこと | モデルで評価した入力と結果 | 実装で追う量 |
|---|---|---|
| 隣接する空き領域は常に結合した方がよいか | 空き長が順に `[2,2,3]` なら first-fit は要求 `[3,2,2]` を完了する。先頭を結合して `[4,3]` にすると、最初の要求が別の穴を選び、最後に長さ2を取れない。 | 空き総量、最大連続長、確保順。[SlabReuse](../../TinylambOpt/ResearchGoals/ResourcePolicies/SlabReuse.lean) |
| 圧縮ビット幅は隣の幅との比較だけで選べるか | `[0,0,129]` のパッチ付きフレームは、幅7・8・9でそれぞれ6・4・5バイト。幅0は例外を1つ持って3バイト。 | レーン、例外、ビットマップを足した実バイト数。[RestartDirectory](../../TinylambOpt/ResearchGoals/ResourcePolicies/RestartDirectory.lean) |
| 語彙が重なる行グループは辞書を共有すべきか | 1バイトの語 A が100回続く群と、A・B が1回ずつの群。個別辞書なら計7バイト、共有すると17バイト。単一語の群にも1ビットの ID が必要になる。 | 辞書本体と ID 列の増減。[SharedVocabulary](../../TinylambOpt/ResearchGoals/ResourcePolicies/SharedVocabulary.lean) |
| メモリを多く返せる演算から完了させるべきか | （保持量、完了に必要な一時量）が `(10,7)` と `(7,1)` の2演算。大きな保持者を先にすると空きが7必要で、逆順なら1で両方完了する。 | 解放前の追加要求量、列全体を完了できる最小の空き。[GrantAdmission](../../TinylambOpt/ResearchGoals/ResourcePolicies/GrantAdmission.lean) |
| WAL 書き込み完了の最大位置でどこまで通知できるか | 半開区間 `[0,2)` と `[4,7)` が完了した時点の連続終端は2。`[2,4)` の完了で7に進む。実装ではさらに fsync の成功を待つ。 | 連続書き込み済み位置、永続化位置、通知待ち件数。[CommitBatches](../../TinylambOpt/ResearchGoals/ResourcePolicies/CommitBatches.lean) |

各例はファイル内の `#guard` で再評価できる。一般の命題について証明・反証を登録する際は、仮定を強めた別命題と元の命題を区別できる。

## プラン選択と SQL 実行（56件）

| モデル | 件数 | 実装判断・測定項目 |
|---|---:|---|
| [BudgetedPlans](../../TinylambOpt/ResearchGoals/ResourcePolicies/BudgetedPlans.lean) | 10 | 結合相手の実行中も残る出力を含めて memo 候補を間引けるか。行数見積もりの誤差と再計画の変換費も扱う。[Cascades 探索](../../../plan/cascades_search.cpp) |
| [PredicateScheduling](../../TinylambOpt/ResearchGoals/ResourcePolicies/PredicateScheduling.lean) | 9 | 述語が落とす行で次の評価を何回、いくら省けるか。例外を起こすキャストと安いフィルタの順序も扱う。[並列フィルタ](../../../executor/detail/parallel_scan_filter.cpp) |
| [JoinFanout](../../TinylambOpt/ResearchGoals/ResourcePolicies/JoinFanout.lean) | 9 | NULL と重複を含む等値結合の出力予約、ハッシュ衝突による過大見積もり、ホットキー分割時のプローブ複製。[ハッシュ結合](../../../executor/detail/pipeline/ooc_hash_join_sink.cpp) |
| [AggregateReplay](../../TinylambOpt/ResearchGoals/ResourcePolicies/AggregateReplay.lean) | 10 | 部分集約の再送を識別する単位、NULL だけのグループ、DISTINCT と局所 top-k の適用順。[グループ集約](../../../executor/detail/pipeline/ooc_group_agg_sink.cpp) |
| [MorselRetirement](../../TinylambOpt/ResearchGoals/ResourcePolicies/MorselRetirement.lean) | 9 | 順序付き LIMIT を打ち切れる完了区間、空結果と未着結果、遅い先頭タスクの後ろで滞留する出力。[並列テーブル走査](../../../executor/detail/parallel_table_scan.cpp) |
| [WindowRetention](../../TinylambOpt/ResearchGoals/ResourcePolicies/WindowRetention.lean) | 9 | ROWS、RANGE、IGNORE NULLS で次のバッチに引き継ぐ行。スライドする DISTINCT の失効と同順位のバッファ長。[ウィンドウ実行](../../../executor/detail/window_eval.cpp) |

## 保存容量と読み書き（64件）

| モデル | 件数 | 実装判断・測定項目 |
|---|---:|---|
| [RestartDirectory](../../TinylambOpt/ResearchGoals/ResourcePolicies/RestartDirectory.lean) | 11 | 再開位置の間隔と点参照の復号量、オフセット表の幅境界、例外ビットマップの rank による点参照。[TLCS 圧縮](../../../table/tlcs_compression.cpp) |
| [SharedVocabulary](../../TinylambOpt/ResearchGoals/ResourcePolicies/SharedVocabulary.lean) | 11 | ローカル ID の変換、共有後の ID 幅、前方一致辞書に1語を挿入したときに符号長が変わる範囲。[TLCS 圧縮](../../../table/tlcs_compression.cpp) |
| [ClusteredPages](../../TinylambOpt/ResearchGoals/ResourcePolicies/ClusteredPages.lean) | 9 | ページ結合で値のない隙間まで読む費用。ヘッダ・アラインメント込みの範囲走査を何回行えば再配置費を回収できるか。[TLCS ファイル](../../../table/tlcs_file.cpp) |
| [ScanCache](../../TinylambOpt/ResearchGoals/ResourcePolicies/ScanCache.lean) | 10 | 可変長ブロックの再利用距離、先読みの世代照合、共有キャッシュと固定シャード割当のミス数。[ページ管理](../../../page/page_manager.cpp) |
| [SpillExtents](../../TinylambOpt/ResearchGoals/ResourcePolicies/SpillExtents.lean) | 12 | 最後の生存ブロックから決まる切り詰め位置、更新失敗後の旧末尾、コピー中の一時ピーク。[spill ストア](../../../common/spill/spill_store.hpp) |
| [MergeScheduling](../../TinylambOpt/ResearchGoals/ResourcePolicies/MergeScheduling.lean) | 11 | マージ順による総書き込みと一時ファイルの同時占有量、読者バッファを含む fan-in、再分割を諦めるまでの I/O。[外部ソート](../../../executor/detail/ooc/external_sorter.cpp)・[再分割](../../../executor/detail/ooc/recursive_partitioner.cpp) |

## メモリ不足と長時間の負荷（44件）

| モデル | 件数 | 実装判断・測定項目 |
|---|---:|---|
| [GrantAdmission](../../TinylambOpt/ResearchGoals/ResourcePolicies/GrantAdmission.lean) | 12 | 保持済みメモリを返せる順番から必要な空きを計算する。キャンセルやプール間移管での二重計上を調べる。[メモリ予算](../../../common/memory_budget.hpp) |
| [SlabReuse](../../TinylambOpt/ResearchGoals/ResourcePolicies/SlabReuse.lean) | 10 | 連続領域の first-fit・best-fit、空きの結合、サイズクラス追加が後続の要求列に及ぼす影響。[割当予算](../../../common/memory_budget.cpp) |
| [ConnectionFlow](../../TinylambOpt/ResearchGoals/ResourcePolicies/ConnectionFlow.lean) | 11 | 部分送信後も保持するバッファ、接続別の読み取り再開条件、分割受信したフィールドの継続。[PostgreSQL サーバー](../../../server/postgres_server.cpp) |
| [MaintenanceDebt](../../TinylambOpt/ResearchGoals/ResourcePolicies/MaintenanceDebt.lean) | 11 | 処理し切れない仕事のピークを区間ごとの流入超過から求める。処理中に同じ対象が再び dirty になったときの再登録も扱う。[チェックポイント](../../../recovery/checkpoint_manager.cpp) |

## 並行制御・復旧・DDL（60件）

| モデル | 件数 | 実装判断・測定項目 |
|---|---:|---|
| [VersionVacuum](../../TinylambOpt/ResearchGoals/ResourcePolicies/VersionVacuum.lean) | 13 | 読者が実際に読む版を残す GC、部分コンパクション時の削除印、走査予算付き GC の再開位置。[トランザクション管理](../../../transaction/transaction_manager.hpp) |
| [CheckpointRetention](../../TinylambOpt/ResearchGoals/ResourcePolicies/CheckpointRetention.lean) | 11 | 境界をまたぐ WAL レコード、永続化済みファイルをすべて持つ世代の選択、旧読者が参照する共有ファイル。[チェックポイント](../../../recovery/checkpoint_manager.cpp) |
| [EvictionWriteback](../../TinylambOpt/ResearchGoals/ResourcePolicies/EvictionWriteback.lean) | 9 | ページ変更・スロット再利用・WAL 待ちの途中に到着した完了通知で、どのディスク像と dirty 状態を採用するか。[ページ管理](../../../page/page_manager.hpp) |
| [CatalogDependencies](../../TinylambOpt/ResearchGoals/ResourcePolicies/CatalogDependencies.lean) | 9 | DDL 後に再利用できる列束縛と物理オフセット。存在しない名前に依存する準備結果やワイルドカードも扱う。[カタログ](../../../database/database.hpp) |
| [WaitQueue](../../TinylambOpt/ResearchGoals/ResourcePolicies/WaitQueue.lean) | 8 | FIFO での読者群と単独ライターの交代、キャンセル後の再選択。待ちターン数と実時間を分けて評価する。[競合待ちの実装](../../../transaction/transaction_manager.cpp) |
| [CommitBatches](../../TinylambOpt/ResearchGoals/ResourcePolicies/CommitBatches.lean) | 10 | WAL の連続終端、再送や部分書き込みをまたぐ待機者通知、同じ fsync を待つクライアント数の上限。[WAL ロガー](../../../recovery/logger.cpp) |
