# TPC-C 改善計画(100倍化ロードマップ)

2026-08-24時点の調査に基づく。現状数値は `BenchmarkHistory.md` 参照:

| 構成 | tps | new_order_tpm | 状態 |
| --- | ---: | ---: | --- |
| SF=1, 1クライアント | 821 | 23047 | New-Order 1.10ms |
| SF=1, 10クライアント(規約構成) | 6.90 | 198 | マルチクライアントで負スケーリング |
| SF=2, 20クライアント | 3.40 | 99 | engine aborts 10件 |

目標「100倍」≈ **規約構成(10ターミナル/warehouse)で数万tps**。
現在はマルチクライアント時に競合で崩壊しており、単一クライアント性能と並列スケール両方の改善が必須。

## Phase 0: 正当性の修正(前提条件)

これらはtpsを直接消費するエンジンアボート・クラッシュの原因。

1. **Delivery の DELETE 失敗**
   - 症状: 1クライアントで `delivery queue delete affected too few rows`、0/2080 コミット。
     SELECT MIN(`new_order`) と DELETE の可視性が一致しない(plan/visibility 不整合)。
   - 影響: 全測定で Delivery が全滅し、mix も Clause 5.4.2 を満たさない。
   - 関連: `benchmark/tpcc_workload.cpp`(Delivery実装)、`table/table.cpp` の `Table::Delete`、
     `transaction/transaction_manager.cpp` のバージョン可視性(`ReadVersion`)。

2. **`invalid page type`(バッファプール競合)**
   - 症状: 10クライアント実行で buffer-pool/latch 系の破綻。page validity check の競合。
   - 関連: `page/page_pool.cpp` の `GetPage()` 再取得競合パス。

3. **`new-order stock update affected too few rows`(高負荷時アボート)**
   - 症状: 20ターミナルで発生。lock wait vs lost update の疑い。
   - 関連: `transaction/lock_manager.cpp` の待機処理(5秒タイムアウト後 false)、
     `executor/update.cpp` の中断挙動。

## Phase 1: 競合排除(最大の壁)

マルチクライアント時の負スケーリングの主因。グローバルロック3点が直列化している。

### 1-1. PagePool の単一グローバル mutex 撤廃
- 現状: `pool_latch`(page/page_pool.hpp:117)が GetPage/Unpin ごとに取得される。
  B+tree の1点参照は root-to-leaf の各レベルで GetPage ×(取得+解放)= 6〜8回の
  グローバルロック獲得。さらにキャッシュヒットでも `Touch()` が LRU リスト要素を
  移動(page/page_pool.cpp:174-181)しキャッシュラインバウンスを起こす。
- 対策:
  - pool_latch を page_id 単位でシャード化(例: 64 shards)
  - pin_count を atomic 化し lock-free な clock-sweep 方式へ(LRUリスト移動を排除)
  - ページラッチ(`Entry::page_latch`)は現状 shared_mutex だが、読み取り主体なら
    seqlock/楽観的バージョン検証も選択肢

### 1-2. LockManager の単一グローバル mutex 撤廃
- 現状: 全行ロックが1つの `std::mutex latch_` + unordered_map 2つに直列化
  (transaction/lock_manager.hpp:36-39)。TPC-C の全行書き込みがここを通る。
- 対策: MVCC 版ストア(first-updater-wins)が既にあるため、行ロック機構自体を
  バージョン管理と統合して削除するのが本筋。
  - `Transaction::AddWriteSet`(transaction/transaction.cpp:77-89)での排他ロック取得を
    version store への pending 登録+競合検出に置換
  - デッドロック検出が必要なら待機グラフを shard 化した上で導入

### 1-3. コミットパスのグローバル直列化解除
- 現状: `CommitVersions` が全必要shardロック + グローバル `transaction_table_lock`
  を保持して commit timestamp を発行(transaction/transaction_manager.cpp:158-194)。
  snapshot 取得(`Begin`)も同じロック。コミットがシステム全体で完全直列化。
- 対策:
  - commit ts は atomic fetch_add、バージョン公開を shard 単位で先に行い ts 発行のみ
    atomic に(公開順と ts 順の一貫性はエポック方式で保証)
  - `Begin` 側のスナップショット取得も atomic 読みに

### 1-4. GC をコミットクリティカルパスから外す
- 現状: `GarbageCollectVersions()` が**非readonlyコミット毎**に呼ばれ
  (transaction/transaction_manager.cpp:230)、全アクティブスナップショット走査 +
  **全shard・全バージョンエントリ**をロックしながら掃除(:233-267)。
  O(生きている全行バージョン)/commit であり、バージョン累積で劣化が加速する。
- 対策: 専用バックグラウンドスレッドで閾値駆動に変更。エポックベースの
  cleanup(最古スナップショット以降のみ残す)は現行ロジックを流用可能。

## Phase 2: 1ステートメントあたりのコスト削減

New-Order 1.10ms(〜38ステートメント/トランザクション)。目標: トランザクション100µs未満。

### 2-1. プランキャッシュ実装
- 現状: SQLテンプレートキャッシュ(query/sql_engine.cpp:105-139)は**ASTまで**。
  キャッシュヒット後も毎回:
  - `BindStatementLiterals` による AST 全体ディープコピー(query/sql_engine.cpp:297-305)
  - `QueryData::Rewrite` による名前解決
  - `Optimizer::Optimize`: 式書き換え + algebra rewrite + Cascades メモ構築と探索
    (plan/optimizer.cpp:479-654)
  - `EmitExecutor`
- 対策: fingerprint → コンパイル済みプラン(Executor ファクトリ)をキャッシュし、
  パラメータだけ差し替えて再実行。統計更新時のみ無効化。

### 2-2. トランザクションあたりSQL発行数削減
- New-Order は 1トランザクション 〜38ステートメント。全てパース→プラン→実行。
- 対策: ステートメント一括実行(multi-statement)、または TPC-C 各トランザクションを
  ネイティブ実行パス(stored-proc的API)で提供。ベンチドライバから直接呼べる内部APIは
  ボトルネック切り分けにも有効。

### 2-3. 行表現のコピー削減
- UPDATE で serialize→deserialize が往復(table/table.cpp:134-163)
- `RegisterVersionWrite` が before/after 行を即時文字列コピー
  (transaction/transaction_manager.cpp:126-129)。before は undo 用だが遅延評価可能
- `Row::Deserialize` が参照のたびに Value 配列再構築(table/table.cpp:211-218)
- 対策: インプレース更新可能な固定レイアウト、delta 更新、Value の SSO/inline 化

### 2-4. 挿入ホットスポット解消
- 現状: `Table::Insert` が常に `last_pid_` ページに書く(table/table.cpp:74)。
  全スレッドが同一ページラッチに集中し、満杯なら next チェーンを辿る。
- 対策: free-space マップ(PFS)による候補ページ分散、per-thread アペンダ

### 2-5. 索引操作コスト削減
- 非ユニーク索引: 挿入/削除のたびに `vector<IndexValueType>` をデコード→追記→
  エンコード(table/table.cpp:237-246)。→ duplicate key の chain 構造化
- 操作ごとに `BPlusTree bpt(idx.pid_)` を生成し root-to-leaf を各レベル
  GetPage(index/b_plus_tree.cpp)。→ カーソル再利用、楽観的ラッチカップリング
- UPDATE 時に全索引キーを再生成して比較(`IndexCoversUnchanged`,table/table.cpp:125-132)
  → 変更列と索引キー列のオフセット比較で早期判定

## Phase 3: ログ・その他

1. **kBegin レコード廃止**: 毎トランザクション1レコード
   (transaction/transaction_manager.cpp:42-44)。ARIES的には txn id 追跡で不要。
2. **ログゼロコピー化**: 全 LogRecord が `Serialize()` で一時文字列生成。
   固定長レコード+直接エンコードへ。
3. **式評価**: 仮想呼び出しツリーのまま。JITは累積2,000万評価超えで有利
   (docs/jit_profile.md)のため OLTP 向けは bytecode 直実行+定数畳み込み済み
   評価器を用意。
4. **Abort時のビジーウェイト**: flush 待ちが1ms sleep ループ
   (transaction/transaction_manager.cpp:78-81)。条件変数化。

## 到達イメージ(概算)

| 施策 | 期待効果(規約構成 tps) |
| --- | --- |
| 現状 | 6.9(SF=1, 10c)/ 821(1c) |
| Phase 0 完了(アボート除去) | 数十〜数百 |
| Phase 1(線形スケール達成) | 821×係数 → 数千 |
| Phase 2(ステートメントコスト 1/5〜1/10) | 数千〜数万 |
| インメモリOLTP化(Silo/Hekaton型) | 数万〜 |

Phase 0〜2 で概ね50〜100倍。残差を狙う場合はディスクページ経由の行アクセス
(GetPage+pin/unpin+ページラッチ)を排除したインメモリストア
(エポックGC + 楽観的CC + delta更新)への移行が現実的な道筋。

## 最初の一歩

投資対効果が高いのは Phase 0 の3件(特に Delivery)。修正後に
`--clients 10 --seconds 60` で再測定し、Phase 1 のどのロックが律速かを
perf で確認してから着手すること。
