# TPC-C 高速化ロードマップ

縮小スケール・単一クライアントの実測と実行計画に基づく。TPC-H 向けのベクトル化・PAX・JIT は OLTP では効いていないため、ここでは後回しにする。完了条件を満たした項目は `[x]` に更新する。

基準（2026-08-19、Release、1 client、W=1/D=1/C=10/I=10/OL=2、3秒）:

- tps 約 40.7、sql_qps 約 390
- New-Order 約 30 ms / Payment 約 23 ms / Delivery 約 32 ms
- 点 SELECT の計画約 5 ms、実行 0.02〜0.2 ms
- `--clients > 1` 不可。顧客 100 や 5 秒超のランは segfault しやすい

## 優先順位

- [x] 1. 文テンプレートの計画キャッシュ（目安: 2〜4日、sql_qps で数倍）
  - [x] SQL リテラルを除いた指紋を定義し、同じ形状の文を同一テンプレートとみなす
  - [x] GoogleSQL の fork/exec をテンプレート単位で省略する
  - [x] キャッシュした AST に実リテラルをバインドしてから最適化する
  - [x] DDL とリテラル数不一致は従来経路へフォールバックする
  - [x] テンプレート抽出・バインド・連続 Prepare の回帰テストを追加する
  - [x] 縮小 TPC-C を再測し、計画時間と tps を記録する
  - 実装メモ: 二項式のバインドは引数評価順に依存しないよう左から順に束縛する。GoogleSQL の完全一致キャッシュは満杯時に 1 件だけ落とす。

- [x] 2. 複合索引の等価／範囲と正しい二次索引（目安: 3〜7日、スケール拡大で数倍）
  - [x] 先頭列だけを見る `AvailableKeyIndex` をやめ、全索引の連続プレフィックスを使う
  - [x] `(w,d,id)` 点検索、氏名索引、最新注文、`new_order` 最小 `o_id` を索引シークにする
  - [x] 索引順が `ORDER BY` と一致する昇順 `LIMIT 1` はソートを省略する
  - [x] 複合キーの単体テストと EXPLAIN の回帰を追加する
  - [x] 顧客 100 規模の TPC-C で計画が IndexScan になることを確認する
  - 残件: プレフィックスの DESC スキャンは終端キーの置き方が未完成のため、`ORDER BY o_id DESC` は索引シーク＋ソートのまま。

- [x] 3. OLTP 用の短い実行経路（目安: 2〜4日、安定性＋レイテンシ）
  - [x] 点 SELECT からクエリスケジューラを外す
  - [x] UPDATE/DELETE の空キー並列ソートをやめ、行位置を逐次マテリアライズする
  - [x] 小カーディナリティの ParallelSort を単スレッドにする
  - [x] 長時間／標準スケールの TPC-C が segfault せず完走することを確認する
  - 実装メモ: 索引ヒープ読みが空でも Evaluate しない。顧客 100・5 秒は完走（Delivery の一部 abort は残る）。

- [x] 4. ロック待ちと衝突時の打ち切り廃止（目安: 3〜7日、複数クライアント）
  - [x] 行ロック獲得を待ちキューにし、失敗即 `kConflicts` をやめる
  - [x] 索引スキャンの衝突でプロセスを落とさず、ステータスで返す
  - [x] ベンチマークの `--clients > 1` を許可する
  - [x] 衝突・待ち・複数クライアントの回帰テストを追加する
  - 残件: 1 client でも Delivery の `order_line` 更新が 0 件になることがあり abort する。`--clients > 1` のスループットは未計測。

- [x] 5. ホット行更新と WAL group commit（目安: 3〜6日、Payment/New-Order）
  - [x] 索引キーと INCLUDE が変わらない UPDATE はヒープだけ更新する
  - [x] WAL をチャンクごとの `fdatasync` から group commit にする
  - [x] district / warehouse / customer 更新のテストと TPC-C 再測を行う

- [ ] 6. TPC-H 成果の OLTP 適用は必要になるまで保留
  - [ ] PAX / zone map / バッチ JIT / 並列 hash join は分析クエリ用として残す
  - [ ] TPC-C のプロファイルで scan/JIT が支配的になってから再評価する

## 計測記録

- 基準 tps: 約 40.7（縮小スケール、1 client、3秒）
- 基準 sql_qps: 約 390
- 基準 New-Order: 約 30.0 ms（14 SQL / 明細2）
- 基準 Payment: 約 23.1 ms
- 基準 Delivery: 約 32.0 ms
- 基準 Order-Status: 約 9.1 ms
- 基準 Stock-Level: 約 5.3 ms
- 基準 点 SELECT 計画/実行: 約 5.0 ms / 約 0.015〜0.035 ms

実装後（2026-08-19、Release、1 client、warmup 0）:

- 縮小 W=1/D=1/C=10/I=10/OL=2、3秒: tps **2479**、sql_qps **24652**。New-Order 0.36 ms、Payment 0.26 ms、Order-Status 1.44 ms、Delivery 0.89 ms（committed 131/307）、Stock-Level 0.78 ms。exit 3（Delivery abort）
- 顧客 100 / items 100 / OL=5、5秒: tps **1350**、sql_qps **20518**。segfault なし。Delivery abort 29 件（`delivery line update affected too few rows`）
