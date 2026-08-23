# TPC-H SF=10 全クエリ合計 60 秒未達成ロードマップ

2026-08-24時点の調査に基づく。現状数値は `BenchmarkHistory.md`(2026-08-20 SF=1
スナップショット)参照。

## 現状と目標のギャップ

| 項目 | 現状(SF=1) | 目標(SF=10) |
| --- | --- | --- |
| lineitem 走査 | 6M行で12〜20秒(約30〜50万行/秒、単スレッド) | 60M行を数秒(実効速度 約1000倍) |
| 完走性 | 17/22。Q8/Q9/Q12/Q13/Q22 は segfault | 22/22 合計60秒(平均2.7秒/クエリ) |

ボトルネックは IO ではなく CPU(RAM上でも12秒かかる)。以下のコードが直接原因。

- 走査: 行ごとの `txn.ReadVersion()` + `Row::Deserialize`
  (table/full_scan_iterator.cpp:78-103)
- `ReadVersion` は全行で shard ミューテックス+ハッシュ参照+文字列コピーを
  version_read_cache_ へ実施(transaction/transaction.cpp:91-112)
- 結合: 両入力完全 materialize + 文字列キーハッシュ(executor/hash_join.cpp)
- 集約(GROUP BY): 行ごと仮想呼び出し Evaluate + Row キーのハッシュ表
  (executor/relational.cpp:2672-2757)

## Phase 0: 正当性(既知問題の解消)

1. **DATE 述語の検証**。SF=1 の過去測定で Q1 が 0 行だった
   (`l_shipdate <= date_sub(...)`)。`Value::DateDays` の int64 日数化は済んでいる
   ように見えるため、まず Q1 が4グループ返ることを小規模データで確認する。
2. **Q8/Q9/Q12/Q13/Q22 の segfault**。hash-join / plan Dump 系のクラッシュ。
   Q7 は実行成功後の `Dump()` で死んだ前例あり。
3. **SF=10 での統計情報**。ヒストグラムの B+tree 分割は済み。
   ロード+ANALYZE 時間が許容範囲か確認。

## Phase 1: 走査の高速化(最大のレバー)

### 1-1. ParallelScan を実行プランに接続
- morsel駆動並列スキャナが実装済み(executor/parallel_scan.hpp:24)だが
  `plan/` から一切参照されずテスト専用。全スキャン単スレッド。
- 対策: FullScanPlan の EmitExecutor で行数・コストが閾値超なら ParallelScan を選択。
- 期待: **8〜16倍**(16コア/32スレッド想定)

### 1-2. 走査時の行ごと MVCC 処理を排除
- 読み取り専用トランザクションでも全行で ReadVersion の mutex+ハッシュ+コピーが走る。
- 対策: スナップショット安定ページへのファストパス
  (read-only txn は物理行をそのまま読む。writer 側は copy-on-write か
  visibility bitmap で整合性を保証)。
- 期待: **2〜4倍**

### 1-3. カラムナ / PAX ページの実装
- docs/pax_page_format.md は設計のみ。実装はメモリ上の PaxBlock
  (辞書+ビットパッキング、page/pax_block.hpp)にとどまりページ型未統合。
- 対策: PAX ページ型を導入し、TPC-H テーブルを PAX で格納。
  必要列のみ連続走査、Row::Deserialize→Value 配列化を廃止。
  DataChunk/ColumnVector(executor/data_chunk.hpp)がそのまま受益。
- 期待: **2〜4倍**

Phase 1 合計で走査は **約1/50〜1/200**。

## Phase 2: 結合・集約・ソート

### 2-1. HashJoin の作り直し(executor/hash_join.cpp)
- 現状:
  - 両入力を `vector<Row>` に完全 materialize(:122,123)
  - 結合キーを行ごと `EncodeMemcomparableFormat()` で std::string 化(アロケーション)
  - `unordered_multimap<std::string, const Row*>` で build(:46-51)
  - 出力も全 materialize してから最初の行を返す(:57)
  - 入力 pull は単スレッド Next()。join フェーズだけパーティション並列
- 対策:
  - int64 直接キー(複合キーはハッシュ合成)のオープンアドレス法ハッシュ表
  - probe 結合結果のストリーミング出力(materialize 廃止)
  - build/probe 両方を並列化(パーティション毎)
  - build サイド選択は統計に基づく(現在は left 固定の模様)
- 期待: **5〜10倍**

### 2-2. GROUP BY のベクトル化集約
- 現状: relational.cpp の Project() 内で unordered_map<Row,size_t> に行ごと
  Evaluate()(仮想呼び出し)で突込む(:2672-2757)。partition_agg はあるが
  SpillFile への Row エンコード経由。
- 対策: 型付きアキュムレータ(int64/double の sum/count/min/max)+選択ベクトル
  投げ込み+並列パーティション集約(ローカル集約→マージ)。
- 期待: Q1 系で **5〜10倍**

### 2-3. フィルタの選択ベクトル化
- Selection は bytecode/JIT+ゾーンマップスキップを持つが、最終絞り込みが
  行ごと ValueAt+Append コピー(executor/selection.cpp:139-147)。
- JIT 昇格閾値が累積2,000万評価(docs/jit_profile.md)で高すぎる。
  OLAP 向けにはバッチ数または推定行数で早期昇格。

### 2-4. 相関サブクエリの decorrelation
- Q17/Q20(not-in/not-exists 型アンチジョイン)、Q21(EXISTS)、
  Q13(LEFT JOIN + LIKE 集約)が遅い・落ちる原因。
- 対策: semi join / anti join 演算子を plan/executor に追加し、
  Cascades 側で相関サブクエリを semi/anti join へ書き換える。

### 2-5. ソートの並列化
- ORDER BY 付き(Q3/Q4/Q5/Q10 等)は SortExecutor が単スレッド。
  エンコード済みキーの radix sort + 並列外部マージへ。

## Phase 3: メモリ・IO

1. **QueryMemoryBudget とスピル回避**。SF=10 は RAM(59 GiB 想定ホスト)に収まる。
   予算判定(hash_join.cpp:132 など)で即 spill せず、インメモリ判定を広げる。
2. **バッファプールの並列アクセス対応**。グローバル pool_latch
   (page/page_pool.hpp:117)は並列スキャン時に頭打ち。
   docs/tpcc-improvements.md Phase 1-1 と共通課題。シャード化 or clock-sweep。
3. **fstream 廃止**(page/page_pool.cpp:106)。pread/pwrite または mmap へ。
   並列読み取り時に file_latch_ がシリアライズ点になる。

## 効果の積算イメージ

| 施策 | 累積効果 |
| --- | --- |
| 現状 | Q1 = 11.9秒(SF=1)、最長34秒 |
| Phase 0(完走22/22) | 測定可能な状態に |
| Phase 1(並列+MVCC排除+カラムナ) | 走査 約1/50〜1/200 |
| Phase 2(結合/集約ベクトル化+decorrelation) | 重いクエリ含め全体 約1/100〜1/500 |

並列化 × カラムナ × 型付きハッシュ集約の組合せで「SF=10 合計60秒」は現実的。
Phase 1 を省略して個別最適化だけでは桁が足りない。

## 測定方法

```console
cmake --build build-rel -j --target tinylamb_tpch_benchmark
./build-rel/tinylamb_tpch_benchmark /var/tmp/tinylamb-tpch-sf10/database \
  --scale-factor 10 --data-dir /var/tmp/tinylamb-tpch-sf10/data
```

- 各フェーズ後に全22クエリを実行し slowest-first サマリで比較
- 正当性チェック: Q1 4グループ、Q6 単行、カーディナリティ妥当なのは
  Q2/Q11/Q16/Q17/Q18/Q19(過去測定での正答クエリ群)を回帰確認に使う
