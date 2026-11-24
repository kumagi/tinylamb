# 派生分析ベンチマーク比較（5種 × 3DB、SF指定・グループ棒グラフ）

[CostBench・Snowset/PBench・Iceberg・Public BI の追加比較](reports/cloud-bi/README.md)は、同じ 3 エンジンのアダプターを使って混在負荷、巨大 SQL、削除適用、実データの文字列集約を測定しています。

`analytics_bench.py` は、既存の3製品比較ハーネス（`run.py`）とは独立した
スタンドアロンツールです。既存ファイルは変更しません。

SF（scale factor）を指定すると、5種類の分析ベンチマークの**似た構造**を持つ
データを合成生成し、tinylamb / DuckDB / ClickHouse の3DBへロードして
クエリを計測し、**SF固定のグループ棒グラフ**を出力します。

## 対象5ファミリ

実データセット（IMDb 約7,400万行、StackExchange、LDBC SNB、TPCx-BB dbgen）は
リポジトリに無いため、既存の「TPC-H-derived」と同じ方針で**構造だけを再現**した
派生物です。公式スコアでも実データでの測定でもありません。

| キー | 元ネタ | 再現している負荷 |
| --- | --- | --- |
| `job` | Join Order Benchmark (IMDb) | 13表の多段結合、年度×制作国×ジャンルの相関、評価情報の偏り、NULL。結合順序／基数推定を攻める5問 |
| `stats_ceb` | CEB / STATS-CEB (StackExchange) | 8表のスター＋チェーン＋FK-FK結合、スコアと閲覧数の相関、投稿へのアクセス偏り、NULL。サブプラン基数を攻める5問 |
| `h2o` | H2O.ai db-benchmark | `x` / `y` の高カーディナリティ GROUP BY とハッシュ JOIN、べき乗則のキー分布。6問 |
| `ldbc_snb` | LDBC SNB BI | Person/Forum/Message の多ホップ自己結合、次数の偏り、返信チェーン、年度別集計。5問 |
| `bigbench` | AMPLab Big Data Benchmark / TPCx-BB | uservisits×rankings のスキャン/テキスト絞り込み、store_sales の星型結合、多段集計。5問 |

各行は降順に並べた Medal ではなく、各クエリの**中央値レイテンシの合計**を
ファミリの値として棒グラフにします（低いほど速い）。別図でクエリ単位の内訳も出します。

## 必要なもの

```bash
python3 -m venv /var/tmp/tinylamb-comparison-venv
/var/tmp/tinylamb-comparison-venv/bin/pip install duckdb==2.0.0 matplotlib==3.11.2 psycopg2-binary==2.9.13
```

- **DuckDB**: 組込み。追加サーバ不要。
- **tinylamb**: GoogleSQL 有効でビルドしたサーバが必要
  （例 `build-verify/tinylamb_server`）。`--server` を渡すと一時DBと専用ポートで
  自動起動・終了します。既存サーバへは `--tinylamb-dsn` で接続します。
- **ClickHouse**: ネイティブ HTTP インターフェースへ到達できること。Python
  ドライバは不要です。`--clickhouse-binary` を渡すと、`clickbench.py
  init-clickhouse` やローカルの TPC-H / TPC-DS 実行と同じ localhost 専用・
  UTC・`max_threads=<--threads>` の構成で一時サーバーを自動起動し、終了時に
  停止します。既存サーバーを使う場合は `--clickhouse-url` を指定します。
  どちらにも到達できない場合は `unavailable` に理由を記録し、残るDBだけで
  グラフを出します。

```bash
# 既存ベンチマークと同じバイナリで ClickHouse を自動起動して3DB比較
python3 benchmark/comparison/analytics_bench.py \
  --output /var/tmp/analytics-sf1 --scale 1 \
  --server build-verify/tinylamb_server \
  --clickhouse-binary build/benchmarks/clickhouse-bin/clickhouse-common-static-26.10.2.103/usr/bin/clickhouse
```

## 実行

```bash
# 5種 × 3DB、SF=1（既定）
python3 benchmark/comparison/analytics_bench.py \
  --output /var/tmp/analytics-sf1 --scale 1 \
  --server build-verify/tinylamb_server

# ファミリ・DB・反復回数を絞る
python3 benchmark/comparison/analytics_bench.py \
  --output /var/tmp/analytics-h2o --scale 0.5 \
  --families h2o ldbc_snb --engines duckdb clickhouse \
  --clickhouse-url http://127.0.0.1:8123 --repeats 5 --warmup 2

# データ生成のみ（DB無しでデータ形状を確認）
python3 benchmark/comparison/analytics_bench.py \
  --output /var/tmp/analytics-data --generate-only --scale 0.01

# 保存済み結果からグラフだけ再描画（DB不要）
python3 benchmark/comparison/analytics_bench.py --report /var/tmp/analytics-sf1
```

`--scale` は全表の行数に線形に効きます（SF=1 の合計は約182万行・全26クエリ）。
`--seed` を変えると相関・偏りを保ったまま別データが生成されます。
計測はウォームアップ1回＋計測2回の中央値が既定です。クエリごとに
開始DBをローテーションし、常に同じDBが温かい有利さを受けないようにしています。

## 出力物

| ファイル | 内容 |
| --- | --- |
| `environment.json` | SF、seed、反復数、利用可能／不能DBとその理由、DBバージョン、データセットSHA-256、git HEAD |
| `results.jsonl` | クエリ×DBごとの全サンプル、中央値、SQL、状態、`answer_match` |
| `summary.json` | ファミリ別合計、最速DB、応答一致状況 |
| `grouped_bars.png` / `.svg` | **SF固定のグループ棒グラフ**（X=5ファミリ、棒=DB） |
| `per_query.png` / `.svg` | クエリ単位の内訳 |
| `dataset/<family>/*.csv` | 合成データ（`\N` がNULL、空文字列は `""`） |

3DBすべてで応答が取れた場合、多重集合比較（整数厳密・浮動小数は
相対1e-7／絶対0.01の許容）で結果が一致するかも検証します。
不一致は `status=mismatch` として保存され、グラフの対象から外れます。

## 限界

- 実データではないため、真のJOB/CEBの選択率や実世界の相関をそのまま
  再現したものではありません。相関・偏り・NULL・多段結合の「形」を
  再現したエンジニアリング probe です。
- 主キー／二次索引は張りません。索引選択の比較ではなく、結合順序・
  基数推定・GROUP BY/JOIN カーネルとメモリ圧力の比較に焦点を当てます。
- ClickHouse は HTTP経由、tinylamb は PostgreSQL Simple Query 経由、
  DuckDB は組込みAPI経由なので、通信・ドライバ費用は同一ではありません。
- 認定ベンチマーク結果ではありません。

## テスト

```bash
python3 -m unittest discover -s benchmark/comparison -p 'analytics_bench_test.py' -v
python3 benchmark/comparison/analytics_bench.py --list-families
python3 benchmark/comparison/analytics_bench.py --list-engines
```

テストはDBサーバを必要としません。DuckDBが入っていれば全26クエリを
小規模データで実際に実行し、matplotlibが入っていればグラフ生成も検証します。
