# 列指向結果から PostgreSQL `DataRow` チャンク直列化（Direct Columnar-to-PG-Wire Chunked Framing）

**未証明。** 大量行返却クエリ（`H2` 37,709 行・`H4` 29,057 行・`L2` 25,779 行・`B1` 19,646 行）では、PostgreSQL Simple Query サーバの `PullSelectWireChunk` が結果を 1 行ずつ汎用 `Row`（`std::vector<Value>`）へコピーして `DataRow(const Row&)` で文字列化しているため、実測時間の **48%〜70%**（`H2` / `H4` / `B1` で +12.8 ms〜+17.5 ms、`L2` で +11.7 ms [28%]）がワイヤ整形＋行変換に費やされている。列ベクトル（`intCol`, `strCol`）から `Row` を経由せず直接 `DataRow` バイナリフレーム（`'D'` + BE32 フレーム長 + BE16 列数 + BE32 列長/NULL`-1` + バイト列）を `kClientStreamChunkBytes` チャンクに書き出しても、連結されたワイヤバイト列が行実体化版と 1 バイトの狂いもなく完全一致することを検証する。

## 対象敗北クエリとボトルネック

実測したサーバ内実行時間（`EXPLAIN ANALYZE Execution Time`）と `psycopg2` クライアント全体の差（`SF=1`, `2026-11-23` 計測）：

| クエリ | 返却行数 | サーバ内実行 | クライアント実測 | ワイヤ＋行変換差分 | 対 DuckDB |
| --- | ---: | ---: | ---: | ---: | --- |
| **H2O H2** | 37,709 | `11.5 ms` | **`29.0 ms`** | **`+17.5 ms` (60%)** | `10.9 ms` (`2.66x`) |
| **H2O H4** | 29,057 | `5.4 ms` | **`18.2 ms`** | **`+12.8 ms` (70%)** | `5.9 ms` (`3.08x`) |
| **BigBench B1** | 19,646 | `15.2 ms` | **`29.0 ms`** | **`+13.8 ms` (48%)** | `11.5 ms` (`2.51x`) |
| **LDBC-SNB L2** | 25,779 | `30.0 ms` | **`41.7 ms`** | **`+11.7 ms` (28%)** | CH `38.5 ms` (`1.08x`) |
| **STATS-CEB C2** | 3,514 | `4.1 ms` | `3.9 ms` | 境界計測誤差内 | `3.8 ms` (`1.03x`) |

※特に **`H4` はサーバ内実行 `5.4 ms` で既に DuckDB (`5.9 ms`) に勝っており、ワイヤ／`Row` 実体化だけで敗北**している。

## 提出型

[`ColumnarToPgWireChunkedFramingEquiv.Goal`](../../../TinylambOpt/ResearchGoals/ColumnarToPgWireChunkedFramingEquiv.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv.Goal` を提出する。

任意の矩形列バッチ（`∀ c ∈ cols, colLength c = n`）と任意の正チャンク上限 `0 < chunkBytes` について、`(optChunkedEncode cols n chunkBytes).flatten = refRowEncode (transposeCols cols n)` を示す。

## 境界例と適用範囲

- 負数 `"-42"`（先頭 ASCII `'-'` = `45`）・`0`（`"0"`, 1 バイト）・`NULL`（長さフィールド `0xFFFFFFFF = [255,255,255,255]`、データ部 0 バイト）・空文字列 `""`（長さ `0` = `[0,0,0,0]`、データ部 0 バイト）のすべてのフレーム境界長が 1 バイト単位で一致する。
- `chunkBytes = 1`, `7`, `16`, `256` のように `DataRow` フレーム境界を跨ぐ位置でチャンク分割されても、連結ストリームは完全に不変。

## 実装との照合先

- `server/postgres_server.cpp` (`PullSelectWireChunk`, Lines 2340–2390 / `kClientStreamChunkBytes`, Line 63)
- `server/postgres_protocol.cpp` (`DataRow(const Row&)`, Lines 377–397)
