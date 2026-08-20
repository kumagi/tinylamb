# TPC-C SF=2 高速化

当面の目標は SF=2、20 ターミナル、60 秒、エンジン abort ゼロ。1 クライアントの `new_order_tpm` は回帰ベースライン。完了したら `[x]` にする。

- [x] 1. `RequiresHistoricalRead()` をプランゲートに使わない。可視性は行単位。フラグは O(1)。
- [x] 2. Stock-Level を `order_line_pk` 範囲スキャンと `stock_pk` プローブに書き換える。
- [x] 3. 読み書きトランザクションでもフルスキャン中にページをピンしたままにする。version 読み取りキャッシュに上限を付ける。
- [x] 4. Stock-Level と Order-Status を `BeginReadOnlyContext()` で実行する。
- [x] 5. `DataChunk` が先頭行の NULL から型を推論しない（`o_carrier_id` / `ol_delivery_d`）。
- [x] 6. ロードを warehouse 単位のトランザクションに分割する。
- [x] 7. ピン underflow を防ぐ。eviction が止まらないようにする。プールを SF=2 向け（約 2 GiB）にする。
- [x] 8. `template_cache_mutex` を分割する。Cascades のデフォルトルール確保をホイストする。
