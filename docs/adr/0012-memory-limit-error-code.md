# メモリ予算による拒否のエラーコード `kMemoryLimitExceeded`

Status: accepted（2026-11-16 決定、2026-11-17 P1 実装済み: `StatusCode::kMemoryLimitExceeded` → SQLSTATE `53200`）
決定者: ユーザーの委任による Claude の判断
関連: [`ooca-v2.md`](../../ooca-v2.md) §4 D10、[ADR 0004](0004-query-memory-grants.md)、
[ADR 0005](0005-spill-store-placement.md)

## 背景

元の設計（`ooca.md`）は予算超過時に `StatusCode::kResourceExhausted` を返すとしたが、
このコードは存在しない。`common/constants.hpp` にある近いコードは次の 2 つである。

- `kNoSpace`: ディスクやページの空きが無い（`std::bad_alloc` は OOM として
  `kMemoryLimitExceeded` に変換する。`common/exception_status.hpp`）
- `kTooBigData`: 1 つのデータが大きすぎる

メモリ予算で拒否された場合と、ディスクが尽きた場合は、利用者の対処が違う
（前者は上限を上げるか同時実行を減らす、後者はディスクを空ける）。同じコードにすると区別できない。

## 決定

`StatusCode` の末尾に `kMemoryLimitExceeded` を足す。使い分けは次のとおり。

| 状況 | コード |
|---|---|
| クエリへのメモリ付与が時間内に得られなかった | `kMemoryLimitExceeded` |
| 予約が失敗し、スピルもできない（最小予算を満たせない） | `kMemoryLimitExceeded` |
| 1 行、または集約結果の 1 値が演算子予算の 1/4 を超えた | `kTooBigData` |
| スピルのディスク上限を超えた、またはディスクが尽きた | `kNoSpace` |
| `std::bad_alloc` を捕まえた | `kMemoryLimitExceeded` |

- メッセージには、要求したバイト数、その時点の上限、どのプールかを入れる。
- `kMemoryLimitExceeded` は再試行で成功しうるエラーとして扱う。
- PostgreSQL プロトコルでは、`kMemoryLimitExceeded` を SQLSTATE `53200`（out_of_memory）、
  `kNoSpace` を `53100`（disk_full）に対応させる。

## 検討した案

| 案 | 採否 |
|---|---|
| `kNoSpace` を使い回す | 不採用。ディスクの問題と区別できない |
| `kResourceExhausted` という名前にする（元文書） | 不採用。何の資源かが名前から分からない |

## 影響

- `StatusCode` を文字列にする箇所と、網羅的な `switch` に 1 件ずつ足す。
- 旧経路が返しているエラーコードは変えない（[ADR 0006](0006-ooc-parts-on-new-sinks-only.md)）。
- サーバに SQLSTATE への対応付けを足す（現状、`53xxx` を返す箇所は無い）。
