# スピル基盤 `SpillStore` の置き場と I/O

Status: accepted（2026-11-16 決定、2026-11-17 P2 実装済み: `common/spill/spill_store.*`, `type/row_block_codec.*`, `executor/detail/ooc/chunk_codec.*`。設定環境変数 `TINYLAMB_TEMP_DIR` / `TINYLAMB_TEMP_LIMIT_BYTES`）
決定者: ユーザーの委任による Claude の判断
関連: [`ooca-v2.md`](../../ooca-v2.md) §4 D3、§5.2、[`ARCHITECTURE.md`](../../ARCHITECTURE.md)

## 背景

- 現行の `SpillFile`（`executor/spill_file.hpp`）は 1 インスタンスにつき 1 ファイルを開き、
  行（`Row`）単位で読み書きする。ランやパーティションが増えるとファイル記述子が足りなくなる。
- 元の設計（`ooca.md`）はスピル管理を `executor/detail/` に置いたが、同じ設計で
  `transaction/`（書き込みセット、未コミットページ）、`table/`、`page/`（LSM のコンパクション）
  もスピルを使う。これらは `executor/detail/` より下の層で、include できない。
- 元の設計では同じ物に `BlockSpillManager`、`SpillSegmentManager`、
  「Pooled Block Spill Store」と 3 つの名前があった。

## 決定

名前は `SpillStore` に統一し、責務を 3 つに分ける。

| 部品 | 置き場 | 内容 |
|---|---|---|
| `SpillStore` | `common/spill/` | バイト列のブロックを少数の一時ファイルに連結して置く。型を知らない |
| 行ブロックの符号化 | `type/` | `Row` と `RowPosition` の直列化。書き込みセットなど下位層の用途 |
| チャンクの符号化 | `DataChunk` と同じ層 | 列指向ブロックの直列化。実行層の用途 |

`SpillStore` の規則:

1. 開くファイル記述子は定数個。ランやパーティションは、ファイル内のブロックの連鎖として表す。
   解放されたブロックは再利用する。
2. ブロックサイズの初期値は 64 KiB（現行の `SpillFile` の読み込みバッファと同じ）。
3. 一時ファイルは、作成した直後に名前を消す。プロセスが落ちれば OS が回収するので、
   起動時の残骸掃除が要らない。名前を消したファイルを扱えない環境（Windows）では、
   閉じたときに削除される指定で開く。この差は `common/platform/file_io` に閉じ込める。
4. 置き場は既存の `ProcessMemoryBudget::ResolveTempDirectory()`（`TINYLAMB_TEMP`）を使う。
5. ディスク使用量の上限 `TINYLAMB_TEMP_LIMIT_BYTES` を新設する。超えたら `kNoSpace`。
6. I/O バッファのメモリは、使う側の予約から払う（[ADR 0003](0003-memory-budget-thresholds.md)）。
7. 書き終えたブロックは `AdviseDontNeed` でページキャッシュから外す。
8. ストレージのページプールは通さない。ページ層の作り直しが終わるまで依存させない。
9. 圧縮、`O_DIRECT`、非同期 I/O は入れない。必要になってから別の ADR にする。

## 検討した案

| 案 | 採否 |
|---|---|
| `executor/detail/` に置く（元文書） | 不採用。下位層から使えず、層の規則に反する |
| ストレージのページプール上の一時ページとして持つ | 見送り。メモリの管理は 1 つにまとまるが、ページ層の作り直しに依存する |
| ランごとに 1 ファイル（現状） | 不採用。ファイル記述子が入力量に比例する |

## 影響

- 旧演算子の `SpillFile` には触らない。旧演算子と一緒に消す
  （[ADR 0006](0006-ooc-parts-on-new-sinks-only.md)）。
- `common/platform/file_io` に「名前の無い一時ファイルを開く」操作を足す。
  macOS / Linux / Windows の `platform_test` に契約を足す。
- `TINYLAMB_TEMP_LIMIT_BYTES` を `docs/environment_variables.md` に書く。

## 採用条件

予約の失敗とスピル I/O の失敗を任意の位置に注入しても、一時ファイルと予約が残らないこと。
ランを 1 万本作ってもファイル記述子の数が変わらないこと。
