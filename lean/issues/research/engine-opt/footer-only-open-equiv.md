# TLCS v3 フッタ限定オープンとオンデマンド列スライス読取りの等価性

**未証明。** 永続化された TLCS v3 ファイルのオープン時にデータ本体を逐次デシリアライズせずフッタ（列チャンクディレクトリ）のみを読み、後続クエリで参照された列チャンクだけをランダムアクセス取得する方式の正しさと I/O 量を検証する。

## 提出型

[`FooterOnlyOpenEquiv.Goal`](../../../TinylambOpt/ResearchGoals/FooterOnlyOpenEquiv.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv.Goal` を提出する。

全列を順次ロードする参照オープンと、フッタ限定オープン＋列要求列 `cols` の結果一致、オープン時読取りバイト数が総データ長および末尾ゴミデータに依存せず `footer.length` 固定であることを要求する。

## 境界例と適用範囲

末尾に余分なバイト列（`trailingGarbage`）が存在しても、ディレクトリ記述の `(offset, len)` スライスにより破損せず一致する。

## 実装との照合先

- `table/tlcs_file.cpp` (`OpenTlcsFile`, TLCS v3 footer directory)
- `database/database.cpp`（DB オープン高速化経路）
