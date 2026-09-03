# PAX page format v1

> **Status(2026-08-24)**: v1 実装済み (`PageType::kPaxPage`,
> `PaxPage::Store/Load`)。全固定幅フィールドは big-endian で、未知の
> version・範囲外/重複領域・未対応 encoding/type は破損として拒否する。


PAXページは、同じページ内で行のMVCC可視性と列ごとの連続領域を分離する。既存`RowPage`とは別のページ型であり、v0 DB との混在読み込みは行わない。

## レイアウト

1. `PaxPageHeader`: 形式version、列数、行数、visibility領域、列directory、payload境界。
2. visibility領域: 将来の行ごと version-chain 参照用に予約された固定幅領域。現実装 (`PaxPage::Store`) は `rows*8` バイトを確保してゼロ埋めし、`Load` は内容を解釈しない。
3. `PaxColumnDirectory[column_count]`: 型、encoding、値領域、NULL bitmap、補助領域のoffsetと長さ。
4. NULL bitmap: 1 bit/row。1をNULLとする。現実装は未使用 capacity bit を 0 のまま残す。
5. 列値領域: INT64/DOUBLEは8 byte配列。VARCHARは`uint32_t[row_count + 1]`のoffset配列とbyte payload。
6. 補助領域: dictionary、bit packing metadata、zone mapを後方互換に追加できる。

すべてのoffsetは`Page::body`先頭からの`uint32_t`相対値とする。各 directory の値/NULL 領域はページ境界内に収まること、NULL bitmap が全行を覆うこと、encoding が `kPlain` であること、型が既知の `ValueType` であることを `Load` が検証し、違反は破損として拒否する。ヘッダの version が未知の場合も読み込みを拒否する。領域同士の重複検査と visibility 内容の解釈は未実装 (予約領域としてゼロ固定)。可視性を列データから独立させることで、MVCC判定後に必要列だけを連続走査できる。
