# PAX page format v1

PAXページは、同じページ内で行のMVCC可視性と列ごとの連続領域を分離する。既存`RowPage`とは別のページ型として導入し、移行中のDBを読み書きできるようにする。

## レイアウト

1. `PaxPageHeader`: 形式version、列数、行数、visibility領域、列directory、payload境界。
2. visibility領域: 行ごとのversion-chain参照を固定幅offsetで保持する。0は物理行を使用することを表す。
3. `PaxColumnDirectory[column_count]`: 型、encoding、値領域、NULL bitmap、補助領域のoffsetと長さ。
4. NULL bitmap: 1 bit/row。1をNULLとし、未使用capacity bitも1にする。
5. 列値領域: INT64/DOUBLEは8 byte配列。VARCHARは`uint32_t[row_count + 1]`のoffset配列とbyte payload。
6. 補助領域: dictionary、bit packing metadata、zone mapを後方互換に追加できる。

すべてのoffsetは`Page::body`先頭からの`uint32_t`相対値とする。領域は重複せずページ境界内に収まり、ヘッダのversionが未知なら読み込みを拒否する。可視性を列データから独立させることで、MVCC判定後に必要列だけを連続走査できる。
