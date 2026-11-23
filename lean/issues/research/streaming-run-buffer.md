# spill ファイルをまたぐ run のバッファ上限とページ出力

[部分整列 FETCH](incremental-sort-page.md) の差し戻し。先頭キーが整列し、出現 ID が入力順に増える。ファイル終端で run を閉じず、先頭キーが変わるときにだけ pending 行をソート・出力する。

## 証明済みの命題

[`StreamingRunPage.Goal`](../../TinylambOpt/ResearchGoals/StreamingRunPage.lean) は [`ResearchProofs/StreamingRunPage.sound`](../../TinylambOpt/ResearchProofs/StreamingRunPage.lean) で証明済み。

SQL の安定ソート・順位によるページ定義と出力が一致し、バッファを更新した瞬間に記録する最大 pending 行数が最長 run 以下であること。ファイルの間に空ファイルがあり、次のファイルに最小の同率キーが2行ある例で、payload の順序とピーク3行を固定した。

## 適用範囲

ピークは未確定行数。ソートの一時割当て・出力領域・バイト数は含まない。入力の fold は停止後もリストをたどるので、物理 I/O の停止はこの提出型からは導けない。
