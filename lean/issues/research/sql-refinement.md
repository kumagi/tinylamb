# SQL の観測結果と既存の補題を接続する

## ORDER BY と FETCH WITH TIES

[提出型 `SqlFetchWithTies.Goal`](../../TinylambOpt/ResearchGoals/SqlFetchWithTies.lean) は行列全体を比較する。参照側は安定ソートして OFFSET を適用し、残った各行について「その行より厳密に先行する行数が k 未満」を判定する。境界キーの番兵、最適化側の比較関数、停止条件を共有しない。同率内の出力順には物理演算子の契約として安定順を採用した。

[キー成分の順序](../../TinylambOpt/Research/TieWatermark.lean) と [両ページ評価器のゼロ件分岐](../../TinylambOpt/Research/IncrementalSortPage.lean) を修正した。`sql_order_component` は NULL と値、値同士の順序を全方向について証明する。`zero_sql` は [SqlFetchWithTies の提出型](../../TinylambOpt/ResearchGoals/SqlFetchWithTies.lean) に定義され、任意の OFFSET と入力で両ページ評価器を上の参照計算に接続する。IncrementalSortPage 側の対応補題は `fetch_zero` である。全件数の `Goal` は未証明。正の件数では、境界と同順位の最後の行までが上の順位条件を満たす補題と、同率内の順序も含むソートの対応が必要。分散入力の読取り・最大メモリはこの出力等価性とは別の観測である。

実装では [SortKeyOrder](../../../type/collation.hpp) が比較契約を持ち、[TopNExecutor::Materialize](../../../executor/topn.cpp) がゼロ件分岐と比較を使う。整数キーと明示的 NULL 順序が今回の対応範囲。CLI で次をトップレベルの各文として確認した。

```sql
CREATE TABLE t(k INT64, id INT64);
INSERT INTO t VALUES (NULL, 9), (2, 2), (1, 1), (2, 3);
SELECT k, id FROM t ORDER BY k DESC NULLS LAST, id;
-- (2,2), (2,3), (1,1), (NULL,9)
SELECT k FROM t ORDER BY k DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES;
-- 2, 2
SELECT id FROM t ORDER BY id FETCH FIRST 0 ROWS WITH TIES;
-- 0 rows
```

## 他の課題で追加する接続命題

以下は監査で残った仕様案。対象の状態機械・評価器・実装経路を決めてから Lean の提出型を固定する。

| 対象 | 既存の定理へ渡す前に示すこと |
|---|---|
| adaptive radix / spill Bloom | 分割・部分消費・再試行後に各出現の所有者が一つに保たれ、完了時に全出現を一度ずつ消費する。Bloom の公開可能条件は seal と再読を含む遷移に関して不変である。そこから既存の `hcov`、`Valid` を導く。 |
| SIMD ページ検索 | 全ロードを実行時に記録する評価器を定義し、空入力、不一致、末尾を含む全経路の各アドレスが読取り可能であることと、出力位置列の一致を示す。 |
| タグ付き DFA / regex cache / 複数 LIKE | 実際の構文からコンパイルし、各遷移で履歴を捨てても優先順位で選ばれるキャプチャが同じことを示す。cache hit の生成コードにも適用する。LIKE は共有オートマトンの出力ビットから既存の必須片条件を導く。 |
| LLVM 翻訳検証 | 実生成カーネルの命令、フラグ、幅、レイアウトとロード条件を保存して検査器に渡す。未対応の構文は拒否する。受理後に既存のテンプレートの意味保存定理を適用する。 |
| 適応的 JOIN / 循環 JOIN / 外部 JOIN | 部分出力済みの組から再開した残余 bag、trie 交差で生成した中間行のトレース、キャンセルと worker 完了を含む NULL 補完の回数を各実行器から導く。 |
| 辞書 JOIN / ASOF | 世代ごとの表構築と失効から `TransSound` を示す。ASOF は境界の再検査も数える評価器で比較数を評価する。 |
| MVCC compaction / 索引積集合 | 実際に版を除去する遷移が読者ごとの可視版を保存することを示す。索引は要求される物理順がある場合、その順の再構成とバッグの多重度まで比較する。 |
| 短絡 DAG / LIMIT 融合 | 逐次履歴を参照せずに要求マスクを生成し、各親の要求ごとに結果・エラーが外へ現れる時点と実際に実行したレーンを比較する。 |
| 窓 DISTINCT | フレーム境界から出入りを直接列挙する更新器と既存の多重度差分を接続する。固定幅の SUM に適用するなら、参照器と同じオーバーフロー観測も必要。 |
