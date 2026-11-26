# ストレージ・トランザクション・SQL 実行にまたがる 100 題

tinylamb の実装で判断が分かれる 10 分野から、各 10 件の命題を選んだ。たとえば「この WAL 同期でどの待ち手まで起こせるか」「この統計で結合結果の上界を保証できるか」のように、ガード・観測結果・費用を Lean で具体化している。全件が個別の `def …Goal : Prop`。既知の反例は各モジュール末尾の `#guard` とともに残した。[一括 import](../../TinylambOpt/ResearchGoals/EngineBoundaries.lean)

完全名は `TinylambOpt.ResearchGoals.EngineBoundaries.<分野>.<命題>.Goal`。表の分野から 10 件すべての宣言と参照計算に移れる。

| 分野 | 取り上げた命題と判断 | 対応する実装・仕様 |
| --- | --- | --- |
| [GroupCommit](../../TinylambOpt/ResearchGoals/EngineBoundaries/GroupCommit.lean) | 最大の待機 LSN でまとめて起こす条件、実行中の同期への合流、書込み途中のレコード、同期失敗後の再試行。同期開始時と完了時の書込み位置を区別する。 | [WAL の永続化待機](../../../recovery/logger.hpp#L144)・[復旧時の永続化境界](../../../docs/recovery_invariants.md) |
| [IndexHistory](../../TinylambOpt/ResearchGoals/EngineBoundaries/IndexHistory.lean) | 有界な索引更新履歴から、あるスナップショット以後にキーレンジが変わったかを答える。全体、索引別、レンジ別の破棄時刻を比較し、完全な履歴と照合する。 | [破棄済み履歴の透かしと精度切替](../../../transaction/transaction_manager.hpp#L798) |
| [WaitCycles](../../TinylambOpt/ResearchGoals/EngineBoundaries/WaitCycles.lean) | 年齢順の待機、循環に属する辺だけの再確認、犠牲者が待ち続けていることだけを確認する案、古い待機の解除が新しい待機を取り消す条件。 | [write intent の待機・検出方針](../../../docs/lock_timeout.md)・[待機グラフの検出](../../../transaction/transaction_manager.cpp#L310) |
| [SpillLeases](../../TinylambOpt/ResearchGoals/EngineBoundaries/SpillLeases.lean) | 論理削除、読取り中の物理ブロック、最終解放の順序。世代の折り返しや同じ世代でのチケット再利用で、遅れた解放が何を消せるかを問う。 | [spill セグメントの削除と読取り契約](../../../common/spill/spill_store.hpp#L84) |
| [MergeTraffic](../../TinylambOpt/ResearchGoals/EngineBoundaries/MergeTraffic.lean) | 外部ソートの二分マージ木を回転させる損益分岐、ページ単位の端数、ピークディスク容量、最終マージをストリームにする場合の書込み削減量。 | [外部ソートの複数パスと最終マージ](../../../executor/detail/ooc/external_sorter.hpp#L29) |
| [RadixBudget](../../TinylambOpt/ResearchGoals/EngineBoundaries/RadixBudget.lean) | 入力をバイト数で測ったときの予算超過、偏った子のフォールバック、次のビットを試す余地、同じハッシュを持つ行の順序、葉の数の上限。 | [再帰分割と 80% の偏り判定](../../../executor/detail/ooc/recursive_partitioner.hpp#L58) |
| [PlanEnvelope](../../TinylambOpt/ResearchGoals/EngineBoundaries/PlanEnvelope.lean) | 起動費・行単価・メモリ超過の費用から、探索候補を区間全体で消せるか。行数とメモリの四隅、spill の折れ点、LIMIT 1 の勝者を再利用したときの損失を扱う。 | [Cascades の LIMIT ヒントと spill 費用](../../../plan/cascades_search.cpp#L998) |
| [JoinStatistics](../../TinylambOpt/ResearchGoals/EngineBoundaries/JoinStatistics.lean) | 頻出キーの寄与を確定し、残りの度数上限から結合件数を挟む。NDV による一様分布推定、NULL 同士の一致、外部結合の欠損行補正も比較する。 | [頻出値の統計](../../../table/table_statistics.cpp#L179)・[既存の結合件数モデル](../../TinylambOpt/ResearchGoals/ExecutionStrategies/SearchBounds.lean) |
| [PeerWindows](../../TinylambOpt/ResearchGoals/EngineBoundaries/PeerWindows.lean) | RANK と DENSE_RANK のシャード合成、同順位の重複を含む CUME_DIST、NTILE を行番号式や先頭何行という条件へ変換できる境界。 | [同順位を含むウィンドウテスト](../../../executor/window_spill_test.cpp)・[NTILE の参照計算](../../../query/sql_aggregate_fuzzer.cpp#L1711) |
| [ReplyQueue](../../TinylambOpt/ResearchGoals/EngineBoundaries/ReplyQueue.lean) | 部分送信中に現在の応答を破棄してエラー応答へ切り替えるとき、直前の応答のどのバイトまで保持するか。短い書込みの合成、逆圧、すでに送信した接頭辞を扱う。 | [送信済みオフセットの更新](../../../server/postgres_server.cpp#L1312)・[キューの巻き戻し](../../../server/postgres_protocol.cpp#L199) |

## 実装判断を変える反例

- **WAL 同期への相乗り**。12 バイトを追記し、7 バイトを書き出して同期を開始する。残りの 5 バイトを書き出した後にその同期が完了しても、このモデルで保証できる耐久位置は 7。書込み位置の 12 で応答する仮説 `LateWritePiggyback` が破れる。LSN はレコード末尾の次の位置で、同期の対象は投入時に捕捉した範囲とした。[GroupCommit](../../TinylambOpt/ResearchGoals/EngineBoundaries/GroupCommit.lean)
- **Cascades の枝刈り**。同じ行数で、候補 P の費用を `max(8−grant,0)`、Q を `2+2·max(4−grant,0)` とする。メモリ割当が 0 と 8 では P が勝つが、4 では P の費用が 4、Q は 2。矩形の隅で勝ったことを使う `CornersOnly` と、各候補の working set も調べる `SpillBreakpoints` を両方登録した。[PlanEnvelope](../../TinylambOpt/ResearchGoals/EngineBoundaries/PlanEnvelope.lean)
- **再帰分割の打ち切り**。ハッシュ 0 と 1 の行が各 6 バイトあり、予算は 6 バイト。上位ビットで分けると 12 バイトすべてが同じ側に寄る。次のビットでは両側が 6 バイトに収まる。この例で、早期フォールバックした後にも有効な分割が残り得ると分かる。[RadixBudget](../../TinylambOpt/ResearchGoals/EngineBoundaries/RadixBudget.lean)
- **部分送信後の巻き戻し**。前の応答の未送信部分が `[10,11]`、今の応答が `[20,21]` で、先頭 1 バイトを送ったとする。失敗時に以前の保存長 2 をそのまま使うと、受信側には `[10,11,20]` が残る。現在のキューから守るべき長さは 1。[ReplyQueue](../../TinylambOpt/ResearchGoals/EngineBoundaries/ReplyQueue.lean)

## SQL でも照合した 2 例

空の DB に対し、既存の `build/tinylamb` で以下を実行した。

```sql
CREATE TABLE t (v INT64);
INSERT INTO t VALUES (0),(0),(1);
SELECT COUNT(*) FROM t a JOIN t b ON a.v = b.v;

CREATE TABLE ords (i INT64);
INSERT INTO ords VALUES (0),(1),(2),(3),(4),(5);
SELECT i, NTILE(4) OVER (ORDER BY i) AS bucket FROM ords ORDER BY i;
```

結合の結果は **5 行**。一様分布を仮定した見積りは `3×3/2 = 4.5` なので、これをそのまま厳密な上界として使えない。頻出キー 0 の寄与は 4、残りは 1 で、この入力なら `upper [0]` が 5 を返す。[JoinStatistics](../../TinylambOpt/ResearchGoals/EngineBoundaries/JoinStatistics.lean)

NTILE の結果は行順に **1, 1, 2, 2, 3, 4**。0 始まりの行番号 3 で、単純な比例式 `1 + floor(3×4/6)` は 3、SQL の結果は 2 になる。先頭のバケットから余りを配る条件を `BucketOccupancy` と `TilePrefix` に分けた。[PeerWindows](../../TinylambOpt/ResearchGoals/EngineBoundaries/PeerWindows.lean)

## モデルの適用範囲と確認

WAL と待機グラフは抽象的なイベント列、spill のチケットと世代は候補設計。マージは入力の合計バイト数が保存される二分木を扱い、radix は 1 段 1 ビットに絞った。プラン費用は定数係数の整数モデルで、結合上界は同一スナップショットの正確な頻度を前提にする。これらの仮説を実装で使うときは、それぞれの前提と実コードの対応を確認する。

`lean/` で `lake build` を実行すると、公開 import から提出型を読み込み、具体例の `#guard` も型検査する。各命題には後から `Goal` または `¬ Goal` の証明を独立に追加できる。
