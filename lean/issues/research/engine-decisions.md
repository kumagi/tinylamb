# Optimizer・MVCC・Executor の実装判断を問う23題

各行が Lean の独立した `Goal : Prop` に対応する。失敗時に同じ実装判断へ戻る補題、テスト入力、SQL 型や演算子の差し替えは件数に含めていない。「接続先」は実装を変更する際に照合する場所を示す。等価性の観測対象は命題ごとに異なり、行順、重複、エラー、可視バージョン、WAL 依存のどこまで含めたかを明記した。

## Optimizer

[Lean の定義](../../TinylambOpt/ResearchGoals/EngineDecisions/Optimizer.lean)

| 命題 | 検討する変更と適用条件 | 接続先 |
|---|---|---|
| `SharedBuild` | 他の部分木がハッシュ表などの構築費を払う可能性があるとき、どの共有状況でも捨てられる候補を判定する。共有対象に同一の ID・価格を与え、両案の物理特性を揃える。 | `plan/cascades_search.cpp` の子コスト加算 |
| `SearchFence` | 探索を中断した際、探索済みのコストと未探索枝の下界の最小値を保存する。再探索時に「この上限を厳密に改善する候補はない」と回答できる範囲を特徴づける。同額プランの優先順位は別に扱う。 | `plan/cascades_search.cpp` の枝刈り |
| `ProjectExcept` | `EXCEPT ALL` の下へ射影を移してよい必要十分条件を求める。同じ出力行になる元行の間で、左右の件数差が正と負に分かれると結果が変わる。 | `plan/set_operation_plan.cpp`、`plan/implementation_rules.cpp` |
| `JoinPrefix` | 順序を保つ一対多結合の左入力を、残余述語を通る出力数の**下界の累計**が `OFFSET + LIMIT` に達したところで止める。左の各出現の結果が連続し、後続の評価を省いてもエラーや副作用を隠さないことが条件。 | `plan/optimizer.cpp` の一対一 TopN 判定 |
| `TupleAnti` | 2列の `NOT IN` を NULL の位置別の存在検査に分解する。右辺に `(2,NULL)` があっても左辺 `(1,3)` は通る。対象は WHERE が行を残すかという観測。 | `plan/optimizer.cpp` の null-aware anti |
| `ProjectedOrder` | 先頭キーを単調なバケット関数で変換したあと、次キーの再ソートを省ける条件を求める。同じバケットへ合流する隣接した非空 run の末尾と先頭を比較する。順序・照合・NULL 順を一致させる。 | `plan/cascades_search.cpp` の射影を通る順序属性 |
| `JoinRevalidation` | 使用しない次元表との内部結合を消した prepared plan を再利用する。旧スナップショットの一対一証明があれば、新たに参照されるキーと次元側の件数が変わった現役キーだけで再検証できるか。対象は非 NULL の等値結合。 | `plan/optimizer.cpp` の join elimination |
| `NullableGroupKey` | `GROUP BY (k,d)` で、非 NULL の `k` から `d` が一意なら、その枝のハッシュキーを狭めて代表行から `d` を返す。`k IS NULL` の枝では `d` をキーに残す。元のキーと件数を含む結果を比較する。 | `executor/grouped_aggregate.cpp` のグループキー構成 |

## MVCC

[Lean の定義](../../TinylambOpt/ResearchGoals/EngineDecisions/MVCC.lean)

| 命題 | 検討する変更と適用条件 | 接続先 |
|---|---|---|
| `PublicationHoles` | 未公開トランザクションがあっても、閉じた読取キー集合の全可視版が確定していれば読み進められるか。書込集合の確定と否定検索の網羅を前提に、より新しい公開版による遮蔽も許す。可視版の時刻まで照合する。 | `transaction/transaction_manager.cpp` の `PublishCommit` |
| `UndoPeak` | 同一ページの rollback 予約量を、各所有者の逆順ログの最大累積要求量の和まで縮める。所有者どうしが任意に交互実行されても足りる最小量を問う。ページ断片化や一時コピーは実装で別途算入する。 | 同 `ReserveIndexUndoBytes` |
| `ChainSummary` | 同じ値の連続書込みをまとめる際、値の変化、未永続 WAL 依存の変化、最新書込時刻を個別に保持する。可視値・読みの依存・古い書き手の競合判定を同時に比較する。 | 同 `ReadVersion` と書込競合検査 |
| `HistoricalCover` | 現在の被覆索引の範囲結果から変更 RID を取り除き、各 RID の最古の before-image を一度だけ戻す。範囲外へ移動した行と `INCLUDE` 列の更新も扱う。安定 RID と完全な変更履歴が前提で、保証は結果の多重集合。 | `index/index_scan_iterator.cpp` と MVCC の索引キー履歴 |
| `GcWakeup` | 掃除直後の不変な版列について、次の版が消える最古スナップショットと、列全体が消える WAL 境界を監視する。書込みや pending 状態の変更は別途通知する。 | `transaction/transaction_manager.cpp` の `GarbageCollectVersions` |
| `OverlayCache` | 固定スナップショットでトランザクション専用の読取キャッシュを共有し、私有バッチの最後の編集だけをキーごとに反映する。未発見のキャッシュと削除マーカーを区別する。対象は値の読取り。 | 同 `ReadVersion` と staged writes |
| `GuardedDelete` | 最低件数制約つき DELETE の write skew を、元の件数と並行差分への所属照会で検出する。対象は集合としての行 ID、純増の挿入、削除。検証と公開を原子的に行う追加ガードを想定する。 | 同 commit の検証・公開経路 |
| `ExistsDurability` | 読取専用の単一 `EXISTS` 応答が以後の全 WAL 接頭辞で再現される最小待機位置を求める。同じ commit 内で最後の該当行を別行に交換する場合は真偽が変わらない。永続済みの初期状態、確定した述語の範囲、WAL 順に再生可能な可視 commit が前提。複数読取りやセッション全体の因果依存は追加の検証を要する。 | 同 `WaitForWalDurable`、`docs/commit_durability.md` |

## Executor

[Lean の定義](../../TinylambOpt/ResearchGoals/EngineDecisions/Executor.lean)

| 命題 | 検討する変更と適用条件 | 接続先 |
|---|---|---|
| `RunOffset` | incremental sort で OFFSET に丸ごと吸収される run のソートを省き、最後の run では `WITH TIES` の同順位まで返す。入力は完全な接頭キーごとの run、観測は出現 ID を含む順序列。 | `executor/incremental_sort.cpp` |
| `HotRunTiles` | 同一ホットキーの直積を行ペア番号で並列分割する。残余述語に通ったペアの左右の一致ビットを全 worker 分集めたあと、full outer の未一致行を出す。空側と任意のタイル完了順を含む。 | `executor/parallel_merge_join.cpp` |
| `DistinctExcludeTies` | ウィンドウの `COUNT(DISTINCT v) EXCLUDE TIES` をフレーム全体と同順位群の頻度で計算する。現在行がフレーム内にあり、その値の最後の出現が除外された場合だけ1を戻す。 | `executor/detail/window_eval.cpp` |
| `RangeFingers` | ソート済み右入力の下端・上端を独立したカーソルで進める。順序付きの左入力に対する全一致区間と、キー比較回数の上界 `2 × (左件数 + 右件数)` を同時に問う。 | `executor/zigzag_join.cpp` の `CandidateRange` |
| `FlushChoice` | メモリ不足1回について、必要バイトを空ける partition の部分集合のうち書込費用が最小のものを求める。解なしも表す。候補選択に使う再帰式を、選べる全組合せと比較する。 | `executor/detail/pipeline/ooc_hash_join_sink.cpp` の staging flush |
| `SealedBuild` | 等値セミ結合・アンチ結合の build 完了待ちを partition 単位にする。既出の一致、NULL probe、封印済み partition の不一致について、許される全追加入力で答えが確定する条件を問う。 | 同 build の完了処理と probe 側 |
| `DemandCache` | 相関フィルタを外側の要求順にキャッシュする。`LIMIT` までに返る行、最初のエラーの位置とコードを保ち、内側の評価回数が実際に要求した異なるキー数と一致するか。スナップショット固定、決定的評価、追い出しなしが前提。 | `executor/apply.cpp`、`executor/detail/subquery_runtime.hpp` |

## 境界の追試

`a(x,y) = [(7,1),(7,2)]`、`b(x,y) = [(7,2),(7,2)]` を使い、tinylamb の CLI で次の2文を実行した。前者は `[7]` を1行返し、後者は0行だった。これは無条件の射影移動への反例で、`ProjectExcept.Goal` は移動が成立する境界を問う。

```sql
SELECT x FROM (SELECT x,y FROM a EXCEPT ALL SELECT x,y FROM b) AS delta;
SELECT x FROM a EXCEPT ALL SELECT x FROM b;
```

[有限検査](fixtures/engine_decision_checks.lean)は複合 NOT IN の819入力、2所有者の undo 列169組（各組の全順序保存インターリーブ）、整列済み結合の左右入力1,225組を全列挙する。各命題の一般形は未証明。C++ の所要時間やメモリ使用量は未計測。

```sh
cd lean
lake build TinylambOpt.ResearchGoals.EngineDecisions
lake env lean issues/research/fixtures/engine_decision_checks.lean
```
