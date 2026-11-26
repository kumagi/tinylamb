# 現行 RDBMS の正しさを問う107件

同じスナップショットなら索引と全走査は同じ行を返すか。コミットがエラーになったとき、読み手にすでに見えた行を取り消せるか。こうした問いを、反例を具体的な入力やイベント列で表せる16個のモデルに分けた。各ファイルの `def 名前.Goal : Prop` が提出対象で、証明には `名前.Goal`、反証には `¬ 名前.Goal` を使う。[一括 import](../../TinylambOpt/ResearchGoals/DatabaseCorrectness.lean)

## 2026年11月27日の再監査

**107件のうち証明104件、反証3件、未解決0件。** 前回の登録時点から、全16分野の証明・反証が作業ツリーの集約に入った。分母は元の `.Goal` の数で、同じ型の補助定理を複数ファイルが公開しても1件と数えた。相反する正負の定理は0件、対象定理が推移的に `sorryAx` に依存する例も0件だった。集約はグローバル監査が直接 import する。作業ツリーでの結果であり、今回確認した証明集約は Git の HEAD には入っていなかった。[証明・反証の集約](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/AllGoalProofs.lean)・[登録型を調べる全体監査](../../CheckAllGoals.lean)

`NullableMembership` は更新途中の監査で5件が未登録だった。最終版では6モジュールすべてを読み込んでビルドでき、6件とも証明になった。6件の定理には `native_decide` のネイティブ計算結果への依存がある。残る101件の登録結果について、依存公理は `propext`、`Classical.choice`、`Quot.sound` の範囲内だった。[浮動小数点の証明集約](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/NullableMembership_All.lean)

証明の保守上は、浮動小数点の5ファイルに同じ732行の証明が複製されている。`bitvec64_decompose` から各ファイルの最終定理 `sound` の直前までが一致し、いずれも `FloatingInSubqueryRefinesRescan.Goal` の定理を含む。このため直接 `Goal` またはその否定を返す定理宣言は全体で112個あるが、扱う命題は107個である。IEEE 等値とキー正規化の根拠を見直す際には、この5か所が同じ結論を保つ必要がある。[IN の証明](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_NullableMembership_FloatingInSubqueryRefinesRescan.lean)・[分割構築の証明](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_NullableMembership_PartitionedSubqueryTruth.lean)

## トランザクション、ストレージ、復旧

| 命題 | 証明 | 反証 | 問いと対応する実装 |
|---|---:|---:|---|
| [Publication](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/Publication.lean) | 7 | 0 | 一つ前のコミットが未公開の間、別スレッドが後のコミットを完了しても、読み手はその穴を越えないか。時刻の予約、未完了一覧からの除去、安定時刻の更新を別操作にした。[TransactionManager](../../../transaction/transaction_manager.cpp) |
| [DurableReplies](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/DurableReplies.lean) | 8 | 0 | 同期待ちをサーバー側へ移しても成功を返せる時点は同じか。非同期コミットでも、自分が読んだ別の書き手への依存が残るか。コミットレコードの先頭と末尾を区別する。[PreCommit](../../../transaction/transaction_manager.cpp) |
| [PinnedScan](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/PinnedScan.lean) | 5 | 1 | ページのラッチを離して取得し直す間に更新・削除が入ったとき、以前の読み手は元の行を返すか。版履歴の選択に既存の `VersionVacuum.readAt` を使う。[全走査](../../../table/full_scan_iterator.cpp) |
| [WalReread](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/WalReread.lean) | 6 | 0 | 書き込みの保持方法をメモリから WAL 再読に変えても、自分の最新の更新と削除を読めるか。他トランザクションとの交錯、最初のレコードのオフセットがゼロ、複数区間の連結を含む。[Transaction](../../../transaction/transaction.cpp) |
| [WinnerRecovery](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/WinnerRecovery.lean) | 7 | 0 | ログのセグメントをまたいでコミットした更新と削除だけをキーごとに再生できるか。チェックポイント前後でコミットの判定に必要な情報が欠ける場合を、分割再生の前提に明記した。[RecoveryManager](../../../recovery/recovery_manager.cpp) |
| [RecoveryBoundary](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/RecoveryBoundary.lean) | 7 | 0 | 最終レコードの短い書き込みと、後方に有効レコードがある破損を区別できるか。チェックポイント位置、フッターの有無、切り詰め失敗、次回起動時の観測を扱う。[WAL 復旧](../../../recovery/recovery_manager.cpp) |
| [VarcharKeys](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/VarcharKeys.lean) | 7 | 0 | バイナリ文字列の順序が8バイト境界や埋め込み NUL を越えて保存されるか。使用タグ、継続マーカー、末尾の長さとゼロ埋めをコード化し、次の列のバイトを消費しないことも問う。[Value の符号化](../../../type/value.cpp) |
| [SnapshotIndex](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/SnapshotIndex.lean) | 6 | 0 | 複合キーの上限につける `FF` で、その接頭辞の子孫を過不足なく拾えるか。更新された行位置も調べた索引走査と、古いスナップショットでの全走査を、行の重複度まで比較する。[範囲走査](../../../index/index_scan_iterator.cpp) |

## カタログ、SQL 実行、クライアント応答

| 命題 | 証明 | 反証 | 問いと対応する実装 |
|---|---:|---:|---|
| [CatalogDrop](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/CatalogDrop.lean) | 6 | 0 | 親テーブルの DROP と子からの外部キー追加の両方が成功し得るか。リースの重複、DROP の中断、同名の別テーブルを作った後の古いリース解放を含む。[カタログ競合テスト](../../../database/catalog_test.cpp) |
| [StatisticsCache](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/StatisticsCache.lean) | 6 | 0 | 同じスレッドとカタログ世代を使う新旧の読者に、それぞれのスナップショットの統計が返るか。DDL 中断、大小文字違いの別名、取得失敗後の再試行も扱う。[TransactionContext](../../../database/transaction_context.cpp) |
| [MergeIntegrity](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/MergeIntegrity.lean) | 8 | 0 | MERGE の重複マッチを元のソース行の識別子で検出できるか。NULL キー、WHEN の順序、自己参照先の変更、削除を準備した後の外部キー違反を扱う。[MERGE 実行](../../../query/sql_engine_statements.cpp) |
| [SortRetry](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/SortRetry.lean) | 6 | 2 | 外部ソートを途中から再開しても、同順位の行の順番と出現回数は保たれるか。行単位の失敗、復元用メモリの割り当て失敗、途中まで返したチャンクの扱いを区別する。[ExternalSorter](../../../executor/detail/ooc/external_sorter.cpp) |
| [ChunkSlices](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/ChunkSlices.lean) | 6 | 0 | UNION ALL の境界で OFFSET と LIMIT が切れたとき、定数列・平坦列の各行、行位置、照合順序が同じ範囲を指すか。空文字と NULL も区別する。[DataChunk](../../../executor/data_chunk.cpp) |
| [NullableMembership](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/NullableMembership.lean) | 6 | 0 | DOUBLE の IN / NOT IN をハッシュ化した結果が、各行の比較を SQL の三値論理で合成した結果と一致するか。NaN だけを含む右辺、空の右辺、両符号のゼロは異なる理由で重要になる。[式評価](../../../executor/detail/expression_eval.cpp) |
| [GroupingOutput](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/GroupingOutput.lean) | 6 | 0 | 空の JOIN から ROLLUP の総計が1行出るか。実データの NULL と集計が生成する NULL は HAVING の時点で区別できるか。COUNT と整数 SUM の分割集計も含む。[グループ集計テスト](../../../query/sql_grouping_sets_test.cpp) |
| [SimpleQuery](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/SimpleQuery.lean) | 7 | 0 | 1メッセージの途中で COMMIT、BEGIN、文エラーが混在したとき、どの書き込みが残るか。永続化を待っていた応答の破棄が、前のメッセージの未送信バイトまで消さないか。[サーバー](../../../server/postgres_server.cpp)・[プロトコル](../../../server/postgres_protocol.cpp) |

## 正式に反証された3件

| 命題 | 反証で使った入力と観測 | 現行実装との対応 |
|---|---|---|
| [ReusingVerdictAcrossRepin](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_PinnedScan_ReusingVerdictAcrossRepin.lean) | 時刻1に値7を直接読めたページに書き手が8を置く。古い直接読み判定を再利用すると8、版履歴を引き直すと7を返す。 | 全走査はページを取得し直すと直接読みの可否も再判定する。回帰テストは並行する書き手を使ってこの境界を通す。[全走査](../../../table/full_scan_iterator.cpp)・[回帰テスト](../../../table/full_scan_mvcc_fastpath_test.cpp) |
| [FailedUndoAllocationStillRetryable](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_FailedUndoAllocationStillRetryable.lean) | 同じソートキーの行IDが5、6の順にある。補充失敗に続いて先頭行の復元にも失敗すると、モデルの次の成功が6を返し、5を飛ばす。 | `NextRow` の復元用 `insert` は割り当てに失敗し得る。その場合は元のエラーを返す。同一カーソルを再利用する場合の性質で、SQL が欠落した結果を成功として返した観測はない。[NextRow](../../../executor/detail/ooc/external_sorter.cpp) |
| [DiscardPartialChunkThenRetry](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_DiscardPartialChunkThenRetry.lean) | 行IDが1、2、3、要求が2行。1を消費した時点で部分出力を捨てると、再試行は元の `[1,2]` に対して `[2,3]` を返す。 | `NextChunk` は入口で出力をリセットし、行ごとに追加する。パイプラインのソート出力も `NextRow` のエラーを呼び出し元に返す。同じカーソルを再開する契約と、文全体を失敗させる契約では観測が異なる。[NextChunk](../../../executor/detail/ooc/external_sorter.cpp)・[パイプラインの NextBatch](../../../executor/detail/pipeline/ooc_sort_distinct_sink.cpp) |

## 再レビューで確認した実装上の境界

### MERGE はソースの順序で成功とエラーが入れ替わる

既存 CLI を使い、ターゲットを `(k=1, v=0)` の1行、ソースを `(k=1, flag=0)` と `(k=1, flag=1)` の2行にした。次の文を昇順と降順で、それぞれ新しいデータベースで実行した。

```sql
CREATE TABLE t (k INT64, v INT64);
INSERT INTO t VALUES (1, 0);
CREATE TABLE src (k INT64, flag INT64);
INSERT INTO src VALUES (1, 0), (1, 1);

MERGE INTO t
USING (SELECT k, flag FROM src ORDER BY flag ASC) AS s
ON t.k = s.k
WHEN MATCHED AND s.flag = 1 THEN UPDATE SET v = 9;
```

| ソース順 | pipeline | legacy | 同じ DB を開き直した後の `SELECT k, v FROM t` |
|---|---|---|---|
| `ASC`、先頭が0 | 成功、更新0件 | 成功、更新0件 | 両方とも `[1, 0]` |
| `DESC`、先頭が1 | 複数マッチの `InvalidArgument` | 同じエラー | 両方とも `[1, 0]` |

現行ソースは見つけた全行をマッチ済みにするが、WHEN 条件の評価には `matches[0]` のソース行を使う。そこで UPDATE または DELETE を選んだときに `matches.size() > 1` を検査する。証明済みの `DuplicateSourceIdentityConflict` は無条件 UPDATE、`FirstMatchingMergeAction` はソースが1行という前提を持つ。複数のソースが異なる条件結果を持つ場合の順序独立性は、どちらからも導けない。ソースを並べ替える実行計画では行の内容に加え、文の成功・失敗まで比較する必要がある。[MERGE 実装](../../../query/sql_engine_statements.cpp)・[MERGE のモデル](../../TinylambOpt/ResearchGoals/DatabaseCorrectness/MergeIntegrity.lean)

### 外部ソートでは、行を戻すだけでブロックを復元できるか

`RestoredWinnerRowRetry` のモデルは、単発の補充失敗が返すカーソルを入力カーソルと同じものとして定義している。C++ では追加の分岐がある。ブロックに次行が残ると `AdvanceMergeStream` はブロックを交換しない。その直後の `RefreshStreamKey` に失敗すると、取り出した行をブロックの先頭に挿入し、`row_idx` を0に戻す。元の移動済みスロットはブロック内に残る。2行 `[5,6]` の例では、5を再試行した次の位置は6の手前にある移動済みスロットになる。復元の割り当てが成功した場合にも、表現上のカーソルと未読行の列の対応を検査する必要がある。[単発の再試行の証明](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SortRetry_RestoredWinnerRowRetry.lean)・[ブロックとキーの更新](../../../executor/detail/ooc/external_sorter.cpp)

この位置のずれは、移動後の C++ 行の値を仮定せず、スロット数だけで確認できる。既存の `FinishRetriesAfterAnIntermediateGroupWasMerged` が再試行するのは中間マージの `Finish` で、ここで問題にしている最終出力のキー再構築とは段階が違う。今回は Lean で位置の例を実行し、C++ では分岐とエラー伝播をソースで確認した。この分岐への故障注入による SQL 出力の再現は未検証である。[境界例](fixtures/database_review_cases.lean)・[既存のソート回帰テスト](../../../executor/detail/ooc/ooc_primitives_test.cpp)

### 索引・復旧・応答で結論を支える前提

- **索引の再検査には候補の完全性が要る。** 行位置4のキーが `a` から `b` に変わった後、古い読者が `a` を検索する。現在の索引候補が空なら、何度スナップショットで再検査しても4は出ない。変更位置に4を加えれば全走査と一致する。証明の結論は行位置の順列同値で、出力順や履歴を作るコードの完全性は別途確認する必要がある。[証明](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_SnapshotIndex_IndexedCandidateHeapScanAgreement.lean)・[境界例](fixtures/database_review_cases.lean)
- **チェックポイントをまたぐ書き手には過去のレコードが要る。** 区切りの前に所有者5がキー3に7を書き、区切りの後でコミットすると、前後で独立にコミット済み所有者を求めた再生では値が消え、全ログでは7が残る。実装はアクティブなトランザクションの先頭 LSN と取得時の WAL 末尾の小さい方を再生開始位置にし、WAL の永続化を待ってからチェックポイントを書き、古い区間を回収する。[分割再生の証明](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/Goal_WinnerRecovery_ClosedCheckpointCutReplay.lean)・[チェックポイント実装](../../../recovery/checkpoint_manager.cpp)・[境界例](fixtures/database_review_cases.lean)
- **コミットの可視性と成功応答は別の時点に決まる。** 版の公開後に永続化待ちが失敗すれば成功応答は返せないが、すでに観測された行は Abort で撤回できない。非同期設定でも、読み取った他トランザクションへの依存 LSN は応答の待機対象に残る。証明は与えられた依存集合の最大値まで待つことを扱うので、各読み取り経路が実際に必要な依存を登録したかは実装側の条件になる。[応答の証明](../../TinylambOpt/ResearchProofs/DatabaseCorrectness/DurableReplies_All.lean)・[PreCommit](../../../transaction/transaction_manager.cpp)

復旧の命題は、検証済みのレコード、またはフレーム・I/O 検査の結果を入力に取る。物理デバイスが本当に書き込みを保持したかは、この抽象化の外にある。複合キー順序のモデルが扱うのは非 NULL のバイナリ VARCHAR、ROLLUP の SUM はオーバーフローしない整数和である。

## SQL と Lean の照合を再現する

前回の54組では、単一の CASE の最初の分岐で IN が真になると NOT IN の結果が隠れた。今回の照合は両方を独立した列に出力する。左辺は NULL、NaN、正負のゼロ、1.5、−2、正負の無限大、正の最小正規化数の9値。右辺は空集合、重複、NULL と NaN の混在など12組。モデルの行ごとの `rescan` とその三値否定を期待値にして、各設定で108行・216個の判定すべてが一致した。CLI の表記は真が1、偽が0、不明が NULL。NaN プローブは C++ で行ごとの比較に戻す経路があり、結果一致だけで内部の全経路を通ったとは判定できない。[SQL と期待値の生成器](fixtures/database_membership_cases.lean)・[実装の分岐](../../../executor/detail/expression_eval.cpp)

リポジトリ直下から、別々の一時データベースで照合できる。

```bash
(
  set -eu
  cd lean
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  fixture=issues/research/fixtures/database_membership_cases.lean
  cli=$(realpath ../build-compile20/tinylamb)
  for part in setup queries expected; do
    lake env lean --run "$fixture" "$part" > "$tmp/$part"
  done
  cd "$tmp"
  for mode in pipeline legacy; do
    TINYLAMB_EXEC_ENGINE="$mode" "$cli" "$mode" < setup > /dev/null
    TINYLAMB_EXEC_ENGINE="$mode" "$cli" "$mode" < queries > "$mode.rows"
    diff -u expected "$mode.rows"
  done
)
```

同じ作業ツリーで Lean の集約とレビューの境界例を再検査するコマンドを示す。全体監査の分母には他の命題群も入る。

```bash
cd lean
lake build
lake build TinylambOpt.ResearchProofs.DatabaseCorrectness.AllGoalProofs
lake env lean issues/research/fixtures/database_review_cases.lean
bash scripts/verify_lean_goals.sh
```

今回も関連する既存 C++ 回帰テスト33件が通過した。SQL 照合・MERGE の観測・CTest には既存の `build-compile20` を使用した。C++ の再ビルドは行っていない。上記の実測と、現在のソースで確認した分岐を区別した。
