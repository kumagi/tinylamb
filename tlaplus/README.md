# tlaplus/

TLA+ による設計モデル。今は 1 本だけ。

| モデル | 対象 |
|---|---|
| `TxSpill.tla` | 未コミットページの専用スピル（[ADR 0007](../docs/adr/0007-uncommitted-pages-private-spill.md)）と、コミット済みだけを再生するリカバリ（[recovery invariants](../docs/recovery_invariants.md)） |

## 実行

```sh
# tla2tools.jar を tools/ に置く（https://github.com/tlaplus/tlaplus/releases）か、
# TLA2TOOLS_JAR=/path/to/tla2tools.jar を指定する
./run.sh              # 全設定を実行し、期待どおりの結果か判定する
./run.sh fixed        # 1 つだけ
```

結果は `out/<設定名>.log` に出る。反例の手順もここに出る。

## TxSpill.tla が表すもの

| モデルの要素 | 実装 |
|---|---|
| `Write` | 行の更新。`AddLog` のあと `live_packed_` を変える |
| `LogCommit` | `PreCommit` の kCommit 追記と `kCommitted` への切り替え（`chain_mu_` 内） |
| `ApplyCommit` | `ApplyTransactionCommitRecord`。1 レコードずつ、コミットの耐久化を待たずに反映する。退避済みページは先に `FaultIn` |
| `FinishPublish` / `Forget` / `Ack` | `CommitVersions`（write intent の解放）/ `ForgetTransaction` / `WaitForDurable` 後の応答 |
| `StartAbort` / `UndoStep` / `FinishAbort` | `Abort`。新しいレコードから順に live を戻し、待たずに kAbort を追記する |
| `Flush` | グループコミットの fdatasync。`durable` が 1 レコードずつ進む |
| `FaultIn` | `EnsureEvictedPageResident`。blocks の committed 像に 'TUPO' オーバーレイを重ね、セグメントを捨てる |
| `EvictClean` | `EvictBatch` の clean_match かつ非 dirty の経路 |
| `EvictCapture` → `EvictWrite` → `EvictPublish` | `EvictBatch` の 3 段（ラッチ下で像を作る → ラッチなしで WAL 待ち・pwrite・fdatasync・SpillStore 書き込み → ラッチを取り直して再確認し公開または断念） |
| `CkptBegin` / `CkptPage` / `CkptFinish` | `CheckpointManager` の anchor 決定 / `WriteFuzzyCheckpoint` のページ単位の走査 / `<db>.db` の rename。`CkptFinish(TRUE)` は blocks の世代交代（参照中の像を新世代へ複写して記述子を更新（モデル上の旧像は残存））を含む |
| `Crash` | RAM・SpillStore・未同期の WAL 末尾・実行中の退避とチェックポイント・Tx 表を失う。スナップショットを読み、anchor 以降のコミット済み Tx だけをスロット LSN で冪等に再生する |

ゴースト変数 `gCommitted` / `gLive` は「本来見えるべき DB」を持つ。`Crash` は `gCommitted` を
「kCommit が耐久化した Tx の更新を LSN 1 から全部再生した状態」に置き直す。そのため
実装側の再起動結果と食い違えば `CommittedStateCorrect` が落ちる。

### 不変条件

| 名前 | 意味 |
|---|---|
| `CommittedStateCorrect` | どこにあるページ（RAM・blocks）でも、コミット済みの値がゴーストと一致する。再起動後も一致する |
| `LiveStateCorrect` | 未コミットの値も一致する（オーバーレイ経由の退避と読み戻し、Abort を含む） |
| `EvictedPageReadable` | 退避済みページの記述子が、消されていない blocks 世代を指す |
| `CheckpointReadable` | スナップショットが消されていない blocks 世代だけを参照する |
| `WalBeforeData` | blocks に書いた各スロットについて、更新レコードと、その更新を公開した Tx の kCommit の両方が耐久化済みである |
| `AckedIsDurable` | 応答したコミットは耐久化している |
| `SpillOnlyForEvicted` | オーバーレイは退避中のページにしか無い |

### 抽象化して省いたもの

- ページの確保と破棄（`kSystemAllocPage` / `kSystemDestroyPage`）。ページは最初から全部確保済みで、`kUncommittedOnlyDiskOffset` の経路は無い
- 列形式スタブ、LSM、索引、カタログ。ページ間リンク（prev/next）
- 1 バッチ複数ページの `EvictBatch`。1 ページずつ退避する（`eviction_mu_` で退避役は 1 つ）
- WAL 読み戻しモード（ADR 0008）。staged 写しと WAL の再読は結果が同じなので区別しない
- WAL の回収。再生は anchor から読むだけで、それより前は使わない
- 非同期コミット。応答は常に `WaitForDurable` の後
- 値は「書いた Tx の ID」だけ。1 Tx は 1 キーを 1 回まで書く

## 設定と期待する結果

| 設定 | 内容 | 期待 |
|---|---|---|
| `fixed` | コミット耐久化を待つ版（最大 2 回の crash/restart を探索） | 全て成立 |
| `fixed_2pages` | 同上を 2 ページで（重い） | 全て成立 |
| `current` | 現行実装のコミット耐久化待ち。1 ページ・最大 1 回の crash | 全て成立 |
| `bug_record` | 修正前の更新レコードだけを待つ版 | `WalBeforeData` 違反 |
| `current_harm` | 現行実装の耐久化待ちで 2 ページの再起動結果を検査 | 全て成立 |
| `bug_record_harm` | 修正前の版、2 ページ。`WalBeforeData` を外し、再起動後の害だけを見る | `CommittedStateCorrect` 違反 |
| `bug_stale` | `EvictBatch` が公開前に `live_seq_` / `committed_seq_` を見ない | 違反 |
| `bug_gen` | 公開前に blocks 世代の生存を見ない（page_manager.cpp の "zombie" コメントの件） | `EvictedPageReadable` 違反 |
| `bug_forget` | 反映が終わる前に `active_transactions_` から外す | `CommittedStateCorrect` 違反 |
| `bug_nowait` | blocks へ書く前に WAL を待たない | 違反 |

`bug_*` は、モデルが既知のバグを実際に捕まえることの確認用。

TLC 1.8.0 で `current` の 42,659,198 状態と `current_harm` の 114,868 状態を全探索し、
各設定の不変条件が成立した。`bug_record` と `bug_record_harm` では表に示した違反をそれぞれ再現した。

## 修正前の反例

修正前に blocks への書き込みが待っていたのは「更新レコードの LSN」までで、そのレコードを含む Tx の
kCommit の耐久化は待っていなかった。

- `ApplyTransactionCommitRecord` はコミットの耐久化前に走り、スロット LSN には更新レコードの
  `record_lsn`（先頭 LSN）を入れる（`RowPage::ApplyCommittedSlot`）
- 修正前の `EvictBatch` は `WaitForDurableLSN(max_batch_lsn)`、`WriteFuzzyCheckpoint` は
  `WaitForDurableLSN(max_dirty_lsn)` を待つ。どちらもその最大スロット LSN で、kCommit の LSN ではない

想定する反例（`bug_record_harm`）:

1. T がページ A、B を更新する（LSN 1, 2）
2. チェックポイント開始。T は登録中なので anchor = 1、snap = 3
3. T の kCommit を追記する（LSN 3、未同期）。A へだけ反映する
4. WAL が LSN 2 まで同期する。チェックポイントが A を書く（待つのは LSN 1 まで）。B はまだ反映前の像を参照する
5. `<db>.db` を rename し、クラッシュ。LSN 3 は失われる
6. 再起動: A のスナップショット像には T の値がある。T は kCommit が無いので再生されず、
   undo も無いので消されない。B には T の値が無い → kCommit の無い Tx が半分だけ見える

チェックポイントの `WaitForDurable(snap_lsn)` は snap より前の kCommit しか守らないので、
snap より後に追記されたコミットがこの穴を通る。

## 現行実装との対応

`PageManager::NoteCommitReflected` は、反映するコミットの終端 LSN を
`PageManager` 全体の最大値 `max_reflected_commit_end_` に記録する。
`EvictBatch` と `WriteFuzzyCheckpoint` は blocks 書き込みやスナップショットの公開時に
この値を耐久化の待機対象に含める。モデルの `commit` モードは、書き出す像に寄与した
各 Tx の kCommit が耐久化済みであることを要求する。実装は別ページに反映した
コミットも待つ場合がある。モデルは WAL をレコード単位で扱う。
実装のバイト単位の待機点は[コミット耐久化の仕様](../docs/commit_durability.md)に記載している。

## 回帰テスト

`CheckpointManagerTest.SnapshotDoesNotPersistCommitWhoseCommitRecordIsNotDurable`
は WAL の同期を止めたままチェックポイントを開始し、その時点のファイルを複写して
再起動する。kCommit が WAL に届いていない更新について、復旧後の行が更新前の値
`old` を保つことを検査する。
