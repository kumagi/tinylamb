# tinylamb 設計判断記録

本書は、2026-08 の全コード精査
([docs/bug_audit_2026-08.md](bug_audit_2026-08.md)) で特定した、正しさ・耐久性・
再起動互換性に関わる設計判断を記録する。2026-09-02 に以下の方針をベースライン
として承認した。以後の実装、コードレビュー、テスト、性能評価は本書を参照する。

本書の決定は、単なる実装上の好みではなく、下位層が守る不変条件を定義する。
既存コードと本書が矛盾する場合は、本書の不変条件を優先し、実装または本書を
同じ変更単位で更新する。

## 共通の実装原則

1. 正しさを性能より優先する。性能最適化は、結果集合、NULL、順序、可視性、
   WAL の再生結果を変えないことをテストで示せる場合に限る。
2. 不明な前提で最適化を発火しない。制約、NULL可否、一意性、キー出自、残余述語、
   順序のいずれかを証明できない場合は、元の経路を維持する。
3. 再実行、クラッシュリカバリ、部分適用を通常の実行条件として扱う。物理バイトが
   変化しない再適用はエラーではなく、論理状態が正しければ成功とする。
4. 設計変更には、最小成功例、NULL、空入力、重複、境界値、エラー、spillまたは
   再起動を含む受け入れテストを付ける。
5. 実装済みの決定を変更する場合は、該当する決定IDを更新し、決定日、変更理由、
   移行手順を追記する。推奨案を過去の記録なしに書き換えない。

## 決定一覧

| ID | 状態 | 決定日 | 方針 |
|---|---|---|---|
| D1 | 実装完了 | 2026-09-02 | WAL enqueue latch をレコード完了まで保持する |
| D2 | 実装完了 | 2026-09-02 | redo/undoを論理的に冪等化する |
| D3 | 実装完了 | 2026-09-02 | DestroyPage redo で FreePage 化し、free list を再構築する |
| D4 | 実装完了 | 2026-09-02 | WAL順序と外部可視化を分離し、公開前に依存LSNを耐久化する |
| D5 | 実装完了 | 2026-09-02 | Cascades ルールを強固な前提ゲートで保護し、定期棚卸しする |
| D6 | 実装完了 | 2026-09-02 | ルート適用、重複集合、最終結果fallbackを併用する |
| D7 | 実装完了 | 2026-09-02 | Bytecode VM に短絡制御フローを実装する |
| D8 | 実装完了 | 2026-09-02 | null-safe / RIGHT / FULL を含む spill 対応 hybrid join を実装する |
| D9 | 実装完了 | 2026-09-02 | WAL レコード末尾に CRC32C を追加し、旧形式を読み取る |
| D10 | 実装完了 | 2026-09-02 | ディレクトリ走査で LSM run を復元する |
| D11 | 実装完了 | 2026-09-02 | root lift-up の前提を厳密化し、subtree を再接続してから回収する |

決定者は本タスクのプロジェクトオーナーによる指示とする。決定を実装へ落と
際は、決定IDをコミットメッセージ、テスト名、関連ドキュメントに付ける。

### 実装状況(2026-09-02 記録)

D1〜D11 の受け入れ条件はすべて対応テスト付きで実装済みであり、フルテスト
(2125 件)と `scripts/check_layering.py` の通過を確認した。テスト名の対応
関係は各決定の docs 参照先と次の文書に記録する。

- D1: `docs/lock_order.md`、`LoggerTest.D1*`
- D2/D3: `docs/recovery_invariants.md`、`recovery_manager_test`、
  `page/*_test.cpp` の `*ImplOperationsAreIdempotent*`・
  `*DestroyPage*`、`CatalogTest.DropTableCrashRecoveryReclaimsPages`、
  `BPlusTreeTest.NodeReclaimWALReplaysAfterCrash`
- D4: `docs/commit_durability.md`、`DurabilityBarrierTest.*`
- D5: `docs/cascades_optimizer.md` のゲート台帳、`CascadesTest`
  の counterexample 群、`ExecutorTest.OptimizerAndRelationalPathsAgree`
- D6: `ExpressionRewriteTest.NotNullInferenceIdempotentAndRootScoped`、
  `ShortPassCapReturnsLastFormWithoutThrowing`、
  `NonConvergingRewriteReturnsLastStableForm`
- D7: `docs/expression_evaluation.md`、`BytecodeTest.D7_*`、
  `DifferentialTest.Evaluate_LogicalShortCircuitErrors_MatchAcrossPaths`
- D8: `ExecutorTest.RelationalNullSafeInnerJoinSpillMatchesUnbudgeted`、
  `RelationalRightOuterJoinSpillMatchesUnbudgeted`、
  `RelationalFullOuterJoinSpillMatchesUnbudgeted`
- D9: `docs/wal_format.md`、`RecoveryManagerTest.MidRecordBitFlip*`、
  `MixedLegacyAndCurrentVersionLogsScanCleanly`、
  `LegacyV1DestroyRecordStillParses`
- D10: `LSMTreeTest.Reopen*`
- D11: `BPlusTreeTest.LiftUpBranchOrphansSiblingRows`(有効化済み)、
  `BPlusTreeTest.RandomChurnTenThousandOpsKeepsStructure`

## 実装順序

依存関係と障害時の影響を考慮し、次の順序で実装する。

```text
D1 → D3 → D2 → D4 → D8 → D6 → D7 → D9 → D10 → D5 → D11
```

D1〜D4はデータ破壊・起動不能・可視性破壊を防ぐ基盤である。D8は誤結果を防ぐ
join実行基盤、D5〜D7は最適化経路の意味論、D9〜D11は形式・永続化・index内部の
強化として扱う。

---

## D1. WAL 複数プロデューサのレコードインターリーブ

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: `AddLog` は enqueue latch を保持したまま、1レコード全体をリング
  バッファへ書き終えるまで待機する。
- 理由: 性能を優先し、レコード単位の直列性を追加コピーなしで保証する。
- 実装範囲: `recovery/logger.cpp` のenqueue待機、`docs/lock_order.md` のロック順序、
  logger の並行性テスト。

### 守る不変条件

- 1レコードの途中で enqueue latch を解放しない。
- バッファが満杯の場合は、flush worker が進めるまで同じ producer が待つ。
- producerが書くバイト列は、他producerのレコードによって分断されない。
- 1レコードの最大サイズはリングバッファで進行可能な上限を超えない。
- flush worker は enqueue latch を取得しない。これにより待機中の循環依存を作らない。

### 受け入れ条件

1. 16KB級のバッファ境界を跨ぐペイロードを複数スレッドから投入し、`Finish()`後に
   全レコードを逐次パースして magic、長さ、payload が一致する。
2. TSANビルドの logger 並行テストでデータ競合がない。
3. fsync有効・無効のTPC-Cベンチで、レコード単位待機による性能退行を測定する。

---

## D2. リカバリの非べき等性

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: redo と undo のページ操作を論理的に冪等化する。対象が既に目的状態なら
  物理変更なしで成功とし、再適用によるエラーを発生させない。
- 理由: リカバリは同じログを複数回適用し得るため、物理バイトの変化ではなく
  論理ページ状態を成功条件にする。
- 実装範囲: `recovery/recovery_manager.cpp`、`page/row_page.cpp`、
  `page/leaf_page.cpp`、`page/branch_page.cpp` の redo/undo操作と回帰テスト。

### 守る不変条件

- 行削除、index entry削除、行挿入、separator操作は、適用済み状態への再適用で
  row count、slot、separator、foster link を壊さない。
- 空slot・不存在keyへのundoは成功するno-opとする。
- redoが物理変更を伴わなくても、対象ページの論理状態が期待状態なら成功とする。
- page LSN の値だけを冪等性の根拠にしない。CLR、元レコード、再起動後の再生順が
  変わっても論理結果を保つ。
- 破損したページや不正な対象を「正常なno-op」として隠さない。対象の不存在が
  期待されるundoと、形式破損・構造破損を区別する。

### 受け入れ条件

1. loser INSERT/DELETEを含むDBをクラッシュさせ、`RecoverFrom`、flush、再起動、
   `RecoverFrom`を2〜3回実行して、RowCount、行内容、`SanityCheckForTest` が毎回
   一致する。
2. leaf と branch の削除・挿入CLRを同じログへ再適用しても、構造検査が成功する。
3. 再適用で物理変更が発生しないケースを成功として検証する。

---

## D3. `kSystemDestroyPage` redo

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: `kSystemDestroyPage` のredoは対象ページを `kFreePage` として初期化する。
  リカバリ完了時にページ範囲を走査し、free list を再構築して再利用可能にする。
- 理由: 壊れたページは孤立したまま保持せず、いずれ初期化されて安全に使い直される
  べきである。DROP後のクラッシュでもDBを再オープン可能にする。
- 実装範囲: `recovery/recovery_manager.cpp`、`page/page_manager.cpp`、free list、
  `DROP TABLE` とB+Tree node reclaimのリカバリテスト。

### 守る不変条件

- DestroyPage redo後のページは、有効なfree page headerを持つ。
- free list に含まれるページは、catalog、table、index、DPTから生存ページとして
  参照されていない。
- free list 再構築の走査範囲は、メタページ、WAL、checkpoint の最大page IDを含む。
- 同じDestroyPageを再適用してもfree pageの構造を壊さない。
- free list とメタページの内容が矛盾した場合は、生存ページ参照を保護した上で
  走査結果を基準に再構築する。

### 受け入れ条件

1. テーブル作成、DROP、クラッシュ、`RecoverFrom`、再オープンを行い、対象ページが
   free listへ戻り、次の割り当てで再利用できる。
2. B+Tree node reclaimを含むWALを再生してもDBが起動する。
3. DestroyPage redoを2回実行しても free list とページヘッダが一致する。

---

## D4. WAL耐久化と外部可視化の分離

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: `AddLog` 完了はWAL上の順序確定として扱い、依存するトランザクションを
  内部的にアンロックしてよい。ただし、ユーザーへ結果を返す前に、その結果が観測
  したコミットの commit LSN を `WaitForDurable` で耐久化する。
- 理由: WALのLSN順序はT2のcommitがT1のcommit recordより先に永続化されることを
  防ぐ。一方、AddLog後・fsync前のT1をT2がユーザーへ見せると、耐久性のない状態を
  外部へ公開するため、外部可視化には別の耐久化バリアが必要である。
- 実装範囲: `transaction/transaction_manager.cpp` のcommit依存追跡、
  `recovery/logger.cpp` のLSN耐久化待機、`query/sql_engine.cpp` とserverの結果公開、
  `docs/commit_durability.md`。

### プロトコル

1. T1はデータWALを `AddLog` し、commit recordを `AddLog` する。
2. commit recordの `AddLog` 完了後、T1は内部のwrite intentを解放してよい。
3. T2がT1の可視化されたversionを読む場合、T2はT1のcommit LSNを依存LSNとして
   記録する。T2の内部処理は先行してよい。
4. T2が結果をユーザーへ返す直前に、依存LSNすべての耐久化を待つ。T2がread only
   でもこの待機を省略しない。
5. T2自身がcommitする場合も、依存LSNの耐久化を満たしてから外部へcommit成功を
   通知する。T2のcommit LSNが依存LSNより後に並ぶことだけを、read-only結果の
   公開保証の代わりにしない。

### 守る不変条件

- `AddLog` 完了前のversionを別トランザクションへ可視化しない。
- `AddLog` 完了後のversionは内部処理で利用できるが、依存commit LSNがdurableに
  なる前にユーザー応答へ含めない。
- commit LSNの順序は、依存先のcommit LSNより前にならない。
- `synchronous_commit` の設定は自身のcommit待機を制御できるが、ユーザーへ返す
  結果が依存するcommitの耐久化バリアを無効化しない。
- checkpointはcommit中のtransaction status、prev LSN、依存LSNを同じ同期規約で
  読み取る。

### 受け入れ条件

1. T1のcommit `AddLog`後・fsync前にT2が読める故障注入を行い、T2の内部処理は
   進むが、T1の耐久化完了前にはユーザー応答が返らない。
2. T2がcommitする場合、再起動後もT1のcommitを前提としたT2の結果が保持される。
3. read only T2についても依存LSN待機を検証する。
4. TSANでcheckpointとcommitの並行処理にデータ競合がない。

---

## D5. Cascades ルールの意味論的安全性

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: すべてのCascadesルールに強固な前提ゲートを置く。前提を証明できない
  ルールは発火せず、意味論を満たさない既存ルールは削除または無効化する。
  ルール棚卸しを定期的に実施する。
- 理由: 最適化機会の損失より、誤結果・行欠落・述語消失を避けることを優先する。
  統計や制約が不足している場合は、安全側へ倒す。
- 実装範囲: `plan/cascades.cpp`、`plan/implementation_rules.cpp`、
  `plan/optimizer.cpp`、ルールごとの前提検証とcounterexampleテスト、監査記録。

### ルールゲートの必須検証

- LIMIT pushdown: joinの一意性、完全なマッチ保証、外部結合のNULL生成を検証する。
- dynamic filter pushdown: 元のprobe述語と残余述語を保持し、フィルタ消失を許さない。
- outer-to-anti: 残余述語、NULL許容性、join kindを検証する。
- aggregate transpose: 結合キー、group key、反対側の一意性と重複影響を検証する。
- IN-list semi join: 収集できなかったOR枝を捨てず、全枝を保持する。
- double sort elimination: キー式、方向、NULL順序、collationを比較する。
- rank/row_number to TopN: windowのpartition、order、frame、列出自を検証する。
- derived group: 固定文字列や件数ではなく、意味内容を含むfingerprintで識別する。
- rule生成: `AddExpression` のschema、arity、child group、property契約を検証し、
  不正な代替を成功した候補として登録しない。

### 棚卸し規約

- ルールの追加・変更・削除時に、前提、不変条件、counterexample、適用後のpropertyを
  同時に更新する。
- リリース前に全ルールの発火数、skip理由、結果差分を監査する。
- 監査では、直接Optimizer経路とrelational経路を同じ入力集合で比較する。
- 実装根拠を失ったルールは、根拠を再構築するまで無効化する。

### 受け入れ条件

1. COUNT(*)、外部結合+IS NULL、3表結合+WHEREについて、Optimizer経路とrelational
   経路の行集合、NULL、schema、順序が一致する。
2. 異なるtarget listやpropertyを持つ式が同一derived groupへ混在しない。
3. 各危険ルールに、発火すべきケースとゲートで発火してはいけないcounterexampleがある。

---

## D6. expression rewrite の収束保証

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: ルール適用をルートのconjunct集合に限定し、追加済み述語を名前ベースの
  重複集合で管理し、上限到達時は例外を投げず最後の安定結果を返す。
- 理由: 3表以上の正当なクエリを不動点未達で拒否せず、同じ述語の再追加による
  循環を防止する。
- 実装範囲: `expression/rewrite.cpp` のconjunct正規化、seen-set、収束上限、
  `expression/rewrite_test.cpp` とoptimizer回帰。

### 守る不変条件

- AND木の内側の部分集合だけを根拠に、全体へ新しい述語を繰り返し追加しない。
- 同じ意味の述語を順序や括弧の違いで別物として追加しない。
- 収束上限は安全網であり、最後に得た式を返す。クエリを理由なく拒否しない。
- fallback結果も入力式の意味論、NULL、エラー動作を維持する。

### 受け入れ条件

1. 同一入力を2回rewriteしてもfingerprintが変わらない。
2. 3表結合+WHEREのrewriteが例外なく完了し、Optimizer経路とrelational経路の
   結果が一致する。
3. 収束上限を意図的に短くしたテストで、最後の式が返り、例外にならない。

---

## D7. Bytecode VM の短絡評価

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: Bytecode命令セットへ `JumpIfFalse`、`JumpIfTrue`、`Jump` 相当の制御フロー
  を追加し、AND/ORを短絡評価する。NULLを含む三値論理の分岐表をASTと一致させる。
- 理由: AST、Bytecode、JITでエラー発生条件と結果を一致させ、評価不要な右辺の
  除算ゼロやvolatile処理を実行しない。
- 実装範囲: `expression/bytecode.cpp/.hpp`、bytecode serialization、batch evaluator、
  JITとの境界、differential test。

### 守る不変条件

- `FALSE AND rhs` はrhsを評価しない。
- `TRUE OR rhs` はrhsを評価しない。
- NULL AND/ORの結果はSQL三値論理表に従い、必要な場合だけrhsを評価する。
- jump targetはプログラム範囲内で、stack depthが各分岐で一致する。
- bytecode結果、AST結果、JIT結果が同じ入力で一致する。

### 受け入れ条件

1. `i != 0 AND 10 / j > 1` を j=0、j≠0、i=0、NULLの組合せで比較する。
2. `TRUE OR error_expression` と `FALSE AND error_expression` が右辺エラーを発生させない。
3. differential test、bytecode単体テスト、JIT有効・無効のSQL回帰が一致する。

---

## D8. spill 対応 hybrid join

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: hybrid hash joinをnull-safe key、RIGHT OUTER、FULL OUTERへ拡張する。
  spillした入力をresident部分だけで処理せず、すべてのpartitionを再処理する。
- 理由: 大規模入力を強制的にメモリへ戻して失敗させるのではなく、メモリ予算内で
  正しい結果を返す。現在のin-memory fallbackによる静かな行欠落を許さない。
- 実装範囲: `executor/hash_join.cpp`、`executor/detail/planning_heuristics.cpp`、
  `executor/spill_file.cpp`、join kindごとのpartition状態と回帰テスト。

### 守る不変条件

- null-safe equalityではNULLとNULLを同じjoin keyとして扱い、通常のequalityでは
  NULLをmatchさせない。
- RIGHT/FULLでは、spill partitionに存在する未一致build rowもNULL paddingで出力する。
- resident rowsとspill rowsを二重出力せず、どちらも欠落させない。
- anti / null-aware antiでは、build側NULLの存在を全partitionから集約する。
- memory budget超過時は正しくエラーまたは追加spillし、行を黙って捨てない。

### 受け入れ条件

1. 64KiB予算でnull-safe inner、null-safe LEFT、RIGHT、FULLの結果を予算なし版と比較する。
2. build/probe双方がspillするケースで、重複、NULL、未一致行を比較する。
3. anti / null-aware antiのNULL probe keyとNULL build keyを比較する。

---

## D9. WAL per-record CRC

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: WALレコード末尾にCRC32Cを追加し、形式versionを更新する。リーダーは旧形式を
  CRCなし形式として読み取り、新規書き込みはCRC付き形式に統一する。
- 理由: レコード内部のビット劣化をtail以外でも検出し、破損したレコード以降を
  安全に再生しない。既存ログの読み取り互換性は維持する。
- 実装範囲: `recovery/log_record.cpp/.hpp`、`recovery/logger.cpp`、
  `recovery/recovery_manager.cpp`、`docs/wal_format.md`、形式互換テスト。

### 守る不変条件

- CRCの対象範囲は、CRCフィールド自身を除く完全なレコードバイト列で固定する。
- magic、version、length、payload、CRCを検証してからレコードを適用する。
- 中間レコードのCRC不一致は、その位置を有効なログ末尾として扱い、後続を再生しない。
- 旧形式は明示的に識別してCRC検証を要求せず、新形式の欠損CRCは破損として扱う。
- CRC計算結果は既存の`common/crc32c.hpp`と一致する。

### 受け入れ条件

1. WAL中間の1バイトを改変し、`ValidLogEnd`が対象レコードで停止する。
2. tail truncation、長さ破損、CRC破損を区別して処理する。
3. 旧形式ログを読み取ってリカバリでき、新形式ログを再起動後も読み取れる。

---

## D10. LSM run の再起動復元

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: LSMディレクトリを起動時に走査し、命名規約からrunのgenerationを読み取り、
  数値generation順に`index_`と`files_`を復元する。MANIFESTなしの現行形式を維持する。
- 理由: flush済みデータを再起動後にも返し、既存のgenerationとBlobFileを真実の源
  として最小変更で永続性を完成させる。
- 実装範囲: `index/lsm_tree.cpp`、run/blob fileの命名・重複検証、起動時復元、
  syncと再オープンの回帰テスト。

### 守る不変条件

- 有効なrunファイルをすべて発見し、generationの重複や不正名を黙って無視しない。
- runの読み取り順序は数値generationで決まり、辞書順に依存しない。
- BlobFileとSortedRunの対応が復元後も一致する。
- 既存generationを再利用した新規run作成を許さない。
- 走査中の一時ファイル、未完了ファイル、破損runは有効runへ昇格させない。

### 受け入れ条件

1. 書込、Sync、破棄、同一パスで再構築を行い、Readとscanがflush済みデータを返す。
2. 再オープン後に追加書込とSyncを行い、3回目のオープンでも全世代が整合する。
3. generation重複、不正ファイル名、破損runを安全に拒否または隔離する。

---

## D11. B+Tree root lift-up

**決定記録**

- 状態: 承認済み
- 決定日: 2026-09-02
- 決定: root lift-upは前提条件が成立した場合だけ実行する。lift時はnext pageの
  separatorとsubtreeをroot側へ正しく付け替え、旧ページが到達不能になったことを
  確認してから回収する。
- 理由: foster chainを壊してsubtreeを孤児化する誤ったliftを防ぎ、既存の削除・挿入・
  前後走査の構造を維持する。
- 実装範囲: `index/b_plus_tree.cpp`、`page/branch_page.cpp`、root縮退・ページ回収、
  `SanityCheckForTest`を用いた回帰・fuzzテスト。

### 守る不変条件

- lift前に、root高さ、foster状態、separator範囲、child数がlift条件を満たす。
- lift後の全subtreeがrootから辿れる。separator順序とchild範囲が一致する。
- 旧root、prev page、next pageを回収する前に、参照が残っていないことを確認する。
- 空branchや負のslot indexを有効なpage IDとして扱わない。
- 前方・後方フルスキャン、挿入、削除、再均衡後の検索結果が一致する。

### 受け入れ条件

1. `DISABLED_LiftUpBranchOrphansSiblingRows`を有効化して成功させる。
2. 1葉1行相当のサイズでrandom insert/deleteを1万回行い、構造検査と前後スキャンが
   期待集合と一致する。
3. lift前後で到達可能ページ、free list、max page countが整合する。

---

## 関連文書の更新契約

各決定の実装開始時と完了時に、次の文書を該当IDとともに更新する。

- D1: [docs/lock_order.md](lock_order.md)
- D2/D3/D4: [docs/recovery_invariants.md](recovery_invariants.md)、
  [docs/commit_durability.md](commit_durability.md)
- D9: [docs/wal_format.md](wal_format.md)
- D5〜D7: [docs/cascades_optimizer.md](cascades_optimizer.md)、
  [docs/expression_evaluation.md](expression_evaluation.md)

実装済みの決定は、受け入れ条件が全件成功するまで「完了」と記録しない。性能を
理由に不変条件を緩める場合は、新しい決定IDを作成して本書の承認履歴を残す。
