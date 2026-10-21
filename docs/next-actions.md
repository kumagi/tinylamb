# 次に対処すべき指摘事項(設計バックログ)

2026-10-20 加筆(第50報)。DML logical / physical plan(Cascades 接続の残り)は
**ペンディング**と判断し、現状の接続範囲・必要な分割・着手点を記録した
(下記「2026-10-20 DML を Cascades の memo に載せる作業の分割」)。

2026-10-20 加筆(第49報)。同節の続き: **SkipLocked / NOWAIT**。GoogleSQL に
文法が無いため NOWAIT はエンジン API(`ForUpdateWaitMode::kNoWait`)として実装し、
プランキャッシュ key に方針を混ぜた。SKIP LOCKED は「ロックしてから WHERE」の
契約と両立しない(述語一致行まで結果から消える)ため見送り、理由と将来の
出発点を記録した。詳細は下記「2026-10-20 FOR UPDATE の待機方針は
エンジン API / SKIP LOCKED は見送り」の節。

2026-10-20 加筆(第48報)。統合TODO(`docs/todo.md` Phase 2 DML / Lock)の次項目:
**LockRows / SELECT FOR UPDATE**。`LockMode` を取り込み、DML ソース契約
(`require_row_position` → スキャン `lock_rows`)を再利用して行ロックを実装。
詳細は下記「2026-10-20 SELECT ... FOR UPDATE は DML ソース契約で行ロック」の節。

2026-10-20 加筆(第47報)。統合TODO(`docs/todo.md` Phase 2 DML / Lock)の次項目:
**UPDATE FROM / DELETE USING**。`UPDATE ... FROM` は MERGE 脱糖として実装済みで、
`DELETE ... USING` は GoogleSQL に構文が無いため等価形（相関 EXISTS / IN）を
契約にし、USING 句を黙って落とさないガードを追加した。詳細は下記
「2026-10-20 UPDATE FROM は MERGE 脱糖 / DELETE USING は等価形が契約」の節。

2026-10-20 加筆(第46報)。統合TODO(`docs/todo.md` Phase 2 CTE / Materialize /
Recursive)の次項目: **CTE materialization / inlining / filter pushdown のコスト
選択**。詳細は下記「2026-10-20 共有CTEへの制約押し下げ (選言だけを押す)」の節。
CTE 節の `[ ]` はこれで 0 件。

2026-10-20 加筆(第45報)。統合TODO(`docs/todo.md` Phase 2 CTE / Materialize /
Recursive)の次項目: **WorkTableIndex**。詳細は下記「2026-10-20 WorkTableIndex
(再帰 UNION DISTINCT の作業テーブル索引)」の節。CTE 節の `[ ]` は残り 1 件
(CTE materialization / inlining / filter pushdown のコスト選択)。

2026-10-20 加筆(第44報)。統合TODO(`docs/todo.md` Phase 2 CTE / Materialize /
Recursive)の次項目: **plan template / cache 再構築時の recursive metadata 保持**。
詳細は下記「2026-10-20 テンプレートキャッシュと再帰メタデータ」の節。

2026-10-20 加筆(第43報)。統合TODO(`docs/todo.md` Phase 2 CTE / Materialize /
Recursive)の次項目: **CTE依存順 / 前方参照 / 相互再帰エラー**。詳細は下記
「2026-10-20 CTE依存順と相互再帰」の節。CTE 節の `[ ]` は残り 3 件。

2026-10-20 加筆(第42報)。統合TODO(`docs/todo.md` Phase 2 CTE / Materialize /
Recursive)の次項目: **recursive JOIN USING / BY NAME / CORRESPONDING の
schema 契約**。詳細は下記「2026-10-20 再帰CTEの列一致契約」の節。CTE 節の
`[ ]` は残り 4 件。

2026-10-20 加筆(第41報)。統合TODO(`docs/todo.md` Phase 2 Window / UNNEST /
生成)の次項目: **`FunctionScan` / TVF**。詳細は下記「2026-10-20 組み込み TVF
(FROM GENERATE_SERIES)」の節。Window / UNNEST / 生成 の `[ ]` は無くなった。

2026-10-19 加筆(第36報)。統合TODO(`docs/todo.md` Phase 2 Scan / Access
Path)の次項目: **内側filterを index へ適用できない場合に heap 側へ残す契約**。
詳細は下記「2026-10-19 LookupJoin の内側 filter 契約」の節。

2026-10-19 加筆(第37報)。統合TODO(`docs/todo.md` Phase 2 Scan / Access
Path)の次項目: **unique内側の1行lookup短絡**。詳細は下記
「2026-10-19 LookupJoin の unique 内側短絡」の節。

2026-10-19 加筆(第38報)。統合TODO(`docs/todo.md` Phase 2 Scan / Access
Path)の次項目: **ソート済み外側からの lookup locality の cost 化**。詳細は
下記「2026-10-19 lookup locality の cost 化」の節。Scan / Access Path の
`[ ]` は `ForeignScan`(保留)のみになった。

2026-10-19 加筆(第39報)。統合TODO(`docs/todo.md` Phase 2 Window / UNNEST /
生成)の次項目: **Window の hash partition + sort**。詳細は下記
「2026-10-19 Window の hash 区画化」の節。

2026-10-19 加筆(第40報)。統合TODO(`docs/todo.md` Phase 2 Window / UNNEST /
生成)の次項目: **Window の spill**。詳細は下記「2026-10-19 Window の spill 形」
の節。Window / UNNEST / 生成 の残りは `FunctionScan` / TVF のみになった。

2026-10-19 加筆(第35報)。統合TODO(`docs/todo.md` Phase 2 Scan / Access
Path)の次項目: **複数インデックスを用いた covering 判定**。詳細は下記
「2026-10-19 複数インデックス covering scan」の節。ForeignScan は外部
ソース形式が未定のため保留のまま。

## 2026-10-20 DML を Cascades の memo に載せる作業の分割

- **[現状] 「Cascades へ接続」の半分は済んでいる。** UPDATE / DELETE は
  対象テーブルを一時 `QueryData`（`from_`, `where_`, 出力列,
  `require_row_position_=true`）に落として `Optimizer::Optimize` に渡し、
  **ソース側のアクセスパス（index range / full scan / filter 順 / row position）は
  Cascades が選ぶ**。その `Plan` の executor を `UpdateExecutor` /
  `DeleteExecutor` が引いて変更を適用する。INSERT ... SELECT はソースを
  materialize し、INSERT VALUES は plan を持たない。
- **[未了] DML ノード自体が memo に無い。** 追加するなら:
  1. `LogicalOperator::kInsert/kUpdate/kDelete` + 物理 `DmlPlan`
     （`plan/` に宣言、`executor/relational_factory.cpp` で executor を組む
     既存パターンに合わせる）。
  2. `PlanRequirements`/`PhysicalProperties` に「子は row position を出す」
     「write-intent 方針（wait / try-lock）」を明示し、`DmlPlan` が子に要求する。
     現状は `QueryData::require_row_position_` / `wait_for_write_intent_` が
     暗黙にその役をしている。
  3. 適用ロジック（SET 評価、制約、トリガ、RETURNING、
     ASSERT_ROWS_MODIFIED、値テーブル別名、nested array / struct field 更新）を
     ノードのデータへ移す。sql_engine の kUpdate/kDelete/kInsert 分岐は
     文あたり 300〜400 行あり、ここが最大のリスク。
  4. コスト比較（index 点更新 vs スキャン + 適用）と、
     `EXPLAIN UPDATE/DELETE`（現状 EXPLAIN は SELECT/WITH のみ）の解禁。
- **[判断] 見送り。** SQL から見た振る舞いは現状で変わらず、得られるのは
  探索の一般性と EXPLAIN の見通し。300〜400 行 × 3 文の移設は
  回帰リスクに対して割に合わないため、`docs/todo.md` では `[ ]` のまま
  理由を書いてペンディングにした。着手するなら 4 → 2 → 1 → 3 の順
  （先に EXPLAIN と property を整えると、移設の正しさをテストで観測できる）。

## 2026-10-20 FOR UPDATE の待機方針はエンジン API / SKIP LOCKED は見送り

- **[文法] `NOWAIT` / `SKIP LOCKED` は GoogleSQL に無い。** パーサは
  `SELECT * FROM t FOR UPDATE NOWAIT` を構文エラーにする（`FOR UPDATE` だけが
  `LockMode` ノードになる）。フロントエンドは外部パーサなので、構文を足す
  手段は無い。
- **[実装] NOWAIT = 非ブロッキング予約。** `SqlEngine::SetForUpdateWaitMode(
  ForUpdateWaitMode::kNoWait)` が `QueryData::wait_for_write_intent_=false` に
  落ち、スキャンは `TryAddWriteSet` で予約する。これは DELETE のソース
  スキャンが既に使っている方針（5 秒待ちでインデックス読み取りガードを
  占有しないため）で、競合した瞬間に `kConflicts` で文が落ちる。
  `kWait`（既定）は従来どおりブロッキング + デッドロック被害者の wounding。
- **[必須] 方針はプランキャッシュ key の一部。** 待機方針は
  `PhysicalProperties::wait_for_write_intent` としてプランに焼き込まれる。
  `kNoWait` のときだけ `FOR UPDATE` を含む SQL の fingerprint に接尾辞を足し、
  `kWait` で埋めたプランを `kNoWait` のエンジンが再生しないようにする
  （接尾辞を消す変異は 5 秒待ちになり `SqlEngineForUpdateNoWaitFailsFast` が
  落ちる）。テンプレートキャッシュ側は AST だけを持つので影響しない
  （方針は plan 時に読む）。
- **[見送り] SKIP LOCKED。** 本エンジンのロックは DML ソース契約で
  「ロックしてから WHERE」なので、ロック済み行を飛ばすと**述語に一致する行まで
  結果から消える**（例: 3 行中 1 行が他トランザクションにロックされていると
  2 行だけ返る）。PostgreSQL の SKIP LOCKED は「述語に一致した行のうち
  ロックできた行」を返すので、意味を保つにはスキャン側に
  「述語評価後にロックし、取れなかった行だけ飛ばす」第三のモードが要る。
  SQL 面が無いまま plan property を増やすのは割に合わないため見送り。
  将来 `FOR UPDATE` に文法が入ったら `table/scan_options.hpp` の
  `lock_rows`/`wait_for_write_intent` を三値モードにし、
  `PhysicalProperties::Key()` に混ぜるのが出発点。

## 2026-10-20 SELECT ... FOR UPDATE は DML ソース契約で行ロック

- **[実装] `FOR UPDATE` は新しいロック層を足さず、DML ソース契約を再利用した。**
  パーサは `LockMode` ノードを出す（GoogleSQL には `FOR UPDATE` しか無く、
  `FOR SHARE`/`NOWAIT`/`SKIP LOCKED` は構文エラー）。visitor が
  `SelectStatement::ForUpdate()` を立て、`PrepareStatement` が
  `PlanRequirements.require_row_position=true` を要求 → スキャンが
  `lock_rows` になり、行を読む前に `Transaction::AddWriteSet` で write-intent を
  取る。意図はトランザクション終了まで保持され、他トランザクションの
  `TryAddWriteSet` は失敗する。
- **[契約] 述語はロックの後。** UPDATE/DELETE のソースと同じ規則なので、
  索引範囲に落ちない述語ではスキャンが訪れた行（フルスキャンなら全行）を
  ロックする。PostgreSQL の「一致行だけ」より広いが、取りこぼしは無い。
  索引範囲が取れる述語ならその範囲に限定される。`WHERE` が索引範囲に
  落ちるかは実行計画依存なので、「条件に一致した行だけがロックされる」とは
  書かない。
- **[契約] head 版をロックして head 版を返す。** ロックは行解決の前に行うので、
  「スナップショットの古い版にロックを付けて新しい版を返す」ことはない。
- **[ゲート] 行位置を持たない形は明示エラー。** JOIN/集計/DISTINCT/GROUP BY/
  HAVING/window/集合演算/CTE/派生表/TABLE SAMPLE は
  `SELECT ... FOR UPDATE requires a single base table ...` で拒否。
  relational 経路（row position を運ばない）に落ちる形も拒否する。
  黙ってロック無しで返す経路を作らないのが要点。
- **[キャッシュ] テンプレート再束縛でフラグを落とさない。**
  `BindSelect` が `SetForUpdate()` を複写しないと、テンプレートキャッシュヒットで
  ロック無しの読みに化ける。`SqlTemplateTest.BindSelectPreservesForUpdate` が固定。
  コンパイル済みプランキャッシュは Plan 自体（`lock_rows` 込み）を再生するので
  ロックは保たれ、同じ SQL テキストしか同じ fingerprint にならない。
- 回帰: `QueryTest.SqlEngineForUpdateLocksScannedRows`（ロック取得・
  非ロック読みは write set を汚さない・abort で解放・同一文の再実行でも
  ロック）, `QueryTest.SqlEngineForUpdateRejectsUnlockableShapes`,
  `SqlTemplateTest.BindSelectPreservesForUpdate`。
- 変異: visitor のフラグ設定削除 / `require_row_position_` 代入削除 /
  形ゲート無効化 / 再束縛のフラグ複写削除 → すべて kill 確認。
- **[残課題] `SkipLocked` / `NOWAIT` は文法が無い。** ZetaSQL のパーサが
  `USING`/`NOWAIT` を拒否するため SQL からは到達できない（次項目の判断材料）。

## 2026-10-20 UPDATE FROM は MERGE 脱糖 / DELETE USING は等価形が契約

- **[判明] `UPDATE ... FROM` は既に実装済みだった。** `VisitUpdate` が
  `UPDATE t SET ... FROM src WHERE <join>` を
  `MERGE INTO t USING src ON <WHERE> WHEN MATCHED THEN UPDATE SET ...` に脱糖する
  （`MergeStatement::DesugaredFromUpdate()`）。診断・`StatementType`・
  `EXPLAIN` は UPDATE のまま（`Update Rows`）。BigQuery と同じく、1 対象行に
  複数ソース行が一致したら拒否する（"MERGE attempted to UPDATE or DELETE the
  same target row more than once"）。`MergeTest.UpdateFrom*` が
  副問合せ源・テーブル源 + 残余 WHERE・無 WHERE・重複一致拒否・コマンド報告を
  固定している。
- **[不可能] `DELETE ... USING` は GoogleSQL に存在しない。** フロントエンドは
  外部パーサ（`execute_query --mode=parse`）なので、`DELETE FROM t USING u` は
  構文エラーになる。よって等価な綴りを契約にする:
  `DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.k)` と
  `DELETE FROM t WHERE k IN (SELECT k FROM u)`。どちらも対象行が複数ソース行に
  一致しても 1 回だけ削除する（MERGE 由来の UPDATE FROM とは規則が違う。
  UPDATE は「1 対象行 1 ソース行」を要求し、DELETE は集合一致で 1 回）。
- **[修正] USING 句を黙って捨てていた。** `VisitDelete` は WHERE 候補から
  `FromClause` を skip していた。パーサが拒否するので SQL 経由では起きないが、
  将来の文法や手組み AST で句が来ると「JOIN の無い WHERE」で誤削除になる。
  句を見つけたら「USING は GoogleSQL に無い + 相関サブクエリで書く」と
  明示エラーにする（`GoogleSqlAstTest.DeleteRejectsUsingClauseInsteadOfDroppingIt`
  が、FromClause を持つ dump でエラーになることを固定）。変異（skip に戻す）は
  このテストで kill 確認。
- 回帰: `MergeTest.UpdateFrom*`（5 件）,
  `QueryTest.SqlEngineDeleteJoinsThroughCorrelatedSubqueries`（EXISTS 綴り・
  IN 綴り・重複一致でも 1 回削除）,
  `GoogleSqlAstTest.DeleteRejectsUsingClauseInsteadOfDroppingIt`。
- **[残課題] DELETE 側の `FROM` 拡張は無し。** 将来 ZetaSQL が `USING` を足したら、
  MERGE 脱糖（`WHEN MATCHED THEN DELETE`）へ寄せるのが自然 — そのとき
  このガードを外して脱糖に差し替える。

## 2026-10-20 共有CTEへの制約押し下げ (選言だけを押す)

- **[課題] 共有 CTE のセルには誰のフィルタも効いていなかった。** 単一参照の
  非再帰 CTE は M4 でインライン化済み、複数参照は 1 回 materialize（Cascades では
  共有セル 1 個の `kValues` 葉）で、正しさは正しい。しかし 2 参照の CTE では
  「利用側 A は `k = 1`、利用側 B は `k = 2` しか読まない」のにセルには全行が
  載り、各参照が同じ全行を filter していた。
- **[実装] 押せるのは「利用側制約の選言」だけ。** 単一利用側の述語をそのまま
  押すのは誤り（`x.k = 1` を押すと `y.k = 2` の行がセルから消え、B が空になる）。
  全利用側の制約の選言 `P1 ∨ P2 ∨ …` は「どの利用側も出し得る行」を過不足なく
  残すので、セル構築前に絞っても意味論は変わらない。`PushSharedCteRestrictions`
  が `InlineSingleUseCtes` の直後・`MaterializeCtes` の直前で本体の WHERE に
  合成する。
- **[安全性ゲート] 選言が意味を持つのは、全参照箇所の制約が読めるときだけ。**
  (1) 参照は「この文または兄弟 CTE 本文の FROM 直下の平テーブル源」に限る
  （派生表・副問合せ・集合演算分岐の中の参照は、WHERE を別スコープから読む
  一般化が必要なので対象外 = 見つけたら押さない）。(2) 結合種別は CROSS/INNER
  のみ。NULL 拡張側（LEFT JOIN の右辺など）では、行を消すと padding 行が
  *増える* ため `k IS NULL` のような制約で結果が変わる。(3) 各参照箇所に
  「その別名だけを見る」制約が最低 1 つ要る（無制約の箇所が 1 つでもあると
  選言が TRUE）。(4) 本体は 1 平テーブルからの無改名の列投影で、GROUP BY /
  DISTINCT / LIMIT / OFFSET / QUALIFY / window / 集合演算 / 副問合せ / USING /
  ON を持たない（別名を剥がした述語が本体の列名に戻る必要がある。LIMIT は
  CTE の内容そのものを固定するので、フィルタを上へ動かせない）。
- **[コストゲート] 統計が無ければ押さない。** `TableStatistics::EstimateSelectivity`
  が選言について 1.0 未満を返すときだけ押す（`k >= 0` のような全行制約は
  1.0 で棄却）。`Rows() == 0`（ANALYZE 前）は「削減量が分からない」なので
  押さない — 構文があるだけで押すと、セルを余計に filter するだけの
  コストを払うことになる。
- **[観測] 押したことと効いたことを別々に見る。** `LastCteRestrictionPushdowns()`
  が判定結果を返し、効いたことは `EXPLAIN ANALYZE` の葉の統計で見る
  （64 行テーブル・2 参照・各 `k = 1` のとき、押すと葉の
  `rows_evaluated=1`、押さないと `=64`）。
- 変異検証: (1) 呼び出しを削る → カウンタ 0 で e2e 失敗、(2) 選言を連言に
  する → 「A は k=1、B は k=2」の形が 0 行になり失敗、(3) 推定ゲートを削る →
  全行制約まで押してしまう（カウンタと `rows_evaluated` の両方で失敗）、
  の 3 種を kill 確認。(4) 呼び出しを `MaterializeCtes` の後ろへ移す変異は
  kill できなかった = セル構築は遅延で、順序は意味に影響しない。順序を
  前にしているのは意図（セル構築前に絞る）を読めるようにするため。
- **[残課題] 参照箇所の一般化。** 副問合せ・派生表・集合演算分岐の中の参照は
  まだ読まない（読むには、そのスコープの WHERE から外側別名だけを見る合流を
  取り出す一般化と、外側結合の padding 安全性の再判定が要る）。また
  複数参照 CTE を参照ごとに別セルへ複製する（per-reference materialization）は
  未着手 — 選言が広すぎる（`P1 ∨ P2` がどちらも弱い）場合の次の一手。

## 2026-10-20 WorkTableIndex (再帰 UNION DISTINCT の作業テーブル索引)

- **[課題] UNION DISTINCT 再帰の重複判定が二次だった。** `ExecuteRecursiveCte`
  は「前のラウンドまでに同じ行が出たか」を `std::vector<Row> seen` の線形走査で
  判定していた。再帰の各ラウンドは数千行を足せるので `seen` は結果全体まで育ち、
  320001 行を生成する再帰（`v*16+k` を `GENERATE_SERIES` で展開）は **3分17秒**
  かかっていた。反復上限 1024 は行数の上限ではないため、行予算 1000 万行まで
  この二次コストは現実に効く。
- **[実装] `WorkTableIndex`**（`executor/detail/work_table_index.{hpp,cpp}`）。
  `Insert(row) -> bool`（新規なら true）と `Contains`。ピア規則は DISTINCT と
  同一で、`EqualUnderCollation` + `CanonicalDistinctValue` の組を使う
  （NaN は 1 ピア、-0.0 == +0.0、大文字小文字非依存の照合は畳み込み、NULL は
  NULL 同値、幅と型タグは区別）。`DistinctOf` もしていた私家版の
  `CanonicalRowHash`/`CanonicalRowEqual` を捨ててこの実装を共有する（規則が
  1 箇所になる）。同じ 320001 行の再帰が **0.27 秒**。
- **[契約] これは結合アクセス経路ではない。** CTE 名は前ラウンドのデルタに
  束縛される（BigQuery の working-table 契約 = 再帰項は累積結果を読めない）。
  よって各ラウンドの結合は join 演算子がそのデルタをハッシュ化しており、
  ラウンドを跨ぐ永続索引は意味を持たない（累積テーブルを引くと意味論が変わる）。
  ループ自身が索引を必要とするのは「どの行が新規か」の判定であり、
  `WorkTableIndex` はそこだけを担う。
- **[観測] ループが索引を引いたことをテストで見る。** `WorkTableIndexStats()` に
  `inserts` / `duplicates` を置き、テストは差分で読む。`EXPLAIN` に出さないのは、
  再帰CTEが Cascades の `RecursiveCteExecutor` 経由になり RelationalExecutor の
  統計行が出ない経路があるため（`shared_exists_build_reuses` と違い、
  ルート非依存に観測できる必要がある）。
- 変異検証: (1) ループを線形走査に戻す → カウンタが動かず e2e 失敗（実行時間も
  195 秒に戻る）、(2) ハッシュから `CanonicalDistinctValue` を外す →
  大文字小文字非依存ペアの unit テスト失敗、(3) 等値判定を `Value::operator==`
  に落とす → 同上（照合の畳み込みが消える）。
- 回帰: `WorkTableIndexTest.*`（8 件）,
  `QueryTest.SqlEngineRecursiveCteDedupesThroughWorkTableIndex`（3 ラウンドの
  カウンタ、NaN / -0.0 / NULL のピア、320001 行のスケールガード）。
- **[残課題] 結合側の索引アクセスは無い。** デルタ束縛のため、再帰項の結合は
  毎ラウンド join 演算子のハッシュ表を作り直す。これを削るには「同一ラウンド内で
  複数の CTE 参照が同じデルタを共有する」形（`t a JOIN t b`）でハッシュ表を
  1 回にまとめる配管が要る（意味論は変わらない）。必要になったら
  `CteMap` の束縛に索引を持たせる形で、`Relation` の共有ビューと一緒に設計する。

## 2026-10-20 テンプレートキャッシュと再帰メタデータ (再構築が実際に走るまで)

- **[課題] 再帰CTEはテンプレートキャッシュに一度も載っていなかった。**
  `Prepare` は fingerprint 一致で `FindTemplate` を引き、`BindStatementLiterals`
  でリテラルを差し替える。失敗すれば黙って「毎回パース」に落ちるので、結果は
  正しいまま性能だけが失われ、テストでは見えなかった。`BindStatementLiterals`
  にリテラル入りの再帰CTEを渡すと "SQL template parameter underflow" で失敗する。
- **[原因1] 集合演算の二表現を両方バインドしていた。** ビジターは集合演算を
  (a) `UnionAll()` / `SetOperationKinds()` / `Matches()` の flat 表現と
  (b) グループ化用の `SetOperationTree`（`first` は枝を足す前のヘッドのコピー、
  `branches` は flat と同じノード）の両方で持つ。`BindSelect` は flat の枝を
  バインドした後に tree の `first` と `branches` をもう一度バインドしていたため、
  本文のリテラルを二重に消費して underflow。再帰CTEの本体は必ず集合演算なので、
  再帰CTEは常にキャッシュ対象外だった（`WITH DEPTH` の有無に関係なく）。
- **[修正1] flat を正とし、tree は結果を共有する。** 枝は flat 側で 1 回だけ
  バインドし、tree の `branches` は `result->UnionAll()` をそのまま共有。
  `first` は「ヘッドが消費したパラメータ範囲（`head_begin`..`head_end`）」を
  スクラッチ index で複製バインドし、消費数が一致しなければ
  "cannot re-bind the set operation tree" を返してパースに落とす。枝の同一性は
  ポインタ比較（`source_tree->branches[i].get() == select.UnionAll()[i].get()`）
  で確認する。flat と tree が別物の手組み Statement では従来どおり両方バインド。
- **[原因2] `WITH DEPTH` の上下限がパラメータとして数えられていた。**
  この上下限は `RecursiveDepthSpec`（`int64_t`）に載り式木には無いが、テキスト
  上はただの整数リテラルなので `ExtractSqlTemplate` がパラメータに変換する。
  バインド側は 1 つも消費しないため、常に "parameter count mismatch"。
- **[修正2] 上下限もテキスト順に消費して書き戻す。** `RecursiveDepthSpec` に
  `lower_is_literal` / `upper_is_literal` を追加（ビジターが `IntLiteral` を
  見たときだけ true）。`BindSelect` は CTE 本文をバインドした直後（メイン
  SELECT の前＝テキスト順）に上下限を消費し、キャッシュされた値ではなく新しい
  値を `SetRecursiveDepth` する。`UNBOUNDED` 側は消費しない。リテラルの深さ
  上限を持つエントリが複数ある WITH は宣言順を復元できないため、本文リテラルと
  同じくテンプレート化を拒否する（"cannot recover WITH declaration order"）。
- **[観測] キャッシュが効いたことをテストで見る。** `PlanCacheCounters` に
  `template_fills`（保存）と `template_hits`（再バインド成功）を追加。従来は
  「毎回パースに落ちた」ことを検出する手段が無く、`BindSelect` の退行が
  性能劣化としてしか現れなかった。これで退行をテストで殺せる
  （変異検証: 二重バインドに戻す → unit テスト失敗、深さ上限をキャッシュ値の
  ままにする → e2e で 1 ラウンド多く出て失敗、再帰フラグのコピーを落とす →
  e2e で "table t not found"）。
- 回帰: `QueryTest.TemplateCacheKeepsRecursiveCteMetadata`,
  `SqlTemplateTest.BindsRecursiveWithSetOperationOnce`,
  `SqlTemplateTest.BindsRecursiveWithDepthBounds`。
- **[残課題] コンパイル済みプランキャッシュは CTE / 集合演算を保存しない。**
  保存は Cascades 経路の `!multi_relation` 側だけで、リレーショナル経路へ
  落ちる文は対象外（`query/plan_cache.hpp` 冒頭の方針）。今回直したのは
  パース済みテンプレート（AST）キャッシュ。プラン側を広げるなら
  「リレーショナル経路のプランはスナップショット依存」の監査が別途要る。

## 2026-10-20 CTE依存順と相互再帰 (WITH スコープの契約)

- **[課題] 依存順の走査が浅かった。** `ExecuteQuery` の WITH ループは各エントリの
  **直下の FROM** だけを見て依存を判定していた。`WITH b AS (SELECT * FROM
  (SELECT * FROM a)), a AS (SELECT 1 AS x) SELECT * FROM b` は `b` が
  `a` を知らないまま先に実行され "table a not found"。スカラーサブクエリ
  （`WITH b AS (SELECT (SELECT MAX(x) FROM a) AS m), ...`）も同じ。集合演算の枝や
  `WHERE x IN (SELECT ... FROM a)` が偶然動いていたのは、単一参照の CTE が
  先にインライン化されて参照がトップレベルへ移っていたため。
- **[課題] 相互再帰が "table not found" になっていた。** `WITH a AS (SELECT *
  FROM b), b AS (SELECT * FROM a)` も、`WITH RECURSIVE a AS (... b ...),
  b AS (... a ...)` も、旧コードは残りを map 順に実行して欠けた関係を
  「table b not found」として報告していた。
- **[課題] 再帰CTEの本体が兄弟 CTE を読むと壊れていた。** Cascades の
  `RecursiveCteExecutor` は `ExecuteRecursiveCte(..., {})` と**継承 CTE なし**で
  本体を実行する。`WITH RECURSIVE a AS (SELECT n FROM b UNION ALL ...),
  b AS (SELECT 1 AS n) SELECT * FROM a` は `ExecuteUnnestSelect` が無条件に
  work-table leaf を作るため "table b not found"（2サイトの自己結合だけは
  lift されずリレーショナル経路で動いていた）。
- **[修正] 走査を `executor/detail/subquery_runtime` へ集約した。**
  `CollectStatementTableRefs` は、派生表・スカラーサブクエリ・JOIN 条件・
  UNNEST 引数・集合演算の枝・入れ子 WITH 本体まで辿り、内側 WITH の名前で
  外側を影にする。`CollectCteBodyOuterRefs` が 1 エントリ分の依存集合を作る。
  置き場所は `executor/detail`（rank "relational"）: `executor/relational.cpp` と
  `query/sql_engine.cpp` の両方から下向きに include でき、新しい layering
  例外を増やさない（`query/query_data.*` へ置くと
  `executor -> sql` の新規辺になった）。
- **[契約] 非再帰エントリは自分の定義内では不可視。** 自己参照は外側
  （外側 WITH、無ければカタログ表）へ解決する。コンプライアンス
  `with_on_multiple_levels` が nested WITH でこの規則を固定している
  （`(with q1 as (select x+100 x from q1) select x from q1)` の内側 q1 は
  外側 q1=1 を読んで 101、隣の `q2` は外側 q1 を読んで 11）。したがって
  **相互参照だけが循環**で、`WITH a AS (SELECT * FROM a)` は循環ではなく
  未解決の関係（従来どおり "table a not found"）。自己参照が work table を
  読むのは `WITH RECURSIVE` のときだけ。
- **[修正] 循環は両層で同じ文言。** `ValidateCteDependencies`（WITH 1 スコープの
  解決可能性検査）を `SqlEngine::PrepareStatement` の kSelect 入口で先に呼び、
  `CteCycleStatus` の "cyclic CTE reference among a, b; mutual references
  have no dependency order" を返す。実行器の CTE ループも同じ status を返す
  （手組み `SelectStatement` 用の backstop）。
- **[修正] 再帰 leaf のルートゲート。** `ExecuteUnnestSelect` は
  `RecursiveCteLiftable(select, cte_name)` が成り立つときだけ work-table leaf を
  作る（単一サイト・兄弟参照なし・出力名一意）。成り立たなければ失敗を返して
  呼び出し側のフォールバックに任せ、依存順リレーショナル経路
  （`ExecuteRecursiveCte` を WITH ループから駆動）が兄弟 CTE を解決する。
- **[却下した案] 兄弟 CTE を `RecursiveCteExecutor` に渡す。** 渡すには
  兄弟を先に materialize する必要があり、依存順・spill・スナップショットの
  契約を Cascades 側へ二重実装することになる。ルートをリレーショナルへ
  落とす方が小さく、既存の `RecursiveCteLiftable` ゲートとも整合する。
- 回帰: `SqlEngineCteDependencyOrderFollowsNestedReferences`,
  `SqlEngineRecursiveCteReadsSiblingCteInDependencyOrder`,
  `SqlEngineCyclicCteReferenceRejected`（変異検証: 依存走査を浅くする →
  1 件失敗、自分の名前を依存に残す → 1 件失敗、フロントエンド検査を外す →
  1 件失敗、unnest ルートのゲートを外す → 1 件失敗）。コンプライアンス
  `with_queries_test` も通る（この検査を入れた直後に
  `with_on_multiple_levels` が false cycle で落ち、自己参照の解決規則を
  確定するきっかけになった）。

## 2026-10-20 再帰CTEの列一致契約 (JOIN USING / BY NAME / CORRESPONDING)

- **[課題] `UNION ALL BY NAME` が位置一致のままだった。** `union_by_name_` は
  `MarkUnionDistinct` でしか立たず、`UNION ALL` では一度も立たなかった。結果、
  `WITH RECURSIVE t AS (SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT a*100 AS b,
  b*100 AS a FROM t WHERE a = 1)` が `(1,2),(100,200)`（正しくは `(200,100)`）を
  返していた。`CORRESPONDING` も同じ経路で位置一致になり、名前不一致は
  NULL 埋めで黙って通っていた。
- **[修正] 集合演算メタデータを per-pair で `SetOperationMatch` に落とす。**
  visitor は `SetOperationMetadataList` を項順に走査し、`BY NAME` /
  `STRICT CORRESPONDING` → `by_name`、`CORRESPONDING` → `corresponding`、
  `ON/BY (col_list)`（`ExpressionList` 子）→ `columns` を記録して
  `AddSetOperation(kind, branch, match)` に渡す。項ごとの解決は
  `SelectStatement::MatchFor(pair_index)`（per-pair が無ければ head の
  `union_by_name_` へフォールバック）で、`sql_template` の再構築も
  `Matches()` をコピーするのでキャッシュ経由でも落ちない。
- **[契約] anchor が再帰CTEの形を決める。** 名前一致は「項の値をどの列へ
  束ねるか」だけを変え、CTE の列集合は anchor のまま（`SELECT * FROM <cte>`
  の可視列がラウンドごとに変わらないための前提）。したがって:
  `BY NAME` / `STRICT CORRESPONDING` は項の列名集合が anchor と完全一致する
  ことを要求、素の `CORRESPONDING` は同名一致＋項だけの列を落とす、
  `ON/BY (col_list)` は列挙列だけを一致対象にし、working shape を覆う必要が
  ある（anchor 列を落とす指定はエラー）。anchor 列を作れない項、重複名、
  無名列は NULL 埋めせずエラー。位置一致の項は anchor 幅を要求（従来は
  余剰列が行に漏れていた）。`WITH DEPTH` の depth 列は整列後の末尾。
- **[却下した案] `CORRESPONDING` で anchor 列を削る（INNER の縮小）。**
  CTE の静的スキーマは anchor の select list から作られるため、実行時に
  縮むと `SELECT * FROM <cte>` の射影が範囲外を読み SIGSEGV した
  （`Projection::NextBatch` → `ColumnVector::ValueAt`）。縮小を成立させるには
  フロントエンドの CTE スキーマ推論まで名前一致を教える必要があり、再帰CTEの
  形を固定する方が契約として素直なので却下。
- **[既知のギャップ] 非再帰の集合演算は依然として位置一致。**
  `SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT b, a FROM (...)` は
  `[2,1]`（`BY NAME` 無視）。一般集合演算（grouped tree / INTERSECT / EXCEPT /
  Cascades `SetOperationPlan`）へ同じ契約を広げるのは別項目。再帰CTEの
  テストでこの誤挙動を固定しない。
- 回帰: `SqlEngineRecursiveCteNameMatchingAlignsEveryUnionMode`,
  `SqlEngineRecursiveCteNameMatchingKeepsDepthLast`,
  `SqlEngineRecursiveCteJoinUsingFeedsNameMatching`,
  `SqlEngineRecursiveCteNameMatchingRejectsMismatchedColumns`
  （変異検証: `MatchFor` を常にフォールバック → 3 件失敗、
  `ResolveNameAlignment` の strict 検査除去 → star 項のテスト失敗、
  位置一致の幅検査除去 → 幅エラーのテスト失敗）。

## 2026-10-20 組み込み TVF (FROM GENERATE_SERIES)

- **[課題] `FROM GENERATE_SERIES(1, 3)` が `unknown table function` で落ちていた。**
  `TVF` AST 節は `CREATE TABLE FUNCTION` の UDF 登録簿しか引かず、組み込みの
  テーブル関数を受ける表面がなかった。`UNNEST(GENERATE_SERIES(...))` は動くので、
  欠けていたのは FROM へ直接書く形だけだった。
- **[修正] 組み込み TVF を暗黙 UNNEST へ落とす。**
  visitor の `TVF` 節で `generate_series` を UDF 参照より先に判定し、
  `SelectSource.unnest = generate_series(...)` を作る。エンジンの
  `GENERATE_SERIES` は ARRAY を返すスカラー関数なので、これは
  `UNNEST(GENERATE_SERIES(...))` と同義で、リレーショナル経路の `LoadSource` と
  Cascades の `kUnnest` の両方がそのまま行源化する。専用 `FunctionScanPlan`
  は作らない（行源化の契約は UNNEST と共有）。
  未別名の値列は BigQuery の TVF 出力に合わせて `value`、別名は UNNEST と
  同じく値列を改名する。名前付き引数 (`START => 1, STOP => 7, STEP => 3`) は
  `TVFArgument/NamedArgument` から位置引数へ並べ替え、重複・未知の名前・
  不足/過多は文エラー。引数は先行ソース列を参照できる（暗黙 UNNEST と同じ
  lateral 契約）。`TABLESAMPLE` は UNNEST と同じく拒否する。
  Join 節のオペランド判定に `TVF` を加え、`FROM a JOIN GENERATE_SERIES(...)
  ON ...` を 2 オペランドの join として扱う（従来は `join arity` エラー）。
- **[付随修正] 無名 select item の `ORDER BY <番号>` が UNNEST 値列を見失っていた。**
  `SELECT MOD(x, 2) FROM UNNEST(...) x ORDER BY 1` が `column x not found` で
  落ちていた（列名の無い射影項目にいったん一致すると、ソートキーに元式を
  そのまま使い、射影の上で再評価して `x` を探すため）。無名項目も隠し
  `$order<i>` 射影列に載せる経路へ倒して修正。`value` 列しか持たない TVF でも
  同じ形が踏める。
- **[回帰]** `QueryTest.BuiltinTableFunctionBecomesRowSource`（列名・名前付き
  引数・別名・lateral join・`JOIN ... ON`・`INSERT SELECT`・EXPLAIN が
  `Unnest: generate_series(...)` で `RelationalPlan` でないこと・引数エラー
  5種）と `QueryTest.UnnestAnonymousOrderByKeyRidesAHiddenColumn`。
  変異検証: Join オペランドから `TVF` を外すと前者、無名項目を隠し列に
  載せない変異で後者が落ちる。
- **[残り]** `EXTERNAL_QUERY` / `ML.*` / `APPENDS` などの TVF は外部ソース・
  時系列基盤が前提なので対象外（`ForeignScan` と同じ保留理由）。

## 2026-10-19 Window の spill 形

- **[課題] window 演算子が全入力をメモリに持っていた。**
  `WindowExecutor` は drain した全行 + 呼び出しごとの計算列を常駐させる。window
  は区画全体を見る必要があるので、素朴には「1 区画以上」をメモリに置くことになり、
  全表に対する `RANK` のような問い合わせでは入力サイズがそのまま上限になる。
- **[修正] 区画単位のハッシュ分割 + 1 ファイル常駐。**
  `WindowPlan` が `WindowMemoryPolicy{memory_budget_bytes,
  spill_partition_count}` を持ち、`window` 実装ルールが子の行推定から
  `WindowPolicyForRows(rows)` で選ぶ（既定 64MB ÷ 64byte/行 ≒ 104 万行）。
  容量を超える推定のときだけ予算を渡し、executor は drain 中に PARTITION BY
  タプルのハッシュで `SpillFile` へ振り分ける。ハッシュは
  `detail/window_eval` の区画化と同じ `WindowPartitionKeyHash` なので、
  「等しいキーは等しいハッシュ」が成り立ち、**区画全体が必ず 1 ファイルに
  収まる**。後はファイルを 1 つずつ読んで `ComputeOneWindow` を回すだけ。
  上限は入力ではなく最大ファイル（＝最悪で 1 区画）。
- **[契約1] 順序の承諾は plan 時に降ろす。**
  buffered 形は子の行順を保つので子の ordering claim を引き継げるが、spill 形は
  ファイル順に出力する。実行時に spill したかどうかで claim を変えることは
  できない（plan が先に決まる）ので、**容量を超え得る推定には予算を渡して
  claim を落とす**。過小推定で実際に spill しなくても claim が嘘にならない側に
  倒れる。推定が容量以下なら予算 0（buffered 固定）で従来と完全に同じ。
- **[契約2] PARTITION BY が揃っているときだけ分割できる。**
  ファイルは 1 つのキーで選ぶので、呼び出しごとに PARTITION BY が違うと、ある
  呼び出しの区画が 2 ファイルに割れる。その場合は spill を諦めて buffered に
  固定する（`WindowPlan::CallsShareOnePartitionKey`）。
- **[落とし穴] 終端は粘着でなければならない。**
  最初の実装は `NextChunk` の先頭で `read_offset_` を 0 に戻していたため、
  終端の false のあとにもう一度引かれると常駐チャンクを再生していた。QUALIFY
  経路の親が空バッチを「再試行」として引き続けるため、**行が無限に増える
  ループ**になり、`WindowSqlCov.QualifyReferencingWindowResult` が 90 秒走って
  spill file 枯渇で失敗した。`exhausted_` を立てて false を粘着にし、回帰
  `WindowSpillTest.EndOfStreamIsStickyForBothShapes` を追加（再導入変異で落ちる）。
- **[回帰] plan 側 `plan/window_shape_test.cpp`（3 件）。**
  容量→形の選択と NaN/∞ 推定、buffered だけが子の claim を保つこと、
  PARTITION BY 不一致で buffered に落ちること、Dump が spill 形だけを名乗ること。
- **[回帰] executor 側 `executor/window_spill_test.cpp`（4 件）。**
  spill と buffered が同じ行・同じ値を出すこと（順序は比較しない）、buffered が
  入力順を保つこと、resident 上限 < 入力行数、batch がファイル境界を越えること、
  PARTITION BY 不一致時の fallback、終端の粘着性。変異検証: 区画を order key で
  切り直すと 2 件、全行を 1 ファイルにすると `SpillFileCount` と
  `PeakResidentRows` が落ちる。
- **[残りのレバー]** 1 区画が予算を超える場合の区画内 spill。frame 評価を
  ストリーム化（ROWS frame は可能、RANGE/GROUPS は peer 境界の再計算が要る）
  すれば、上限を「ファイル 1 つ」から「frame 1 つ」に下げられる。

## 2026-10-19 Window の hash 区画化

- **[課題] 区画分けが見つけた順の代表キー線形走査だった。**
  `BuildWindowOrderLayout` は行ごとに「これまでに見つけた代表キー」を前から
  比較し、一致しなければ新規グループにしていた（O(行 × 区画数)）。全表に対する
  顧客ごとの RANK のような「小さな区画が大量」の形ではこれが支配項になる。
- **[修正] 代表キータプルをハッシュ表で引く。**
  `std::unordered_map<std::vector<Value>, size_t, PartitionKeyHash,
  PartitionKeyEq>`。グループは初出順に `partitions` へ push するので
  **レイアウトも出力位置も旧実装と同一**で、探索だけが期待 O(1) になる。
  区画内の並べ替え（事前評価した `order_values` + `peer_end` + `stable_sort`）は
  変更なし。つまり TODO の "sort" 半分は既にあり、"hash partition" 半分が
  これで入った。
- **[契約] ハッシュは等価性と一方向に整合していればよい。**
  等価判定は `ValuesEqual` のまま:
    - NULL 同士は同一区画（SQL では `NULL = NULL` は unknown だが区画は同じ）
    - 全 NaN が1つの peer group
    - interval 風テキストは interval として比較（`EqualsSlow`）
  ハッシュは `std::hash<Value>` で、同じく NULL を1パターンに、NaN を正規化し、
  interval テキストを parse してハッシュするので「等しい値 ⇒ 等しいハッシュ」が
  成り立つ。逆向きは不要で、衝突（同じビット列の INT64 と UINT64 など）は
  `PartitionKeyEq` が弾くので区画が混ざることはない。
- **[回帰] `query/sql_extra_test.cpp` の 2 件。**
  `InterleavedPartitionsKeepTheirOwnFrames`: 400 行 / 50 グループを
  `g = i % 50` で非連続に置き、各行の `COUNT(*) OVER (PARTITION BY g)` が 8、
  `SUM` が `8g + 1400`、`ROW_NUMBER` が自分のグループ内順位であることを固定。
  `NullAndNanPartitionKeysEachFormOneGroup`: NULL 3 行が1区画、NaN 2 行が
  1区画であることを `COUNT`/`SUM` の2つの window で固定。
  グループを再利用しない変異（毎行を新規区画にする）で両方が落ちることを確認。
- **[残りのレバー]** 次項の Window spill。区画化はメモリ常駐前提で、行も
  区画も予算を持たない。`order_values` の事前評価は区画あたり O(行 log 行) の
  並べ替えを軽くしているが、メモリは減らさない。

## 2026-10-19 lookup locality の cost 化

- **[課題] index nested-loop の局所コストが外側の順序を見ていなかった。**
  `JoinAlternatives` は `l_rows * 2.0 + equi`（probe 1回 + 行組み立て1回）で
  見積もっていた。外側が probe キー順に来ていれば、連続する probe は直前の
  リーフか隣のリーフに着地し、根からの下降路とリーフページが buffer pool に
  残る。同じ仕事量でも順序ありの方が安いのに、コストは同額だった。
- **[修正] probe 半分だけを割り引く。**
  `l_rows * (ordered ? 0.75 : 1.0) + l_rows + equi`。返る行とその組み立ては
  順序に依存しないので触らない。`DeliversProbeOrder` は
  `probe->left_columns`（索引キー順に並べた左カラム）を
  `ColumnValueExp` にして `IsOrderedBy` に渡すだけでよい
  （`OrderMatches` は修飾/非修飾の両表記を解決する）。
- **[判断] measured order も対象。** `OrderIsMeasured` が early-stop 割引から
  外す理由は「順序*を得る*のに鎖を最後まで歩いた」ことであって、順序が
  存在しないことではない。probe には実際に昇順キーが届くので局所性は本物。
  なお clustered 主張を含む文はコンパイル済み計画キャッシュから除外される
  （`AnyResolvedTableHasClusteredOrder`）ため、取り消し可能な主張が計画に
  焼き付く問題もない。
- **[回帰] 形状が割引の証拠になる形にした。**
  `OptimizerTest.OrderedOuterDiscountsTheIndexProbeIntoAWin`。同じ100行の
  外側を2つ作り、片方だけ join キーに索引を持たせる（⇒ その外側だけが
  索引順を供給できる）。フィルタ後の外側20行・内側37行（一意キー）では
    - 順序あり probe = 0.75\*20 + 20 + 20 = 55
    - 最良の hash 向き = 37 + 20 = 57（逆向きは 20 + 37 + 8.5 = 65.5）
    - 順序なし probe = 20\*2 + 20 = 60
  なので、順序ありだけが index join を選ぶ。`Index Join` の有無で判定し、
  両者の出力行が一致することも確認（割引は形状だけを変える）。係数を 1.0 に
  戻す変異で落ちる。
- **[残りのレバー]** 子グループに「probe キー順」を *要求* していないので、
  割引は「最安の外側計画がたまたま順序を供給する」ときだけ効く。結合の子に
  interesting order を要求する仕組み（`PhysicalProperties` の ordering
  requirement を join の子へ渡す）は未実装。実行器側も
  `BPlusTree::Read` の `hint_leaf` を read 経路へ通していないため、現在の
  効果はキャッシュ局所性に限られる（根からの下降を実際に省くのは別作業）。

## 2026-10-19 LookupJoin の unique 内側短絡

- **[状況] 扉はあったが契約が無かった。** `LookupJoin::at_most_one_` は
  `!range_position_` かつ `StoresSingleValue()` かつ
  `left_cols_.size() == index.sc_.key_.size()` のとき立ち、一致した候補で
  索引カーソルを進めず外側カーソルを進める。実装自体は入っていたが、
  (a) 何が「1行」を保証するのか、(b) どこまで打ち切ってよいのか、
  (c) 形状をどう監査するのか、がどこにも書かれておらず、回帰も
  「結果が正しい」ことしか見ていなかった(結果は短絡の有無で変わらないので、
  扉が壊れても緑のままだった)。
- **[保証] `kUnique` はキーごとに単一値。** 索引は NULL 以外のキーを
  `IndexValueType` 1個で持ち、重複キーの挿入は `BPlusTree::Insert` が
  `kDuplicates` で弾く。よって完全キーへの点 probe の候補は高々1行。
  `kVersionedUnique` は同じキーに複数 position が並び得る(削除保持のための
  ベクタ表現で、挿入は一意性を強制しない)ので対象外。
- **[修正] 打ち切ってよいのは「一致した」候補だけ。** 旧実装は最初の**候補**で
  打ち切り、`Matches` が偽ならその外側行を捨てていた。点 probe では未達だが、
  `BuildSeekRange` の契約は「使えない bound は前置走査へ広げる。広がるだけで
  誤った行は通さない」であり、この前提が崩れた瞬間に fallback が**行の
  取りこぼし**へ変わる。一致した候補だけ短絡し、不一致は通常の走査へ落とす形に
  した(`Matches` が唯一の判定者)。挙動は不変、契約だけが強くなる。
- **[監査] `AtMostOneProbe()` + dump の `at-most-one`。** 形状は EXPLAIN では
  なく実行器 dump に出るので、ログで「なぜこの probe が歩かないのか」が読める。
- **[回帰]** `ExecutorTest.LookupJoinUniquePointProbeShortCircuits` /
  `...UniquePointProbeHandlesRepeatedOuterKeys`(外側キー重複。hold を捨てて
  次の行を probe する状態機械を固定) / `...NonUniqueInnerKeepsTheFullMatchSet` /
  `...UniqueCompositeKeyNeedsTheWholeKey`(前置のみ → 短絡しない、完全キー →
  短絡する、を同一索引で対比) / `...RetainedUniqueIndexIsNotASingleValue`。
  SQL: `QueryTest.UniqueInnerJoinEmitsOneRowPerOuterRow`(外側キー重複・内側に
  無いキー・COUNT の3方向)。扉を `&& false` で殺す変異で3件が落ちる。
- **[残りのレバー]** 外側が同じキーを連続で持つとき probe を作り直さない
  locality 最適化(次項「ソート済み外側からの lookup locality」の実行器側)と、
  その cost 化。内側 filter の定数 bound を seek range へ載せる話は前節のまま
  未実装。

## 2026-10-19 LookupJoin の内側 filter 契約

- **[旧ゲート] 右の scan group に filter があれば index join 形状を出さない。**
  `LookupJoin` は右の子 plan を完全にバイパスして生テーブルを index で probe
  するため、右の子に載っている filter は実行されない。旧コメントはこれを
  「黙って落ちる」事情として形状ごと拒否していた。実際には
  `push_selection_through_join` / `split_selection_over_join` は conjunct を
  scan group へ**併記**するだけで join の Selection からは落とさないので、
  gate が守っていた bug は現状発火しない。つまりこれは**過剰に保守的な拒否**で、
  そのせいで nullable equi-join(NULL 安全性の推論が
  `l.x = r.a AND l.x IS NOT NULL AND r.a IS NOT NULL` を打ち込む形)は
  index nested-loop に到達できていなかった。
- **[新契約] 拒否理由ではなく residual の積荷。** gate を撤去し、
  residual の供給元を 3 系統に広げた:
  1. probe が確定しなかった join 述語の conjunct(従来どおり)
  2. **右の子 plan に押し込まれた conjunct** —— probe はその子を消費しない
  3. **内側 relation の scan group filter**

  既に適用済みという印 `pushed` は**左の子だけ**にした(右の子はバイパス
  されるので「消費済み」に数えてはいけない)。結合結果は右の全カラムを持つ
  ので、内側 conjunct はそこで元の意味のまま評価される。付け加える conjunct
  はすべて元の WHERE 由来なので、誤って行を**含める**方向にしか動かない
  ——既存の結果が悪化しないことが構成上保証される。
- **[回帰]** `OptimizerTest.FilteredSelfJoinStillPlansThroughTheIndexNestedLoop`
  (hash/NL を無効化して probe 形状を強制。旧ゲートを戻すと「Index Join なし」
  で落ちる)。既存の `FilteredSelfJoinKeepsRightSidePredicate` は「どの join
  が勝っても結果は同じ」という契約としてそのまま green。
- **[残りのレバー]** 内側 filter の conjunct を probe の seek range へ出す
  (定数 bound を先頭キーへ載せる)最適化は**未実装**。現状は全部 residual に
  落ちるので、probe は少数の index range を読むが inner rows は全部
  materialize される。
- **検証**: 全 ctest **3940/3940 合格**(意図的 skip 3)。`check_layering.py`
  成功(allowlist 65辺)。clang-format 済。

## 2026-10-19 複数インデックス covering scan の成果

- **[方針] どの1本も被覆しないなら、2本を束ねて被覆する**。既に
  `IndexOnlyScanPlan` が「1本のインデックス(キー+INCLUDE)が述語列と出力列を
  被覆する」場合を面倒見ていた。残っていたのは **どれも単独では被覆しない**
  形。`MultiIndexCoveringScanPlan`(`plan/multi_index_covering_scan_plan.*`)
  と `executor/multi_index_covering_scan.*` を追加した。
- **[plan] 参加者は「先頭キー列に範囲を1つ持つ非保持インデックス」1本ずつ。**
  最小被覆は貪欲法、他の参加者が既に持つ被覆列しかない参加者は削除(1本だけ
  残ったら index-only 経路の担当なので不発)。`kVersionedUnique`(削除エントリ
  を保持)は committed image を保証しないので参加者にしない。
- **[executor] 行位置で交差**し、交差集合の行を index image だけで組み立てる。
  エントリの行ページにバージョン鎖が無ければ `TryReadIndexOnly` と同じ可視性
  根拠で heap fetch を省き、鎖があれば snapshot 解決の heap read に落とす。
  Cover 列はテーブル slot 順の和集合(位置で埋められる決定的 layout)。
  範囲は「一致行の超集合」に過ぎないので、**交差後の recheck が答えを決める**
  (短絡には決してしない)。
- **[実装で判明した落とし穴(重要)] 計画キャッシュが literal を ParameterSlot に
  書き換えるため、計画時の sniffed 範囲は emit 時に必ず古くできている。**
  当初「slot があれば full scan に落とす」という `BitmapScanPlan` と同じ契約
  にしていただけると、**EXPLAIN には MultiIndexCoveringScan が出ても SQL
  実行時は常に full scan になり、演算子が dead code だった**。突変テスト
  (recheck を off にした時に結果が変わらないこと)で発覚し、
  `IndexOnlyScanPlan::EmitExecutor` と同じく **bound 済み述語から
  `ReboundIndexKeysFromPredicate` で各参加者の seek キーを張り直す** よう
  修正した。先頭キーの bound を失った参加者が1つでもあればノード全体を
  full scan に落とす(狭まらない promise をできないので)。read guard は全参加者
  を index root 昇順で一括取得。
- **回帰**: `executor/multi_index_covering_scan_test.cpp` 9件。SQL 結果は
  インデックスなしの双子テーブル `parts_plain` のフルスキャン結果と行単位で
  一致。突変検証: (a) recheck を切ると 7件が落ちる (b) 交差の drop を切ると
  直接実行器ケースが落ちる(＝交差と recheck が独立に固定されている)。
  直接実行器のケースでは `region` だけを recheck に残すことで、交差が
  唯一の絞り込みになっている答えを検証している。
- **検証**: 全 ctest **3939/3939 合格**(意図的 skip 3、うち新規9件込み)。
  `check_layering.py` 成功(allowlist 65辺)。clang-format 済。
- **既知の残り**: 交差表は `BitmapHeapScan` と同じく位置列を全件メモリに
  持つ。両経路共通のメモリ予算化は別項目。

2026-10-17 加筆(第34報)。統合TODOの次項目: **複合インデックスの等式 +
不等式混在**(LookupJoin probe の区間境界)。ForeignScan は外部ソース形式が
未定のため保留し、次の具体項目から着手した。詳細は下記
「2026-10-17 LookupJoin 区間 probe」の節。

## 2026-10-17 LookupJoin 区間 probe の成果

- **[方針] 等式 prefix + 同一キー列の両側境界**(ユーザー判断で ForeignScan を
  一旦スキップ)。従来の probe は「等式 prefix + 片側の範囲 bound 1 個」だけで、
  `lo < b < hi` のような両側不等式は片側しか probe に入らず、残りは残差として
  過剰フェッチを絞るだけだった。下限・上限を 1 個ずつまで採用して区間
  `[prefix + lower, prefix + upper]` をシークし、両端の開閉は候補行ごとの
  再検査で決める。
- **[plan] `LookupSeek` に `second_range_left_column` / `second_range_operation`
  を追加**(区間の 2 個目の bound。値は左行のその列から取り、比較は
  `right <op> left` の向きで保持)。`ProductPlan::SetLookupRange` は 2 回目の
  呼び出しを「同じ位置・反対側」のときだけ受け入れ、同側の 2 個目・3 個目・
  位置違いは `false` を返して conjunct を残差に戻す。
- **[rule] `FindLookupRange` → `FindLookupRanges`**: 対象キー列について下限・
  上限を 1 個ずつまで集める(conjunct 順は不問)。採用した bound は両方とも
  `enforced` に入り、残差 `Selection` から消える。
- **[executor] `LookupJoin`** は区間 seek を組み立て(`BuildSeekRange` が
  lower を begin、upper を end へ)、`Matches` で両 bound を再検査する。
  2 個目の bound が NULL/NaN の左行は「述語が TRUE になり得ない」ので
  キー NULL と同じく読み飛ばす。片側 bound が実行時に使えない場合
  (UINT64 タグ等)はその側だけ落として走査を広げ、再検査が行を決める。
- **[EXPLAIN] `probe:{a, lo < b < hi}`** の形で区間を表示(下限は
  `right <op> left` の格納向きを反転して `left < right` として描画)。
- **回帰**: `OptimizerTest.IndexJoinRuleAdoptsBothIntervalBoundsIntoTheProbe`
  (両 bound 採用・残差なし・鏡像一致)、
  `ExecutorTest.LookupJoinCompositeIntervalProbe`(区間 seek、bound の順序交換、
  開閉端、NULL 上端)、`QueryTest.CompositeIndexLookupJoinIntervalProbe`
  (SQL 経路の鏡像一致)、`PlanTest.LookupRangeAcceptsOnlyInt64AndDate`
  (反対側の採用と区間表記、同側・3 個目・位置違いの拒否)。
- **既知の残り**: 同じ側の 2 個目の境界(例: `b > x AND b > y`)と 3 個目以降の
  キー列の境界は残差のまま(前者は区間を狭めないため採用しない)。
- **検証**: 全 ctest **3930/3930 合格**(意図的 skip 3)。
  plan(59)/optimizer(125)/executor(201)/query(187)/plan_extra(10)/
  relational_regression(20)/late_materialization_join(9) green、
  `check_layering.py` 成功(65辺)。

2026-10-16 加筆(第33報)。統合TODOの次項目: **LSM / WiscKey の `LsmScan`
実装規則**(plan/executor 層のみ)。詳細は下記「2026-10-16 LsmScan 実装規則」の節。

## 2026-10-16 LsmScan 実装規則の成果

- **[方針] スコープは plan/executor 層のみ**(ユーザー判断)。LSM は現在
  テーブル/SQL/カタログに未接続で、トランザクション統合(WAL/undo)を伴う
  副索引化は複数セッション規模のため、今回は論理ノード→実装規則→plan→
  executor の縦の一本を完成させ、SQL からの到達は将来の storage 統合に委ねる。
- **[plan] `LogicalOperator::kLsmScan`(葉)** を追加。ペイロードは
  caller-owned の `const LSMTree*` と行数推定(`lsm_estimated_rows`)。
  Fingerprint にツリー同一性(ポインタ)と推定値を含める。Memo 検証・
  RequiredChildProperties は葉として扱う。
- **[plan] `LsmScanPlan`**: 出力スキーマ = (key VARCHAR, value VARCHAR)。
  `IsOrderedBy` はキー1列の昇順のみ主張(ビューは run をキー順にマージする。
  NULL は出ないため NULL 配置要求は常に充足)。
- **[executor] `LsmScan`**: `LSMTree::GetView()` のスナップショットを構築時に
  取得し、`LSMView::Iterator` で世代最新勝ち・tombstone スキップの live pair
  を流す。blob/IO 失敗は sticky status。**ビューは登録済み run のみを対象**
  とするため、最新の書き込みは `Sync`(またはフラッシャーの flush)後に見える。
- **[rule] `lsm_scan`**: `kLsmScan` → `LsmScanPlan`(推定行数をコスト)。
- **回帰**: `LsmScanTest.StreamsLivePairsInKeyOrder`(上書き勝ち・tombstone
  スキップ・blob 長キー)、`RuleSelectsThePlanAndEmitsTheSameRows`
  (規則→plan→EmitExecutor→行、順序主張)、`EmptyTreeYieldsNoRows`、
  `FingerprintSeparatesTreesAndEstimates`。
- **既知の残り(将来の storage 統合時)**: SQL 到達(DDL/カタログ)、キー範囲
  境界(ビューは seek 非対応)、トランザクション統合(WAL/undo/可視性)。
- **検証**: 全 ctest **3927/3927 合格**(意図的 skip 3)。
  lsm_scan(4)/cascades(192)/cascades_rules(82)/plan(59)/
  plan_extra(10)/optimizer(124)/executor(200)/query(186)/TPC-H(8)/
  compliance(296) green、`check_layering.py` 成功(65辺)。

2026-10-16 加筆(第32報)。統合TODOの次項目: **複数レンジ IndexScan の順序回復**
(point-union / OR-of-prefixes の大域順序主張)。詳細は下記
「2026-10-16 複数レンジ IndexScan の順序回復」の節。

## 2026-10-16 複数レンジ IndexScan の順序回復の成果

- **[planner] `ScanAlternatives` の point-union と disjunctive-prefix 候補が
  全キー列を provided order として提示するようにした。** どちらもレンジ列は
  ソート済み・非重複に構築され (`std::ranges::sort` + `unique`、prefix は
  同サイズで disjoint)、等値プレフィックスは全レンジで一定なので、
  レンジを順にスキャンするだけで大域的にキー順になる(マージ不要)。
  従来は point-union が単一レンジのときだけサフィックス列を提示し、
  複数レンジでは順序主張なし → SQL 層が Sort を挿入していた。
- **効果**: `WHERE k IN (...) ORDER BY k` や `(...prefix...) OR ...` の
  ORDER BY が Sort なしで満たされる。SQL 経路の EXPLAIN で
  `ParallelSort`/`BufferedSort` が消えることを CLI とテストで確認。
- **回帰**: `OptimizerTest.InListPointUnionKeepsKeyOrderWithoutSort` /
  `DisjunctivePrefixRangesKeepKeyOrderWithoutSort`(plan の order 主張と
  昇順行)、`SqlClusteredOrderTest.InListPointUnionKeepsKeyOrder`
  (EXPLAIN に Sort なし、end-to-end)。
- **検証**: 全 ctest **3923/3923 合格**(意図的 skip 3)。
  optimizer(124)/plan(59)/cascades(192)/cascades_rules(82)/
  query(186)/TPC-H(8)/TPC-DS(15)/compliance(296)/sql_* 各スイート green、
  `check_layering.py` 成功(65辺)。

2026-10-16 加筆(第31報)。統合TODOの次項目: **PrefetchingIndexScan /
ParallelIndexScan / RangePartitionedScan**(インデックススキャンの先読み・
リーフ境界分割・並列化)。詳細は下記「2026-10-16 インデックススキャン
先読み+並列化」の節。

## 2026-10-16 インデックススキャン先読み+並列化の成果

- **[executor] PrefetchingIndexScan。** 直列 `IndexScan` に先読みカーソルを
  追加: `kIndexScanPrefetchDistance`(8) エントリ先を並走し、その位置の
  行頁を `GetPage`+即解放でバッファプールにウォームする(先読みは主経路の
  1 歩につき 1 歩=定数償却)。ゲージ `index_scan_prefetch_pages`、
  A/B hatch `TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH=1`(無効時はウォーム 0 を
  確認済み)。失敗は無視(主経路が再取得して自分の status を返す)。
- **[index] RangePartitionedScan。** `BPlusTree::ScanPartitionBoundaries` が
  リーフ鎖を歩き、`leaves_per_partition` 枚ごとの非空リーフ先頭キー
  (begin より厳密に大、end 以内、重複排除済み)を memcomparable で返す。
  `IndexScanIterator::PartitionBoundaries` が復号してキー像にする。
  空リーフ・foster・高フェンスの扱いは `BPlusTreeIterator::operator++` と
  同一の歩き方。
- **[executor] ParallelIndexScan。** キー範囲を半開モーセル
  `[b_i, b_{i+1})` に分割し、worker は静的所有(worker w が
  w, w+W, …)で 1 モーセルずつ直列 `IndexScan` を回す。内部境界キーは
  次モーセルが所有するため除外(重複キーがリーフ境界を跨いでも欠落・重複
  なし)。モーセル順ハンドオフで**直列と同じキー順・行・RowPosition** を
  返す。残余述語は AST 評価の単一スレッド契約に合わせて上位 Selection に
  乗せる。閾値 `kParallelIndexScanMinRows`(8192)、hatch
  `TINYLAMB_DISABLE_PARALLEL_INDEX_SCAN`(無効時は直列 `IndexScan` に
  フォールバックすることを確認済み)。DML ソース(lock_rows)と降順は直列維持。
- **回帰**: `ParallelIndexScanTest` 4件(範囲一致/全範囲/空範囲/バッチ経路)、
  `IndexScanIteratorTest.PartitionBoundariesTileTheLeafChain`、
  `OptimizerTest.ParallelIndexScanEmittedForLargeIndexRange`(大範囲で並列・
  小範囲で直列)、`ExecutorTest.IndexScanPrefetchesHeapPagesAhead`。
- **検証**: 全 ctest **3920/3920 合格**(意図的 skip 3)。
  index_scan_iterator(22)/parallel_index_scan(4)/executor(200)/
  b_plus_tree(70)/b_plus_tree_iterator(32)/index(11)/table(47)/
  optimizer(122)/plan(59)/cascades(192)/cascades_rules(82)/
  query(186)/TPC-H(8)/TPC-DS(15)/compliance(296)/sql_* 各スイート green、
  `check_layering.py` 成功(65辺)。

2026-10-16 加筆(第30報)。統合TODOの次項目: **covering index 選択のコスト精度**
(covering 候補の検出漏れと、covering へのクラスタリング罰の誤適用の修正)。
詳細は下記「2026-10-16 covering index コスト精度」の節。

## 2026-10-16 covering index コスト精度の成果

- **[planner] covering(IndexOnly)候補の検出をラップ前に移動。**
  `ScanAlternatives` は `BuildIndexScan` の戻り値を Selection/Projection/
  Rename で包んだ後に型判定していたため、covering 判定が常に外れていた
  (無拘束経路も finalize 後に判定しており、別名付きでは外れていた)。
- **[planner] covering 候補のコストを「マッチ行数 ×
  `kCoveringIndexCostFactor` (0.5)」に統一し、range スキャンの
  クラスタリング罰 (`RangeClusteringPenalty`) は covering には適用しない**
  (インデックスのみを読み、エントリはキー順に到着するためヒープシークが
  無い)。非 covering は従来通り matched rows × penalty。無拘束 covering
  経路も同じ定数へ統一。
- **効果**: 同一 range の非 covering インデックス(先に作成された場合でも)
  に covering が勝つ / 非クラスタ列の covering range がフルスキャンに勝つ。
- **回帰**: `OptimizerTest.CoveringIndexWinsOverNonCoveringIndexForTheSameRange`
  / `CoveringIndexRangeSkipsTheClusteringPenalty`。IN リストの point-union
  テストは、IN が非選択的な既存データでは covering range が正当に勝つため、
  選択的な IN を持つ専用テーブル形状へ更新(意図=point-union 選択は不変)。
- **既知の残り(別項目)**: IN リストの point-union 候補は
  `IndexScanPlan` 直構築で、`IndexOnlyScanPlan` が複数レンジを持たないため
  covering にならない(複数レンジ covering は未実装)。
- **検証**: 全 ctest **3913/3913 合格**(意図的 skip 3)。
  optimizer(121)/cascades(82)/googlesql_compliance(296)/TPC-H(8)/TPC-C(1)/
  TPC-DS(15)/query(186)/sql_* 各スイート green、`check_layering.py`
  成功(65辺)。

2026-10-16 加筆(第29報)。統合TODO(docs/todo.md)先頭項目の消化:
**IndexOnlyScan の MVCC visibility map 相当**(チェーンフリー頁のエントリを
B+Tree 像から直接返し heap fetch を省略)。詳細は下記
「2026-10-16 IndexOnlyScan visibility map」の節。

## 2026-10-16 IndexOnlyScan visibility map の成果

- **[executor/index] `IndexScanIterator::TryReadIndexOnly` を新設。**
  非保持インデックス(kUnique/kNonUnique)のエントリが、行ページにバージョン
  チェーンを持たないとき、B+Tree エントリそのもの(キー列+INCLUDE ペイロード)
  を可視イメージとして返せる。根拠はフルスキャンの `PhysicalReadEligible`
  と同じ write-before-latch / GC-drop 不変条件: チェーンの無い頁は
  コミット済み状態のみを持ち、非保持エントリは削除・キー移動時に物理削除
  される。呼び出し側(`IndexOnlyScanPlan::EmitExecutor`)の stale-key ゲート
  (index read guard 取得の前後で `IndexKeysMayBeStale` を再確認)が
  エントリの鮮度を、スキャン中の read guard が INCLUDE ペイロードの凍結を
  保証する。kVersionedUnique は古い版のエントリを保持するため従来通り
  ヒープ解決。旧スナップショットが pin したチェーン・未コミット書き込みの
  チェーンも同様に対象外。
- **[executor] `IndexOnlyScan::Next` がこの高速路を最初に試し**、非該当時のみ
  ヒープ行を解決して可視版+INCLUDE を得る。
- **[計測] `TransactionRuntimeStats::index_only_visible_serves`** を追加
  (A/B hatch `TINYLAMB_DISABLE_INDEX_ONLY_VISIBILITY_MAP=1` で従来経路に
  固定できる。ハッチ無効化時はカウンタが動かないことを手動確認済み)。
- **回帰**: `IndexScanIteratorTest.IndexOnlyVisibilityMap*` 5件
  (チェーンフリー頁の提供/保持エントリ拒否/チェーン頁拒否/同キー更新後の
  INCLUDE 鮮度/キー移動後の旧エントリ消滅)、
  `ExecutorTest.IndexOnlyScanServesVisibilityMapWithoutHeapFetch`(直接実行器+
  カウンタ)、`SqlClusteredOrderTest.CountStarIndexOnlyScanServesVisibilityMap`
  (SQL COUNT(*) 経路の end-to-end)。
- **検証**: index_scan_iterator(21)/executor(199)/table(47)/
  transaction(35+16+70)/full_scan系(9+10)/differential(15)/
  sql_clustered_order(16) green、`check_layering.py` 成功(allowlist 65辺)、
  clang-format 済。

2026-10-10 加筆(第28報)。トランザクションエンジン強化(第15ラウンド):
**チェックポイント後の再オープンでコミット済みデータが大量欠落する P0 の根治**
(リサイクル済みWALプレフィックス上のコミット事前走査がセグメント末尾の
プリロールパディングで静止するバグ)。kill -9 ソーク 5/5・全ctest 3894/3894・
欠落アーティファクトからの 6,001,215 行完全復元で検証。詳細は下記
「2026-10-10 トランザクションエンジン強化(第15ラウンド)」の節。

## 2026-10-10 トランザクションエンジン強化(第15ラウンド)の成果

**計測(10-09のゴールドDB作成失敗の再現)から始めた。**
`TINYLAMB_CHECKPOINT_SECONDS=5` × `--load-workers 8` の SF1 ロードは
クリーン終了したのに、再オープンで
`full scan reached a non-row page` + 行数欠落(6,001,215→850,000)、
フリーリストが 21,907 ページ(全ページの過半)に膨らむことを再現した。
毎回の再オープンで「1 loser chain rolled back」が繰り返され、保持WALが
開くたびに 1.2GB→2.7GB→6.3GB と成長する二次被害も確認。

**[P0根治] リサイクル済みプレフィックス上のコミット事前走査(prescan)が
最初のセグメント末尾パディングで無音break。** メカニズムは3段:

1. master record の anchor より前のコミットは prescan
   ([log_start, anchor))が収集する。一方 redo は
   max(redo_start_point=min DPT recLSN, log_start) から再生するため、
   anchor 以前のコミット済みレコードが再生対象に含まれる。
2. prescan は「読めないレコードで break」していた。セグメント末尾の
   プリロールパディング(ゼロ埋め)は毎境界に存在するため、prescan は
   **最初のセグメント境界で必ず静止**し、それより先の anchor までの
   コミットが committed_txn に載らない。
3. per-page undo(`PageReplay` の undo フェーズ)は committed_txn に無い
   txn のレコードを物理 undo する → **コミット済み INSERT の行削除 +
   kSystemAllocPage の CompensateAllocation(ページ解放)** が大量発火。
   実測では 1 回の復元で 21,762 件のアロケーション補償と 32,644 件の
   stale-skip WARN。

修正は prescan を分析パスと同じ `PreRollPaddingEnd` スキップに対応させた
こと(実ダメージ=非パディング破損・セグメント欠落は従来通り break で
fail-closed)。あわせて2件の硬化:

- **loser_heads から txn 0 を除外**: システムレコード(ALLOC/FPW/DESTROY)
  は kCommit を持たないため常に「loser head」候補になり、head レコードが
  kSystemAllocPage だとコミット済みページを解放し得る(今回の実行でも
  「Rolling back loser transaction 0 (0 records)」が毎回出ていた)。
- リグレッション `RecoveryExtraTest.PrescanSurvivesSegmentPaddingWhenRecycling`
  (新設): リサイクル済みプレフィックス + フラッシュ無効チェックポイント +
  redo バッチ境界を跨ぐ 3,000 行×12 文の挙動。修正前は 21,000 行欠落 +
  3 ページ誤解放で失敗、修正後は全行生存。**注意: 単発の 1 行 INSERT では
  ピンにならない** —— redo のセーフティネット(バッチ走査中のコミット
  収集)がバッチ 1 回分なら全コミットを回収するため、境界を跨ぐ巨大 txn
  が存在して初めて露出する(実際のロードの 50k 行バッチがまさにそれ)。

**検証**: 欠落アーティファクトのスナップショットから
6,001,215/1,500,000/150,000 行を完全復元(anchor 503M のままの状態で)。
kill -9 ソーク 5/5(2秒チェックポイント×8並列ロードを17-21秒で kill→
再オープン検証、全て成功)。全 ctest 3894/3894。`check_layering.py`
成功(allowlist 65 辺。旧 `executor/scan_filter.cpp` の stale コピーが
文書化なき violation を出していたため削除 —— 実ビルド対象は
`executor/detail/scan_filter.cpp`)。clang-format 済。

**[計測] チェックポイント有効化の副作用解消(ゴールドDB作成プロトコル更新)。**
本修正により `CHECKPOINT_SECONDS=5 × --load-workers 8` でのロードと
再オープンが安全になった。10-09 第12ラウンドで記録した「ゴールドDBは
`CHECKPOINT_SECONDS=0 + --load-workers 1` で作る」の制約は**本バグ回避が
実体**であり、チェックポイント/並列ロード自体は問題ない(以後の作成は
通常設定で可。要再検証時は本テストと kill ソークでカバー)。

**[計測] TPC-C W=1/16cl/60s/seed42 の現行プロファイル(修正後ベースライン)。**
tps 931-1052(2回)。支配項: WAL グループコミット待ち 累計 264-285 秒
(クライアント時間の ~30%、平均 4.5-5.5ms/回、CHECKPOINT_SECONDS=0 でも
不変=デバイス fsync 固有でエンジン課題ではない)。以下は過去報の
構造レバーの再確認: table_tail 待ち 36s(~3.8%)、index_gate 21s(~2.3%)、
write_intent_conflict 1,289-1,693(全 txn の ~3%、delivery 同士の
同一キュ行)。`tm.index_undo_reserved_pages=0`・
`mvcc.write_intent.wait_timeouts=0`・ゲート long_waits=0 で
長時間稼働ゲージに異常なし。WAL セグメント数は ~40 で定常プラトー
(リサイクル正常)。

**次のレバー:** (a) redo バッチ境界を跨ぐ txn の露出は prescan 修正で
閉じたが、安全網の「バッチ内までのコミットしか見えない」性質自体は
契約として文書化されているべき(将来の redo 並列化時の罠)。
(b) WAL fsync 待ちが支配の環境では、グループコミットのバッチ幅
(`--wal-sync-ms`)の自動調整が次の計測候補。(c) table_tail 待ちの
平均 60ms×608 回(第13報の 7回/394ms から形状が変化)は要観察。

---

2026-10-10 加筆(第27報)。分析SQL完成度パス(第13ラウンド): JSON関数族、
列DEFAULT句、CREATE VIEW永続化+DROP VIEW。詳細は下記「2026-10-10 分析SQL
完成度パス(第13ラウンド)」の節。

## 2026-10-10 分析SQL完成度パス(第13ラウンド)の成果

**実機プローブ(60項目の機能候補を一括検査)から始めた**。検出された欠落の
うち、パーサpinに依存しない3機能を実装した(GROUPSウィンドウフレーム/
EXCLUDE/FILTER句/配列スライス/BETWEEN SYMMETRIC/VALUES多重FROM/UNIQUE・
IDENTITY はパーサ更新待ちとして記録)。

- **[関数] JSONドキュメント関数族を実装**(`expression/json_doc.hpp` 新設)。
  `JSON_TYPE(json[,path])` / `JSON_KEYS` / `JSON_ARRAY_LENGTH` /
  `JSON_SET` / `JSON_INSERT`(既存パスはエラー) / `JSON_REPLACE`(欠落パスは
  エラー) / `JSON_REMOVE`(欠落メンバはno-op、配列位置は境界チェック) /
  `JSON_ARRAY(...)` / `JSON_OBJECT(k,v,...)`(重複キー・NULLキーはエラー) /
  `LAX_INT64/FLOAT64/BOOL/STRING`(数値丸め・文字列パース・bool→1/0。
  変換不能はNULL、不正JSONもNULL=エラーにしない)。rewrite.cppにあった
  JVal/パーサ/パスナビゲータをヘッダへ移動し、AST評価器
  (function_call_expression.cpp)と関係評価器(expression_eval.cpp)と新畳み込み
  規則 `json_family_constant_fold`(全引数定数ならPrepare時に折り畳み)が
  同一実装を共有する。`SQL_ERROR: invalid JSON text` は READ/MUTATE系のみ
  (LAX系はNULL)。TRUE→1 は TO_JSON_STRING と同一のBOOL=INT64表現規約。
  `TINYLAMB` の `unsupported function` から抜け出す結果型は2つのチェーンに
  追加(json_keys→kArray、array_length/lax→kBigInt 等)。回帰:
  `AnalyticalFeaturesTest.JsonDocumentFamilyReadersAndMutators` /
  `LaxCoercionsFollowJsonSemantics` / `SqlAstFunctionSweepTest.JsonDocumentFamily`
  / 関係スイープ25ケース+pin10件 / `ExpressionRewriteTest.JsonFamilyConstantFold`。
- **[DDL/DML] 列DEFAULT句を実装**(プローブで「解析はされるが黙ってNULL」の
  黙った意味変更を確認)。定数式はCREATE時に評価して
  `Constraint::kDefault`(当初からあった未配線スロット、Valueペイロード付き)
  に格納 → **カタログ永続化はフォーマット変更ゼロ**。INSERTは
  (a) 列リスト省略列、(b) VALUES内DEFAULTキーワード(ビジターが
  `__column_default` マーカーに下げ、実行時に目標列の既定値へ解決。plan-cache
  shapeはマーカー入りで不発=authoritative経路)、(c) INSERT...SELECT の
  列リスト、すべてで既定値を適用。UPDATE SET col = DEFAULT も解決。
  **未宣言列のDEFAULT→NULL(エラーなし)** がBigQuery正規挙動
  (dml_*コンプライアンスpinと一致——最初エラー実装で7本落として修正)。
  揮発式(RAND)・サブクエリはCREATE時に明示拒否。SimpleColumnSchemaの
  構造子(PathExpression/TypeParameterList=VARCHAR(10)/ColumnAttributeList/
  Collate)をDEFAULT式と区別してskip(TypeParameterList漏れで VARCHAR(10)
  テストが全滅した事件あり)。PK/FKとの同列併用は単一スロット設計により
  拒否(パーサも隣接を拒否)。`__column_default` がINSERT VALUES以外に
  漏れると unsupported function で fail-visible。回帰:
  `QueryTest.ColumnDefault*` 4件(省略列/DEFAULTキーワード両形式/再開後
  永続化/揮発拒否)。
- **[DDL] CREATE VIEW をカタログ永続化、DROP VIEW を実装**(旧挙動は
  セッション限定TEMPビューで、再オープン後に「table not found」)。
  `Database::kViewKeyPrefix` に定義テキストを永続化(MVと同一経路)+
  改行連結の名前インデックスレコード(範囲スキャンAPIを追加せず列挙)。
  `SqlEngine::EnsureViewsRegistered` が Prepare 冒頭でスキーマエポックを
  比較し、永続定義をセッション ViewRegistry へ再シード(静穏時コストは
  エポック比較1回)。FROM参照時展開なのでベース表に追従、ビュー連鎖可、
  INSERTは不変(NotExists)。OR REPLACE対応、表/MVとの名前衝突は
  「already exists」、DROP非存在は「does not exist」。**訪問者側の即時
  ViewRegistry登録は廃止**——失敗/ロールバック後もthread_localに残り、
  後続のCTE名(`plain`)と衝突する事件の根治。登録は文実行(コミット)時
  のみ。回帰: `QueryTest.ViewPersistsAcrossRestartAndDropsCleanly` /
  `ViewOrReplaceAndNameConflicts`。
- **[調査・記録] TPC-H SF1 ベースライン再計測**(変更後の現行ツリー、
  taskset 16-31、gold複製): 合計≈10.9s。上位は Q9 2208ms /
  Q21 2074ms / Q18 1145ms / Q7 724ms / Q13 598ms。Q21は第12ラウンドの
  兄弟集約融合により scan_rows 7.5M(≈lineitem 1.25回分)まで縮退済みで、
  第11報「EXISTSビルド共有の正準レイアウト」レバーは役目を終えた。
  残存fatは構造的(Q9=4-hop materializing join鎖のstar-join融合、
  Q18=6M行GROUP BY自体)。
- **[事故と教訓]** セッション途中の再ビルド後、旧バイナリがprobe.dbに
  焼いたプランキャッシュが再生され「フィルタ定数を変えると一致行が
  文の実行順序と同期する」という幻のバグが数分だけ再現(新鮮DBコピー+
  単文プロセスで解消=汚染確認)。**A/B前のgrep確認に加え、プローブDBは
  バイナリ更新のたびに作り直す**こと。

**検証:** 全体ctest **3904/3904合格**(意図的skip 3)。`check_layering.py`
成功(allowlist 65辺のまま)。触ったファイルはclang-format済み。
TPC-DS SF1 94q は failed=0(合計 3435.6ms、`base1010/tpcds_base.txt`)。

---

2026-10-09 加筆(第26報)。分析SQL完成度パス(第12ラウンド): 派生表集約統合
(TPC-H Q21の本丸レバー)+ BytecodeCompilerへのCASE下げ+ CREATE/DROP
MATERIALIZED VIEW + テーブル関数(TVF)参照。詳細は下記「2026-10-09 分析SQL
完成度パス(第12ラウンド)」の節。

## 2026-10-09 分析SQL完成度パス(第12ラウンド)の成果

**計測(TPC-H SF1 gold)から始めた**: Q21 2022msの支配は既知の
`aggregate_input_rows=9,794,511`(multi/only_late 2派生表の2重集約)で、
第20報の「次のレバー」をそのまま着手した。

- **[planner] 派生表集約統合(sibling derived-aggregate fusion)。**
  同一ベース表+同一GROUP BYキーリストを持つ兄弟派生表を1つの集約へ融合する
  (`FuseSiblingDerivedAggregates`、`query/sql_engine.cpp`。ルーティング前の
  kSelect書き換えチェーン、DecorrelateSingleTableLateralsの直後に配置)。
  書き換え形:
  `(SELECT k,a1 FROM t WHERE p1 GROUP BY k) d1, (SELECT k,a2 FROM t WHERE p2
  GROUP BY k) d2` → `(SELECT k AS __fk0, <p1?a1> AS __fa0_0, <p2?a2> AS
  __fa1_0, COUNT(CASE WHEN p1 THEN 1 END) AS __fw0 ...) __fagg GROUP BY k`,
  外側WHEREにメンバーごとのHAVING(融合列参照へ置換)とウィットネス
  `__fw > 0` をANDで追加。正しさの契約:
  (a) CASE包みはメンバーシップ集約(COUNT/SUM/AVG/MIN/MAX、DISTINCT含む)
  すべてで完全(NULLスキップがCASEの非該当行を落とす=メンバーのWHEREフィルタ
  と同一)。実機でAVG(all-NULL CASE)=NULL(ゼロ除算なし)も確認済み。
  (b) グループの存在はCASE包みでは運べない(COUNT=0が「該当行なし」と
  「x全部NULL」を区別しないため)、WHERE付きメンバーは明示ウィットネスを
  必要とする。(c) HAVINGのみに現れる集約(Q21の
  `HAVING COUNT(DISTINCT l_suppkey)`形)は融合列として公開してから持ち上げる。
  全ての派生片を事前組み立てし、全チェック通過後に初めて文を改変する
  all-or-nothing。裸名メンバー参照・外側SELECT star・サブクエリ入り外側は
  不発(黙った意味変更の防止)。**ApplyOrderByが射影後の出力スキーマで
  評価される**ため、ORDER BY内のメンバー参照は同一テキストのSELECT項の
  出力名へ写像する(一致がなければ不発)。
  `TINYLAMB_FUSE_SIBLING_AGG=0`でA/B。カウンタ
  `SqlEngine::LastFusedSiblingAggregates()`。回帰:
  `SiblingAggregateFusionTest` 9件+`QueryTest.SiblingDerivedAggregatesFuseIntoSingleScan`。
- **[expression] BytecodeCompilerがCASE(検索形)を下げるようになった。**
  融合後の集約引数が型付き並列foldに乗るための前提工事だが、効果は汎用:
  `SUM(CASE WHEN ... THEN ... END)`のTPC-DSピボット族がバイトコードVMで
  走る。下げ形は 条件; JumpIfTrue→THEN; Pop の連鎖+ELSE/NULLフォールバック
  (新命令kPop、条件ジャンプはpeek型なので両経路で明示pop)。三値論理・
  評価順エラー伝播はTryPickBranchと一本一致( `differential_test` でpin)。
  ついでにkUnaryExpのコンパイルゲートを緩和:
  IS [NOT] NULL / IS [NOT] {TRUE,FALSE} は型非依存(両インタプリタの
  カーネルが元々任意型に対応)で、VARCHAR列へのIS NULLがバイトコードに
  乗る。回帰: `BytecodeTest.SearchedCase_LowersToConditionalJumpsAndMatchesAst`。
- **[DDL] CREATE/DROP MATERIALIZED VIEW を実装**(パーサは
  `CreateMaterializedViewStatement[is_if_not_exists]` /
  `DropMaterializedViewStatement` を出す)。結果を同名カタログ表へ実体化
  (CTASと同一のスキーマ導出/型コヒーション経路)し、定義SELECTテキストを
  functions木の予約プレフィックス鍵
  (`Database::kMaterializedViewKeyPrefix`)に永続化。問い合わせは通常の表
  スキャンとして答え(鮮度は手動: 自動増分リフレッシュは将来枠)、
  `DROP MATERIALIZED VIEW` は表+定義を片付ける(非MV表へのDROPは拒否)。
  IF NOT EXISTSは既存定義を保持するno-op。再起動後も行+定義が生存
  (`matview_test` 4件: 往復/名前規則/永続化/DROP+CREATEによる手動リフレッシュ)。
  REFRESH構文はピン留めパーサが非対応(将来のparser更新待ち)。
- **[フロントエンド] テーブル関数(TVF)のFROM参照を実装**。
  `CREATE TABLE FUNCTION` が本体Query ASTをUdfRegistryへ保存するようにし、
  `SELECT * FROM tf(arg, ...)` が呼び出し側引数で本体を展開
  (`SubstituteInSelect` でパラメータ名→定数を置換)。引数は文境界で定数に
  限る(揮発関数は明示拒否 — RAND()1回分が展開に凍結されるのを防ぐ)。
  末尾DEFAULT引数のパディングはスカラーUDFと同一規則。未知関数・非定数引数・
  引数過多は明示的エラー。回帰:
  `GoogleSqlAstTest.TableValuedFunctionExpandsStoredBody` +
  `QueryTest.TableValuedFunctionInvokesStoredBody`。
- **[測定環境の事故と回避(記録)]** 定期チェックポイント有効
  (CHECKPOINT_SECONDS=5)×8並列ローダーのSF1ロードは、再オープン後に
  `full scan reached a non-row page` + 行数欠落(6,001,215→800,000)を
  再現した(既知のcheckpoint×並列append領域。第14ラウンドの実験アーティ
  ファクト noanalyze.db も同形状)。**ゴールドDBは CHECKPOINT_SECONDS=0 +
  --load-workers 1** で作成(6,001,215行を再検証済み)。
- **[測定] TPC-H SF1 gold、taskset 16-31 交互ペア(並走プロジェクトの
  ヘルススイートが全コアを占有する高負荷帯。±5-10%の実行間揺らぎ):**
  **Q21 は静穏窗の2ペアで 1515 vs 2129ms(−28.8%) / 1689 vs 1993ms
  (−15.3%)**、プロファイルは `aggregate_input_rows` 9,794,511→6,005,442、
  `scan_rows` 13.5M→7.5M(3 lineitemスキャン→2へ)。スイート合計は
  交互2ペアで 45.5/48.9s vs 54.5/45.9s(平均−6.0%、負荷揺らぎ内)。
  非融合クエリの対照(Q9: +5.7%/−0.4%、Q18: +3.6%/+7.2%)はノイズレベルで
  平坦=融合の副作用なし。高負荷窗のペア(Q21 +7.4%)は捨てた。
  TPC-DS 94qは決定論15/15(融合+CASE bytecodeの全経路で行数一致)。

**検証:** query(180)/matview(4)/sibling_aggregate_fusion(9)/
googlesql_ast(47)/googlesql_frontend(28)/differential(15)/bytecode(11)/
executor(198)/merge(16)/alter_table(6)/catalog(35)/sql_dml_paths(5) 全パス、
TPC-DS 94q決定論 15/15、`check_layering.py` 成功(allowlist 65辺のまま)、
触ったファイルは clang-format 済。全体ctestは最終検証で実施。

---

2026-10-09 加筆(第25報)。トランザクションエンジン強化(第14ラウンド):
abort-undoの一時的読み取り失敗への有界リトライ+完全診断、アボート理由の
分類計測、そして**3つの並行性レバーの測定による棄却**(DELETEスキャンの
write-intent待機化・deliveryの0行許容・予約拒否チェックのページスコープ化
——いずれもTPC-C W=1のA/Bで100倍級のスループット崩壊または中位数2割減を
確認し、現行のグローバルdrain均衡がロードベアリングであることを実証)。
WAL破棄は持続負荷ソークで再実証(セグメント299→~40でプラトー、検証5群
PASS)。詳細は下記「2026-10-09 トランザクションエンジン強化(第14ラウンド)」
の節。

## 2026-10-09 トランザクションエンジン強化(第14ラウンド)の成果

**[堅牢化] abort-undoの「unreadable log」一時窓への有界リトライ+完全診断。**
2026-10-08 health passで隔離されていた負荷時フレーク
(`BPlusTreeTest.LiftUpBranchWithFosterOther`がフルスイート-j32で1/3回
"Abort undo: unreadable log"→fail-stop)に対し、`read_undo_record`を
「失敗→(volatileならグループコミット待ち→再読み)→最大8回×5msの
有界リトライ」に強化した。一時的にどの媒体にも載っていない瞬間(リングと
セグメントファイルのhandoff窗口)を吸収してWARN 1行で続行し、永続破損は
従来通りfail-stopする。DIAG2は失敗モード(`DebugReadStatusFor`)と
buffered/durable/flushed/ring_floorを記録するよう拡張
(`Logger::RingFloorLSN`・`RecoveryManager::DebugReadStatusFor`を新設)。
静的解析で読み出し経路の全レース候補を消去した結果、実行時に「永続的に
読めない」経路は確認できておらず、リトライで吸収されなかった場合は
DIAG2の証拠が次の一手を与える。

**[計測資産] TPC-Cベンチにエンジンアボートの理由分類を追加。**
`abort.write_intent_conflict / abort.write_intent_timeout /
abort.index_undo_drain_timeout / ...`カウンタ。実測(W=1/16cl):
アボートの92.5%が`write_intent_conflict`(delivery同士の同一キュー行)、
drainタイムアウトは数千回に3回と稀だが1回あたり30ステートメントを無駄にする。

**[並行性] 3レバーの測定的棄却(インターリーブA/B、すべてW=1/16cl/seed42)。**

1. **DELETEスキャンのwrite-intent待機化**(`wait_for_write_intent_=true`):
   tps 1,213→12。待機中のスキャンがインデックスread guardを保持し続け、
   deliveryのコミット(数十ステートメント)を待つ間に全スプリットが閉塞→
   districtインテント保持が延び→5秒ハッチのタイムアウトが全域に連鎖。
2. **deliveryの0行削除許容(競合済みキューを区画スキップ)**単独:
   tps 1,156→9。loserが即死しなくなるとdeliveryの生存期間が伸び、
   そのインデックスアンドゥ予約(マージ由来の~16KB)が全RMWのdrainを
   誘発(waiter-of-waiterのdrain連鎖をWARNで確認)。
3. **予約拒否チェックのページスコープ化+pre-drain撤去**: tps 12(崩壊)。
   ゲート自由のpre-drainがゲート保持時間を守るロードベアリング部品と判明。
   pre-drainを戻してスコープ化のみでも中位数−24%(高速なtry-lock回転が
   失われるため)。**結論: グローバルチェック+drain均衡は保守ではなく
   設計**。将来のレバーは予約アドミション契約ごとの再設計
   (per-page admission key、オーナー順drain)であって、拒否サイトの
   局所化ではない。

**[WAL破棄] 持続負荷での再実証(変更不要の再確認)。**
`TINYLAMB_LOG_SEGMENT_BYTES=1MiB TINYLAMB_CHECKPOINT_SECONDS=2`で
W=2/16cl/60s: セグメント数は初期299→~25-56で定常プラトー(保持~40MiB)、
LSNは1.6GB超まで進行。終端検証5群PASS。4項フロア+
`lowest_segment_`カーソル+境界バリデータの現行設計が正しく機能している。

**[検証]** 影響スイート(recovery/transaction/page/table/index系467件)green、
`check_layering.py`成功(allowlist 65辺)、clang-format済。
中間状態でのA/B再実行時に、リバート漏れ(フラグ行の消え残り)により
ウェッジを誤因づけした事件あり——**バイナリA/Bの前には必ず挙動を決める行
(grep)で差分を確認する**こと(本報の教訓)。

**次のレバー:** (a) deliveryストームの構造対策はベンチ形状(16クライアント
>10区画、複数deliveryが同一最古行を狙う)に由来し、エンジン単独では
解けない。(b) indexゲート(~5%)の緩和は共有→排他昇格の構造変更
(第13報記録のまま)。(c) フレークのDIAG2証拠待ち(再発時の解析手順は
read_undo_recordのWARNが出す)。

---

2026-10-08 加筆(第24報)。SQL完成度パス(第11ラウンド): UPDATE ... FROM、
ALTER TABLE (RENAME TO / RENAME COLUMN)、USER()/SESSION_USER()、相関EXISTS
ビルドの兄弟共有。詳細は下記「2026-10-08 SQL完成度パス(第11ラウンド)」の節。

## 2026-10-08 SQL完成度パス(第11ラウンド)の成果

**実機プローブで機能ギャップを再棚卸しした**(スカラー関数面はほぼ完成:
ARRAY_AGG/PIVOT/pipe/窓関数/JSON/日付系/GROUPING SETSすべて動作。
残存は DML/DDL: UPDATE ... FROM、ALTER TABLE、CREATE MATERIALIZED VIEW)。

- **[フロントエンド] UPDATE ... FROM を MERGE へ脱糖して実装**
  (`query/googlesql_ast_visitor.cpp` VisitUpdate)。従来ビジターは
  FromClause を「認識して無視」していた(静黙の意味変更の恐れがあった)。
  `USING <from> ON (<where>) WHEN MATCHED THEN UPDATE SET ...` へ変換し、
  MERGEの1ターゲット行=1アクション契約が BigQuery の複数源行マッチエラー
  と一致、FK強制・plan-cache・サーバ経路を共用。ターゲット別名付き/
  ネストDML付き/ASSERT_ROWS_MODIFIED付きは明示的拒否。
  `MergeStatement::DesugaredFromUpdate()` マーカーで診断・サーバタグは
  "UPDATE n" を報告(plan-cache再バインドとQueryResult型にも配線)。
  回帰: `MergeTest.UpdateFrom*` 5件。
- **[フロントエンド/DDL] ALTER TABLE の RENAME TO / RENAME COLUMN を実装**
  (`VisitAlterTable` + `Database::RenameTable/RenameColumn`)。
  両者ともカタログレベル操作(Table値を新スキーマで再シリアライズ。
  行データ・インデックスページは無改名)。RENAME COLUMNは列INDEXキーの
  統計を保持、RENAME TOは統計を新名へ移し、FK親の改名を拒否(DropTableと
  同一の参照走査)、既存名(大文字小文字無視)と衝突したら kConflicts。
  Schema::WithName/WithRenamedColumn を新設(type/schema)。
  ADD/DROP COLUMNは行レイアウト変更(表再構築パス)が必要なため明示的
  エラー(旧来は文種ごと unsupported だったのを fail-visible に改善)。
  回帰: `query/alter_table_test.cpp` 6件(移行・旧名解消・永続化・
  FK親拒否・重複名拒否・行レイアウト操作の明示エラー)。
- **[関数] USER()/SESSION_USER() を実装**(両評価器: AST
  `function_call_expression.cpp` + 関係 `expression_eval.cpp`)。
  セッション層はないため OS アカウント(USER/LOGNAME 環境変数、
  既定 "tinylamb")を返す。
- **[executor] 相関EXISTSの兄弟ビルド共有**
  (`executor/detail/subquery_runtime.cpp`)。同一ベース表・同一単一
  INT64/DATE相関列の exists-only サブクエリ間で、`int_rows` キー付き
  ビルドを1本に共有(発行者はビルド述語なしのときのみ公開。消費者は
  自分の残余連言全体をプローブ時評価に繰り延べる)。ドメインフィルタ
  付きビルドは同一登録コンテキスト(outer relation + serial 一致)での
  み共有。鍵は素の列名シーケンス(兄弟は各自の別名で同じ物理列を
  修飾するため)。`shared_exists_build_reuses` ゲージをプロファイルに
  追加、`TINYLAMB_DISABLE_SHARED_EXISTS_BUILD=1` でA/B。
  回帰: `SqlSubqueryShapesCoverageTest.SiblingExistsBuildsShareOneScan`
  (EXPLAIN ANALYZE の reuses=1 を pin)。
  **限界(記録):** Q21 の EXISTS/NOT EXISTS は要求列が違う
  ({ord,sup} vs {ord,sup,amt})ため共有が発火しない。発火条件は
  「同一列シーケンスのロード」で、Q21を救うには共有ビルドの正準
  レイアウト(全列ロード)か兄弟間のunion射影が必要 — 次回のレバー。
  Q21の実測支配はEXISTS/NOT EXISTS 2ビルドの lineitem 再スキャン
  (cold: 9.79M行の集約入力、warm: ドメインフィルタ後も2回のビルド
  スキャン)であり、単一パス統合が引き続き本命。
- **[計測] TPC-H SF1 gold 再計測(変更後)**: Q9 2408 / Q21 2128 /
  Q18 974 / Q13 580 ms、全22クエリの行数が変更前と一致(回帰なし)。

**検証:** differential(15)/sql_relational_eval_coverage(6)/schema(8)/
column(6)/catalog(35)/database_extra(4)/googlesql_frontend(28)/query(178)/
sql_subquery_shapes_coverage(11)/merge(16)/alter_table(6)/
executor_feature(91) 全パス。`check_layering.py` 成功(allowlist 65辺の
まま)。触ったファイルは clang-format 済。

---

2026-10-08 加筆(第23報)。トランザクションエンジン強化(第13ラウンド)。
マルチクラッシュ後のページラッチウェッジ(第11/12ラウンドのP1隔離)を
stale foster根治として解消し、コミット済み値の複数パス生存をpin。テール
成長待ちの初計測を実施。詳細は下記「2026-10-08 トランザクションエンジン
強化(第13ラウンド)」の節。

## 2026-10-08 トランザクションエンジン強化(第13ラウンド)の成果

**計測と実機解剖から始めた。** 第12ラウンドが `TINYLAMB_STRICT_WEDGE=1`
で隔離していたウェッジ("failed to latch page 6: Resource deadlock
avoided")をgdbで解剖し、2層修正+新規リグレッション2本を追加した。

- **[P1根治] マルチクラッシュ後のstale foster —— ラッチウェッジの根因。**
  再現の実体: pass 2リカバリ後、ルート葉(page 5)のfosterリンクが
  「同一リカバリで解放済みのページ6」を指したまま生存し、最初のINSERTの
  `GrowTreeHeightIfNeeded`が (1) 高さ成長の兄弟ページをフリーリストhead
  (=6)から再発行、(2) 同一ページ6をfoster子としてGetPage —— 同一スレッド
  二重ラッチ(EDEADLK)。ラッチが無くても新ルートが同一ページを二重参照する
  破綻を踏む。fosterが生き残った経路: loserのSetFoster物理undoが
  エビデンス/型規則でスキップされた後、**torn掃除のSPRがbody復元アンカー
  (in-place型変更のkCompensateAllocPage)を再生してfosterを復活**させる
  —— undoより後の再構築が、undoの成果を上書きする第11ラウンド「body復元
  蘇生」の別顔。修正は3層:
  1. **stale routing-link掃除の配置修正と拡張**(`RecoverFrom`尾部):
     セパレータ掃除(kInsertBranch)をundo直後から**torn掃除+フリーリスト
     再構築の後**(ページの生死が最終確定する唯一の時点)へ移動し、同形状の
     **foster掃除(kSetFoster、親は葉でも可)**を新設。子がFreeで現リンクが
     正確に一致する死んだリンクだけをCLR先出しで吸収する。あわせて分析窓の
     **チェックポイント以前の事前走査**を追加(チェックポイントより前の
     kSetFoster/kInsertBranchはFPWに写るがレコードが分析窓の外だった)。
     コミット済みtxnのリンクは子がFreeにならないため過剰収集は無害。
  2. **実行時自己修復**(`GrowTreeHeightIfNeeded`): foster子が
     生きた葉/枝ページでなければ(旧WALのDB・予見外経路の防御)fosterを
     吸収してから成長する。リカバリ掃除が発火するようになった後も二層防御
     として維持。
  3. テストの隔離を解除: `PartialLoserUndoThenAnchorRestoreKeepsIndex
     Consistent` のINSERTが全停止点で成功することを無条件にpin
     (`TINYLAMB_STRICT_WEDGE`は撤去)。
  回帰: 同テスト5停止点+`swept ... loser foster link(s)` WARNが全passで
  発火(実行時吸収は0=リカバリ側で完治)。recovery_extra 20件・
  recovery/transaction系12スイート全green。

- **[リグレッションピン] コミット済み値の複数パス生存。**
  `CommittedValuesSurviveRepeatedInterruptedRecovery`(新設): loserの
  分割窓の**途中に**コミット済みtxn(INSERT+UPDATE)を割り込ませ、
  undo停止→torn→全再生→クリーン再起動の4パス×5停止点で、行数に加えて
  **UPDATE/INSERTの値**を検証する。soakで見えた「orders行は生きているのに
  district更新だけ消える」形状(ghost entry)の決定的unit pin。現行ツリーは
  green(第12ラウンドまでのL2/L3/floor修正の効果を確認)。
  並行variantは試みたが撤去: 長時間txn(203ページ分のindex-undo予約)が
  同一ページに当たる他書き込みを正しく拒否するのは予約アドミッションの
  設計契約(保守的拒否+排水)であり、テスト形状が契約と衝突しただけ。

- **[並行処理] テール成長待ちの初計測**(第9報「テール成長結合」の
  実体把握)。`ReserveTableTail`/`WaitForTableTailPage`に>1msサンプリング
  計装を追加(ゲートと同一方式、`wait.table_tail.*`として
  --profile-waitsに出力)。TPC-C W=1/16cl/20s: **7回・累計2.8秒・平均
  394ms** —— 稀だが長い。テール所有権は成長者のコミットまで解決されない
  ため、1人の遅い成長者がページ境界の追い越し待ちを convoy する。
  クライアント時間の~0.9%で構造改善の優先度は中。あわせてゲートを再計測:
  競合7,222回・累計16.9秒(クライアント時間の~5.3%、平均2.34ms、
  long_waits=0)。スキャン側は `IndexKeysMayBeStale` カウンタ主導の
  フォールバック設計で、ゲート排他の実質はDDL排除と構造操作の直列化
  ——「変異を共有に緩める」改善は共有→排他昇格のデッドロック回避が前提の
  構造変更であり、別セッション規模と判断(記録のみ)。

- **[監査済み(変更不要)] チェックポイント後のWAL破棄。** 4項フロア
  (begin LSN / 最古DPT recLSN / 最古アクティブtxn first LSN / 未fsync
  FPW)+`lowest_segment_`カーソル+境界バリデータ+保持WARNのピン名表示を
  再監査。リサイクル系16テスト+持続負荷リサイクル回帰がgreen。リカバリ
  側の新掃除パスは`LogStart`(リサイクル床)以降のみを読むため相互作用なし。

**検証:** recovery_extra(20) / recovery_manager(85) / checkpoint(16) /
logger(18+13) / transaction(35+16) / lock_manager(6) / b_plus_tree(70) /
table(47) / page_manager(26) / database_extra(4) 全green、
`check_layering.py` 成功(allowlist 65辺のまま)、clang-format 済。
新規/変更テストの10連回しでフレーキー 0。sql_oracle/sql_sessionファザー
(各3ワーカー、約40分)で新規再現ファイル 0。TPC-C 対測(seed 42):
変更前 1,574.7 → 変更後 1,547.5 tps(−1.7%、実行間ばらつき ±2.5~13% 内。
計装は>1msサンプリング、掃除はリカバリ時のみでホットパス不変の設計)。

---

2026-10-07 加筆(第22報)。健全化パス: 第10報テストが最後に抱えていた
**コミット済み行の巻き込み欠落**(第11ラウンドのstale separator修復後も
`PartialLoserUndoThenAnchorRestoreKeepsIndexConsistent`のpass-3カウントが
2/3になっていた形状)を根因特定・修復し、厳格警告・clang-tidy・フォーマット
を0化、ついでにMERGEのplan-cache/server経路の欠落を埋めた。詳細は下記
「2026-10-07 健全化パス(第12ラウンド)」の節。

## 2026-10-07 健全化パス(第12ラウンド)の成果

**[P0根治] loserロールバックの頁解放がコミット済み行を巻き込む欠落。**
第11ラウンド修復後も同テストのpass 3が `CountRows==2`(期待3, 'after'消失)で
落ちていた形状の根因。WALレコード単位の解剖(捨てリプロ+`w23_wal`)で確定:

- 時系列: loser txn がリーフ7を分割で獲得 → 自DELETEのマージで7を破壊 →
  pass 1/2のundoが7を復元(CompensateDestroyPage) → **その後、別のコミット済み
  txnが7へ 'after' を挿入** → pass 3のundoがchain残部を消化し、split移動の
  undone(base行を7→5へ戻す)ののち、**7の割当undo(CompensateAllocPage)が
  内容に関係なく7を解放** —— 'after'はloserのどの操作にも対応しないため
  誰も移動せず、ページごと消えた。
- 本質: **リカバリ再開後の世界では、loserのロールバック再開時点でそのページへ
  コミット済み挿入が横入りし得る**(ランタイムのアボートはゲート下で分割され
  ないため起きない、リカバリ固有の欠陥)。alloc系undoの「ページ解放」は
  ページが空である前提の操作で、その前提が崩れている。
- 修正(`UndoLoserChains`): kSystemAllocPage undoの適用前に
  `HasLiveEntries`でページ内容を検査し、非空なら**解放を全chain終了まで延期**
  (`deferred_frees`)。全body復元(in-placeルート成長の巻き戻し)が終わった
  後で、生存エントリを定着済みspineリーフへ `kCompensateDeleteLeaf`(redoが
  Insert適用)としてCLR先出しで再挿入し、その後に解放を実行する。
  CLRと解放の間で落ちても次パスはkDuplicatesを成功扱いし冪等。spineが
  見つからない/非リーフ/挿入不能のときは**ページを解放せず確保し続ける**
  (データ保全優先のavailability floor逆転、WARN明示)。
  再試行経路の型不一致(再発行済みページ)は従来のstale経路のまま。
- 回帰: 同テストが全停止点でgreen(pass 3カウント3、'after'到達可能)。
  `recovery_extra_test` 19件・recovery/transaction系8スイート335件 green。

**[機能隙間] MERGEのplan-cache再バインドとサーバコマンドタグ。**
第20報のMERGE実装で`StatementType::kMerge`が増えたが、
`sql_template.cpp`のバインドswitch(プリペアドテンプレート再バインド)と
`postgres_server.cpp`のCommandTag switchが未対応のまま:
plan-cache経由のMERGEは`bound`がnullのまま落ち、postgres経路のMERGEは
タグが"OK"になっていた。それぞれMERGE文の再バインド(source/ON/clause毎の
condition・SET・insert値)と`MERGE <n>`タグを追加。厳格警告(-Wswitch)0化の
過程で検出。

**[衛生] 厳格警告・tidy・フォーマット0化。** clang(pedantic+conversion)で
検出された5件を修正(-Wreorder-init-list / -Wsign-conversion /
-unused-parameter / -Wswitch×2)。clang-tidy全396TUを--fixパス→残り手直し
(`misc-use-internal-linkage`/`performance-no-automatic-move`/
`readability-container-contains`/`clang-analyzer-core.NullDereference`
 — row.cppのunsigned_flagsはkNoSlot不変条件の解析不能性をベクタ直接アクセス
で解消)。触及ファイルはclang-format済。googletest(_deps)由来の
suspicious-includeはゲート外。

**[並行セッション調整(記録)]** 本パス中、別セッションがscan_filter等を
編集(第20報のMERGE/第21報のperf)。`executor/detail/scan_filter.cpp`への
tidy 1件修正は同セッションの06:19編集後に最小適用。ctest 2重起動による
BAD_COMMANDアーティファクトが1回発生(最終検証は単独実行で実施)。


2026-10-07 加筆(第21報)。トランザクションエンジン強化(第11ラウンド)。
kill -9後のstale separator(第10報のP0残課題)を根因から修復し、torn再構築後の
undo/redo再挿入をavailability floor化、indexゲート待ちを計測。詳細は下記
「2026-10-07 トランザクションエンジン強化(第11ラウンド)」の節。

## 2026-10-07 トランザクションエンジン強化(第11ラウンド)の成果

**計測(実機アーティファクトの解剖)から始めた。** 第10報のkill -9検証で
FAILしたTPC-C DB(`.soak2/s2.db`)をWALパーサとページ構造プローブで解析し、
「descent routed to a non-branch page」の根因を特定・修復した。

- **[P0根治] kill -9後のstale separator — body復元アンカーとundo
  エビデンスの相互作用。** リーフ分割でルートbranchに挿入されたloserの
  セパレータ(kInsertBranch)のundoが、複数回のクラッシュ間で
  「page incarnation changed」の型チェック skip によって物理適用されない
  一方、子ページの割当undo(CompensateAllocPage)は無条件に走り、
  「生きたbranchが解放済み子を指す」形で確定していた(実アーティファクト:
  ルート1ページが115本のFreePage経路を保持、WAL窓にルートの構造レコードは
  FPWの1件のみ)。トリガーは**body全体を復元するSPRアンカー**
  (kFullPageImage / kCompensateDestroyPage / in-place型変更の
  kCompensateAllocPage): 前回パスのCLRが物理適用済みでも、その復元がCLR以前の
  内容(セパレータ)を蘇らせ、stale_undoエビデンスが再削除を封じる。
  修正は2層:
  1. **エビデンス無効化**(`UndoLoserChains`): body復元アンカーが適用された
     ページでは、アンカーより古いCLRエビデンスを無効化してundoを再試行する
     (`RecoverBrokenPages`がアンカーLSN地図を収集)。alloc系undoはこの経路を
     通らないため、再発行済みページの再解放(同チェックの本来の守り)は不変。
     物理適用はLogUndoのページ型チェックが守る。
  2. **セパレータ掃除パス**(`RecoverFrom`尾部): undo後時点で「loserの
     kInsertBranchが指す子がFree・親が生きたBranch・セパレータ実在」の
     3条件を満たす経路を削除(CLR先出しで冪等)。
  回帰: `RecoveryExtraTest.PartialLoserUndoThenAnchorRestoreKeepsIndexConsistent`
  — 1トランザクションでインデックスを成長させ(太いVARCHARキーで分割)、
  `TINYLAMB_TEST_UNDO_STOP_AFTER=N`(テスト用フック: loser undoをN件で停止 =
  undo途中のkill -9)でパス1を中断し、branchページを破壊してパス2を走らせる。
  N=1..8の全停止点で、修正前は**テストがハング**(exit 124、3分タイムアウト
  で確認)、修正後は全点で読み取り一貫性+パス3冪等がgreen。

- **[P0根治] torn再構築後のundo/redo再挿入が開口を失敗させる経路の全面
  floor化。** 実アーティファクトで、未floorの再挿入が`kNoSpace`/`kTooBigData`
  を返し**リカバリ全体が失敗して2度と開かない**実害を確認(第10報の
  leaf/branch版に追加で3経路):
  - `LogUndo` kDeleteRow(行ページの再挿入。deliveryの削除undoが満杯行ページに
    当たる)、kSetFoster(復元されるfoster対が現在のページより大きい)。
  - `LogRedo` kCompensateDelete{Row,Leaf,Branch}の再挿入
    (`RedoCompensateReinsert`新設): マルチパスのアンカートリムで、バイトを
    解放したはずの削除がアンカーより下に切られ、その補償再挿入だけが
    アンカー上で再生されて満杯に当たる。forward(committed)挿入は従来通り
    fail-closed。
  効果: 実アーティファクトが「開かない(NoSpace)」から「開く+444件の境界付き
  損失WARN」に転換。損失は1 loserエントリ単位でログに明示される。

- **[並行処理] indexゲート待ちの計装(第9/10報からの持ち越しの前提工事)。**
  `AcquireIndexWriteGuard`にサンプリング計装(1ms超の待ちのみ記録、無競合の
  大多数はアトミック代ゼロ)を追加。TPC-C W=1相当・16クライアント・60秒の
  初計測: **contended 7,385回 / 累計19.2秒 / long_waits(1秒超)=0**。RMW全体の
  ~1.6%が平均2.6ms待つ形で、write-intent競合(0.11%)とともにゲートは
  支配ボトルネックではない——**ゲートフリーRMW再構築の優先度を下げる判断
  データ**が出た。ゲージは`tm.index_gate_*`と`wait.index_gate.*`として
  `--profile-waits`に出力。

- **[検査済み(変更不要)] チェックポイント後のWAL破棄。** 4項フロア
  (begin LSN / 最古DPT recLSN / 最古アクティブtxn first LSN / 未fsync FPW) +
  `lowest_segment_`カーソル+境界バリデータを再監査し、リサイクル系16テスト
  (持続負荷リサイクル回帰を含む)がgreenであることを確認。

- **判明した既知課題(未修正・次回最優先P1): 複数回クラッシュ中断リカバリを
  挟んだコミット済み更新の部分消失(ghost entry)。** kill -9ソーク
  (`.fuzzscratch/w23soak`)で、(a) リカバリが繰り返し途中死したDBでは
  loser挿入の一部がインデックス+行にゴーストとして残存(orders 4/9行、
  districtのnext_o_idは未進)、(b) クリーンな単一killでも2回目の検証以降、
  コミット済みnew_orderのorders行が生存しつつdistrict更新だけ消失する形状を
  確認。コミット済みトランザクションの効果の部分消失であり、マルチパスの
  エビデンス/物理適用の断絶が別の顔で出ている。再現一式(アーティファクト、
  検証手順、WAL解析方法)は`.fuzzscratch/w23soak`と`.fuzzscratch/w23v`に保存。
  本ラウンドのL2/L3/floorはこの族の既知メンバー(body復元蘇生・アンカー越え
  再挿入)を塞いだが、この形状の根治には次の一手が必要:
  複数回パスを確定的に作れるunit再現(3パス以上)を`recovery_extra_test`に
  固定し、CLR発行と物理適用の差分をレコード単位でダンプして特定すること。
  付随して、このシェイプで最初の書き込みが同一スレッドのページラッチ
  ウェッジ("failed to latch page 6: Resource deadlock avoided"、ラッチ取得は
  全経路で収支均衡を確認済み)に当たる別バグも固定済み(同一再現テスト、
  `TINYLAMB_STRICT_WEDGE=1`でテストが失敗扱いになる)。

- **並行セッション調整(記録):** 本ラウンド中、別セッションのMERGE実装が
  `query/statement.hpp`を一時的に壊した(DeleteStatementが置換され、
  テール重複)。DeleteStatementをHEADから復元し、孤立テールを除去して復旧
  (MERGE追加は保持)。同セッションの第20報と本報はファイルの先頭で共存。

**検証:** recovery_extra(19) / recovery_manager(85) / transaction(35) /
transaction_extra(16) / checkpoint_manager(16) / logger(18) / log_record(32) /
b_plus_tree(70) 全green、`check_layering.py` 成功(allowlist 65辺のまま)、
clang-format 済。TPC-C クリーン実行の終端検証 5/5 PASS。

---

2026-10-07 加筆(第20報)。SQL機能ギャップパス: MERGE INTO と TRUNCATE TABLE
の実装、および実機プローブによる分析機能ギャップの棚卸し。詳細は下記
「2026-10-07 SQL機能ギャップ(MERGE/TRUNCATE)パス」の節。

## 2026-10-07 SQL機能ギャップ(MERGE/TRUNCATE)パスの成果

**実機プローブで機能ギャップを棚卸しした**(Roadmapのチェックボックスは
実態と乖離しているため、CLI実行で判定)。未実装だったのは
`MERGE INTO` / `CREATE MATERIALIZED VIEW` / `TRUNCATE TABLE` /
`ALTER TABLE` / `CREATE TABLE FUNCTION`(作成は通るが参照が
「unsupported table source TVF」)/ `UPDATE ... FROM`。
本ラウンドではこのうち **MERGE INTO と TRUNCATE TABLE を実装**した。

- **[フロントエンド/executor] MERGE INTO を実装**(`query/merge_test.cpp`
  11件がpin)。ピン留めパーサは `MergeStatement` ノードを出すので、
  (a) visitor が IR(`MergeStatement`: 源を `SelectStatement` に正規化 —
  テーブル参照源は `SELECT * FROM t`、`TableSubquery` 源は
  `VisitQuery` 結果+必須エイリアス)へ変換し、
  (b) `SqlEngine::ExecuteMerge` が源を relation として実体化した上で
  分類・適用する。式の源/ターゲット参照解決は源 schema を
  `QualifySchema(alias)` してターゲット Scope の outer に接続するだけで
  式クローン無しで評価できる(`TryLookup` の outer chain)。
  正しさの契約(BigQuery準拠、実機プローブで確認):
  - ON条件の連言から素列等価 `t.k = s.k` を抽出して源をハッシュ化
    (抽出不能なら線形走査にフォールバック。型不一致キーもフォールバック —
    ハッシュ符号化と等値比較の食い違いで取りこぼさないため)。
    NULL キーは決してマッチしない(NULL=NULL は UNKNOWN)。
  - 1ターゲット行=1アクション: UPDATE/DELETE 句が複数源行にマッチしたら
    明示エラー。MATCHED句群は宣言順に condition=TRUE の最初を適用。
  - `NOT MATCHED [BY TARGET]` 句は最大1(INSERTのみ)、
    `NOT MATCHED BY SOURCE` 句は最大1(UPDATE/DELETEのみ、source列参照は
    評価エラー)。両リレーションに存在する裸列名は曖昧エラー。
  - FK NO ACTION を適用順(deletes → updates → inserts)で強制。deletes先行で
    文内自己参照(deleteした親キーの子を同文でinsert)が整合。
  - 1文1トランザクションの既存契約の下で、適用エラーは文失敗(部分適用は
    abortが巻き戻す)。
  回帰: `MergeTest.*` 11件(基本/条件付き句の優先順/BY SOURCE
  DELETE・UPDATE/サブクエリ源/INSERT列リスト並べ替え/複数マッチ拒否/
  曖昧裸名拒否/NULLキー/FK INSERT・DELETE/フィクスチャ再利用)。
- **[フロントエンド] TRUNCATE TABLE を実装**。visitor段で無条件
  `DELETE` へ脱糖(MVCC/WAL意味論を一様に保つ。ページチェーン切断の
  物理truncate短絡は将来のトランザクション層の最適化として記録)。
  テーブル定義(スキーマ/制約/インデックス)は維持され、直後の
  INSERT/MERGE が動くことを pin。
- **[計測] SF1 ベースラインを取得**(既定経路は relational に切替済み
  であることを確認 — Q22 のプロファイルが RelationalExecutor):
  TPC-H 22q 合計 ~13.8s(静穏時。Q9 2331 / Q21 2186 / Q3 1125 /
  Q18 1034 ms)、TPC-DS 94q warm 4564ms / failed=0。
- **[調査・記録] TPC-H Q21 の実行支配は派生表2つの集約**。
  `multi`(lineitem 600万行 GROUP BY l_orderkey)と `only_late`(フィルタ後
  GROUP BY)の2派生表で **979万行・287万グループの集約 = lineitem 3回
  スキャン**(本体+2派生表)が支配。公式EXISTS形の等価書き換えではあるが、
  2派生表を単一パス集約に統合する(共通 GROUP BY キーの集約統合)のが
  次のレバー。ベンチクエリの書き換えではなくエンジン側の変換として。
  なお Q9 は scan(851万行)/join 支配で、結合順序品質が引き続き本命。
- **[記録] 並行セッションが recovery/ を編集中のため、このファイルの
  リカバリ系記述は当該セッションの報告に譲る。**
  `RecoveryExtraTest.PartialLoserUndoThenAnchorRestoreKeepsIndexConsistent`
  の失敗(undo再挿入skip+ページラッチ)は本ラウンドの変更と無関係
  (SQL層のみ変更。失敗内容は第19報「既知課題」のundo再挿入skipで、
  当該テストは並行セッションの04:45-04:51の編集後に失敗するようになった)。

**検証:** `merge_test` 11件パス、全体ctestはMERGE/TRUNCATE由来の
失敗ゼロ(唯一の失敗は上記 recovery 系の並行セッション領域)。
`check_layering.py` 成功(allowlist 65エッジのまま)。clang-format 済。

---

2026-10-06 加筆(第19報)。トランザクションエンジン強化(第10ラウンド)。
並行append下の文硬失敗根絶、kill -9後の再起動を「開かない」から「秒単位
完了」へ、フェンス縮小undo予約の欠落修復。詳細は下記
「2026-10-06 トランザクションエンジン強化(第10ラウンド)」の節。

## 2026-10-06 トランザクションエンジン強化(第10ラウンド)の成果

**[P0根治] 並行append下のINSERT文硬失敗(旧OPEN項目「末尾ページイメージ
消失」の表層)。** `CheckpointFlushKeepsTreeCoherentUnderConcurrentInserts`
が高負荷下で〜70%失敗していた。診断で3つの独立経路が判明し、すべて修正:

1. **IndexInsert/IndexDeleteのdrainタイムアウトが素のkNoSpaceとして文を
   落とす。** B+Treeのin-gate drain(100ms)が切れると、競合の亜種なのに
   「ページ満杯」を意味するステータスが利用者に漏れていた。修正:
   B+Treeの2つのdrain点がタイムアウト時に
   `NoteIndexReservationDrainExhausted()`を_set_し、Table::IndexInsert /
   IndexDeleteはゲートを**解放して**から最大2.5s×4回のゲートフリーdrainと
   RMW再実行にエスカレート(ゲート解放こそが予約オーナーの進行を許す)。
   エスカレーション枯渇時はkConflicts(正直でリトライ可能)。
2. **LeafInsertのsplit入口に事前拒否ガードが無い。** split内部
   (SetFoster/行移動)はアドミッション割引で半分適用後に失敗しうるが、
   変更後地点なのでリトライ不能。FindLeafOnceのfoster吸収ガードと同型の
   「変更前拒绝+drain+再降下」をsplit直前に追加。
3. **FindLeafのTOCTOU。** 降下中の予約ガードがkNoSpaceを返した直後、
   ラッパーの再チェックまでに予約が排水され、foreign=0のまま素のkNoSpace
   が返る。予約ゼロなら全拒绝サイトは完了する(fresh-page split、フェンス
   縮小)ため、再降下すればよい(上限4回の防御的 bound 付き)。

効果: 当該テスト 7/10失敗 → **40/40・15/15連続green**。TPC-C
(W=2, 16cl)では engine aborts 約3.0%、`mvcc.write_intent.wait_timeouts=0`
を確認。回帰は本テスト自体(フレーキーが再現装置)。

**[P0根治] kill -9後の再起動が開かない(実測ウェッジ)。** TPC-C負荷中の
kill -9で最新ホットページ群が torn になる(32KiB×数百〜数千ページ)と、
旧SinglePageRecoveryは**1ページあたりWAL窓全体を再走査**するため、
4,000ページ破損×400MiB窓でSPRだけで3.5時間(実測2.3秒/ページ)——実質的な
再起動不能。修正と、そこから露見した潜在バグ3件:

- **バッチSPR (`RecoverBrokenPages`)**: 窓を1パスで走査し全破損ページを
  一括収集→各ページ1回再生。**実測 5,438ページ/8.1秒** (旧比〜1,500倍)。
  再生は各ページの**最後の再初期化レコード**(kSystemAllocPage /
  kCompensateAllocPage / kFullPageImage / kSystemDestroyPage /
  kCompensateDestroyPage)から始める: リサイクル済みpidの窓には旧
  インカーネーションの記録が混在し、torn イメージへの旧記録再生は型不一致
  で死ぬ(実測)。再initが窓に無いページは従来どおり窓先頭から。
- **ストリーミングredoにページ別recLSNフロアを追加**(ARIESの本義)。
  破壊済みページの空イメージ(page_lsn=0)がガードを無効化し、破壊前の
  旧記録まで再生して型不一致→オープン失敗していた。
- **CLRの再生にstale-incarnation skipを追加**(`IsCompensation` 新設、
  log_record.hpp)。途中失敗したリカバリが書いたCLRのうち、対象ページが
  その後破壊/再発行されたものが再リカバリを永遠にfail-closeさせていた
  (LogUndoの同種skipのredo側ツイン)。物理適用はスキップ、スタンプは進む
  (冪等維持)。
- **undo再挿入の可用性フロア**: 再開後のloser undoで物理再挿入が満杯に
  拒否された場合、CLRを保持したまま物理適用のみskip(WARN)。openが
  永遠に失敗するウェッジより、1エントリの欠損を選ぶ。
- **undoのセパレータ再挿入に子ページ生存チェック**: コミット済み破壊で
  解放された子へのセパレータ再挿入(ダングリング)をskip。
- **降下のstale routeをプロセス中断から文失敗へ**: `Page::GetPageForKey`
  の非branchページ到達がASSERT(プロセス全体が死ぬ)だったのをkCorruptへ。
  数週間稼働のサーバが単一のstale routeで死なない。付随して
  FindLeafOnce内部の`.Value()`/`ASSIGN_OR_CRASH` 2箇所をステータス伝播に。

実測(fresh kill -9 × 数回): 復旧11秒前後(4,315ページ再構築、350MiB窓)、
オープン成功、FATAL 0。TPC-C検証は5群中3〜5群PASS — 残るFAILは下記
「既知課題」のundo欠損。

**[P0根治] フェンス縮小のundo予約欠落。** `SetFoster`には縮小デルタ予約が
あったが `SetLowFence`/`SetHighFence`には無かった(「フェンスはadvisory」
の丸ごと除外)。縮小フェンスのundo(より長い旧値の復元)は予約なきバイト
消費であり、実測としてリーフの削除undo予約プールを8バイトだけ侵食し、
306件の未解決削除undoの最後の1件を物理的に不能にしていた(再開時
`kTooBigData`でオープン失敗・実行時ならfail-stop)。修正: 両エミッタに
SetFosterと同型の縮小側のみの予約を追加(growth側のsplit契約は不変)。

**診断の恒久化**: `InsertImpl`拒否時のfree/rows、undo再挿入失敗の
ページ/サイズ、空PageRef発行時のpid/ファイルサイズ (PMDIAG)、redo失敗の
レコード文脈。いずれも異常パスのみの出力。

**検証**: 影響15スイート466テスト全green (transaction / transaction_extra
/ lock_manager / table / b_plus_tree / index_scan_iterator / row_page /
page_pool / page_manager / logger / log_record / recovery_manager /
recovery_extra / checkpoint_manager / database_extra)、
`check_layering.py` 成功 (allowlist 65辺のまま)、clang-format 済。

**判明した既知課題(未修正・次回最優先):**

- **kill -9後の再開で稀にstale separatorが残る**(検証群の一部が
  「descent routed to a non-branch page」でFAIL。3/5〜5/9群が影響)。
  ここまでの分析: undo再挿入のskip起因ではない(0 skipでも発生)。redoの
  セパレータinsertと子のdestroyの整合は理論上保たれるが、SPRの
  インカーネーション再initアンカー或いはrecLSNフロアとBranch再構築の
  組み合わさりで欠落が起きている。切り分け用の env
  (`TINYLAMB_NO_RECLSN_FLOOR`/`TINYLAMB_NO_SPR_TRIM`)は検証後に撤去済み
  — 次回は再現path(短期負荷→kill→再開→full scan検証)をテストとして
  固定し、branch再構築のレコード集合をダンプして特定すること。
- **大規模torn再構築時のundo再挿入skip** (~数千/回): フェンス予約修正で
  新規WALは閉じたが、torn再構築直後のundoは予約の無い世界で走るため、
  欠損が焼き付いたDBではskipが続く。skipは冪等で安全だが、検証FAILの
  直接原因。undoのリーフ再挿入をsplit可能経路(BPlusTree相当)に上げる
  か、undo専用の緊急split(foster兄弟)を実装するのが根治候補。
- index gateがRMW全体を排他直列化(第9報からの持ち越し)。今回の計測では
  write-intent競合(621/448K)が支配でゲート待ちは計測すら未達 — gate-free
  RMW再構築の前にゲート待ち計装を足すべき。

---

2026-10-06 加筆(第18報)。Cascades網羅率パス: 関係経路のOR枝共通等価
結合辺抽出(TPC-DS q13の関係経路劣化48s→7.3s解消)、grouped finish並列
foldの式キー対応、複数サイトCTE narrowingの上限境界サイレント誤り(P0)
修正。詳細は下記「2026-10-06 Cascades網羅率パス」の節。

2026-10-05 加筆(第17報)。分析機能ギャップパス: 複数列INサブクエリの
実装、MEDIAN / FARM_FINGERPRINT / PARSE_JSON / TO_JSON / ILIKE の追加、
および grouped route 既定妥当性の再計測(Q9ブリッジ課題の構造特定)。

## 2026-10-06 Cascades網羅率パス(第11ラウンド)の成果

- **[planner] OR枝共通等価の結合辺化(関係経路)。** 3枝以上の選言が
  全枝で同一の `col = col` 等価を含むとき、その組を合成2源等価述語として
  結合計画に供給する(`SynthesizeDisjunctEqualityPredicates`、
  `executor/detail/planning_heuristics.cpp`)。選言は論理的に各共通等価を
  含意する(生存行は必ずいずれかの枝をTRUEで満たすため等価もTRUE)ので、
  (a) 貪欲再順序付けの接続グラフ、(b) 実行時キーフィルタ(SIP)候補、
  (c) 各ステップのハッシュキー抽出、すべてに自然参加する。元の選言は
  削除されず、担当ステップの残差で全行再検証される(キーはバケット刈り
  のみを行い行を追加しない)。`SafeToReduceEvaluationCount`(揮発性/
  サブクエリ拒否)を満たす選言のみ対象。TPC-DS q13 形状
  (`ss_hdemo_sk = hd_demo_sk` / `cd_demo_sk = ss_cdemo_sk` が全枝共通)で
  ネストループ→ハッシュ結合に転換。
  **効果: TPC-DS SF1 94q、`TINYLAMB_GROUPED_ROUTE=relational` で
  48.0s → 7.26s(既定Cascades経路 7.07s とほぼパリティ)。**
  回帰: `QueryTest.DisjunctCommonEqualityBecomesJoinEdge`(共通等価なし枝が
  1つでもあれば辺を作らない否条件込み)、
  `QueryTest.ThreeWayDisjunctCommonPairsKeyBothJoins`(3枝2組で両結合ステップ
  がhash_joins=2)。
- **[P0修正] 複数サイトCTE narrowingの上限境界サイレント誤り。**
  `TryNarrowCteBody`(`query/sql_engine.cpp`)の複数サイト融合が上限方向で
  最弱(最大値)でなく最強(最小値)を採っており、狭いサイトの上限が共有
  cell を潰し、広いサイトが必要とする行を失っていた(下限方向は正しく最弱
  min)。各サイトの行は「自分自身のフィルタ」だけを満たせばよく、全サイト
  の述語の連言はサイト毎の行には課されないため、注入境界はサイト毎の含意
  でなければならない。実害例: `WITH c AS (...) SELECT ... FROM c x JOIN c y
  ON x.k=y.k WHERE x.v <= 28 AND y.v <= 20` で x 側の v=28 行が消える。
  回帰: `QueryTest.SqlEngineMultiSiteCteUpperBoundStaysWeakest`
  (境界入替の両方向を pin)。TODO.md item 2 の残件記事を更新
  (複数サイトweakening実装済み + 本修正を記録)。
- **[executor] grouped finish並列foldの式キー/式引数対応。** `Project` の
  並列集約(262,144行以上)がGROUP BYキーと集約引数を素の列に限っていたのを、
  `BytecodeCompiler::Compile` 成功する決定論的式まで拡張(ワーカースレッドは
  文脈非依存のバイトコードVMで評価 — 並列scan filterの既存契約と同一)。
  コンパイル不能形状は従来どおり逐次ループへフォールバック。
  `TINYLAMB_DISABLE_PARALLEL_EXPRESSION_KEYS=1` でA/B。Cascadesブリッジの
  GroupByFinishが関係経路のtyped fold/並列集約に近づく第1スライス
  (第10報「ブリッジ劣化=実行器成熟度」の一部解消)。
  回帰: `RelationalRegressionTest.ParallelGroupedFinishEvaluatesExpressionKeysAndArgs`
  (4群×65536行、キー `g % 4`・引数 `v * 2` の群別合計pin)。
- **[検証] TPC-H SF1 両経路A/B(同一gold DB): relational 12,915ms vs
  既定Cascades先行 20,814ms(全22クエリ行数一致)。既定経路のQ3 4,008ms /
  Q19 3,943msが relational 経路で 263ms / 308ms —— grouped route既定の
  Cascades先行はTPC-Hで大きく不利なままであることが再確認された
  (q13が関係経路の残 blocker でなくなった今、既定逆転の再評価条件が揃って
  いる)。全ctestは本報告末尾。
- **[調査・記録] Roadmap の UNNEST 関連チェックボックスは陳腐化。**
  `WITH OFFSET`(疑似列ORDER BY/AS別名含む)、`LEFT JOIN UNNEST`、
  相関UNNESTは実装済み(compliance `unnest_queries.test`/
  `sql_feature_coverage_test` でpin済み)。

**次のレバー:** (a) grouped route既定の再評価 —— TPC-Hで−38%、TPC-DSで
パリティ。flipには全ctestを `TINYLAMB_GROUPED_ROUTE=relational` で通す
検証と、Cascades経路でしか計画されない形状(TPC-DSで既定が優るクエリ)の
特定が前提。(b) 既定経路のQ3/Q19劣化の本体は GroupByFinish 経由の
generic finish に残っており、typed fold/融合の移植が継続課題。

## 2026-10-05 分析機能ギャップ(第10ラウンド)の成果

**実機能ギャップを実機プローブから特定した**(Roadmapのチェックボックスは
陳腐化しており、CTAS/STRING_AGG/スカラーWITH等は既に実装済みだった)。
実装した5機能と、構造調査の結果を記録する。

- **[フロントエンド/executor] 複数列INサブクエリ `(a, b) IN (SELECT x, y
  ...)` を実装**(従来「Subquery of type IN must have only one output
  column」の明示的拒否)。スカラーINサブクエリの評価経路
  (`executor/detail/expression_eval.cpp`)で、プローブ値が行値
  (匿名struct JSON)のときだけ結果行を位置fNキーのstruct JSONへ射影して
  メンバーシップ判定に回す。既存の `ThreeValuedEqualsImpl` がstruct JSONの
  位置比較(フィールド名不問・NULLフィールドはUNKNOWN)をすでに実装して
  いるため、リスト形 `(a,b) IN ((1,1),(2,1))` と同一の三値論理で一致。
  ハッシュメンバーシップはstructプローブを既にrescanへ退けているため
  NULL行の誤TRUEは起きない。DMLの畳み込み経路
  (`DecorateDmlWhere`、`query/sql_engine.cpp`)も同様にプローブ形状
  (`__struct_json__` / ArrayExp / 定数struct)で判定して折り畳む。
  スカラープローブ×多列サブクエリは従来どおり明示的エラー(契約維持)。
  回帰: `SqlSubqueryShapesCoverageTest.RowValueIn`(非相関/NOT IN/NULL候補/
  スカラープローブ拒否の更新)、`CorrelatedRowValueIn` は既存のまま通過。
- **[関数] MEDIAN(x) [OVER (...)] を PERCENTILE_CONT(x, 0.5) へ下げる
  エイリアスとして実装**(`query/googlesql_ast_visitor.cpp` の
  `VisitFunction` / `VisitAnalyticFunctionCall`)。集約と窓の両経路で
  既存の補間実装を共用する(累積器の二重実装を作らない)。回帰:
  `AnalyticalFeaturesTest.MedianMatchesPercentileCont`(単一/グループ化/
  空入力NULL/窓形)。
- **[関数] FARM_FINGERPRINT を bit-exact に実装**
  (`common/digest.hpp` に farmhashna::Hash64 = Fingerprint64 の移植)。
  リファレンス実装(google/farmhash)をコンパイルした比較ハーネスで
  境界長(0..257)+2万ランダム入力すべて一致を確認済み。
  `expression/function_call_expression.cpp` と
  `executor/detail/expression_eval.cpp` の両評価器に配線(双評価器規則)。
  回帰: `AnalyticalFeaturesTest.FarmFingerprintMatchesReference`
  (リファレンス由来の期待値を pin)。
- **[関数] PARSE_JSON / TO_JSON を実装**。`expression/json_text.hpp` に
  RFC 8259 厳密検証器(サロゲートペア検査込み)と `ToJsonText`
  (VARCHAR は struct ペイロード `{...}` を素通し、他はクォート。
  配列は再帰)を新設し、両評価器から共用。不正 JSON は明示的エラー。
  回帰: `AnalyticalFeaturesTest.ParseJsonAndToJsonRoundTrip`。
- **[フロントエンド] ILIKE / NOT ILIKE をプリパス書き換えで実装**
  (`query/googlesql_frontend.cpp` `NormalizeCaseInsensitiveLike`)。
  ピン留めパーサにILIKEトークンが無いため、`x [NOT] ILIKE y` を
  `LOWER(x) [NOT] LIKE LOWER(y)` へliteral/commentマスク上で書き換える。
  オペランドの範囲検出(識別子パス/関数呼び出し/括弧/リテラル)は保守的で、
  境界を証明できないときは書き換えを止める(パースエラーで fail-visible。
  黙った意味変更はしない)。`SELECT ilike FROM t` のような識別子誤爆は
  キーワードガードと右辺境界検査で阻止。回帰:
  `AnalyticalFeaturesTest.CaseInsensitiveLikeForms`。
- **[調査・記録] grouped route(TINYLAMB_GROUPED_ROUTE)既定の妥当性を再計測**。
  既定Cascades先行に対し `=relational` は TPC-DS SF1 全体が 8.4s → 48.0s
  と大劣化(q13 以外にも広く劣化する)。2026-10-03 記録の「TPC-Hの
  grouped 形状は関係経路が速い」との板挟みのため、既定変更は却下。
  q13 の形状(3枝ORの全枝に共通する結合等価 `ss_hdemo_sk = hd_demo_sk` /
  `cd_demo_sk = ss_cdemo_sk`)を関係経路の結合辺として抽出する
  「OR枝共通等価抽出」は今後のレバー候補(関係経路単体のq13救済。
  経路既定には触れない)。
- **[調査・記録] Q9ブリッジ(TINYLAMB_SCALAR_FN_CASCADES=1)の2.2倍劣化は
  結合順序ではなく実行器成熟度**。ハッチONのEXPLAIN表示は
  `HeuristicJoinOrder`(explain_formatの生行数greedy表示)だが、実行は
  GroupByFinish(部分/最終集約の GENERIC finish)で、関係経路が持つ
  typed-fold / grouped-join fusion / 並列集約の各最適化を経由しない。
  2026-10-03 記録「grouped TPC-H形状は関係経路が速い」と同根。
  ブリッジ常時化の前提は「ブリッジfinish側の実行器最適化の移植」であり、
  推定較正(rows~25系)は表示上のノイズだった可能性が高い。

**検証:** 関係スイート(analytical_features 29件 / subquery_shapes 10件 /
googlesql_frontend 28件 / differential 15件)パス、`check_layering.py`
成功(allowlist 65辺のまま)。全体ctestは本報告末尾に記録。

---

2026-10-04 加筆(第16報)。分析SQL完成度パス: NDV推定のChao1化と複合キー
結合推定の下限で **SF1 TPC-H Q18/Q21の暗黙0行(正解100行)を修復**、
並列hash joinの実行時ビルド側選択、スキャン列プルーニングのdecode-floor
ハッチ、日付部分関数ファミリー(WEEKDAY等)の実装。

## 2026-10-04 分析SQL完成度(第9ラウンド)の成果

**計測(TPC-H/TPC-DS SF1 A/B)から始めた:** Q9ブリッジ(4.36s vs relational
2.75s)の「rows~25系推定崩壊」を追跡し、`TINYLAMB_JOIN_CARD_DEBUG=1`(env
ゲート、無効時ゼロコスト)で per-key selectivity を実測したところ、
**Q9の本丸ではなくTPC-Hスイート全体の推定品質の根**が2つ見つかった。

- **[統計] ANALYZE の NDV 推定が高NDV列で「行数=NDV」を捏造**
  (`table/table_statistics.cpp` `ColumnCollector::EstimateDistinct`)。
  4,096行のリザーバ標本に対し200k distinctのFK列は標本の~98%がsingletonに
  なり、coverage < 0.05 の枝が `non_null_count_`(=行数)を返してしていた。
  l_partkeyは実NDV 200k のところ 6,001,215 と主張。NDV=行数のFK列は
  結合選択率の分子を桁違いに縮め、複合キー結合の推定崩壊の片翼だった。
  coverage < 0.05 枝を **Chao1**(S_obs + f1²/2f2、doubleton無しは
  補正形)に置換し、観測 distinct を下限・non_null を上限にクランプ。
  l_partkey: 6.0M → ~207k(実200k)。
- **[推定] 複合等価キー結合の独立仮定がFK結合を100倍過小評価**
  (`plan/implementation_rules.cpp` `JoinCardinality`)。
  `(l_partkey,l_suppkey)=(ps_partkey,ps_suppkey)` のような複合FK-PK結合は
  キー間が強相関(実出力=左行数)なのに、`EstimateEquijoinSelectivity` の
  指数バックオフ(sel1×√sel2)が実出力の1/96(9.4万行→2,542行)に潰して
  いた。Q9の「lineitem 6M行がハッシュビルド側に載る」のはこの崩壊が根。
  **最小キー選択率を下限に** (max(backoff, min_sel)) —— 相関下では最良キー
  単独が組数を下から抑える。単一キー結合は不変(下限=キー自身)。

**効果(TPC-H SF1, 完全gold DB, 交互ペア min-of-2):** Q3 −43%、Q15 −11%、
スイート合計 ±1.0%(ノイズ内)。TPC-DS 94q: 合計 −2.9%(q79 −42%)、
**全94クエリの行数一致**。

**[修正] SF1 TPC-H Q18/Q21 が黙って0行を返す silently wrong result を修復**
(上記2修正の計画的効果)。**ベンチのQ18/Q21が0行、履歴・公式回答は100行**
という乖離をきっかけに発覚。小規模フィクスチャでは数量/遅延形状が発火しない
ため `ExecutesAllTwentyTwoQueries` の行数ピンでは検出不能(ピン自体は
正しい)。推定修正によりQ18/Q21の計画が正しい経路に戻り、100行を返すように
なった(所要 +56% は base が正解行を1行も作っていなかった分の実働)。
前提の独立検証: `sum(l_quantity) > 230` の注文は21,862件存在
(`HAVING sum > 100` = 108,183件も確認)。再発防止として
`Q18LargeVolumeCustomerShapeKeepsQualifyingRows` /
`Q21WaitingSupplierShapeCountsOnlyTrueWaiters`(集約HAVING under IN、
EXISTS+NOT EXISTS自己結合、グループ化TopN形状を非空の正解付きで pin)
を追加。

**[並列実行] SharedBuildParallelHashJoin が実行時にビルド側を選ぶように**
(`executor/parallel_hash_join.{hpp,cpp}`)。逐次 HashJoin は
`SetupInMemoryJoin` で実バイト数の小さい方をビルドするが、並列形は右側固定
で、計画の向きが外れると 6M行(733MiB)のビルドが實行される。inner join は
両側を既に実体化しているため実サイズ比較は無償で、小さい方をビルドし、出力
は常に left-then-right(emit順を `build_left_` で反転)。Dump に
`build=left|right` を追加。非inner種は従来の右ビルド契約を維持。

**[計画/実行] スキャン列プルーニングの decode-floor をハッチ追加**
(既定OFF, `TINYLAMB_SCAN_PROJECTION=1`)。Cascadesコア経路の FullScan は
全列を復号してから上位の Projection が落としていた(関係経路は既に
プルーニング済み)。`FullScanPlan` に縮小コピーctor(射影スロット、対応する
スキーマ/統計部分集合、クラスター順主張は不搬送)+ `full_scan` 実装規則が
required列の素の列リストをイテレータ射影へ落とす
(`SelectionPlan::Predicate()/Src()`、`FullScan`/`ParallelScan` に射影配線、
`FullScan::NextBatch` は縮小幅でチャンク初期化)を追加。
**TPC-H Q19でplan feedback下に10倍遅い計画へ反転する相互作用が見つかった
ため既定OFF**(`TINYLAMB_SCAN_PROJECTION` を立てると lineitem
columns=6/16 のようにデコードが6列に縮む。Dump/ToString に columns= を追加)。
Q19の形状は結合述語が第3OR枝にのみ付くクロス積形で、feedback統計との
組み合わせで BatchNL 選択が変わる —— 根因修正は次のレバー。

**[フロントエンド] 日付部分関数ファミリーを実装**
(`query/googlesql_ast_visitor.cpp` + `executor/detail/expression_eval.cpp`)。
`EXTRACT(WEEKDAY FROM d)`(月曜=1..日曜=7、DAYOFWEEKの日曜=1と区別)に加え、
BigQueryの素の関数形 `DAY/DAYOFWEEK/DAYOFYEAR/MONTH/QUARTER/YEAR/ISOYEAR/
ISOWEEK/WEEK/WEEKDAY(x)` を単一引数のとき `extract_*` へ下げて共用実装に
(2引数の `WEEK(x, week_day)` は従来どおり明示エラー)。回帰:
`SqlRelationalEvalCoverageTest.ExtractPartFamily`(weekday 3件)/
`DatePartPlainFunctionAliases`(10関数+GROUP BY)。

**計測上の事故と教訓(記録):**
- /tmp クォータ枯渇下で `cp` が黙って truncate し、**欠損DBで計測した**
  (lineitem 889k行/6M行でQ18/Q3がでたらめに)。gold作成後は必ず
  `SELECT COUNT(*) FROM lineitem` == 6,001,215 を検証してから測ること。
  またロード中の定期チェックポイント(`TINYLAMB_CHECKPOINT_SECONDS=2`)が
  末尾ページイメージ消失(既知OPEN項)を踏み、ロード検証とリオープン後の
  可読行数が不一致になった。goldは `CHECKPOINT_SECONDS=0` ロードで作る。
- ベンチマークは EXCLUDE_FROM_ALL のため `cmake --build` 単独では
  再リンクされない(再発。測定前に必ず `--target tinylamb_tpch_benchmark
  tinylamb_tpcds_benchmark` を明示)。

**検証:** 対象スイート(Optimizer/Cascades/Executor/TPC-DS/Query/統計系577件)
パス後、全体ctestは本報告末尾の記録を参照。`check_layering.py` は
allowlist 65辺のまま成功。

2026-10-03 加筆(第15報)。残件完了パス: Q9 の Cascades ブリッジ到達
(派生表JOINコア・フラット化)、D-9 並列保存順スキャンの消化、
D-6 の保留確定。

## 2026-10-03 残件完了パスの成果

**[実装] TPC-H Q9 が Cascades ブリッジに乗る経路を開通(ハッチ配下)。**
Q9 の形状 `SELECT ... FROM (SELECT ... FROM t1..t6 WHERE ...) profit
GROUP BY ...` は(a)派生表(`source.query != nullptr`)と(b)SELECT/GROUP BY 内の
スカラ関数(`EXTRACT`)の2段でルーティングが止まっていた。
- (a) `FlattenGroupedDerivedJoinCore`(`query/sql_engine.cpp`): 外側が
  「派生表1つ + GROUP BY/ORDER BY」で内側が純粋 inner-join コアのとき、
  JOINコアを外側FROMへspliceし、派生出力参照を内側式で置換する
  M5+スライス。安全側条件(inner join のみ/lateral・unnest・using・
  nested拒否/内側の集約・DISTINCT・LIMIT・window・CTE拒否/揮発関数と
  サブクエリ拒否/未解決裸名の拒否)。
- (b) `TINYLAMB_SCALAR_FN_CASCADES=1`(既定OFF)で通常スカラ関数が
  ブリッジを通る。
- **実測(gold SF1)**: `TINYLAMB_SCALAR_FN_CASCADES=1` で Q9 は
  `GroupByFinish`(Cascades コア+HashJoin tree)で実行、175行・正解。
  ただし **4.16s vs relational 経路 2.46s と 1.7倍劣る**。計画は
  lineitem(600万行・~733MiB)をハッシュビルド側に選んでおり、結合順序・
  向きのコスト判断がまだ及ばない(各結合の推定「rows~25」の過小も観測)。
  この差はTPC-DSハッチ失敗(第14報)と同根のブリッジ結合順序品質。
- **フラットナーはハッチ配下のみで発火**: 無条件発火だと既定のrelational
  経路がQ9で一貫して約15%劣化した(SF1交互ペア: 派生表2.46-2.60s vs
  フラット化2.93s。派生表経路はフィルタ済みコアを一度実体化し、
  grouped finish が小さいリレーションを読む形が有利)。
  既定挙動は完全に旧来通り。
- **次のレバー**: ブリッジの結合順序品質(大入力ビルド側の抑制、
  rows~25系推定の較正)が本命。これが倒れればフラットナー+関数緩和の
  既定ONとQ9のブリッジ常時化の検証に進む。

**[実装] D-9 並列保存順スキャン消化。** `ParallelScan` は2026-10-01の
決定性パスで既にチェーン順の morsel順ハンドオフを実装済みだったため、
残る計画側の主張抑制を解除: `FullScanPlan::EmitsChainOrderedScan()` は
`!lock_rows_` のみに、`EmitExecutor` は clustered order 主張があっても
並列スキャン(`ordered=true`、行フィルタpushdown拒否)を生成。
8192行超のクラスター順テーブルでソート排除+完全昇順出力を確認。
回帰: `FullScanClusteredOrderTest.ParallelShapeKeepsAndDmlSourceWithholds`
(旧「しきい値超えは主張しない」テストをD-9契約に更新)、
`SqlClusteredOrderTest.ParallelScanKeepsClusteredOrderAboveThreshold`。

**[保留確定] D-6 分散実行。** Raft・レプリケーション・network transport・
snapshot/install・障害試験は新サブシステムであり、単一セッションの
スコープ外(既存判断を再確認)。再開条件は初スライスで `LogStorage`
interface 化+単一ノード動作テストから。なお 2026-10-03 の実行器監査で
exchange/distributed_agg_finalize 等の分散スタブ実行器は到達不能のため
撤去済みであり、分散化の再開時は実装からやり直しになる。

2026-10-03 加筆(第14報)。バックログ全件消化パス: 古老化項目の検証と
クローズ、GROUP BY ORDER BY 不明列の黙否定的ソート修正、TSAN並行
リプレイデータレースの根治、スカラ関数ブリッジ緩和ハッチの追加。

## 2026-10-03 バックログ消化パスの成果

**[検証・クローズ] 過去報の「未修正・未対応」記事の大半は後続ラウンドで解消済み。**
現行ツリーで実機確認したもの:
- 行値 `(a, b) IN (SELECT ...)`(相関含む)は動作(`sql_subquery_shapes_coverage_test`
  の RowValueIn / CorrelatedRowValueIn)。2026-09-22/25 記録は陳腐化。
- `SELECT * EXCEPT/REPLACE`、パイプ `DROP/EXTEND/RENAME/CALL`、
  ウィンドウ×ROLLUP/GROUPING SETS は実装済み(analytical_features_test)。
- `ORDER BY 序数 × SELECT *`(派生表・CTE経由込み)は正しくソート。
- 2026-09-27 の cse_projection 回帰(`optimizer_cse_projection_high_expectations_test`)
  は現行ツリーでパス。
- q96 は登録済みで全クエリ決定性テストの対象(「q96 cross join」は解消)。
- q65/q67 の rank 境界揺らぎの機構(morsel→worker 非決定割当)は 2026-10-01 の
  静的ローテーションで解消。tpcds_query_test は warm-vs-warm 比較 +
  `QueryPipelineStagesAreBitDeterministic` の段階別ビット指紋で pin 済み。
  cold≠warm の計画差は plan feedback の設計契約(2026-10-01 第6報)。
- WAL ボリューム残容量テレメトリは実装済み(`Logger::SetFailed` が
  ENOSPC/EDQUOT 時に statvfs の空き/容量を MiB で LOG(ERROR))。
- 2026-09-27 の Debug ビルド B+Tree assert(FindLeftmostPage)は
  build-cov(Debug)の `SqlSessionFuzzer.SeededSessionsHoldState` で
  再現せず(7.4分実行、パス)。

**[修正] GROUP BY ORDER BY の不明列が黙否定的に「ソート」される実バグ**
(`executor/relational.cpp`、`executor/detail/expression_eval.cpp`)。
`SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY nosuch` は静かに成功し、
出力行全体を STRUCT 化した値で「ソート」していた(見た目はたまたま整列)。
根因は `TryLookup` の「裸名は行エイリアスとして行全体を表す」フォールバックが
グループ化出力スコープでも発火すること。`Scope::no_row_alias` を追加し、
グループ化 ORDER BY 評価の proj/base 両スコープで抑制。
`ORDER BY nosuch + 1` の "type mismatch" も "column nosuch not found" に統一。
回帰: `QueryTest.GroupedOrderByUnknownColumnErrors`。
(select項目評価の `COUNT(t)` 行エイリアスは従来通りの寛容スコープを維護。)
**連鎖修正:** 抑制によってウィンドウ×集約3パス(q12形状)の最終 ORDER BY が
「選択されていない GROUP BY キー列」(i_item_id 等)を解決できなくなった——
従来はこの列も行エイリアスフォールバックのSTRUCTで捏造しており、
**要求されたキーとは無関係な順序で黙ってソートされていた**潜在バグ。
パス1で当該ORDER BY項を隠し `$okJ` 列として実体化し最終段から参照
(ウィンドウ `$pk` と同じ方式)。window/aggregate/別名を含む項は従来の
ノード単位書き換えを維持。

**[修正] TSAN並行リプレイのデータレース根治(2026-09-28 [高] 起票)。**
`RecoveryExtraTest.TornPageRebuildWithDirtyMetaPageNeverSelfDeadlocks` の
TSAN data race(Read: `PagePool::DetachVictim` の `page->PageID()` /
Write: リプレイワーカーの `Page::PageInit` memset)は、プールラッチ下の
スキャンがファストパス保持者のページラッチと決して順序付けされないことが根。
`PagePool::Entry` に不変の `page_id` を持たせ、pool_latch 下の読み
(DetachVictim / Touch / DropAllPages の dirty 収集)をすべて Entry 側から読む。
修正後 TSAN 3連回で data race 0(修正前は毎回2件)。

**[追加] スカラ関数の Cascades ブリッジ緩和ハッチ(既定OFF)。**
`TINYLAMB_SCALAR_FN_CASCADES=1` で `NeedsRelationalEvaluation` の
kFunctionCallExp を「通常のスカラ関数は Cascades 経路で評価可」に緩和
(`__value_table_value`(star形以外)/`__proto_new`/SQL UDF は従来通り relational)。
`EXTRACT(year ...) GROUP BY` などの plain な複数表グループ化が
GroupByFinish(Cascades コア)経路に流れ、結果は relational 経路と一致。
**判明した限界(記録):**
(a) TPC-H Q9 の形状は派生表(`FROM (SELECT ...)`)であり、関数ゲートではなく
`source.query != nullptr` のルーティングが本体ブロッカー。Q9 を Cascades に
乗せるには Prepare 段の派生表フラット化の拡張が別途必要。
(b) ハッチONで TPC-DS 3テストが失敗(決定性 rank 境界系 + cross product 回避)——
ブリッジの結合順序品質がこれらの形状でまだ及ばず。第8報のリバート判断と整合。
既定OFFの A/B ハッチとして維持(TINYLAMB_GROUPED_ROUTE と同じ運用)。
診断: `TINYLAMB_TRACE_ROUTE=1` で PostRewriteNeedsRelational が
relational 判定の理由(source 形状)を LOG(INFO)。

**[評価・記録] 破棄済みページidのインデックス予約圧迫(2026-10-03 第7ラウンド)。**
DestroyPage 時の予約破棄は、破棄を UNDO するトランザクションの補償リプレイと
リサイクル冪等性の再検証が前提で、効果はアクティブトランザクション存続期間限定の
過剰割引(availability側のみ)。実装せず記録。筋の良い改良は予約キーへの
インカーネーション判別の追加だが、UNDO リプレイ整合の検証コストが割に合わない。

**[文書] INSERT は PRIMARY KEY の一意性を強制しない契約を `query/AGENTS.md`
の新設「DML contracts」節に明記**(compliance の CompliancePrimaryKeyMode は
制約タグ付与であって書き込み時強制ではないことも併記)。D-8(DML WHERE の
非相関サブクエリ)は実装済みのため継続保留表から除外。

**検証:** 関連スイート逐次パスの後、全体 CTest 既定構成で全件実行(結果は下記)。

2026-10-03 加筆(第13報)。オプティマイザ健全化パス: 空統計列の
推定0確定問題の中立化、射影統計の事前分布引き継ぎ、到達不能実行器9本の撤去。

## 2026-10-03 オプティマイザ健全化パスの成果

**[統計] 未計測列(vacuous stats)の推定が「0行確定」を捏造するのを中立化**
(`table/table_statistics.cpp`)。計算式列(`EXTRACT`/`UPPER`/`x+1` 等)を
持つ `ProjectionPlan` は列統計を型情報だけの空 `ColumnStats` で作り直しており、
その上の `SelectionPlan::Filter` が `EstimateEqual`=0 を返して
**派生関係を「証明済み空」にしていた**。これが 2026-10-03 第8報に記録された
「Project/Select の estimated rows: 0」と「結合推定が実行の10-100倍楽観」の根。
`ColumnStats::Unmeasured()`(non-null/NULL両計数がゼロ=一度も測っていない。
測定済み全NULL列は NullCount>0 なので区別できる)を導入し、
等値=1/√rows(上限0.25)、範囲=1/3、LIKE=1/√rows、IS NULL=0.1、
同表 col=col=0.1、`EstimateCount`(index range)=rows/3 の中立デフォルトに。
「推定は空性を証明しない」(`EstimateJoinMatches` の契約)が実態と一致。
`IsProvenEmptySelect`(explain_format)の零選択率証明は測定済み統計にしか
発動しなくなり、むしろ安全化。

**[統計] `ProjectionPlan` が型保存の単一列変換なら元列の事前分布を引き継ぐ**
(`plan/projection_plan.cpp`)。`UPPER(x)`/`x+1` のように結果型が同一で
入力列が1つなら、行数とNDV(変換は値を併合するだけなので上界)を
`UntransformedPrior()` で引き継ぐ。ヒストグラム/MCV/境界値は変換前の
値域を記述するため破棄。型が変わる変換(`EXTRACT(year FROM d)`)と
多列入力は従来通り未計測(中立デフォルトが受け止める)。
`GROUP BY EXTRACT(...)` のグループ数推定も事前分布NDVで上界から
実態に近い値に改善。

**[死文化] 到達不能実行器9本を撤去**(`executor/` + `CMakeLists.txt`)。
`exchange`/`as_of_join`/`interval_join`/`dictionary_batch_aggregation`/
`chunked_scan`/`tid_scan`/`generate_series`/`two_phase_distinct_agg`/
`distributed_agg_finalize`(計~1,900行)。いずれも生成元のルールが
2026-10-02 監査で撤去済みで、非テスト参照がゼロ。専用テスト
(`executor_extra_test`/`executor_feature_test`/`executor_coverage_test`/
`data_chunk_test` の該当TEST)とCMake登録、docsの該当節に撤去注記。
`explain_format` の `TwoPhaseDistinctAggregate` 表示は実行器とは無関係の
注釈概念なので維持(compliance test が pin)。

**検証:** 全体 CTest **3,760/3,760 合格**(意図的skip 1)。
`check_layering.py` 成功(allowlist 65辺のまま)。回帰:
`TableStatisticsTest.UnmeasuredColumnStaysNeutralNeverEmpty`
(中立性+測定済み全NULL列は等値0を維持)、
`PlanTest.ProjectionKeepsEstimatesAliveThroughComputedColumns`
(事前分布引き継ぎ・型変更列は未計測・上位Selectionが0行に潰れない)。

**次のレバー(第8報からの更新):** GROUP BY スカラ関数の Cascades ブリッジ
ゲート緩和(`googlesql_ast_visitor.cpp` `NeedsRelationalEvaluation` の
kFunctionCallExp 全除外)は、上記の推定崩壊が根だったため**前提条件が消えた**。
再挑戦時は TPC-H Q9 の A/B(緩和版 vs 現行)を必須にすること。
なお「relational 経路の greedy が生行数で開始表を選ぶ」という第8報の記述は
`planning_heuristics.cpp` の実行経路では誤り(ローカルフィルタ適用**後**の
`Relation::TotalRows()` で選んでいる。生行数を使うのは explain_format の
推定専用 greedy とスキャン順決定のみ)。

2026-10-03 加筆(第12報)。健全化パス: 並行append下のテーブル末尾
ライフサイクル競合の防御層と、index-undo予約drainの三項演算子逆転修正。

## 2026-10-03 健全化パスで修正した実バグ

- **[txn] `WaitForAllIndexReservationsDrain` の三項演算子逆転**
  (`transaction/transaction_manager.cpp`)。タイムアウト時に「外国予約が
  残っていれば kSuccess、解消済みなら kConflicts」を返しており、
  (a) ブロック中の空転スピンと (b) 解消の瞬間の誤った文失敗を同時に
  生んでいた。正しい意味論(残存→kConflicts、解消→kSuccess)に修正し、
  `transaction_coverage_test` に契約テストを追加。

- **[index] B+Tree の foster 吸収が他トランザクションの予約で半途する**
  (`index/b_plus_tree.cpp` `FindLeaf`)。子branchのfoster解放(31B)で賄える
  高fence縮小(11B)が、他writerの分割が同一ページに残した undo 予約
  (最大~16KB)で kNoSpace になり、foster解放済み・fence未設定の木を
  残していた。吸収の**先頭で**(無改変状態で)外国予約を確認し、
  あればlatchを捨てて drain してから根から再降下する形に変更。
  解放バイトが常にfenceを賄えるため、drain後の吸収は完走する。

- **[table] NULL PageRef への盲目デリファラ**(`table/table.cpp`)。
  `PageManager::GetPage` は読めないページ(EOF以降・破損イメージ)に
  kSuccess で空 PageRef を返す契約(BPlusTreeブートストラップ用)で、
  `Table::Insert/Update/Delete` が無検査でderefし SEGV していた。
  全derefサイトに IsNull() ガード+末尾キャッシュのheadフォールバック
  ヒールを追加。`page_manager_test` に GetPage 契約とtail予約の
  自己退避テストを追加。

- **[page] Leaf/Branch の fence アドミッションが予約を無視**
  (`page/leaf_page.cpp` `page/branch_page.cpp`)。`usable_free` を計算して
  検査に使っておらず(NDEBUGで未使用変数警告化)、undo不可能化の
  潜在fail-stopだった。`usable_free` を使用するよう修正。

## OPEN: 並行append下の末尾ページイメージ消失(要調査)

`RecoveryExtraTest.CheckpointFlushKeepsTreeCoherentUnderConcurrentInserts`
(4 writer × 8ページプール × 64KBセグメント)で、まれに(〜1/10実行)
コミット済みの行ページのディスクイメージが一時的に読めなくなり
(`GetPage` が EOF 以降/チェックサム不一致を返す)、テールキャッシュと
連鎖して挿入が競合で失敗する。2026-10-03 の防御層(NULL PageRefガード+
headフォールバックヒール+writerのkConflictsリトライ)で SEGV と
大半の失敗は消えたが、根因(コミット済みページのwrite-backが
一時的に失われているように見える経路: FPW/checkpoint flush/eviction
の相互作用、あるいは未コミットリンクの可視性TOCTOU)は未固定。
診断の糸口: `Reopen()` 起動時の `GetPage(1..3)` invalid は正常な
ブートストラップなのでノイズとして無視してよい。

トランザクションエンジン強化(第7ラウンド):
Leaf/Branch削除undo空間衝突の根治、WAL redoのストリーミング化、
write-intent待ちの進捗リセット。同日の第10報は分析SQL完成度第8ラウンド。

## 2026-10-03 分析SQL完成度(第8ラウンド)の成果

**計測(TPC-DS SF=1, 94クエリ, コールドDB)から始めた:** スイート合計
13,400ms のうち **計画時間の合計が 9,097ms(68%)** を占め、上位は
q25 plan=3,059ms / q29 plan=2,021ms / q17 plan=474ms / q22 230ms /
q31 148ms と計画暴発が支配していた。

**根因(`TINYLAMB_PLAN_PROFILE=1` + 規則別タイマーで特定):**

- **`infer_filter_from_equivalence_class` が 1 試行で 2,175ms。**
  (a) ルール本体がクラス全走査の `LogicalProperties::AreEqual` を
  各メンバーごとに呼んでおり、`AreEqual` はさらに全クラスを走査するため
  O(述語数 × クラス数²)。(b) 根因に拍車をかけて
  `DeriveLogicalProperties` が join グループの式1本ごとに子のクラスと
  自己の同値ペアを**重複込み**で蓄積し、8表join のルートでクラス配列が
  式数(≈1,500)に比例して膨張していた。q17 が探索予算(50,000ステップ)
  を飽和していたのは枝刈りの問題ではなく、この走査がステップ単価を
  ~250µs まで膨らませていたため。

**修正(実装):**

- **[optimizer] 同値クラスの重複蓄積を除去**(`plan/cascades.cpp`
  `PushEquivalenceClass`)。`DeriveLogicalProperties` の全 push サイトを
  メンバー集合のソート済みキーで重複排除(増分更新時は既存クラスから
  シード)。ルートグループのクラス数は join 辺数オーダーに収まる。
- **[optimizer] 同値クラス推論を union-find に再構成**(同ファイル)。
  記録されたクラスは個々の join 式由来のペアなので、union-find でマージし
  a=b ∧ b=c の連鎖を透過的に推論する(従来は直接ペアのみ)。
  `AreEqual` の裸名(非修飾名)マッチは削除した —— 異なる関係の同名列を
  誤って同一視する正しさのハザードだった(推論が機会損失になるだけで
  誤推論しない、修飾名の完全一致に統一)。推論の重複判定は
  `ToString()` の線形再走査から文字列集合へ。
- **効果(q25 で実測):** explore 2,220ms → 42ms(53倍)、クエリ全体
  3.4s → 0.13s。q29 2,021ms → 40ms、q17 474ms → 39ms(予算飽和も解消)、
  q22 230ms → 0.8ms、q31 148ms → 0.3ms。**TPC-DS 94クエリの計画時間合計
  9,097ms → 4,942ms(−46%)、スイート合計 13,400 → 10,505ms(−22%)、
  全94クエリの行数が修正前と一致。** TPC-H 22クエリも全行数一致
  (Q17 391→278ms、Q14 425→325ms、Q20 446→341ms など全般に微減。
  残存の計画支配項は ROLLUP/set-op 枝の Prepare 内実行で、
  計測仕様上 plan に計上される実働である)。
- **診断の強化(いずれも env ゲート、無効時ゼロコスト):**
  `TINYLAMB_RULE_STATS=1` が規則別の試行回数に加えて**所要時間**を出すよう
  拡張(時間降順ソート)。`TINYLAMB_PLAN_PROFILE=1` で Prepare の
  parse-subprocess / memo.Build / explore / lower の各フェーズ時間を
  1行ずつ出力(将来の計画時間調査の足場)。
- **[frontend] GROUPING SETS 内のネスト ROLLUP/CUBE を実装**
  (`query/googlesql_ast_visitor.cpp`)。`GROUPING SETS ((k), ROLLUP(v))` の
  ようなリスト要素が ROLLUP/CUBE ノードのとき、その階層をリストへ
  ユニオン展開する(`... ((k), ROLLUP(v))` ≡ `((k), (v), ())`)。
  従来は「unsupported expression Rollup」の明示的拒否。GROUPING()/
  GROUPING_ID() の定数畳み込みは既存の枝展開機構がそのまま適用される
  (枝ごとのビットマスクを pin)。ネスト CUBE も同様(7列上限は共有)。
  ネスト GROUPING SETS はピン留めパーサが構文自体を拒否するため対象外。
  回帰: `SqlGroupingSetsTest.GroupingSetListUnionsNestedRollup` /
  `GroupingSetListUnionsNestedCubeAndList`。
- **回帰(plan):** `CascadesTest.InferFilterFromEquivalenceClassTransitiveChain`
  (ペア {a,b} と {b,c} から a へのフィルタが c に伝播すること。union-find
  マージの pin)。

**判明した既知課題(未修正・記録のみ):**

- 残存する TPC-DS 計画支配項(q22 ~200ms / q31 ~150ms)は grouping
  sets/CTE 枝の **Prepare 内実行**(ベンチマークの計測仕様上 plan に
  計上)で、枝ごとの 30k 行スキャンが本体。枝数に比例する構造コストで、
  M4 CTE 葉化/枝計画共有のさらなる活用が次のレバー。
- ピン留めパーサ由来の未対応(エンジン側では対応不可・明示的エラー):
  `GROUPS` ウィンドウフレーム単位、ウィンドウ ORDER BY の一部の式
  (`k % 2` 等の演算子式)、`WITH RECURSIVE name(col...)` の列リスト、
  ネスト GROUPING SETS 構文。いずれも parser アセット更新(SHA 再ピン)
  が前提。
- DML の非相関サブクエリ(旧 backlog D-8)は **既に実装済み**
  (`DecorateDmlWhere` の EXISTS/スカラー/量化比較の畳み込み。
  実機で DELETE ... WHERE k IN (SELECT ...) / UPDATE ... WHERE k =
  (SELECT MAX ...) / NOT EXISTS を確認)。本ファイルの D-8 行は
  陳腐化のため次回整理候補。

---

## 2026-10-03 トランザクションエンジン強化(第7ラウンド)の成果

**[P0根治] Leaf/Branchページの削除・縮小更新undo空間衝突。**
第6ラウンドでRowPageを直した同型課題のIndexページ側: `LeafPage::Delete`は
削除直後に`free_size_`を戻し、`LeafPage::Insert`はそれを即座に消費できる。
インテント窓内の並行insertがそのバイトを消すと、削除者のundo再挿入が
`InsertImpl`の`kNoSpace`で物理的に不可能になり、ランタイムアボートは
エンジン全体をfail-stopさせ、**再起動リカバリが同一ページに同一undoを
再生するため二度と開かない**。さらに`void LeafPage::UpdateImpl`経由の
縮小更新undoは`UpdateSlotImpl`の失敗を黙って捨てる(サイレント破損)。

修正はRowPageと同型の「TM側予約」: 削除/縮小更新のWALレコードのappendが
成功した直後、undoが再消費するバイト数を`TransactionManager::
ReserveIndexUndoBytes`に登録し(page_id×オーナー別、64シャード)、
トランザクションがactive tableを離れるとき(ForgetTransaction)に解放する。
admission(leaf/branchのInsert・Updateの伸長)は**他トランザクションの**
予約をfree_size_から控除する。自分の予約は自分を妨げない: undoはLIFOで
走るため、自分が後から消費したバイトは自分のundoが先に解放する。

設計上の重要な限定が2つ:

- **フェンス/フォスターは予約の対象外。** フェンスはadvisory(誤フェンスは
  fosterチェーン辿りで自己修復)、フォスターリンクは吸収済みセパレータと
  冗長。構造メタデータまで割引にすると、分裂/absorbの失敗契約が全面で変わり
  (`COERCE`前提のコードが45箇所以上)、availabilityが崩れる。エントリ
  レベル(削除・縮小更新)のみ割り引く。
- **同一トランザクション内のリトライは「変更前」地点でのみ安全。**
  LeafInsertのループ先頭の`leaf->InsertLeaf`はadmission→mutationの順で、
  ここでのkNoSpaceはゼロ変更が保証される。この一点でのみ(変更をWALに
  載せた後に失敗した場合、その部分状態はabort undoが担当し、同一txnでの
  再試行は部分適用された分裂の上に乗って**エントリを非到達化**する——
  実測で6437コミット中936行しかスキャンできなくなるデータ消失を確認済み)、
  ラッチを放出して`WaitAllIndexReservationsDrain`(write-intent上限が上限)
  で全予約の排水を待ち、再降下する。

あわせて構造操作の`COERCE`(プロセスkill)45箇所をステータス伝播に変換
(b_plus_tree.cpp 64 + leaf/branch 15)。失敗は文失敗→abort undoが担当し、
数か月稼働のプロセスを青天井のNO SPACEで落とさない。

計装: `tm.index_undo_reserved_pages`ゲージ(active=0で増え続けたら解放漏れ
——TPCCベンチが出力)。診断トグル`TINYLAMB_DISABLE_INDEX_RESERVATION`
(0=on, 1=off, 2=共有map無効, 3=txnベクタ無効, 4=割引のみ無効,
5=リトライのみ無効)。回帰:
`RecoveryManagerTest.PendingLeafDeleteReservesSpaceForAbortUndo` /
`PendingLeafShrinkUpdateReservesSpaceForAbortUndo` /
`PendingBranchDeleteReservesSpaceForAbortUndo`(修正前コードで失敗することを
確認済み)+ ゲージゼロ検査。
`RecoveryExtraTest.CheckpointFlushKeepsTreeCoherentUnderConcurrentInserts`
(4並列writer×分割嵐×チェックポイント)が5/5でパス。

**[リカバリ] WAL redoのストリーミング化。** `RecoverFrom`のredo収集は
窓内の全ページ操作レコードをメモリに持ち、DPT全ページをreplayまでラッチ
保持していた——チェックポイントが長時間失敗し続けたDBの再起動でメモリ
プロファイルがWAL窓(ギガバイト規模)に比例する。解析パスが先に完了するため
確定コミット集合は最初のバッチ前に揃い、PageReplayはpage_lsnガードで冪等
なので、**~64MiB/16kレコード単位のバッチでストリーミングreplay**しても
二重適用も再分類も起きない。再起動のメモリはバッチサイズに律速され、
DPT全体の事前ラッチも廃止。リカバリ系324テスト全パス。

**[並行処理] write-intent待ちの進捗リセット。** TPC-C(W=1, 32クライアント)
で5秒エスケープハッチが60秒に90回発火していた。ホルダーはアボートしておらず
(holder_abort_giveups=0)、実体は**ホット行の待機行列の末尾が進捗なしに5秒を
計上されてアボートされる**コンボイ(TPC-C deliveryは各districtの最古
new_order行を全トランザクションが削除しに来る)。修正: インテントの
**手渡し(ホルダー交代)を進捗とみなし、エスケープハッチ期限と250msの
holder-abort猶予を手渡し時点でリスタート**する。単一の動かないホルダーは
従来通り上限で打ち切られる(latch-cycle保護の維持)。
`TINYLAMB_INTENT_NO_PROGRESS_RESET=1`で旧挙動(A/B用)。
効果(交互ペア): engine aborts 4011-4178 → 2235-3076 (-25~30%)、
5秒打ち切り 90/33 → 12-44。なお測定中に観測した「tps 62・22.8Mアボート」の
livelock様事象は**/tmpクォータ枯渇(ENOSPC)**が原因だった(クォータ解放後は
再現せず。負荷試験の前には`df /tmp`を確認すること)。

**監査済み(変更不要):** チェックポイント後のWAL破棄——4項フロア
(begin LSN / 最古DPT recLSN / 最古active txn first LSN / 未fsync FPW) +
`lowest_segment_`カーソル + 境界バリデータ + レコード境界規則を再監査。
フロア項はすべて`WaitForDurable(durable_end)`以前にappendされたLSNに収束
するため、fsync前セグメントのunlinkは起きない。

**判明した既知課題(未修正・記録のみ):**

- 破棄済みページidのインデックス予約がリサイクル先の新インカーネーションを
  過剰に圧迫する(旧インカーネーションのundo保護。正しさに影響なしの
  availability側の非効率。DestroyPage時の予約破棄を検討)。
- TPC-Cの`sql_collect_total_ms`がスループット支配項(実行経路の話で
  トランザクション層の外。次回以降のexecutorラウンド候補)。
- `payment`の`customer`行でのlatch-cycleエスケープハッチが依然稀に発火
  (進捗リセットは単一ホルダー詰まりを防がない——それはhatchの仕事)。

## 2026-10-02 分析SQL完成度(第7ラウンド)の成果

**計測(TPC-DS SF=1, 94クエリ, 共有環境の高負荷帯)から始めた:**
実行支配の q5(646ms)/q16(500ms)/q80(494ms)と計画支配の
q17(312ms)/q25(283ms)/q29(280ms)/q22(268ms)が上位。
q5 を ROLLUP 枝数で A/B したところ、枝数に比例してコストが増え
(1枝 0.15s → 3枝 0.50s user)、`scan_rows=474,472` が CTE 本体
3本(≈118k行)の約4倍で、**grouping set 枝ごとに CTE 本体が
再実行されている**ことを特定した。

**修正(実装):**

- **[frontend/executor] grouping sets 展開が枝へ WITH 複製を載せないように
  した**(`query/googlesql_ast_visitor.cpp` + `query/statement.hpp` の
  `ClearWithQueries()`)。ROLLUP/CUBE/GROUPING SETS の UNION ALL 枝は
  元文の丸ごとコピー(WITH 含む)で、各枝の実行が `ExecuteQuery` の
  WITH ブロックで CTE 本体を再実行して `ctes` マップを上書きしていた。
  宣言は先頭文に保持し、枝の参照は実行時の inherited CteMap で解決する。
- **[executor] UNION ALL head コピーの WITH 再実行を除去**
  (`executor/relational.cpp` の UNION ALL 折り畳み)。先頭文の WITH 実行後に
  `head` コピーが同じ本体をもう一度実行する2重実行を
  `head.ClearWithQueries()` で除去。上記修正後も残っていた
  scan_rows 118,618(=本体2回分)を 118,618→1回分へ。
- **効果(TPC-DS SF=1, 同一刻のスナップショットA/B):** q5 562→191ms
  (−66%)、q82 582→37ms、q80 487→148ms、q78 −116ms、q77 −72ms。
  スイート合計 10,701→9,201ms(−14%、並行負荷ありの条件)。
  **全94クエリの行数が一致**(決定論比較は既存の
  `TpcdsQueryTest` 系で継続)。回帰:
  `SqlGroupingSetsTest.CteBodyExecutesOncePerGroupingSetExpansion`
  (結果 pin + `scan_rows` が本体1回分に収まること)。
- **[executor] `RANGE BETWEEN INTERVAL n UNIT` ウィンドウフレームを実装**
  (`executor/detail/window_eval.cpp`)。時系列ローリング窓
  (`SUM(x) OVER (ORDER BY d RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND
  CURRENT ROW)`)がこれまで「RANGE offset requires one constant key」で
  拒否されていた。offset を数値に加えて INTERVAL 定数
  (未折り畳みの `IntervalExp` と折り畳み済み TEXT 定数の両形)として受理し、
  DATE 順序キーに対して `TryAddDateIntervalDays` 経由のカレンダ演算で
  境界を計算(MONTH/YEAR の月末クランプは `DATE + INTERVAL` と同一)。
  PRECEDING/FOLLOWING × start/end の4位置 + DESC の鏡像、PARTITION BY 併用、
  NULL キー(ピアグループへ縮退)を対応。INT64/FLOAT キー + INTERVAL は
  「RANGE INTERVAL offset requires a DATE order key」の明示エラー
  (黙って数値化しない契約)。回帰:
  `WindowSqlCov.RangeIntervalFramesOnDateKeys`(ローリング/MONTH クランプ/
  FOLLOWING 端/DESC/NULL キー/DATE 以外キーの拒否)。

**判明した既知課題(未修正・記録のみ):**

- **q17 が Cascades 探索予算(50,000 steps)を飽和**(8表結合、313ms の
  計画時間の大半、`TINYLAMB_RULE_STATS=1` で確認)。join 規則群が
  各 ~2,760 回試行され best-so-far 縮退で返る。実行は 14ms なので
  計画時間のみが無駄。予算を増やす方向ではなく、探索の枝刈り
  (interesting orders の列挙幅、predicate push の fixpoint 回数)が
  次のレバー。
- **スカラ関数を含む GROUP BY は Cascades ブリッジから漏れて
  relational 経路へ落ちる**(`NeedsRelationalEvaluation` の
  kFunctionCallExp が `__value_table_value` 以外の全関数で true のため。
  Q9 の `EXTRACT(year ...)`、`UPPER(x)` など)。本ラウンドで
  ブリッジゲートの局所緩和を実装・検証したが、**Q9 が 8.4s → 122s に
  悪化したためリバートした**。Cascades コアが選んだ 6 表結合計画の
  推定が根で崩れている(Project/Select の `estimated rows: 0`、
  join 推定が実行の 10-100 倍楽観)ため、ゲートを緩める前に
  コストモデルの较正が必要。relational 経路の
  `HeuristicJoinOrder=greedy_table_rows` も filtered 後の行数ではなく
  生テーブル行数で開始表を選ぶため、p_name フィルタで 2k 行になる
  part が最後に来る(Q9 の 6M 行中間を 3 join 分抱えている)。
  次のレバー: (a) greedy 開始選択にフィルタ後行数と一意キー方向を反映、
  (b) Cascades コアの root 推定復元。
- **並行セッションの進行中変更が高負荷時に顕在化**:
  `googlesql_ast_visitor.cpp` の kFunctionCallExp ゲート整理(2026-10-02
  04:33時点)の下で `TpcdsQueryTest.AllQueriesExecuteDeterministically` が
  q91 空結果で失敗する状態を観測(本ラウンドの変更をリバートした
  スナップショットでも再現するため、当該整理の WIP が起源)。
  当該ファイルを触る場合は q91(`hd_buy_potential LIKE 'unknown%'` +
  `ORDER BY SUM(...)`)で確認すること。

2026-10-02 加筆(第8報)。トランザクションエンジン強化(第6ラウンド):
ランタイムアボートの無待機化、チェックポイント後のWAL破棄の監査、
そして削除undo空間予約によるエンジン全体 fail-stop(P0)の根治。
前報(2026-10-01 第7報)は並列実行の完全決定論化。

## 2026-10-02 トランザクションエンジン強化(第6ラウンド)の成果

**計測(TPC-C W=2, 32クライアント, tmpfs, 交互ペア)から始めた:** 第5ラウンド
記録の「エンジンアボート 2.6%」の実体を追ったところ、ランタイム `Abort` が
undo ウォークの開始前に `WaitForDurable(BufferedLSN())` —— **自分以外を含む
全トランザクションのバッファ済みレコードの fsync** —— を待っていたことが分かった
(undo ウォークの `ReadLog` がディスク上のレコードしか読めないため。アボート記録の
恒久化は不要: クラッシュで揮発分が失われても再起動 loser cleanup が同一の
ロールバックを再現する)。

**修正(実装):**

- **[並行処理/リカバリ] ランタイムアボートの undo 前待ちを撤去。**
  `Logger::TryCopyBuffered` を新設: リングバッファ内の揮発バイト列を
  ロックフリーで読む(範囲は `[max(ring_floor, B - capacity), B)`。
  `ring_floor_lsn_` は open 時 WAL 末端に初期化 —— 再オープン直後のリングは
  ゼロ初期化なので、ファイル由来バイトを絶対に捏造しない。コピー後に
  buffered の再読込で「コピー中に上書きされなかった」を検証
  (ポインタは単調なので、通過検査で十分)。`RecoveryManager::ReadAtDetailed`
  は生 Logger への任意リンク(`SetLogger`、PageStorage::Create で配線)を
  見て揮発テールをリングから先に読む。`TransactionManager::Abort` の
  事前 `WaitForDurable(BufferedLSN())` を削除し、undo ウォークを待ち無しで
  開始する。レコードがリング外かつ未ディスク(WAL 書き出し滞留 > 8MiB)の
  ときだけ 1 回のグループコミット待ちにフォールバック
  (`read_undo_record`)。あわせて `ReadLogDetailed` の増加窗口プローブは
  生 Logger 接続時 `BufferedLSN` にクランプする: リングは buffered 超過を
  拒否しファイルにはまだ書き出されていないバイトを 4096 バイト窗口で
  突き抜けると、完全に読める末端レコードに kShort/kEof を捏造する
  (ランタイム undo はまさに末端レコードを読む。worker の初回 write が
  遅れた瞬間に決定的に失敗するレースとして coverage テストで捕捉)。
  効果: アボート 1 件あたり 1 グループコミット周期
  (= 全員のレコードを巻き込む fsync)が消える。
  回帰: `RecoveryManagerTest.RuntimeAbortUndoDoesNotWaitForWalFlush`
  (60 秒 sync 間隔のスタックで、アボート中に DurableLSN が動かないことを
  決定論的に pin)、`LoggerTest.TryCopyBuffered*` 2 件
  (リング窓/ラップ境界/再オープン後の ring_floor)。
- **[長時間稼働/P0根治] 削除 undo の空間予約 —— エンジン全体 fail-stop の
  根本解決。** A/B 計測中に TPC-C delivery が実戦で fail-stop した
  (hatch 5 秒タイムアウト → アボート → undo の再挿入が `kNoSpace` →
  fail-stop)。根本: `RowPage::DeleteRow` が空きバイトを即座に
  `free_size_` に返す一方、write intent は **スロット** だけを保護し
  バイトは保護しないため、インテント窓内の並行 INSERT(新規スロット経由)が
  解放されたバイトを丸ごと消費でき、削除者の undo 再挿入が物理的に不可能に
  なる。さらに再起動リカバリは同一ページの同一 undo を再生するため、
  **データベースが二度と開かない**恐れまであった。
  修正: `DeleteRow` はホールに **行サイズを保持**し(offset のみゼロ)、
  `RowPage::Insert` のアドミッションは「他者の未解決 delete が intent を
  保持するホールのバイト」を利用可能枠から控除する(ホールの intent 取得に
  成功したらそれは利用可能 —— 削除者は解決済みで undo はもう不要)。
  `GetRow` はホールを(サイズに関係なく)空で返す。ページフォーマット変更は
  なし(free_size_ の意味も不変。デフラグは真の空きを再計算するので
  予約と整合)。回帰:
  `RecoveryManagerTest.PendingDeleteReservesSpaceForAbortUndo`
  (修正前コードで失敗することを確認済み: 並行 INSERT が予約を食い
  アボートが失敗する現行動作を再現)。
- **[長時間稼働] チェックポイント定期ワーカに例外境界。**
  `CheckpointManager::WorkerThreadTask` の `WriteCheckpoint()` を try/catch
  で包み、DPT スナップショットやレコード直列化の一時的 bad_alloc が
  ワーカスレッドから脱して std::terminate で数か月稼働のプロセスを
  落とせないようにした(チェックポイント失敗は「次回間隔でリトライ」が
  既定契約)。起動時の EOR チェックポイントは従来通り fail-visible。
- **[性能] ロガーの満杯 park が worker を起こせないロストウェイクを修正。**
  リング満杯で park した producer は `flushed < buffered` が偽になる状態
  (全バイト書き出し済み・未排水)を作れず、旧アイドル述語は満杯 park 者を
  見ていなかった —— producer は worker の idle 待ち(最大 sync_interval_)
  が満了するまで排水されない。`sync_interval_` 1ms の通常配備では測定不能な
  ストール、大間隔設定では区間全体の書き込み停止として現れた。
  修正: park 自身が `work_cv_` を起床し、アイドル述語に
  `full_buffer_waiters_ != 0` を追加(どちらも work_mu_ 下で競合しない)。
  60 秒 sync 間隔のテストで 60 秒 → 2ms を確認。
- **[性能] デッドロック検出器の 2ms バックストップを 25ms へ。**
  サイクル形成は `AddWaitForEdge` の通知で即起床するため、タイムド待ちは
  通知漏れの保険にすぎない。待機者が 1 つでも存在する限り 500 回/秒
  起きていたのを 40 回/秒へ。
- **[運用] `Logger::Create` が起動時に `*.log.meta.tmp` 残骸を掃除**
  (master record の `.tmp` 掃除のWAL版。クラッシュが temp 書き込みと
  rename の間で死んだ場合の溜まり)。

**検査済み(変更不要):** WAL リサイクル床の4項(begin LSN / 最古 DPT
recLSN / 最古アクティブ txn first LSN / 未fsync FPW)と
`lowest_segment_` カーソル、`RecycleUpTo` の境界バリデータ、リテンション/
ボリューム空き容量警告、master record の atomic 公開、プランキャッシュ LRU、
GC・fd キャッシュ・FPW マップの有界性 —— 長時間稼働の観点で既存実装は
健全だった。Leaf/Branch ページの同型削除 undo 衝突(リーフは key 単位で
同一の窓が作れる)は本ラウンドでは未対応の既知課題。

**測定:** TPC-C W=2/32 クライアント交互ペアの結果は
BenchmarkHistory.md 参照。同環境の実行間ばらつき(±13%)に対し tps 本体は
有意差なし(アボート 2.6% の fsync 削除は tail 寄与が主で、平均 tps には
埋もれる)。機構効果は上記回帰テストで決定論的に pin。

**判明した既知課題(未修正・記録のみ):**

- Leaf/Branch ページの削除 undo 衝突(上記)。RowPage と同型の予約が要る。
- `RecoverFrom` の redo 収集が窓内の全レコードをメモリに持つため、
  チェックポイントが長く失敗し続けた DB の再起動でメモリプロファイルが
  WAL 窓に比例する(ストリーミング redo は別セッション規模)。
- TPC-C の write-intent 5 秒エスケープハッチは 60 秒走行で約 1 回発火
  (既知の delivery ホットロウ ラッチサイクル。hatch 自体の構造対策は
  第4/5ラウンド記録の通り)。

## 2026-10-01 分析SQL完成度(第6ラウンド)の成果

**背景:** TPC-DS q65/q67/q70 が実行ごとに内容が揺れる問題
(並列 DOUBLE 部分集約のマージ順、として 2026-09-30 に root-cause 済みと
思われていた)を、ランク境界でテスト可能な形にして追求した。 bisect の結果、
揺らぎの実体は集約ではなく **3つの並列スキャン/集約実装すべてにわたる
「アトミックカウンタの morsel/chunk grab」** だった。ワーカーがどの
morsel(または8192行チャンク)を処理するかがスケジューリング依存で決まり、
(1) 各ワーカーの部分和の内容、(2) shard 連結後のリレーション行順、
(3) hash map の挿入順=グループ出力順、のすべてが実行ごとに変わる。
q65 の内側 `(SELECT ... GROUP BY ...)` 単体を 5 回実行するだけで
生スキャンの行順が揺れることを fingerprint 比較で実証し、段階的に潰した。

**修正(実装、すべて「静的ローテーション割当」に統一):**

- **[並列スキャン/executor] `ParallelScan` を morsel 順出力に作り替え**
  (`executor/parallel_scan.{hpp,cpp}`)。worker w は morsel w, w+W, ... を
  所有し、デコード済みチャンクは morsel インデックス付きで手渡し、
  コンシューマは morsel 0, 1, 2, ... の順に排出する。逆圧は
  `inflight_chunks_ < cap || morsel_index <= next_out_morsel_` の
  優先接続(コンシューマが待っている morsel のチャンクは上限超過でも受ける)
  でデッドロックフリーを保証。morsel 0 が未取得のまま後続 morsel が
  キューを埋める競合は静的所有で構造的に発生しない。
  worker filter の parked 行は drain 時に RowPosition 順へソート
  (旧実装は push 順=競合順)。これで **executor 系の並列スキャン出力は
  ページ(=クラスター化順)の決定論関数になった**。D-9(保存順並列
  フルスキャン)の器もここで完成。`EmitsChainOrderedScan()` を並列に
  広げるのは別判断(morsel 順 = クラスター化順なので技術的には可能)。
- **[並列集約/executor] `HashAggregateExecutor::BuildParallel` と
  `ParallelAggregationExecutor::Next` を turn-taking に**。worker w は
  バッチ w, w+W, ... を担当(input_mutex 下の turn カウンタ+condvar)。
  stop/疲弊フラグは待機者の lost-wakeup を防ぐため input_mutex 下で書く。
  子のバッチ列が決定論的なら(ParallelScan の morsel 順出力と合わさり)
  部分和も完全決定論。
- **[並列集約/relational] グループ化 chunk fold を静的ローテーションに**
  (`executor/relational.cpp` ExecuteGroupedSelect の parallel grouping)。
  worker w は 8192 行チャンク w, w+W, ... を担当。kParallelGroupMinRows
  (262144)以上で発火。
- **[並列集約/relational] `RunTypedParallelStreamAggregate` の morsel
  ループを静的ローテーションに**(単一表 GROUP BY の typed 経路)。
  fill キャッシュの publish は既に morsel 順だったため、これで全部が揃う。
- **[並列スキャン/relational] `TryParallelTableScan` を静的ローテーションに**
  (`executor/detail/scan_filter.cpp`)。**これが q65/q67 の実際の揺らぎ源**。
  relational 経路の LoadSource が持つ第3の並列スキャン実装で、worker
  shard を worker 順に連結するため、grab 競合がそのままリレーション行順の
  競合になっていた。date_dim(365行, 1 morsel)は決定論、store_sales
  (30000行, 24 morsels)が毎回違う順で返ることを source fingerprint で
  実証して特定。

**検証:**

- フル ctest: **3,820/3,820 パス**(意図的スキップ 1 件のみ)。TPC-DS クエリ
  スイート 14/14。TSAN(build-tsan, detect_deadlocks=0): executor_feature 98、
  parallel_exec_coverage、grouped_aggregate 31、tpcds 決定論テストのすべてで
  data race 0。
- TPC-DS SF=1 全94クエリ: TPCDS_TOTAL 5,740-5,896 ms / failed=0(最遅 q5
  490 ms)。TPC-H SF=1 全22クエリ: 最遅 Q9 2,355 ms / Q21 2,053 ms。
  決定論化(静的ローテーション/turn-taking)による性能劣化は観測されない
  (並行負荷ありの条件のため、過去記録との厳密比較ではなく兆候なし確認)。
- 初期調査でフルスイートが 50 分以上沈没したように見えた件は、並行セッション
  の run-clang-tidy -j6 + ビルド + テストによる CPU 枯渇で、デッドロックでは
  なかった(静穏時に同一スイートが 45 秒で完走、20 秒 stall ウォッチドッグ
  発火 0 回で確認の上除去)。

**テスト:**

- `TpcdsQueryTest.ParallelFloatAggregatesAreBitDeterministic`(新設):
  q65/q67/q70/q86 を 6 回実行し、ウォーム実行(2回目以降)間で
  `%.17g` 完全一致を要求。**1回目だけ異なるのは仕様**(初回実行の
  plan feedback が統計を更新し、以後の計画が変わるため)で、
  ウォーム実行間の完全一致が決定論の契約。
- `TpcdsQueryTest.QueryPipelineStagesAreBitDeterministic`(新設):
  q65 のパイプライン段階(素スキャン / join / grouped detail / avg /
  派生素通し / sb⋈sc)ごとに fingerprint を pin。並列演算子が順序を
  揺らしたらこのテストが段階名を出す。
- `TpcdsQueryTest.AllQueriesExecuteDeterministically`: q65/q67 の
  content skip を「コールド1回目 vs ウォーム」比較の代わりに
  「ウォーム vs ウォームの完全一致」へ強化。

**判明した既知課題(未修正・記録のみ):**

- cold 実行と warm 実行で計画が変わる(plan feedback)ため,
  「同一DB・同一クエリの1回目と2回目の結果の完全一致」は契約に含まれない。
  EXPLAIN の計画形状も同様。

## 2026-10-01 トランザクションエンジン強化(第5ラウンド)の成果

**計測(TPC-C W=2, 32クライアント, --profile-waits)から始めた:**
エスケープハッチの発火 gauge は 60 秒走行で 0、`write_intent.conflicts`
約1,500/1.3M attempts (0.11%) と待ち自体は健全だった。一方で delivery 等
のエンジンアボートの first_error がすべて「write intent wait timed out」
と表示されており、第4ラウンドの分析(エスケープハッチが実戦で発火)は
このメッセージの誤表示に引きずられていた可能性が高い —— AddWriteSet の
全失敗経路(wound・policy拒否・try-mode競合)が同じ文言を流用していた。

**修正(実装):**

- **[診断/並行処理] インテント取得失敗の理由を区別して伝播。**
  `Transaction::IntentWaitFailure`(kConflict/kWounded/kHolderAborting/
  kTimeout/kFailStopped)を記録し、`executor/index_scan.cpp` のエラー文言が
  実際の理由(「aborted as a deadlock/wound victim」等)を出す。これで
  postmortem が 5 秒タイムアウトと通常競合を混同しなくなる。
- **[並行処理] ホルダー・アボート検知の高速ギブアップ。**
  `Transaction::MarkAbortStarted`(Abort 入口で全 write 経路に設定)を
  待機者側が `kHolderAbortGiveupDelay`(250ms)以上待った後に観測したら、
  5 秒のエスケープハッチを待たずに即座に諦める。ラッチサイクル(待機者が
  ページラッチを保持したまま待ち、ホルダーのアンドゥがそのラッチを必要と
  する)は検出器から見えず、待機者しか解錠できないため。250ms の猶予は
  「クリーンな高速アボートの後にインテントを勝ち取る」既存契約を保ち、
  スタックしたアボートだけを切り捨てる。gauge
  `mvcc.write_intent.holder_abort_giveups` を新設(TPC-C が出力)。
  kDeadlockDetect 経路では give-up 時に wait-for エッジも自分で削除
  (stale エッジは偽サイクル検出の種)。
- **[運用/長時間稼働] WAL ボリューム空き容量テレメトリ。**
  チェックポイント毎に `std::filesystem::space` を見て、閾値未満
  (TINYLAMB_WAL_FREE_WARN_BYTES、既定 1 GiB)ならレート制限付き WARN
  (ENOSPC は設計通り fail-stop なので、その前段の傾向監視)。
  `Logger::SetFailed` も ENOSPC/EDQUOT 時に WAL ボリュームの空き/全容量を
  1 行で出す。master record の `.tmp` 残骸(書き込みと rename の間で
  クラッシュした場合)を CheckpointManager 構築時に掃除。
- **テスト**: `WriteIntentWaitTest.WaiterGivesUpWhenHolderStartsAbort`
  (決定論: フラグ直接設定で 10s 待ち上限を待たないことを検証)、
  `AbortArmsHolderAbortFlag`(全アボート経路でフラグが立つ)、
  `CheckpointTest.ConstructorRemovesStaleMasterTmp`。
  `DeadlockDetectGrantsAfterHolderMovesOn` /
  `DeadlockDetectorBreaksThreeTransactionCycle` は新契約に合わせ両成果を
  許容する形に改訂(クリーンアボートなら待機者がインテントを勝ち取る)。

**測定(本環境 / taskset 0-15, tmpfs, 60s×3 平均):** tps 930-944
(変更前 963、±2.5% は実行間ばらつき内)、engine aborts 2.6% 不変、
delivery 平均レイテンシ不変。本環境ではラッチサイクルが稀(ハッチ発火 0)
のため効果は観測外だが、機構テストで 5s→250ms を確認。**エラーメッセージ
の分類により、本環境の delivery アボートの実体は「conflicting writer」
(try-mode競合)と死毒(wound)であることが判明** —— 第4ラウンド記録の
「5秒エスケープハッチが実戦で発火」は誤表示の可能性大。

**判明した既知課題(未修正・記録のみ):**

- 本環境(linux 6.x, 32 core, 共有デスクトップ負荷あり)では TPC-C の
  絶対値が過去記録(5,000+ tps)より低く、A/B は同一条件下ペア比較が必須。
  perf(paranoid=4)と ptrace(yama=1)が使えないため、実行中プロファイル
  は callgrind か --profile-waits に限る。sql_collect(実行時間)が
  スループットの支配項。

## 2026-09-30 分析SQL完成度(第4ラウンド)の成果

**達成:**

- **TPC-DS サブセット 60 → 94 クエリ**(残り5本は q8/zip定数400、
  q14/q24/q64/q66 が fixture 列の大規模追加待ち)。fixture に
  `web_site` / `web_page` / `catalog_page` / `ship_mode` / `time_dim`
  (1分粒1440行) を追加し、`web_sales` に `ws_sold_time_sk` /
  `ws_web_site_sk` / `ws_web_page_sk` / `ws_ship_date_sk` /
  `ws_warehouse_sk` / `ws_ship_mode_sk` / `ws_ship_hdemo_sk` /
  `ws_ship_addr_sk` / `ws_ext_ship_cost`、`web_returns` に
  `wr_returning_*`、`catalog_sales` に `cs_sold_time_sk` /
  `cs_catalog_page_sk` / `cs_ship_mode_sk`、`catalog_returns` に
  `cr_catalog_page_sk` / `cr_call_center_sk` / `cr_returning_*`、
  `call_center` に `cc_name` / `cc_manager`、`customer` に
  `c_last_review_date` を追加。**すべて post-hoc aux RNG / index 派生
  で既存ピンは1バイトも不変**(60クエリ全テストで確認)。web_sales の
  注文番号は 5行/注文 (1+(i-1)/5, index派生) にして q94/q95 の
  同一注文×複数倉庫自己結合が当たるようにした。
  新規移植: q5 q25 q30 q32 q36 q38 q39 q41 q44 q45 q49 q51 q54 q56 q57
  q58 q62 q71 q72 q74 q78 q80 q81 q82 q83 q85 q88 q90 q91 q94 q95 q96
  q97 q99 (34本)。全94本が実行成功・空結果0。
- **[性能] 関係経路 ordered join の述語配置と内側 run 並べ替え**
  (`executor/detail/planning_heuristics.cpp`)。従来の ordered path
  (外部結合/USING 存在時)は (a) 2ソース以上の WHERE 連言を CROSS ステップ
  にしか配置できず、`JOIN ... ON` の inner 連鎖では結合カスケードの上端で
  評価されていた (q72: cs⋈inv が 1.27M 行の後段フィルタ)。また (b) FROM の
  構文順序を強制し、フィルタ済み次元テーブルを先に結合する形が表現不能
  だった。修正: (1) inner/cross の各ステップで「参照ソースが全部 join
  済み」になった連言を ON に畳み込む(σp(A⋈cB)≡A⋈(c∧p)B、等価キーは
  hash 化され残余は再検証)。外部結合の ON は従来通り不変。(2) 外部結合/
  USING/lateral/ネストで区切られた最大 inner run のみ貪欲に並べ替え
  (接続性 + ON 束縛可能性ゲート + 最小行数アンカー)。正しさの担保:
  ON 未束縛ソースの早期結合禁止、どの候補も束縛不能なら構文順序へフォール
  バック、`TINYLAMB_DISABLE_JOIN_REORDER=1` で無効化。**A/B: 全94クエリの
  結果が FP 1e-6 相対許容で一致**。q72 11.8s → 0.12s (100倍)。
- **[性能] q83 移植の浮いた date_dim クロス積を除去。** テンプレートの
  アンカー週フィルタを外した適応で date_dim が述語なし残留し
  2500×1200×2191×3 ≒ 17.5M 行の交積になっていた (50.6秒)。date_dim を
  CTE 本体から除去して即時。
- **テスト**: `TpcdsQueryTest.AllQueriesAvoidCrossProducts` を「Cross Join
  文字列の存在」から「estimated rows による無境界判定」に改訂
  (q88/q96 の次元×次元 16行クロスは正当な bushy star 形状。EXPLAIN の
  行数推定をキーにするためテストは ExplainSql ベースに変更)。
  manifest は94本に更新。

**判明した既知課題(未修正・記録のみ):**

- **並列部分集約の浮動小数マージ順が非決定**(q65/q67/q70 の rank 境界
  揺らぎの根)。morsel→ワーカー割当がスケジューリング依存のため、
  ワーカー部分和の内容が実行ごとに変わり、ワーカー順マージの最終桁が
  揺れる。根治には (a) 決定的 morsel 割当 + (b) グループ集約では
  グループキー→ワーカーの固定シャッフル(各グループの部分状態が単一
  partial に集約される)が必要で、並列集約の再設計規模。A/B 検証では
  q67/q70 以外は全クエリが許容内で一致した。
- **q5/q80/q76 の計画 2-3秒**: ROLLUP/UNION ALL 枝ごとの CTE 計画共有の
  対象(M4 CTE 葉化の優先候補として既記録の延長)。
- `common/serdes_test.cpp` は並行セッションが作成中の未追跡ファイル。
  配列初期化の閉じ括弧欠落でビルドが壊れていたため `4)}};` →
  `4)}}};` の1文字修正のみ実施(内容には触れていない)。


## 2026-09-30 トランザクションエンジン強化(第4ラウンド)の成果

**計測(TPC-C W=2, 32クライアント, `--profile-waits`)から始めた:**
コミット系グローバルロックの待ちは第2ラウンド時点と同様に無視できる水準
(`wait.mvcc_commit_shards` 43ms/15s、`wait.wal` 平均58µs)。一方で
エンジンアボート424件中に **write-intent待ちの5秒エスケープハッチが
実戦で発火**していた(delivery の new_order DELETE)。ラッチサイクル
(待機者がページラッチを保持したまま待ち、ホルダーのアボートがその
ラッチを必要とする)はデッドロック検出器から見えないため、待機者側の
タイムアウトが唯一の解錠手段 — すなわち **可視化と運用チューニングが
このラウンドの正解**だった。レジストリ/タイムスタンプのシャーディングは
見送り(測定根拠なしの複雑化を避ける)。

**修正(実装):**

- **[運用] リカバリの進捗とサマリの可視化。** 巨大な保持WALを持つDBの
  再起動が数分間無言で、進捗とハングの区別が付かなかった。
  `RecoverFrom` の全走査フェーズ(torn-tail scan / analysis / redo
  collection / torn-page sweep)に2秒レート制限の進捗ログを追加し、
  完了時に1行サマリ(WAL窓、コミット/敗者数、WAL再構築ページ数、所要ms)を
  出すよう修正。
- **[性能] torn-pageチェックサム掃討の並列化。** 掃討はDPT外の全ページ
  (1..scan_high_water)をpreadで分類するため、大規模DBの再起動時間の
  主因だった。分類のみを `RecoveryWorkerCount` ワーカで並列化
  (SPR自体はアロケータ再入のため逐次、昇順で実行)。broken検出順は
  ソートして従来と同じ決定論を保持。
- **[堅牢性] リカバリ成功後の即時チェックポイント(end-of-recovery
  checkpoint)。** これまでは再起動直後のリプレイ済み状態がWAL上にのみ
  存在し、(a)リカバリ直後にクラッシュすると全リプレイを再支払い、
  (b)リカバリに必要なWALは定期チェックポイント(既定10秒)が来るまで
  リサイクル不能だった。`PageStorage::Create` が `RecoverFrom` 成功後
  (WAL空のときはスキップ)に同期的に1回チェックポイントを書き、新 master
  record を公開する。失敗は非致命(定期ワーカが再試行)。
  PostgreSQLのend-of-recovery checkpointと同型。
  回帰: `RecoveryExtraTest.CrashRecoveryPublishesMasterRecordWithoutTimer`。
- **[診断] WALリテンション警告が「どの項がフロアをピン留めしているか」を
  特定して出力。** 以前は候補3項の列挙だけだった。`RecycleFloorTerms`
  (begin LSN / 最古DPT recLSN+page id / 最古アクティブtxn first LSN+txn id
  / 未fsync FPWフロア)をWriteCheckpointが記録し、argminを警告に名前付きで
  出す。長時間稼働でWALが肥大したとき、3つの対策(長期txnを終わらせる /
  ホットページのフラッシュを待つ / fsync遅延を調査る)のどれが正解かが
  ログ1行で分かる。
- **[可視化] write-intent待ちタイムアウトの計装。** エスケープハッチの
  発火ごとに `RuntimeStats().write_intent_wait_timeouts` を+1(TPC-C
  ベンチの `mvcc.write_intent.wait_timeouts` として出力)し、txn id と
  行位置を60秒レート制限でLOG(WARN)。健全な配備では0付近、
  ラッチサイクル頻発で即座に上がる。
- **[運用] `TINYLAMB_WRITE_INTENT_WAIT_MS` でエスケープハッチ上限を起動時
  設定可能に**(既定5000ms不変。短いOLTP専用配備では例えば500msに下げて
  最悪スタール長を1/10にできる。長いトランザクションが意図を長く保持する
  ワークロードでは既定維持)。

- **[堅牢性→実バグ修正] リカバリ後のランタイムアボートが永久にkCorruptで
  失敗する形状を修正。** `RecoverFrom` のフリーリスト再構築が、復元する
  アロケータ高位水準(max_page_count)より上の空きページを含む状態を
  復元できていた(WAL証拠は部分的にしかmaxを再導出しないため。敗者/
  補償済み割当は意図的に除外される)。この状態でリサイクル割当が起こると
  そのアンドゥペイロードの自己整合検証(recycled_next ≤ old_max が
  head ≤ max のとき要求される)が恒久的に拒否し、当該トランザクションの
  アボートが必ずkCorruptでfail-stopした。session fuzzer seed 29で発現
  (EORチェックポイントがページのtorn/非tarnを変えたことで潜在バグが
  顕在化)。修正: 再構築したフリーリストと保存済みフリーチェインの
  先頭を required_allocator_max に折り込み、高位水準が全フリーページを
  カバーするように復元する。fresh割当がmax+1を発行するため
  「フリーページ ⊆ max」は二重割当防止の負荷に耐える不変条件。
  idの即時再利用はフリーリスト経由で維持される(fresh-undoでmax縮小を
  ピンしていた2テストは新しい不変条件に更新:
  `FreshLoserAllocationUndoRestoresIdForImmediateReuse`,
  `AllocationCrashUndoHandlesZeroLsn`)。あわせて割当ペイロード検証の
  失敗メッセージが全デコード値を出すように、ランタイムアボートの
  `LogUndoWithPage` 失敗が無音でなかった(レコードとステータスをWARN)。
  決定的ガードは SeededSessionsHoldState の種0-63スイープ(seed 29含む)、
  補助回帰として
  `RecoveryExtraTest.AbortedRecycledAllocationUndoSurvivesRestart`。

**検査済み(変更不要):** リカバリ後の `WaitForDurable` 依存(D4)と
リサイクルの整合(コミットLSNはリサイクル前に必ずdurable)、
`wal_snapshot_lsns_` / `unsynced_pages_` のライフタイム(retention floor
で刈り込まれる、ページid総数で有界)、`Logger::AddLog` のENOBUFS系
(失敗時fail-stopで設計通り)。TSAN lock-order-inversion 33件は
docs/lock_order.md 記載のフェーズ分離偽陽性とSPRの meta↔target 再入
(RecoverFrom コメントの通りメタページを最後にラッチする設計)で、
データレースは0。

---

2026-09-29 加筆(第2報)。分析SQL完成度セッション(第3ラウンド): star修飾子、
バイアス結合キー、TPC-DS 60クエリ。前報(同日 第1報)はトランザクションエンジン
強化第3ラウンド。

## 2026-09-29 分析SQL完成度セッション(第3ラウンド)の成果

**達成:**

- **`SELECT * EXCEPT (...)` / `SELECT * REPLACE (expr AS col, ...)` を実装**
  (これまで「unsupported expression StarWithModifiers」の明示的拒否だった)。
  設計: `NamedExpression` に `star_modifiers` ペイロード
  (`expression/named_expression.hpp` の `StarModifiers`)を追加し、star の
  expression そのものは従来どおり `ColumnValueExp("*")` のまま所以外の
  star 判定(~44 箇所)を無変更で通す。修飾子は star を列へ展開する
  chokepoint だけで消費される:
  (1) `query/sql_engine.cpp` の QueryData 主展開(裸 star の多表連結では
  各表の列に応じて EXCEPT/REPLACE を解決し、どんな関係にも解決しなかった
  名は明示的エラー)、(2) `ExpandUnionBranchStars`、(3) M5 派生表フラット化の
  内部 star、(4) relational 経路の `star_groups` 展開
  (`executor/relational.cpp` — EXCEPT のみ対応、REPLACE は式スプライスが
  できないためこの経路では明示的エラー)。
  REPLACE 式は素の列名を star 対象の基底列へ再束縛する
  (`RebindStarReplaceExpression`)ので `* REPLACE (b * a AS b)` のような
  自己参照置換が正しく基底列を読む。出力順は置換対象列の位置を保持。
  ピン留めパーサが修飾付き EXCEPT 項(`t.col`)を構文エラーにするため
  項は常に裸識別子(`StarModifiers` は PathExpression 綴りも防御的に受理)。
  エイリアス付き star 修飾 / AS VALUE との組合せ / PB 値表は明示的拒否。
  回帰: `AnalyticalFeaturesTest.Star*` 9件(単表・複数表・派生表・UNION 枝・
  relational 経路・大小文字・エラー系)。
- **バイアス結合キー: `col = col +/- 定数` の結合述語をハッシュキー化**
  (`executor/detail/planning_heuristics.{hpp,cpp}`)。週次/年次自己結合
  (`l.week = r.week - 52` 等)は列=式なのでキーにできず、残った単一キーの
  バケット内で残余述語を全ペア再検証して桁外れの比較数になっていた
  (TPC-DS q59 は y⋈x で 161万比較 ≒ 8秒の大半)。`EqualityKey` に
  `left_offset/right_offset` を追加し、式側の定数をキー値に折り畳んで
  両側が一致するようにハッシュする(INT64/DATE、|K|<=2^40 に制限)。
  正しさの構造: `ResidualJoinPredicates` が全結合述語を返す既設計により、
  マッチしたペアは必ず元述語で再検証される — バイアスはバケットを狭める
  だけで行を増やせない。カーディナリティ見積(`EstimateJoinRows*`)は
  バイアスキーを除外(生値の頻度マップでは偽 0 に潰れて結合順序が崩れるため。
  これは実測で q2/q4/q11/q16 の 2〜3 倍劣化として観測され、除外で解消)。
  hybrid(spill)結合と null-safe キー混在時はバイアスキーを残余へ退避
  (正しさ優先の縮退)。parallel probe は残余が空でないため不発(従来どおり)。
  効果: TPC-DS q59 7,918ms → 1,317ms(-83%)、スイート全体 25,694 →
  18,616ms(-27%、48クエリ時代のクリーン計測)。回帰:
  `QueryTest.BiasedJoinKeysMatchPlainEquality`(内部/LEFT/左右反転/DATE、
  無バイアス綴りとの行一致を pin)。
- **TPC-DS サブセット 48 → 60 クエリ**。新規移植(公式 Spark テンプレート準拠、
  fixture 適応は各クエリのヘッダコメントに記載): q29(3日付自己結合)、
  q34(ticket密度に合わせ cnt 窓 1..3、county は Franklin Parish)、q35
  (EXISTS×3 + 三チャネル OR)、q37(inventory+item+catalog_sales)、
  q43(曜日別売上ピボット)、q48(状態3組 OR 結合)、q63(window avg 比率)、
  q76(チャネル列の NULL プローブを IS NOT NULL に反転 — fixture は NULL キーを
  書かないため公式綴りでは空結果)、q77(6 CTE + UNION ALL + ROLLUP。
  web_page 非搭載のため web チャネルを ws_ship_customer_sk キーに、
  戻り額は fixture 綴りの wr_return_amount、store_returns には sr_net_loss が
  無いため sr_return_amt を代用)、q79、q84(住所/所得帯を fixture プールに)、
  q98(クラス内売上比率 window)。全て `tpcds_query_test` の決定性比較を通過
  (q98 は部分順序キーを col 1 = i_category に指定: revenueratio は区分内で
  再起動するため全体順序キーになれない)。
- **判明した既知課題(未修正・記録のみ):**
  - q29 の計画 ~1.0秒 / q76 の実行 ~1.8秒: 8表結合の列挙と UNION ALL 枝の
    item 結合が新たな计划/実行ホットスポットになった(q59 型のさらなる
    改善は M4 CTE 葉化待ち)。
  - グループ化経路で未知の列名が出したエラー文が文脈により
    "NotExists"(スカラ経路)と "InvalidArgument: numeric value required"
    (グループ化経路)に分かれる。q77 移植時に混乱した(実態は列名不一致)。
    エラー品質の統一は別セッション候補。
  - `LimitedQueriesUseBoundedFinishPath` は再構築時に HEAD 版から復元した
    (WIP 版テキストは本セッションの誤上書きで失われた)。現在は
    トップレベル ORDER BY+LIMIT クエリにスコープし、TopNFinish に加えて
    Limit ノード表現も受理する。set-op 枝内 LIMIT は対象外。

## 2026-09-29 トランザクションエンジン強化(第3ラウンド)の成果

**修正(実バグ):**

- **[高→解決] 並行リカバリリプレイのデータレース(TSAN 実証済み)。**
  根因は `PageRef::PageUnlock` が**ラッチ解放後に** `page_->PageID()` を読んで
  いたこと(同一 Page オブジェクトの `PageInit` による page_id 書き込みと、
  クリティカルセッションが重ならないためレース成立)。ラッチ保持中に id を
  読んでから unlock する順序に修正。`recovery_extra_test` 16件を TSAN で実行し
  `data race` 0件を確認(残る lock-order-inversion は
  docs/lock_order.md 記載のフェーズ分離 TSAN 偽陽性)。
- **[高→解決] `BranchPage::GetValue out of range: -1` → page 0 (MetaPage)
  ルーティング(2026-09-25 の TPC-C クラッシュ 2件の根本原因)。**
  `BranchPage::GetRow` が **size==0 のセンチネルスロット(空 foster /
  ±inf フェンス)からも stale offset バイトを文字列として復号**しており、
  Search の二分探索が foster を下限として必ず probe するため、空 foster の
  ゴミキーがソート上位に来ると着地が -1 に張り付いた。修正:
  (1) GetRow は size==0 を空文字(=負の無限大の意味論)で返す、
  (2) `GetPageForKey` の less_than 着地 p=-1(「子 -1」= lowest_page_)を
  正規の lower_page_ 着地として処理、
  (3) GetValue/GetKey の範囲外は CHECK で fail-loud(戻り値 0 による
  MetaPage ルーティングと Split への 0 子孫永続化を根絶)、
  (4) `BranchPage::Delete` 空ブランチ kCorrupt / Split pivot 範囲外 kCorrupt。
  回帰: `BranchPageTest.LessThanSeekBelowSecondSeparatorLandsLowest` /
  `EmptyFosterSlotIsMinusInfinityProbe`。
- **[中→解決] StoredChecksum が Page ヘッダのアラインメントパディング
  (未初期化ヒープバイト)を CRC に混入。** `Page::PageInit` がオブジェクト
  全体をゼロクリアするように修正(standard-layout + trivially-destructible
  の static_assert 済みのためバイトクリアは安全)。これで
  `PageChecksumTest.RepairedInMemoryPageIsValidButStillDirty` の ASAN
  フルバイナリでの不安定(malloc_fill_byte 依存)が根絶。
  回帰: `PageChecksumTest.FreshPageChecksumIsIndependentOfPriorMemory`。
- **[中→解決] PagePoolTest.DropAllPagesWaitsForActiveWriteback が Debug
  ビルドで確定的ハング。** テスト末尾の `SetDurabilityGate(nullptr)` が
  コメントアウトされたまま(他4テストで修正済みと同型の stack-use-after-return:
  `[&]` 捕獲の gate ラムダが TearDown の最終 WriteBack から死んだローカルを
  参照し永久スリープ)。パディング修正でチェックサムが決定論になり
  TearDown 時のダーティページが常在化したことで顕在化。アンインストールを復活。
- **[低→解決] fail-stop の無音化。** PreCommit の publish 途中 catch パスが
  ログなしで fail-stop しており、負荷ソーク中の fail-stop が診断不能だった。
  例外種別と txn id を LOG(ERROR) するよう修正。
- **[低→解決] `tm.oldest_snapshot_lag` が生存スナップショット 0 のとき
  commit_ts 全幅を報告。** tpcc_benchmark 側で oldest==0 を lag 0 に修正
  (GC-pin 警告条件は元々 optional で正しく、表示のみの不具合)。

**性能(TPC-C W=2, 32 clients, RelWithDebInfo, taskset, 同日ペア比較):
tps 4,367 → 5,044-5,344 (+16〜22%)。** プロファイル由来の3修正:

1. **デッドロック検出器のビジースピン解消(プロファイル 4.7%→消失)。**
   「辺が存在する限り即真」の wait 述語のため、待機者が常在する負荷下で
   O(V+E) 解析を無限に再実行していた。`wait_for_generation_`(辺変化で
   wait_for_mu_ 下で加算)を追加し、述語を「辺あり AND グラフ変化あり」に
   変更。グラフバッファは検出器スレッド専用メンバとして clear() 再利用
   (毎パスの rehash/malloc 解消)。サイクル解消レイテンシは
   通知駆動のまま(従来比 5s → 通知即時)。
2. **Logger ワーカの通知削減(5.0%→2.4%)とアイドル起床のバックオフ。**
   work_cv_ の待機者はリング満杯時のみ存在する。当初「満杯からの排出時にのみ
   通知」を占有度サンプリングで実装したところ **LoggerTest.AppendMany
   (32バイトリング)が確定的デッドロック**した——producer が観測した満杯は
   producer の (buffered,flushed) スナップショット上にしか存在せず、worker の
   サンプリングは満杯を一度も観測しないまま排水でき、限界待ちの producer が
   永久スリープする。最終実装は **park 中の producer 数を work_mu_ 下で
   直接管理**(満杯待ちの前後で ++/--)し、worker は1パスにつき1回
   「park 者がいれば通知」に変更。サンプリングでは解けないクラス。
   教訓: **限界待ちの起床は状態サンプリングでなく待機者カウントで**。
   合わせてアイドル時の timed wait を sync_interval_ から最大 100ms まで
   倍増バックオフ(仕事があるときは即リセット)——空閒データベースの
   每秒1,000回起床を解消。producer のparkは必ず buffered 前進を伴うため
   worker の述語は即真になり、バックオフがレイテンシに乗らない。
3. **Transaction::mutated_index_roots_ を unordered_set から線形 dedup の
   vector に(毎トランザクションの rehash/malloc 解消)。**

**検証:**

- フル ctest (Debug): 3788/3789 パス(残1は上記 DropAllPages ハング、修正済み・
  3連続パス確認済み)。`SqlSessionFuzzer.SeededSessionsHoldState` も完走
  (2026-09-26 の livelock 懸念は本木で解消済み)。
- TSAN: recovery_extra_test 16件 `data race` 0。
- `logger_test` 全16件を修正後40連続実行で全パス(デッドロック修正の
  焼き鈍し確認。修正前は ~27回に1回の確率でハング)。
- TPC-C 60s ソーク(静寂環境, tmpfs): コミット 179,249 / エンジンアボート
  2.9% / fail-stop 無し / WAL セグメント数収束(リサイクル継続) /
  version_chains 有界 (35k) / oldest_snapshot_lag=0。
- `check_layering.py` OK(許可リスト増減なし)。

**残項目(次ラウンド候補):**

- 共有 tmpfs (/tmp) の他プロセス巨大ログでクォータ逼迫 → WAL 追記が
  ENOSPC になり得る。今回の負荷ソーク(load 40+)での fail-stop はこれが
  疑惑(デッドロック追加ログが次回原因を名前を出す)。ENOSPC → fail-stop
  は設計通りだが、運用者向けに「WAL ボリューム残容量」のテレメトリが
  あると尚良い。
- delivery の `DELETE FROM new_order` 5秒 write-intent タイムアウトは
  TPC-C 構造上のホットロウ競合で残存(エンジンアボートの大半)。
  スキップスキャン/キーワード分布のテーブル側対策の余地。
- Schema::Offset(ColumnName) が行復号ごとに文字列引き(プロファイル 1.9%)
  ——SQL/executor 側のホイスト候補(本ラウンドの守備範囲外)。


## 2026-09-28 健全性パス(並行セッション)の成果と残項目

**修正(実バグ):**

- **[高] `FullScanPlan::stats_` が dangling 参照(stack-use-after-scope、ASAN 実証済み)。**
  `plan/full_scan_plan.hpp` のメンバが `const TableStatistics&` で、
  `PlanTest.NullSafeIndexJoinShapeFallsBackToHashJoin` のように一時オブジェクトを
  渡すと構築直後に dangling し、`EmitExecutor → ReportsClusteredOrder →
  stats_.Rows()` が死んだスタックを読む。全 21 プランクラス中唯一の参照持ちで、
  値持ち(`TableStatistics stats_;`)に統一して修正。ASAN/通常ビルド両方で当該
  テスト・全 ctest がパス。コンテキスト所有統計への参照が前提の呼び出し規約は
  ヘッダコメントに明記。
- **[中] `SqlGroupingSetsTest.UnsupportedShapesRejectWithDiagnostics` が
  window×ROLLUP 実装済みによって陳腐化。** `RANK() OVER (ORDER BY a) ... GROUP BY
  ROLLUP(a)` は 2026-09-28 の第2ラウンドで正当に実行可能になったため、拒否アサート
  を撤去し、合成ランクの正テスト
  (`WindowOverRollupRanksComposite`: 2 つの NULL 行が rank 1 で同着、
  {1} グループが rank 3 になることで枝横断評価をピン)を追加。

**新規起票(未修正・要調査):**

- **[高] 並行リカバリリプレイ中のデータレース(TSAN 実証)。**
  `RecoveryExtraTest.TornPageRebuildWithDirtyMetaPageNeverSelfDeadlocks` を TSAN
  ビルドで実行すると、gtest 自体は通るが `ThreadSanitizer: data race` が報告される:
  - Write: `Page::PageInit` (`page/page.cpp:60`) ← リプレイワーカー T25
    (`ReplayPagesInParallel` → `PageReplay` → `LogRedo`)
  - Previous read: `Page::PageID` (`page/page.hpp:45`) ← リプレイワーカー T24
    (`ReplayAllocation` → `ApplyAllocationState` の `PageRef` デストラクタ
    → `PageUnlock`)
  - 両者は**異なるページのラッチを保持したまま同一の `Page` オブジェクト領域**に
    接触している(TSAN: mutexes write M0 / write M1)。頁の再初期化
    (SPR/アロケーションリプレイ経由)が、他ワーカーがまだ `PageRef` を握る
    物理頁に重なっている疑い。ロック順逆転 7 件は
    2026-09-27 記録の既知グラフ循環と同一だが、データレースは新規。
    `RecoveryExtraTest` 通過は assert 種別が運良く一致するためで、
    修復は `recovery_manager.cpp` の並行リプレイの頁ライフタイム解析から。
- **[中] `PageChecksumTest.RepairedInMemoryPageIsValidButStillDirty` が ASAN
  フルバイナリでのみ不安定**(単独実行・`malloc_fill_byte=0`では安定パス、
  実バイナリのヒープ履歴や `malloc_fill_byte=255`で失敗)。
  `StoredChecksum` (`page/page.cpp:414`) が Page オブジェクトの生バイト
  (header のパディングや body 以外の領域)をハッシュしており、初期化されて
  いないバイトが CRC に混入する経路がある。修正方向: PageInit でオブジェクト
  全体をゼロ埋めするか、StoredChecksum をヘッダ固定フィールド+body への
  明示範囲ハッシュに再構成する。チェックサム形式に関わるため
  page 層オーナー判断が必要。
- **[中] `SqlSessionFuzzer.SeededSessionsHoldState` が TSAN の deadlock detector
  内部 CHECK で異常終了する場合がある**
  (`sanitizer_deadlock_detector1.cpp:166 "isHeld"`。gtest 計算は完走)。
  既知の lock-order グラフ循環の別顔。テスト安定化には TSAN 側の
  `detect_deadlocks=0` 指定か、グラフ循環の解消が必要。

**検査済み(変更不要):** `sql_grouping_sets_test` 18/18、
`ParallelScan` ワーカーフィルタ経路(`DrainDeferred` の happens-before、
`FailWith` の sticky 伝播)、`row_page.cpp` の TOCTOU 再チェックと WAL 失敗時補償、
`blob_file.cpp` のマイグレーション・マーカ契約。

**追加修正(テスト):** `DropAllPagesWaitsForActiveWriteback` が ASAN の
stack-use-after-return で断続落ちする実バグを修正 -- durability gate /
WAL snapshot sink のラムダが `[&]` で TestBody ローカルを捕獲したまま
プールに残り、`TearDown` の `~PagePool` 最終 WriteBack から死んだフレームを
参照していた。該当4テストの末尾で `SetDurabilityGate(nullptr)` /
`SetWalSnapshotSink(nullptr)` を明示 uninstall。
`database_extra_test` に `TIMEOUT 2700` を設定(checkpoint storm は静退出で
実測 2003 秒、既定 1500 秒ではロード時に切れる)。

**追加テスト:** worker スキャンフィルタの正/誤/遅延3決定
(`MorselDrivenParallelScanWorkerFilterDecidesRows`)、遅延行のエラー表面化
(`MorselDrivenParallelScanSurfacesDeferredRowErrors`; INT64 除算の FLOAT64 昇格を
踏まえた述語設計)、blob レガシー破損の fail-closed 2件
(`ShortLegacySegmentFailsClosed` / `TruncatedSegmentMetadataFailsClosed`)。

## 2026-09-28 分析SQL完成度セッション(第2ラウンド)の成果

**達成:**

- **`PARTITION BY` に任意式を書くと黙って捨てられる silent wrong result を
  修正**(`query/googlesql_ast_visitor.cpp` `ParseWindowSpecification`)。
  従来は PartitionBy ノードの子のうち `PathExpression` だけを収集しており、
  `PARTITION BY grouping(a)+grouping(b)` や `UPPER(x)`、算術式などの
  非カラムパス式が**無言で落ちて単一パーティションに縮退**していた
  (rank/row_number が全行連番になる等)。Location 以外の全子を
  `VisitExpression` するように変更。回帰:
  `GoogleSqlAstTest.WindowPartitionKeepsArbitraryExpressions`、
  `AnalyticalFeaturesTest.WindowPartitionByExpressionPartitions`。
- **ウィンドウ関数 × GROUPING SETS / ROLLUP / CUBE を実装**
  (同ファイル `ExpandGroupingSetsWithWindows`)。従来は visit 時に明示エラー
  になった(黙っては落ちない契約)。標準意味論では window は**全 grouping set
  の合成結果**を見る(rank が枝ごとに再起動してはならない)ため、
  (1) 各 grouping set を UNION ALL 枝へ展開する際、select 項を合成名
  `$gsc<n>` にリネームし、window 入力式(PARTITION BY / ORDER BY / 引数 /
  inner ORDER BY の各 post-aggregation 式)を枝ごとに GROUPING() 定数畳み込み
  ・inactive key NULL パディングした hidden 列 `$gsw<k>` として materialize、
  (2) 合成を派生表とする wrapper SELECT が元の select 項を再投影し、window
  呼び出しを hidden 列参照へ rewire した上で合成行集合上で評価。
  ORDER BY / LIMIT は wrapper へ移動。ORDER BY 項は select 項との
  alias/列名/fingerprint 照合で `$gsc` 参照へ再束縛し、合致しない
  (集約を含む等)項は window 入力と同じく materialize。window エイリアスを
  素で並べた項は wrapper 出力への通常の ORDER BY 束縛に委譲。
  明示的拒否を維持する形状: QUALIFY、SELECT DISTINCT、ORDER BY 直書きの
  window 呼び出し、`AGG(x WHERE p) OVER (...)`、window のネスト
  (いずれも silently wrong にならないようエラー)。回帰:
  `AnalyticalFeaturesTest.WindowOverRollupSpansAllGroupingSets` /
  `WindowOverRollupPartitionByCaseOfGrouping`(q70 形状) /
  `WindowOverGroupingSetsWithSumArgument`(GROUPING SETS × sum(window 引数))。
- **TPC-DS サブセット 36 → 48 クエリ**。新規移植:
  q70 / q67(ROLLUP × rank — 上記機能で到達可能になった)、
  q86(ROLLUP × rank × CASE パーティション)、
  q4(3チャネル CTE UNION ALL + 6重自己結合 + CASE 比率)、
  q28(6個の単行集約バケットの交叉結合 + count distinct)、
  q33(3チャネル CTE + IN サブクエリ + ca_gmt_offset フィルタ)、
  q50(返回ラグ集計。`"30 days"` 等の空白入り別名は `days_0_30` 等へ変更、
  fixture の store 表に無い `s_street_*` / `s_suite_number` 列は除去、
  返回ラグ(sale + 5..60日)が重なる 2000 Q2 窓に固定)、
  q59(週番号自己結合の曜日別売上比率。CTE 集約ボディが
  `MaterializeCtes` の 1024 行予算を超え mapped 維持のため ~8 秒と最遅。
  CTE 葉化(M4 残課題)の優先候補)、
  q53(avg-of-sum window + 外側 WHERE で window 結果フィルタ。
  category/class プールを fixture の 8 カテゴリ構成へ適応)、
  q69(EXISTS + NOT EXISTS ×2 のチャネル選別)、
  q75(Catalog/Web の LEFT JOIN を含む UNION DISTINCT CTE + 前年比。
  DECIMAL キャスト除去。mapped CTE 自己結合が Sort+Limit で止まり
  TopN 融合は未適用 — M4 残課題として記録)、
  q87(EXCEPT DISTINCT 連鎖の count。括弧付き set-op オペランドは
  ピン留めパーサが拒否するため平坦な連鎖形)。DMS / 年月の define は全て
  fixture 範囲に固定(1176 = 1998-01 等)。fixture / DDL 変更はゼロ。
- **判明した既知バグ(未修正・記録のみ):**
  - **q67(8 列 ROLLUP × rank)/ q65(brand 曝露比率の rank 境界)で実行ごとに
    内容が揺れる**。小計行とその唯一の構成行が並列 DOUBLE 部分和のマージ順で
    最下位ビットだけ異なり
    (`%.9g` 表示は同一)、`rank() <= 100` + LIMIT 境界で sort 位置/rank が
    反転し、生き残る行が変わる(q65 は 2026-09-28 の健全性パスで
    `brand#11` vs `brand#15` の生存行反転を追加観測)。並列浮動小数集約に
    固有の揺らぎが rank 境界で増幅される形。`tpcds_query_test` の決定性比較からは
    q67 / q65 を除外(実行カバレッジは他テストで維持)。rank 境界での
    再現性が必要になったら部分和の決定的マージ順(kCommitOrder 等)を検討。
- **判明した既知バグ/未対応(次回以降):**
  - `SELECT * EXCEPT (...)` / `SELECT * REPLACE (...)`(StarWithModifiers)は
    未対応(明確なエラーになる)。star 展開は `name == "*"` をキーにする
    44 箇所(query/executor/plan)に散らばり、修飾子メタデータを式レベルで
    運搬する必要がある機能追加規模。visitor 段ではカタログが無く列を解決
    できないため parse 時の展開は不可能。着手時は sql_engine.cpp の
    star 解決 chokepoint(派生表展開と statement 列収集)から
    修飾子を消費する設計が候補。
  - q5 / q62 / q85 は fixture に `catalog_page` / `web_site` / `web_page` /
    `wr_returning_*` 列が無いため移植見送り(表追加は既存ピンをずらす)。
  - q8 は 400 個の ZIP 定数 IN リストが fixture の zip 生成と噛み合わず、
    適応コストが割に合わないため見送り。

## 2026-09-28 トランザクションエンジン強化セッション(第2ラウンド)

**背景:** 2026-09-27 の強化セッション(PagePool 書き戻し並列化、再起動後の
テーブル成長修復、Logger/チェックポイント監査)に続くラウンド。TPC-C
(W=2, `--profile-waits`)でボトルネックを再計測したところ、コミット系の
グローバルロック(`pending_commits_mutex_`、`transaction_table_lock`)は
実測では無視できる水準(`wait.mvcc_commit_shards` 全体で 62ms/10s)で、
実際の壁は **デッドロック時の 5 秒書き込み意図待ちと、その帰結としての
アボート爆発** だった。

- **変更: デッドロックポリシーのデフォルトを `kLegacy` →
  `kDeadlockDetect` に変更。** TPC-C W=2 実測: 32 クライアントで tps
  3775→5281 (+40%)、エンジンアボート 14,734→1,457 (1/12)。64 クライアント
  では legacy が 48,533 アボートに対し detect は 1,435。8/16 クライアント
  でもデグレなし。`kLegacy` は明示指定で利用可能(ベンチマーク比較用)。
  ベンチマーク本体は `--deadlock-policy` 未指定時にエンジン既定に従うよう
  変更(従来は無条件で legacy を上書き)。
- **変更: 待機グラフ検出器をイベント駐動化。** 従来は 2ms ポーリングで
  待機辺の有無を監視しており、デフォルト常駐化すると常時 CPU を消す。
  辺数のアトミックカウンタを述語にし、`AddWaitForEdge` が cv ミューテックス
  を保持して通知する構造に変更(通知が述語チェックとブロックの間に滑り込む
  ロストウェイクを構造的に排除)。辺がない限りスリープし続ける。
  巡回ウォーク自体も waiter→holder の隣接マップを1回作る O(V+E) に変更
  (従来はホップ毎の線形探索)。
- **変更: 長時間稼働テレメトリ。** `TransactionRuntimeStats` に
  `active_transactions` / `version_chains` / `oldest_snapshot_ts` /
  `current_commit_ts` を追加( gauge)。`GarbageCollectVersions` は
  「生存スナップショットが 10万コミット以上遅れて大量チェーンを保持」の
  条件でレート制限付き WARN を出す(月単位で走り続けるプロセスが
  メモリ圧迫に気づけない問題の根本対策は「検出可能にする」こと)。
  TPC-C `--profile-waits` 出力にも `tm.*` として反映。
- **修正: 敗者アンドゥの再適用がコミット済みページを解放(データ損失級)。**
  `SqlSessionFuzzer.SeededSessionsHoldState` シード0が
  `BPlusTree::FindLeftmostPage` のアサーションで落ちる回帰を根因から修正。
  経路: 敗者Txがページを fresh 確保 → 実行時アボートでなくクラッシュで終端
  → リカバリがCLRを書くが終端レコードは書かない(設計通り) → 間にコミット済み
  Txが自由リスト経由で同じページIDを再確保 → 次回リカバリの敗者チェーン
  再ウォークが `UndoLoserChains` でALLOC undoを再適用し、
  コミット済みページを `PageInit(kFreePage)` で破壊。ページ単位の
  `PageReplay` には CLR マッチング保護(`compensated_ops`)があったが、
  ページをまたぐグローバル敗者パスには同等の保護が欠けていた。
  修正: `RecoverFrom` の解析パスで窓内の全 `kCompensate*` から
  (txn, 閉じるop型, pid, op-id) → {CLR LSN, CLR末尾} のエビデンスマップを
  構築し、`UndoLoserChains` は「当該レコードより新しいCLRが存在し、かつ
  対象ページの page_lsn ≥ CLR末尾」の場合に物理再アンドゥをスキップ。
  page_lsn 再チェックは「CLR追記後・物理変異前」のクラッシュ窓で
  アンドゥ欠落が出ないための保守化。
  回帰: `RecoveryExtraTest.LoserAllocationUndoDoesNotFreeRecycledPage`
  (敗者確保→リカバリ→コミット済み再確保→再リカバリ×2回)。
- **検査済み(変更不要):** `RecoveryManager::SegmentFdFor` の読み出し
  fd キャッシュは `kMaxCachedReadFds` で有界、`RecoverFrom` の torn 掃滴・
  遊離リスト再構築・`UndoLoserChains` のチェーン検証は堅牢、
  WAL セグメントリサイクルの持続負荷回帰テスト
  (`CheckpointKeepsRecyclingUnderSustainedConcurrentLoad`)は 2026-09-25
  に追加済みで再カバー不要。
- **残項目:** `pending_commits_mutex_` / `transaction_table_lock` は
  32 クライアント程度では依然無視できるが、128+ クライアントの超並列
  OLTP では初めて顔を出す可能性がある(次に計測したくなったら
  `Begin`/`ForgetTransaction` にコミットシャードと同型の待ち時間計測を
  足してから判断すること)。ロックフリー化(ring buffer)の設計メモは
  本ファイル 2026-09-28 時点のメモリには残していない。
- TSAN `PagePoolTest.DropAllPagesWaitsForActiveWriteback` の既存フレーク
  (環境/TSAN 由来)は 2026-09-27 記録のまま未対応。

## 2026-09-27 分析SQL完成度セッションの成果

**達成:**

- **パイプ構文(`|>`)が黙って無視される重大バグを修正**し、核心演算子を
  実装した(`query/googlesql_ast_visitor.cpp` `FoldPipeOperators`)。従来は
  `SELECT k FROM t |> WHERE v > 10` のようなクエリで `Pipe*` AST ノードを
  全て無視し、**先頭の SELECT だけを静かに実行してしていた**( silently
  wrong results)。実装方式: 各パイプ演算子を「前ステートメントを単一派生
  表ソースとするラッパ SELECT」へ畳み込み、既存のスコープ/集約/順序/LIMIT
  機構にその意味論を委譲。派生表は M5 flatten が単純形状をインライン化する
  ため性能劣化もない。
  - 対応: `WHERE` / `SELECT` / `AGGREGATE ... GROUP BY` / `ORDER BY` /
    `LIMIT OFFSET` / `JOIN`(inner/left/right/full、ON の入力側修飾子は
    ラッパ別名へ再束縛) / `UNION [ALL]` / `INTERSECT [ALL]` /
    `EXCEPT [ALL]` / `AS`(入力別名変更) / `FROM t |> ...` の FROM-first 形。
  - set-op 折り畳みでは、set-op 幅検査がリテラル select list を読むため
    star を入力列の明示参照へ解決する `ResolvePipeInputItems` を追加。
  - 明示的拒否( silently pass しない契約): `DROP` / `RENAME` / `SET` /
    `EXTEND` / `CALL` / `PIVOT` / `TABLESAMPLE`。EXTEND/DROP/RENAME は
    ピン留めパーサが AST ペイロードを載せないこと + 出力列名の知識が必要な
    ことが理由。
  - 回帰: `AnalyticalFeaturesTest.PipeSyntaxComposesOperators`(フィルタ、
    FROM-first 集約連鎖、JOIN の ON 再束縛、LEFT JOIN NULL pad、set-op、
    AS、未対応演算子の throw)。
- **単一表 GROUP BY × ORDER BY 集約の「aggregate was not prepared」バグを
  修正**(`executor/relational.cpp` stream_agg 経路)。
  `SELECT k, SUM(v) FROM t GROUP BY k ORDER BY SUM(v) DESC` 等で、ORDER BY
  項の集約ノードが選択リストの集約と別インスタンスのため
  AggregateResultMap(ポインタキー)に登録されず実行時エラーになっていた。
  結合経路 `ExecuteGroupedSelect` と同じ再束縛(選択リスト集約への
  ToString 一致畳み込み + 別名解決 + resolved keys の
  `CollectAggregates` 追加)を適用。`ORDER BY COUNT(*)`、非射影集約、
  別名、TopN 形すべてで確認。
  回帰: `QueryTest.GroupedOrderBySpelledAggregate`。
- **TPC-DS サブセット 30 → 36 クエリ**。新規移植: q20(q12 の catalog_sales
  版 revenue ratio ウィンドウ)、q23a(3 CTE + IN サブクエリ + UNION ALL 枝)、
  q27(ROLLUP + GROUPING())、q31(2 CTE の 6 重自己結合 + CASE 比率比較)、
  q60(3 CTE + UNION ALL + IN)、q65(ネストした派生表 + store/item 結合)。
  適応: q31 は d_year を 2002 に固定(fixture の 2000 データは両 CASE 比較を
  満たす郡が存在せず空結果)、q60 は i_category='Music' → 'Sports'
  (fixture 8 カテゴリに Music なし。カテゴリ追加は既存ピンをずらす)。
  fixture / DDL 変更はゼロ。36/36 が SF1 でパス。
- **既知バグの解消確認**: 2026-09-26 記録の
  `sql_session_fuzz-repro-6000602889109468625.test` は
  `TINYLAMB_SESSION_REGRESSION_DIR` を設定した replay でパスすることを
  再確認(WAL 作業の副作用で解消済み)。

**判明した既知バグ/未実装(次回以降):**

- **`SELECT * EXCEPT (...)` / `SELECT * REPLACE (...)`(StarWithModifiers)は
  未対応**。現在は「unsupported expression StarWithModifiers」と明確な
  エラーになる(黙っては落ちない)。実装には star 展開の全箇所
  (query/plan/executor で `name == "*"` をキーにする ~20 箇所)に修飾子
  メタデータの運搬が必要で、機能追加規模。
- **パイプ `EXTEND/DROP/RENAME/SET` も同様に未対応**(明示的拒否)。これらは
  前段出力の列名を AST 段階で知る必要があり、加えてピン留めパーサが
  PipeExtend/PipeDrop/PipeRename ノードに子を載せないため
  `SliceSource` によるテキスト再解釈(規約違反)はしない方針。
- **ウィンドウ関数 × GROUPING SETS/ROLLUP/CUBE は未対応**(visitor が
  明確なエラーを返す)。TPC-DS q70/q86 がこの形で、正式対応までは
  ポート対象外。対応時は UNION ALL 枝ごとの window 計算または
  group-all 行への pad 戦略の設計が必要。
- **`WITH RECURSIVE name(col...)` の列リスト非対応**、**`GROUPS` フレーム
  単位非対応**、**`GROUPING SETS` 内のネスト ROLLUP/CUBE 非対応** —
  いずれも既知のパーサ/ビジタギャップ(明確なエラーになる)。

## 2026-09-26 分析SQL完成度セッションの成果

**達成:**

- **相関行値 `(a, b) IN (SELECT AS STRUCT ...)` の誤ルーティング修正**
  (`query/sql_engine.cpp` `CanUseDecorrelatedSubqueryOptimizer`)。
  テスト式が単一 `kColumnValue` でない場合(行値は deferred `__struct_json__`
  FunctionCallExp に低減される)は keyed semi/anti 書換えの対象にならないのに、
  内部相関が列同値のときだけ「decorrelate 可能」と判定して Cascades コアへ
  回していた。デコリレーションは実際には発火せず、conjunct が文脈なしの物理
  `Selection` に取り残されて実行時に "query expression requires relational
  evaluation" で落ちていた。非列テストは `return false` でスコープ付き
  relational 評価器に留めるよう修正。
  回帰: `SqlSubqueryShapesCoverageTest.CorrelatedRowValueIn`
  (IN/NOT IN の三値論理、NULL を含む build 側、非相関 semi-join 経路の
  非後退を pin)。
- **行値INの記録訂正**。2026-09-22 の「3層修正が必要」分析は部分的に古い:
  - `(a, b) IN (SELECT x, y ...)`(AS STRUCT なし)の
    "Subquery of type IN must have only one output column" は
    **GoogleSQL 準拠の正しいエラー**(BigQuery 同文)。バグではない。
  - `IN (SELECT AS STRUCT ...)` / `= (SELECT AS STRUCT ...)` /
    HAVING / JOIN ON / 非相関 semi-join 化は既に正しく動作する
    (相関含め `ThreeValuedEquals` の構造体経路で三値論理も正しい)。
  実際に壊れていたのは上記の相関行値INのルーティングのみ。
- **q47 の ORDER BY 序数 `3` を復活**。`SELECT *` + ORDER BY 序数は
  2026-09-24 の修正以降、relational 経路を含む全経路で解決されるように
  なっており、`s_store_name` への置き換え適応は不要だった
  (tpcds_query_test 全パスで確認)。既知残項目「ORDER BY 序数 × SELECT * ×
  relational 経路」は**解消済み**。
- **TPC-DS サブセット 27 → 30 クエリ**。q22 (inventory+item+warehouse の
  4列 ROLLUP + d_month_seq 範囲)、q92 (web_sales の相関 1.3×avg スカラ
  サブクエリ + 90日 date window)、q93 (store_sales LEFT JOIN store_returns
  の複合キー + reason フィルタ + CASE 集約)。fixture は item に
  `i_product_name` を追加(インデックス派生の12値プール。RNG ストリームを
  消費しないので既存ピンは不変)。q93 は `r_reason_desc` → `r_reason_text`
  ('Did not like the color') に適応。SF1 min-of-3 で 30/30 成功
  (TPCDS_TOTAL 4998.9 ms。q22 は計画 ~0.9 秒 / 実行 0.7 秒で、q18 型の
  ROLLUP 枝計画共有の対象候補)。
- **診断計装**:`ScopedDb::CrashAndReopen` の再オープン失敗時に
  `LOG(ERROR)` で実際の `Status` を出すようにした(長時間ファズ sweep の
  "replay crash reopen failed" を調査可能に)。

**判明した既知バグ(未修正・最優先):**

- **コミット済みリグレッション
  `query/testdata/session_regressions/sql_session_fuzz-repro-6000602889109468625.test`
  が失敗**(`SqlSessionFuzzer.ReplayCommittedRegressionFiles`、9ce7837 で
  コミット済みのファイル。HEAD 自体はビルド不可のため、コミット時点での
  検証は確認できない)。私の変更と無関係に再現(修正リバート後も失敗)。
  - 症状: ステップ37(3回目の `-- crash`)で `Database::Create` が
    `Corrupt: WAL operation page type does not match target` を返し
    再オープン失敗。
  - 切昧: クラッシュ#2時に `CREATE INDEX idx0 ON tbl0` を含むトランザクションが
    未コミットのまま中断。敗者トランザクションの `kSystemAllocPage` が
    クラッシュ#2のリカバリで再適用(あるいはCLR化されず)された結果、
    以降の INSERT が再利用したページIDと型が噛み合わなくなり、
    クラッシュ#3の redo で row-op → 非rowページの型不一致で棄てられる。
    つまり **ネストしたクラッシュをまたいだ敗者割り当てのリカバリ冪等性** の
    穴。WAL セグメンテーション / FPW の作業領域に近く、`page_storage.cpp` /
    `recovery/` を活発に変更中の WIP と競合するため、本セッションでは
    修正せず記録のみ。`recovery_extra_test` 系のネストクラッシュ形状
    (未コミット DDL → クラッシュ → 再構築 → クラッシュ)の追加を推奨。
  - **2026-09-27 解決済み。** 上記リグレッションは 2026-09-26 health pass の
    LogUndo stale-incarnation 修正以降パスしている。改めて実ファイルを使った
    再現調査を行った結果、真の根本原因は敗者割り当てではなく
    **再起動後のテーブル成長バグ**(下記「再起動後のテーブル成長が恒久的に
    kConflicts」)だったと結論:テスト対象セッションは、再起動後にテーブル
    末尾がいっぱいになり新規ページ割り当てが必要な挿入を行い、そこで
    `insert failed (kConflicts)` が起き、さらに当時のテストドライバが
    ストリームエラーを握り潰して「空コミットの成功」に見えていた。
    ネストクラッシュ×敗者DDL形状そのものは
    `RecoveryExtraTest.LoserDdlAllocationSurvivesNestedCrashes` で直接カバー。
  - 高負荷下での `SqlSessionFuzzer.SeededSessionsHoldState` の Timeout も
    同時刻に観測。`AnalyticalFeaturesTest.SystemTableSampleBandOnMultiPageTable`
    もフル ctest 高負荷時のみ 1 回落ちて単独では 128 ms で安定パス
    (2026-09-23 記録のタイミング系フレークと同系統。外部パーサ子プロセスと
    -j 満載時のリソース競合の疑い、要 root-cause)。
    **2026-10-20 解決**: 負荷ではなく **seed 無しの統計フレーク**だった。
    この表はページ数が少なく、1 ブロックが全行の 1/8 前後を占めるため、
    50% のブロック抽選は seed 次第で 12%〜88% まで振れる。単独実行でも 4 回に
    2 回は帯 [4000,16000] の外に出た。`REPEATABLE(123)` で抽選を固定し
    （6570 行）、帯は「全表 / 空 / 桁違い」を落とす網として残した。

## 2026-09-27 トランザクションエンジン強化セッション

- **修正: 再起動後のテーブル成長が恒久的に kConflicts(データ損失級)。**
  `PageManager` のテーブル末尾キャッシュは初回参照時にカタログの `last_pid_`
  (`Table` シリアライズ値、DDL時しか更新されない)からシードされる。
  再起動後は実チェーン末尾との乖離が必ず起こり得るのに、
  `Table::Insert` の成長経路 `ReserveTableTail(first_pid_, expected=実tail)`
  は `published_tail(古いヒント) != expected` で即 `kConflicts` を返し、
  **以後そのプロセスが生きている間、どのトランザクションも新規ページを
  割り当てられなかった**(挿入が全て失敗する)。セッション内でのみ発見的で、
  既存テストは再起動後に「穴再利用」しか試さなかったため未検出だった。
  修正: `Table::Insert` のチェーン辿りでコミット済み next リンクを横断する際に
  `PageManager::AdvanceTableTail(first_pid_, predecessor, next)` で末尾キャッシュ
  を修復する(期待値一致でのみ前進するので並行安全)。
  回帰: `RecoveryExtraTest.RestartedTableGrowsAgain`(クリーン再起動×成長)、
  `RecoveryExtraTest.LoserDdlAllocationSurvivesNestedCrashes`(ネストクラッシュ
  ×敗者DDL×再成長×3回目のクラッシュ)。
  教訓: `RunSql` 系ヘルパがストリームエラー(`Executor::GetStatus()`)を
  見ず `PreCommit` の成功だけで判断していたため、失敗したDMLが
  空トランザクションのコミットとして偽装されていた。
  `recovery_extra_test.cpp` の `RunSql` は現在ストリーム状態を検査する。
  他ファイルの同型ヘルパは未監査(要確認)。
- **並行化: `PagePool::WriteBack` / `SyncDataFile` の入口がグローバル
  `pool_latch` を独占取得しており、チェックポイントのフラッシュ掃引や
  エビクションを含む全書き戸しが1本の排他 critical section に直列化されて
  いた。** `dropping_pages_` フリップとの順序契約(DropAllPages の
  「何も書き戻さない」保証)を維持したまま、書き戸し側は `shared_lock` で
  登録し、`DropAllPages` はフリップを `pool_latch` 排他で行うように変更。
  これで並行する書き戸し(複数エビクタ + チェックポイントフラッシュ)が
  レジストレーションで互いに排他されなくなった。
- **監査結果(変更不要と確認できた領域):** Logger のセグメント
  リサイクル(床計算・ストラドル境界保護・`lowest_segment_` カーソル・
  unlink失敗時の保守的停止)、チェックポイントの master record
  パブリケーション順序、ENOSPC/EIO 時の fail-closed 経路
  (WAL死→fail-stop、データファイル書き込み失敗→ページを dirty 保持して
  エラー伝播、メタフラッシュ失敗→リサイクル中止)、
  `Database::DeleteAll` / `RemoveCreatedDatabaseFiles` の削除ファイル集合
  (セグメント列 + `.log.meta(.tmp)` + `.last_checkpoint(.tmp)` を含む)、
  MVCC GC の適応パス、fd ライフサイクル。
- **既知の残項目:** TSAN ビルドで
  `PagePoolTest.DropAllPagesWaitsForActiveWriteback` が ~80% で
  TSAN runtime 内部の DEADLYSIGNAL で落ちる(2026-09-27 時点で変更前から
  再現する環境/TSAN由来の既存フレーク、通常ビルド・ASANでは不検出)。

2026-09-22 加筆(2026-09-23 検証更新)。TPC-DS サブセット
(`tinylamb_tpcds_benchmark`)の整備とテスト保守で判明した未解決事項。
**2026-09-23 時点の ctest は 3152/3152 全パス(1件意図的スキップを除く)。**

## 2026-09-25 分析SQL完成度セッションの成果

**達成(全て `query/analytical_features_test.cpp` で回帰 pin 済み):**

- **`MODE()` 集約**:グループ/ウィンドウ/DISTINCT 対応。非NULL 値の最頻値、
  同数は先に到達した値で決定的。シリアル累算器
  (`expression_eval.cpp`)とウィンドウ評価器(`window_eval.cpp`)の両方、
  並列集約はホワイトリスト外(逐次)。`window_frame_sort_sharing` の
  order-insensitive 白名図にも追加。
- **`REGR_*` 線形回帰集約 9種**(SLOPE / INTERCEPT / COUNT / R2 / AVGX /
  AVGY / SXX / SYY / SXY):COVAR/CORR と同じ long-double 累算を流用
  (y=子、x=trailing 引数、どちらか NULL の行をスキップ)。
  `IsStatisticalAggregate` に加わり並列集約の部分和マージ経路も対応
  (`IsTwoInputStatAggregate` を新設し parallel_aggregation 側の
  2入力判定を統一)。REGR_COUNT のみ INT64。
- **`BIT_COUNT` / `NORMALIZE_AND_CASEFOLD` / `NORMALIZE` スカラ**:
  AST(ground truth)と relational 両評価器に実装。BIT_COUNT は保存幅
  (INT64 スロット)の popcount。正規化は構成形(NFC/NFKC)のみ受付で
  分解形(NFD/NFKD)は明示的に `kNotImplemented`(黙って誤正規化しない
  ための契約)。`UnicodeCaseFold` は ß/final sigma/micro sign/合字の
  完全折り畳み + それ以外は小文字。
- **`TABLESAMPLE SYSTEM (p PERCENT)`**:ページブロック標本として実装
  (ページIDをハッシュした include/exclude 単位決定、REPEATABLE(seed)
  で決定的)。旧来の PERCENT(行単位 Bernoulli 相当)と RESERVOIR は
  不変。
- **`GENERATE_DATE_ARRAY` の暦ステップ**:`INTERVAL n MONTH/QUARTER/YEAR`
  を DATE_ADD と同じ月末クランプで生成(両評価器)。月と日の混在ステップ
  は明示エラー。
- **正しさ修復1:LIMIT × ウィンドウ**。ORDER BY なし LIMIT のクエリで
  `CanPushLimitIntoFullScan` がスキャンへ max-rows を注入し、パーティ
  ション全体フレーム(`OVER ()` 既定 / UNBOUNDED FOLLOWING / LEAD 等)
  が切り詰められた入力で計算されていた(例:`SUM(x) OVER (ROWS BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ... LIMIT 1` が先頭行値を
  返す)。集約と同じく select 出力にウィンドウ関数を含む場合は注入しない
  ようゲート拡張(`ContainsWindowFunctionExpression` を plan 層に新設)。
- **正しさ修復2:QUALIFY 列のスキャン列プルーニング**。
  `CollectStatementColumns` が QUALIFY を見ておらず、QUALIFY だけが
  参照する列がスキャンから落ちて集約の子が誤束継/未解決になっていた。
  QUALIFY の列を required に追加。
- **QUALIFY × GROUP BY 対応**。`ExecuteWindowOverAggregate`(relational
  経路)が QUALIFY を拒否していたのを、pass-2 のウィンドウ計算に
  QUALIFY 由来のウィンドウ呼び出しを加え、集約は `$aggK` 隠し列として
  事前登録し、grouped+$win 行へのフィルタ(三値論理で NULL/FALSE 落とし)
  として実装。`GROUP BY ... QUALIFY COUNT(*) >= 2` 等が動作。

**判明した既知バグ(未修正):**

- 行値 `(a, b) IN (SELECT x, y ...)` は従来どおり未対応(本セッションの
  調査でも「Subquery of type IN must have only one output column」を
  確認)。frontend の deferred struct 構築・`ProjectSubqueryRow` の
  単列前提・IN ハッシュメンバーシップの単一ドメイン前提の 3 層修正が
  必要で、機能追加規模。次の専用セッションの最優先候補。
  **(2026-09-26 訂正: 上位の 2026-09-26 節を参照。AS STRUCT 経路は
  既に動作しており、非 AS STRUCT のエラーは GoogleSQL 準拠。
  実際の修正は相関行値INのルーティングゲートで完了。)**

## 2026-09-24 完成度向上セッションの成果と残項目

**達成:**

- **TPC-DS サブセット 16 → 27 クエリ**。新規移植: q2 (CTE内UNION ALL +
  CTE複数参照 + CASE-SUM集約)、q9 (CASE内スカラサブクエリ)、q10 (相関
  EXISTS×3 + OR)、q11 (UNION ALL CTE の4重自己結合)、q13/q15 (OR-of-ANDs
  6表結合 / 選言sarg)、q16 (EXISTS + NOT EXISTS の同一order番号比較)、
  q19/q40/q46/q47。fixture に `catalog_returns` / `web_returns` /
  `call_center` を追加し、`catalog_sales` に出荷列、`date_dim` に
  `d_week_seq`/`d_day_name`、`customer` に `c_login`/`c_email_address`、
  `item` に `i_manufact`、`store` に `s_zip` を追加。追加列の乱数は
  独立 aux RNG に分離し、既存のピン止めデータを一切変更しない。
  全27クエリがSF1で最終測定2.93秒（min-of-3、最遅q11 1.24秒、27/27成功）。
- **`GROUPING_ID(...)` を実装** (grouping sets 展開の AST 畳み込みで
  枝ごとの定数へ)。GoogleSQL ビットベクタ順 (左端引数 = 最上位ビット)。
  複数引数 `GROUPING(...)` も同じ順に統一 (従来は左端 = LSB で、
  `GroupingMultiArgBuildsBitmask` の期待値を更新)。
- **`ProductPlan::InstallParameterSlots` の SEGFAULT 修正**: index join 形
  (right_src_ null, right_tbl_ 保持) での null 参照。
  3 テスト (CascadesRulesCoverageTest ×2, compliance optimizer_join) 復帰。
- **`SlotizeLiterals` の集約属性保存**: type/child/distinct だけで再構築し
  `AGG(x WHERE p)` の inline WHERE・inner ORDER BY/LIMIT・STRING_AGG
  delimiter が落ちていた (`TemplateRebindKeepsAggregateAttributes` 復帰)。
  `ContainsParameterSlot` / `AppendParameterSlots` も全属性を辿るよう修正。
- **ordered join 経路の述語配置** (`planning_heuristics.cpp`
  `has_ordered_join`): 外部結合を含む FROM では WHERE が一切配置されず、
  カンマ結合が cross 積のまま最上位 Filter に流れていた
  (TPC-DS q40 が計画爆発で 15 分超 → 109 ms)。単一ソース述語はスキャンへ、
  カンマ結合の最後の参照表到達時に結合条件へ配置。volatile /
  サブクエリ述語は配置せず最上位評価を維持 (二重評価を防ぐ)。
- **ORDER BY 序数の部分修正**: `SELECT *` と序数の組合せで visitor が
  「ordinal out of range」を出していた点を、star 展開後に解決する形で
  planner 経由は修正 (`ApplyOrderBy` も定数キーを出力列へ解決)。
  回帰: `PinnedDumpTable` に star+序数の pin を追加。

**残項目(次回以降):**

- **ORDER BY 序数 × `SELECT *` × relational 経路** (CTE/派生表ソースなど
  `emit_relational` へ流れた文): 定数キーが無秩序ソートとして無音に機能し
  ない (q47 は `s_store_name` を明示する形に適応して回避)。
  `emit_relational` 経路での定数キー検出と解決 or エラー化が残課題。
- 監査 A 項の残りは docs/optimizer_todo.md 監査節に 2026-09-24 時点で
  解決済みとして反映 (mark_join_to_filter / eager_aggregation_over_join /
  xor_boolean_identity / distribute_or_over_and_budgeted)。

## 2026-09-23 再検証で撤回した誤診

- **「`NOT EXISTS (相関サブクエリ)` が誤行/空を返す」は誤りだった。**
  CLI は SELECT の複数行を 1 行ずつ `[v]` で出力するため、前文の 1 行結果を
  次文の結果と取り違えると「誤行」に見える。EXISTS/NOT EXISTS/IN の 15 形状
  (内側・外側・両側残差、OR、不等号相関)を 1 文 1 セッション + ユニークな
  射影マーカーで機械検証した結果、すべて正解だった。サブクエリの「誤結果」
  を疑うときは必ず 1 クエリ = 1 セッション + マーカーで再検証すること。

## 2026-09-22 カバレッジ拡充セッションで判明した既知バグ(未修正)

1. **行値式 `(a, b) IN (SELECT ...)` が常に空**(HEAD でも同様、長期課題)。
   - 2026-09-23 追試で再確認。原因は 3 層に跨る:
     (a) フロントエンドが `(a, b)` を `StructConstructorWithParens` に低減し、
     `query/googlesql_ast_visitor.cpp` の構造体コンストラクタ経路は各フィールドを
     低減時に `TryEvaluate(Row(), Schema())` で先行評価するため、列参照を含む
     フィールドが deferred になる(deferred 経路の実行時スコープ解決が不完全)。
     (b) サブクウエリ側の `ProjectSubqueryRow`(executor/detail/
     subquery_runtime.cpp)は非 AS STRUCT の場合 1 列目しか投影しないため、
     複数列行値の構造比較がそもそも表現できない。
     (c) `expression_eval.cpp` の IN 経路のハッシュメンバーシップは
     単一ドメインの Value 前提。
   - 対応には frontend 低減・名前解決・両評価器(AST/bytecode)・
     サブクエリ投影の整合が必要で、機能追加規模の作業。
3. **`WITH RECURSIVE name(col...)` の列リスト非対応**、**`UNION` の
   DISTINCT/ALL 省略非対応**(いずれもパーサ文法ギャップ、GoogleSQL では
   標準形)。2026-09-23 追記: SQL フロントエンドは SHA256 ピン留め済みの
   外部 `execute_query` バイナリ(CMakeLists `_googlesql_executable`)であり、
   両ギャップはこの外部パーサの文法が起源。リポジトリ側での SQL テキスト
   前処理による回避は Roadmap の「ad-hoc workaround 禁止」原則に抵触するため
   行わない。対応には parser アセットの更新(SHA 再ピン留め)が前提。
4. **INSERT は PRIMARY KEY の一意性を強制しない**(`sql_dml_paths_coverage_
   test` で現行契約として pin 済み)。意図仕様なら docs 側での明記を推奨。
5. **高負荷(-j 並列 ctest)時のタイミング系フレーク**:
   `TransactionCoverageTest.ReadOnlyPreCommitFailsWhenDependencyBarrierBreaks`
   と `CliMainTest.ScriptExecutesAndPrintsRowsAfterCommit` が並列実行時に
   稀に落ちる(単独実行では安定)。外部パーサ子プロセスまわりの
   レース/リソース競合の疑い。
   2026-09-23 追記: build-asan でも高負荷(-j8 ctest × tidy -j16 並走)時に
   `SqlFunctionEdgeTest.ErrorTable` / `SqlExpressionMatrixTest.ErrorTable`
   (should-throw のクエリが throw しない方向で稀に失敗)を確認。
   単独シリアル実行では 10/10 安定。`DATE '...' + INTERVAL 24 HOUR` が
   負荷下で稀に成功へ転じる経路があり、式評価/プラン選択の
   非決定性の疑い。要 root-cause。

## 2026-09-22 カバレッジ拡充セッションで修正したもの

- `Project()`(`executor/relational.cpp`)の emit 連鎖から `} else {` が
  欠落し、非グループ化射影(FROM なし SELECT、`RelationalExecutor` 経由の
  クエリ)が常に 0 行を返していた。HEAD の分岐を復元。
- 型付き並列集約(`RunTypedParallelStreamAggregate`)の int_key 高速パスが
  NULL グループキーを 0 に畳み込み、NULL グループがキー 0 に統合される
  (`GROUP BY d` で NULL 行が 0 行グループに加算される)。NULL キーを
  正規化行インデックスへ迂回させて解決。ついでに typed 経路で
  `aggregate_updates` テレメトリが加算されない問題も修正。
- 同一表を 2 回参照する文(`reusable_base_relations` 登録対象)で型付き
  並列集約が共有ベースリレーションキャッシュの部分充填を読み、スカラー
  非相関サブクエリ `(SELECT MAX(a) FROM t)` が全行で誤値を返す。
  `!reusable` 時のみ typed 経路を使うゲートで回避(根本修正はキャッシュの
  fill/read 順序管理)。
- `main.cpp`: SQL 文ごとの Prepare 失敗で LastError が空(外部パーサ子
  プロセスの一時不調)の場合に 1 回だけリトライ。

## 2026-09-22 に検出した既知バグ(未修正)

1. **READ ONLY トランザクションで派生表(集約)の結合が黙って 0 行**
   - **解消済み(2026-09-23 再検証)**。q61 コア形状・フル形状の両方を
     `BeginReadOnlyContext()` で実行し、read-write と同一の 1 行を返すことを
     独立ハーネスで確認。pgwire 経由の派生表集約結合も正常。
   - 推定箇所: 派生表の実体化と read-only スナップショット
     (`full_scan_iterator.cpp` の physical-read fast path)の組合せ。
2. **GROUP BY ROLLUP でプラン探索が爆発**(q18: 実行は一瞬、計画に ~33 秒)
   - **改善済み(2026-09-23 計測)**。同一ビルドでの q18 は、コア共有なし
     5799 ms / 共有あり 1970 ms（どちらも 37 行。セット演算は Prepare 中に
     枝を実行するため、この時間のほぼ全ては計画で、実行は以前測った
     ~0.02 秒のまま）。2026-09-22 の記録 32547 ms からの短縮は約 16 倍。
     起点は MayApply 後の「計画 ~5.7 秒 / 実行 0.02 秒」。Clang Release
     (`-O3 -DNDEBUG`) では q18 は 295 ms、16 クエリ合計 960 ms
     (`BenchmarkHistory.md` の 2026-09-23 Release 節)。入った変更:
     (a) ROLLUP の UNION ALL 各枝が同一 FROM+WHERE コアの Cascades 計画を
         共有する(`SqlEngine::shared_grouping_cores_`。後続枝は必要列が
         計画スキーマに含まれるときだけ再利用);
     (b) 連結した結合グループは csg-cmp で一度だけ列挙する;
     (c) 結合コストの `TableStatistics` は `ScaleCardinality` で行数だけ
         縮尺し、ヒストグラムと MCV は複製しない;
     (d) 複数等値キーのカーディナリティは指数バックオフ
         (`EstimateEquijoinSelectivity`)。
   - 実施済み(先行): `setop_empty_*` / `no_op_window_elimination` 等 8 規則の
     MayApply プリフィルタ(target 複数指定)で 1 枝あたり 53613 → 36731
     ステップ(-31.5%)。`TINYLAMB_RULE_STATS=1` で規則別試行数を取得可能。

## 2026-09-22 に修正したもの(参考)

- `GROUP BY GROUPING SETS (())` 等の group-all 文が空入力で
  グランドトータル行を欠落(`relational.cpp` の stream_agg 経路の
  空入力条件に `IsGroupAll()` が含まれていなかった)。
- UNION DISTINCT 派生表への外側述語押し込みが効かない
  (`OptimizeDerivedBoundaries` の `is_union` 条件を kUnion/kUnionAll
  両対応に拡張。フィルタと DISTINCT は可換のため安全)。
- 集約高速化経路(FinishQuery 直集約)が `AggregateAccumulator` へ
  入力スキーマを渡していなかったため、ARRAY_AGG の全 NULL グループで
  要素型が `ARRAY<>` に崩壊(スキーマ引き渡しを修正)。
- グループ化クエリの `ORDER BY <集約式>`(TPC-DS q42)が
  "aggregate was not prepared" で失敗(ORDER BY 項の集約ノードを
  select list の同一集約へ引き当て、準備対象に追加)。

## 2026-09-23 カバレッジ拡充セッションで修正したもの

- **量化比較の membership stash ゲート反転**(`executor/detail/
  planning_heuristics.cpp`)。`col > ANY (SELECT ...)` 等の非等価量化が
  「等価 IN のみスタッシュ可」のゲートを素通りして等価キーセットとして
  stash され、空結果に縮んでいた。反対にゲートが守るべき `= ANY` 形は
  スキップされていた。条件の誤った `!` を外し、意図通り
  (`kIn` または `= ANY` のみ stash)に修正。
  回帰: `QueryTest.QuantifiedSubqueryKeepsOperatorAndNegation`
  (WIP で追加済みのテストが本修正で初めて緑化)。
- **論理規則 8 件の MayApply プリフィルタ**(`plan/cascades.{hpp,cpp}`)。
  `setop_empty_simplification` / `setop_empty_identity` /
  `no_op_window_elimination` / `join_empty_simplification` /
  `intersect_except_cost_based_lowering` / `grouping_sets_expansion` /
  `distinct_and_group_by_interchange` / `aggregate_projection_merge` が
  `Pattern::Any()` + `std::nullopt` target で全式×全訪問に試行されていた
  (TPC-DS q18 の 1 枝あたり探索ステップの ~36%)。`Rule` に複数 target
  演算子の ctor を追加し、body の実際の根演算子集合を宣言させた。
  1 枝あたり 53613 → 36731 ステップ(-31.5%)。body のゲート変更はなし
  (プリフィルタのみで意味論不変)。
  回帰: `CascadesTest.RuleTargetListPrefiltersOtherOperators`。
- **計測器**: `TINYLAMB_RULE_STATS=1` で探索の規則別試行数を LOG(INFO)
  に出力(env ゲート、無効時ゼロコスト)。プラン時間の内訳追参考期。
- **破損WIPの修復**: 未コミット作業ツリーに置換事故由来の構文破損が
  複数ファイルで発生(tpcds_benchmark.cpp / sql_session_fuzzer.cpp /
  query_test.cpp / function_call_ast_*_sweep_test.cpp /
  sql_expression_matrix_test.cpp / sql_statement_sweep_test.cpp /
  sql_function_edge_test.cpp / expression_eval_coverage_test.cpp /
  googlesql_ast_visitor.cpp の重複行)。文脈から復元しビルド通過まで修復。

## 2026-08-24 時点の記録(参考)

2026-08-24 時点。各改善計画書(tpcc-improvements / tpch-sf10-plan / improvement3)の
実施可能項目は全て消化済み。ここには**意図的に先送りした設計判断・
大規模項目**だけを残す。完了履歴は git 履歴と BenchmarkHistory.md を参照。

## 2026-08-24 再監査で消化した項目

- **D-8: AST 直評価の release 廃止** はバックログから除外した。
  `docs/expression_evaluation.md` では AST (`Expression::Evaluate` /
  `EvaluateBinary`) が意味論の正本、bytecode はバッチ実行用 IR であり、
  AST fallback は意図した構成である。したがって AST 廃止は現行方針と矛盾する。
  実際に残っていた detail 経路の `CONCAT(..., NULL, ...)` の差は、
  AST と同じ strict NULL 伝播に統一し、differential test で固定した。
- **D-1: LockManager 完全 MVCC 統合** — production の行ロック依存を外し、
  version chain 上の write intent へ統合した。TPC-C 5.11.0 のisolation testに
  合わせ、競合writerは短時間待機後に先行commitの最新版を読んで継続する
  strict write locking とした。未stage intentは通常readerには不可視である。
- **D-2: Engine facade** — `SqlEngine::Execute` / `QueryResult` を導入し、CLI、
  pgwire、TPC-C/TPC-H benchmark の準備・stream・affected rows 処理を統一した。
- **D-3: TransactionContext 分解** — `CatalogReader` capability を抽出して
  `Database*` を除去し、executor の `thread_local active_runtime` を
  `TransactionContext` 経由の明示 runtime に置換した。
- **D-4: relational IR の Cascades memo 接続** — `kRelational` logical operator
  と `RelationalPlan` implementation rule を追加し、CTE/subquery/outer join 等の
  specialized relational IR も memo/search を経由して物理実装を選ぶ。
- **D-5: PAX ページ型の統合** — `PageType::kPaxPage`、page union、v1 directory、
  null bitmap、固定幅/VARCHAR の `DataChunk` 永続化を実装した。
- **D-7: serdes BE固定 + magic/version ヘッダ** — scalar codec、page、WAL、
  master checkpoint record を big-endian v1 に更新した。ユーザー承認により
  v0 dual reader は設けない破壊的 format bump とした。

## 継続して先送りする項目

| # | 項目 | 保留理由と再開条件 |
|---|---|---|
| D-6 | **分散実行**(`distributed*.md`) | Raft、replication、network transport、snapshot/install、障害試験が未実装の新サブシステムで、小規模リファクタではない。分散化をロードマップに戻したとき、最初のスライスで `LogStorage` interface と単一ノード動作テストを追加 |
| D-9 | **保存順を保つ並列フルスキャン** — **2026-10-03 消化済み**。前提が陳腐化していた: 2026-10-01 の並列決定性パスで `ParallelScan` は既に morsel順ハンドオフ(静的ローテーション割当+consumer がチェーン順に morsel 排出)を実装済みで、出力は保存チェーン順。残るは計画側の主張だけだったため、`EmitsChainOrderedScan()` から並列閾値条件を除去し、ordered モードの `ParallelScan`(行フィルタ pushdown を拒否 — deferred 駐留が順序を壊すため)が clustered order を報告するようにした。回帰: `FullScanClusteredOrderTest.ParallelShapeKeepsAndDmlSourceWithholds`、`SqlClusteredOrderTest.ParallelScanKeepsClusteredOrderAboveThreshold`(24020行・ソートなし昇順) |

意味論・公開API・disk formatを理由に保留していた項目は残っていない。
ユーザー判断により破壊的変更を許容してD-1〜D-5/D-7を消化済み。D-8(DML WHERE の
非相関サブクエリ)は2026-10-03時点で実装済み(`DecorateDmlWhere` の EXISTS/スカラー/
量化比較の畳み込み。実機で DELETE ... WHERE k IN (SELECT ...) / UPDATE ... WHERE k =
(SELECT MAX ...) / NOT EXISTS を確認)のため表から除外、D-9(並列保存順スキャン)も
2026-10-03 に消化済み(上記)。継続保留は分散基盤D-6のみである。

## 測定ゲート

性能系の効果検証は次のコマンドで行い、結果を BenchmarkHistory.md に記録:

- TPC-H: `tinylamb_tpch_benchmark`
- TPC-C: `tinylamb_tpcc_benchmark --clients 10 --seconds 60`
- ベンチ回帰ゲート: `scripts/bench_gate.py`(CI: .github/workflows/benchmark.yml 週次)

## 2026-09-23 コード健全性セッションの成果と残項目

**達成:**
- コンパイラ警告 0 件(GCC 15.2 / Clang 21.1、`-Wall -Wextra -Wpedantic -Wshadow
  -Wformat=2 -Wundef -Wextra-semi -Wimplicit-fallthrough -Wconversion
  -Wsign-conversion`。`build-warn-gcc` / `build-warn-clang` は
  `TINYLAMB_ENABLE_CONVERSION_WARNINGS=ON`)。
- ASAN / TSAN 両ビルドで 3152/3152 全パス(データレース・メモリ違反なし)。
- clang-format 全 659 ファイル適用済み。
- `/tmp` に蓄積していた漏出 spill ファイル(tinylamb-spill-*.bin 約 5.3GB)
  を掃除。SpillFile デストラクタは正常。蓄積は過去のファザー abort 実行の
  副作用 → 長時間ファズ sweep の後は `scripts/clean_root_artifacts.py` を
  推奨(自動化済み・CIは既に実行)。
- WAL セグメント 0 はベースファイル自身(`SegmentPath(base, 0) == base`)
  という契約を `MigrateLegacyLog` 含め確定・実装。
  `LoggerTest.AppendExponential` は 1GiB セグメントを明示指定して
  回転を回避(回転経路の asserts は別テスト群が担当)。
- `test_workspace_cleanup_env` 系の並列 ctest 競合:同一バイナリの gtest
  ケースが同一 DB 名を共有すると -j で WAL 破 corrupt → 5 テストフィクスチャの
  DB 名に `RandomString()` サフィックスを復元し解消
  (sql_function_coverage / sql_expression_matrix / executor_feature /
   sql_statement_sweep / sql_plan_and_cast_sweep / sql_extra)。
- `EmitSnapshotIfNeeded` を `StatusOr<lsn_t>` にし、FPW シンク失敗時に
  データファイル書き込みを中断させる(WAL-before-data の強化)。

**残項目(次回以降):**
- clang-tidy: プロダクションコード(common〜server)は 0 件。
  plan/executor/query のテストファイルにニット(設計初期化子・生文字列・
  `char[]`→`std::array` 等)が残存。ベースライン 376 件から大幅減。
- 行値式 `(a, b) IN (SELECT ...)`(上記 1 項目、機能追加規模)。

## 2026-09-27 健全性パス(並行セッション)が発見した Debug ビルド-blocking 項目

- **B+Tree assert: `FindLeftmostPage` が非 branch ページに到達**
  (`index/b_plus_tree.cpp:67`)。再現: Debug/coverage ビルドで
  `sql_session_fuzzer_test --gtest_filter='SqlSessionFuzzer.SeededSessionsHoldState'`
  (数シード以内に決定的に発生)。直前のログは
  `Restoring meta allocator high-water mark: 0 -> N` +
  `Rebuilt free list: 1 pages` + pages 1..N が全て "broken, start SPR" という
  新リカバリ経路(高位ウォーター復元 + torn-page sweep + フリーリスト再構築)の
  後。SPR 再構築後のページ型と木の親チェーン(latest_page_ / lowest_page_)の
  整合、または meta 復元が DPT/木ページを巻き込んでいないかの確認が必要。
  Release(NDEBUG)では同一経路で 3765/3766 テストが通るため、黙って通過して
  いる可能性にも注意。
- **TSAN lock-order-inversion 報告** (`postgres_session_fuzzer_replay_test`,
  exit 66): ワーカー target→meta(`ReplayAllocation→GetMetaPage`) と
  メインの sweep(`RecoverFrom:1538` 経由) meta→target が TSAN のグローバル
  閉鎖グラフで循環。フェーズ直列化(pages.clear() → worker 起動 → join 後
  sweep)により実デッドロックにはならないが、設計意図とテスト安定性の観点で
  `docs/lock_order.md` の「Recovery restart」節を参照。meta を最後に取得する
  よう DPT バッチ(`RecoverFrom` 1392 付近)は調整済み。
- **[回帰] `optimizer_cse_projection_high_expectations_test` が失敗**
  (`duplicate_uncorrelated_scalar_subquery_is_shared`)。04:06 開始のフル ctest
  では Passed(#2597)、08:30 のフル ctest では Failed に転化。実値
  `m2 = (SELECT MAX(a) …) + 1` が `11`(= 行の `a` 10 + 1)となり、共有済み
  サブクエリ結果スロットでなく外側列 `a` を束縛している(スロット束縛/CSE の
  バグ)。05:23 以降に `query/sql_engine.cpp` 等へ加えられた編集と相関。
  CSE×スカラー副問い合わせのスロット割り当てを確認されたし。
