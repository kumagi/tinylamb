# won't-fix / work-as-intended クローズ案件の再精査レポート

- 実施日: 2026-11-23〜24 (リポジトリ HEAD: 909b1b5 + 作業ツリー)
- 対象: ローカル Bug Tracker 上の `won't-fix` / `work-as-intended` 全 5088 件 (work-as-intended 4461 / won't-fix 627、うち 10 件アーカイブ済み)
- 観点: (a) 最新コードに対してもクローズ判断が妥当か、(b) 妥当として、コードを次回読むエージェントがバグとして再起票しうるか

## 方法

- high/normal 609 件 (重複含む、588 一意タイトル) は 59 バッチに分割し、各案件のクローズ理由ポストを現行コード上でシンボル/文字列検索により実証確認した (ビルドなし、読み取り監査)。
- low 4479 件 (3995 一意) は機械 triage: 引用ファイルの存在、タイトル/本文のシンボル生存チェック、クローズ理由文のパターン分類、怪しい案件(8件)の個別確認、12件のランダムスポット検証を実施。

## 結果サマリ

| 区分 | 件数(一意) | 処置 |
|---|---|---|
| high/normal hold_low (判断妥当・痕跡あり・再票されにくい) | 509 | なし |
| high/normal hold_refile (判断妥当だがコード内痕跡なく再票リスク) | 62 | 各案件に検証結果を post 済み |
| high/normal stale-gone (後続修正で論点消滅・判断実質上書き) | 14 | 各案件に post 済み (ステータス維持) |
| high/normal stale-bug-now (現行コードで判断不成立・実質バグ) | 3 | post + **needs decision** に変更 |
| low 機械 triage hold | 3987 | なし (うち reserve 系 2343 は下記の注記参照) |
| low 個別確認 (files-gone/typo-fixed) | 8 | 全件 hold 確認 (ファイル移動・行番号ズレのみ) |
| low クローズ理由ポスト皆無 | 5 | 判断維持。検証根拠を post で補完済み |

## 実質バグとして needs decision に戻した 3 件

### #15057 EncodeJsonString fast path appends raw UTF-8 bytes without validating them
- 現行位置: `common/json_string.hpp:81-84 EncodeJsonString fast path`
- 理由: fast path 無検証パススルー現存。BYTES 別経路は不成立: FROM_HEX/b'\xff' が直行

### #15392 TinylambOpt.lean imports 8 ProposedGoals but zero ProposedProofs
- 現行位置: `lean/CheckProofs.lean:1 / lean/TinylambOpt.lean:117-125`
- 理由: CheckProofs.lean は全14行で TinylambOpt のみ import。lean/ 全体で ProposedProofs の import 箇所はゼロ。証明は今も guarded 環境外のまま。

### #9238 [normal] type/row_block_codec.cpp:265 の col_count 無上限 reserve が Status 境界を越えて bad_alloc
- 現行位置: `type/row_block_codec.cpp:309-314`
- 理由: サイトは特定可能。弱検査直後の raw reserve 現存で sizeof(Value) 倍増幅 bad_alloc が StatusOr 境界を越える。隣接配列側のみクランプ済み

## stale-gone (後続修正で論点が消滅している 14 件)

クローズ時点の判断は正しいが、その後のコミットでコード側が変更され、WAI 前提そのものが消えている。次の大規模リネーム/削除時に obsoleted への整理を推奨:

- #10595 [normal] TryConsume の無制限グラント経路が使用量のオーバーフローを検査しない — 無制限経路は fetch_add から CAS 飽和加算に書換え済み（8e91eef0）。rationale の lock-free 前提・
- #11804 [normal] expression/jit.cpp:352 ライブラリコードで abort/LOG(FATAL) — jit.cppからstd::abort/LOG(FATAL)全廃。Comparison既定節はnullptr返すfail-closed化、論
- #11997 [normal] executor/parallel_hash_join.cpp:443 同一関数内の複数 mutex 取得（ロック順序要確 — 指摘されたerror_mutex例外ハンドラは現行ファイルから消滅。RunParallelTasksへ一元化済み。
- #12023 [normal] executor/parallel_hash_join.cpp:660 同一関数内の複数 mutex 取得（ロック順序要確 — 現行ファイルにerror_mutexもcatchブロックも残存せず。報告箇所自体がリファクタで消滅し論点不存.
- #12027 [normal] executor/parallel_hash_join.cpp:674 同一関数内の複数 mutex 取得（ロック順序要確 — #12023と同一。現行にerror_mutex/catchなし。対象コード消滅で論点不存.
- #15039 ReleaseFile on Windows can double-close the underlying handle when _cl — 現行 ReleaseFile の Win 分岐は CloseFile のみ。報告の CloseHandle 回収は削除されコメントで明文化済
- #15055 SortedDistinctRuns::ForEach consumes values from a merged run but on a — consume例外でもfinalがlevels_へ返る設計に修正済み。retry可能性をテストがピン留め。
- #15255 Extra runtime filters silently capped at four, dropping join predicate — 現行コードはコンスーサ上限4→7になりproducer側上限7と一致。「4件で黙って切る」前提が消滅。
- #15384 [normal] expression/bytecode.cpp:3611 の列指向ショートカットが spec 添字を境界チェックせず配列外 — 列指向ショートカットは現行で operand<size() を検査(:4183/:4213)。無検査前提の WAI が不成立。
- #15822 [normal] common/incarnation.hpp:463 キャッシュ更新時に dynamic_bytes が nullptr  — 更新臂はdynamic_bytes非null時のみ上書きしコメント追加。報告のnullptr無条件上書きは修正済みで論点不存在
- #15828 [normal] expression/aggregate_expression.cpp:17 HAVING 修飾子が条件 null でも文 — 現行 ToString は having_cond_ を必須化し dangling HAVING を出さない。報告の論点が現存せず
- #15838 [normal] ooc_setop_incsort_sink.cpp:1298 prefix_equal が値タグの照合を落とし上流ソート — 現行prefix_equalはSortKeyOrderで静的collation+値タグを両方解決(collation.cpp:243-252
- #15907 [normal] coord の両 cap が 0 のとき AcquireSlot が無応答 — 両cap=0は起動時に拒否されるGuardが現存(exit 2)。無応答シナリオは発生せず論点消滅
- #16764 [high] ZigZagJoin が右側全行を予算チェックなしでメモリに抱える — LoadRightはTryAdd採用済みのため『予算チェックなしで抱える』は現行で不成立。wai判断は後続修正で上書き。

## hold_refile 62 件 (判断は妥当、再起票リスクあり)

ロック順序の静的監査誤検出 (docs/lock_order.md 準拠だが箇所にコメントなし) が大半。各案件に現在のコード位置と理由を post 済み。将来エージェントが同じ箇所を読んだとき再起票しうるため、余裕があれば該当箇所に意図のコメント追加を推奨:

### ロック順序静的監査 (29 件)
- #11896 [normal] common/memory_budget.cpp:1019 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11911 [normal] common/platform/socket.cpp:220 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11931 [normal] database/database.cpp:1202 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11932 [normal] database/database.cpp:1215 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11948 [normal] executor/detail/parallel_table_scan.cpp:2434 同一関数内の複数 mutex 取得（ロック順序要確認
- #11949 [normal] executor/detail/parallel_table_scan.cpp:2446 同一関数内の複数 mutex 取得（ロック順序要確認
- #11950 [normal] executor/detail/parallel_table_scan.cpp:5103 同一関数内の複数 mutex 取得（ロック順序要確認
- #11952 [normal] executor/detail/parallel_table_scan.cpp:5164 同一関数内の複数 mutex 取得（ロック順序要確認
- #11956 [normal] executor/detail/scan_filter.cpp:6664 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11961 [normal] executor/detail/subquery_runtime.hpp:194 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11962 [normal] executor/detail/subquery_runtime.hpp:205 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11964 [normal] executor/detail/subquery_runtime.hpp:230 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11966 [normal] executor/detail/subquery_runtime.cpp:3331 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11972 [normal] executor/detail/subquery_runtime.hpp:241 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11987 [normal] executor/parallel_aggregation.cpp:1673 同一関数内の複数 mutex 取得（ロック順序要確認）
- #11988 [normal] executor/parallel_hash_join.cpp:165 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12016 [normal] executor/parallel_scan.cpp:1421 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12021 [normal] executor/parallel_scan.cpp:1484 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12082 [normal] page/page_manager.cpp:2418 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12280 [normal] table/table.hpp:62 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12332 [normal] tools/distcomp/distcomp_coord.cpp:105 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12344 [normal] transaction/transaction.cpp:979 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12360 [normal] transaction/transaction_manager.cpp:1373 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12371 [normal] transaction/transaction_manager.cpp:1641 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12375 [normal] transaction/transaction_manager.cpp:1733 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12657 [normal] common/platform/socket.cpp:299 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12684 [normal] executor/detail/planning_heuristics.cpp:2210 同一関数内の複数 mutex 取得（ロック順序要確認
- #12688 [normal] executor/detail/subquery_runtime.hpp:282 同一関数内の複数 mutex 取得（ロック順序要確認）
- #12722 [normal] executor/hash_join.cpp:1898 同一関数内の複数 mutex 取得（ロック順序要確認）

### その他 (27 件)
- #10401 [high] [RACE] LabelContinues が完全な4連クォートの文字列リテラル '''' を「未閉のトリプルクォート」と誤判定し、ダンプ残り全行
- #11762 [normal] common/status_or.hpp:67 ライブラリコードで abort/LOG(FATAL)
- #11813 [normal] query/googlesql_compliance_file.cpp:2217 stoi/stoull の例外未処理
- #12619 [normal] relational.cpp: radix join の高速 NULL スキップで null ビットマップの長さを検証していない
- #13279 [normal] query/googlesql_ast.cpp:128 引用スパン走査がバッククォート内でもバックスラッシュをエスケープとみなす
- #13545 [normal] table/full_scan_iterator.cpp:2240-2248, 2283-2290 stub 一致計算が int_cells 
- #14096 [論理] XOR の NULL 排斥判定が片側だけ
- #14859 [ポータビリティ] 非 Windows 分岐で Linux 専用 O_DIRECTORY を使用
- #14917 [null 参照] 予約失敗を空 shared_ptr で成功返却
- #15035 LinearPrefixVM::DescribeDelimitedCapture may return prefixes that omit mandatory
- #15037 SyncDirectory uses O_DIRECTORY which is not defined on all supported POSIX platf
- #15042 RunProcess POSIX path treats a stopped child (WIFSTOPPED) as an error but waitpi
- #15052 ASSIGN_OR_ASSERT_FAIL_CONST validates only the status code, not value presence
- #15084 [normal] executor/parallel_hash_join.cpp:619 サイズ saturate 後も output_.reserve(tot
- #15087 [normal] executor/relational.cpp:13873 INTERSECT の早期 break 削除で後続ブランチのエラーが顕在化する
- #15108 [normal] relational_sort_limit.cpp:612 TotalRows()*160 が size_t をオーバーフローする
- #15121 [normal] ooc_setop_incsort_sink.cpp:607 leaf_reserved + ready_bytes が size_t をオー
- #15142,#16082 [normal] expression/jit.cpp:3876 Extract の use_sel 経路で selection 長が検証されない
- #15187,#16106 [normal] query_data.cpp:599 単一列リレーションの struct 化がテスト専用 proto 型名をハードコードする
- #15272 [normal] plan/cascades_search.cpp:1236 部分的な sort_ascending が provided ordering を
- #15283 [normal] googlesql_ast_visitor_expr.cpp:469 の 8 進エスケープが 0xFF 超を 2 バイト UTF-8 として符
- #15391 CHECK_DEADLOCK FALSE in every cfg hides a reachable stuck state in the fixed mod
- #15810 [normal] expression/bytecode.cpp:243 ExprMayBeUnsigned が部分文字列 'UINT' だけで UINT を判
- #15980 [normal] fused_grouped_join.cpp:4038 dense PK インデックス構築の並列失敗が握りつぶされ部分的な grp.dense
- #16474 [normal] SetFailed の work_cv notify も work_mu_ の外
- #16656 [normal] ReadLeafRows 对 spilled leaf 的空 store/无效段静默返回成功
- #16863 [normal] scale 指定の無い 105 万行テストが 60 秒制約に抵触しうる

### docs 行番号ズレ (5 件)
- #16544 [normal] PreCommit の行番号が 15 行ずれている
- #16547 [normal] Abort の行番号が 20 行ずれている
- #16557 [normal] CommitVersions の行番号が約 1900 行ずれている
- #16559 [normal] コミット AddLog の行番号が 16 行ずれている
- #16562 [normal] §5.4 の Abort 行番号が本文 §0 とも矛盾してずれている

### static共有状態 (1 件)
- #11745 [normal] executor/detail/build_input.cpp:9836 関数内 static 可変状態（データレース疑い）
## low 重大度 4479 件の一括 triage

### 構造的発見: auto-generated reserve 系が 59%

- `[low][PERF] <file> の vector X が reserve なしでループ内 push_back される` パターンの自動生成案件が **2343 一意 (2648 件)**。
- クローズ理由の大半が定型文 (`no-safe-automatic-fix` / `no-safe-size` / 「サイトを特定できませんでした」 のいずれか、うち 225 件は site 未特定のままクローズ)。
- 12 件のランダムスポット検証では 5 件がこの reserve 系で、うち 4 件は『適切なサイズが無い』という定型理由と矛盾して **ループ回数が宣言時点で確定している** (例: #7661 は ceil(flat_refs.size()/kChunk)、#6114 は WithQueries().size()、#7659 は sig_slots.size())。#8767 は『サイトが見つからない』に対し宣言行が現行 #108 に実在。
- とはいえ影響はテスト/ベンチ/マイクロパフォ水準で正しさには無関係。プロジェクトとして手動 reserve の自動追加を避ける方針 (参考: #16938 で手動 2 倍成長 reserve 自体を重複とみなす判断あり) と整合しており、**バグ再興ではなく方針判断として保持**が妥当。
- 再起票リスク: 次回静的スキャンエージェントは同じ族を量産する。**「reserve 系マイクロパフォ指摘は方針として自動対応しない」旨の一文をプロジェクト文書 (例: AGENTS.md §3 か TODO.md) に明記する**のを推奨。本レポートがその根拠記録を兼ねる。

### 非 reserve 系 1652 一意

- 機械チェック (引用ファイル存在・シンボル生存) で全件ファイル/シンボルは現行ツリーで確認。files-gone 8 件は全て個別確認済みで判断維持:
  - #9157 → `common/platform/synced_stream.hpp` に移動、フォールバックにも `emit()` 実装ありで理由どおり
  - #9282 → `common/platform/endian.hpp` に移動、`!defined(htobe16)` パターンと Linux/macOS/Windows 限定方針は不変
  - #14658 → `ooc_window_sink.cpp:3562` に `active_mem_rows_.shrink_to_fit()` 現存。パーティション終了時にメモリトークンを全返却する設計で理由維持
  - #14864 → `common/runtime_filter.cpp:526` の `&distinct` はコンストラクタ内で dense ビットマップ構築時のみ使用、メンバ `exact_keys_` は所有 parts からのみ借用で宙吊りなし (理由ポストの主張をコードで確認)
  - 残り 4 件は codespell 誤検出系 (#15006 #15007 #15009 #15031、`unparseable` はむしろ正しい綴り) で WAI 維持
- クローズ理由ポストが皆無だった 5 件 (#7193 #11858 #12383 #12484 #13736) は、現行コードで検証した判断根拠を各案件に post で補完済み。全て判断維持。
- 条件付きクローズ (設計変更時に再評価) 3 件: #15902/#16186 (`bin_size_t` を 32bit 化したら再設計)・#16955 (WAL セグメント fd キャッシュの index 退去)。発動条件が tracker に残っており可視性十分。判断維持。
- 「既に修正済み」としながら WAI で閉じている 4 件 (#13142 #13940 #14676 #15126) はステータスは obsoleted が本来。実害はないので次回の整理時でよい。

## 総合結論

1. **クローズ判断の健全性**: high/normal 588 件のうち現行コードで判断が崩れたのは 3 件 (0.5%) のみ。low 主体の自動生成 reserve 族は『理由の定型文が不正確』なものが混在するが、実害基準では方針として許容範囲。全体として WAI/WF 運用は概ね健全。
2. **再起票リスクの実態**: 最大のリスク源は (a) 自動スキャンによる reserve 族・codespell 族の再量産 (プロジェクト文書への方針明記で対策)、(b) hold_refile 62 件のうち 29 件を占めるロック順序静的監査誤検出のように『docs/lock_order.md 準拠だが箇所別のコメントなし』案件 (tracker 側 post で記録済み、任意でコードコメント化)。
3. 実質バグ疑いの 3 件 (#15057 TO_JSON の不正 UTF-8 スループット、#9238 DecodeSpillRow の reserve 増幅 bad_alloc、#15392 Lean の ProposedProofs 未 import) は needs decision に戻して検証結果を post 済み。

### 検証データ一式 (再検証用)

- 判定 JSONL: `~/tinylamb_ws/wai_audit_2026-11-24/verdicts/batch_*.jsonl` (hi+normal 588 グループ、バッチ入力も `batches/` に保存)
- low 機械 triage: `~/tinylamb_ws/wai_audit_2026-11-24/low_mech.json`・クローズ理由ダンプ `low_rationales.json`
- spot 検証: `~/tinylamb_ws/wai_audit_2026-11-24/spot_verdicts.jsonl`・post 適用一覧 `todo_posts.json`