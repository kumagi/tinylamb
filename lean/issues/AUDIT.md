# 最適化 30 課題の仕様監査

各節の冒頭は元の課題で求めた命題。続いて実際の定理の結論、適用条件、検証範囲を記した。実装に適用する際の新しい証明課題は [研究 Issue](research/README.md) に収録した。証明の対象は各 Lean モジュールで定義された計算。

## checked-eager-sum

片側の INT64 値を結合キーごとに先に足しても、元の GROUP BY と成功・エラー・NULL を含む出力 bag が一致する十分条件を導く。

SQL の出力 bag を証明。sql_checked_sum_reaggregation は各段が実在キーを列挙し、任意の最終グループで NULL と重複を保存する。部分・結合後・再集約の各バケットに正負寄与の範囲を要求する。結合キーでまとめる場合は全入力の寄与と最大 fan-out から十分条件も導いた。 [Lean モジュール](../TinylambOpt/CheckedEagerSum.lean)

## nullable-mark-join

相関キーごとに build が空か、NULL を含むか、等値ヒットするかを蓄え、外側の各行で NOT IN の三値を復元する。

右入力から作る `buildMarks` の各エントリが相関等号でフィルタしたバケットの要約と一致する。`correlated_not_in_rows` は空 build、NULL キー、別キーの NULL を含む射影列、`built_mark_join` は WHERE の通過列も比較する。 [Lean モジュール](../TinylambOpt/NullableMarkJoin.lean)

## outer-or-branches

ON p OR q を二つのマッチ枝と一つの未マッチ枝に分け、右の NULL 補完を含む bag の等価性を示す。

元の bag 等価を証明。各左行の相手を排他的に分割し、両枝が空のときだけ補完する。重複行と UNKNOWN を含む。 [Lean モジュール](../TinylambOpt/OuterOrBranches.lean)

## window-halo

出力位置 [i,j) と ROWS BETWEEN a PRECEDING AND b FOLLOWING に対し、必要な前後行を足して入力を切り、出力を切り戻しても各窓値が一致する。

順序確定済みの一パーティションについてフレームと出力を保存する。安定順序とパーティション分割は入力条件。 [Lean モジュール](../TinylambOpt/WindowHalo.lean)

## outer-fetch-ties

左側だけの順序キーで並べる LEFT JOIN に対し、左から上位 K と同率の行を残し、結合後に元の FETCH WITH TIES を適用しても出力 bag が一致する条件を求める。

元の leftOuterJoin の出力列について証明。任意の三値 ON、未マッチ行、境界の重複を含む。左右で同じ左キー順、OFFSET・後段フィルタなし。nullableRank は NULL を INT64 の両端の外に置く。 [Lean モジュール](../TinylambOpt/OuterFetchTies.lean)

## filter-eval-order

p AND q の順を入れ替えても、WHERE の通過行と観測されるエラーが保存される、両方の全域性より弱い十分条件を示す。

決定的な行評価について、通過とエラー種別を保存する条件を証明。両述語の全域性より弱い条件でよい。適用時は全対象行で条件を導く必要がある。 [Lean モジュール](../TinylambOpt/FilterEvalOrder.lean)

## mask-to-ranges

マスクの連続した1の区間だけを読み、順につなぐと、元の行列を選択ビットで filter したものに等しい。

`physical_ranges_sound` は物理 BitVec 列を LSB 順で読み、有効行数で切ったあとに区間を生成する。展開列の一致に加えて全区間の正長と隣接区間の間に最低一行の隙間があることを証明。行 63・64 が一つの区間になるケースは高位 padding を全部 1 にして検査した。 [Lean モジュール](../TinylambOpt/MaskToRanges.lean)

## adaptive-selection

処理済みのカーソルを引き継いで表現を切り替えた後、全出力と再開位置が最初から一方の表現で処理したときと一致する。

`physical_switch` は切替前に消費したビット数から、残りの密マスクが疎な選択行列の drop と一致することを導く。先頭オフセット 0、有効行 0、容量 0 を含む。`cursor_after_switch` は二回の要求後の物理カーソルも比較する。 [Lean モジュール](../TinylambOpt/AdaptiveSelection.lean)

## lazy-dictionary

選択済み行が初めて使う辞書コードで述語を評価し、キャッシュした結果を配ると、逐次評価と最初のエラー位置まで一致する。

キャッシュの健全性を保存し、出力と最初のエラー行の一致を証明。入力は選択済み行、述語は辞書値・世代に対して決定的。未参照コードのエラーは発生しない。 [Lean モジュール](../TinylambOpt/LazyDictionary.lean)

## late-gather-versions

選択時に保存した行参照を使う後段 gather が、最初から値を保持した結果列と一致するための参照寿命とスナップショットの条件を求める。

`gather_after_append` は pin が旧チェーンを保持し、追加版がすべて snapshot より後に生まれるという更新規則から、任意の保存スロット列の値を復元する。compaction が物理スロットを再利用する実装には別の寿命証明が要る。 [Lean モジュール](../TinylambOpt/LateGatherVersions.lean)

## limit-error-frontier

行順の参照器が K 件通過した瞬間に停止する場合、バッチ内の K 件目とエラーレーンの前後関係から同じ出力とエラーを決められることを示す。

`limit_rows_and_error` と `limit_batch_trace` は失敗前の全出力、最初の失敗、停止または残需要を複数バッチで比較する。分類済みレーンが無副作用で、親の需要まで逐次観測する契約が前提。既に子で失敗したバッチのエラーを外側の LIMIT で消す変換には使えない。 [Lean モジュール](../TinylambOpt/LimitErrorFrontier.lean)

## snapshot-zone-map

基底ブロックと可視な更新差分それぞれの範囲要約を使い、読み飛ばし後の行が MVCC 参照スキャンと一致する条件を証明する。

`snapshot_skip_with_deletes` はバージョンを commit 時刻の降順に選び、最も新しい可視版が墓標なら行を消す参照読取りに対して除外を証明する。物理順序は任意。テストでは同じキーの更新・削除・再挿入を三時点で読む。要約は対応する版の値を被覆する必要がある。 [Lean モジュール](../TinylambOpt/SnapshotZoneMap.lean)

## block-kleene

列要約から式が取り得る TRUE/FALSE/UNKNOWN/ERROR の上界を計算し、TRUE と観測可能な ERROR がないブロックを丸ごと飛ばせることを示す。

比較は全域な整数述語を受け取り、NULL と評価エラーを伝播する。`dictionary_block_skip` は辞書値のデコード結果から列要約を作って被覆を導き、三値の短絡を含めて通過行も観測可能なエラーもないことを示す。 [Lean モジュール](../TinylambOpt/BlockKleene.lean)

## trigram-skip

エスケープ解釈後の必須リテラルから選んだ3バイトがブロック要約にないとき、そのブロックには一致行がないことを示す。

`raw_trigram_skip` はエスケープを解釈したパターンの連続リテラル片を UTF-8 の参照 LIKE に結び、入力行から構築したハッシュ要約で判定する。対象片が本当に連続するリテラルトークンであることを適用条件に含む。ハッシュの単射性は不要。 [Lean モジュール](../TinylambOpt/TrigramSkip.lean)

## topn-block-fence

ブロックのどの行も現在の K 番目を改善できないと判定する辞書式境界を構成し、残りを読んだ参照 Top-N と照合する。

`sorted_payload_fence` はペイロードを保つ二本の安定ソート済み run の merge と take K を比較する。確定候補が K 件以上、保持した先頭 K 件が閾値以下、残余の上界が閾値より厳密に悪いことが条件。同率保持は既存の順位定理で扱う。 [Lean モジュール](../TinylambOpt/TopNBlockFenceExplicitTerms.lean)

## bloom-block-intersection

完全な build キーの Bloom と probe ブロック要約のビット積がゼロなら、そのブロックから inner equi-join の出力は生じないことを示す。

`nullSafeSkip_sound` は build/probe の実キーから作ったビット集合と両側の NULL 存在を組み合わせ、NULL 安全等号でも一致がないことを証明する。通常の等号の非互換時フォールバックは従来の補題を使う。 [Lean モジュール](../TinylambOpt/BloomBlockIntersection.lean)

## rle-run-skip

値が一定なランについて、述語を一度だけ評価し可視マスクとの交差を出力すると、展開・可視性判定・行評価の参照順と一致する。

`validated_run_scan` は物理 run の開始位置・長さ・終端を検査する実行可能なガードから被覆を導く。正しい run は展開と同じ行列・失敗を返し、不正な run は拒否する。述語は run の値に依存する。 [Lean モジュール](../TinylambOpt/RleRunSkip.lean)

## rank-scatter

選択レーン i の出力位置を下位 i ビットの popcount として scatter した配列が、逐次 filter の順序と長さに一致する。

`lower_bits_popcount` は低位ビットの抽出と BitVec.cpop をレーン列の rank に対応づけ、`packed_scatter_slot` はその位置に元の値を書いたことを示す。幅と同じシフトを必要としない。`bounded_scatter` は容量分だけ実際に書いた配列を読み戻し、保存した物理位置からの再開とつなげて逐次 filter の全値を復元する。 [Lean モジュール](../TinylambOpt/RankScatter.lean)

## first-error-ballot

行ごとに複数の演算を持つ式で、短絡活性マスクを掛けたエラーから辞書式に最初の (行,演算) を選ぶと、参照器と同じエラー種別を返す。

`packed_error_ballot` はパックしたエラービットの非ゼロを検査して BitVec.ctz を使い、行番号・演算位置・エラー種別を復元する。`and_active_error_ballot` は AND の左辺の値と失敗から右辺の活性を導く。投機する演算は失敗をデータとして分類できることが前提。 [Lean モジュール](../TinylambOpt/FirstErrorBallot.lean)

## simd-in-range

lo ≤ x ≤ hi を U(x-lo) ≤ U(hi-lo) に置換する W ビット lowering が INT64 の全域で一致する適用条件を求める。

半周の幅制限を外した。`simd_in_range_sound` は任意の整列した signed 区間、`int64_range_lowering` は実際の 64-bit の減算と unsigned 比較について証明する。MIN..MAX も含む。LLVM への適用条件は wrapping sub。 [Lean モジュール](../TinylambOpt/SimdInRange.lean)

## simd-group-conflicts

同一スロットのレーンを検出して一回だけ scatter するバッチ更新が、各グループの nullable COUNT/SUM 状態の逐次更新と一致する。

`representatives_per_key` は実際の代表表を任意のキーで引いた COUNT と nullable SUM を逐次更新と比較する。`checked_batch_scatter` は検査付きで構築した代表表全体を返す。ゼロおよび既存状態から始めた各キーの prefix が INT64 に収まる条件で、検査付き部分和・最終更新も同じ状態を返すことを示す。 [Lean モジュール](../TinylambOpt/SimdGroupConflicts.lean)

## deopt-resume

バッチ途中のガード失敗時、確定済みの接頭辞と保存したカーソルからスカラー経路を再開すれば、最初から参照経路を走らせた結果とエラーが一致する。

`arbitrary_guard_resume` は各行の JIT 固有ガードを任意の関数とし、書きかけの出力と集約状態を復元して同じ行から再評価する。スカラーで成功する guard miss と実際の算術エラーをともに扱う。 [Lean モジュール](../TinylambOpt/DeoptResume.lean)

## masked-load-tail

残り r<W の入力から生成する短いロード列が、有効な r レーンでスカラー読取りと等しく、割当て外のアドレスを一度も読み取らないことを示す。

`generated_tail_load` は有効長から作ったビットマスクを読取り命令に渡し、発生したアドレス列が有効行だけのスカラー読取りに等しいことを示す。割当て境界と値は既存の読取りトレースの定理と合成する。実バイナリの load 命令は検証範囲外。 [Lean モジュール](../TinylambOpt/MaskedLoadTail.lean)

## like-segment-chain

二片用の既存定理を任意長のトークン列に拡張し、先頭・末尾アンカーを持つ LIKE が、各片の非重複な出現位置を探すコードと一致することを示す。

`anchored_chain_iff` はアンカーの重なりを許さず、固定した先頭・末尾の間で任意数の片を検索する。空のアンカーで開いた端を表す。バイナリ照合で %ab が aba を拒否し abab を受理する回帰例を追加した。 [Lean モジュール](../TinylambOpt/LikeSegmentChain.lean)

## like-underscore

入力とパターンの対象区間が ASCII なら _ を1バイト消費として生成したコードが参照 LIKE と等しいこと、および高速経路を途中で離脱しても結果が保存されることを示す。

`tokenize` はエスケープされた %・_ と末尾のバックスラッシュを扱う。参照器の % 再試行は UTF-8 文字単位で進み、不正な先頭バイトの幅を 1 とする。`raw_like_fast_sound` は全行 ASCII なら高速経路、それ以外は行頭からの再評価を含む。 [Lean モジュール](../TinylambOpt/LikeUnderscore.lean)

## like-shift-and

文字ごとのビットマスクと carry を使う複数ワードの更新で、各入力位置の一致ビットが「その位置で終わるリテラル接頭辞」と一致することを示す。

`shift_and_contains` は状態をワード列に保ったまま一回走査し、全境界での受理の有無と、行中に非空 needle が存在することの同値を証明する。行末より前の一致を含む。 [Lean モジュール](../TinylambOpt/LikeShiftAnd.lean)

## regex-mandatory-literal

受理した正規表現 AST から抽出したリテラルについて、参照検索が成功する全ての行にそのリテラルが現れることを示す。

`mandatory_search_sound` は非アンカー検索の一致部分から行全体の必須リテラル包含を導く。前置フィルタの有無で最終的な行列が一致するので、重複行も保存する。 [Lean モジュール](../TinylambOpt/RegexMandatoryLiteral.lean)

## regex-bitset-nfa

リテラル・文字クラス・連接・選択・star の存在検索を対象に、生成したビット遷移列の各ステップが ε 閉包を含む参照 NFA の到達状態集合と一致することを示す。

`compiled_search_iff` は任意の AST の独立した言語意味論から数値 Thompson NFA、ε 閉包、ワード実行、非アンカー検索までを結ぶ。構造上のノードに枝ごとのタグを持たせ、表にするときだけ連番にする。文字クラス・入れ子の選択・nullable な star を含む。状態数がワード幅以内であることが適用条件。 [端から端の証明](../TinylambOpt/ThompsonSearch.lean) [Lean モジュール](../TinylambOpt/RegexBitsetNfa.lean)

## regex-capture-priority

順序付き選択、貪欲な任意接頭辞、区切り付きキャプチャの制限文法で、生成した探索コードが参照器と同じ最初のマッチおよびキャプチャ範囲を返す条件を求める。

`searchSpan_iff` は開始・終了位置とキャプチャを返し、それより前の位置では参照探索が失敗したことも証明する。対象は順序付き選択と区切り付きキャプチャ等の制限文法。外部 API の構文・フラグの振り分けは別の研究課題。 [Lean モジュール](../TinylambOpt/RegexCapturePriority.lean)

## dfa-block-summary

入力断片を「開始 DFA 状態から終了状態と途中受理フラグへの写像」に要約し、順番を保って合成すれば逐次検索と一致することを証明する。

`chunked_rows_reset` は同一の初期状態から各行を独立に開始し、行頭・行末の遷移を各一回適用する。空行を含む任意の分割で、逐次 DFA と全行の受理フラグが一致する。境界遷移自体の構築は入力側にある。 [Lean モジュール](../TinylambOpt/DfaBlockSummary.lean)

## 実装への適用境界

- LIMIT の pull 経路は `NextBatch` の要求件数を子へ渡す。Pipeline の `LimitOp::Process` は評価済みの DataChunk を受け取る。既に子で発生したエラーの扱いを確認する必要がある。 [pull 経路](../../executor/limit.cpp)・[pipeline 経路](../../executor/detail/pipeline/pipeline_ops.cpp)・[需要伝播の Issue](research/pull-demand-fusion.md)
- バイト LIKE の幅とエスケープは実装の分岐を照合した。正規表現の AST 入力とキャプチャ文法のディスパッチ、LLVM IR のフラグとメモリアクセスには別の実装対応が要る。 [UTF-8 幅](../../common/string_util.hpp)・[LIKE 実装](../../common/literal_like_pattern.hpp)・[IR 翻訳検証の Issue](research/ir-translation-validation.md)
- snapshot 定理は更新履歴・墓標・pin の明示したモデルを扱う。実ストレージの再利用、ガベージコレクション、並行 compaction の履歴から前提を導く課題を登録した。 [版保持の Issue](research/mvcc-compaction-pin.md)
