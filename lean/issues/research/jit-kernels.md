# JIT の高速化を検討する281件の命題

30個のモデルに、演算名と保証の内容で命名した281件の `….Goal : Prop` を追加した。各行のモデル名から、具体的なアルゴリズム、適用条件、比較する結果を読める。状態の再構築やコードの回収は将来の JIT 設計を対象とする。Tinylamb の現行の呼び出し規約や、実機で達成した性能を表すものではない。[Lean 登録モジュール](../../TinylambOpt/ResearchGoals/JitKernels.lean)・[現行の JIT インターフェース](../../../expression/jit.hpp)

## 31件を実際に小入力で試すとどうなるか

ワード内検索、逆数乗算、浮動小数点集約の3モデルを選び、その中の全命題を Lean で評価した。検査プログラムが登録済みの命題に引数を代入し、前提を含む式を判定する。反例が出た命題は15件、探索範囲で反例が出なかった命題は16件だった。入力とモデルを選んだ検査なので、この比率から全281件の反証率は推定できない。[再現用の Lean プログラム](fixtures/jit_kernel_census.lean)

| 対象 | 探索した境界 | 反例あり | 範囲内で反例なし |
|---|---|---:|---:|
| ワード内の一致検出 | 下位3バイトを8種から全列挙した512ワード。前後のレーン、検索バイト、マスクの切れ目も変更 | 5 | 5 |
| 逆数乗算による除算 | 被除数・上限0〜32、除数0〜12、精度0〜7。狭い演算幅、短い集約列についても検査 | 4 | 6 |
| 浮動小数点集約 | 正負のゼロ、非正規化数、丸め境界、有限最大値、無限大、NaN を含む17ビットパターンと整数の行数 | 6 | 5 |

- **同じワード命令でも、文字を探す用途と全件列挙する用途で結果が変わる。** 下位バイトが順に `00 00 01` のワードで、減算の借りが3バイト目に伝わる。ゼロバイトの位置を全列挙すると余分な一致が出る。最初の一致だけを取る版では、今回の探索で不一致はなかった。[BorrowMasks](../../TinylambOpt/ResearchGoals/JitKernels/BorrowMasks.lean)
- **除算の補正を消せるかどうかは上端だけでは決まらない。** 除数3、シフト精度1では生成する式が `x / 2` になる。上限の3では正しい商1を返すが、範囲内の2でも1を返す。剰余類ごとの最後の代表値を検査する版も登録した。[ReciprocalWindows](../../TinylambOpt/ResearchGoals/JitKernels/ReciprocalWindows.lean)
- **正の有限値だけでも並列 SUM の演算順は観測できる。** binary64 の `1, 1, 2^53` で、先に二つの1を足すと結果は `2^53 + 2`、後ろから足すと `2^53`。平均の除算についても、整数の行数3と合計10で、除算と逆数乗算の結果が1 ULP 違う。結果ビットまで一致させる候補を調べる場合の境界になる。[FloatingReduction](../../TinylambOpt/ResearchGoals/JitKernels/FloatingReduction.lean)

再現は `lean/` で `lake build` の後に `lake env lean --run issues/research/fixtures/jit_kernel_census.lean`。出力の `witness` は最初の反例入力、`clear` は検査した有限範囲での一致を表す。個別の結果は定理として登録していない。

## ハッシュ、SIMD、メモリアクセス（55件）

| モデル | 件数 | 狙う高速化と境界 |
|---|---:|---|
| [ControlByteProbe](../../TinylambOpt/ResearchGoals/JitKernels/ControlByteProbe.lean) | 10 | ハッシュ表の制御バイトをまとめて読み、最初の空きより前の候補だけで実キーを調べる。削除印、折り返し、同じタグの別キーを含む。 |
| [ConflictScatter](../../TinylambOpt/ResearchGoals/JitKernels/ConflictScatter.lean) | 10 | 衝突するレーンをまとめてカウンタを一度だけ更新する。和がゼロでもグループが存在する場合や、途中だけオーバーフローする場合を扱う。 |
| [RadixCursors](../../TinylambOpt/ResearchGoals/JitKernels/RadixCursors.lean) | 10 | バケット内順位から書き込み先を生成し、カウンタを狭める。NULL レーンとベクトルをまたぐ繰り上がりが、後続バケットの位置を変える。 |
| [SelectionNetworks](../../TinylambOpt/ResearchGoals/JitKernels/SelectionNetworks.lean) | 10 | 二段 gather を融合し、同一タイルへの参照をまとめる。入力順、重複する行番号、無効番号の除外をいつ復元するかを問う。 |
| [DivergentWaves](../../TinylambOpt/ResearchGoals/JitKernels/DivergentWaves.lean) | 8 | 長い探索レーンだけが残る SIMD ループで、処理長順の整列や空いたレーンへの再投入が何反復を節約できるか。 |
| [FusionTraffic](../../TinylambOpt/ResearchGoals/JitKernels/FusionTraffic.lean) | 7 | フィルタと射影を融合する際の密なロードと疎な gather の分岐点を、選択行の位置・共有キャッシュライン・発行費から求める。 |

## 復号を省くカーネル、文字列と時刻（74件）

| モデル | 件数 | 狙う高速化と境界 |
|---|---:|---|
| [PackedFieldLoads](../../TinylambOpt/ResearchGoals/JitKernels/PackedFieldLoads.lean) | 8 | ビット詰めした2値を一度に抜く。開始ビットの位相、最終レーンでの実読み取り幅、フレーム間のパディングを区別する。 |
| [PlanePredicates](../../TinylambOpt/ResearchGoals/JitKernels/PlanePredicates.lean) | 9 | ビット面上で比較・SUM を済ませる。列ごとのビット人口だけで積和まで復元できるか、閾値が表現域外ならどうなるかも問う。 |
| [DeltaLaneScan](../../TinylambOpt/ResearchGoals/JitKernels/DeltaLaneScan.lean) | 10 | 差分列の SUM や疎な gather をデルタの重み付き和にする。行を先に捨てた場合の carry と、一時的な範囲逸脱を扱う。 |
| [BorrowMasks](../../TinylambOpt/ResearchGoals/JitKernels/BorrowMasks.lean) | 10 | 8バイト並列比較の結果を先頭一致、逆方向検索、rank、2バイトリテラル照合に使える条件。 |
| [SampledEquality](../../TinylambOpt/ResearchGoals/JitKernels/SampledEquality.lean) | 9 | 先頭・中央・末尾の4バイト窓が覆う範囲では実キー比較を省き、未観測部分だけを後で読む。生の窓の一致とハッシュ値の一致は区別している。 |
| [LiteralSkip](../../TinylambOpt/ResearchGoals/JitKernels/LiteralSkip.lean) | 10 | 稀なアンカーバイトから候補位置を作り、複数開始点を飛ばす。自己重なりするパターン、チャンク境界、一致し得る位置の偏りが境界になる。 |
| [DecimalChunks](../../TinylambOpt/ResearchGoals/JitKernels/DecimalChunks.lean) | 10 | 数字群を一括で積和にし、範囲検査をまとめる。先頭ゼロ、負の最小値、折り返し後だけ範囲内に見える値を扱う。 |
| [CivilTextGathers](../../TinylambOpt/ResearchGoals/JitKernels/CivilTextGathers.lean) | 8 | 正準時刻文字列から時・分を固定オフセットで直接ロードする。必要な2バイトと実ロード4バイト、UTC 日境界と分単位の時差を扱う。 |

## 数値カーネルのガードと強度削減（48件）

| モデル | 件数 | 狙う高速化と境界 |
|---|---:|---|
| [ExcursionGuards](../../TinylambOpt/ResearchGoals/JitKernels/ExcursionGuards.lean) | 10 | チャンク内の部分和の上下端を使って、チェック付き加算ループから各行のチェックを取り除く。逆走査や最初の失敗行も対象。 |
| [FloatingReduction](../../TinylambOpt/ResearchGoals/JitKernels/FloatingReduction.lean) | 11 | 水平加算、NULL のゼロ埋め、AVG の逆数乗算、MIN の同値分岐で binary64 の観測結果を比較する。 |
| [ReciprocalWindows](../../TinylambOpt/ResearchGoals/JitKernels/ReciprocalWindows.lean) | 10 | 除数と入力範囲に固有の乗算・シフト命令列を生成する。商の補正を除去できる範囲と、剰余の偏りを使った集約を調べる。 |
| [InductionLowering](../../TinylambOpt/ResearchGoals/JitKernels/InductionLowering.lean) | 9 | 二次式を二段の加算に落とす。飛ばした行や SIMD ストライドぶんの進行量、最後の出力後にだけ起きるオーバーフローを扱う。 |
| [GuardCertificates](../../TinylambOpt/ResearchGoals/JitKernels/GuardCertificates.lean) | 8 | 区間と合同類を合わせ、実際に出現できる値だけでゼロ除算・オーバーフローのガードを判定する。 |

## SQL の観測結果、コード生成、途中再開（74件）

| モデル | 件数 | 狙う高速化と境界 |
|---|---:|---|
| [MaskSpecialization](../../TinylambOpt/ResearchGoals/JitKernels/MaskSpecialization.lean) | 10 | 実際に読まれた引数だけにガードを置く。短絡分岐、NULL、エラーの優先順位は既存の DemandSSA 評価器と比較する。 |
| [InListLowering](../../TinylambOpt/ResearchGoals/JitKernels/InListLowering.lean) | 9 | IN をホットな値域のビットセットと残余リストに分割する。WHERE と NOT IN で NULL の除去条件が変わる。 |
| [DispatchTables](../../TinylambOpt/ResearchGoals/JitKernels/DispatchTables.lean) | 9 | CASE をジャンプ表に変換し、高頻度の枝を前に出す。区間の重なり、最初に一致した枝、NULL を返す枝を保持する。 |
| [NullWordFusion](../../TinylambOpt/ResearchGoals/JitKernels/NullWordFusion.lean) | 10 | NULL ビットマップを展開せず CASE・COALESCE・比較を実行する。末尾パディング、選択レーン、ワード境界を調べる。 |
| [BailoutPackets](../../TinylambOpt/ResearchGoals/JitKernels/BailoutPackets.lean) | 9 | SIMD で失敗だけ検出して該当区間を再実行する。選択後の順序、元の行番号、失敗前に書いた出力を照合する。 |
| [BytecodeStackMaps](../../TinylambOpt/ResearchGoals/JitKernels/BytecodeStackMaps.lean) | 9 | 途中再開に必要なレジスタだけを保存し、重複する論理スロットは式から復元する。再計算そのものが失敗する場合も含む。 |
| [ParallelMoves](../../TinylambOpt/ResearchGoals/JitKernels/ParallelMoves.lean) | 8 | PHI や実行経路の切り替えに伴う同時コピーを機械命令列に落とす。循環、上書き済みのソース、例外経路で生きている一時レジスタを扱う。 |
| [PredicatedStores](../../TinylambOpt/ResearchGoals/JitKernels/PredicatedStores.lean) | 10 | 部分ストアの融合、ロード転送、不要ストアの削除が許される条件を、別名と観測されるバイト単位で問う。 |

## コンパイル費、コード選択、生成コードの寿命（30件）

| モデル | 件数 | 狙う高速化と境界 |
|---|---:|---|
| [TierInvestment](../../TinylambOpt/ResearchGoals/JitKernels/TierInvestment.lean) | 10 | 成功バッチの行数と失敗時の再実行費を使い、どこからコンパイル費を回収できるかを計算する。 |
| [VersionDispatch](../../TinylambOpt/ResearchGoals/JitKernels/VersionDispatch.lean) | 10 | 既知のビット条件で特殊化ガードをまとめる。優先順位の異なる重複バージョンや前回ヒットの再利用を扱う。 |
| [CodeReachability](../../TinylambOpt/ResearchGoals/JitKernels/CodeReachability.lean) | 10 | 将来コードを回収する場合に、実行中フレーム、停止中の継続、直接呼び出しからどの世代まで保持すべきか。 |
