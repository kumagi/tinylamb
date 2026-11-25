# 圧縮と SQL 実行の未証明命題 1,000 件

列の保存サイズを縮めても、必要な行にたどり着くまでの展開量や、並べ替えた行の位置を戻す索引が大きいと利得は消える。今回はこの収支と、圧縮表現から直接返せるクエリ結果を扱った。**10 分野、各 10 モデル、各モデル 10 件**を独立した `def …Goal : Prop` として公開している。各モデルの 8 件は分野共通の契約を具体的な計算に適用したもの、2 件はそのモデル固有の式や適用条件である。[Lean の入口](../../TinylambOpt/ResearchGoals/StorageExecution.lean)

## 実データで分かれた採用条件

ClickBench の同じ 65,536 行を使い、4 種類の述語から選択ビットマップを作った。64 行ごとに、立っているビット数を 1 B、同じビット数のパターンの中での辞書順順位を必要最小のバイト数で書く。密ビット列は固定 8 B。各ブロックの実バイト列を復号し、すべての行の真偽を元の述語結果と照合した。

| 述語 | 該当行数 | 密ビット列 | 個数＋順位 | 密形式より大きいブロック / 1,024 |
|---|---:|---:|---:|---:|
| `IsRefresh <> 0` | 32,010 | 8,192 B | 9,212 B | 1,021 |
| `IsMobile <> 0` | 3,205 | 8,192 B | 2,116 B | 31 |
| `SearchPhrase <> ''` | 8,732 | 8,192 B | 5,257 B | 44 |
| `RegionID % 2 = 1` | 44,124 | 8,192 B | 5,465 B | 239 |

モバイル判定で 6,076 B 節約できた一方、更新判定では 1,020 B 増えた。採用を決めるのは各ブロックの該当行数で、ファイル全体の選択率だけでは決まらない。表の対象は選択ビットマップの本体。形式は列ごとに既知、ブロック長は固定とし、別の点参照索引や形式混在用のタグは含めていない。Lean の参照仕様は受理可能なビット列を分岐ごとに数え、実測スクリプトは二項係数で同じ分岐数を求める。この対応も提出型に含めた。元の DB は読み取り専用で開いた。以下をリポジトリ直下で実行すると同じ比較ができる。[実測スクリプト](fixtures/ranked_masks.py)・[組合せ数と保存フレームの提出型](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean)

```sh
python3 lean/issues/research/fixtures/ranked_masks.py \
  build/benchmarks/clickbench-full-20261116/reference.duckdb
```

実測には DuckDB 2.0.0 の Python バインディングを使い、`rowid < 65536 ORDER BY rowid` で読み出した。SQL の UNKNOWN は選択マスク上で false とした。今回の実測値はこの標本のサイズであり、SQL の所要時間は未計測。

## ベンチマークと対応する研究対象

| 狙い | 分野（各 100 件） | 検証内容 |
|---|---|---|
| ClickBench の選択結果や削除位置を小さく持つ | [RankedMasks](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean) | 固定個数、孤立したヒット、単一区間などの制約別に順位を計算する。列の一部が既知のとき、該当する順位区間だけで読取りを再開できるか。 |
| 列ストアの幅と点参照を両立する | [BitLayouts](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean) | 行方向、ビットプレーン、8 行タイル、上位・下位の混合配置で、述語や gather が触る異なるバイトを数える。単一行の更新で隣の値を壊さない条件も扱う。 |
| TLCS の整数ページで形式と切断位置を選ぶ | [FrameSelection](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean) | 実際のエンコーダが返すバイト列に対し、raw への退避、ヘッダ、再始動位置、点参照時の展開量を含む選択を比較する。 |
| ClickBench / Public BI の文字列を部分復号する | [GrammarScans](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean) | 連結・反復で表した辞書語から状態遷移表を作る。URL のエスケープ、区切り、数字や ASCII 判定が辞書語の境界をまたぐときも扱う。 |
| H2O H5、JOB、LDBC で大きな結合結果を展開しない | [BagKernels](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean) | 整数キーの多重度を物理表現から求め、区間結合、外部結合、順序統計量、INTERSECT ALL まで評価する。上書きの重複や無向辺のループも対象。 |
| TPC-DS の範囲結合やイベントの時間窓を絞る | [PostingJoins](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean) | 右のソート済みキーを互いに重ならない区間へ変換し、二分探索で件数・合計・k 番目を返す。NULL、右の重複、削除済みの位置を区別する。 |
| TPC-H の金額式や複数列のフィルタでページを飛ばす | [DerivedZones](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean) | 金額積、丸めた比、循環する時刻、距離などについて式の上下界を導き、範囲・上位 k 件・semi-join を評価する。 |
| 列をまたぐ読取りや OOC のスピルをまとめる | [GatherPlans](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean) | 途中の不要データも読む費用とリクエストを分ける費用を比較。セクタ境界、独立復号点、辞書読込み、再展開量で最適な範囲がどう変わるか。 |
| BI の累積窓や CostBench の時系列で再走査を省く | [OrderedFooters](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean) | ページの両端や集約状態を結合し、最大下落幅、最長ゼロ列、順序つきペア件数、前行差分の符号長を再計算する。 |
| クラスタリングの圧縮利得と行 ID の費用を比べる | [ClusterOrder](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean) | Morton 順、対角線、XOR などの並べ方で、2 列の RLE 費用と物理順から論理順に戻す索引を合算。LIMIT、後からの列取得、削除を元の行に対応づける。 |

この対応は既存のクエリ形状に基づく研究仮説である。たとえば H2O H5 では結合が 23,995,178 行、LDBC 派生 L1 は 2,124,422 経路になった。前回の ClickBench の全列ファイル圧縮でも、復号単位を 1 MiB から 64 KiB にした場合には約 1.80 GB の追加を要した。今回のモデルでは、それぞれ「出力行を展開する前の多重度計算」と「読取りの粒度を変えたときの費用」を取り出した。[クエリと観測件数](analytics-workloads/README.md)・[保存サイズの比較](compression.md)

## 先に証明・反証すると実装判断につながる例

- **圧縮形式の貪欲な切替。** 先頭値と例外を持つ形式で入力を `[0] | [1] | [1]` とすると、最初の 2 要素を結合する局所費用は 12 B から 10 B に下がる。ところが全体では、その切断を捨てると最短でも 14 B、元の切断を残した `[0] | [1,1]` は 13 B になる。`ConstantExceptions.GreedyCut.Goal` の具体的な反例を `#guard` として残した。最適解と比較する `ByteFrontier` では全分割を参照仕様とし、別実装の動的計画法で各接尾辞の最小値を一度ずつ計算する。[ページ選択](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean)・[分割の動的計画法](../../TinylambOpt/ResearchGoals/StorageExecution/Boundaries.lean)
- **疎な選択結果からの読取り。** `Contiguous.PositiveGapRule.Goal` は、最適な読取り費用を隣り合う要求の間隔だけで算出できる条件を扱う。`CodecRestarts.SharedRestart.Goal` は、同じ復号開始点を持つ二つの要求をまとめても、後ろの一点を読む以上の展開が生じないケース。[読取り範囲の選択](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean)
- **展開しない結合。** `SharedPrefix.SplitSubtrees.Goal` は、上位ビットで結合する際、親区間のファンアウトを二つの子の件数で復元する。`RepeatedSlices.AppendedSuffixes.Goal` は、接尾辞を共有する列へ追記したとき、既存行との組合せで増える多重度を直接求める。[区間結合](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean)・[多重度計算](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean)
- **行並べ替えの下限。** 異なる 2 行 `[(0,0),(1,1)]` はすでに両列が整列し、値の並びを変えてもラン数を減らせない。新しい形式タグと位置表の分だけ大きくなる。`TwoColumnWin.Goal` のように全ページでの圧縮改善を主張する場合、現行形式への退避か、それらの費用を埋める前提が必要になる。[クラスタリングの比較](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean)

## 宣言の引き方

`import TinylambOpt` で全件を参照できる。完全名の形は `TinylambOpt.ResearchGoals.StorageExecution.<分野>.<モデル>.<命題>.Goal`。下の各リンクはモデルの宣言位置を指す。共通契約では名前の右辺をたどると、参照計算・比較対象・成立条件を読める。

- **BagKernels**　[MultiplicityRuns](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L54)、[IntervalCover](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L77)、[AdditiveProduct](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L100)、[DictionaryAliases](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L123)、[ExceptionOverlay](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L146)、[RotationCycle](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L173)、[EdgeDegrees](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L196)、[RepeatedSlices](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L220)、[DeletedOrdinals](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L243)、[CoGroup](../../TinylambOpt/ResearchGoals/StorageExecution/BagKernels.lean#L266)
- **BitLayouts**　[RowMajor](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L65)、[Vertical](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L85)、[SerpentineRow](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L105)、[SerpentinePlane](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L125)、[TileTranspose](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L145)、[ByteAlignedRow](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L165)、[ByteAlignedPlane](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L185)、[NetworkBitOrder](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L205)、[RotatedPlane](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L225)、[SplitNibble](../../TinylambOpt/ResearchGoals/StorageExecution/BitLayouts.lean#L245)
- **ClusterOrder**　[Lexicographic](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L67)、[AttributeFirst](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L87)、[Interleaved](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L107)、[GrayPair](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L127)、[Diagonal](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L147)、[UnorderedPair](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L167)、[RatioClass](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L187)、[PowerBucket](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L207)、[DyadicTile](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L227)、[XorClass](../../TinylambOpt/ResearchGoals/StorageExecution/ClusterOrder.lean#L247)
- **DerivedZones**　[Product](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L68)、[SaturatingLoss](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L89)、[Distance](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L110)、[FloorRatio](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L131)、[BucketSum](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L152)、[WrappedSum](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L173)、[Manhattan](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L194)、[BitwiseXor](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L216)、[CappedPair](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L237)、[QuadraticGap](../../TinylambOpt/ResearchGoals/StorageExecution/DerivedZones.lean#L258)
- **FrameSelection**　[SeparatedLengths](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L79)、[ConstantExceptions](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L100)、[DictionaryRuns](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L123)、[PatchedOctet](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L145)、[XorPredecessor](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L166)、[OrdinalResidual](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L187)、[SplitHigh](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L210)、[MinimumDelta](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L232)、[SparsePlanes](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L257)、[SharedDivisor](../../TinylambOpt/ResearchGoals/StorageExecution/FrameSelection.lean#L281)
- **GatherPlans**　[Contiguous](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L62)、[Sector](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L85)、[MirrorZones](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L105)、[CodecRestarts](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L125)、[TwoTier](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L145)、[ObjectRanges](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L165)、[StripedWorkers](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L185)、[QuadraticDecode](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L205)、[DictionaryFault](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L225)、[TieredPrefetch](../../TinylambOpt/ResearchGoals/StorageExecution/GatherPlans.lean#L245)
- **GrammarScans**　[ContainsSlash](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L91)、[EndsInDigit](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L111)、[QuotedField](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L131)、[PercentEscape](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L151)、[LineBoundary](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L178)、[AsciiOnly](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L198)、[SlashDepth](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L218)、[DecimalMod](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L238)、[EscapedTail](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L258)、[CStringEnd](../../TinylambOpt/ResearchGoals/StorageExecution/GrammarScans.lean#L278)
- **OrderedFooters**　[LongestHole](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L72)、[Drawdown](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L96)、[MonotoneBreaks](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L118)、[DeltaBytes](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L140)、[PairSubsequence](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L162)、[RollingFingerprint](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L184)、[BalanceFloor](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L206)、[BitOccupancy](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L230)、[FirstLastChange](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L252)、[IndexWeighted](../../TinylambOpt/ResearchGoals/StorageExecution/OrderedFooters.lean#L276)
- **PostingJoins**　[Following](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L71)、[Annulus](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L92)、[SameBucket](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L113)、[AdjacentBucket](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L136)、[ModuloClass](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L157)、[SharedPrefix](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L178)、[Submask](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L199)、[ReciprocalPrice](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L224)、[ScaledFloor](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L245)、[Apart](../../TinylambOpt/ResearchGoals/StorageExecution/PostingJoins.lean#L266)
- **RankedMasks**　[FixedPopcount](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L76)、[AtMostHits](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L104)、[IndependentHits](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L124)、[PairedHoles](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L144)、[PrefixOnes](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L164)、[EvenParity](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L184)、[BalancedNesting](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L204)、[RunLimit](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L224)、[SingleIsland](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L244)、[Cooldown](../../TinylambOpt/ResearchGoals/StorageExecution/RankedMasks.lean#L264)

## 検査と適用範囲

```lean
import TinylambOpt

#check TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.PopulationFrame.Goal
#check TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions.GreedyCut.Goal
#check TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Contiguous.PositiveGapRule.Goal
```

`cd lean && lake build` が公開モジュールと実行例を検査する。今回の 1,000 件はすべて未証明の提出型である。原値を扱う整数計算は条件に記した範囲での数学的整数、文字列はバイト列。NULL を許すモデルは `Option`、SQL の結合は多重度を保持する。比較の単位は各提出型に明記したバイト・行・読取り回数などで、アクセス機器の料金係数や、辞書・ソート順・補助索引を用意する前提は実装側で確かめる。
