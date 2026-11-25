# SQL エンジンの高速化仮説 500 件

既存の結合・集約・物理実行の証明から先へ進むため、100 のクエリ形状・列形式・アルゴリズムに対し、それぞれ 5 件の提出型を登録した。下の索引から、各宣言の参照計算、置き換える計算、入力条件を読める。すべて `def …Goal : Prop` で、各提出型に独立した名前がある。複数の形状に共通する分数辺被覆や合同式の議論は、将来の証明で補題を共有できる。

## どの差を縮めたいか

出発点は、保存されている tinylamb と DuckDB の比較。例えば SF1 の H2O H5 は 162.6 ms / 7.7 ms、LDBC 派生 L1 は 81.9 / 16.0 ms、SF10 の TPC-DS Q72 は 293.45 / 87.35 ms だった。以下のモデルには、それらのクエリに現れる演算と、それを自己結合・更新・複数消費者へ広げた研究用の変形がある。速度向上の実測値は今後の検証項目となる。[比較値と実行条件](duckdb-gaps.md)・[派生ベンチマークの SQL とデータ形状](analytics-workloads/README.md)・[前回の命題](frontiers.md)

| 分野 | 件数 | 対象と削減したい処理 |
| --- | ---: | --- |
| JoinPlans | 100 | JOB、CEB、LDBC、TPC-H の橋表・自己結合。境界変数での集約、変更辺だけの修復、分数辺被覆による出力上界、頂点衝突の包除、無向クリークの 24 重の対称性。 |
| RangePlans | 100 | TPC-DS の在庫不等号や CostBench の時系列から派生した 20 種の非等値条件。符号付き矩形、一次元端点索引、ページ単位の採否、anti-join、順位の二分探索。 |
| OrderedMeasures | 100 | 価格、スコア、セッションの順序付き分析。全行対・全フレームの列挙を、端点・モーメント・接頭辞衝突で置き換える。 |
| EncodedPages | 100 | 20 種の列配置。値と物理位置を別々の等差列で表し、復号前に等値結合、周期削除、ordinal での積和を計算する。 |
| PlanSearch | 20 | CEB の述語順序、PBench 派生の候補選択、安定マージ順序、連続 morsel の分割、spill 予約量の最適化。 |
| SnapshotMaintenance | 20 | Iceberg 相当の削除、二時間軸の履歴、CostBench の refresh。過去版の棄却、ゼロ境界だけの DISTINCT 更新、任意時点の集約復元。 |
| TextExecution | 20 | ClickBench の URL・Title と Public BI の辞書列。文法圧縮上の検索、編集距離候補の棄却、文書 ID の wavelet tree、辞書の隣接 LCP。 |
| ExecutorResources | 20 | 読取りと評価の重ね合わせ、宛先別送信、疎な gather、SIMD 圧縮。経過時間のモデル値、パケット数、読取りページ、デコード量を比較する。 |
| SynopsisBounds | 20 | JOB・CEB・H2O の基数と容量見積り。非復元抽出の有限確率則、周辺分布からの輸送費下界、頻出キーを外した残余ファンアウト、外れ値の件数上界。 |

## H5 と C1 で上界を照合した結果

`HeavyBounds.TailFanout` は、頻出キー集合を H とすると、H の結合件数を正確に数え、残りを「左行数 × 右の最大頻度」と「右行数 × 左の最大頻度」の小さい方で抑える。`MonotoneRefine` は H にキーを追加するたびこの上界が下がるかを問う。

| クエリ | 実際の結合行数 | 頻出キー 0 個の上界 | 16 個 | 64 個 | 256 個 |
| --- | ---: | ---: | ---: | ---: | ---: |
| H2O H5 | 23,995,178 | 30,800,000 | 28,294,033 | 28,144,720 | 27,264,916 |
| STATS-CEB C1 | 291,943 | 8,755,005 | 1,944,929 | 1,032,070 | 611,059 |

SF1、seed 1729。H5 は x と y の id4 の頻度を使用した。C1 は `score > 5 AND view_count < 10000` の posts に限定し、comments と `vote_type = 1` の votes の post_id 頻度を使用した。2 本とも DuckDB の元の結合で件数を照合した。H の選択には両側の正確な頻度積の降順、同点ではキーの昇順を使った。C1 は 16 キーで上界が約 4.5 分の 1 まで縮む。H5 での縮小は小さい。この計算には既知の頻度を使っており、実行時にそれを集める費用は別途かかる。[SQL と生成器](../../../benchmark/comparison/analytics_bench.py)・[上界の提出型](../../TinylambOpt/ResearchGoals/Atlas/SynopsisBounds.lean)

再現用の入力は次のコマンドで空の出力先に生成できる。

```sh
python3 benchmark/comparison/analytics_bench.py --generate-only --families h2o stats_ceb \
  --scale 1 --seed 1729 --output /tmp/atlas-h5-c1
```

## 具体的な反例を残した 10 件

各モジュール末尾の `#guard` で具体例を評価する。一般命題とその否定の証明は未登録のままにした。

| 分野・宣言 | 小さな反例 |
| --- | --- |
| StrictOverlap.PageVerdict | probe が [0,2)、build が重み 7 の空期間 [1,1) なら、端点比較だけのページ判定は 7、正の長さの交差を求める走査は 0。他の同形の提出型には build 側の非空条件を入れた。 |
| PeakCount.DuplicateLoss | [0,1,0] の厳密ピークは 1 個。各行を二重化すると山頂が平らになり 0 個。 |
| PredicateOrder.MarginalTie | 全二行で同数を通す互いに排他的な二述語でも、先行フィルタが 0 だけを残すと評価費用は順序により 4 と 3。 |
| RiskEnvelope.MeanReserve | 負荷 0 が 3 回、8 が 1 回。平均 2 を予約した場合の絶対損失は 12、予約 0 の損失は 8。 |
| Versions.DropTombstones | 値 7 の次に tombstone を公開したキーを読むと、tombstone を索引から落とす方式は古い 7 を再公開する。 |
| BatchWatermarks.FinalSummary | (時刻 0, +4), (時刻 1, −4) と、両時刻で寄与 0 のログは終端も総和も同じだが、時刻 0 で 4 と 0 に分かれる。 |
| Grammar.InteriorCache | 語 [1] と [0] の内部では [1,2] はいずれも 0 回。後ろに [2] を足すと 1 回と 0 回。 |
| Exchanges.GlobalPacking | 容量 4 で別宛先に 1 バイトずつ送るには 2 パケット必要。総量だけで切り上げると 1。 |
| Couplings.AlignedMarginals | 行 (0,1), (1,0) は両列の周辺分布が一致するが、行ごとの絶対差の和は 2。 |
| PageMoments.MomentHistogram | [−2,1,1] と [−1,−1,2] は件数・和・二乗和が一致し、2 以上の件数は 0 と 1。 |

## 提出型の読み方

完全名は `TinylambOpt.ResearchGoals.Atlas.<分野>.<モデル>.<宣言>.Goal`。下表のモデル名を開くと Lean の定義と個別の適用条件を読める。`cd lean && lake build` でトップレベルから型検査できる。

- 結合は物理行の多重度を持つ。差分には符号付き整数を使う。分数辺被覆で抑えるのは非零キー割当て数で、任意の重み付き SUM は別の観測量である。変数消去の関数は各境界で実体化可能な部分表の仕様を与える。
- 範囲索引のビルド側は明示した有限領域内の整数座標。端点は半開区間で、穴は負係数の矩形として表す。順位は同点を含むスコアの袋を比較する。
- 順序付き集約は物理タイブレーク済みの非 NULL 整数列上で定義する。圧縮ページの欠損行は有効件数と SUM から除く。空集合の SQL SUM を返す側は有効件数 0 を NULL に戻せる。
- プラン費用は各モデルの明記した仮定に依存する。例として述語順序の比による整列は独立な通過率を使い、二段パイプラインは独立した二資源と無制限の中間キューを使う。非復元抽出は値が同じ物理行も別の標本として数える。

型検査に加え、小入力で参照計算と照合した。圧縮列は 20 形式 × 108 設定（空ページ、負・ゼロ・正の増分、範囲外の切替を含む）で実体化結果・順位・等値結合・位置削除・積和を比較した。範囲条件は 20 種 × 25 プローブで走査、区間和、順位を比較し、結合 20 形状は小さい符号付き表 3 組で変数消去・差分修復・衝突補正を照合した。これらの有限例の一致と、500 件の一般命題の証明は別々に検査できる。

### JoinPlans（100 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [MovieCredits](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L96) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`TriangleNorm`、`CollisionCorrection` |
| [KeywordCompany](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L128) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`TerminalKey`、`CollisionCorrection` |
| [CastOverlap](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L161) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`GramSquare`、`CollisionCorrection` |
| [DoubleBilling](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L193) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`SquaredDelta`、`CollisionCorrection` |
| [PostDiamond](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L227) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`SquarePathCounts`、`CollisionCorrection` |
| [VoteBadge](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L259) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`IndependentArms`、`CollisionCorrection` |
| [ReplyCycle](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L293) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`UndirectedTrace`、`CollisionCorrection` |
| [FriendBridge](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L326) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`BidirectionalMeet`、`CollisionCorrection` |
| [CoLikes](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L359) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`DistinctMessages`、`CollisionCorrection` |
| [ForumBowtie](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L394) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`ArticulationSquare`、`CollisionCorrection` |
| [CountryReturn](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L426) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`DeltaForecast`、`CollisionCorrection` |
| [OrderSupplier](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L460) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`SharedDestination`、`CollisionCorrection` |
| [ReturnedItem](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L493) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`AnchoredChord`、`CollisionCorrection` |
| [ChannelBridge](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L526) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`TripleOverlap`、`CollisionCorrection` |
| [PromoInventory](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L559) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`DeltaForecast`、`CollisionCorrection` |
| [TwoHopAntiWitness](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L593) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`IndependentWitness`、`CollisionCorrection` |
| [HubWithChords](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L627) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`ChordComponents`、`CollisionCorrection` |
| [SupplierClique](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L661) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`OrbitRepresentative`、`CollisionCorrection` |
| [IntersectingCycles](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L695) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`TwoSeparators`、`CollisionCorrection` |
| [ParallelLineage](../../TinylambOpt/ResearchGoals/Atlas/JoinPlans.lean#L729) | `BoundaryMessages`、`AnchoredRepair`、`FractionalCap`、`DeltaForecast`、`CollisionCorrection` |

### RangePlans（100 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [ContainsTick](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L71) | `PrefixIndex`、`PageVerdict`、`EndpointSweep`、`UnionAnti`、`RankSeek` |
| [StrictOverlap](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L110) | `PrefixIndex`、`PageVerdict`、`StartMinusEnd`、`UnionAnti`、`RankSeek` |
| [NestedVersion](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L149) | `PrefixIndex`、`PageVerdict`、`OuterCertificate`、`UnionAnti`、`RankSeek` |
| [StaggeredHandoff](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L187) | `PrefixIndex`、`PageVerdict`、`ProbeTransposition`、`UnionAnti`、`RankSeek` |
| [EndpointContact](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L227) | `PrefixIndex`、`PageVerdict`、`EndpointHash`、`UnionAnti`、`RankSeek` |
| [PriceCorridor](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L269) | `PrefixIndex`、`PageVerdict`、`SlidingStrip`、`UnionAnti`、`RankSeek` |
| [ParetoFront](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L308) | `PrefixIndex`、`PageVerdict`、`SkylineWitness`、`UnionAnti`、`RankSeek` |
| [ReversedRank](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L348) | `PrefixIndex`、`PageVerdict`、`InversionPairs`、`UnionAnti`、`RankSeek` |
| [OutsideBand](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L389) | `PrefixIndex`、`PageVerdict`、`ComplementSum`、`UnionAnti`、`RankSeek` |
| [RingNeighborhood](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L429) | `PrefixIndex`、`PageVerdict`、`PerimeterHistogram`、`UnionAnti`、`RankSeek` |
| [ExactlyOneKey](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L469) | `PrefixIndex`、`PageVerdict`、`SubtractiveGrouping`、`UnionAnti`、`RankSeek` |
| [PriorTradingDay](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L511) | `PrefixIndex`、`PageVerdict`、`DailyState`、`UnionAnti`、`RankSeek` |
| [LexSuccessor](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L551) | `PrefixIndex`、`PageVerdict`、`ReverseCursor`、`UnionAnti`、`RankSeek` |
| [EitherEndpointInside](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L591) | `PrefixIndex`、`PageVerdict`、`SplitBoundaryDebt`、`UnionAnti`、`RankSeek` |
| [CrossChannel](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L632) | `PrefixIndex`、`PageVerdict`、`ProbeTransposition`、`UnionAnti`、`RankSeek` |
| [VarianceBudget](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L673) | `PrefixIndex`、`PageVerdict`、`ProbeTransposition`、`UnionAnti`、`RankSeek` |
| [AlignedWeek](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L715) | `PrefixIndex`、`PageVerdict`、`BucketEquality`、`UnionAnti`、`RankSeek` |
| [GapSession](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L755) | `PrefixIndex`、`PageVerdict`、`ProbeTransposition`、`UnionAnti`、`RankSeek` |
| [OverdueOrLost](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L795) | `PrefixIndex`、`PageVerdict`、`ProbeTransposition`、`UnionAnti`、`RankSeek` |
| [AdjacentTiles](../../TinylambOpt/ResearchGoals/Atlas/RangePlans.lean#L836) | `PrefixIndex`、`PageVerdict`、`SparseNeighbors`、`UnionAnti`、`RankSeek` |

### OrderedMeasures（100 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [TotalVariation](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L35) | `Seam`、`Rescale`、`Monotone`、`DropDuplicates`、`EndpointFence` |
| [QuadraticVariation](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L66) | `SeamEnergy`、`Affine`、`EndpointEnergy`、`LinearRamp`、`LocalEdit` |
| [RisePairs](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L97) | `HistogramBridge`、`ReverseDebt`、`RankCoding`、`BlockRepeat`、`MergeSorted` |
| [TiePairs](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L130) | `Multiplicity`、`DeleteBatch`、`DisjointKeys`、`CollisionPenalty`、`FanoutBound` |
| [Drawdown](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L163) | `CrossExtreme`、`Bidirectional`、`QuoteFlip`、`DeleteMonotone`、`DuplicatePeriods` |
| [BestFrame](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L196) | `CrossFrame`、`RepeatedGain`、`NegativePage`、`PrefixDrawdown`、`DeletionFence` |
| [RunTransitions](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L229) | `PageBoundary`、`CoarseDictionary`、`MaskedSubsequence`、`PeriodicBoundary`、`SortedDictionary` |
| [PlateauFrames](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L263) | `BoundaryRuns`、`Uniform`、`InjectiveCodes`、`ExpandRuns`、`TieFence` |
| [PrefixExposure](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L297) | `Multiplicities`、`InheritedBalance`、`ReverseComplement`、`Deposit`、`BatchRepeat` |
| [PrefixEnergy](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L329) | `OffsetQuadratic`、`ConstantPage`、`UnitScaling`、`ReverseEnergy`、`ThresholdBound` |
| [LagProduct](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L359) | `BoundaryProduct`、`CenterShift`、`EnergyIdentity`、`RunStretch`、`RemoveInterior` |
| [FrameTotals](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L390) | `RowIncidence`、`PrefixSuffix`、`UniformShift`、`LocalCorrection`、`EvenSplitter` |
| [TripleRise](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L422) | `MiddlePivot`、`Concatenate`、`DisjointRanges`、`StrictPage`、`StableExpand` |
| [AlternatingBalance](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L454) | `ParityCarry`、`BackwardPhase`、`PeriodicParity`、`DuplicateCancellation`、`ShiftResidue` |
| [PositiveJumps](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L486) | `BoundaryJump`、`ReverseBalance`、`SharedMagnitude`、`NonincreasingSkip`、`FilterBudget` |
| [CenteredScatter](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L518) | `MomentCollapse`、`PairMerge`、`Translation`、`ReplicatedGroup`、`OutlierCap` |
| [GiniDistance](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L549) | `SortedWeights`、`SeparatedBlocks`、`DictionaryPairs`、`Lipschitz`、`VarianceFence` |
| [ZeroFrames](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L579) | `PrefixCollision`、`BoundaryBalances`、`Nonnegative`、`Uniform`、`EarliestPrune` |
| [EdgeCurvature](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L611) | `AffineTrend`、`Backward`、`EndpointRepair`、`FlatAcceleration`、`TrendCertificate` |
| [PeakCount](../../TinylambOpt/ResearchGoals/Atlas/OrderedMeasures.lean#L643) | `BoundaryPeaks`、`OrderDictionary`、`AdjacentCap`、`DuplicateLoss`、`BoundedRepair` |

### EncodedPages（100 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [LinearOrdinal](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L97) | `SliceState`、`InverseLookup`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [SaturatingCounter](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L140) | `SliceState`、`PlateauFrequency`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [SegmentedSlope](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L180) | `SliceState`、`TurnExtrema`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [MirroredTail](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L221) | `SliceState`、`OverlapDistinct`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [BucketRuns](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L261) | `SliceState`、`PartialBucket`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [WrappingSequence](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L302) | `SliceState`、`QuotientHistogram`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [DictionaryCycle](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L343) | `SliceState`、`DuplicateCodes`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [DictionaryDrift](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L385) | `SliceState`、`PredicateRank`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [AlternatingSign](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L427) | `SliceState`、`CancelPairs`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [TriangleWave](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L468) | `SliceState`、`PeakPositions`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [LaneMajor](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L509) | `SliceState`、`FrequencySeparation`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [Boustrophedon](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L550) | `SliceState`、`FullTiles`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [RaggedStripe](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L590) | `SliceState`、`NullCount`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [PeriodicPatch](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L631) | `SliceState`、`PatchBudget`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [SparseSample](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L672) | `SliceState`、`PredicateRank`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [DeletedSpan](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L713) | `SliceState`、`SeekSurvivor`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [RotatedPage](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L755) | `SliceState`、`PermutationAggregate`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [GrayTile](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L796) | `SliceState`、`PowerOfTwo`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [RestartedDelta](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L837) | `SliceState`、`PredicateRank`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |
| [PagedTombstones](../../TinylambOpt/ResearchGoals/Atlas/EncodedPages.lean#L878) | `SliceState`、`SkipEpochs`、`ResidueJoin`、`DeleteProgression`、`AlignedProduct` |

### PlanSearch（20 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [PredicateOrder](../../TinylambOpt/ResearchGoals/Atlas/PlanSearch.lean#L7) | `RatioSort`、`ConditionalSwap`、`RepeatedGuard`、`DeferredPass`、`MarginalTie` |
| [AlphabeticMerge](../../TinylambOpt/ResearchGoals/Atlas/PlanSearch.lean#L47) | `OptimalTree`、`MonotoneCut`、`Quadrangle`、`ReweightBound`、`ScaleReuse` |
| [ParallelCuts](../../TinylambOpt/ResearchGoals/Atlas/PlanSearch.lean#L84) | `Feasible`、`CapacitySearch`、`ReverseScan`、`BoundarySlack`、`LocalCorrection` |
| [RiskEnvelope](../../TinylambOpt/ResearchGoals/Atlas/PlanSearch.lean#L116) | `QuantileReserve`、`ReservationSlope`、`HistogramFold`、`RareWorkload`、`MeanReserve` |

### SnapshotMaintenance（20 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [Versions](../../TinylambOpt/ResearchGoals/Atlas/SnapshotMaintenance.lean#L5) | `DominancePrune`、`RunTournament`、`SlabReuse`、`LocalPublish`、`DropTombstones` |
| [IntervalDeletes](../../TinylambOpt/ResearchGoals/Atlas/SnapshotMaintenance.lean#L46) | `SignedEndpoints`、`FooterRuns`、`OverlapDebt`、`LiveSelect`、`RebaseMask` |
| [ThresholdViews](../../TinylambOpt/ResearchGoals/Atlas/SnapshotMaintenance.lean#L84) | `ZeroCrossing`、`Coalesce`、`SemijoinRepair`、`MarginCertificate`、`ReplayLedger` |
| [BatchWatermarks](../../TinylambOpt/ResearchGoals/Atlas/SnapshotMaintenance.lean#L118) | `PartialOpen`、`CoarseTime`、`HistoricalBand`、`EquivalentHistory`、`FinalSummary` |

### TextExecution（20 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [Grammar](../../TinylambOpt/ResearchGoals/Atlas/TextExecution.lean#L9) | `BoundaryHits`、`LongPower`、`ShortPower`、`SliceDecode`、`InteriorCache` |
| [EditFilter](../../TinylambOpt/ResearchGoals/Atlas/TextExecution.lean#L52) | `GramLowerBound`、`PigeonholeAnchor`、`PositionalAnchor`、`ClassLength`、`HistogramFence` |
| [Wavelet](../../TinylambOpt/ResearchGoals/Atlas/TextExecution.lean#L83) | `RangeMap`、`Quantile`、`SameBranch`、`BetweenCodes`、`DictionaryGrowth` |
| [Lcp](../../TinylambOpt/ResearchGoals/Atlas/TextExecution.lean#L121) | `RangeMinimum`、`DictionarySize`、`PagePrefix`、`CompareSkip`、`ShardBridge` |

### ExecutorResources（20 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [Pipeline](../../TinylambOpt/ResearchGoals/Atlas/ExecutorResources.lean#L4) | `TwoStageOrder`、`CriticalCut`、`BatchSummary`、`SteadyState`、`SplitOverlap` |
| [Exchanges](../../TinylambOpt/ResearchGoals/Atlas/ExecutorResources.lean#L36) | `PacketLowerBound`、`FlushDebt`、`FrameScale`、`ReceiverCoalesce`、`GlobalPacking` |
| [Gather](../../TinylambOpt/ResearchGoals/Atlas/ExecutorResources.lean#L64) | `OptimalRead`、`CostEnvelope`、`CoarserPages`、`ExtentCount`、`ScatterBack` |
| [LaneCompaction](../../TinylambOpt/ResearchGoals/Atlas/ExecutorResources.lean#L103) | `MaskCompose`、`ChunkOffsets`、`ExpandBack`、`DecodeSavings`、`EarlyDemand` |

### SynopsisBounds（20 件）

| モデル | 個別の宣言名 |
| --- | --- |
| [Samples](../../TinylambOpt/ResearchGoals/Atlas/SynopsisBounds.lean#L5) | `EmptySample`、`SampleMean`、`SampleEnergy`、`SelectivityLaw`、`ShardSampling` |
| [Couplings](../../TinylambOpt/ResearchGoals/Atlas/SynopsisBounds.lean#L43) | `Rearrangement`、`TransportLower`、`PrefixTransport`、`FarRows`、`AlignedMarginals` |
| [HeavyBounds](../../TinylambOpt/ResearchGoals/Atlas/SynopsisBounds.lean#L70) | `TailFanout`、`MonotoneRefine`、`AliasError`、`TwoHashes`、`JointKey` |
| [PageMoments](../../TinylambOpt/ResearchGoals/Atlas/SynopsisBounds.lean#L102) | `UnroundedTail`、`ShiftOrigin`、`TrimError`、`AllInside`、`MomentHistogram` |
