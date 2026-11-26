# JIT コンパイルの境界条件を問う 1,000 件

同じ SQL 式をもっと安く計算するとき、どのガードを外せるか、どこでスカラー実行に戻ればよいかを問う。100 個の具体的な変換・カーネルに、それぞれ分野共通の観測項目 8 件とモデル固有の境界条件 2 件を割り当てた。全件が個別に参照できる未証明の `def …Goal : Prop`。反例が出た強い仮説も、その入力と一緒に残している。[一括 import](../../TinylambOpt/ResearchGoals/JitFrontiers.lean)

## 分野別の索引

名前は `TinylambOpt.ResearchGoals.JitFrontiers.<分野>.<モデル>.<命題>.Goal`。たとえば `PrefixKernels.SignedSum.ExactEntryGuard.Goal` は、途中で一度もオーバーフローしない初期累計値を、ブロック内の各接頭辞から正確に逆算できるかを問う。

| 分野（各 100 件） | 省きたい処理 | 成否を分ける観測 |
| --- | --- | --- |
| [CheckedRewrites](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean) | 加減算の相殺、乗算の因数分解、狭い整数への特殊化で中間検査を省く | 元の中間演算と最終演算を区別した最初のエラー、ガードの必要十分性 |
| [WordPromises](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean) | ガード下で LLVM の `nsw`、`nuw`、`exact`、`disjoint` を使う | 64 ビットで定義される領域、シフト量 63/64、選ばれた分岐の poison、freeze の値 |
| [DemandSSA](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean) | CASE、AND、OR、COALESCE の共通式や分岐を消す | 値、NULL、エラー箇所、評価したスロットの順序と回数、WHERE での採否 |
| [VectorMemory](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean) | 入力を SIMD で先読みし、重複した gather や store をまとめる | 同一ベクトル内の先行書込みと後続読取り、scatter の最終書込み、末尾の有効アドレス |
| [PrefixKernels](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean) | 集約ループをアフィン変換にまとめ、ブロック単位で検査する | 初期累計、負の傾き、NULL によるリセット、途中で最初に失敗する行 |
| [RelationalGuards](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean) | 分岐で得た `x−y` と各列の範囲から、比較やオーバーフロー検査を除く | 整数領域の真の最小・最大、折れ点、分岐の合流後も上界が有効か |
| [BailoutState](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean) | グループ表が満杯でも既存キーを処理し、失敗した行から通常経路で再開する | 最初の未反映行と集計状態、境界行の欠落・二重計上、容量増加の効果 |
| [PackedKeys](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean) | 複合キーを 64 ビット比較に変え、JOIN、DISTINCT、ORDER BY に流用する | 復元、NULL 同士の扱い、結合の多重度、符号・昇降順、通常経路との再合流 |
| [PredicateBallots](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean) | TRUE・NULL・エラーのビットマスクで 64 行をまとめ、不要な右辺を読まない | 右辺を評価する最小レーン集合、部分ベクトル、LIMIT 1 が先に見る成功行とエラー行 |
| [SpecializationCache](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean) | 定数、辞書、NULL 方針を埋め込んだカーネルを再利用する | キーに必要な成分、退避後も保持された生成物、同じキーの公開順、追加コンパイル回数 |

<details>
<summary>100 モデルの宣言位置</summary>

- **CheckedRewrites**　[CancelAdd](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L62)、[CancelSubtract](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L86)、[DoubleHalf](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L110)、[NegateTwice](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L134)、[DistributeScale](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L158)、[ProductMinusInput](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L182)、[SquareDistance](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L209)、[LinearRebias](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L233)、[ScaleZero](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L257)、[RecenterAbs](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean#L281)
- **WordPromises**　[SignedAdd](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L58)、[UnsignedAdd](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L81)、[SignedSub](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L104)、[UnsignedSub](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L127)、[SignedProduct](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L150)、[UnsignedProduct](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L173)、[ExactRightShift](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L196)、[UnsignedLeftShift](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L219)、[DisjointOr](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L242)、[SignedNarrow](../../TinylambOpt/ResearchGoals/JitFrontiers/WordPromises.lean#L265)
- **DemandSSA**　[AndCommute](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L81)、[OrFactor](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L102)、[CaseAnd](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L123)、[TrueGuard](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L144)、[CoalesceDispatch](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L165)、[AbsorbAnd](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L186)、[DoubleNegation](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L207)、[DeMorgan](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L228)、[IdenticalArms](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L249)、[NestedChoice](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean#L270)
- **VectorMemory**　[ForwardCopy](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L71)、[ReverseInput](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L92)、[StrideRead](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L113)、[StrideStore](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L134)、[NeighborSum](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L155)、[BackwardDelta](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L176)、[IndexedGather](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L197)、[IndexedScatter](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L218)、[RingWindow](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L239)、[PackedBits](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean#L260)
- **PrefixKernels**　[SignedSum](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L68)、[Product](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L91)、[NonNullCount](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L111)、[AlternatingDifference](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L131)、[ResetOnNull](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L151)、[DecimalAppend](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L173)、[PositiveFilter](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L193)、[DifferenceFromBase](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L213)、[LastPresent](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L233)、[SignedMagnitude](../../TinylambOpt/ResearchGoals/JitFrontiers/PrefixKernels.lean#L253)
- **RelationalGuards**　[CorrelatedSub](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L75)、[ScaledDifference](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L95)、[DoubleMinus](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L115)、[WeightedMix](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L135)、[AbsLag](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L155)、[SquareLag](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L175)、[PositiveExcess](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L195)、[LeastShift](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L215)、[ClampedDiff](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L235)、[ProductCorners](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean#L255)
- **BailoutState**　[CountRows](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L67)、[SumValid](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L90)、[MinValid](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L113)、[LastValue](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L136)、[FirstValue](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L159)、[Packed32](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L182)、[BucketCount](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L205)、[SignedDigit](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L228)、[ChangedOnly](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L251)、[ParityCounter](../../TinylambOpt/ResearchGoals/JitFrontiers/BailoutState.lean#L274)
- **PackedKeys**　[SignedHalves](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L70)、[OrderBiased](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L93)、[MixedDirection](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L116)、[CorrelatedDelta](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L139)、[TaggedNull](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L162)、[ZigzagPair](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L185)、[XorSide](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L208)、[DeltaMagnitude](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L231)、[ScaledKey](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L254)、[ShardLocal](../../TinylambOpt/ResearchGoals/JitFrontiers/PackedKeys.lean#L277)
- **PredicateBallots**　[Conjunction](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L83)、[Disjunction](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L106)、[ExclusiveOr](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L129)、[BoolEqual](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L152)、[Coalesce](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L175)、[NullSafeEqual](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L198)、[ConditionalNull](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L221)、[AntiPredicate](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L244)、[Implication](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L267)、[IfMissingTest](../../TinylambOpt/ResearchGoals/JitFrontiers/PredicateBallots.lean#L290)
- **SpecializationCache**　[BoundComparison](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L90)、[CheckedAffine](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L114)、[NullPolicy](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L138)、[SignDirection](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L162)、[PowerRemainder](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L186)、[ProjectPair](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L210)、[DictionaryContents](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L234)、[DictionaryChecksum](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L258)、[ConstantLane](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L282)、[TruncateKey](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean#L306)

</details>

## 適用を左右する具体例

- **最後に相殺されても途中で失敗する。** 符号付き 8 ビットの `(127 + 1) − 1` は加算で失敗する。JIT が式を `127` に畳むなら中間加算のガードが要る。一方 `x*y` の許容境界は符号で非対称になる。負の積なら絶対値 128 を許せるため、`ProductMinusInput` は積を作らず、符号で選んだ上限を除算してガードする。[CheckedRewrites](../../TinylambOpt/ResearchGoals/JitFrontiers/CheckedRewrites.lean)
- **共通項をくくるとエラーが消える。** `(a AND b) OR (a AND c)` に `a=NULL, b=TRUE, c=error(7)` を与えると、参照評価は順に `a,b,a,c` を読み、エラー 7 を返す。`a AND (b OR c)` は `a,b` だけを読んで NULL を返す。WHERE でも差が観測できる。エラーのない三値の真理表だけでは検出できない。[DemandSSA](../../TinylambOpt/ResearchGoals/JitFrontiers/DemandSSA.lean)・[AST の短絡評価](../../../expression/binary_expression.cpp#L1357)
- **頂点の積がすべてゼロでも、内部は負になり得る。** `0 ≤ x ≤ 4, −4 ≤ y ≤ 0, x−y=4` で `x*y` の両端は 0、内部の `(2,−2)` は −4。頂点だけなら区間を [0,0] と誤る。差分制約を使ったチェック除去では、アフィン式と積で別の候補点が必要になる。[RelationalGuards](../../TinylambOpt/ResearchGoals/JitFrontiers/RelationalGuards.lean)
- **アドレスがすべて有効でも SIMD 化で値が変わる。** `[1,2,3]` の最初の 2 語を 1 語右へ順にコピーすると `[1,1,1]`。先に 2 語ともロードしてから書くと `[1,1,2]`。ここで必要なガードは、先行レーンの書込み先が後続レーンの読取り先と重なるかどうかである。[VectorMemory](../../TinylambOpt/ResearchGoals/JitFrontiers/VectorMemory.lean)
- **辞書の世代とチェックサムが一致しても埋込み定数は違い得る。** 同じ世代の辞書 `[1,2]` と `[2,1]` は要素和が等しく、コード 0 の値が異なる。仮説 `DictionaryChecksum.Substitute.Goal` はこの入力で破れる。`DictionaryContents` と比べると、キーの縮小でどの条件を失ったかを追える。[SpecializationCache](../../TinylambOpt/ResearchGoals/JitFrontiers/SpecializationCache.lean)

### tinylamb の SQL で短絡の反例を再現する

空のデータベースで次の表を作り、2 本の SELECT を個別に実行した。既存の `build/tinylamb` では、元の式が `IsInfinity: division by zero`、共通項をくくった式が 1 行の `[NULL]` を返した。

```sql
CREATE TABLE t (a INT64, b INT64, z INT64);
INSERT INTO t VALUES (NULL, 1, 0);

SELECT ((a = 1) AND (b = 1)) OR ((a = 1) AND (10/z > 1)) AS result FROM t;
SELECT (a = 1) AND ((b = 1) OR (10/z > 1)) AS result FROM t;
```

## 実装・ワークロードとの対応

既存の JIT 課題 16 件には、加算の失敗レーン、複合順序キー、三値述語がある。今回のモデルでは、ガードが必要な入力全体、最初のエラー、実際に読むレーン、途中状態からの再開までを個別に問える。実装の照合先は、検査付き射影、フィルタと SUM の融合、分岐先の範囲推論、2×32 ビット radix カーネルである。[既存 16 件](jit-skip/README.md)・[検査付き射影](../../../expression/jit.cpp#L929)・[融合ループ](../../../expression/jit.cpp#L655)・[範囲推論](../../../expression/jit.cpp#L3276)・[radix カーネル](../../../expression/jit.cpp#L4686)

保存済みの DuckDB 比較から見ると、TPC-H Q8 の国別分岐には `DemandSSA` と `PrefixKernels`、Iceberg の等値削除には `PackedKeys` と `BailoutState`、Public BI の MLB 分類には辞書を埋め込む `SpecializationCache` が候補になる。これらは適用箇所の仮説であり、今回計測したのは下記の意味比較の不一致数である。[クエリ・中央値・既存の追試](duckdb-gaps.md)

LLVM のモデルは定義済みの入力を受ける単一命令の結果を扱う。`none` は poison を表し、SQL のエラー、即時の未定義動作、ポインタの寿命は別の意味論を要する。累計モデルは各アフィン更新の結果で検査する。乗算と加算が個別に失敗し得る実装への適用には、乗算のガードも必要になる。キャッシュモデルの保持済み生成物は不変値であり、ORC のリソース解放との同期は実装側の条件である。[LLVM LangRef](https://llvm.org/docs/LangRef.html)・[LLVM の未定義動作と poison](https://llvm.org/docs/UndefinedBehavior.html)・[融合の実装テスト](../../../expression/jit_test.cpp#L205)

## 有限入力での照合と再現

| 入力と観測 | 比較した入力数（規則ごとの延べ数） | ガードが通った入力 | ガード内の不一致 | ガードを外した不一致 |
| --- | ---: | ---: | ---: | ---: |
| 符号付き 8 ビット整数の全入力対 × 算術 10 規則。値またはエラー箇所を比較 | 655,360 | 429,812 | 0 | 225,534 |
| 3 スロットそれぞれに TRUE、FALSE、NULL、エラー 3、エラー 7 × 述語 10 規則 | 1,250 | 1,085 | 2（射影） | 92（WHERE） |

述語のガード内で食い違った 2 入力は、上記の共通項をくくる変換で、右端のエラー箇所が 3 または 7 の場合。表の右の 2 列は、述語については別々の観測を集計している。算術の 0 件は、列挙した 8 ビット入力での結果である。幅 64 を含む一般命題は証明の対象として残る。[有限照合の実行コード](fixtures/jit_boundary_census.lean)

リポジトリのルートから、次のコマンドで全体の型検査と規則別の集計を再現できる。

```sh
cd lean
lake build
lake env lean --run issues/research/fixtures/jit_boundary_census.lean
```
