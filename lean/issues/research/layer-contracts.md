# データ表現・SQL 実行・復旧の境界に関する 1,000 題

100 個の具体的なモデルについて、分野共通の性質 8 件とモデル固有の境界条件 2 件を、個別の未証明命題として登録した。各ファイルの先頭に参照計算と候補実装があり、末尾の `#guard` で境界入力を実行できる。名前は `TinylambOpt.ResearchGoals.LayerContracts.<分野>.<モデル>.<性質>.Goal`。[一括 import](../../TinylambOpt/ResearchGoals/LayerContracts.lean)

前回は WAL の耐久位置、索引更新履歴、プランの費用境界などを扱った。今回は同じ層にも別の観測を置いた。たとえば復旧では、同期を待つ範囲から、一部反映済みのセルに合成ログを再生してよい条件へ移った。索引では、更新履歴を使ったレンジの再検証から、述語を複数のシークに変換したときの被覆・順序・多重度へ移った。下記の監査では、公開された新旧の提出型を全 100,000 組について Lean の定義的等価性で比較できる。[前回の 100 題](engine-boundaries.md)・[監査コード](fixtures/audit_layer_contracts.lean)

## 分野別索引

| 分野（各 100 件） | 証明・反証で判断できること | tinylamb での照合先 |
| --- | --- | --- |
| [CalendarRanges](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean) | DATE の等値条件を半開の日付範囲に変換する。月末、閏世紀、ISO 週年、年度、営業日の週末繰越しで周期短縮や平均幅による除算が使えるか。 | [DATE の表現範囲と日付 API](../../../type/date.hpp) |
| [SlotPlacement](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean) | 変長行の格納先を飛び石で探す。整列、ヘッダ予約、分離配置を含めて、最初の空きと満杯判定が全探索と一致するか。 | [行ページの空き領域と行の挿入](../../../page/row_page.cpp#L51) |
| [SeekIntervals](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean) | 余り、飽和減算、範囲の補集合などを索引区間へ変換する。ヒット集合の一致に加え、重複行、逆順 LIMIT、シーク回数まで保存できるか。 | [索引スキャン](../../../index/index_scan_iterator.hpp) |
| [ReferenceEdits](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean) | 親子キーの差分検査。親の置換、子の付替え、明示的な連鎖削除で、各編集直後と文末の判定はいつ一致するか。再試行で行数が増えないか。 | [親キーの参照検査](../../../executor/foreign_key.cpp#L990) |
| [ScopeLookup](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean) | 候補の名前索引、スコープの平坦化、束縛結果のキャッシュ。内側の名前による遮蔽、同名の曖昧さ、引数の型や個数を維持できるか。 | [ORDER BY 名の曖昧さの検査](../../../query/googlesql_ast_visitor.cpp#L202) |
| [RedoPatches](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean) | 同一セルの複数のログ更新を 1 件にまとめる。完全上書き、加算、ビットフィールド、条件付き更新について、省略・再開・順序交換を別々に検証する。 | [コミット済みレコードの再生](../../../recovery/recovery_manager.cpp#L951) |
| [ArrayOffsets](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean) | 配列展開の行数と OFFSET を入力から直接求める。空配列、NULL 要素、zip の欠けた側、直積、重複した値を追跡する。 | [相関 UNNEST の書き換え](../../../query/googlesql_ast_visitor_expr.cpp#L638) |
| [FrameRetractions](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean) | ウィンドウから 1 行抜けたとき、直前の集計結果と件数から更新できるか。再走査が情報量の上で必要か、NULL を件数に含めるとどうなるか。 | [ウィンドウフレームと現在行の除外](../../../executor/detail/window_eval.cpp#L4008) |
| [RecursivePaths](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean) | 再帰クエリの既訪問キーを頂点だけに縮められるか。偶数長、順序付きラベル、通行費の予算などを条件に、探索打ち切りと到達可能性を比較する。 | [再帰 CTE の実行器](../../../executor/recursive_cte.cpp) |
| [LexicalCuts](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean) | 文・フィールドの区切りを複数チャンクで検出する。引用・エスケープ・コメント深度をどこまで次チャンクに引き継ぐ必要があるか。 | [Simple Query の文分割器](../../../server/postgres_protocol.cpp#L565) |

<details>
<summary>100 モデルの宣言位置</summary>

- **CalendarRanges**　[Month](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L57)、[Quarter](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L79)、[CivilYear](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L100)、[FiscalApril](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L121)、[HalfMonth](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L142)、[BusinessDay](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L163)、[Retail445](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L184)、[Decade](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L205)、[IsoWeekYear](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L226)、[Payroll](../../TinylambOpt/ResearchGoals/LayerContracts/CalendarRanges.lean#L247)
- **SlotPlacement**　[Contiguous](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L61)、[AlignedWord](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L83)、[TrailerWord](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L105)、[FenceStripe](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L127)、[ReserveHeader](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L149)、[TwoFragments](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L171)、[ForwardGrowth](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L193)、[Interleave](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L215)、[SlotGroup](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L237)、[PhaseAligned](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean#L259)
- **SeekIntervals**　[NotEqual](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L55)、[OutsideClosed](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L77)、[RingSlice](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L99)、[DistanceBand](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L121)、[QuotientBand](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L143)、[Residue](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L165)、[TwoLiterals](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L187)、[SaturatingSubtract](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L209)、[MinClamp](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L231)、[XorLowBit](../../TinylambOpt/ResearchGoals/LayerContracts/SeekIntervals.lean#L253)
- **ReferenceEdits**　[ParentFirst](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L64)、[ChildFirst](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L85)、[Reparent](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L106)、[Cascade](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L127)、[SetNull](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L148)、[ReplaceParent](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L169)、[Adopt](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L190)、[DropPair](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L211)、[SwapReferences](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L232)、[RekeyLate](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean#L253)
- **ScopeLookup**　[BareColumn](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L59)、[QualifiedColumn](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L81)、[QuotedLabel](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L103)、[OrderOrdinal](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L125)、[TypedFunction](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L147)、[Variadic](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L169)、[WildcardOwner](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L191)、[FieldNumber](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L213)、[CompatibleType](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L235)、[VisiblePrefix](../../TinylambOpt/ResearchGoals/LayerContracts/ScopeLookup.lean#L257)
- **RedoPatches**　[Overwrite](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L51)、[Increment](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L74)、[Affine](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L97)、[HighWater](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L120)、[Ceiling](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L143)、[XorFlags](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L166)、[FieldStore](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L189)、[Consume](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L212)、[CompareSwap](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L235)、[Modulo](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean#L258)
- **ArrayOffsets**　[LeftElements](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L58)、[ReverseOffsets](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L81)、[ZipShortest](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L104)、[ZipPad](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L127)、[Cartesian](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L150)、[NonNullOnly](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L173)、[EvenOffsets](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L196)、[AppendFields](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L219)、[LeadPair](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L242)、[MatchValue](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean#L265)
- **FrameRetractions**　[Sum](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L55)、[Product](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L78)、[Minimum](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L101)、[Maximum](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L124)、[BitUnion](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L147)、[BitParity](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L170)、[DistinctCount](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L193)、[FloorMean](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L216)、[RangeWidth](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L239)、[LowerMedian](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean#L262)
- **RecursivePaths**　[EvenHops](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L66)、[AtLeastOneRed](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L87)、[Alternating](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L108)、[SingleRed](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L129)、[TollBudget](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L150)、[OrderedClass](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L171)、[LastBlue](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L192)、[PatternAB](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L213)、[ModuloCost](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L234)、[BothTypes](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean#L255)
- **LexicalCuts**　[StringStmt](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L61)、[QuotedField](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L83)、[BacktickPath](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L105)、[EscapedString](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L127)、[DashComment](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L149)、[NestedComment](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L171)、[DollarPair](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L193)、[CsvRecord](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L215)、[ArrayComma](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L237)、[CrLf](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean#L259)

</details>

## 実装候補をふるいにかける有限照合

**ページの配置。** 5 バイトのページの全占有パターンに、要求長 1〜3、旧位置 0〜2、ストライプ引数 0〜2 を組み合わせた。配置方式ごとに 864 入力、計 8,640 入力。全位置を調べる参照計算が空きを見つけたのに、占有バイト直後と先頭だけを候補にした探索が失敗したのは **18 件**で、すべてヘッダと本体の分離配置だった。より小さい例では、4 バイト中の位置 2 だけが占有され、配置に `i` と `i+2` が必要なとき、唯一の解は位置 1。候補の 0 と 3 はどちらも失敗する。[SlotPlacement](../../TinylambOpt/ResearchGoals/LayerContracts/SlotPlacement.lean)

**ウィンドウ集計。** 値 0〜3、長さ 1〜4 の全フレームで、各行を 1 回ずつ取り除いた。重複した行も別々に数え、10 方式で計 12,520 試行。切り捨て平均の候補は 1,252 試行すべてで再走査を省き、**552 件で再計算と不一致**だった。たとえば `[0,1,1]` の整数平均は 0 で、0 を取り除いた `[1,1]` の平均は 1。直前の平均に件数を掛けても、失われた余りは復元できない。下側中央値の候補は同じ 1,252 試行で 810 件を再走査に戻し、直接更新した 442 件はすべて一致した。一般の入力で成立するかは未証明である。[FrameRetractions](../../TinylambOpt/ResearchGoals/LayerContracts/FrameRetractions.lean)・[列挙と計数のコード](fixtures/layer_contract_census.lean)

## 証明・反証の出発点

- **ログの合成と再開。** 初期値 10 に LSN 1 で `+2`、LSN 2 で `+3` を適用すると 15。初期状態からは `+5` にまとめられるが、LSN 1 まで反映した値 12 にその記録を適用すると 17 になる。`Increment.Fusion` と `Increment.RestartInside` を分けた理由がこの入力にある。[RedoPatches](../../TinylambOpt/ResearchGoals/LayerContracts/RedoPatches.lean)
- **制約検査のタイミング。** 空の DB に子キー 3 を追加し、その後に親キー 3 を追加すると、文末には参照先があるが、最初の編集直後にはない。同じ違反判定でも観測時点が違えば許容できるバッチが変わる。[ReferenceEdits](../../TinylambOpt/ResearchGoals/LayerContracts/ReferenceEdits.lean)
- **再帰探索の既訪問判定。** 頂点 0 に赤い自己辺が 1 本あり、「赤い辺を一度でも通った経路」を問う。出発時と 1 歩後は同じ頂点だが受理状態が違う。頂点だけで重複排除すると、唯一の結果が消える。[RecursivePaths](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean)
- **コメントの深度。** 入力の続きが「コメント終了、セミコロン」のとき、開始深度が 1 なら文は切れ、2 なら切れない。チャンクに入った時点で「コメント内か」の 1 ビットだけをキーにする案を区別できる。[LexicalCuts](../../TinylambOpt/ResearchGoals/LayerContracts/LexicalCuts.lean)

## SQL 入力での照合

既存の `build/tinylamb` で次の 2 本を実行した。1 本目の結果は順に `(NULL,0), (7,1), (7,2)`。NULL にもオフセットがあり、等しい値 7 は 2 行を占める。2 本目は `0` を 1 行返した。再帰テーブルは頂点と状態の両方を列として持つ。[配列の提出型](../../TinylambOpt/ResearchGoals/LayerContracts/ArrayOffsets.lean)・[経路の提出型](../../TinylambOpt/ResearchGoals/LayerContracts/RecursivePaths.lean)

```sql
SELECT x, off
FROM UNNEST([CAST(NULL AS INT64), 7, 7]) AS x WITH OFFSET AS off
ORDER BY off;

WITH RECURSIVE walk AS (
  SELECT 0 AS v, 0 AS seen
  UNION DISTINCT
  SELECT v, 1 FROM walk WHERE seen = 0
)
SELECT v FROM walk WHERE seen = 1;
```

## モデルの前提と再現

新しい候補には、まだエンジンに採用していない配置方式・暦・パッチ命令・名前探索規則も含む。日付は各暦の起点以後、索引は非 NULL の自然数キー、集約はオーバーフローのない非負整数に範囲を定めた。参照制約モデルは親キーの存在を数え、親の一意性やトランザクション間の競合は独立の条件とする。字句モデルは各構文の断片を扱い、入れ子コメントでは開始・終了をすでに識別した記号を入力する。

```sh
cd lean
lake build
lake env lean issues/research/fixtures/audit_layer_contracts.lean
lake env lean --run issues/research/fixtures/layer_contract_census.lean
```
