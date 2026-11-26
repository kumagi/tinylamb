import TinylambOpt

/-!
This executable illustrates selected rewrite rules using finite Lean examples. It lets a
reader compare the observable rows or values of modeled SQL plans.
-/

open TinylambOpt

/-- `lake exe demo` でターミナルから実行できる具体例デモ。
    （定理自体は `lake build` 時点で「無限通りの全入力」に対して数学的に証明済みですが、
      実際にどんな行データでバグが起き・どう防がれるかを目で見るための実行ファイルです） -/
def main : IO Unit := do
  IO.println "=== [1] or_to_union: 三値論理 NULL と Bag 重複度の検証 ==="
  -- 行データ: (p0, p1) の三値ブール対（同じ行が 2 回現れる重複データを含む）
  let sampleRows : Rel (SqlBool × SqlBool) := [
    (.tNull, .tTrue),   -- ← 素朴な `p1 AND NOT p0` だと消滅する危険な行！
    (.tTrue, .tTrue),   -- ← 素朴な `UNION ALL` だと 2 倍に増える行！
    (.tTrue, .tTrue),   -- （元テーブルに最初からあった重複行）
    (.tFalse, .tNull)   -- ← 落ちるべき行
  ]
  let orig := select (fun r => r.1.or r.2) sampleRows
  let brokenNaive :=
    unionAll
      (select (fun r => r.1) sampleRows)
      (select (fun r => r.2.and r.1.not) sampleRows)
  let tinylambVerified :=
    unionAll
      (select (fun r => orBranch0 r.1) sampleRows)
      (select (fun r => orBranch1 r.1 r.2) sampleRows)

  IO.println s!"  元の Selection(p0 OR p1) 結果 ({orig.length} 行): {repr orig}"
  IO.println s!"  ❌ 素朴な `p1 AND NOT p0` 分解 ({brokenNaive.length} 行 -> `(.tNull, .tTrue)` が消失！): {repr brokenNaive}"
  IO.println s!"  ✅ tinylamb `or_to_union` 分解 ({tinylambVerified.length} 行 -> 重複度まで一致！):   {repr tinylambVerified}"
  IO.println ""

  IO.println "=== [2] outer_to_anti_join: 反例と Lean で発見した新ルールの検証 ==="
  let lTable : Rel Int := [1, 2, 2, 3]
  let rTable : Rel RightRow := [
    { id := some 1, note := none },   -- id=1 はマッチするが note は NULL
    { id := none,   note := some 99 } -- R.id 自体に NULL 行が混入しているテーブル！
  ]
  let equiOn := fun (x : Int) (y : RightRow) => sqlEq (some x) y.id

  -- 反例: 結合キーではない `r.note IS NULL` で AntiJoin 化してしまうと id=1 が消える
  let noteNullOrig := select (rightColIsNull RightRow.note) (leftOuterJoin equiOn lTable rTable)
  let noteNullBadAnti : Rel (Int × Option RightRow) := padRightNull (antiJoin equiOn lTable rTable)
  IO.println s!"  [反例] WHERE r.note IS NULL の本来の結果: {repr noteNullOrig}"
  IO.println s!"  [反例] ❌ 誤って AntiJoin 化すると id=1 が脱落: {repr noteNullBadAnti}"
  IO.println ""

  -- 新発見ルール: カタログ NOT NULL 無し（実際に rTable に id=NULL が混入！）＋ 残余フィルタ付きでも
  -- `ON l = r.id` 自体が `r.id = NULL` を弾くため、残余フィルタを左へ押し込んだ AntiJoin と完全一致！
  let residualQ := fun (x : Int) (ry : Option RightRow) =>
    -- 例: `COALESCE(r.note, l) >= 2`
    let v : Int := match ry with
      | none => x
      | some r => r.note.getD x
    if v ≥ 2 then SqlBool.tTrue else SqlBool.tFalse

  let slowOuterPlan :=
    select (fun lr => (rightColIsNull RightRow.id lr).and (residualQ lr.1 lr.2))
      (leftOuterJoin equiOn lTable rTable)
  let fastAggressivePlan : Rel (Int × Option RightRow) :=
    padRightNull (antiJoin equiOn (select (fun x => residualQ x none) lTable) rTable)

  IO.println s!"  [新発見] 元の LeftOuterJoin + Selection 結果:       {repr slowOuterPlan}"
  IO.println s!"  [新発見] ✅ アグレッシブ AntiJoin + 左押し込み結果: {repr fastAggressivePlan}"
  IO.println s!"  完全一致チェック: {slowOuterPlan == fastAggressivePlan}"
  IO.println ""

  IO.println "=== [3] M:N Join 両側同時事前集約: テーブル横断式 SUM(L.x + R.y) の罠と正解式 ==="
  -- L: (key, x), R: (key, y) ともに key=1 が重複する M:N 結合（さらに NULL 値も混在！）
  let mnl : Rel (SqlVal Int × SqlVal Int) := [
    (some 1, some 10), (some 1, some 20), (none, some 999)
  ]
  let mnr : Rel (SqlVal Int × SqlVal Int) := [
    (some 1, some 1), (some 1, none), (some 1, some 2)
  ]
  let mnOrig := equiJoinPairs Prod.fst Prod.fst mnl mnr
  let trueAddSum := sqlSum (fun xy => sqlAdd xy.1.2 xy.2.2) mnOrig
  let trueMulSum := sqlSum (fun xy => sqlMul xy.1.2 xy.2.2) mnOrig

  -- 両側を Join 前に 1 行ずつへ圧縮！
  let pL := groupBy Prod.fst (fun _ b => (sqlSum Prod.snd b, sqlCountCol Prod.snd b, sqlCountStar b))
    (mnl.filter (fun r => r.1.isSome))
  let pR := groupBy Prod.fst (fun _ b => (sqlSum Prod.snd b, sqlCountCol Prod.snd b, sqlCountStar b))
    (mnr.filter (fun r => r.1.isSome))
  let dualJ := equiJoinPairs Prod.fst Prod.fst pL pR

  let buggyAddSum := sqlSum (fun pq =>
    sqlAdd (sqlMul pq.1.2.1 (some pq.2.2.2.2)) (sqlMul pq.2.2.1 (some pq.1.2.2.2))) dualJ
  let verifiedAddSum := sqlSum (fun pq =>
    sqlAdd (sqlMul pq.1.2.1 (some pq.2.2.2.1)) (sqlMul pq.2.2.1 (some pq.1.2.2.1))) dualJ
  let verifiedMulSum := sqlSum (fun pq => sqlMul pq.1.2.1 pq.2.2.1) dualJ

  IO.println s!"  M:N 生結合行数: {mnOrig.length} 行 -> 両側事前集約後の結合行数: {dualJ.length} 行"
  IO.println s!"  SUM(L.x + R.y) 本来の値:                       {repr trueAddSum}"
  IO.println s!"  ❌ COUNT(*) で重み付けした誤式 (NULL 行で狂う): {repr buggyAddSum}"
  IO.println s!"  ✅ COUNT(col) で重み付けした Lean 証明済み式:   {repr verifiedAddSum}"
  IO.println s!"  ✅ SUM(L.x * R.y) 独立積公式 ({repr trueMulSum} == {repr verifiedMulSum}): {trueMulSum == verifiedMulSum}"
  IO.println ""

  IO.println "=== [4] LEFT OUTER JOIN 右側事前集約: COUNT(*) の COALESCE(..., 1) トラップ ==="
  let depts : Rel (SqlVal Int × String) := [(some 10, "Eng"), (some 20, "EmptyDept")]
  let sales : Rel (SqlVal Int × SqlVal Int) := [(some 10, some 100), (some 10, none), (some 10, some 50)]
  let origLeft := groupBy (fun ds => ds.1.2)
    (fun _ b => (sqlCountStar b, sqlCountCol (fun ds => ds.2.bind Prod.snd) b, sqlSum (fun ds => ds.2.bind Prod.snd) b))
    (leftOuterJoin (fun d s => sqlEq d.1 s.1) depts sales)
  let partSales := groupBy Prod.fst
    (fun _ b => RightPreAgg.mk (sqlCountStar b) (sqlCountCol Prod.snd b) (sqlSum Prod.snd b))
    (sales.filter (fun s => s.1.isSome))
  let joinedLeft := leftOuterJoin (fun d p => sqlEq d.1 p.1) depts partSales
  let buggyLeft := groupBy (fun dp => dp.1.2)
    (fun _ b => (
      aggFold countMonoid (fun dp => match dp.2 with | none => 0 | some p => p.2.cntStar) b,
      aggFold countMonoid (fun dp => match dp.2 with | none => 0 | some p => p.2.cntCol) b,
      aggFold sumMonoid   (fun dp => match dp.2 with | none => none | some p => p.2.sumCol) b))
    joinedLeft
  let verifiedLeft := groupBy (fun dp => dp.1.2)
    (fun _ b => (
      aggFold countMonoid (fun dp => match dp.2 with | none => 1 | some p => p.2.cntStar) b,
      aggFold countMonoid (fun dp => match dp.2 with | none => 0 | some p => p.2.cntCol) b,
      aggFold sumMonoid   (fun dp => match dp.2 with | none => none | some p => p.2.sumCol) b))
    joinedLeft
  IO.println s!"  本来の D LEFT JOIN S 集約 (dept, (COUNT(*), COUNT(val), SUM(val))):"
  IO.println s!"    {repr origLeft}"
  IO.println s!"  ❌ COUNT(*) に COALESCE(..., 0) を使った誤変換 (EmptyDept が 0 に化ける！):"
  IO.println s!"    {repr buggyLeft}"
  IO.println s!"  ✅ COUNT(*) に COALESCE(..., 1) を使った Lean 証明済み変換:"
  IO.println s!"    {repr verifiedLeft}"
  IO.println s!"  完全一致チェック: {origLeft == verifiedLeft}"
  IO.println ""

  IO.println "=== [5] DISTINCT / MIN / MAX 冪等集約: 非 Unique Join の SemiJoin 縮退 & LEFT JOIN 完全消去 ==="
  -- key=2 が R 側で 3 回重複マッチする非 Unique データ
  let items : Rel (Int × Int × SqlVal Int) := [
    (100, 1, some 10), (100, 2, some 20), (100, 2, some 20), (100, 3, some 999)
  ]
  let tagsNonUnique : Rel Int := [1, 2, 2, 2]
  let itemTagOn := fun (it : Int × Int × SqlVal Int) (tg : Int) => sqlEq (some it.2.1) (some tg)

  -- 通常の AVG(val) は SemiJoin にすると偏った重複 (key=2 が 3 回) が消えて狂う！
  let origPlainAvg := sqlAvg (fun lr => lr.1.2.2) (innerJoin itemTagOn items tagsNonUnique)
  let badSemiPlainAvg := sqlAvg (fun it => it.2.2) (semiJoin itemTagOn items tagsNonUnique)
  IO.println s!"  通常の AVG(val): InnerJoin={repr origPlainAvg} vs ❌ SemiJoin={repr badSemiPlainAvg} (偏った fan-out で不一致！)"

  -- 一方 (MIN, MAX, COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT)) は SemiJoin でも 100% 一致！
  let origIdemInner := groupBy (fun lr => lr.1.1)
    (fun _ b => evalIdempotentAggs (fun lr => lr.1.2.2) b)
    (innerJoin itemTagOn items tagsNonUnique)
  let fastIdemSemi := groupBy (fun it => it.1)
    (fun _ b => evalIdempotentAggs (fun it => it.2.2) b)
    (semiJoin itemTagOn items tagsNonUnique)
  IO.println s!"  ✅ 冪等5種 (MIN, MAX, CNT_D, SUM_D, AVG_D): InnerJoin == SemiJoin -> {origIdemInner == fastIdemSemi}"
  IO.println s!"     結果: {repr fastIdemSemi}"

  -- さらに LEFT JOIN では非 Unique な右テーブル自体を丸ごと消去しても 100% 一致！
  let origIdemLeft := groupBy (fun lr => lr.1.1)
    (fun _ b => evalIdempotentAggs (fun lr => lr.1.2.2) b)
    (leftOuterJoin itemTagOn items tagsNonUnique)
  let fastIdemNoJoin := groupBy (fun it => it.1)
    (fun _ b => evalIdempotentAggs (fun it => it.2.2) b)
    items
  IO.println s!"  ✅ 冪等5種での非 Unique LEFT JOIN 丸ごと消去: LeftJoin == 単体Scan -> {origIdemLeft == fastIdemNoJoin}"
  IO.println ""

  IO.println "=== [6] HAVING × AVG/MIN/MAX: LEFT JOIN → INNER JOIN 降格と COUNT(*) 同居トラップ ==="
  let dCoarse : Rel (Int × String) := [(1, "HQ"), (2, "HQ"), (3, "Branch")]
  let sSparse : Rel (Int × SqlVal Int) := [(1, some 200)] -- dept=2 と dept=3 は売上なし
  let dsOn := fun (d : Int × String) (s : Int × SqlVal Int) => sqlEq (some d.1) (some s.1)
  let havingAvgGt100 := fun (_ : String) (amm : Option ExactRatio × SqlVal Int × SqlVal Int) =>
    match amm.1 with
    | some r => if r.num > 100 * (r.den : Int) then .tTrue else .tFalse
    | none   => .tNull
  let outerHaving := havingFilter havingAvgGt100
    (groupBy (fun ds => ds.1.2)
      (fun _ b => (sqlAvg (fun ds => ds.2.bind Prod.snd) b,
                   sqlMin (fun ds => ds.2.bind Prod.snd) b,
                   sqlMax (fun ds => ds.2.bind Prod.snd) b))
      (leftOuterJoin dsOn dCoarse sSparse))
  let innerHaving := havingFilter havingAvgGt100
    (groupBy (fun ds => ds.1.2)
      (fun _ b => (sqlAvg (fun ds => ds.2.bind Prod.snd) b,
                   sqlMin (fun ds => ds.2.bind Prod.snd) b,
                   sqlMax (fun ds => ds.2.bind Prod.snd) b))
      (innerJoin dsOn dCoarse sSparse))
  IO.println s!"  ✅ 右列集約のみの HAVING AVG(s.v) > 100 で LEFT JOIN → INNER JOIN 降格:"
  IO.println s!"     LeftJoin={repr outerHaving} == InnerJoin={repr innerHaving} -> {outerHaving == innerHaving}"
  IO.println ""

  IO.println "=== [7] C++ 既存実装の直訳監査で発見した実在バグと修正検証 ==="
  let badIn := AuditExpr.isFalse (.inList (.paddedCol 0) [])
  let badSub := AuditExpr.notE (.sub (.constVal (some 1)) (.constVal (some 1)))
  IO.println s!"  🚨 Bug 1a `(r.a IN ()) IS FALSE`: 旧C++ RejectsNulls={cppBuggyRejectsNulls badIn} なのに NULL行評価={repr (valToSqlBool (evalAuditExpr (fun _ => none) (fun _ => none) badIn))} -> 修正後={repairedRejectsNulls badIn}"
  IO.println s!"  🚨 Bug 1b `NOT (1 - 1)`:          旧C++ RejectsNulls={cppBuggyRejectsNulls badSub} なのに NULL行評価={repr (valToSqlBool (evalAuditExpr (fun _ => none) (fun _ => none) badSub))} -> 修正後={repairedRejectsNulls badSub}"
  IO.println s!"  🚨 Bug 2  `ON R.pk = R.col`:      旧C++ PinsRightPk={cppBuggyPinsRightPk .rightPk .rightCol} -> 修正後={repairedPinsRightPk .rightPk .rightCol}"
  let gkeys := [GroupKeyItem.colKey "a", GroupKeyItem.exprKey "abs(b)"]
  let oldBug4 := cppBuggyAllGroupingProjected gkeys ["a"]
  let newBug4 := repairedAllGroupingProjected gkeys [.colKey "a"]
  IO.println s!"  🚨 Bug 4  `GROUP BY a, ABS(b)`:   旧C++ AllGroupingProjected={oldBug4} -> 修正後={newBug4}"
  IO.println ""

  IO.println "=== [8] 高リスク領域の第2弾監査で発見した C++ 実在バグと検証 ==="
  let nanRel : Rel DoubleSqlVal := [.nan]
  let setOpNan := nanRel.filter (fun l => nanRel.any (fun r => setOpSame l r))
  let semiNan := semiJoin cppIsNotDistinctFromDouble nanRel nanRel
  IO.println s!"  🚨 Bug H1 `INTERSECT` on DOUBLE NaN: 本来={repr setOpNan} vs 旧C++ SemiJoin(IS NOT DISTINCT FROM)={repr semiNan}"
  let eqChain := [("t1.id", "t2.id"), ("t1.x", "t1.x")]
  let envNull : String → SqlVal Int := fun s => if s == "t1.x" then none else some 1
  IO.println s!"  🚨 Bug H2 `t1.id = t2.id AND t1.x = t1.x`: 本来評価={evalEqConjuncts envNull eqChain} vs 旧C++ Union-Find削減後={evalEqConjuncts envNull (cppBuggyFilterEqs eqChain)}"
  let fp1 := cppBuggyInListFingerprint ["a;b", "c"]
  let fp2 := cppBuggyInListFingerprint ["a", "b;c"]
  IO.println s!"  🚨 Bug H6 `in_list_to_semi_join` タグ衝突: ['a;b','c']->'{fp1}' == ['a','b;c']->'{fp2}' ({fp1 == fp2})"
  IO.println ""

  IO.println "=== [9] 第3弾監査 (Window / Unnest / LATERAL / INT64 Overflow / IEEE 2^53) ==="
  let b1cOld := cppBuggyOuterUsesWindow ["w"] [("", "*")]
  let b1cNew := repairedOuterUsesWindow ["w"] [("", "*")]
  IO.println s!"  🚨 Bug M1c `no_op_window_elimination` SELECT * 見落とし: 旧C++={b1cOld} -> 修正後={b1cNew}"
  let b3aOld := cppBuggyTouchesUnnest "" "" "" "unnest"
  let b3aNew := repairedTouchesUnnest "" "" "" "unnest"
  IO.println s!"  🚨 Bug M3a `unnest_filter_pushdown` 既定列 `unnest` 見落とし: 旧C++={b3aOld} -> 修正後={b3aNew}"
  let b5Eq := ieee53Round 9007199254740993 == 9007199254740992
  IO.println s!"  🚨 Bug M5  `INT64/DOUBLE` 2^53 非推移性: round(2^53+1)==2^53 -> {b5Eq} なのに 2^53+1 == 2^53 -> false"
  let ovBug := checkedAdd64 (checkedAdd64 (.val int64Min) (.val (-1))) (.val 1)
  let ovBad := checkedAdd64 (.val int64Min) (.val 0)
  IO.println s!"  ✅ INT64 異符号再結合トラップ: (INT64_MIN + -1) + 1 = {repr ovBug} != INT64_MIN + 0 = {repr ovBad}"
  IO.println ""

  IO.println "=== [10] TPC-DS 向け新規アグレッシブ最適化 6 系統の実行検証 ==="
  -- (A) Q65 / Q1 / Q81: 自己結合集約 -> Window 1 パス吸収 (+ NULL キー罠)
  let q65S : Rel (SqlVal Int × Int) := [(some 1, 10), (some 1, 90), (some 2, 50), (none, 5)]
  let q65B := groupBy Prod.fst (fun _ b => sqlSum (fun r => some r.2) b) q65S
  let q65Pred : (SqlVal Int × Int) → SqlVal Int → SqlBool := fun r v =>
    match v with | some tot => if r.2 * 2 <= tot then .tTrue else .tFalse | none => .tNull
  let q65Orig := ((innerJoin (fun s b => sqlEq s.1 b.1) q65S q65B).filter (fun sb =>
    match sb.2 with | none => false | some (_, v) => (q65Pred sb.1 v).inWhere)).map Prod.fst
  let q65Win := ((evalPartitionWindow Prod.fst (fun b => sqlSum (fun r => some r.2) b) q65S).filter
    (fun rw => rw.1.1.isSome && (q65Pred rw.1 rw.2).inWhere)).map Prod.fst
  IO.println s!"  ✅ Q65/Q1 自己結合集約 -> Window 1 パス吸収: {repr q65Orig} == {repr q65Win} ({q65Orig == q65Win})"

  -- (B) Q97: FULL OUTER JOIN 3 指標 SUM -> SemiJoin 包除原理 (+ 部分 NULL キー罠)
  let ssci : Rel (SqlVal Int × SqlVal Int) := [(some 1, some 100), (some 2, some 200), (some 10, none)]
  let csci : Rel (SqlVal Int × SqlVal Int) := [(some 2, some 200), (some 3, some 300), (some 10, none)]
  let q97Orig := evalFullOuterCustomerPresenceCounts ssci csci
  let sOk := ssci.filter (fun x => x.1.isSome && x.2.isSome)
  let cOk := csci.filter (fun x => x.1.isSome && x.2.isSome)
  let sPart := ssci.filter (fun x => x.1.isSome && x.2.isNone)
  let cPart := csci.filter (fun x => x.1.isSome && x.2.isNone)
  let inter := (sOk.filter (fun x => cOk.contains x)).length
  let q97Fast := (some (Int.ofNat (sOk.length - inter + sPart.length)),
                  some (Int.ofNat (cOk.length - inter + cPart.length)),
                  some (Int.ofNat inter))
  IO.println s!"  ✅ Q97 FULL OUTER JOIN 指標和 -> SemiJoin 包除原理 (部分NULL含む): {repr q97Orig} == {repr q97Fast} ({q97Orig == q97Fast})"
  IO.println ""

  IO.println "=== [11] ClickBench（1億行単一ワイド表）向け新規最適化 6 系統の実行検証 ==="
  -- (CB-2) Q18: 順序なし `GROUP BY UserID, SearchPhrase LIMIT K` のキー・ファイバー厳密部分集約
  let cb18Rows : Rel (Int × String) := [
    (10, "shoes"), (10, "shoes"), (10, "boots"),
    (21, "camera"), (21, "lens"), (33, "watch"), (33, "watch"), (33, "watch")
  ]
  let cb18Full := groupBy id (fun _ b => b.length) cb18Rows
  let cb18Fiber := (groupBy id (fun _ b => b.length) (cb18Rows.filter (fun r => r.1 % 2 == 1))).take 2
  let cb18AllExact := cb18Fiber.all (fun kv => cb18Full.contains kv)
  IO.println s!"  ✅ Q18 順序なし GROUP BY LIMIT 2 キー・ファイバー集約: {repr cb18Fiber} ⊆ 全表結果 ({cb18AllExact})"

  -- (CB-3) Q22/Q23: Top-N 勝者決定後の遅延 `MIN(URL)` 実体化 (+ NULL グループの SQL `IN` 罠)
  let cb23Rows : Rel (SqlVal Int × Int) := [
    (none, 50), (none, 12), (some 1, 99), (some 2, 70), (some 2, 30), (some 2, 40)
  ]
  let leDescCnt : (SqlVal Int × Nat) → (SqlVal Int × Nat) → Bool := fun a b => b.2 ≤ a.2
  let cb23Eager := ((sortByRank (fun a b => leDescCnt (a.1, a.2.1) (b.1, b.2.1))
    (groupBy Prod.fst (fun _ b => (b.length, sqlMin (fun x => some x.2) b)) cb23Rows)).take 2)
  let cb23Top1 := (sortByRank leDescCnt (groupBy Prod.fst (fun _ b => b.length) cb23Rows)).take 2
  let cb23Winners := cb23Top1.map Prod.fst
  let cb23Filtered := cb23Rows.filter (fun x => cb23Winners.contains x.1)
  let cb23Late := cb23Top1.map (fun kv =>
    (kv.1, (kv.2, sqlMin (fun x => some x.2) (cb23Filtered.filter (fun x => x.1 == kv.1)))))
  let cb23BuggyIn := cb23Top1.map (fun kv =>
    let inRows := cb23Rows.filter (fun x => cb23Winners.any (fun w => (sqlEq x.1 w).inWhere))
    (kv.1, (kv.2, sqlMin (fun x => some x.2) (inRows.filter (fun x => x.1 == kv.1)))))
  IO.println s!"  ✅ Q22/Q23 Top-N 2段遅延集約 (NULL勝者含む): Eager={repr cb23Eager} == Late={repr cb23Late} ({cb23Eager == cb23Late})"
  IO.println s!"  ❌ Pass 2 を SQL `IN` で書いた場合の NULL グループ破壊: {repr cb23BuggyIn}"

  -- (CB-5) Q40: 条件付き `CASE WHEN` キーの排他パーティション分割（Referer 未デコード高速経路）
  let cb40Rows : Rel ((Int × Int) × String × String) := [
    ((0, 0), "https://google.com", "/home"),
    ((0, 0), "", "/home"),
    ((1, 0), "https://ads.example/huge_ignored_string", "/home"),
    ((1, 0), "https://other.ignored/string", "/home")
  ]
  let cb40Orig := groupBy (fun r => (r.1, r.2.2, if r.1 == (0, 0) then r.2.1 else "")) (fun _ b => b.length) cb40Rows
  let cb40Split :=
    groupBy (fun r => (r.1, r.2.2, r.2.1)) (fun _ b => b.length) (cb40Rows.filter (fun r => r.1 == (0, 0))) ++
    (groupBy (fun r => (r.1, r.2.2)) (fun _ b => b.length) (cb40Rows.filter (fun r => !(r.1 == (0, 0))))).map
      (fun kv => ((kv.1.1, kv.1.2, ""), kv.2))
  IO.println s!"  ✅ Q40 条件付き CASE キー排他分割 (Referer デコード省略): {repr cb40Orig} == {repr cb40Split} ({cb40Orig == cb40Split})"
  IO.println ""

  IO.println "=== [12] TPC-H SF=10（約8,658万行）対 DuckDB 敗北クエリ向け新規最適化 6 系統の実行検証 ==="
  -- (TPCH-1) Q19: Join 横断 3枝 OR の共通因子括り出し + 単一テーブル選言包絡線プッシュダウン
  let tpch19Parts : Rel (Int × Int) := [(1, 53), (2, 13), (3, 99)]
  let tpch19Line : Rel (Int × Int × Bool) := [
    (1, 8, true), (2, 20, true), (3, 10, true), (1, 8, false)
  ]
  let tpch19Branches : List (((Int × Int) → SqlBool) × ((Int × Int × Bool) → SqlBool) × ((Int × Int) → (Int × Int × Bool) → SqlBool)) := [
    ((fun p => if p.2 == 53 then .tTrue else .tFalse),
     (fun l => if l.2.2 && 5 ≤ l.2.1 && l.2.1 ≤ 15 then .tTrue else .tFalse),
     (fun _ _ => .tTrue)),
    ((fun p => if p.2 == 13 then .tTrue else .tFalse),
     (fun l => if l.2.2 && 15 ≤ l.2.1 && l.2.1 ≤ 25 then .tTrue else .tFalse),
     (fun _ _ => .tTrue))
  ]
  let tpch19FullOr := fun (p : Int × Int) (l : Int × Int × Bool) =>
    sqlOrList (tpch19Branches.map (fun b => SqlBool.and (b.1 p) (SqlBool.and (b.2.1 l) (b.2.2 p l))))
  let tpch19EnvP := fun (p : Int × Int) => sqlOrList (tpch19Branches.map (fun b => b.1 p))
  let tpch19EnvL := fun (l : Int × Int × Bool) => sqlOrList (tpch19Branches.map (fun b => b.2.1 l))
  let tpch19Slow := select (fun pl => match pl with | (p, some l) => tpch19FullOr p l | _ => .tFalse)
    (innerJoin (fun p l => sqlEq (some p.1) (some l.1)) tpch19Parts tpch19Line)
  let tpch19Fast := select (fun pl => match pl with | (p, some l) => tpch19FullOr p l | _ => .tFalse)
    (innerJoin (fun p l => sqlEq (some p.1) (some l.1)) (select tpch19EnvP tpch19Parts) (select tpch19EnvL tpch19Line))
  IO.println s!"  ✅ Q19 選言包絡線プッシュダウン: 全走査結合={repr tpch19Slow} == 包絡線先行絞込={repr tpch19Fast} ({tpch19Slow == tpch19Fast})"

  -- (TPCH-2) Q13: LEFT JOIN 右側先行 COUNT 集約 + 左 UNIQUE NOT NULL による内側 GROUP BY 完全消去
  let tpch13Cust : Rel (SqlVal Int) := [some 1, some 2, some 3]
  let tpch13Ord : Rel (SqlVal Int × Bool × SqlVal Int) := [
    (some 1, true, some 100), (some 1, false, some 101), (some 2, true, some 200), (none, true, some 999)
  ]
  let tpch13Slow := groupBy (fun p => p.1)
    (fun _ b => sqlCountCol (fun p => match p.2 with | some o => o.2.2 | none => none) b)
    (leftOuterJoin (fun c o => SqlBool.and (sqlEq c o.1) (if o.2.1 then .tTrue else .tFalse)) tpch13Cust tpch13Ord)
  let tpch13OrdAgg := groupBy (fun o => o.1) (fun _ b => sqlCountCol (fun o => o.2.2) b)
    (tpch13Ord.filter (fun o => o.2.1 && o.1.isSome))
  let tpch13Fast := (leftOuterJoin (fun c kv => sqlEq c kv.1) tpch13Cust tpch13OrdAgg).map
    (fun p => (p.1, match p.2 with | some kv => kv.2 | none => 0))
  IO.println s!"  ✅ Q13 LEFT JOIN 先行集約 + 内側 GROUP BY 消去: {repr tpch13Slow} == {repr tpch13Fast} ({tpch13Slow == tpch13Fast})"

  -- (TPCH-3) Q18: 自己準結合 IN (SELECT ... GROUP BY HAVING SUM > 230) の 1 パス吸収
  let tpch18Ord : Rel (String × SqlVal Int) := [("CustA", some 10), ("CustB", some 20)]
  let tpch18Line : Rel (SqlVal Int × SqlVal Int) := [
    (some 10, some 150), (some 10, some 100), (some 20, some 80)
  ]
  let tpch18Sub := (groupBy Prod.fst (fun _ b => sqlSum Prod.snd b)
    (tpch18Line.filter (fun l => l.1.isSome))).filter (fun kv => match kv.2 with | some s => s > 230 | none => false)
  let tpch18Orig := groupBy (fun p => p.1)
    (fun _ b => sqlSum (fun p => p.2.2) b)
    (tpch18Ord.flatMap (fun o =>
      if tpch18Sub.any (fun kv => (sqlEq o.2 kv.1).inWhere) then
        (tpch18Line.filter (fun l => (sqlEq o.2 l.1).inWhere)).map (fun l => (o, l))
      else []))
  let tpch18Abs := groupBy (fun p => p.1)
    (fun _ b => sqlSum (fun p => p.2.2) b)
    (tpch18Ord.flatMap (fun o => (tpch18Sub.filter (fun d => (sqlEq o.2 d.1).inWhere)).map (fun d => (o, d))))
  IO.println s!"  ✅ Q18 自己準結合 HAVING SUM 1パス吸収: {repr tpch18Orig} == {repr tpch18Abs} ({tpch18Orig == tpch18Abs})"

  -- (TPCH-4) Q15: CTE `val = (SELECT MAX(val) FROM R)` -> `IS NOT NULL` 付き WITH TIES (+ 全NULL罠)
  let tpch15AllNull : Rel (Int × SqlVal Int) := [(1, none), (2, none)]
  let tpch15OrigNull := select (fun r => sqlEq r.2 (sqlMax Prod.snd tpch15AllNull)) tpch15AllNull
  let tpch15SafeNull := (tpch15AllNull.filter (fun r => r.2.isSome))
  let tpch15BuggyTies := tpch15AllNull.filter (fun r => tpch15AllNull.all (fun r2 =>
    match r.2, r2.2 with | none, none => true | some v, some v2 => decide (v2 ≤ v) | some _, none => true | none, some _ => false))
  IO.println s!"  ✅ Q15 全NULL時 MAX 比較: 本来={repr tpch15OrigNull} == IS NOT NULL ガード有={repr tpch15SafeNull} vs ❌ ガード無 WITH TIES={repr tpch15BuggyTies}"

  -- (TPCH-6) Q20: 相関 `avail > SUM(...)` の INNER JOIN 相関除去 vs ❌ Kim の COUNT(*) バグ
  let tpch20Ps : Rel (SqlVal Int × SqlVal Int) := [(some 1, some 50), (some 2, some 50)]
  let tpch20L : Rel (SqlVal Int × SqlVal Int) := [(some 1, some 20)] -- key=2 は 0 件一致！
  let tpch20CorrSum := tpch20Ps.filter (fun ps =>
    let m := tpch20L.filter (fun l => (sqlEq ps.1 l.1).inWhere)
    (sqlGtInt ps.2 (sqlSum Prod.snd m)).inWhere)
  let tpch20LAgg := groupBy Prod.fst (fun _ b => sqlSum Prod.snd b) tpch20L
  let tpch20InnerSum := tpch20Ps.flatMap (fun ps =>
    (tpch20LAgg.filter (fun kv => (sqlEq ps.1 kv.1).inWhere && (sqlGtInt ps.2 kv.2).inWhere)).map (fun _ => ps))
  let tpch20CorrCnt := tpch20Ps.filter (fun ps =>
    let cnt := sqlCountStar (tpch20L.filter (fun l => (sqlEq ps.1 l.1).inWhere))
    (sqlGtInt ps.2 (some cnt)).inWhere)
  IO.println s!"  ✅ Q20 相関 SUM 不等号 -> INNER JOIN 相関除去: {repr tpch20CorrSum} == {repr tpch20InnerSum} ({tpch20CorrSum == tpch20InnerSum})"
  IO.println s!"  ❌ もし COUNT(*) だった場合 (key=2 の 0件一致行が生き残る Kim の罠): {repr tpch20CorrCnt}"
  IO.println ""

  IO.println "=== [13] ClickBench 第2弾（CB-7..CB-12: 商バケット刈り込み・Markov HAVING・アフィン和・ボトム除外下界）==="
  -- (CB-7) Q34/Q35/Q17/Q19: 商ハッシュバケット上界による Heavy-Hitter 厳密刈り込み
  let cb7Rows : Rel String := ["u1", "u1", "u1", "u1", "u1", "u2", "u2", "u2", "u2", "rareA", "rareB"]
  let cb7Bucket : String → Int := fun s => if s == "u1" then 0 else if s == "u2" then 1 else 2
  let cb7CandBuckets : List Int := [0, 1]
  let cb7Pruned := cb7Rows.filter (fun s => cb7CandBuckets.contains (cb7Bucket s))
  let u1P := groupCount id cb7Pruned "u1"
  let u1F := groupCount id cb7Rows "u1"
  let u2P := groupCount id cb7Pruned "u2"
  let u2F := groupCount id cb7Rows "u2"
  IO.println s!"  ✅ CB-7 商バケット Heavy-Hitter 刈り込み: u1={u1P}/{u1F}, u2={u2P}/{u2F}"

  -- (CB-9) Q30: アフィン線形和 `SUM(x + c) = SUM(x) + c * COUNT(x)` (+ 空NULL保存)
  let cb9Rows : List (SqlVal Int) := [some 10, none, some 20, some 30]
  let cb9Direct := sqlSum (fun r => sqlAddOpt r (some 7)) cb9Rows
  let cb9Affine := sqlAddOpt (sqlSum id cb9Rows) (sqlMulOpt (some 7) (some (sqlCountNonNull cb9Rows)))
  let cb9Empty : List (SqlVal Int) := [none, none]
  let cb9EmptyDirect := sqlSum (fun r => sqlAddOpt r (some 7)) cb9Empty
  let cb9EmptyAffine := sqlAddOpt (sqlSum id cb9Empty) (sqlMulOpt (some 7) (some (sqlCountNonNull cb9Empty)))
  IO.println s!"  ✅ CB-9 アフィン線形和準同型: 通常={repr cb9Direct}=={repr cb9Affine}, 全NULL={repr cb9EmptyDirect}=={repr cb9EmptyAffine}"

  -- (CB-12) Q26: ボトム元 `""` 除外による Zone-Map 非空下界引き締め
  let cb12Blk : List String := ["", "zebra", "yak"]
  let cb12Surv := cb12Blk.filter (fun s => s != "")
  let cb12AllGt := cb12Surv.all (fun s => s > "cat")
  IO.println s!"  ✅ CB-12 ボトム元除外下界 (cutoff=cat): 生存行={repr cb12Surv}, 全要素 > cat = {cb12AllGt}"
