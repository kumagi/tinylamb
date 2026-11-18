import TinylambOpt.AggJoinDiscovery

/-
# Step 5: `AVG / MIN / MAX` × `LEFT JOIN` × `HAVING` × `DISTINCT` 集約の網羅的検証と新ルール探索

このモジュールでは、残る実戦的な集約パターンを一気に網羅します：
1. **`AVG / MIN / MAX` の多段分解**: 丸め誤差ゼロの有理平均モデルと M:N `InnerJoin` 越しの 2 段集約
2. **`LEFT OUTER JOIN` × `AVG / MIN / MAX`**: 右側（NULL 補完側）事前集約 ＆ 左側（保存側）事前集約の両方向定理と内包 `COALESCE` の罠
3. **`DISTINCT` 集約 (`MIN/MAX/COUNT/SUM/AVG(DISTINCT)`)**:
   - `MIN(DISTINCT) = MIN` / `MAX(DISTINCT) = MAX` の無条件簡約（`CanMerge` 解禁根拠）
   - 3 キー事前重複排除による M:N Join 越しの `COUNT/SUM/AVG(DISTINCT)` 押し込み
   - 冪等集約条件下での **M:N `InnerJoin` → `SemiJoin` 縮退** と **Unique 制約不要の未使用 `LEFT JOIN` 完全消去**
4. **`HAVING` 句との相互作用とアグレッシブ変換**:
   - `HAVING MAX(x) > c` / `HAVING MIN(x) < c` のベーステーブル `WHERE` への貫通プッシュダウン（および複合集約で壊れる罠）
   - **`HAVING` の右集約 Null-Rejecting 性による `LEFT OUTER JOIN` → `INNER JOIN` 降格**（および `COUNT(*)` 同居時の境界条件）
-/

namespace TinylambOpt

/-!
================================================================================
## Section 1: `AVG / MIN / MAX` の厳密モデルと M:N Join 多段集約
================================================================================
-/

/-- 浮動小数点の丸め誤差に依存しない厳密な平均値表現（既約分数 `num / den`）。 -/
structure ExactRatio where
  num : Int
  den : Nat
  deriving DecidableEq, Repr

/-- 合計 `sum` と非 NULL 件数 `cnt` から平均値を算出（`cnt = 0` のときは SQL 仕様通り `none` = NULL）。 -/
def mkExactAvg (sum : Int) (cnt : Int) : Option ExactRatio :=
  if cnt ≤ 0 then
    none
  else
    let cNat := cnt.toNat
    let g := Int.gcd sum cNat
    some { num := sum / (g : Int), den := cNat / g }

/-- SQL の `AVG(expr)`: 非 NULL 要素の合計と非 NULL 件数から算出する。 -/
def sqlAvg {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : Option ExactRatio :=
  match sqlSum f r with
  | none   => none
  | some s => mkExactAvg s (sqlCountCol f r)

/-- **罠 1-1（`AVG` の 2 段目を「部分平均の平均」にするとバケットサイズ差で狂うバグ）**:
    バケット1 `[10]`（平均 10）と バケット2 `[20, 20, 20]`（平均 20）の全体平均は `70/4 = 35/2 (17.5)` だが、
    部分平均同士の平均を取ると `(10 + 20)/2 = 15/1` になり壊れる！ -/
theorem avg_of_avg_bug :
    let b1 : Rel (SqlVal Int) := [some 10]
    let b2 : Rel (SqlVal Int) := [some 20, some 20, some 20]
    let trueAvg := sqlAvg id (b1 ++ b2)
    -- 部分平均 10 と 20 を単純平均すると 15 になってしまう
    let naiveAvgOfAvg := sqlAvg id [some 10, some 20]
    trueAvg ≠ naiveAvgOfAvg := by
  decide

/-- **罠 1-2（`AVG(col)` を `SUM(col) / COUNT(*)` に分解すると `NULL` 行で分母が狂うバグ）**:
    `[some 10, none]` の本来の `AVG` は `10 / 1 = 10` だが、`COUNT(*)` で割ると `10 / 2 = 5` になる！ -/
theorem avg_div_by_count_star_null_bug :
    let r : Rel (SqlVal Int) := [some 10, none]
    let trueAvg := sqlAvg id r
    let buggyAvg := match sqlSum id r with
      | none => none
      | some s => mkExactAvg s (sqlCountStar r)
    trueAvg ≠ buggyAvg := by
  decide

/-- 2 つの可換モノイドの直積モノイド（複数の集約状態を 1 パスで同時に運ぶための構造）。 -/
def prodMonoid {M1 M2 : Type} (m1 : AggMonoid M1) (m2 : AggMonoid M2) : AggMonoid (M1 × M2) where
  zero := (m1.zero, m2.zero)
  op a b := (m1.op a.1 b.1, m2.op a.2 b.2)
  op_assoc a b c := Prod.ext (m1.op_assoc a.1 b.1 c.1) (m2.op_assoc a.2 b.2 c.2)
  op_comm a b := Prod.ext (m1.op_comm a.1 b.1) (m2.op_comm a.2 b.2)
  zero_op a := Prod.ext (m1.zero_op a.1) (m2.zero_op a.2)
  op_zero a := Prod.ext (m1.op_zero a.1) (m2.op_zero a.2)

private theorem aggFold_prod {Row M1 M2 : Type}
    (m1 : AggMonoid M1) (m2 : AggMonoid M2)
    (f1 : Row → M1) (f2 : Row → M2) (r : Rel Row) :
    aggFold (prodMonoid m1 m2) (fun x => (f1 x, f2 x)) r =
    (aggFold m1 f1 r, aggFold m2 f2 r) := by
  induction r with
  | nil => rfl
  | cons x xs ih =>
    simp only [aggFold_cons, ih]
    rfl

/-- `(AVG, MIN, MAX)` を計算するための 4 成分部分状態 `(sumCol, cntCol, minCol, maxCol)` モノイド。 -/
abbrev AvgMinMaxState := (SqlVal Int × Int) × (SqlVal Int × SqlVal Int)

def avgMinMaxMonoid : AggMonoid AvgMinMaxState :=
  prodMonoid (prodMonoid sumMonoid countMonoid) (prodMonoid minMonoid maxMonoid)

def extractAvgMinMaxState {Row : Type} (f : Row → SqlVal Int) (x : Row) : AvgMinMaxState :=
  ((f x, if (f x).isSome then 1 else 0), (f x, f x))

def finalizeAvgMinMax (st : AvgMinMaxState) : Option ExactRatio × SqlVal Int × SqlVal Int :=
  let avgVal := match st.1.1 with
    | none   => none
    | some s => mkExactAvg s st.1.2
  (avgVal, st.2.1, st.2.2)

theorem finalize_aggFold_eq_avg_min_max {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) :
    finalizeAvgMinMax (aggFold avgMinMaxMonoid (extractAvgMinMaxState f) r) =
    (sqlAvg f r, sqlMin f r, sqlMax f r) := by
  unfold avgMinMaxMonoid extractAvgMinMaxState
  rw [aggFold_prod, aggFold_prod, aggFold_prod]
  rfl

/-- `groupBy` の集約値に後処理関数 `post` を適用する変換補題。 -/
private theorem groupBy_post_perm {R1 R2 K S V : Type} [DecidableEq K]
    (k1 : R1 → K) (f1 : K → Rel R1 → S) (r1 : Rel R1)
    (k2 : R2 → K) (f2 : K → Rel R2 → S) (r2 : Rel R2)
    (post : S → V)
    (h : BagEq (groupBy k1 f1 r1) (groupBy k2 f2 r2)) :
    BagEq (groupBy k1 (fun k b => post (f1 k b)) r1)
          (groupBy k2 (fun k b => post (f2 k b)) r2) := by
  have hMap : ∀ {R : Type} (kf : R → K) (ff : K → Rel R → S) (rel : Rel R),
      groupBy kf (fun k b => post (ff k b)) rel =
      (groupBy kf ff rel).map (fun kv => (kv.1, post kv.2)) := by
    intro R kf ff rel
    unfold groupBy
    rw [List.map_map]
    rfl
  rw [hMap, hMap]
  exact h.map _

/-- **主定理 1-1 (`eager_avg_min_max_over_mn_join_sound`)**:
    対向側 `R` に Unique 制約が無い M:N 内部結合の上でも、
    左テーブル `L` を `(gL(L), L.k)` ごとに 4 成分状態 `(sum, cntCol, min, max)` へ事前集約し、
    `R` と結合後にマージしてから `AVG = sum / cntCol` を確定させる 2 段集約は、
    元の `(AVG(L.v), MIN(L.v), MAX(L.v))` と **全 NULL・重複パターンで 100% Bag 等価**！ -/
theorem eager_avg_min_max_over_mn_join_sound {L R K GL G : Type}
    [DecidableEq K] [DecidableEq GL] [DecidableEq G]
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (gL : L → GL) (gFinal : GL → R → G)
    (lv : L → SqlVal Int)
    (l : Rel L) (r : Rel R) :
    let lClean := l.filter (fun x => (lk x).isSome)
    let partialL := groupBy (fun x => (gL x, lk x))
      (fun _ b => aggFold avgMinMaxMonoid (extractAvgMinMaxState lv) b) lClean
    BagEq
      (groupBy (fun xy => gFinal (gL xy.1) xy.2)
               (fun _ b => (sqlAvg (fun xy => lv xy.1) b,
                            sqlMin (fun xy => lv xy.1) b,
                            sqlMax (fun xy => lv xy.1) b))
               (equiJoinPairs lk rk l r))
      (groupBy (fun py => gFinal py.1.1.1 py.2)
               (fun _ b => finalizeAvgMinMax (aggFold avgMinMaxMonoid (fun py => py.1.2) b))
               (equiJoinPairs (fun p => p.1.2) rk partialL r)) := by
  intro lClean partialL
  have hRaw := eager_group_agg_over_mn_join_sound
    avgMinMaxMonoid lk rk gL gFinal (extractAvgMinMaxState lv) l r
  have hPost := groupBy_post_perm _ _ _ _ _ _ finalizeAvgMinMax hRaw
  have hLHS :
      groupBy (fun xy : L × R => gFinal (gL xy.1) xy.2)
        (fun _ b => finalizeAvgMinMax (aggFold avgMinMaxMonoid (fun xy => extractAvgMinMaxState lv xy.1) b))
        (equiJoinPairs lk rk l r) =
      groupBy (fun xy : L × R => gFinal (gL xy.1) xy.2)
        (fun _ b => (sqlAvg (fun xy => lv xy.1) b,
                     sqlMin (fun xy => lv xy.1) b,
                     sqlMax (fun xy => lv xy.1) b))
        (equiJoinPairs lk rk l r) := by
    unfold groupBy
    congr 1; funext g
    change (g, finalizeAvgMinMax (aggFold avgMinMaxMonoid (extractAvgMinMaxState (fun xy : L × R => lv xy.1)) _)) = _
    rw [finalize_aggFold_eq_avg_min_max]
  rw [← hLHS]
  exact hPost

/-!
================================================================================
## Section 2: `LEFT OUTER JOIN` × `AVG / MIN / MAX`（右側事前集約 ＆ 左側事前集約の両方向）
================================================================================
-/

/-- **定理 2-1 (`eager_right_avg_min_max_left_join_sound` — `LEFT JOIN` 右側＝NULL 供給側の事前集約)**:
    `D LEFT JOIN S ON dk(d) = sk(s)` において右テーブル `S` を事前集約するとき、
    `AVG(S.v)`, `MIN(S.v)`, `MAX(S.v)` では未マッチ行 `(d, none)` の `S.v` が `NULL` になるため、
    `COUNT(*)` のときと違って未マッチ行にはそのままモノイド単位元 `avgMinMaxMonoid.zero`（`(none, 0), (none, none)`）
    を渡せばよく、0 件部署の `(NULL, NULL, NULL)` グループまで含めて **100% Bag 等価** になる！ -/
theorem eager_right_avg_min_max_left_join_sound {D S K GD : Type}
    [DecidableEq K] [DecidableEq GD]
    (dk : D → SqlVal K) (sk : S → SqlVal K)
    (sval : S → SqlVal Int) (gD : D → GD)
    (dRel : Rel D) (sRel : Rel S) :
    let sClean := sRel.filter (fun s => (sk s).isSome)
    let partialS := groupBy sk
      (fun _ b => aggFold avgMinMaxMonoid (extractAvgMinMaxState sval) b) sClean
    let origJoined := leftOuterJoin (fun d s => sqlEq (dk d) (sk s)) dRel sRel
    let optJoined  := leftOuterJoin (fun d p => sqlEq (dk d) p.1) dRel partialS
    BagEq
      (groupBy (fun ds => gD ds.1)
        (fun _ b => (sqlAvg (fun ds => ds.2.bind sval) b,
                     sqlMin (fun ds => ds.2.bind sval) b,
                     sqlMax (fun ds => ds.2.bind sval) b))
        origJoined)
      (groupBy (fun dp => gD dp.1)
        (fun _ b => finalizeAvgMinMax
          (aggFold avgMinMaxMonoid
            (fun dp => match dp.2 with | none => avgMinMaxMonoid.zero | some p => p.2) b))
        optJoined) := by
  intro sClean partialS origJoined optJoined
  -- 4 成分状態のレベルで `BagEq` を示し、`groupBy_post_perm` で `(AVG, MIN, MAX)` へ持ち上げる
  suffices hState :
      BagEq
        (groupBy (fun ds => gD ds.1)
          (fun _ b => aggFold avgMinMaxMonoid (extractAvgMinMaxState (fun ds : D × Option S => ds.2.bind sval)) b)
          origJoined)
        (groupBy (fun dp => gD dp.1)
          (fun _ b => aggFold avgMinMaxMonoid
            (fun dp => match dp.2 with | none => avgMinMaxMonoid.zero | some p => p.2) b)
          optJoined) by
    have hPost := groupBy_post_perm _ _ _ _ _ _ finalizeAvgMinMax hState
    have hLHS :
        groupBy (fun ds => gD ds.1)
          (fun _ b => finalizeAvgMinMax
            (aggFold avgMinMaxMonoid (extractAvgMinMaxState (fun ds : D × Option S => ds.2.bind sval)) b))
          origJoined =
        groupBy (fun ds => gD ds.1)
          (fun _ b => (sqlAvg (fun ds => ds.2.bind sval) b,
                       sqlMin (fun ds => ds.2.bind sval) b,
                       sqlMax (fun ds => ds.2.bind sval) b))
          origJoined := by
      unfold groupBy; congr 1; funext g
      dsimp only
      rw [finalize_aggFold_eq_avg_min_max]
    rw [← hLHS]; exact hPost
  apply groupBy_bagEq_of_keys_and_vals
  · -- (1) 左テーブル `D` のグループ集合は `LEFT OUTER JOIN` で完全に保存される
    intro g
    have hPres : ∀ {R' : Type} (onP : D → R' → SqlBool) (r' : Rel R'),
        (∃ dr ∈ leftOuterJoin onP dRel r', gD dr.1 = g) ↔ (∃ d ∈ dRel, gD d = g) := by
      intro R' onP r'
      constructor
      · rintro ⟨⟨d', optR⟩, hMem, hKey⟩
        simp only [leftOuterJoin, List.mem_flatMap] at hMem
        rcases hMem with ⟨d, hd, hIn⟩
        by_cases hE : (r'.filter (fun y => (onP d y).inWhere)).isEmpty
        · simp [hE] at hIn; exact ⟨d, hd, hIn.1 ▸ hKey⟩
        · simp [hE, List.mem_map] at hIn
          rcases hIn with ⟨_, _, hEq, _⟩
          exact ⟨d, hd, hEq.symm ▸ hKey⟩
      · rintro ⟨d, hd, hKey⟩
        cases hF : r'.filter (fun y => (onP d y).inWhere) with
        | nil =>
          refine ⟨(d, none), ?_, hKey⟩
          simp only [leftOuterJoin, List.mem_flatMap]; exact ⟨d, hd, by simp [hF]⟩
        | cons y0 ys =>
          refine ⟨(d, some y0), ?_, hKey⟩
          simp only [leftOuterJoin, List.mem_flatMap]; exact ⟨d, hd, by simp [hF]⟩
    unfold origJoined optJoined; rw [hPres, hPres]
  · -- (2) 各グループ `g` 内で、左の各行 `d` ごとの 4 成分状態寄与が一致！
    intro g _
    unfold origJoined optJoined
    rw [leftOuterJoin_filter_left _ (fun d => gD d == g),
        leftOuterJoin_filter_left _ (fun d => gD d == g)]
    unfold leftOuterJoin
    rw [aggFold_flatMap, aggFold_flatMap]
    congr 1; funext d
    dsimp only
    cases hdk : dk d with
    | none =>
      have hE1 : sRel.filter (fun s => (sqlEq none (sk s)).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]; intro s _; simp [sqlEq, SqlBool.inWhere]
      have hE2 : partialS.filter (fun p => (sqlEq none p.1).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]; intro p _; simp [sqlEq, SqlBool.inWhere]
      simp only [hE1, hE2, List.isEmpty_nil, ↓reduceIte, aggFold_cons, aggFold_nil]
      rfl
    | some kv =>
      let bucket := @List.filter S (fun s => @BEq.beq (SqlVal K) instBEqOfDecidableEq (sk s) (some kv)) sClean
      have hBucketEq : sRel.filter (fun s => (sqlEq (some kv) (sk s)).inWhere) = bucket := by
        unfold bucket sClean; rw [List.filter_filter]; apply List.filter_congr; intro s _
        cases hsk : sk s with
        | none =>
          have hne : (@BEq.beq (SqlVal K) instBEqOfDecidableEq none (some kv)) = false :=
            @beq_eq_false_iff_ne (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr (by simp)
          simp [sqlEq, SqlBool.inWhere, hne]
        | some kv' =>
          by_cases h : kv = kv'
          · subst h; simp [sqlEq, SqlBool.inWhere]
          · have h2 : kv' ≠ kv := Ne.symm h
            have hne : (@BEq.beq (SqlVal K) instBEqOfDecidableEq (some kv') (some kv)) = false :=
              @beq_eq_false_iff_ne (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr (by simp [h2])
            simp [sqlEq, SqlBool.inWhere, h, hne]
      have hPartEq :
          partialS.filter (fun p => (sqlEq (some kv) p.1).inWhere) =
          if some kv ∈ distinctKeys (sClean.map sk) then
            [(some kv, aggFold avgMinMaxMonoid (extractAvgMinMaxState sval) bucket)]
          else [] := by
        unfold partialS groupBy bucket
        exact filter_nodup_keys_eq_some
          (fun k => aggFold avgMinMaxMonoid (extractAvgMinMaxState sval)
            (@List.filter S (fun row => @BEq.beq (SqlVal K) instBEqOfDecidableEq (sk row) k) sClean))
          kv (distinctKeys (sClean.map sk)) (nodup_distinctKeys _)
      rw [hBucketEq, hPartEq]
      by_cases hMem : some kv ∈ distinctKeys (sClean.map sk)
      · have hNonEmpty : bucket.isEmpty = false := by
          rw [mem_distinctKeys, List.mem_map] at hMem
          rcases hMem with ⟨s0, hs0, hsk0⟩
          have hs0In : s0 ∈ bucket := by
            unfold bucket; rw [List.mem_filter]
            exact ⟨hs0, @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr hsk0⟩
          cases hL : bucket with
          | nil => rw [hL] at hs0In; cases hs0In
          | cons _ _ => rfl
        simp only [hMem, ↓reduceIte, hNonEmpty, Bool.false_eq_true, List.isEmpty_cons,
                   List.map, aggFold_cons, aggFold_nil]
        rw [avgMinMaxMonoid.op_zero, aggFold_map]
        rfl
      · have hIsNil : bucket = [] := by
          unfold bucket; rw [List.filter_eq_nil_iff]
          intro s hs hBeq
          have hEq := @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mp hBeq
          apply hMem; rw [mem_distinctKeys, List.mem_map]; exact ⟨s, hs, hEq⟩
        simp only [hMem, ↓reduceIte, hIsNil, List.isEmpty_nil, aggFold_cons, aggFold_nil]
        rfl

/-- **罠 2-1（`AVG(COALESCE(S.v, 0))` や `MIN(COALESCE(S.v, 0))` をそのまま事前集約すると未マッチ部署で狂うバグ！）**:
    `S.v` 自体ではなく `COALESCE(S.v, 0)` を集約している場合、未マッチ行 `(d, none)` は `NULL` ではなく **`0` 1件** として
    分母・最小値に参加するため、未マッチ時に単位元（無視）を渡すと `some 0` であるべき結果が `none` に化ける！ -/
theorem coalesce_inside_avg_min_left_join_trap :
    let dRel : Rel Int := [20] -- 売上 0 件の部署
    let sRel : Rel (Int × SqlVal Int) := []
    let joined := leftOuterJoin (fun d (s : Int × SqlVal Int) => sqlEq (some d) (some s.1)) dRel sRel
    -- 本来の `MIN(COALESCE(s.val, 0))`: `(20, none)` に対して `COALESCE(NULL, 0) = 0` なので `some 0`！
    let trueMin := sqlMin (fun ds => some ((ds.2.bind Prod.snd).getD 0)) joined
    -- 誤って未マッチを `minMonoid.zero` (`none`) で処理すると `none` になってしまう！
    let buggyMin : SqlVal Int := none
    trueMin ≠ buggyMin := by
  decide

/-- **定理 2-2 (`eager_left_avg_min_max_left_join_sound` — `LEFT JOIN` 左側＝保存側の事前集約)**:
    `L LEFT JOIN R ON lk(x) = rk(y)` の上で左列 `(AVG(L.v), MIN(L.v), MAX(L.v))` を `GROUP BY gFinal(gL(L), Option R)` するとき、
    `L.k` に `NULL` が大量に含まれていても、`R` が非 Unique（M:N）でも、
    **`L` を `(gL(L), L.k)` で事前集約してから `LEFT JOIN R` して再集約** すると **100% Bag 等価** になる！
    （※ 右側事前集約と違い、`L.k IS NULL` の行は `LEFT JOIN` で生き残るため**事前除去せず** `(gL, none)` バケットに集約するのが急所！） -/
theorem eager_left_avg_min_max_left_join_sound {L R K GL G : Type}
    [DecidableEq K] [DecidableEq GL] [DecidableEq G]
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (gL : L → GL) (gFinal : GL → Option R → G)
    (lv : L → SqlVal Int)
    (l : Rel L) (r : Rel R) :
    let partialL := groupBy (fun x => (gL x, lk x))
      (fun _ b => aggFold avgMinMaxMonoid (extractAvgMinMaxState lv) b) l
    let origJoined := leftOuterJoin (fun x y => sqlEq (lk x) (rk y)) l r
    let optJoined  := leftOuterJoin (fun p y => sqlEq p.1.2 (rk y)) partialL r
    BagEq
      (groupBy (fun xyr => gFinal (gL xyr.1) xyr.2)
        (fun _ b => (sqlAvg (fun xyr => lv xyr.1) b,
                     sqlMin (fun xyr => lv xyr.1) b,
                     sqlMax (fun xyr => lv xyr.1) b))
        origJoined)
      (groupBy (fun pyr => gFinal pyr.1.1.1 pyr.2)
        (fun _ b => finalizeAvgMinMax (aggFold avgMinMaxMonoid (fun pyr => pyr.1.2) b))
        optJoined) := by
  intro partialL origJoined optJoined
  -- `leftOuterJoin` は各左行 `x` に対して「1要素リスト `[none]` または `matches.map some`」という
  -- `pk = (gL x, lk x)` のみに依存する非空リスト `expand(lk x) : List (Option R)` との直積に他ならない！
  let expandR : SqlVal K → Rel (Option R) := fun k =>
    let ms := r.filter (fun y => (sqlEq k (rk y)).inWhere)
    if ms.isEmpty then [none] else ms.map some
  have hOrigEq : origJoined = l.flatMap (fun x => (expandR (lk x)).map (fun oy => (x, oy))) := by
    unfold origJoined leftOuterJoin expandR
    congr 1; funext x
    dsimp only
    cases (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).isEmpty
    · simp [List.map_map]
    · rfl
  have hOptEq : optJoined = partialL.flatMap (fun p => (expandR p.1.2).map (fun oy => (p, oy))) := by
    unfold optJoined leftOuterJoin expandR
    congr 1; funext p
    dsimp only
    cases (r.filter (fun y => (sqlEq p.1.2 (rk y)).inWhere)).isEmpty
    · simp [List.map_map]
    · rfl
  suffices hState :
      BagEq
        (groupBy (fun xyr => gFinal (gL xyr.1) xyr.2)
          (fun _ b => aggFold avgMinMaxMonoid (fun xyr => extractAvgMinMaxState lv xyr.1) b)
          origJoined)
        (groupBy (fun pyr => gFinal pyr.1.1.1 pyr.2)
          (fun _ b => aggFold avgMinMaxMonoid (fun pyr => pyr.1.2) b)
          optJoined) by
    have hPost := groupBy_post_perm _ _ _ _ _ _ finalizeAvgMinMax hState
    have hLHS :
        groupBy (fun xyr => gFinal (gL xyr.1) xyr.2)
          (fun _ b => finalizeAvgMinMax (aggFold avgMinMaxMonoid (fun xyr => extractAvgMinMaxState lv xyr.1) b))
          origJoined =
        groupBy (fun xyr => gFinal (gL xyr.1) xyr.2)
          (fun _ b => (sqlAvg (fun xyr => lv xyr.1) b,
                       sqlMin (fun xyr => lv xyr.1) b,
                       sqlMax (fun xyr => lv xyr.1) b))
          origJoined := by
      unfold groupBy; congr 1; funext g
      change (g, finalizeAvgMinMax (aggFold avgMinMaxMonoid (extractAvgMinMaxState (fun xyr : L × Option R => lv xyr.1)) _)) = _
      rw [finalize_aggFold_eq_avg_min_max]
    rw [← hLHS]; exact hPost
  rw [hOrigEq, hOptEq]
  apply groupBy_bagEq_of_keys_and_vals
  · -- (1) グループキー集合の一致
    intro g
    constructor
    · rintro ⟨⟨x, oy⟩, hMem, hKey⟩
      simp only [List.mem_flatMap, List.mem_map] at hMem
      rcases hMem with ⟨x', hx, oy', hoy, hPairEq⟩
      cases hPairEq
      rcases exists_mem_groupBy (fun row => (gL row, lk row))
        (fun _ b => aggFold avgMinMaxMonoid (extractAvgMinMaxState lv) b) l hx with ⟨st, hpMem⟩
      refine ⟨(((gL x, lk x), st), oy), ?_, hKey⟩
      simp only [List.mem_flatMap, List.mem_map]
      exact ⟨_, hpMem, oy, hoy, rfl⟩
    · rintro ⟨⟨pRow, oy⟩, hMem, hKey⟩
      simp only [List.mem_flatMap, List.mem_map] at hMem
      rcases hMem with ⟨p', hp, oy', hoy, hPairEq⟩
      cases hPairEq
      unfold partialL groupBy at hp
      rw [List.mem_map] at hp
      rcases hp with ⟨⟨gl, k⟩, hkMem, hpEq⟩
      rw [mem_distinctKeys, List.mem_map] at hkMem
      rcases hkMem with ⟨x, hx, hpkEq⟩
      cases hpEq; cases hpkEq
      refine ⟨(x, oy), ?_, hKey⟩
      simp only [List.mem_flatMap, List.mem_map]
      exact ⟨x, hx, oy, hoy, rfl⟩
  · -- (2) 各グループ `g` の状態一致: `expandR` に対するフィルタに直して `general_join_preagg_fold` をそのまま適用！
    intro g _
    have hF1 :
        (l.flatMap (fun x => (expandR (lk x)).map (fun oy => (x, oy)))).filter
          (fun xyr => gFinal (gL xyr.1) xyr.2 == g) =
        l.flatMap (fun x => ((expandR (lk x)).filter (fun oy => gFinal (gL x) oy == g)).map (fun oy => (x, oy))) := by
      rw [List.filter_flatMap]; congr 1; funext x
      induction (expandR (lk x)) with
      | nil => rfl
      | cons oy oys ih =>
        cases hg : (gFinal (gL x) oy == g) <;> simp [List.filter, hg, ih]
    have hF2 :
        (partialL.flatMap (fun p => (expandR p.1.2).map (fun oy => (p, oy)))).filter
          (fun pyr => gFinal pyr.1.1.1 pyr.2 == g) =
        partialL.flatMap (fun p => ((expandR p.1.2).filter (fun oy => gFinal p.1.1 oy == g)).map (fun oy => (p, oy))) := by
      rw [List.filter_flatMap]; congr 1; funext p
      induction (expandR p.1.2) with
      | nil => rfl
      | cons oy oys ih =>
        cases hg : (gFinal p.1.1 oy == g) <;> simp [List.filter, hg, ih]
    rw [hF1, hF2]
    -- `pk = (gL x, lk x)` ごとのバケット分解 + Fubini！
    let pkFn := fun x : L => (gL x, lk x)
    have hPerm := perm_distinctKey_buckets pkFn l
    rw [aggFold_flatMap, aggFold_perm avgMinMaxMonoid _ hPerm, aggFold_flatMap]
    unfold partialL groupBy
    rw [aggFold_flatMap]
    clear hPerm hF1 hF2 hOrigEq hOptEq
    induction (distinctKeys (l.map pkFn)) with
    | nil => rfl
    | cons pk pks ih =>
      simp only [List.map_cons, aggFold_cons, ih]
      congr 1
      rw [← aggFold_flatMap]
      let targetOys := (expandR pk.2).filter (fun oy => gFinal pk.1 oy == g)
      have hSame :
          (@List.filter L (fun row => @BEq.beq (GL × SqlVal K) instBEqOfDecidableEq (pkFn row) pk) l).flatMap
            (fun x => ((expandR (lk x)).filter (fun oy => gFinal (gL x) oy == g)).map (fun oy => (x, oy))) =
          (@List.filter L (fun row => @BEq.beq (GL × SqlVal K) instBEqOfDecidableEq (pkFn row) pk) l).flatMap
            (fun x => targetOys.map (fun oy => (x, oy))) := by
        apply flatMap_const_matches
          (fun x => (expandR (lk x)).filter (fun oy => gFinal (gL x) oy == g))
          targetOys Prod.mk
        intro x hx
        rw [List.mem_filter] at hx
        have hEq := @beq_iff_eq (GL × SqlVal K) instBEqOfDecidableEq _ _ _ |>.mp hx.2
        have h1 : gL x = pk.1 := congrArg Prod.fst hEq
        have h2 : lk x = pk.2 := congrArg Prod.snd hEq
        simp [targetOys, h1, h2]
      rw [hSame, aggFold_flatMap]
      have hInner :
          (fun x : L => aggFold avgMinMaxMonoid (fun xyr : L × Option R => extractAvgMinMaxState lv xyr.1)
            (targetOys.map (fun oy => (x, oy)))) =
          (fun x : L => aggFold avgMinMaxMonoid (fun _ => extractAvgMinMaxState lv x) targetOys) := by
        funext x; rw [aggFold_map]
      rw [hInner, aggFold_comm avgMinMaxMonoid (fun x _ => extractAvgMinMaxState lv x), aggFold_map]

/-!
================================================================================
## Section 3: `DISTINCT` 集約 (`MIN / MAX / COUNT / SUM / AVG (DISTINCT)`) の網羅検証
================================================================================

`tinylamb` の現行コード（`executor/detail/aggregate_accumulator.cpp:1312` や `plan/cascades.cpp:7034`）では、
`COUNT(DISTINCT)` 以外の `DISTINCT` 集約は部分マージ不可として一括除外されています。
しかし Lean で「値のサポート集合（どの非 NULL 値が 1 回以上出現したか）」に注目すると、
実は **通常の `SUM` / `AVG` よりも遥かにアグレッシブな Join 縮退・消去ルール** が成立することが分かります！
-/

def sqlCountDistinct {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : Int :=
  sqlCountStar (distinctKeys (r.filterMap f))

def sqlSumDistinct {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : SqlVal Int :=
  sqlSum some (distinctKeys (r.filterMap f))

def sqlAvgDistinct {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : Option ExactRatio :=
  sqlAvg some (distinctKeys (r.filterMap f))

def sqlMinDistinct {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : SqlVal Int :=
  sqlMin some (distinctKeys (r.filterMap f))

def sqlMaxDistinct {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : SqlVal Int :=
  sqlMax some (distinctKeys (r.filterMap f))

/-- 重複度に依存しない「冪等（Idempotent）集約 5 種セット」:
    `(MIN(x), MAX(x), COUNT(DISTINCT x), SUM(DISTINCT x), AVG(DISTINCT x))` -/
abbrev IdemAggTuple := SqlVal Int × SqlVal Int × Int × SqlVal Int × Option ExactRatio

def evalIdempotentAggs {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : IdemAggTuple :=
  (sqlMin f r, sqlMax f r, sqlCountDistinct f r, sqlSumDistinct f r, sqlAvgDistinct f r)

/-- `SemiJoin(onPred, l, r)`（`L ⋉ R`）:
    右テーブル `r` にマッチする行が 1 件以上存在する左行 `x` だけを（**複製せずに 1 回だけ**）残す。 -/
def semiJoin {L R : Type} (onPred : L → R → SqlBool) (l : Rel L) (r : Rel R) : Rel L :=
  l.filter fun x => r.any (fun y => (onPred x y).inWhere)

/-!
### 3-1. 半束（冪等可換モノイド）の基本補題と `MIN(DISTINCT) = MIN` / `MAX(DISTINCT) = MAX`
-/

private theorem idem_op_mem {α M : Type} (m : AggMonoid M)
    (hIdem : ∀ a : M, m.op a a = a) (g : α → M) (u : α) :
    ∀ ys : List α, u ∈ ys → m.op (g u) (aggFold m g ys) = aggFold m g ys
  | [], h => by cases h
  | y :: ys, h => by
    rw [List.mem_cons] at h
    cases h with
    | inl hEq =>
      subst hEq
      simp only [aggFold_cons, ← m.op_assoc, hIdem]
    | inr hMem =>
      simp only [aggFold_cons]
      rw [← m.op_assoc, m.op_comm (g u) (g y), m.op_assoc, idem_op_mem m hIdem g u ys hMem]

private theorem idem_aggFold_absorb {α M : Type} (m : AggMonoid M)
    (hIdem : ∀ a : M, m.op a a = a) (g : α → M) :
    ∀ (xs ys : List α), (∀ u ∈ xs, u ∈ ys) →
      m.op (aggFold m g xs) (aggFold m g ys) = aggFold m g ys
  | [], _, _ => m.zero_op _
  | x :: xs, ys, hSub => by
    have hx : x ∈ ys := hSub x (List.mem_cons_self ..)
    have ih := idem_aggFold_absorb m hIdem g xs ys (fun z hz => hSub z (List.mem_cons_of_mem x hz))
    simp only [aggFold_cons]
    rw [m.op_assoc, ih, idem_op_mem m hIdem g x ys hx]

/-- **冪等モノイドの台集合不変性**:
    `m.op a a = a` を満たす集約（`MIN`, `MAX`, `BOOL_AND`, `BOOL_OR`, `BIT_AND`, `BIT_OR`）は、
    2 つのリストに出現する要素集合が同じなら、重複回数や順序がどれだけ違っても必ず同じ値になる！ -/
theorem idem_aggFold_eq_of_mem_iff {α M : Type} (m : AggMonoid M)
    (hIdem : ∀ a : M, m.op a a = a) (g : α → M)
    {xs ys : List α} (hIff : ∀ u, u ∈ xs ↔ u ∈ ys) :
    aggFold m g xs = aggFold m g ys := by
  have h1 := idem_aggFold_absorb m hIdem g xs ys (fun u hu => (hIff u).mp hu)
  have h2 := idem_aggFold_absorb m hIdem g ys xs (fun u hu => (hIff u).mpr hu)
  rw [← h2, m.op_comm, h1]

private theorem aggFold_filterMap_id {Row : Type} (m : AggMonoid (SqlVal Int))
    (hZero : m.zero = none) (f : Row → SqlVal Int) :
    ∀ r : Rel Row, aggFold m f r = aggFold m some (r.filterMap f)
  | [] => rfl
  | x :: xs => by
    cases hx : f x with
    | none =>
      simp only [aggFold_cons, List.filterMap_cons_none hx, hx, ← hZero, m.zero_op]
      exact aggFold_filterMap_id m hZero f xs
    | some v =>
      simp only [aggFold_cons, List.filterMap_cons_some hx, hx]
      congr 1
      exact aggFold_filterMap_id m hZero f xs

private theorem minMonoid_idem (a : SqlVal Int) : minMonoid.op a a = a := by
  cases a with
  | none => rfl
  | some v => simp [minMonoid, mergeMin]

private theorem maxMonoid_idem (a : SqlVal Int) : maxMonoid.op a a = a := by
  cases a with
  | none => rfl
  | some v => simp [maxMonoid, mergeMax]

/-- **定理 3-1 (`min_max_distinct_elim_sound` — `MIN(DISTINCT)` / `MAX(DISTINCT)` の `DISTINCT` 無条件除去)**:
    どんなテーブル・NULL 混在でも `MIN(DISTINCT x) = MIN(x)` かつ `MAX(DISTINCT x) = MAX(x)`。
    これにより `tinylamb` の並列 `CanMerge()`・`aggregate_union_transpose`・Join 多段集約が即座に解禁できます！ -/
theorem min_max_distinct_elim_sound {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) :
    sqlMinDistinct f r = sqlMin f r ∧ sqlMaxDistinct f r = sqlMax f r := by
  constructor
  · unfold sqlMinDistinct sqlMin
    rw [aggFold_filterMap_id minMonoid rfl f r]
    exact idem_aggFold_eq_of_mem_iff minMonoid minMonoid_idem some (mem_distinctKeys _)
  · unfold sqlMaxDistinct sqlMax
    rw [aggFold_filterMap_id maxMonoid rfl f r]
    exact idem_aggFold_eq_of_mem_iff maxMonoid maxMonoid_idem some (mem_distinctKeys _)

/-- **冪等 5 種集約の台集合マスター補題 (`evalIdempotentAggs_eq_of_same_support`)**:
    2 つのバケット `b1`, `b2` に現れる非 NULL 値の集合が一致するなら、
    `(MIN, MAX, COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT))` の 5 つ組はすべて完全一致する！ -/
theorem evalIdempotentAggs_eq_of_same_support {R1 R2 : Type}
    (f1 : R1 → SqlVal Int) (b1 : Rel R1)
    (f2 : R2 → SqlVal Int) (b2 : Rel R2)
    (hSupp : ∀ v : Int, (∃ x ∈ b1, f1 x = some v) ↔ (∃ y ∈ b2, f2 y = some v)) :
    evalIdempotentAggs f1 b1 = evalIdempotentAggs f2 b2 := by
  have hMemFM : ∀ v : Int, v ∈ b1.filterMap f1 ↔ v ∈ b2.filterMap f2 := by
    intro v
    simp only [List.mem_filterMap]
    exact hSupp v
  have hPermDK : BagEq (distinctKeys (b1.filterMap f1)) (distinctKeys (b2.filterMap f2)) := by
    apply nodup_perm_of_iff (nodup_distinctKeys _) (nodup_distinctKeys _)
    intro v
    simp only [mem_distinctKeys]
    exact hMemFM v
  have hMin : sqlMin f1 b1 = sqlMin f2 b2 := by
    unfold sqlMin
    rw [aggFold_filterMap_id minMonoid rfl f1 b1,
        aggFold_filterMap_id minMonoid rfl f2 b2]
    exact idem_aggFold_eq_of_mem_iff minMonoid minMonoid_idem some hMemFM
  have hMax : sqlMax f1 b1 = sqlMax f2 b2 := by
    unfold sqlMax
    rw [aggFold_filterMap_id maxMonoid rfl f1 b1,
        aggFold_filterMap_id maxMonoid rfl f2 b2]
    exact idem_aggFold_eq_of_mem_iff maxMonoid maxMonoid_idem some hMemFM
  have hCntD : sqlCountDistinct f1 b1 = sqlCountDistinct f2 b2 :=
    aggFold_perm countMonoid _ hPermDK
  have hSumD : sqlSumDistinct f1 b1 = sqlSumDistinct f2 b2 :=
    aggFold_perm sumMonoid _ hPermDK
  have hAvgD : sqlAvgDistinct f1 b1 = sqlAvgDistinct f2 b2 := by
    unfold sqlAvgDistinct sqlAvg
    have hC : sqlCountCol some (distinctKeys (b1.filterMap f1)) =
              sqlCountCol some (distinctKeys (b2.filterMap f2)) :=
      aggFold_perm countMonoid _ hPermDK
    have hS : sqlSum some (distinctKeys (b1.filterMap f1)) =
              sqlSum some (distinctKeys (b2.filterMap f2)) := hSumD
    rw [hS, hC]
  unfold evalIdempotentAggs
  rw [hMin, hMax, hCntD, hSumD, hAvgD]

/-!
### 3-2. `DISTINCT` 集約の罠と 3 つの超アグレッシブ Join 書き換え定理
-/

/-- **罠 3-1（`COUNT(DISTINCT x)` の部分カウントを足し合わせると重複値で狂うバグ）**:
    部署 10 にも部署 20 にも商品 `x = 1` があるとき、同じ `region = 100` にまとめるとき部分カウントを足すと
    `1 + 1 = 2` になり、本来の `COUNT(DISTINCT x) = 1` と食い違う！ -/
theorem count_distinct_sum_partials_bug :
    let sales : Rel (Int × SqlVal Int) := [(10, some 1), (20, some 1)]
    let depts : Rel (Int × Int) := [(10, 100), (20, 100)]
    let joined := equiJoinPairs (fun s => some s.1) (fun d => some d.1) sales depts
    let trueCntD := sqlCountDistinct (fun sd => sd.1.2) joined
    let preByDept := groupBy Prod.fst (fun _ b => sqlCountDistinct Prod.snd b) sales
    let buggySumOfCntD := aggFold countMonoid (fun pd => pd.1.2)
      (equiJoinPairs (fun p => some p.1) (fun d => some d.1) preByDept depts)
    trueCntD ≠ buggySumOfCntD := by
  decide

/-- **定理 3-2 (`eager_dedup_distinct_agg_over_mn_join_sound` — 3 キー事前重複排除による M:N Join 押し込み)**:
    数値には潰せなくても、`L` を `(gL(L), L.k, L.x)` の 3 つ組で **Join 前に事前 `DISTINCT`（＋ `L.k IS NOT NULL`）** すれば、
    対向側 `R` が重複だらけの M:N 結合でも `(MIN, MAX, COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT))` が
    任意の `GROUP BY gFinal(gL, R)` のもとで **100% Bag 等価** になる！ -/
theorem eager_dedup_distinct_agg_over_mn_join_sound {L R K GL G : Type}
    [DecidableEq K] [DecidableEq GL] [DecidableEq G]
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (gL : L → GL) (gFinal : GL → R → G)
    (lv : L → SqlVal Int)
    (l : Rel L) (r : Rel R) :
    let lClean := l.filter (fun x => (lk x).isSome)
    -- Join 前に `(gL x, lk x, lv x)` のユニーク組だけに圧縮！
    let preDedupL : Rel (GL × SqlVal K × SqlVal Int) :=
      distinctKeys (lClean.map (fun x => (gL x, lk x, lv x)))
    BagEq
      (groupBy (fun xy => gFinal (gL xy.1) xy.2)
        (fun _ b => evalIdempotentAggs (fun xy => lv xy.1) b)
        (equiJoinPairs lk rk l r))
      (groupBy (fun ty => gFinal ty.1.1 ty.2)
        (fun _ b => evalIdempotentAggs (fun ty => ty.1.2.2) b)
        (equiJoinPairs (fun t => t.2.1) rk preDedupL r)) := by
  intro lClean preDedupL
  have hCleanEq : equiJoinPairs lk rk l r = equiJoinPairs lk rk lClean r :=
    equiJoinPairs_filter_left_isSome lk rk l r
  rw [hCleanEq]
  apply groupBy_bagEq_of_keys_and_vals
  · intro g
    constructor
    · rintro ⟨⟨x, y⟩, hMem, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter] at hMem
      rcases hMem with ⟨x', hxClean, y', ⟨hy, hMatch⟩, hEq⟩
      cases hEq
      have htMem : (gL x, lk x, lv x) ∈ preDedupL := by
        unfold preDedupL; rw [mem_distinctKeys, List.mem_map]; exact ⟨x, hxClean, rfl⟩
      refine ⟨((gL x, lk x, lv x), y), ?_, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨_, htMem, y, ⟨hy, hMatch⟩, rfl⟩
    · rintro ⟨⟨t, y⟩, hMem, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter] at hMem
      rcases hMem with ⟨t', htMem, y', ⟨hy, hMatch⟩, hEq⟩
      cases hEq
      unfold preDedupL at htMem
      rw [mem_distinctKeys, List.mem_map] at htMem
      rcases htMem with ⟨x, hxClean, htEq⟩
      cases htEq
      refine ⟨(x, y), ?_, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨x, hxClean, y, ⟨hy, hMatch⟩, rfl⟩
  · intro g _
    apply evalIdempotentAggs_eq_of_same_support
    intro v
    constructor
    · rintro ⟨⟨x, y⟩, hMem, hv⟩
      rw [List.mem_filter] at hMem
      rcases hMem with ⟨hInJ, hgEq⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter] at hInJ
      rcases hInJ with ⟨x', hxClean, y', ⟨hy, hMatch⟩, hPair⟩
      cases hPair
      have htMem : (gL x, lk x, lv x) ∈ preDedupL := by
        unfold preDedupL; rw [mem_distinctKeys, List.mem_map]; exact ⟨x, hxClean, rfl⟩
      refine ⟨((gL x, lk x, lv x), y), ?_, hv⟩
      rw [List.mem_filter]
      refine ⟨?_, hgEq⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨_, htMem, y, ⟨hy, hMatch⟩, rfl⟩
    · rintro ⟨⟨t, y⟩, hMem, hv⟩
      rw [List.mem_filter] at hMem
      rcases hMem with ⟨hInJ, hgEq⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter] at hInJ
      rcases hInJ with ⟨t', htMem, y', ⟨hy, hMatch⟩, hPair⟩
      cases hPair
      unfold preDedupL at htMem
      rw [mem_distinctKeys, List.mem_map] at htMem
      rcases htMem with ⟨x, hxClean, htEq⟩
      cases htEq
      refine ⟨(x, y), ?_, hv⟩
      rw [List.mem_filter]
      refine ⟨?_, hgEq⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨x, hxClean, y, ⟨hy, hMatch⟩, rfl⟩

/-- **罠 3-2（通常の `AVG(L.v)` は `InnerJoin` を `SemiJoin` に縮退させると偏った重複で壊れる反例！）**:
    `x = (key=1, val=10)` は `R` に 1 回マッチし、`x = (key=2, val=20)` は `R` に 3 回マッチするとき、
    本来の `InnerJoin` の `AVG` は `(10 + 20*3)/4 = 35/2` だが、`SemiJoin` にすると `(10 + 20)/2 = 15/1` になり壊れる！ -/
theorem avg_skewed_fanout_breaks_semijoin_bug :
    let l : Rel (Int × SqlVal Int) := [(1, some 10), (2, some 20)]
    let r : Rel Int := [1, 2, 2, 2]
    let onPred := fun (x : Int × SqlVal Int) (y : Int) => sqlEq (some x.1) (some y)
    sqlAvg (fun lr => lr.1.2) (innerJoin onPred l r) ≠
    sqlAvg Prod.snd (semiJoin onPred l r) := by
  decide

/-- **定理 3-3 (`inner_join_to_semijoin_idempotent_aggs_sound` — 冪等集約下の `InnerJoin` → `SemiJoin` 縮退！)**:
    `GROUP BY gL(L)` で集約が `(MIN, MAX, COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT))` のみ（`R` 列を出力しない）なら、
    **任意の結合条件 `onPred`（非等値結合でも！）かつ `R` に一切の Unique 制約が無くても**、
    `InnerJoin(L, R)` を **`SemiJoin(L, R)` に無条件で縮退** できる！ -/
theorem inner_join_to_semijoin_idempotent_aggs_sound {L R GL : Type} [DecidableEq GL]
    (onPred : L → R → SqlBool)
    (gL : L → GL) (lv : L → SqlVal Int)
    (l : Rel L) (r : Rel R) :
    BagEq
      (groupBy (fun lr => gL lr.1) (fun _ b => evalIdempotentAggs (fun lr => lv lr.1) b)
        (innerJoin onPred l r))
      (groupBy gL (fun _ b => evalIdempotentAggs lv b)
        (semiJoin onPred l r)) := by
  apply groupBy_bagEq_of_keys_and_vals
  · intro g
    constructor
    · rintro ⟨⟨x, _⟩, hMem, hKey⟩
      simp only [innerJoin, List.mem_flatMap, List.mem_map, List.mem_filter] at hMem
      rcases hMem with ⟨x', hx, y, ⟨hy, hMatch⟩, hEq⟩
      cases hEq
      refine ⟨x, ?_, hKey⟩
      simp only [semiJoin, List.mem_filter, List.any_eq_true]
      exact ⟨hx, y, hy, hMatch⟩
    · rintro ⟨x, hMem, hKey⟩
      simp only [semiJoin, List.mem_filter, List.any_eq_true] at hMem
      rcases hMem with ⟨hx, y, hy, hMatch⟩
      refine ⟨(x, some y), ?_, hKey⟩
      simp only [innerJoin, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨x, hx, y, ⟨hy, hMatch⟩, rfl⟩
  · intro g _
    apply evalIdempotentAggs_eq_of_same_support
    intro v
    constructor
    · rintro ⟨⟨x, _⟩, hMem, hv⟩
      rw [List.mem_filter] at hMem
      rcases hMem with ⟨hInJ, hgEq⟩
      simp only [innerJoin, List.mem_flatMap, List.mem_map, List.mem_filter] at hInJ
      rcases hInJ with ⟨x', hx, y, ⟨hy, hMatch⟩, hEq⟩
      cases hEq
      refine ⟨x, ?_, hv⟩
      rw [List.mem_filter]
      refine ⟨?_, hgEq⟩
      simp only [semiJoin, List.mem_filter, List.any_eq_true]
      exact ⟨hx, y, hy, hMatch⟩
    · rintro ⟨x, hMem, hv⟩
      rw [List.mem_filter] at hMem
      rcases hMem with ⟨hInS, hgEq⟩
      simp only [semiJoin, List.mem_filter, List.any_eq_true] at hInS
      rcases hInS with ⟨hx, y, hy, hMatch⟩
      refine ⟨(x, some y), ?_, hv⟩
      rw [List.mem_filter]
      refine ⟨?_, hgEq⟩
      simp only [innerJoin, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨x, hx, y, ⟨hy, hMatch⟩, rfl⟩

/-- **定理 3-4 (`unused_left_join_elim_idempotent_aggs_sound` — Unique 制約不要の `LEFT JOIN` 完全消去！)**:
    `tinylamb` の現行 `unused_left_join_elimination`（`docs/cascades_optimizer.md:332`）は、
    重複マッチによる行数増加を恐れて **右側キーに `UNIQUE / PK` 制約があるときだけ** `LEFT JOIN` を消去します。
    しかし上段が `GROUP BY gL(L)` ＋ 冪等集約 `(MIN, MAX, COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT))` なら、
    左の各行はマッチ 0 件でも 1 回・マッチ $m \ge 1$ 件でも $m$ 回（常に **1 回以上**！）現れるため、
    **`R` に Unique 制約が一切無くても、どんな `ON` 条件であっても `LEFT JOIN R` ごと丸ごと消去（`L` 単体へ置換）できる**！ -/
theorem unused_left_join_elim_idempotent_aggs_sound {L R GL : Type} [DecidableEq GL]
    (onPred : L → R → SqlBool)
    (gL : L → GL) (lv : L → SqlVal Int)
    (l : Rel L) (r : Rel R) :
    BagEq
      (groupBy (fun lr => gL lr.1) (fun _ b => evalIdempotentAggs (fun lr => lv lr.1) b)
        (leftOuterJoin onPred l r))
      (groupBy gL (fun _ b => evalIdempotentAggs lv b) l) := by
  -- 補助: `x ∈ l` は `leftOuterJoin onPred l r` の第1成分として必ず 1 回以上出現し、逆も成り立つ
  have hProjIff : ∀ x0 : L,
      (∃ oy : Option R, (x0, oy) ∈ leftOuterJoin onPred l r) ↔ x0 ∈ l := by
    intro x0
    constructor
    · rintro ⟨oy, hMem⟩
      simp only [leftOuterJoin, List.mem_flatMap] at hMem
      rcases hMem with ⟨x, hx, hIn⟩
      by_cases hE : (r.filter (fun y => (onPred x y).inWhere)).isEmpty
      · simp [hE] at hIn; exact hIn.1 ▸ hx
      · simp [hE, List.mem_map] at hIn
        rcases hIn with ⟨_, _, hEq, _⟩; exact hEq ▸ hx
    · intro hx0
      cases hF : r.filter (fun y => (onPred x0 y).inWhere) with
      | nil =>
        refine ⟨none, ?_⟩
        simp only [leftOuterJoin, List.mem_flatMap]
        exact ⟨x0, hx0, by simp [hF]⟩
      | cons y0 ys =>
        refine ⟨some y0, ?_⟩
        simp only [leftOuterJoin, List.mem_flatMap]
        exact ⟨x0, hx0, by simp [hF]⟩
  apply groupBy_bagEq_of_keys_and_vals
  · intro g
    constructor
    · rintro ⟨⟨x, oy⟩, hMem, hKey⟩
      exact ⟨x, (hProjIff x).mp ⟨oy, hMem⟩, hKey⟩
    · rintro ⟨x, hx, hKey⟩
      rcases (hProjIff x).mpr hx with ⟨oy, hMem⟩
      exact ⟨(x, oy), hMem, hKey⟩
  · intro g _
    apply evalIdempotentAggs_eq_of_same_support
    intro v
    constructor
    · rintro ⟨⟨x, oy⟩, hMem, hv⟩
      rw [List.mem_filter] at hMem
      exact ⟨x, List.mem_filter.mpr ⟨(hProjIff x).mp ⟨oy, hMem.1⟩, hMem.2⟩, hv⟩
    · rintro ⟨x, hMem, hv⟩
      rw [List.mem_filter] at hMem
      rcases (hProjIff x).mpr hMem.1 with ⟨oy, hInJ⟩
      exact ⟨(x, oy), List.mem_filter.mpr ⟨hInJ, hMem.2⟩, hv⟩

/-!
================================================================================
## Section 4: `HAVING` 句との相互作用と、`HAVING` 起点の超アグレッシブ変換
================================================================================
-/

/-- SQL の `HAVING h(group_key, agg_val)`: `GROUP BY` の出力行に対する三値論理フィルタ。 -/
def havingFilter {K V : Type} (h : K → V → SqlBool) (groups : Rel (K × V)) : Rel (K × V) :=
  select (fun kv => h kv.1 kv.2) groups

/-- **定理 4-1 (`having_preserves_bagEq`)**:
    任意の多段集約ルールで `GROUP BY` 出力が `BagEq` なら、その上にどんな複雑な `HAVING` 句
    （例: `HAVING AVG(x) > 100 AND MIN(x) < MAX(x)`）を載せても無条件で `BagEq` が保たれる！ -/
theorem having_preserves_bagEq {K V : Type}
    (h : K → V → SqlBool) {g1 g2 : Rel (K × V)} (hPerm : BagEq g1 g2) :
    BagEq (havingFilter h g1) (havingFilter h g2) :=
  hPerm.filter _

/-- **罠 4-1（`HAVING MIN(x) > c` を `WHERE x > c` に押し込むと幻影グループが生まれる大バグ！）**:
    グループ `(g=1, vals=[5, 20])` の本来の `MIN` は `5` なので `HAVING MIN(x) > 10` で **除外（0行）** されるべきだが、
    先に `WHERE x > 10` で `5` を消してしまうと残った `[20]` の `MIN` が `20 > 10` となって **通過してしまう**！ -/
theorem having_min_gt_pushed_to_where_bug :
    let r : Rel (Int × SqlVal Int) := [(1, some 5), (1, some 20)]
    let gt10 : SqlVal Int → SqlBool
      | some v => if v > 10 then .tTrue else .tFalse
      | none   => .tNull
    let trueQuery := havingFilter (fun _ m => gt10 m) (groupBy Prod.fst (fun _ b => sqlMin Prod.snd b) r)
    let buggyPushed := groupBy Prod.fst (fun _ b => sqlMin Prod.snd b) (select (fun x => gt10 x.2) r)
    trueQuery ≠ buggyPushed := by
  decide

/-- **罠 4-2（`HAVING MAX(x) > c` でも、同時に `AVG(x)` や `MIN(x)` を出力していると `WHERE` 押し込みで値が狂う！）**:
    グループ `(g=1, vals=[5, 20])` は `MAX = 20 > 10` なので生き残るが、先に `WHERE x > 10` で `5` を捨てると
    同じグループの `AVG(x)` が `25/2 (12.5)` から `20/1` に変質してしまう！ -/
theorem having_max_gt_breaks_sibling_avg_bug :
    let r : Rel (Int × SqlVal Int) := [(1, some 5), (1, some 20)]
    let gt10 : SqlVal Int → SqlBool
      | some v => if v > 10 then .tTrue else .tFalse
      | none   => .tNull
    let trueQuery := havingFilter (fun _ avm => gt10 avm.2)
      (groupBy Prod.fst (fun _ b => (sqlAvg Prod.snd b, sqlMax Prod.snd b)) r)
    let buggyPushed := groupBy Prod.fst (fun _ b => (sqlAvg Prod.snd b, sqlMax Prod.snd b))
      (select (fun x => gt10 x.2) r)
    trueQuery ≠ buggyPushed := by
  decide

/-- SQL の `v > c` 三値比較ヘルパ。 -/
def sqlGtConst (c : Int) : SqlVal Int → SqlBool
  | some v => if v > c then .tTrue else .tFalse
  | none   => .tNull

/-- 核補題: 単一バケット `b` について、`sqlMax f b` が `> c` であることは `f x > c` を満たす要素が存在することと同値であり、
    かつそのとき `f x ≤ c` や `NULL` の要素を事前に捨てても `sqlMax` の値は一切変わらない！ -/
private theorem sqlMax_gt_filter_lemma {Row : Type} (f : Row → SqlVal Int) (c : Int) :
    ∀ b : Rel Row,
      ((sqlGtConst c (sqlMax f b)).inWhere = true ↔
        ∃ x ∈ b, (sqlGtConst c (f x)).inWhere = true) ∧
      ((sqlGtConst c (sqlMax f b)).inWhere = true →
        sqlMax f (b.filter (fun x => (sqlGtConst c (f x)).inWhere)) = sqlMax f b)
  | [] => by simp [sqlGtConst, SqlBool.inWhere]
  | x :: xs => by
    obtain ⟨ihIff, ihEq⟩ := sqlMax_gt_filter_lemma f c xs
    cases hx : f x with
    | none =>
      have hxFalse : (sqlGtConst c none).inWhere = false := rfl
      refine ⟨?_, ?_⟩
      · simp only [sqlMax_cons, hx, mergeMax, ihIff, List.mem_cons]
        constructor
        · rintro ⟨y, hy, hGt⟩; exact ⟨y, Or.inr hy, hGt⟩
        · rintro ⟨y, rfl | hy, hGt⟩
          · rw [hx, hxFalse] at hGt; cases hGt
          · exact ⟨y, hy, hGt⟩
      · intro hGt
        simp only [sqlMax_cons, hx, mergeMax] at hGt ⊢
        simp only [List.filter, hx, hxFalse]
        exact ihEq hGt
    | some v =>
      by_cases hv : v > c
      · have hxTrue : (sqlGtConst c (some v)).inWhere = true := by
          simp [sqlGtConst, hv, SqlBool.inWhere]
        have hMaxTrue : (sqlGtConst c (mergeMax (some v) (sqlMax f xs))).inWhere = true := by
          cases hSub : sqlMax f xs with
          | none => simp [mergeMax, sqlGtConst, hv, SqlBool.inWhere]
          | some m2 =>
            have hge : v ≤ max v m2 := Int.le_max_left v m2
            have hgt : max v m2 > c := by omega
            simp [mergeMax, sqlGtConst, hgt, SqlBool.inWhere]
        refine ⟨?_, ?_⟩
        · simp only [sqlMax_cons, hx, hMaxTrue, true_iff, List.mem_cons]
          exact ⟨x, Or.inl rfl, by rw [hx]; exact hxTrue⟩
        · intro _
          simp only [sqlMax_cons, List.filter, hx, hxTrue]
          by_cases hSubGt : (sqlGtConst c (sqlMax f xs)).inWhere = true
          · rw [ihEq hSubGt]
          · have hFilterNil : xs.filter (fun y => (sqlGtConst c (f y)).inWhere) = [] := by
              rw [List.filter_eq_nil_iff]
              intro y hy hGt
              exact hSubGt (ihIff.mpr ⟨y, hy, hGt⟩)
            rw [hFilterNil, sqlMax_nil]
            cases hSub : sqlMax f xs with
            | none => rfl
            | some m2 =>
              rw [hSub] at hSubGt
              have hm2Le : m2 ≤ c := by
                by_cases h2 : m2 > c
                · simp [sqlGtConst, h2, SqlBool.inWhere] at hSubGt
                · omega
              have hMaxEqV : max v m2 = v := Int.max_eq_left (by omega)
              simp [mergeMax, hMaxEqV]
      · have hxFalse : (sqlGtConst c (some v)).inWhere = false := by
          simp [sqlGtConst, hv, SqlBool.inWhere]
        have hMaxIffSub :
            (sqlGtConst c (mergeMax (some v) (sqlMax f xs))).inWhere =
            (sqlGtConst c (sqlMax f xs)).inWhere := by
          cases hSub : sqlMax f xs with
          | none => simp [mergeMax, sqlGtConst, hv, SqlBool.inWhere]
          | some m2 =>
            by_cases hm2 : m2 > c
            · have hMaxEq : max v m2 = m2 := Int.max_eq_right (by omega)
              simp [mergeMax, hMaxEq, sqlGtConst, hm2, SqlBool.inWhere]
            · have hMaxLe : ¬ (max v m2 > c) := by
                have : max v m2 ≤ c := Int.max_le.mpr ⟨by omega, by omega⟩
                omega
              simp [mergeMax, sqlGtConst, hm2, hMaxLe, SqlBool.inWhere]
        refine ⟨?_, ?_⟩
        · simp only [sqlMax_cons, hx, hMaxIffSub, ihIff, List.mem_cons]
          constructor
          · rintro ⟨y, hy, hGt⟩; exact ⟨y, Or.inr hy, hGt⟩
          · rintro ⟨y, rfl | hy, hGt⟩
            · rw [hx, hxFalse] at hGt; cases hGt
            · exact ⟨y, hy, hGt⟩
        · intro hGt
          simp only [sqlMax_cons, hx] at hGt ⊢
          rw [hMaxIffSub] at hGt
          simp only [List.filter, hx, hxFalse]
          rw [ihEq hGt]
          cases hSub : sqlMax f xs with
          | none => rw [hSub] at hGt; cases hGt
          | some m2 =>
            rw [hSub] at hGt
            have hm2 : m2 > c := by
              by_cases h2 : m2 > c
              · exact h2
              · simp [sqlGtConst, h2, SqlBool.inWhere] at hGt
            have hMaxEq : max v m2 = m2 := Int.max_eq_right (by omega)
            simp [mergeMax, hMaxEq]

/-- **定理 4-2 (`having_max_gt_pushdown_to_where_sound` — `MAX` 単独クエリでの `HAVING` → 基底 `WHERE` 貫通押し込み！)**:
    `SELECT gFinal(gL(L), R), MAX(L.v) FROM L JOIN R ON L.k = R.k GROUP BY ... HAVING MAX(L.v) > c`
    のように出力集約が `MAX(L.v)` のみの場合、**`HAVING MAX(L.v) > c` を `GROUP BY` も `JOIN` も突き抜けて
    左ベーステーブルの `WHERE L.v > c`（インデックススキャン対象！）へ押し込んでも 100% Bag 等価** になる！ -/
theorem having_max_gt_pushdown_to_where_sound {L R K G : Type}
    [DecidableEq K] [DecidableEq G]
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (gFinal : L → R → G) (lv : L → SqlVal Int) (c : Int)
    (l : Rel L) (r : Rel R) :
    let origPlan := havingFilter (fun _ m => sqlGtConst c m)
      (groupBy (fun xy => gFinal xy.1 xy.2) (fun _ b => sqlMax (fun xy => lv xy.1) b)
        (equiJoinPairs lk rk l r))
    let pushedPlan := groupBy (fun xy => gFinal xy.1 xy.2) (fun _ b => sqlMax (fun xy => lv xy.1) b)
      (equiJoinPairs lk rk (select (fun x => sqlGtConst c (lv x)) l) r)
    BagEq origPlan pushedPlan := by
  intro origPlan pushedPlan
  -- `equiJoinPairs lk rk (select ... l) r` は `(equiJoinPairs lk rk l r).filter (lv xy.1 > c)` と一致！
  have hJoinPush :
      equiJoinPairs lk rk (select (fun x => sqlGtConst c (lv x)) l) r =
      (equiJoinPairs lk rk l r).filter (fun xy => (sqlGtConst c (lv xy.1)).inWhere) := by
    symm
    exact equiJoinPairs_filter_left_pred lk rk (fun x => (sqlGtConst c (lv x)).inWhere) l r
  let J := equiJoinPairs lk rk l r
  unfold origPlan pushedPlan havingFilter
  rw [hJoinPush]
  unfold select
  have hNodup1 :
      ((groupBy (fun xy : L × R => gFinal xy.1 xy.2) (fun _ b => sqlMax (fun xy => lv xy.1) b) J).filter
        (fun kv => (sqlGtConst c kv.2).inWhere)).Nodup :=
    (nodup_groupBy _ _ _).filter _
  have hNodup2 :
      (groupBy (fun xy : L × R => gFinal xy.1 xy.2) (fun _ b => sqlMax (fun xy => lv xy.1) b)
        (J.filter (fun xy => (sqlGtConst c (lv xy.1)).inWhere))).Nodup :=
    nodup_groupBy _ _ _
  apply nodup_perm_of_iff hNodup1 hNodup2
  rintro ⟨g, mVal⟩
  simp only [groupBy, List.mem_filter, List.mem_map, mem_distinctKeys]
  -- 2重フィルタの整理補題
  have hBucketComm :
      (J.filter (fun xy : L × R => (sqlGtConst c (lv xy.1)).inWhere)).filter
        (fun row => gFinal row.1 row.2 == g) =
      (J.filter (fun row : L × R => gFinal row.1 row.2 == g)).filter
        (fun xy => (sqlGtConst c (lv xy.1)).inWhere) := by
    rw [List.filter_filter, List.filter_filter]
    apply List.filter_congr; intro xy _; exact Bool.and_comm _ _
  let bG := J.filter (fun row : L × R => gFinal row.1 row.2 == g)
  obtain ⟨hIffG, hEqG⟩ := sqlMax_gt_filter_lemma (fun xy : L × R => lv xy.1) c bG
  constructor
  · rintro ⟨⟨g', ⟨xy0, hxy0, hg0⟩, hPair⟩, hGt⟩
    cases hPair
    change (sqlGtConst c (sqlMax (fun xy => lv xy.1) bG)).inWhere = true at hGt
    rcases hIffG.mp hGt with ⟨xy1, hxy1InBG, hxy1Gt⟩
    have hxy1Mem : xy1 ∈ bG := hxy1InBG
    rw [List.mem_filter, beq_iff_eq] at hxy1Mem
    refine ⟨g, ⟨xy1, ⟨hxy1Mem.1, hxy1Gt⟩, hxy1Mem.2⟩, ?_⟩
    rw [hBucketComm]
    change (g, sqlMax (fun xy => lv xy.1) (bG.filter _)) = (g, sqlMax (fun xy => lv xy.1) bG)
    rw [hEqG hGt]
  · rintro ⟨g', ⟨xy1, ⟨hxy1J, hxy1Gt⟩, hg1⟩, hPair⟩
    cases hPair
    have hxy1InBG : xy1 ∈ bG := by
      unfold bG; rw [List.mem_filter, beq_iff_eq]; exact ⟨hxy1J, hg1⟩
    have hGt : (sqlGtConst c (sqlMax (fun xy => lv xy.1) bG)).inWhere = true :=
      hIffG.mpr ⟨xy1, hxy1InBG, hxy1Gt⟩
    refine ⟨⟨g, ⟨xy1, hxy1J, hg1⟩, ?_⟩, ?_⟩
    · rw [hBucketComm]; change (g, sqlMax _ bG) = (g, sqlMax _ (bG.filter _)); rw [hEqG hGt]
    · rw [hBucketComm]; change (sqlGtConst c (sqlMax _ (bG.filter _))).inWhere = true
      rw [hEqG hGt]; exact hGt

/-- SQL の `v < c` 三値比較ヘルパ。 -/
def sqlLtConst (c : Int) : SqlVal Int → SqlBool
  | some v => if v < c then .tTrue else .tFalse
  | none   => .tNull

private def negSqlVal : SqlVal Int → SqlVal Int
  | some v => some (-v)
  | none   => none

private theorem negSqlVal_involutive (x : SqlVal Int) : negSqlVal (negSqlVal x) = x := by
  cases x with
  | none => rfl
  | some v => simp [negSqlVal]

private theorem sqlMin_eq_neg_sqlMax_neg {Row : Type} (f : Row → SqlVal Int) :
    ∀ b : Rel Row, sqlMin f b = negSqlVal (sqlMax (fun x => negSqlVal (f x)) b)
  | [] => rfl
  | x :: xs => by
    rw [sqlMin_cons, sqlMax_cons, sqlMin_eq_neg_sqlMax_neg f xs]
    cases hx : f x with
    | none => simp [negSqlVal, mergeMin, mergeMax]
    | some a =>
      cases hSub : sqlMax (fun y => negSqlVal (f y)) xs with
      | none => simp [negSqlVal, mergeMin, mergeMax]
      | some b =>
        simp only [negSqlVal, mergeMin, mergeMax]
        congr 1
        by_cases hab : a ≤ -b
        · rw [Int.min_eq_left hab, Int.max_eq_left (by omega)]; omega
        · rw [Int.min_eq_right (by omega), Int.max_eq_right (by omega)]

private theorem sqlLtConst_eq_gt_neg (c : Int) (v : SqlVal Int) :
    sqlLtConst c v = sqlGtConst (-c) (negSqlVal v) := by
  cases v with
  | none => rfl
  | some a =>
    have hIff : (a < c) ↔ (-a > -c) := by omega
    by_cases h : a < c
    · have h2 : -a > -c := hIff.mp h
      simp [sqlLtConst, sqlGtConst, negSqlVal, h, h2]
    · have h2 : ¬ (-a > -c) := fun hc => h (hIff.mpr hc)
      simp [sqlLtConst, sqlGtConst, negSqlVal, h, h2]

/-- **定理 4-2' (`having_min_lt_pushdown_to_where_sound` — 双対定理: `MIN` 単独クエリの `HAVING MIN(L.v) < c` 貫通押し込み！)**:
    符号反転の双対性 `MIN(x) = -MAX(-x)` により、`HAVING MIN(L.v) < c` も同様にベーステーブル `WHERE L.v < c` へ押し込める！ -/
theorem having_min_lt_pushdown_to_where_sound {L R K G : Type}
    [DecidableEq K] [DecidableEq G]
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (gFinal : L → R → G) (lv : L → SqlVal Int) (c : Int)
    (l : Rel L) (r : Rel R) :
    let origPlan := havingFilter (fun _ m => sqlLtConst c m)
      (groupBy (fun xy => gFinal xy.1 xy.2) (fun _ b => sqlMin (fun xy => lv xy.1) b)
        (equiJoinPairs lk rk l r))
    let pushedPlan := groupBy (fun xy => gFinal xy.1 xy.2) (fun _ b => sqlMin (fun xy => lv xy.1) b)
      (equiJoinPairs lk rk (select (fun x => sqlLtConst c (lv x)) l) r)
    BagEq origPlan pushedPlan := by
  intro origPlan pushedPlan
  have hDual := having_max_gt_pushdown_to_where_sound lk rk gFinal (fun x => negSqlVal (lv x)) (-c) l r
  have hMapPost := hDual.map (fun kv : G × SqlVal Int => (kv.1, negSqlVal kv.2))
  -- 左辺・右辺がそれぞれ `hMapPost` の左辺・右辺とリストとして完全一致することを確認
  have hL :
      ((havingFilter (fun _ m => sqlGtConst (-c) m)
        (groupBy (fun xy => gFinal xy.1 xy.2) (fun _ b => sqlMax (fun xy => negSqlVal (lv xy.1)) b)
          (equiJoinPairs lk rk l r))).map (fun kv => (kv.1, negSqlVal kv.2))) = origPlan := by
    unfold origPlan havingFilter select groupBy
    generalize distinctKeys ((equiJoinPairs lk rk l r).map (fun xy => gFinal xy.1 xy.2)) = ks
    induction ks with
    | nil => rfl
    | cons g gs ih =>
      simp only [List.map_cons]
      let b := (equiJoinPairs lk rk l r).filter (fun row => gFinal row.1 row.2 == g)
      have hMinMax : sqlMin (fun xy : L × R => lv xy.1) b =
                     negSqlVal (sqlMax (fun xy : L × R => negSqlVal (lv xy.1)) b) :=
        sqlMin_eq_neg_sqlMax_neg (fun xy : L × R => lv xy.1) b
      have hCond : (sqlLtConst c (sqlMin (fun xy : L × R => lv xy.1) b)).inWhere =
                   (sqlGtConst (-c) (sqlMax (fun xy : L × R => negSqlVal (lv xy.1)) b)).inWhere := by
        rw [hMinMax, sqlLtConst_eq_gt_neg, negSqlVal_involutive]
      cases hGt : (sqlGtConst (-c) (sqlMax (fun xy : L × R => negSqlVal (lv xy.1)) b)).inWhere
      · have hLt : (sqlLtConst c (sqlMin (fun xy : L × R => lv xy.1) b)).inWhere = false := by
          rw [hCond]; exact hGt
        dsimp only at ih ⊢
        simp only [List.filter, show (sqlGtConst (-c) (sqlMax (fun xy : L × R => negSqlVal (lv xy.1))
          ((equiJoinPairs lk rk l r).filter (fun row => gFinal row.1 row.2 == g)))).inWhere = false from hGt,
          show (sqlLtConst c (sqlMin (fun xy : L × R => lv xy.1)
          ((equiJoinPairs lk rk l r).filter (fun row => gFinal row.1 row.2 == g)))).inWhere = false from hLt, ih]
      · have hLt : (sqlLtConst c (sqlMin (fun xy : L × R => lv xy.1) b)).inWhere = true := by
          rw [hCond]; exact hGt
        dsimp only at ih ⊢
        simp only [List.filter, show (sqlGtConst (-c) (sqlMax (fun xy : L × R => negSqlVal (lv xy.1))
          ((equiJoinPairs lk rk l r).filter (fun row => gFinal row.1 row.2 == g)))).inWhere = true from hGt,
          show (sqlLtConst c (sqlMin (fun xy : L × R => lv xy.1)
          ((equiJoinPairs lk rk l r).filter (fun row => gFinal row.1 row.2 == g)))).inWhere = true from hLt,
          List.map_cons, ih]
        unfold b at hMinMax
        rw [hMinMax]
  have hR :
      ((groupBy (fun xy => gFinal xy.1 xy.2) (fun _ b => sqlMax (fun xy => negSqlVal (lv xy.1)) b)
        (equiJoinPairs lk rk (select (fun x => sqlGtConst (-c) (negSqlVal (lv x))) l) r)).map
        (fun kv => (kv.1, negSqlVal kv.2))) = pushedPlan := by
    have hSel : select (fun x => sqlGtConst (-c) (negSqlVal (lv x))) l =
                select (fun x => sqlLtConst c (lv x)) l := by
      unfold select; apply List.filter_congr; intro x _
      dsimp only
      rw [← sqlLtConst_eq_gt_neg]
    unfold pushedPlan
    rw [hSel]
    unfold groupBy
    rw [List.map_map]
    congr 1; funext g
    simp only [Function.comp]
    rw [← sqlMin_eq_neg_sqlMax_neg (fun xy : L × R => lv xy.1)]
  rw [← hL, ← hR]
  exact hMapPost

/-!
### 4-2. `HAVING` の右集約 Null-Rejecting 性による `LEFT OUTER JOIN` → `INNER JOIN` 降格！
-/

/-- **罠 4-3（`HAVING AVG(s.val) > 100` があっても、`COUNT(*)` が同居していて `GROUP BY` が粗いと `INNER JOIN` 降格で壊れる！）**:
    同じ `region = 100` に属する部署 1（売上 200）と部署 2（売上なし）があるとき、
    `HAVING AVG(s.val) > 100` は `AVG = 200` で通過するが、`INNER JOIN` に降格すると部署 2 が消えて
    同居していた `COUNT(*)` が `2` から `1` に減ってしまう！ -/
theorem having_demote_breaks_count_star_on_coarse_group :
    let depts : Rel (Int × Int) := [(1, 100), (2, 100)] -- 両方 region = 100
    let sales : Rel (Int × SqlVal Int) := [(1, some 200)] -- 部署 2 は未マッチ
    let onPred := fun (d : Int × Int) (s : Int × SqlVal Int) => sqlEq (some d.1) (some s.1)
    let hv := fun (_ : Int) (ca : Int × Option ExactRatio) =>
      match ca.2 with
      | some r => if r.num > 100 * (r.den : Int) then .tTrue else .tFalse
      | none   => .tNull
    let orig := havingFilter hv
      (groupBy (fun ds => ds.1.2)
        (fun _ b => (sqlCountStar b, sqlAvg (fun ds => ds.2.bind Prod.snd) b))
        (leftOuterJoin onPred depts sales))
    let buggyInner := havingFilter hv
      (groupBy (fun ds => ds.1.2)
        (fun _ b => (sqlCountStar b, sqlAvg (fun ds => ds.2.bind Prod.snd) b))
        (innerJoin onPred depts sales))
    orig ≠ buggyInner := by
  decide

/-- **定理 4-3 (`having_null_rejecting_demotes_left_join_sound` — `HAVING` による `LEFT JOIN` → `INNER JOIN` 降格定理！)**:
    `SELECT gD(d), AVG(s.v), MIN(s.v), MAX(s.v) FROM D LEFT JOIN S ON p(d, s) GROUP BY gD(d) HAVING h`
    のように集約対象が右テーブル列 `s.v` のみ（未マッチ NULL 行をスキップする集約のみ）であり、
    かつ **`HAVING` 条件 `h` が全 NULL 集約結果 `(none, none, none)` を弾く**
    （例: `HAVING AVG(s.v) > 100` や `HAVING MIN(s.v) IS NOT NULL`）ならば、
    **任意の結合条件 `onPred` かつ粗い `GROUP BY gD(d)` であっても `LEFT OUTER JOIN` を `INNER JOIN` へ無条件降格できる**！ -/
theorem having_null_rejecting_demotes_left_join_sound {D S GD : Type} [DecidableEq GD]
    (onPred : D → S → SqlBool)
    (gD : D → GD) (sval : S → SqlVal Int)
    (h : GD → (Option ExactRatio × SqlVal Int × SqlVal Int) → SqlBool)
    (hRejectAllNull : ∀ g : GD, (h g (none, none, none)).inWhere = false)
    (dRel : Rel D) (sRel : Rel S) :
    BagEq
      (havingFilter h
        (groupBy (fun ds => gD ds.1)
          (fun _ b => (sqlAvg (fun ds => ds.2.bind sval) b,
                       sqlMin (fun ds => ds.2.bind sval) b,
                       sqlMax (fun ds => ds.2.bind sval) b))
          (leftOuterJoin onPred dRel sRel)))
      (havingFilter h
        (groupBy (fun ds => gD ds.1)
          (fun _ b => (sqlAvg (fun ds => ds.2.bind sval) b,
                       sqlMin (fun ds => ds.2.bind sval) b,
                       sqlMax (fun ds => ds.2.bind sval) b))
          (innerJoin onPred dRel sRel))) := by
  -- 補助1: 任意の `dsList` から右側 `none` の行を除去したサブリストは `innerJoin` 側と一致し、
  -- かつ右側 `none` の行は `extractAvgMinMaxState` が単位元 `zero` なので `(AVG, MIN, MAX)` を一切変えない！
  have hDropNoneState : ∀ rows : Rel (D × Option S),
      aggFold avgMinMaxMonoid (extractAvgMinMaxState (fun ds => ds.2.bind sval)) rows =
      aggFold avgMinMaxMonoid (extractAvgMinMaxState (fun ds => ds.2.bind sval))
        (rows.filter (fun ds => ds.2.isSome)) := by
    intro rows
    induction rows with
    | nil => rfl
    | cons r0 rs ih =>
      cases hr0 : r0.2 with
      | none =>
        have hZ : extractAvgMinMaxState (fun ds : D × Option S => ds.2.bind sval) r0 = avgMinMaxMonoid.zero := by
          simp only [extractAvgMinMaxState, hr0, Option.bind, Option.isSome, Bool.false_eq_true, ↓reduceIte]
          rfl
        simp only [aggFold_cons, hZ, avgMinMaxMonoid.zero_op, List.filter, hr0, Option.isSome]
        exact ih
      | some s =>
        simp only [aggFold_cons, List.filter, hr0, Option.isSome, ih]
  have hOuterFilterSomeEqInner : ∀ ds : Rel D,
      (leftOuterJoin onPred ds sRel).filter (fun dr => dr.2.isSome) =
      innerJoin onPred ds sRel := by
    intro ds
    induction ds with
    | nil => rfl
    | cons d ds ih =>
      unfold leftOuterJoin innerJoin at ih ⊢
      simp only [List.flatMap_cons, List.filter_append, ih]
      congr 1
      cases hM : sRel.filter (fun s => (onPred d s).inWhere) with
      | nil => simp [List.filter]
      | cons s0 ss =>
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte]
        rw [List.filter_eq_self]
        intro dr hMem; rw [List.mem_map] at hMem
        rcases hMem with ⟨_, _, rfl⟩; rfl
  -- 両辺ともキー `g` が `Nodup` なので、`nodup_perm_of_iff` で要素完全一致を示す
  let lhs := havingFilter h
    (groupBy (fun ds => gD ds.1)
      (fun _ b => (sqlAvg (fun ds => ds.2.bind sval) b,
                   sqlMin (fun ds => ds.2.bind sval) b,
                   sqlMax (fun ds => ds.2.bind sval) b))
      (leftOuterJoin onPred dRel sRel))
  let rhs := havingFilter h
    (groupBy (fun ds => gD ds.1)
      (fun _ b => (sqlAvg (fun ds => ds.2.bind sval) b,
                   sqlMin (fun ds => ds.2.bind sval) b,
                   sqlMax (fun ds => ds.2.bind sval) b))
      (innerJoin onPred dRel sRel))
  have hNodL : lhs.Nodup := (nodup_groupBy _ _ _).filter _
  have hNodR : rhs.Nodup := (nodup_groupBy _ _ _).filter _
  apply nodup_perm_of_iff hNodL hNodR
  rintro ⟨g, res⟩
  unfold lhs rhs havingFilter select groupBy
  simp only [List.mem_filter, List.mem_map, mem_distinctKeys]
  have hBucketSome :
      ((leftOuterJoin onPred dRel sRel).filter (fun row => gD row.1 == g)).filter (fun ds => ds.2.isSome) =
      (innerJoin onPred dRel sRel).filter (fun row => gD row.1 == g) := by
    have hSwap :
        ((leftOuterJoin onPred dRel sRel).filter (fun row => gD row.1 == g)).filter (fun ds => ds.2.isSome) =
        ((leftOuterJoin onPred dRel sRel).filter (fun ds => ds.2.isSome)).filter (fun row => gD row.1 == g) := by
      rw [List.filter_filter, List.filter_filter]
      apply List.filter_congr; intro a _; exact Bool.and_comm _ _
    rw [hSwap, hOuterFilterSomeEqInner]
  have hEvalEq :
      (sqlAvg (fun ds : D × Option S => ds.2.bind sval)
         ((leftOuterJoin onPred dRel sRel).filter (fun row => gD row.1 == g)),
       sqlMin (fun ds : D × Option S => ds.2.bind sval)
         ((leftOuterJoin onPred dRel sRel).filter (fun row => gD row.1 == g)),
       sqlMax (fun ds : D × Option S => ds.2.bind sval)
         ((leftOuterJoin onPred dRel sRel).filter (fun row => gD row.1 == g))) =
      (sqlAvg (fun ds : D × Option S => ds.2.bind sval)
         ((innerJoin onPred dRel sRel).filter (fun row => gD row.1 == g)),
       sqlMin (fun ds : D × Option S => ds.2.bind sval)
         ((innerJoin onPred dRel sRel).filter (fun row => gD row.1 == g)),
       sqlMax (fun ds : D × Option S => ds.2.bind sval)
         ((innerJoin onPred dRel sRel).filter (fun row => gD row.1 == g))) := by
    rw [← finalize_aggFold_eq_avg_min_max, ← finalize_aggFold_eq_avg_min_max,
        hDropNoneState, hBucketSome]
  constructor
  · rintro ⟨⟨g', ⟨dr0, hdr0, hg0⟩, hPair⟩, hHav⟩
    cases hPair
    cases hBIn : (innerJoin onPred dRel sRel).filter (fun row => gD row.1 == g) with
    | nil =>
      rw [hEvalEq, hBIn] at hHav
      have hZero : (sqlAvg (fun ds : D × Option S => ds.2.bind sval) [],
                    sqlMin (fun ds : D × Option S => ds.2.bind sval) [],
                    sqlMax (fun ds : D × Option S => ds.2.bind sval) []) = (none, none, none) := rfl
      rw [hZero, hRejectAllNull g] at hHav
      cases hHav
    | cons xIn xsIn =>
      have hxInMem : xIn ∈ (innerJoin onPred dRel sRel).filter (fun row => gD row.1 == g) := by
        simp [hBIn]
      rw [List.mem_filter, beq_iff_eq] at hxInMem
      refine ⟨⟨g, ⟨xIn, hxInMem.1, hxInMem.2⟩, ?_⟩, hHav⟩
      rw [hEvalEq]
  · rintro ⟨⟨g', ⟨xIn, hxIn, hgIn⟩, hPair⟩, hHav⟩
    cases hPair
    have hxOuter : xIn ∈ leftOuterJoin onPred dRel sRel := by
      rw [← hOuterFilterSomeEqInner] at hxIn
      exact (List.mem_filter.mp hxIn).1
    refine ⟨⟨g, ⟨xIn, hxOuter, hgIn⟩, ?_⟩, hHav⟩
    rw [← hEvalEq]

end TinylambOpt
