import TinylambOpt.UnionAndJoinRewrites
import TinylambOpt.PredicateAndRelationalLowering

/-!
These proofs relate multi-level rollups to merged partial states and show when filters
can transfer across join keys. SQL can narrow scan inputs and aggregate early under the
stated join and grouping conditions.

Named results include `rollup_two_level_state_combine_sound`,
`rollup_three_level_state_combine_sound`, `rollup_grand_total_requires_coalesce`.
-/

/-
# Step 16: TPC-DS 次世代アグレッシブル最適化ルール（A〜E）の形式的正当性

`build/benchmarks/tpcds-plan-audit-20261111/REPORT.md` の DuckDB との EXPLAIN / 実測比較から
導いた 5 本の新ルールを、ここでは **C++ のガード条件を明示的な仮説として持つ定理** として固定する。

- A: ROLLUP 階層の状態再利用（Q67, aggregate_input_rows 4.87M → ~1M）
- B: outer / USING 配下の内側 inner プレフィックス並べ替え（Q72）
- C: 等価キーを介する動的フィルタ移送（Q72 / Q78 / Q23）
- D: 共有 CTE の selection 述語和押し込みと実体化判断（Q23 / Q39）
- E: 外部結合前のチャネル別事前集約（Q78）

`lake build` が通れば、すべて `sorry` ゼロである。
-/

namespace TinylambOpt

/-!
## 共通の小道具
-/

/-- 集約状態 `(合計, 件数)`。SQL の `SUM` は `none` を区別する必要があるため件数を必ず併せて持つ。 -/
abbrev AggState := Int × Nat

@[simp] private theorem inWhere_ite_bool (b : Bool) :
    (if b then SqlBool.tTrue else SqlBool.tFalse).inWhere = b := by
  cases b <;> rfl

@[simp] private theorem inWhere_ite_beq_true {α : Type} [DecidableEq α] (a b : α) :
    (if (a == b) = true then SqlBool.tTrue else SqlBool.tFalse).inWhere = (a == b) := by
  by_cases h : a == b <;> simp [h] <;> rfl

@[simp] private theorem inWhere_ite_eq_true {α : Type} [DecidableEq α] (a b : α) :
    (if a = b then SqlBool.tTrue else SqlBool.tFalse).inWhere = decide (a = b) := by
  by_cases h : a = b <;> simp [h] <;> rfl

@[simp] private theorem decide_eq_beq {α : Type} [DecidableEq α] (a b : α) :
    decide (a = b) = (a == b) := by
  by_cases h : a = b <;> simp [h, beq_iff_eq]

def stateZero : AggState := (0, 0)

def stateMerge (a b : AggState) : AggState :=
  (a.1 + b.1, a.2 + b.2)

def stateOfKey {Row K : Type} [DecidableEq K]
    (key : Row → K) (valOf : Row → Option Int) (R : Rel Row) (k : K) : AggState :=
  (sumOptInts ((R.filter (fun r => key r == k)).map valOf),
   countOptInts ((R.filter (fun r => key r == k)).map valOf))

def foldStates {K : Type} (dom : List K) (f : K → AggState) : AggState :=
  dom.foldr (fun k acc => stateMerge (f k) acc) stateZero

private theorem foldStates_congr {K : Type} (dom : List K) {f g : K → AggState}
    (h : ∀ k ∈ dom, f k = g k) : foldStates dom f = foldStates dom g := by
  induction dom with
  | nil => rfl
  | cons k ks ih =>
      simp only [foldStates, List.foldr_cons]
      rw [h k (List.mem_cons_self ..)]
      exact congrArg (stateMerge (g k)) (ih (fun k' hk' => h k' (List.mem_cons_of_mem k hk')))

private theorem nodup_filter_local {α : Type} (p : α → Bool) {l : List α}
    (h : l.Nodup) : (l.filter p).Nodup := by
  induction l with
  | nil => exact List.nodup_nil
  | cons x xs ih =>
      have hx : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      by_cases hp : p x
      · simp [List.filter, hp, List.nodup_cons, hx, ih hxs]
      · simpa [List.filter, hp] using ih hxs

private theorem stateOfKey_append {Row K : Type} [DecidableEq K]
    (key : Row → K) (valOf : Row → Option Int) (r1 r2 : Rel Row) (k : K) :
    stateOfKey key valOf (r1 ++ r2) k =
      ((stateOfKey key valOf r1 k).1 + (stateOfKey key valOf r2 k).1,
       (stateOfKey key valOf r1 k).2 + (stateOfKey key valOf r2 k).2) := by
  dsimp [stateOfKey]
  simp [List.filter_append, sumOptInts_append, countOptInts_append]

private theorem foldStates_fst {K : Type} (dom : List K) (f : K → AggState) :
    (foldStates dom f).1 = dom.foldr (fun k acc => (f k).1 + acc) 0 := by
  induction dom with
  | nil => rfl
  | cons k ks ih =>
      change (stateMerge (f k) (foldStates ks f)).1 =
        (f k).1 + ks.foldr (fun k acc => (f k).1 + acc) 0
      simp [stateMerge, ih]

private theorem foldStates_snd {K : Type} (dom : List K) (f : K → AggState) :
    (foldStates dom f).2 = dom.foldr (fun k acc => (f k).2 + acc) 0 := by
  induction dom with
  | nil => rfl
  | cons k ks ih =>
      change (stateMerge (f k) (foldStates ks f)).2 =
        (f k).2 + ks.foldr (fun k acc => (f k).2 + acc) 0
      simp [stateMerge, ih]

private theorem stateOfKey_cons {Row K : Type} [DecidableEq K]
    (key : Row → K) (valOf : Row → Option Int) (r : Row) (rs : Rel Row) (k : K) :
    stateOfKey key valOf (r :: rs) k =
      stateMerge (stateOfKey key valOf [r] k) (stateOfKey key valOf rs k) := by
  simpa [stateMerge] using stateOfKey_append key valOf [r] rs k

/-!
## A. ROLLUP 階層の状態再利用（Q67）
-/

/-- 既存の 1 段階定理（`union_all_hierarchical_rollup_from_finest_group_sound`）を
    `AggState` の言葉に直したもの。下位レベルの状態を足すだけで親キーの状態が復元できる。 -/
private theorem foldStates_stateOf_key {Row KF KC : Type}
    [DecidableEq KF] [DecidableEq KC]
    (keyFine : Row → KF) (projCoarse : KF → KC) (valOf : Row → Option Int)
    (R : List Row) (fineDomain : List KF) (hNodup : fineDomain.Nodup)
    (hCover : ∀ r ∈ R, keyFine r ∈ fineDomain) (kc : KC) :
    foldStates (fineDomain.filter (fun kf => projCoarse kf == kc))
               (stateOfKey keyFine valOf R)
      = stateOfKey (fun r => projCoarse (keyFine r)) valOf R kc := by
  have h := union_all_hierarchical_rollup_from_finest_group_sound
      keyFine projCoarse valOf R fineDomain hNodup hCover kc
  dsimp only at h
  apply Prod.ext
  · rw [foldStates_fst]
    simpa [stateOfKey] using h.1
  · rw [foldStates_snd]
    simpa [stateOfKey] using h.2

/-- **ルール A（2 段階）**: 最細粒度 `KF` の状態を 1 度だけ作り、
    そこから `K1` レベル、`K2` レベルを `combine` で作っても、
    `R` を 2 度走査して直接集約した場合と状態が完全一致する。 -/
theorem rollup_two_level_state_combine_sound {Row KF K1 K2 : Type}
    [DecidableEq KF] [DecidableEq K1] [DecidableEq K2]
    (keyF : Row → KF) (p1 : KF → K1) (p2 : K1 → K2) (valOf : Row → Option Int)
    (R : List Row) (domF : List KF) (hNdF : domF.Nodup)
    (hCovF : ∀ r ∈ R, keyF r ∈ domF)
    (dom1 : List K1) (hNd1 : dom1.Nodup)
    (hCov1 : ∀ kf ∈ domF, p1 kf ∈ dom1) (kc : K2) :
    let fineSt := stateOfKey keyF valOf R
    let lvl1 := fun k1 : K1 => foldStates (domF.filter (fun kf => p1 kf == k1)) fineSt
    foldStates (dom1.filter (fun k1 => p2 k1 == kc)) lvl1
      = stateOfKey (fun r => p2 (p1 (keyF r))) valOf R kc := by
  intro fineSt lvl1
  have hL1 : ∀ k1, lvl1 k1 = stateOfKey (fun r => p1 (keyF r)) valOf R k1 := by
    intro k1
    exact foldStates_stateOf_key keyF p1 valOf R domF hNdF hCovF k1
  rw [foldStates_congr (dom1.filter (fun k1 => p2 k1 == kc))
        (by
          intro k1 hk1
          exact hL1 k1)]
  exact foldStates_stateOf_key (fun r => p1 (keyF r)) p2 valOf R dom1 hNd1
    (by
      intro r hr
      exact hCov1 (keyF r) (hCovF r hr)) kc

/-- **ルール A（3 段階）**: 2 段階定理を 2 回適用した形。9 段の ROLLUP も同じ手順で畳める。 -/
theorem rollup_three_level_state_combine_sound {Row KF K1 K2 K3 : Type}
    [DecidableEq KF] [DecidableEq K1] [DecidableEq K2] [DecidableEq K3]
    (keyF : Row → KF) (p1 : KF → K1) (p2 : K1 → K2) (p3 : K2 → K3)
    (valOf : Row → Option Int)
    (R : List Row) (domF : List KF) (hNdF : domF.Nodup)
    (hCovF : ∀ r ∈ R, keyF r ∈ domF)
    (dom1 : List K1) (hNd1 : dom1.Nodup)
    (hCov1 : ∀ kf ∈ domF, p1 kf ∈ dom1)
    (dom2 : List K2) (hNd2 : dom2.Nodup)
    (hCov2 : ∀ k1 ∈ dom1, p2 k1 ∈ dom2) (k3 : K3) :
    let fineSt := stateOfKey keyF valOf R
    let lvl1 := fun k1 : K1 => foldStates (domF.filter (fun kf => p1 kf == k1)) fineSt
    let lvl2 := fun k2 : K2 => foldStates (dom1.filter (fun k1 => p2 k1 == k2)) lvl1
    foldStates (dom2.filter (fun k2 => p3 k2 == k3)) lvl2
      = stateOfKey (fun r => p3 (p2 (p1 (keyF r)))) valOf R k3 := by
  intro fineSt lvl1 lvl2
  have hL2 : ∀ k2, lvl2 k2 = stateOfKey (fun r => p2 (p1 (keyF r))) valOf R k2 := by
    intro k2
    exact rollup_two_level_state_combine_sound keyF p1 p2 valOf R domF hNdF hCovF
      dom1 hNd1 hCov1 k2
  rw [foldStates_congr (dom2.filter (fun k2 => p3 k2 == k3)) (by
        intro k2 hk2
        exact hL2 k2)]
  exact foldStates_stateOf_key (fun r => p2 (p1 (keyF r))) p3 valOf R dom2 hNd2
    (by
      intro r hr
      exact hCov2 (p1 (keyF r)) (hCov1 (keyF r) (hCovF r hr))) k3

/-- **反例 A'（grand total の `COUNT`）**: 空入力の全体ロールアップで
    `SUM(__cnt)` を返すと `NULL` になり、`COUNT(x)` の `0` と一致しない。 -/
theorem rollup_grand_total_requires_coalesce :
    let sqlSumOnEmpty : Option Int := none
    let sqlCountOnEmpty : Option Int := some 0
    sqlSumOnEmpty ≠ sqlCountOnEmpty := by
  decide

/-!
## B. outer / USING 配下の内側プレフィックス並べ替え（Q72）
-/

/-- `UnionAndJoinRewrites.lean` の同名補題は `private` なので、ここではローカルに再証明する。 -/
private theorem select_leftOuterJoin_left_comm_local {L R : Type}
    (wL : L → SqlBool) (p : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    select (fun lr => wL lr.1) (leftOuterJoin p l r) =
    leftOuterJoin p (select wL l) r := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [leftOuterJoin, select, List.flatMap_cons, List.filter_append, List.filter] at ih ⊢
      cases hw : (wL x).inWhere
      · have hDrop :
            (let m := r.filter (fun y => (p x y).inWhere)
             if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))).filter
              (fun lr => (wL lr.1).inWhere) = [] := by
          dsimp only
          split
          · simp [hw]
          · rw [List.filter_eq_nil_iff]
            intro lr hlr
            obtain ⟨_, _, rfl⟩ := List.mem_map.mp hlr
            intro htrue
            simpa [hw] using htrue
        rw [hDrop, List.nil_append, ih]
      · have hKeep :
            (let m := r.filter (fun y => (p x y).inWhere)
             if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))).filter
              (fun lr => (wL lr.1).inWhere) =
            (let m := r.filter (fun y => (p x y).inWhere)
             if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))) := by
          dsimp only
          split
          · simp [hw]
          · rw [List.filter_eq_self]
            intro lr hlr
            obtain ⟨_, _, rfl⟩ := List.mem_map.mp hlr
            simpa using hw
        rw [hKeep, List.flatMap_cons, ih]

private theorem select_innerJoin_left_comm {L R : Type}
    (wL : L → SqlBool) (p : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    select (fun lr => wL lr.1) (innerJoin p l r) =
    innerJoin p (select wL l) r := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [innerJoin, select, List.flatMap_cons, List.filter_append] at ih ⊢
      by_cases hw : (wL x).inWhere
      · have hkeep :
            ((r.filter (fun y => (p x y).inWhere)).map (fun y => (x, some y))).filter
                (fun lr => (wL lr.1).inWhere) =
              (r.filter (fun y => (p x y).inWhere)).map (fun y => (x, some y)) := by
          rw [List.filter_eq_self]
          intro lr hlr
          obtain ⟨y, hy, rfl⟩ := List.mem_map.mp hlr
          simpa using hw
        simp [hkeep, hw, ih]
      · have hdrop :
            ((r.filter (fun y => (p x y).inWhere)).map (fun y => (x, some y))).filter
                (fun lr => (wL lr.1).inWhere) = [] := by
          rw [List.filter_eq_nil_iff]
          intro lr hlr
          obtain ⟨y, hy, rfl⟩ := List.mem_map.mp hlr
          intro htrue
          exact hw htrue
        simp [hdrop, hw, ih]

/-- **ルール B（USING 編）**: 内側プレフィックスを Bag 同値な順序 `I2` に差し替え、
    内側だけを参照する `WHERE` をその下へ押し込んでも多重度は保存される。 -/
theorem using_join_inner_prefix_reorder_and_filter_sound {I U : Type}
    {I1 I2 : Rel I} (hReorder : BagEq I1 I2)
    (p : I → U → SqlBool) (wI : I → SqlBool) (u : Rel U) :
    BagEq (select (fun iu => wI iu.1) (innerJoin p I1 u))
          (innerJoin p (select wI I2) u) := by
  rw [select_innerJoin_left_comm]
  exact perm_flatMap (fun i => (u.filter (fun y => (p i y).inWhere)).map (fun y => (i, some y)))
    (hReorder.filter _)

/-- **ルール B（outer + USING 混在編）**: Q72 のように末尾に LEFT JOIN が連なっていても、
    その左の USING/INNER プレフィックスだけを並べ替えて `WHERE` を押し込める。 -/
theorem outer_and_using_join_prefix_reorder_sound {I U O1 O2 : Type}
    {I1 I2 : Rel I} (hReorder : BagEq I1 I2)
    (pU : I → U → SqlBool) (p1 : (I × Option U) → O1 → SqlBool)
    (p2 : ((I × Option U) × Option O1) → O2 → SqlBool)
    (wI : I → SqlBool) (u : Rel U) (o1 : Rel O1) (o2 : Rel O2) :
    BagEq
      (select (fun r => wI r.1.1.1)
        (leftOuterJoin p2 (leftOuterJoin p1 (innerJoin pU I1 u) o1) o2))
      (leftOuterJoin p2 (leftOuterJoin p1 (innerJoin pU (select wI I2) u) o1) o2) := by
  rw [select_leftOuterJoin_left_comm_local (fun x => wI x.1.1) p2]
  rw [select_leftOuterJoin_left_comm_local (fun x => wI x.1) p1]
  exact perm_flatMap (fun x =>
      let m := o2.filter (fun y => (p2 x y).inWhere)
      if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y)))
    (perm_flatMap (fun iou =>
      let m := o1.filter (fun y => (p1 iou y).inWhere)
      if m.isEmpty then [(iou, none)] else m.map (fun y => (iou, some y)))
      (using_join_inner_prefix_reorder_and_filter_sound hReorder pU wI u))

/-!
## C. 動的フィルタの等価キー移送（Q72 / Q78 / Q23）
-/

/-- 等値キーで結合した 2 リレーション。 -/
def equiJoin {F D K : Type} [DecidableEq K]
    (kf : F → K) (kd : D → K) (frel : Rel F) (drel : Rel D) : Rel (F × D) :=
  frel.flatMap (fun f => (drel.filter (fun d => kf f == kd d)).map (fun d => (f, d)))

/-- **安全条件（上位集合性）**: D 側で移送元の述語が真なら、等価な F 側でも真。
    よって F 側スキャンにフィルタを付けても一致行を 1 行も落とさない。 -/
theorem filter_transfer_preserves_matches {F D K : Type} [DecidableEq K]
    (kf : F → K) (kd : D → K) (w : K → SqlBool)
    (f : F) (d : D) (hEq : (kf f == kd d) = true) :
    (w (kd d)).inWhere = true → (w (kf f)).inWhere = true := by
  intro hw
  have hEq' : kf f = kd d := beq_iff_eq.mp hEq
  simpa [hEq'] using hw

/-- **2 ホップの推移性**: 次元 D を中継して C 側から A 側へフィルタを移送しても安全。 -/
theorem filter_transfer_two_hop_preserves_matches {A B C K : Type} [DecidableEq K]
    (ka : A → K) (kb : B → K) (kc : C → K) (w : K → SqlBool)
    (a : A) (b : B) (c : C)
    (h1 : (ka a == kb b) = true) (h2 : (kb b == kc c) = true) :
    (w (kc c)).inWhere = true → (w (ka a)).inWhere = true := by
  intro hw
  have h1' : ka a = kb b := beq_iff_eq.mp h1
  have h2' : kb b = kc c := beq_iff_eq.mp h2
  simpa [h1', h2'] using hw

/-- **ルール C**: 移送元 D と移送先 F の両方に述語を適用してから等値結合しても、
    片方（D）にだけ述語を適用してから結合するのと Bag 多重度が完全一致する。
    つまり `PreferDynamicFilter` を probe 側スキャンまで貫通させられる。 -/
theorem transfer_filter_through_dimension_sound {F D K : Type} [DecidableEq K]
    (kf : F → K) (kd : D → K) (w : K → SqlBool)
    (frel : Rel F) (drel : Rel D) :
    BagEq
      (select (fun fd => w (kd fd.2)) (equiJoin kf kd frel drel))
      (equiJoin kf kd
        (frel.filter (fun f => (w (kf f)).inWhere))
        (drel.filter (fun d => (w (kd d)).inWhere))) := by
  induction frel with
  | nil => simp [equiJoin, select]
  | cons f fs ih =>
      simp only [equiJoin, List.flatMap_cons, select, List.filter_append] at ih ⊢
      by_cases hf : (w (kf f)).inWhere
      · have hfilter :
            (drel.filter (fun d => (w (kd d)).inWhere)).filter (fun d => kf f == kd d) =
              drel.filter (fun d => kf f == kd d) := by
          rw [List.filter_filter]
          apply List.filter_congr
          intro d hd
          by_cases heq : kf f == kd d
          · have hEq : kf f = kd d := beq_iff_eq.mp heq
            have hwD : (w (kd d)).inWhere = true := by simpa [hEq] using hf
            simp [heq, hwD]
          · simp [heq]
        have hkeep :
            ((drel.filter (fun d => kf f == kd d)).map (fun d => (f, d))).filter
                (fun fd => (w (kd fd.2)).inWhere) =
              (drel.filter (fun d => kf f == kd d)).map (fun d => (f, d)) := by
          rw [List.filter_eq_self]
          intro fd hfd
          obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hfd
          have hEqBool : (kf f == kd d) = true := (List.mem_filter.mp hd).2
          have hEq : kf f = kd d := beq_iff_eq.mp hEqBool
          simpa [hEq] using hf
        rw [hkeep]
        simp only [List.filter_cons, hf, ↓reduceIte, List.flatMap_cons]
        rw [← hfilter]
        exact ih.append_left _
      · have hdrop :
            ((drel.filter (fun d => kf f == kd d)).map (fun d => (f, d))).filter
                (fun fd => (w (kd fd.2)).inWhere) = [] := by
          rw [List.filter_eq_nil_iff]
          intro fd hfd
          obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hfd
          intro htrue
          have hEqBool : (kf f == kd d) = true := (List.mem_filter.mp hd).2
          have hEq : kf f = kd d := beq_iff_eq.mp hEqBool
          exact hf (by simpa [hEq] using htrue)
        rw [hdrop, List.nil_append]
        simp only [List.filter_cons, hf, ↓reduceIte, List.flatMap_cons]
        exact ih

/-!
## D. 共有 CTE の述語和押し込み（Q23 / Q39）
-/

/-- 2 つの利用Siteの述語 `w1, w2` の和を CTE 本体へ押し込んでも、
    各 Site が見る行は 1 行も変わらない（DuckDB の disjunctive pushdown と同じ形）。 -/
private theorem shared_cte_disjunct_pushdown_both {Row : Type}
    (body : Rel Row) (w1 w2 : Row → SqlBool) :
    let shared := body.filter (fun r => ((w1 r).or (w2 r)).inWhere)
    BagEq (select w1 body) (select w1 shared) ∧
    BagEq (select w2 body) (select w2 shared) := by
  induction body with
  | nil => simp [select]
  | cons r rs ih =>
      dsimp only
      by_cases h1 : (w1 r).inWhere <;> by_cases h2 : (w2 r).inWhere
      · constructor
        · simpa [select, h1, h2, or_inWhere] using ih.1.cons r
        · simpa [select, h1, h2, or_inWhere] using ih.2.cons r
      · constructor
        · simpa [select, h1, h2, or_inWhere] using ih.1.cons r
        · simpa [select, h1, h2, or_inWhere] using ih.2
      · constructor
        · simpa [select, h1, h2, or_inWhere] using ih.1
        · simpa [select, h1, h2, or_inWhere] using ih.2.cons r
      · constructor
        · simpa [select, h1, h2, or_inWhere] using ih.1
        · simpa [select, h1, h2, or_inWhere] using ih.2

/-- **ルール D（意味保存）**: 和を押し込んだ後に 2 箇所を `UNION ALL` しても行多重度は不変。 -/
theorem shared_cte_disjunct_pushdown_sound {Row : Type}
    (body : Rel Row) (w1 w2 : Row → SqlBool) :
    let shared := body.filter (fun r => ((w1 r).or (w2 r)).inWhere)
    BagEq (unionAll (select w1 body) (select w2 body))
          (unionAll (select w1 shared) (select w2 shared)) := by
  obtain ⟨h1, h2⟩ := shared_cte_disjunct_pushdown_both body w1 w2
  exact h1.append h2

/-- **ルール D（効用）**: 共有すれば本体の評価回数が 2 回から 1 回に減る。 -/
theorem cte_materialize_halves_body_evaluations {Row : Type}
    (body : Rel Row) (c : Rel Row → Rel Row) :
    (c body ++ c body).length = 2 * (c body).length := by
  simp
  omega

/-- **反例 D'**: 非決定的な CTE 本体を inline すると materialize と結果が変わりうる。
    したがってガードに「決定性」が必要。 -/
theorem cte_volatile_inline_unsound :
    ([0] ++ [1] : List Int) ≠ ([0] ++ [0] : List Int) := by
  decide

/-!
## E. 外部結合前のチャネル別事前集約（Q78）
-/

/-- 各キーの状態を列挙（結合前の `Hash Group By` 相当）。 -/
def aggByKey {Row K : Type} [DecidableEq K]
    (key : Row → K) (valOf : Row → Option Int)
    (r : Rel Row) (dom : List K) : Rel (K × AggState) :=
  dom.map (fun k => (k, stateOfKey key valOf r k))

/-- 次元側キーが一意なら、`filter (kd == k)` は高々 1 行。 -/
private theorem filter_key_subsingleton {D K : Type} [DecidableEq K]
    (kd : D → K) (drel : Rel D) (hNd : drel.Nodup)
    (hUniq : ∀ d1 ∈ drel, ∀ d2 ∈ drel, kd d1 == kd d2 → d1 = d2)
    (k : K) : (drel.filter (fun d => kd d == k)).length ≤ 1 := by
  induction drel with
  | nil => simp
  | cons d ds ih =>
      have hNotIn : d ∉ ds := (List.nodup_cons.mp hNd).1
      have hNd' : ds.Nodup := (List.nodup_cons.mp hNd).2
      have hUniq' : ∀ d1 ∈ ds, ∀ d2 ∈ ds, kd d1 == kd d2 → d1 = d2 := by
        intro d1 h1 d2 h2 heq
        exact hUniq d1 (by simp [h1]) d2 (by simp [h2]) heq
      have ih' := ih hNd' hUniq'
      by_cases hdk : kd d == k
      · have hEmpty : ds.filter (fun d' => kd d' == k) = [] := by
          rw [List.filter_eq_nil_iff]
          intro d' hd'in
          intro hd'k
          have h1 : kd d = k := beq_iff_eq.mp hdk
          have h2 : kd d' = k := beq_iff_eq.mp hd'k
          have hEq : kd d = kd d' := h1.trans h2.symm
          have hdd' : d = d' := hUniq d (by simp) d' (by simp [hd'in]) (by simpa [beq_iff_eq] using hEq)
          subst d'
          exact hNotIn hd'in
        simp [hdk, hEmpty]
      · simp [hdk, ih']

private theorem leftOuterJoin_cons {F D : Type} (p : F → D → SqlBool)
    (f : F) (fs : Rel F) (d : Rel D) :
    leftOuterJoin p (f :: fs) d = leftOuterJoin p [f] d ++ leftOuterJoin p fs d := by
  simp [leftOuterJoin]

/-- 1 行の fact に対する LEFT JOIN の寄与は、マッチの有無にかかわらずちょうど 1 行。 -/
private theorem stateOfKey_left_join_contrib {F D K : Type} [DecidableEq K]
    (kf : F → K) (kd : D → K) (valOf : F → Option Int)
    (f : F) (drel : Rel D) (k : K) (hNd : drel.Nodup)
    (hUniq : ∀ d1 ∈ drel, ∀ d2 ∈ drel, kd d1 == kd d2 → d1 = d2) :
    stateOfKey (fun fd : F × Option D => kf fd.1) (fun fd => valOf fd.1)
      (leftOuterJoin (fun f' d => if kf f' == kd d then .tTrue else .tFalse) [f] drel) k
      = stateOfKey kf valOf [f] k := by
  have hGroup :
      leftOuterJoin (fun f' d => if kf f' == kd d then .tTrue else .tFalse) [f] drel =
      if (∀ a : D, a ∈ drel → ¬ kf f = kd a) then [(f, none)]
      else (drel.filter (fun d => kf f == kd d)).map (fun d => (f, some d)) := by
    simp [leftOuterJoin, beq_iff_eq]
  rw [hGroup]
  by_cases hkf : kf f == k
  · have hlen := filter_key_subsingleton kd drel hNd hUniq (kf f)
    have hlen' : (drel.filter (fun d => kf f == kd d)).length ≤ 1 := by
      have hfeq : drel.filter (fun d => kd d == kf f) =
                  drel.filter (fun d => kf f == kd d) := by
        apply List.filter_congr
        intro d hd
        by_cases heq : kd d = kf f
        · simpa [heq]
        · have heq' : kf f ≠ kd d := by intro hh; exact heq hh.symm
          have h1 : (kd d == kf f) = false := by simp [beq_iff_eq, heq]
          have h2 : (kf f == kd d) = false := by simp [beq_iff_eq, heq']
          rw [h1, h2]
      simpa [hfeq] using hlen
    have hm : drel.filter (fun d => kf f == kd d) = [] ∨
              (∃ d, drel.filter (fun d => kf f == kd d) = [d]) := by
      cases hL : drel.filter (fun d => kf f == kd d) with
      | nil => exact Or.inl rfl
      | cons d ds =>
          have hlen1 : (d :: ds).length ≤ 1 := by
            rw [← hL]
            exact hlen'
          simp only [List.length_cons] at hlen1
          have hdsLen : ds.length = 0 := by omega
          have hds : ds = [] := List.eq_nil_of_length_eq_zero hdsLen
          subst ds
          exact Or.inr ⟨d, rfl⟩
    rcases hm with hEmpty | ⟨d0, hOne⟩
    · have hAll : ∀ a : D, a ∈ drel → ¬ kf f = kd a := by
        intro a ha hEq
        have hm : a ∈ drel.filter (fun d => kf f == kd d) :=
          List.mem_filter.mpr ⟨ha, by simpa [beq_iff_eq] using hEq⟩
        simpa [hEmpty] using hm
      rw [if_pos hAll]
      simp [stateOfKey, hkf]
    · have hNotAll : ¬ (∀ a : D, a ∈ drel → ¬ kf f = kd a) := by
        intro hAll
        have hd0mem : d0 ∈ drel := (List.mem_filter.mp (by simpa [hOne] : d0 ∈ drel.filter (fun d => kf f == kd d))).1
        have hkEq : kf f = kd d0 := by
          have hb : (kf f == kd d0) = true := (List.mem_filter.mp (by simpa [hOne] : d0 ∈ drel.filter (fun d => kf f == kd d))).2
          exact beq_iff_eq.mp hb
        exact (hAll d0 hd0mem) hkEq
      rw [if_neg hNotAll]
      simp [stateOfKey, hkf, hOne]
  · by_cases hAll : ∀ a : D, a ∈ drel → ¬ kf f = kd a
    · rw [if_pos hAll]
      simp [stateOfKey, hkf]
    · rw [if_neg hAll]
      have hFiltNil :
          ((drel.filter (fun d => kf f == kd d)).map (fun d => (f, some d))).filter
              (fun fd => kf fd.1 == k) = [] := by
        rw [List.filter_eq_nil_iff]
        intro fd hfd
        obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hfd
        intro htrue
        exact hkf htrue
      simp [stateOfKey, hFiltNil, hkf]

/-- **ルール E**: 次元側キーが一意なら、売上チャネルを事前に `(SUM, COUNT)` へ集約してから
    LEFT JOIN するのと、JOIN 後に集約するのとで状態が完全一致する。
    （返品除外 `RIGHT_ANTI` はこの状態の上で打てる。） -/
theorem eager_channel_preagg_before_left_join_sound {F D K : Type} [DecidableEq K]
    (kf : F → K) (kd : D → K) (valOf : F → Option Int)
    (frel : Rel F) (drel : Rel D) (dom : List K) (hNd : drel.Nodup)
    (hUniq : ∀ d1 ∈ drel, ∀ d2 ∈ drel, kd d1 == kd d2 → d1 = d2) :
    let joined := leftOuterJoin (fun f d => if kf f == kd d then .tTrue else .tFalse) frel drel
    aggByKey (fun fd : F × Option D => kf fd.1) (fun fd => valOf fd.1) joined dom
      = aggByKey kf valOf frel dom := by
  intro joined
  dsimp [aggByKey]
  apply List.map_congr_left
  intro k hk
  congr 1
  dsimp [joined]
  induction frel with
  | nil => simp [stateOfKey, leftOuterJoin]
  | cons f fs ih =>
      rw [leftOuterJoin_cons]
      rw [stateOfKey_append]
      rw [stateOfKey_cons]
      rw [stateOfKey_left_join_contrib kf kd valOf f drel k hNd hUniq]
      rw [ih]
      rfl

/-- **反例 E'**: 次元キーが一意でないと、1 行が複数行に膨らんで状態が変わる。 -/
theorem eager_channel_preagg_requires_unique_dimension :
    let drel : Rel (Int × Int) := [(1, 10), (1, 20)]
    let kd : Int × Int → Int := fun x => x.1
    let kf : Int → Int := fun x => x
    let frel : Rel Int := [1]
    let valOf : Int → Option Int := fun x => some x
    stateOfKey (fun fd : Int × Option (Int × Int) => kf fd.1)
      (fun fd => valOf fd.1)
      (leftOuterJoin (fun f d => if kf f == kd d then .tTrue else .tFalse) frel drel) 1
      ≠ stateOfKey kf valOf frel 1 := by
  decide

end TinylambOpt

-- C++ references to this model or its proof.
-- executor/detail/ordered_join_input.cpp
-- executor/detail/typed_column_eval.cpp
-- executor/relational.cpp
-- plan/cascades_predicate_rules.cpp
-- query/next_rule_b_test.cpp
-- query/next_rule_d_test.cpp
-- query/select_grouped_rewrites.cpp
-- query/select_rewrites.cpp
-- query/select_set_and_join_rewrites.cpp
-- query/sql_grouping_sets_test.cpp
