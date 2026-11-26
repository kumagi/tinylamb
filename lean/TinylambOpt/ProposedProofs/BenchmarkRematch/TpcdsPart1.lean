import Std
import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart1

/-!
# Proofs for the first TPC-DS benchmark-rematch rewrite goals

This module certifies the exact arithmetic, filtering, aggregation, set, and
prefix claims stated in `TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart1`.
The proofs model execution changes that may move filters before decoding, fuse
scans or aggregates, restrict join build sides, and replace correlated probes by
precomputed key sets while preserving the stated exact integer result.
-/

namespace TinylambOpt.ProposedProofs.BenchmarkRematch.TpcdsPart1Proofs

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart1

private theorem total_filter_or_partition {α : Type} (xs : List α)
    (p q : α → Bool) (f : α → Int) :
    total (xs.filter (fun x => p x || q x)) f =
      total (xs.filter p) f + total (xs.filter (fun x => (!p x) && q x)) f := by
  induction xs with
  | nil => simp [total]
  | cons x xs ih =>
      unfold total at ih
      cases hp : p x <;> cases hq : q x <;>
        simp [List.filter_cons, hp, hq, total, ih,
          Int.add_assoc, Int.add_comm, Int.add_left_comm]

private theorem total_filter_or_disjoint {α : Type} (xs : List α)
    (p q : α → Bool) (f : α → Int)
    (hdisjoint : ∀ x ∈ xs, p x = true → q x = false) :
    total (xs.filter (fun x => p x || q x)) f =
      total (xs.filter p) f + total (xs.filter q) f := by
  induction xs with
  | nil => simp [total]
  | cons x xs ih =>
      cases hp : p x <;> cases hq : q x <;>
        simp_all [List.filter_cons, total, Int.add_assoc,
          Int.add_comm, Int.add_left_comm]

/-- Splitting a covered fact list by a duplicate-free key list preserves exact
predicated totals, so date-key preaggregation can precede the week rollup. -/
private theorem total_partition_by_keys {ρ κ : Type} [BEq κ] [LawfulBEq κ]
    (facts : List ρ) (keys : List κ) (key : ρ → κ) (payload : ρ → Int)
    (p : κ → Bool) (hkeys : keys.Nodup)
    (hcovered : ∀ r ∈ facts, key r ∈ keys) :
    total (facts.filter (fun r => p (key r))) payload =
      ((keys.filter p).map (fun k =>
        total (facts.filter (fun r => key r == k)) payload)).sum := by
  induction keys generalizing facts with
  | nil =>
      have hfacts : facts = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro r hr
        simpa using hcovered r hr
      simp [hfacts, total]
  | cons k ks ih =>
      have hknot : k ∉ ks := (List.nodup_cons.mp hkeys).1
      have hks : ks.Nodup := (List.nodup_cons.mp hkeys).2
      have hrest : ∀ r ∈ facts.filter (fun r => !(key r == k)), key r ∈ ks := by
        intro r hr
        have hrFacts := (List.mem_filter.mp hr).1
        have hne : key r ≠ k := by simpa using (List.mem_filter.mp hr).2
        rcases List.mem_cons.mp (hcovered r hrFacts) with heq | hmem
        · exact False.elim (hne heq)
        · exact hmem
      have hhead : ((facts.filter (fun r => key r == k)).filter
          (fun r => p (key r))) =
          (if p k = true then facts.filter (fun r => key r == k) else []) := by
        cases hp : p k with
        | false =>
            apply List.filter_eq_nil_iff.mpr
            intro r hr hpr
            have heq : key r = k := by simpa using (List.mem_filter.mp hr).2
            simp [heq, hp] at hpr
        | true =>
            apply List.filter_eq_self.mpr
            intro r hr
            have heq : key r = k := by simpa using (List.mem_filter.mp hr).2
            simp [heq, hp]
      let selected := facts.filter (fun r => p (key r))
      have htaut : selected.filter
          (fun r => key r == k || !(key r == k)) = selected := by
        apply List.filter_eq_self.mpr
        intro r _
        cases heq : key r == k <;> simp [heq]
      have htotal := total_filter_or_partition selected
        (fun r => key r == k) (fun r => !(key r == k)) payload
      have hkeyComm : selected.filter (fun r => key r == k) =
          (facts.filter (fun r => key r == k)).filter (fun r => p (key r)) := by
        simp only [selected, List.filter_filter]
        congr 1
        funext r
        exact Bool.and_comm _ _
      have hnotComm : selected.filter (fun r => !(key r == k)) =
          (facts.filter (fun r => !(key r == k))).filter (fun r => p (key r)) := by
        simp only [selected, List.filter_filter]
        congr 1
        funext r
        exact Bool.and_comm _ _
      have htotal' : total selected payload =
          total ((facts.filter (fun r => key r == k)).filter (fun r => p (key r))) payload +
          total ((facts.filter (fun r => !(key r == k))).filter (fun r => p (key r))) payload := by
        simpa only [htaut, hkeyComm, hnotComm, Bool.and_self] using htotal
      have hrest' : ∀ r ∈
          (facts.filter (fun r => !(key r == k))).filter (fun r => p (key r)),
          key r ∈ ks := by
        intro r hr
        exact hrest r (List.mem_filter.mp hr).1
      rw [htotal', hhead,
        ih (facts.filter (fun r => !(key r == k))) hks hrest]
      have hclean : ∀ k' ∈ ks,
          (facts.filter (fun r => !(key r == k))).filter (fun r => key r == k') =
          facts.filter (fun r => key r == k') := by
        intro k' hk'
        rw [List.filter_filter]
        apply List.filter_congr
        intro r _
        by_cases heq : key r == k'
        · have hrk' : key r = k' := by simpa using heq
          have hkne : k' ≠ k := by
            intro h
            exact hknot (h ▸ hk')
          simp [heq, hrk', hkne]
        · simp [heq]
      have hmap :
          (ks.filter p).map (fun k' =>
            total ((facts.filter (fun r => !(key r == k))).filter
              (fun r => key r == k')) payload) =
          (ks.filter p).map (fun k' =>
            total (facts.filter (fun r => key r == k')) payload) := by
        apply List.map_congr_left
        intro k' hk'
        exact congrArg (fun rs => total rs payload)
          (hclean k' (List.mem_filter.mp hk').1)
      rw [hmap]
      cases hp : p k <;> simp [hp, total]

private theorem repeated_filtered_sum {α : Type} [BEq α] (xs : List α)
    (k : α) (v : Int) :
    ((xs.filter (fun x => x == k)).map (fun _ => v)).sum = (xs.count k : Int) * v := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases heq : x == k <;>
        simp [heq, ih, List.count_cons, Int.natCast_add, Int.natCast_one,
          Int.add_mul, Int.add_comm]

/-- Q01: doubling both sides of the positive-denominator average threshold is
an exact equivalence in widened integer arithmetic. -/
theorem Q01_ExactCrossMultipliedAvgThreshold :
    Q01_ExactCrossMultipliedAvgThresholdGoal := by
  intro x s cnt _
  constructor
  · intro h
    rw [show 10 * (cnt : Int) * x = 2 * (5 * (cnt : Int) * x) by ac_rfl,
      show 12 * s = 2 * (6 * s) by ac_rfl]
    omega
  · intro h
    rw [show 10 * (cnt : Int) * x = 2 * (5 * (cnt : Int) * x) by ac_rfl,
      show 12 * s = 2 * (6 * s) by ac_rfl] at h
    omega

/-- Q02: covered date keys form disjoint fact partitions, allowing channel
preaggregation by date before rolling the same totals into week/day buckets. -/
theorem Q02_DateKeyPreaggBeforeWeekRollup :
    Q02_DateKeyPreaggBeforeWeekRollupGoal := by
  intro facts dateKeys weekOf w hnodup hcovered
  simpa [List.filter_map] using
    (total_partition_by_keys facts dateKeys Prod.fst Prod.snd
      (fun d => weekOf d == w) hnodup hcovered)

/-- Q04: a positive prior-year denominator lets the ratio comparison be
replaced by its exact cross-product without changing the selected customer. -/
theorem Q04_PositivePriorYearRatioCrossProduct :
    Q04_PositivePriorYearRatioCrossProductGoal := by
  intro t1 t2 s1 s2 _ _
  omega

/-- Q07: all four fused measures can be accumulated from one shared match
list; the count and two modeled payload sums are definitionally identical. -/
theorem Q07_FusedMultiMeasureCountSum : Q07_FusedMultiMeasureCountSumGoal := by
  intro rows item
  rfl

/-- Q11: disjoint year selections partition a single customer scan, so their
sums equal the sum of the two separate filtered scans. -/
theorem Q11_SinglePassTwoYearChannelSplit : Q11_SinglePassTwoYearChannelSplitGoal := by
  intro rows y1 y2 c hy
  let p : Nat × Nat × Int → Bool := fun r => (r.1 == c) && (r.2.1 == y1)
  let q : Nat × Nat × Int → Bool := fun r => (r.1 == c) && (r.2.1 == y2)
  have hdisjoint : ∀ r ∈ rows, p r = true → q r = false := by
    intro r _ hp
    cases hq : q r with
    | false => rfl
    | true =>
        have hp1 : r.2.1 = y1 := by
          simpa [p] using (Bool.and_eq_true_iff.mp hp).2
        have hq1 : r.2.1 = y2 := by
          simpa [q] using (Bool.and_eq_true_iff.mp hq).2
        exact False.elim (hy (hp1.symm.trans hq1))
  have h := total_filter_or_disjoint rows p q (fun r => r.2.2) hdisjoint
  have hf : rows.filter (fun r => p r || q r) =
      rows.filter (fun r => (r.1 == c) && ((r.2.1 == y1) || (r.2.1 == y2))) := by
    apply List.filter_congr
    intro r _
    cases hc : r.1 == c <;> cases h1 : r.2.1 == y1 <;>
      cases h2 : r.2.1 == y2 <;> simp [p, q, hc, h1, h2]
  rw [hf] at h
  exact h

/-- Q14: after restricting to one coarse key, duplicate-free fine keys
partition its payload, so the finest buckets sum to the coarse total. -/
theorem Q14_RollupFromFinestGroupBuckets : Q14_RollupFromFinestGroupBucketsGoal := by
  intro rows fineKeys coarse hnodup hcovered
  let selected := rows.filter (fun r => r.1 == coarse)
  have hcov : ∀ r ∈ selected, r.2.1 ∈ fineKeys := by
    intro r hr
    have hrRows := (List.mem_filter.mp hr).1
    have hrCoarse : r.1 = coarse := by simpa using (List.mem_filter.mp hr).2
    exact hcovered r hrRows hrCoarse
  have h := total_partition_by_keys selected fineKeys (fun r => r.2.1)
    (fun r => r.2.2) (fun _ => true) hnodup hcov
  have htrue : fineKeys.filter (fun _ => true) = fineKeys :=
    List.filter_eq_self.mpr (by simp)
  simpa [selected, total, htrue, List.filter_filter, Bool.and_comm] using h

/-- Q15: address matches and high-price-only matches are disjoint and cover
the disjunctive row filter, preserving the exact sales-price sum. -/
theorem Q15_AddressBitsetOrPriceFilter : Q15_AddressBitsetOrPriceFilterGoal := by
  intro rows addrOk priceMin
  exact total_filter_or_partition rows (fun r => addrOk r.1)
    (fun r => decide (priceMin < r.2)) Prod.snd

/-- Q16: restricting a return list to candidate keys cannot change whether a
candidate occurs in that list, so the anti-join result is unchanged. -/
theorem Q16_CandidateRestrictedAntiAndMultiWarehouse :
    Q16_CandidateRestrictedAntiAndMultiWarehouseGoal := by
  intro cands returns
  apply List.filter_congr
  intro o ho
  apply Bool.eq_iff_iff.mpr
  simp_all [List.contains_iff_mem, List.mem_filter, Bool.not_eq_true']

/-- Q17: translation by an anchor changes the two moment accumulators but
cancels algebraically in the scaled variance numerator, including for empty
samples and negative values. -/
theorem Q17_ShiftInvariantScaledVariance : Q17_ShiftInvariantScaledVarianceGoal := by
  intro xs a
  have hmoments :
      (xs.map (fun x => x - a)).sum = xs.sum - (xs.length : Int) * a ∧
      (xs.map (fun x => (x - a) * (x - a))).sum =
        (xs.map (fun x => x * x)).sum - (a * xs.sum + a * xs.sum) +
          (xs.length : Int) * (a * a) := by
    induction xs with
    | nil => simp
    | cons x xs ih =>
        constructor
        · rw [List.map_cons, List.sum_cons, List.length_cons, ih.1]
          simp only [List.sum_cons, List.length_cons, Int.natCast_add,
            Int.natCast_one, Int.add_mul, Int.sub_eq_add_neg]
          omega
        · have hx : (x - a) * (x - a) = x * x - (a * x + a * x) + a * a := by
            simp only [Int.sub_eq_add_neg, Int.mul_add, Int.add_mul,
              Int.neg_add, Int.neg_mul, Int.mul_neg, Int.mul_comm]
            omega
          rw [List.map_cons, List.sum_cons, List.length_cons, ih.2, hx]
          simp only [List.map_cons, List.sum_cons, List.length_cons,
            Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
            Int.sub_eq_add_neg, Int.mul_comm]
          omega
  rw [hmoments.1, hmoments.2]
  simp only [Int.sub_eq_add_neg, Int.mul_add, Int.add_mul,
    Int.neg_add, Int.neg_mul, Int.mul_neg, Int.mul_comm,
    Int.mul_left_comm, Int.mul_assoc]
  omega

/-- Q21: flattening distributes over block concatenation and a block without a
date-window witness contributes no selected row, so only witness blocks matter. -/
theorem Q21_DisjointDateBlockPruning : Q21_DisjointDateBlockPruningGoal := by
  intro blocks lo hi
  let p : Nat × Int → Bool := fun r => decide (lo ≤ r.1 ∧ r.1 ≤ hi)
  have hfilter : blocks.flatten.filter p =
      ((blocks.filter (fun b => b.any p)).flatten).filter p := by
    induction blocks with
    | nil => rfl
    | cons b bs ih =>
        simp only [List.flatten_cons, List.filter_append, List.filter_cons,
          ih, List.any_append]
        cases hb : b.any p
        · have hbfilter : b.filter p = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro r hr
            exact (List.any_eq_false.mp hb) r hr
          cases hbs : bs.flatten.any p <;> simp [hb, hbs, hbfilter]
        · cases hbs : bs.flatten.any p <;> simp [hb, hbs]
  exact congrArg (fun rs => total rs Prod.snd) hfilter

/-- Q25: a key known to be in the semi-join key list survives the pushed
filter, so its catalog payload total is unchanged. -/
theorem Q25_SalesReturnKeySemijoinOnCatalog :
    Q25_SalesReturnKeySemijoinOnCatalogGoal := by
  intro cat srKeys k hk
  have hmem : srKeys.contains k = true := List.contains_iff_mem.mpr hk
  congr 1
  rw [List.filter_filter]
  apply List.filter_congr
  intro r _
  by_cases heq : r.1 == k
  · have hrk : r.1 = k := by simpa using heq
    simp [heq, hrk, hk]
  · simp [heq]

/-- Q29: expanding one source row once per matching catalog row is weighted
by exactly that catalog key's multiplicity. -/
theorem Q29_CatalogMultiplicityWeighting : Q29_CatalogMultiplicityWeightingGoal := by
  intro srRows catRows
  induction srRows with
  | nil => simp
  | cons r rs ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append,
        List.map_cons, List.sum_cons, ih]
      rw [repeated_filtered_sum]

/-- Q30: filtering the state partition directly equals filtering the original
customer list by state and then by the same state-aggregate threshold. -/
theorem Q30_StatePartitionSinglePassRatio : Q30_StatePartitionSinglePassRatioGoal := by
  intro custs st
  simp [List.filter_filter, Bool.and_comm]

/-- Q35: conjunction distributes over the two-stage customer filter, allowing
web and catalog existence probes to be restricted to store-existing customers. -/
theorem Q35_StoreRestrictedDisjunctiveExistsBitset :
    Q35_StoreRestrictedDisjunctiveExistsBitsetGoal := by
  intro custs hasSS hasWS hasCS
  simp [List.filter_filter, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]

private theorem eraseDups_map_of_injective {α β : Type} [BEq α] [BEq β]
    (code : α → β) (a : List α)
    (hinj : ∀ x y, (code x == code y) = (x == y)) :
    (a.map code).eraseDups = (a.eraseDups).map code := by
  induction hn : a.length using Nat.strongRecOn generalizing a with
  | ind n ih =>
      cases a with
      | nil => rfl
      | cons x xs =>
          let ys := xs.filter (fun y => !(y == x))
          have hlen : ys.length < n := by
            calc
              ys.length ≤ xs.length := List.length_filter_le _ _
              _ < (x :: xs).length := Nat.lt_succ_self _
              _ = n := hn
          have hrec := ih ys.length hlen ys rfl
          rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons,
            List.filter_map]
          have hf : (fun b : β => !b == code x) ∘ code =
              (fun y : α => !y == x) := by
            funext y
            exact congrArg Bool.not (hinj y x)
          rw [hf]
          change code x :: (ys.map code).eraseDups =
            code x :: (ys.eraseDups).map code
          rw [hrec]

private theorem contains_map_of_injective {α β : Type} [BEq α] [BEq β]
    (code : α → β) (a : List α) (x : α)
    (hinj : ∀ x y, (code x == code y) = (x == y)) :
    (a.map code).contains (code x) = a.contains x := by
  induction a with
  | nil => rfl
  | cons y ys ih =>
      simp only [List.map_cons, List.contains_cons, ih]
      exact congrArg (fun b => b || ys.contains x) (hinj x y)

private theorem members_map_of_injective {α β : Type} [BEq α] [BEq β]
    (code : α → β) (a b : List α)
    (hinj : ∀ x y, (code x == code y) = (x == y)) :
    members (a.map code) (b.map code) = (members a b).map code := by
  simp only [members, eraseDups_map_of_injective code a hinj,
    List.filter_map, List.map_map, Function.comp_def]
  apply congrArg (List.map code)
  apply List.filter_congr
  intro x _
  exact contains_map_of_injective code b x hinj

/-- Q38: an injective canonical name code preserves duplicate elimination and
intersection membership, so compact code triples have the same cardinality. -/
theorem Q38_InjectiveCodeTripleIntersectCardinality :
    Q38_InjectiveCodeTripleIntersectCardinalityGoal := by
  intro α β _ _ code a b c hinj
  simp only [members_map_of_injective code a b hinj,
    members_map_of_injective code (members a b) c hinj,
    List.map_map, Function.comp_def, List.length_map]

private theorem sum_const_int (xs : List Int) (v : Int)
    (hv : ∀ x ∈ xs, x = v) : xs.sum = (xs.length : Int) * v := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx : x = v := hv x (by simp)
      have htail : ∀ y ∈ xs, y = v := by
        intro y hy
        exact hv y (by simp [hy])
      simp [List.sum_cons, hx, ih htail, Int.natCast_add, Int.add_mul,
        Int.add_comm]

private theorem sum_map_const_int {α : Type} (xs : List α) (f : α → Int) (v : α)
    (hv : ∀ x ∈ xs, x = v) : (xs.map f).sum = (xs.length : Int) * f v := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx : x = v := hv x (by simp)
      have htail : ∀ y ∈ xs, y = v := by
        intro y hy
        exact hv y (by simp [hy])
      simp [List.map_cons, List.sum_cons, hx, ih htail, Int.natCast_add,
        Int.add_mul, Int.add_comm]

/-- Q39: a constant integer group has identically zero translated moment
numerator, permitting immediate variance rejection without division. -/
theorem Q39_ConstantGroupZeroVarianceRejection :
    Q39_ConstantGroupZeroVarianceRejectionGoal := by
  intro xs v hv
  rw [sum_const_int xs v hv, sum_map_const_int xs (fun x => x * x) v hv]
  simp only [Int.natCast_add, Int.natCast_one, Int.mul_add, Int.add_mul,
    Int.sub_eq_add_neg, Int.neg_mul, Int.mul_neg, Int.add_left_neg,
    Int.add_right_neg, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]

/-- Q40: every surviving sale key remains in the pruned return build, so its
matching return rows and exact left-join refund sum are unchanged. -/
theorem Q40_LeftJoinReturnBuildRestriction :
    Q40_LeftJoinReturnBuildRestrictionGoal := by
  intro sales returns
  dsimp only
  let saleKeys := sales.map Prod.fst
  let pruned := returns.filter (fun r => saleKeys.contains r.1)
  intro s hs
  have hsKey : saleKeys.contains s.1 = true := by
    simp only [saleKeys, List.map_cons, List.map_nil, List.contains_iff_mem,
      List.mem_map, Prod.fst]
    exact ⟨s, hs, rfl⟩
  have hsMem : s.1 ∈ saleKeys := List.contains_iff_mem.mp hsKey
  have hfilt : returns.filter (fun r => r.1 == s.1) =
      pruned.filter (fun r => r.1 == s.1) := by
    simp only [pruned, List.filter_filter]
    apply List.filter_congr
    intro r _
    by_cases heq : r.1 == s.1
    · have hrs : r.1 = s.1 := by simpa using heq
      simp [heq, hrs, hsMem]
    · simp [heq]
  exact congrArg (fun rs => total rs Prod.snd) hfilt

/-- Q41: a positive filtered count is exactly existence of one qualifying
row, which is exactly membership in the qualifying manufacturer list. -/
theorem Q41_CorrelatedPositiveCountAsSetMembership :
    Q41_CorrelatedPositiveCountAsSetMembershipGoal := by
  intro items m
  simp [countWhere, List.contains_iff_mem, List.length_pos_iff_exists_mem,
    List.mem_filter, List.mem_map]

private theorem mergeSort_filter_low_high (xs : List Nat) (tau : Nat) :
    xs.mergeSort = ((xs.filter (fun x => decide (x ≤ tau))).mergeSort ++
      (xs.filter (fun x => !decide (x ≤ tau))).mergeSort) := by
  let le : Nat → Nat → Bool := fun a b => decide (a ≤ b)
  let p : Nat → Bool := fun x => decide (x ≤ tau)
  have hleTrans : ∀ a b c, le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have hleTotal : ∀ a b, (le a b || le b a) = true := by
    intro a b
    rcases Nat.le_total a b with hab | hba
    · simp [le, hab]
    · simp [le, hba]
  have hsorted : List.Pairwise (fun a b => le a b = true)
      (((xs.filter p).mergeSort le) ++ ((xs.filter (fun x => !p x)).mergeSort le)) := by
    rw [List.pairwise_append]
    refine ⟨List.pairwise_mergeSort hleTrans hleTotal _,
      List.pairwise_mergeSort hleTrans hleTotal _, ?_⟩
    intro a ha b hb
    have haLow : a ≤ tau := by
      have ha' : a ∈ xs.filter p := List.mem_mergeSort.mp ha
      simpa [p] using (List.mem_filter.mp ha').2
    have hbHigh : tau < b := by
      have hb' : b ∈ xs.filter (fun x => !p x) := List.mem_mergeSort.mp hb
      have hnot : !(decide (b ≤ tau)) = true := (List.mem_filter.mp hb').2
      have : ¬ b ≤ tau := by simpa using hnot
      omega
    exact decide_eq_true (Nat.le_of_lt (Nat.lt_of_le_of_lt haLow hbHigh))
  have hperm : (((xs.filter p).mergeSort le) ++
      ((xs.filter (fun x => !p x)).mergeSort le)).Perm xs := by
    exact ((List.mergeSort_perm (xs.filter p) le).append
      (List.mergeSort_perm (xs.filter (fun x => !p x)) le)).trans
        (List.filter_append_perm p xs)
  exact (List.Perm.eq_of_pairwise
    (fun a b _ _ hab hba => Nat.le_antisymm (of_decide_eq_true hab)
      (of_decide_eq_true hba))
    (List.pairwise_mergeSort hleTrans hleTotal xs) hsorted
      ((List.mergeSort_perm xs le).trans hperm.symm))

/-- Q44: if at least `k` scores are at most the cutoff, those scores are a
prefix of the globally sorted list, so pruning larger scores preserves `best k`. -/
theorem Q44_BoundedTopAndBottomKPrefix : Q44_BoundedTopAndBottomKPrefixGoal := by
  intro xs k tau hk
  have hlen : k ≤ (xs.filter (fun x => decide (x ≤ tau))).length := by
    simpa [countWhere] using hk
  change (xs.filter (fun x => decide (x ≤ tau))).mergeSort.take k =
    xs.mergeSort.take k
  have hlenSort : ((xs.filter (fun x => decide (x ≤ tau))).mergeSort).length =
      (xs.filter (fun x => decide (x ≤ tau))).length :=
    (List.mergeSort_perm _ _).length_eq
  calc
    (xs.filter (fun x => decide (x ≤ tau))).mergeSort.take k =
        (((xs.filter (fun x => decide (x ≤ tau))).mergeSort ++
          (xs.filter (fun x => !decide (x ≤ tau))).mergeSort).take k) := by
      rw [List.take_append]
      have hsub : k - ((xs.filter (fun x => decide (x ≤ tau))).mergeSort).length = 0 := by
        rw [hlenSort]
        omega
      rw [hsub]
      simp
    _ = xs.mergeSort.take k :=
      congrArg (fun zs => zs.take k) (mergeSort_filter_low_high xs tau).symm

/-- Q45: once physical surrogate-key membership agrees with logical target
membership on every sale, either disjunctive filter selects identical sums. -/
theorem Q45_LogicalItemToPhysicalKeyDisjunction :
    Q45_LogicalItemToPhysicalKeyDisjunctionGoal := by
  intro sales zipOk logicalOf targetLogical physKeys hagree
  congr 1
  apply List.filter_congr
  intro r hr
  simp only [hagree r hr]

end TinylambOpt.ProposedProofs.BenchmarkRematch.TpcdsPart1Proofs
