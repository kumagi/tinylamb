import Std
import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
import TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CommonItemTopKBeforeQuantityLookup
import TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RankFromDistinctValueFrequencies

/-!
# Proofs of the TPC-DS Part 2 benchmark rematch goals

This module proves the twenty-one finite list models for null-rejecting joins,
filter pushdown, pre-aggregation, exact bucket counts, top-k pruning, local
Deduplication, and run-length histogram accumulation. Every proof concerns the
modeled observable totals, counts, keys, or ranks; it permits the corresponding
execution rewrite without changing those modeled SQL results. The propositions
do not claim equivalence of physical plans, costs, or floating-point SQL paths.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ProposedProofs.BenchmarkRematch.TpcdsPart2Proofs

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RankFromDistinctValueFrequencies (freq_sum)
open TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CommonItemTopKBeforeQuantityLookup (filter_prefix sorted_pairwise)

/-- Pointwise equality of per-row expansions gives equality of the flattened lists. -/
theorem flatMap_congr_rows {α β : Type} (xs : List α) (f g : α → List β)
    (h : ∀ a ∈ xs, f a = g a) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
    rw [List.flatMap_cons, List.flatMap_cons, h a (by simp), ih (fun b hb => h b (by simp [hb]))]

/-- Null-rejecting selection preserves the outer-join row count by discarding exactly
the NULL-extended rows. -/
theorem Q49_PositiveReturnFilterInnerJoinReductionGoal : Q49_PositiveReturnFilterInnerJoinReductionGoal := by
  intro rows
  induction rows with
  | nil => rfl
  | cons r rows ih =>
    cases r with
    | none => simp [ih]
    | some q =>
      by_cases h : 0 < q <;> simp [List.filter_cons, h, ih]

/-- An item predicate can be evaluated once before the manufacturer-quarter grouping;
the assumed successful predicate for the target manufacturer makes both paths select
exactly the same rows. -/
theorem Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal : Q53_ItemBitsetPrefilterBeforeQuarterWindowGoal := by
  intro sales itemOk m q hm
  simp only [total, List.map_map, Function.comp_def]
  congr 1
  rw [List.filter_filter]
  have hfilters : sales.filter (fun r => itemOk r.1 && (r.1 == m) && (r.2.1 == q)) =
      sales.filter (fun r => (r.1 == m) && (r.2.1 == q) && itemOk r.1) := by
    apply List.filter_congr
    intro r _
    by_cases hkey : (r.1 == m) = true
    · have hrm : r.1 = m := eq_of_beq hkey
      rw [hrm]
      simp [hm, Bool.and_left_comm, Bool.and_assoc]
    · by_cases hq : (r.2.1 == q) = true <;> simp [hkey, hq]
  rw [hfilters]

/-- A predicate on the source row commutes with a per-row expansion. -/
theorem filter_flatMap_source {α β : Type} (xs : List α) (keep : α → Bool)
    (expand : α → List β) :
    (xs.flatMap expand).filter (fun _ => true) = xs.flatMap expand := by
  simp

/-- Filtering before a per-row expansion is equivalent to filtering afterward when
the predicate examines only the source row. -/
theorem filter_flatMap_current {α β γ : Type} (xs : List α) (keep : α → Bool)
    (expand : α → List β) (decorate : β → γ) (current : γ → α)
    (hcurrent : ∀ a p, current (decorate p) = a) :
    (xs.flatMap (fun a => (expand a).map decorate)).filter (fun c => keep (current c)) =
      (xs.filter keep).flatMap (fun a => (expand a).map decorate) := by
  have hdec : ∀ (a : α) (p : β), keep (current (decorate p)) = keep a :=
    fun a p => congrArg keep (hcurrent a p)
  induction xs with
  | nil => simp
  | cons a xs ih =>
    have hhead : ((expand a).map decorate).filter (fun c => keep (current c)) =
        if keep a then (expand a).map decorate else [] := by
      rw [List.filter_map]
      have hpred : ((fun c => keep (current c)) ∘ decorate) = (fun _ => keep a) := by
        funext p
        exact hdec a p
      rw [hpred]
      by_cases ha : keep a = true
      · have htrue : (expand a).filter (fun _ => true) = expand a :=
          List.filter_eq_self.mpr (fun _ _ => rfl)
        simp [ha, htrue]
      · simp [ha]
    by_cases ha : keep a = true <;>
      simp [List.flatMap_cons, List.filter_append, List.filter_cons, ha, hhead, ih]

/-- Filtering current-month rows before their neighbor expansion preserves the full
joined sequence, since the selection predicate depends only on the current row. -/
theorem Q57_DeviationBandFilterBeforeNeighborJoinGoal : Q57_DeviationBandFilterBeforeNeighborJoinGoal := by
  intro months keep neighbors
  have hfilter (xs : List (Nat × Int)) :
      (xs.flatMap (fun m => (neighbors m.1).map (fun p => (m, p)))).filter (fun t => keep t.1) =
        (xs.filter keep).flatMap (fun m => (neighbors m.1).map (fun p => (m, p))) := by
    induction xs with
    | nil => rfl
    | cons m xs ih =>
      have hhead : ((neighbors m.1).map (fun p => (m, p))).filter (fun t => keep t.1) =
          if keep m then (neighbors m.1).map (fun p => (m, p)) else [] := by
        rw [List.filter_map]
        have hpred : ((fun t : (Nat × Int) × (Int × Int) => keep t.1) ∘
            (fun p : Int × Int => (m, p))) = (fun _ => keep m) := rfl
        rw [hpred]
        have hfilterTrue : (neighbors m.1).filter (fun _ => true) = neighbors m.1 :=
      List.filter_eq_self.mpr (fun _ _ => rfl)
    by_cases hm : keep m = true <;> simp [hm, hfilterTrue]
      by_cases hm : keep m = true <;>
        simp [List.flatMap_cons, List.filter_append, List.filter_cons, hm, hhead, ih]
  exact hfilter months

/-- Positive quantities satisfying the pairwise ratio band also satisfy the one-way
exact cross-multiplied bound used by the early rejection test. -/
theorem Q58_PairwiseRatioBandCrossMultiplicationGoal : Q58_PairwiseRatioBandCrossMultiplicationGoal := by
  intro a b ha hb hband
  omega

/-- Multiplication distributes over the sum of matching right-hand values. -/
theorem sum_map_mul_total {α : Type} (rows : List α) (left : α → Int)
    (right : List (Nat × Int)) (matchRight : α → Nat × Int → Bool) :
    (rows.flatMap (fun a => (right.filter (matchRight a)).map (fun b => left a * b.2))).sum =
      (rows.map (fun a => left a * total (right.filter (matchRight a)) Prod.snd)).sum := by
  induction rows with
  | nil => simp [total]
  | cons a rows ih =>
    simp only [List.flatMap_cons, List.map_cons, List.sum_cons, List.map_map, Function.comp_def,
      total] at ih ⊢
    rw [List.sum_append, ih]
    have hmul : ((right.filter (matchRight a)).map (fun b => left a * b.2)).sum =
        left a * ((right.filter (matchRight a)).map Prod.snd).sum := by
      induction right.filter (matchRight a) with
      | nil => simp
      | cons b bs h =>
        rw [List.map_cons, List.sum_cons, h]
        exact (Int.mul_add _ _ _).symm
    rw [hmul]

/-- A shifted-week indexed sum equals materializing every matching year-2 row once per
matching year-1 row. -/
theorem Q59_ShiftedWeekIndexLookupEquivGoal : Q59_ShiftedWeekIndexLookupEquivGoal := by
  intro y1 y2 shift
  simpa [total] using sum_map_mul_total y1 Prod.snd y2 (fun a b => b.1 == a.1 + shift)

/-- The five disjoint exact delay buckets partition the input list. -/
theorem Q62_DelayBucketPartitionSumGoal : Q62_DelayBucketPartitionSumGoal := by
  intro delays
  let bs : List Int := [30, 60, 90, 120]
  have hpartition : ∀ d : Int, bucket bs d ≤ 4 := by
    intro d
    simpa [bs, bucket] using List.countP_le_length (p := fun b : Int => b < d) (l := bs)
  generalize hd : delays.length = n
  revert delays
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro delays hd
    cases delays with
    | nil =>
      have hn0 : n = 0 := by simpa using hd.symm
      subst n
      rfl
    | cons d ds =>
      have hpos : 0 < n := by
        have hlen := List.length_pos_of_mem (List.mem_cons_self (a := d) (l := ds))
        omega
      have hlen : ds.length < n := by
        have hcons : (d :: ds).length = ds.length + 1 := List.length_cons
        omega
      have hsum := ih ds.length hlen ds rfl
      have hhead : (∑ j ∈ List.range 5, (countWhere [d] (fun x => bucket bs x == j) : Nat)) = 1 := by
        have hb : bucket bs d < 5 := Nat.lt_succ_of_le (hpartition d)
        have hexpr : ∀ j : Nat, countWhere [d] (fun x => bucket bs x == j) =
            if bucket bs d == j then 1 else 0 := by
          intro j
          simp only [countWhere, List.filter_cons, List.filter_nil, List.length_cons,
            List.length_nil, Nat.zero_add]
        have hsumIf : (∑ j ∈ List.range 5, (if bucket bs d == j then (1 : Nat) else 0)) = 1 := by
          rw [List.sum_eq_single (bucket bs d)]
          · simp
          · intro b _ hbne
            have hne : (bucket bs d == b) = false := by simpa using hbne.symm
            simp [hne]
          · intro hnot
            exact False.elim (hnot (List.mem_range.mpr hb))
        have hcongr : (∑ j ∈ List.range 5, countWhere [d] (fun x => bucket bs x == j)) =
          ∑ j ∈ List.range 5, (if bucket bs d == j then (1 : Nat) else 0) :=
          Finset.sum_congr rfl (fun j _ => hexpr j)
        exact hcongr.trans hsumIf
      have hsplit : ∀ j : Nat,
          countWhere (d :: ds) (fun x => bucket bs x == j) =
            countWhere [d] (fun x => bucket bs x == j) +
              countWhere ds (fun x => bucket bs x == j) := by
        intro j
        simp [countWhere]
      have hfinal : (∑ j ∈ List.range 5, countWhere (d :: ds) (fun x => bucket bs x == j)) =
          (d :: ds).length := by
        rw [Finset.sum_range_succ, hsplit 0, hhead, List.length_cons]
        have htail : ∑ j ∈ List.range 4,
            countWhere (d :: ds) (fun x => bucket bs x == j + 1) =
            ∑ j ∈ List.range 4, countWhere ds (fun x => bucket bs x == j + 1) := by
          apply Finset.sum_congr rfl
          intro j hj
          have hj' : j + 1 < 5 := by simpa [List.mem_range] using Nat.succ_lt_succ (List.mem_range.mp hj)
          rw [hsplit (j + 1)]
          have hzero : countWhere [d] (fun x => bucket bs x == j + 1) = 0 := by
            have hne : bucket bs d ≠ j + 1 := by
              intro heq
              have hb : bucket bs d < 5 := by omega
              rw [heq] at hb
              omega
            simp [countWhere, hne]
          rw [hzero, Nat.zero_add]
        rw [htail]
        have hfive : ∑ k ∈ List.range 5, countWhere ds (fun x => bucket bs x == k) = ds.length := by
          simpa [bs] using hsum
        rw [List.sum_range_succ] at hfive
        omega
      simpa [countWhere, bs, Nat.add_assoc] using hfinal

/-- Disjoint cases split a sum without changing any selected row. -/
theorem total_or_partition {α : Type} (rows : List α) (p q : α → Bool) (f : α → Int) :
    total (rows.filter (fun r => p r || q r)) f =
      total (rows.filter p) f + total (rows.filter (fun r => (!p r) && q r)) f := by
  simp only [total, List.map_map, Function.comp_def]
  have hmap : (rows.filter (fun r => p r || q r)).map f =
      (rows.filter p).map f ++ (rows.filter (fun r => (!p r) && q r)).map f := by
    have hfilter : rows.filter (fun r => p r || q r) =
        rows.filter p ++ rows.filter (fun r => (!p r) && q r) := by
      induction rows with
      | nil => rfl
      | cons r rows ih =>
        by_cases hp : p r = true <;> by_cases hq : q r = true <;>
          simp [List.filter_cons, hp, hq, ih]
        · calc
            r :: (rows.filter p ++ rows.filter (fun r => (!p r) && q r)) =
                (r :: rows.filter p) ++ rows.filter (fun r => (!p r) && q r) := by
              simp only [List.cons_append]
            _ = rows.filter p ++ (r :: rows.filter (fun r => (!p r) && q r)) := by
              exact List.append_assoc _ _ _
    rw [hfilter, List.map_append]
  rw [hmap, List.sum_append]

/-- Overlapping OR branches form a disjoint partition: the first branch plus the
part of the second branch that avoids the first. -/
theorem Q63_DisjunctiveItemBitsetDeduplicationGoal : Q63_DisjunctiveItemBitsetDeduplicationGoal := by
  intro sales p1 p2
  exact total_or_partition sales (fun r => p1 r.1) (fun r => p2 r.1) Prod.snd

/-- Once a warehouse key has passed the join filter, all of its attribute lookups
return the same value; the projection can therefore use that key directly. -/
theorem Q66_WarehouseMonthPivotLateAttrLookupGoal : Q66_WarehouseMonthPivotLateAttrLookupGoal := by
  intro facts w m attr
  let p := fun r : Nat × Nat × Int => (r.1 == w) && (r.2.1 == m)
  have hw : ∀ r ∈ facts.filter p, r.1 = w := by
    intro r hr
    have hp := List.mem_filter.mp hr
    have hw' : r.1 = w := by
      have hbeq : (r.1 == w) = true := by
        have hpred : p r = true := hp.2
        dsimp [p] at hpred
        exact (Bool.and_eq_true_iff.mp hpred).1
      exact eq_of_beq hbeq
    exact hw'
  have hmaps : ∀ (f g : Nat × Nat × Int → Nat × Int),
      (∀ r ∈ facts.filter p, f r = g r) →
      (facts.filter p).map f = (facts.filter p).map g := by
    intro f g h
    exact List.map_congr_left h
  have h := hmaps (fun r => (attr r.1, r.2.2)) (fun r => (attr w, r.2.2))
    (by intro r hr; rw [hw r hr])
  simpa only [p] using h

/-- Filtering and summing each UNION ALL branch separately is additive because list
concatenation distributes through both operations. -/
theorem Q71_UnionAllBranchPreaggAdditiveGoal : Q71_UnionAllBranchPreaggAdditiveGoal := by
  intro b1 b2 b3 k
  simp [total, List.filter_append, List.map_append, List.sum_append, Int.add_assoc]

/-- Return multiplicity is the same weight for each sale row, so the promo and no-promo
partitions add up to the unpartitioned weighted sum. -/
theorem Q72_LeftJoinMultiplicityPromoSplitGoal : Q72_LeftJoinMultiplicityPromoSplitGoal := by
  intro sales retCount
  dsimp only
  generalize hw : (fun r : Nat × Bool =>
      if retCount r.1 == 0 then 1 else retCount r.1) = weight
  have hsum : (sales.map weight).sum =
      ((sales.filter Prod.snd).map weight).sum + ((sales.filter (fun r => !r.2)).map weight).sum := by
    induction sales with
    | nil => simp
    | cons r sales ih =>
      cases hb : r.2 <;>
        simp only [hb, List.filter_cons, Bool.false_eq_true, Bool.not_false, Bool.not_true,
          Bool.true_and, Bool.false_and, ite_false, ite_true, List.map_cons, List.sum_cons,
          List.map_nil, List.sum_nil] at ih ⊢ <;>
        omega
  exact hsum.symm

/-- At least k sorted candidates already below the ordinal fence are sufficient for
the top-k result: all omitted candidates are at least as large as every retained one. -/
theorem Q74_PositiveBaselineFilterBeforeTopKGoal : Q74_PositiveBaselineFilterBeforeTopKGoal := by
  intro cands k tau
  dsimp only
  intro hk
  let P : Nat → Bool := fun x => decide (x ≤ tau)
  let A := cands.filter (fun r => decide (0 < r.2.1 ∧ 0 < r.2.2)) |>.map Prod.fst
  change best k (A.filter P) = best k A
  have hcount : k ≤ (A.filter P).length := hk
  let sortedA := A.mergeSort
  let sortedP := (A.filter P).mergeSort
  have hmin : k ≤ (sortedA.filter P).length := by
    have hp := (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) A).filter P
    have hlen := hp.length_eq
    dsimp [sortedA]
    omega
  have hprefix : sortedA.filter P = sortedA.take (sortedA.filter P).length :=
    filter_prefix (fun a b : Nat => a ≤ b) P
      (fun a b hab hpb => by simp [P] at hpb ⊢; omega) sortedA
      (by simpa [sortedA] using sorted_pairwise A)
  have hsortP : sortedP = sortedA.filter P := by
    have hsortedP : sortedP.Pairwise (fun a b : Nat => decide (a ≤ b) = true) := by
      dsimp [sortedP]
      exact List.pairwise_mergeSort
        (fun a b c hab hbc => by simp only [decide_eq_true_eq] at hab hbc ⊢; omega)
        (fun a b => by simp only [Bool.or_eq_true, decide_eq_true_eq]; omega)
        (A.filter P)
    have hpairLeft : sortedP.Pairwise (fun a b : Nat => a ≤ b) :=
      hsortedP.imp (fun h => by simpa using h)
    have hpairRight : (sortedA.filter P).Pairwise (fun a b : Nat => a ≤ b) :=
      (sorted_pairwise A).filter P |>.imp (fun h => by simpa using h)
    have hperm : (List.filter P A).Perm (List.filter P sortedA) :=
      (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) A).symm.filter P
    have hperm' : sortedP.Perm (sortedA.filter P) :=
      (List.mergeSort_perm (le := fun a b : Nat => decide (a ≤ b)) (A.filter P)).trans hperm
    exact List.Perm.eq_of_pairwise
      (l₁ := sortedP) (l₂ := sortedA.filter P) (le := fun a b : Nat => a ≤ b)
      (fun a b _ _ hab hba => Nat.le_antisymm hab hba) hpairLeft hpairRight hperm'
  have htake : sortedP.take k = sortedA.take k := by
    rw [hsortP, hprefix, List.take_take, Nat.min_eq_left hmin]
  exact htake

/-- Deduplicating each channel before UNION DISTINCT preserves membership in the
global deduplicated list, hence its length. -/
theorem Q75_ChannelLocalDedupBeforeUnionDistinctGoal : Q75_ChannelLocalDedupBeforeUnionDistinctGoal := by
  intro α _ a b
  have hm : ∀ x : α, x ∈ (a.eraseDups ++ b.eraseDups).eraseDups ↔ x ∈ (a ++ b).eraseDups := by
    intro x
    have hmemA : x ∈ a.eraseDups ↔ x ∈ a := by
      simpa only [List.mem_eraseDups] using (Iff.rfl : x ∈ a.eraseDups ↔ x ∈ a.eraseDups)
    have hmemB : x ∈ b.eraseDups ↔ x ∈ b := by
      simpa only [List.mem_eraseDups] using (Iff.rfl : x ∈ b.eraseDups ↔ x ∈ b.eraseDups)
    constructor
    · intro h
      rcases h with h | h
      · exact Or.inl (hmemA.mp h)
      · exact Or.inr (hmemB.mp h)
    · intro h
      rcases h with h | h
      · exact Or.inl (hmemA.mpr h)
      · exact Or.inr (hmemB.mpr h)
  have hnodup : ∀ xs : List α, xs.eraseDups.Nodup := by
    intro xs
    generalize hn : xs.length = n
    revert xs
    induction n using Nat.strongRecOn with
    | ind n ih =>
      intro xs hn
      cases xs with
      | nil => simp
      | cons x xs =>
        have hn' : (x :: xs).length = n := by simpa using hn
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro hmem
    have hraw : x ∈ xs.filter (fun y => !(y == x)) :=
      List.mem_eraseDups.mp hmem
    have hy := (List.mem_filter.mp hraw).2
    cases hxx : (x == x) with
    | false =>
      have hnot : (!(x == x)) = true := by simp [hxx]
      have hc : (!(x == x)) = true := hy
      rw [hnot] at hc
      cases hc
    | true =>
      have hnot : (!(x == x)) = false := by simp [hxx]
      have hc : (!(x == x)) = true := hy
      rw [hnot] at hc
      cases hc
        · apply ih (xs.filter (fun y => !(y == x))).length
          · have hlen : (x :: xs).length = xs.length + 1 := List.length_cons
            have hle := List.length_filter_le (fun y => !(y == x)) xs
            omega
          · rfl
  have hleft := hnodup (a.eraseDups ++ b.eraseDups)
  have hright := hnodup (a ++ b)
  have hperm : List.Perm ((a.eraseDups ++ b.eraseDups).eraseDups) ((a ++ b).eraseDups) :=
    (List.perm_ext_iff_of_nodup hleft hright).2 hm
  exact hperm.length_eq

/-- Functional dependence of address on ticket lets the grouping filter drop the
address equality after fixing the ticket key. -/
theorem Q79_FunctionalAddressKeyElisionGoal : Q79_FunctionalAddressKeyElisionGoal := by
  intro rows addrOf t hdep
  simp only [total, List.map_map, Function.comp_def]
  congr 1
  have hfilters : rows.filter (fun r => (r.1 == t) && (r.2.1 == addrOf t)) =
      rows.filter (fun r => r.1 == t) := by
    apply List.filter_congr
    intro r hr
    by_cases ht : (r.1 == t) = true
    · have hrt : r.1 = t := eq_of_beq ht
      have hra : r.2.1 = addrOf t := hrt ▸ hdep r hr
      simp [ht, hra]
    · simp [ht]
  rw [hfilters]

/-- The customer-level state partition is exactly the same filtered list used for the
state count and sum. -/
theorem Q81_StateAvgFromCustomerTotalsGoal : Q81_StateAvgFromCustomerTotalsGoal := by
  intro custTotals st
  rfl

/-- Restricting store rows to a selected top-k item set does not change the quantity
sum when the requested item belongs to that set. -/
theorem Q83_CommonItemTopKBeforeQtyLookupGoal : Q83_CommonItemTopKBeforeQtyLookupGoal := by
  intro sRows cItems wItems k item
  dsimp only
  intro hmem
  let top := best k (members (members (sRows.map Prod.fst) cItems) wItems)
  have hsel : ∀ r ∈ sRows, r.1 = item → top.contains r.1 = true := by
    intro r _ hri
    simp [top, hri, List.contains_iff_mem, hmem]
  have hfilters : sRows.filter (fun r => r.1 == item) =
      sRows.filter (fun r => (r.1 == item) && top.contains r.1) := by
    apply List.filter_congr
    intro r hr
    by_cases hri : (r.1 == item) = true
    · simp only [hri, Bool.true_and, hsel r hr (eq_of_beq hri)]
    · simp [hri]
  have hbase : ((sRows.filter (fun r => r.1 == item)).map Prod.snd).sum =
      (((sRows.filter (fun r => top.contains r.1)).filter (fun r => r.1 == item)).map Prod.snd).sum := by
    congr 1
    simp only [hfilters, List.filter_filter, Bool.and_comm]
  exact hbase

/-- Equality of two non-NULL demographic codes is exactly the lookup hit predicate. -/
theorem Q85_EqualNonNullDemoCodeFilterGoal : Q85_EqualNonNullDemoCodeFilterGoal := by
  intro x y hhit
  simp only [hit, Bool.and_eq_true, beq_iff_eq] at hhit
  exact ⟨hhit.2, hhit.1⟩

/-- Frequencies of distinct scores partition the rows, so summing frequencies above
the target counts exactly the rows above it. -/
theorem Q86_CompetitionRankFromHigherCountGoal : Q86_CompetitionRankFromHigherCountGoal := by
  intro scores x
  simp only [countWhere, frequency]
  exact (freq_sum (fun s : Int => decide (x < s)) scores).symm

/-- Restricting the subtraction-side membership marks to store candidates cannot change
whether a store candidate is found in either list. -/
theorem Q87_CandidateOnlyDifferenceMarksGoal : Q87_CandidateOnlyDifferenceMarksGoal := by
  intro α _ s c w
  dsimp only
  let sDedup := s.eraseDups
  congr 1
  apply List.filter_congr
  intro x hx
  have hs : x ∈ s.eraseDups := hx
  have hmem : ∀ ys : List α, (ys.filter (fun y => sDedup.contains y)).contains x =
      ys.contains x := by
    intro ys
    have hmarked : sDedup.contains x = true := by
      cases hs' : sDedup.contains x with
      | false =>
        have hnone : ¬ x ∈ sDedup := by
          intro hxmem
          have hxx : (x == x) = true := by
            cases hh : (x == x) with
            | false => simp [hh] at hxmem
            | true => exact hh
          have hall : ∃ y ∈ sDedup, (x == y) = true := ⟨x, hxmem, hxx⟩
          have hc : sDedup.contains x = true := by
            have hany := List.any_eq_true.mpr hall
            simpa only [List.contains_eq_any_beq] using hany
          simp [hs'] at hc
        exact False.elim (hnone hs)
      | true => rfl
    have hmemFilter : x ∈ ys.filter (fun y => sDedup.contains y) ↔ x ∈ ys := by
      constructor
      · intro h
        exact (List.mem_filter.mp h).1
      · intro hxys
        exact List.mem_filter.mpr ⟨hxys, hmarked⟩
    have hleft : (ys.filter (fun y => sDedup.contains y)).contains x = true ↔
        x ∈ ys.filter (fun y => sDedup.contains y) := by
      constructor
      · intro hc
        by_contra hb
        have hempty : ys.filter (fun y => sDedup.contains y) = [] := List.filter_eq_nil_iff.mpr hb
        simp [hempty] at hc
      · intro h
        have hxx : (x == x) = true := by
          cases hh : (x == x) with
          | false => simp [hh] at h
          | true => exact hh
        have hany := List.any_eq_true.mpr (show ∃ y ∈ ys.filter (fun y => sDedup.contains y),
          (x == y) = true from ⟨x, h, hxx⟩)
        have hc := List.contains_eq_any_beq (l := ys.filter (fun y => sDedup.contains y)) (a := x)
        rw [hc]
        exact hany
    have hright : ys.contains x = true ↔ x ∈ ys := by
      constructor
      · intro hc
        by_contra hb
        have hempty : ys = [] := List.filter_eq_nil_iff.mp (by
          simpa only [List.mem_filter, true_and] using hb)
        simp [hempty] at hc
      · intro h
        have hxx : (x == x) = true := by
          cases hh : (x == x) with
          | false => simp [hh] at h
          | true => exact hh
        have hany := List.any_eq_true.mpr (show ∃ y ∈ ys, (x == y) = true from ⟨x, h, hxx⟩)
        have hc := List.contains_eq_any_beq (l := ys) (a := x)
        rw [hc]
        exact hany
    have hiffBool : ((ys.filter (fun y => sDedup.contains y)).contains x = true) ↔
        (ys.contains x = true) := hleft.trans (hmemFilter.trans hright.symm)
    cases hleftC : (ys.filter (fun y => sDedup.contains y)).contains x <;>
      cases hrightC : ys.contains x <;>
      simp only [hleftC, hrightC] at hiffBool ⊢
    all_goals
      first | rfl | contradiction
  change (!c.contains x && !w.contains x) =
    (!(c.filter (fun y => sDedup.contains y)).contains x &&
      !(w.filter (fun y => sDedup.contains y)).contains x)
  rw [show (c.filter (fun y => sDedup.contains y)).contains x = c.contains x from hmem c,
    show (w.filter (fun y => sDedup.contains y)).contains x = w.contains x from hmem w]

/-- With positive year sum, conversion to Nat preserves the exact absolute-value
inequality boundary, for every positive month count. -/
theorem Q89_MonthlyDeviationBandExactCheckGoal : Q89_MonthlyDeviationBandExactCheckGoal := by
  intro m yearSum cnt hcnt hyear
  have hyear' : 0 ≤ yearSum := by omega
  have hypos : 0 < yearSum.toNat := by omega
  have habs : (yearSum.natAbs : Int) = yearSum.toNat := by
    rw [Int.natAbs_of_nonneg hyear', Int.toNat_of_nonneg hyear']
  have hnatToInt : (yearSum.toNat : Int) > 0 ↔ yearSum.toNat > 0 := by
    omega
  have hiff : (yearSum.natAbs : Int) > 0 ↔ yearSum.toNat > 0 := by
    rw [habs]
    exact hnatToInt
  constructor
  · intro h
    have hpos : (0 : Int) < 10 * Int.natAbs ((cnt : Int) * m - yearSum) := by omega
    have hnat : (0 : Nat) < 10 * Int.natAbs ((cnt : Int) * m - yearSum) := by
      exact_mod_cast hpos
    have hle : 10 * Int.natAbs ((cnt : Int) * m - yearSum) ≤
        10 * (yearSum.natAbs : Int) :=
      exact Int.mul_le_mul_of_nonneg_left (show
        Int.natAbs ((cnt : Int) * m - yearSum) ≤ Int.natAbs yearSum by
          have habs' : (Int.natAbs yearSum : Int) = yearSum := Int.natAbs_of_nonneg hyear'
          have hdelta := Int.le_natAbs (a := (cnt : Int) * m - yearSum)
          omega) (by decide)
    have hb : 10 * Int.natAbs ((cnt : Int) * m - yearSum) ≤ 10 * (yearSum.toNat : Nat) := by
      exact_mod_cast (show (10 * Int.natAbs ((cnt : Int) * m - yearSum) : Int) ≤
        (10 * (yearSum.toNat : Nat) : Int) from by simpa [habs] using hle)
    exact Nat.lt_of_lt_of_le
      (Nat.zero_lt_succ (10 * Int.natAbs ((cnt : Int) * m - yearSum))) hb
  · intro h
    have hb0 : 10 * Int.natAbs ((cnt : Int) * m - yearSum) < 10 * (yearSum.toNat : Nat) := by
      simpa only [Nat.mul_lt_mul_left (by decide : 0 < 10)] using h
    have hb : Int.natAbs ((cnt : Int) * m - yearSum) < yearSum.toNat := by
      exact Nat.lt_of_mul_lt_mul_left (a := yearSum.toNat)
        (b := Int.natAbs ((cnt : Int) * m - yearSum)) (c := 10) (by simpa [Nat.mul_comm] using hb0)
    have hb' : (Int.natAbs ((cnt : Int) * m - yearSum) : Int) < (yearSum.toNat : Int) := by
      exact_mod_cast hb
    omega

/-- Under the disjoint time-band assumption, a single OR count is the sum of the two
individual counts. -/
theorem Q90_SinglePassAmPmTimeBandCountsGoal : Q90_SinglePassAmPmTimeBandCountsGoal := by
  intro rows isAm isPm hdisj
  simp only [countWhere]
  have hdisj' : ∀ h, (isAm h && isPm h) = false := by
    intro h
    have hh := hdisj h
    cases hb : (isAm h && isPm h) with
    | false => rfl
    | true => simp [hb] at hh
  have hlen : (rows.filter (fun r => r.2 && (isAm r.1 || isPm r.1))).length =
      (rows.filter (fun r => r.2 && isAm r.1)).length +
        (rows.filter (fun r => r.2 && isPm r.1)).length := by
    induction rows with
    | nil => rfl
    | cons r rows ih =>
      have hhead : countWhere [r] (fun x => x.2 && (isAm x.1 || isPm x.1)) =
          countWhere [r] (fun x => x.2 && isAm x.1) +
            countWhere [r] (fun x => x.2 && isPm x.1) := by
        by_cases hr : r.2 = true <;> by_cases ha : isAm r.1 = true <;>
          by_cases hp : isPm r.1 = true
        · have hboth : (isAm r.1 && isPm r.1) = true := by simp [ha, hp]
          have hfalse := hdisj' r.1
          simp [hboth] at hfalse
        · simp [countWhere, hr, ha, hp]
        · simp [countWhere, hr, ha, hp]
        · simp [countWhere, hr, ha, hp]
        · simp [countWhere, hr, ha, hp]
        · simp [countWhere, hr, ha, hp]
        · simp [countWhere, hr, ha, hp]
        · simp [countWhere, hr, ha, hp]
      have hcons : ∀ p : Nat × Bool → Bool,
          countWhere (r :: rows) p = countWhere [r] p + countWhere rows p := by
        intro p
        cases hp : p r <;> simp [countWhere, hp, Nat.add_comm]
      simp only [countWhere] at hcons
      rw [hcons, hcons, hcons]
      change countWhere [r] (fun x => x.2 && (isAm x.1 || isPm x.1)) +
          (rows.filter (fun r => r.2 && (isAm r.1 || isPm r.1))).length =
        countWhere [r] (fun x => x.2 && isAm x.1) +
          (rows.filter (fun r => r.2 && isAm r.1)).length +
          (countWhere [r] (fun x => x.2 && isPm x.1) +
            (rows.filter (fun r => r.2 && isPm r.1)).length)
      rw [hhead, ih]
      omega
  exact hlen

/-- Replicating each run bucket by its run length counts exactly the sum of lengths of
runs carrying the target bucket. -/
theorem Q99_RunLengthDelayHistogramAccumGoal : Q99_RunLengthDelayHistogramAccumGoal := by
  intro runs target
  induction runs with
  | nil => simp [countWhere]
  | cons r runs ih =>
    change countWhere (List.replicate r.2 r.1 ++
      runs.flatMap (fun r => List.replicate r.2 r.1)) (fun b => b == target) =
      ((r :: runs).filter (fun r => r.1 == target) |>.map Prod.snd).sum
    change countWhere (List.replicate r.2 r.1 ++
      runs.flatMap (fun r => List.replicate r.2 r.1)) (fun b => b == target) =
      ((r :: runs).filter (fun r => r.1 == target) |>.map Prod.snd).sum
    have htail : countWhere (runs.flatMap (fun r => List.replicate r.2 r.1))
        (fun b => b == target) = (runs.filter (fun r => r.1 == target) |>.map Prod.snd).sum := ih
    rw [countWhere, List.filter_append, List.length_append]
    change (List.filter (fun b => b == target) (List.replicate r.2 r.1)).length +
      countWhere (runs.flatMap (fun r => List.replicate r.2 r.1)) (fun b => b == target) = _
    rw [htail]
    by_cases hr : (r.1 == target) = true
    · simp [List.filter_cons, hr, List.filter_replicate, Nat.add_comm]
    · have hr' : (r.1 == target) = false := by simpa using hr
      simp [List.filter_cons, hr', List.filter_replicate]

end TinylambOpt.ProposedProofs.BenchmarkRematch.TpcdsPart2Proofs
