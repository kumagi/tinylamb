import Std
import TinylambOpt.ProposedGoals.BenchmarkRematch.Tpch
import TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.SupplierYearVolumePreaggregation

/-!
# TPC-H benchmark rematch proofs

This module proves all eight proposed TPC-H soundness goals. The assumptions are
those stated by each goal: block-wide bounds, nonnegative component revenues,
unique supplier keys, and membership of the examined candidate pair.

The results justify changing scan and aggregate execution without changing the
modeled SQL result: dense block totals may replace row filtering (Q01, Q06),
sound upper bounds may prune pending top-k work (Q03), directional nation pairs
may be accumulated separately (Q07), supplier lookup may follow preaggregation
(Q08), zero-degree outer-join output may be counted complementarily (Q13), a
shared shipment predicate may be factored (Q19), and candidate-restricted
quantity sums may be used for Q20.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ProposedProofs.BenchmarkRematch.Tpch

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.Tpch
open TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.SupplierYearVolumePreaggregation
  (sum_by_keys forKey_filter_peru)

/-- The sum over a disjunction of two filters is the sum of their separate
totals whenever the two predicates cannot accept the same row. This lets Q07
accumulate each directed nation pair in its own slot without double counting. -/
private theorem total_filter_or_of_disjoint {α : Type} (rows : List α)
    (p q : α → Bool) (value : α → Int)
    (hdisjoint : ∀ r, p r = true → q r = true → False) :
    total (rows.filter (fun r => p r || q r)) value =
      total (rows.filter p) value + total (rows.filter q) value := by
  induction rows with
  | nil => simp [total]
  | cons r rows ih =>
      have ih' := ih
      unfold total at ih'
      by_cases hp : p r = true
      · by_cases hq : q r = true
        · exact False.elim (hdisjoint r hp hq)
        · simp only [List.filter_cons, hp, hq, Bool.or_true, Bool.or_false,
            Bool.true_and, Bool.false_and, Bool.false_eq_true, ↓reduceIte,
            total, List.map_cons, List.sum_cons]
          rw [ih']
          omega
      · by_cases hq : q r = true
        · simp only [List.filter_cons, hp, hq, Bool.or_true, Bool.or_false,
            Bool.true_and, Bool.false_and, Bool.false_eq_true, ↓reduceIte,
            total, List.map_cons, List.sum_cons]
          rw [ih']
          omega
        · simp only [List.filter_cons, hp, hq, Bool.or_true, Bool.or_false,
            Bool.true_and, Bool.false_and, Bool.false_eq_true, ↓reduceIte,
            total, List.map_cons, List.sum_cons]
          rw [ih']

/-- Componentwise nonnegative zip bounds imply the same ordering of list sums.
This helper lets Q03 apply a strict aggregate bound to a pending order. -/
private theorem sum_le_sum_of_nonnegative_zip (rev bound : List Int)
    (hlen : rev.length = bound.length)
    (hzip : ∀ p ∈ rev.zip bound, 0 ≤ p.1 ∧ p.1 ≤ p.2) :
    rev.sum ≤ bound.sum := by
  induction rev generalizing bound with
  | nil =>
      cases bound with
      | nil => simp
      | cons b bs => simp at hlen
  | cons r rev ih =>
      cases bound with
      | nil => simp at hlen
      | cons b bound =>
          have hlen' : rev.length = bound.length := by simpa using hlen
          have hzip' : ∀ p ∈ rev.zip bound, 0 ≤ p.1 ∧ p.1 ≤ p.2 := by
            intro p hp
            apply hzip
            simp [hp]
          have hrb : 0 ≤ r ∧ r ≤ b := hzip (r, b) (by simp)
          have hsum := ih bound hlen' hzip'
          simp only [List.sum_cons]
          omega

/-- Q01: when every block meets the ship-date cutoff, flattening the per-block
group totals equals filtering the flattened rows. Execution may therefore skip
the redundant date predicate while retaining the exact dense-code totals. -/
theorem Q01_InteriorBlockDenseCodeTotals :
    Q01_InteriorBlockDenseCodeTotalsGoal := by
  intro blocks cutoff k hblocks
  induction blocks with
  | nil => rfl
  | cons b blocks ih =>
      have hb : ∀ r ∈ b, r.1 ≤ cutoff :=
        fun r hr => hblocks b (by simp) r hr
      have hrest : ∀ b' ∈ blocks, ∀ r ∈ b', r.1 ≤ cutoff :=
        fun b' hb' r hr => hblocks b' (by simp [hb']) r hr
      have hfilter : b.filter
          (fun r => decide (r.1 ≤ cutoff) && (r.2.1 == (k : Int))) =
          b.filter (fun r => r.2.1 == (k : Int)) := by
        apply List.filter_congr
        intro r hr
        have hle := hb r hr
        simp [hle]
      rw [List.flatten_cons, List.filter_append]
      simp only [total, List.map_append, List.map_cons, List.sum_append, List.sum_cons]
      have htail := ih hrest
      simp only [total] at htail
      rw [hfilter, htail]

/-- Q03: equal-length nonnegative revenues bounded componentwise by `bound`
have a total no greater than `bound.sum`. A pending order whose strict upper
bound is below the achieved top-k threshold can therefore be pruned. -/
theorem Q03_NonnegativeRevenueUpperBoundDominance :
    Q03_NonnegativeRevenueUpperBoundDominanceGoal := by
  intro rev bound tau hlen hzip hbound
  have hsum := sum_le_sum_of_nonnegative_zip rev bound hlen hzip
  omega

/-- Q06: inside a block wholly contained in the date window, the date test is
always true. Mask evaluation may omit that test while preserving the exact
discounted revenue sum. -/
theorem Q06_InteriorDateMaskRevenueSum :
    Q06_InteriorDateMaskRevenueSumGoal := by
  intro rows dLo dHi qHi discOk hdates
  have hfilter : rows.filter (fun r =>
      decide (dLo ≤ r.1 ∧ r.1 < dHi) && discOk r.2.1 && decide (r.2.2.1 < qHi)) =
      rows.filter (fun r => discOk r.2.1 && decide (r.2.2.1 < qHi)) := by
    apply List.filter_congr
    intro r hr
    have hd := hdates r hr
    simp [hd.1, hd.2]
  exact congrArg (fun rs => total rs (fun r => r.2.2.2)) hfilter

/-- Q07: for distinct target nations, the two directed equality predicates
are disjoint. Their revenue totals may be accumulated independently and added,
which is the two-slot directional array used by the optimized execution. -/
theorem Q07_OrderedNationPairPartition :
    Q07_OrderedNationPairPartitionGoal := by
  intro rows n1 n2 hne
  apply total_filter_or_of_disjoint
  intro r hp hq
  simp only [Bool.and_eq_true, beq_iff_eq] at hp hq
  exact hne (hp.1.symm.trans hq.1)

/-- Q08: with unique supplier keys covering every row, the target-nation volume
equals the sum of target-key subtotals. The executor may preaggregate volume by
`(year, supplier)` and perform the nation classification once per supplier key. -/
theorem Q08_SupplierPreaggMarketShare :
    Q08_SupplierPreaggMarketShareGoal := by
  intro rows isTarget keys hnodup hcover
  let selected := rows.filter (fun r => isTarget r.1)
  let targetKeys := keys.filter isTarget
  have hselectedCover : ∀ r ∈ selected, r.1 ∈ targetKeys := by
    intro r hr
    have hr' := List.mem_filter.mp hr
    exact List.mem_filter.mpr ⟨hcover r hr'.1, hr'.2⟩
  have htargetNodup : targetKeys.Nodup := hnodup.filter isTarget
  have hsum := sum_by_keys targetKeys selected hselectedCover htargetNodup
  have hselectedTotals : (selected.map Prod.snd).sum =
      (targetKeys.map (fun s => total (selected.filter (fun r => r.1 == s)) Prod.snd)).sum := by
    simpa only [total, forKey] using hsum
  have horiginalTotals :
      (targetKeys.map (fun s => total (selected.filter (fun r => r.1 == s)) Prod.snd)).sum =
      (targetKeys.map (fun s => total (rows.filter (fun r => r.1 == s)) Prod.snd)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro s hs
    have htarget := (List.mem_filter.mp hs).2
    have hf := forKey_filter_peru rows s isTarget htarget
    simpa only [selected, total, forKey] using congrArg List.sum hf
  simpa only [total, selected, targetKeys] using
    hselectedTotals.trans horiginalTotals

/-- Every customer has either zero or a positive qualifying-order degree, so
the two complementary counts sum to the customer count. Q13 may represent
unmatched customers by a complement count instead of emitting NULL-padded rows. -/
theorem Q13_LeftJoinZeroDegreeComplement :
    Q13_LeftJoinZeroDegreeComplementGoal := by
  intro custs deg
  induction custs with
  | nil => simp [countWhere]
  | cons c custs ih =>
      by_cases hz : deg c = 0
      · have hp : decide (0 < deg c) = false := by simp [hz]
        simp only [countWhere, List.length_cons, List.filter_cons] at ih ⊢
        rw [← ih]
        simp [hz, hp, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hp : decide (0 < deg c) = true := by
          simp only [decide_eq_true_eq]
          omega
        simp only [countWhere, List.length_cons, List.filter_cons] at ih ⊢
        rw [← ih]
        simp [hz, hp, Nat.add_assoc]

/-- Q19: a shipment predicate shared by all three disjuncts can be evaluated
before the branch predicates. Boolean distributivity preserves exactly the
qualifying rows and hence their revenue total. -/
theorem Q19_SharedShipmentPredicateFactoring :
    Q19_SharedShipmentPredicateFactoringGoal := by
  intro α rows common b1 b2 b3 rev
  have hfilter : rows.filter (fun r =>
      (common r && b1 r) || (common r && b2 r) || (common r && b3 r)) =
      (rows.filter common).filter (fun r => b1 r || b2 r || b3 r) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro r hr
    cases hc : common r <;> cases h1 : b1 r <;>
      cases h2 : b2 r <;> cases h3 : b3 r <;> rfl
  exact congrArg (fun rs => total rs rev) hfilter

/-- Q20: for a pair known to belong to the candidate set, adding the candidate
semi-join before the key filter removes no matching lineitem. The correlated
quantity threshold may therefore use the candidate-restricted scan unchanged. -/
theorem Q20_CandidatePairQuantitySumEquiv :
    Q20_CandidatePairQuantitySumEquivGoal := by
  intro lineitems cands k hk
  have hcontains : cands.contains k = true := List.contains_iff_mem.mpr hk
  have hfilter : lineitems.filter (fun r => r.1 == k) =
      (lineitems.filter (fun r => cands.contains r.1)).filter (fun r => r.1 == k) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro r hr
    by_cases heq : r.1 == k
    · have hrk : r.1 = k := by simpa using heq
      simp only [hrk, hcontains, Bool.true_and, Bool.and_true]
    · simp [heq]
  exact congrArg (fun rs => total rs Prod.snd) hfilter

end TinylambOpt.ProposedProofs.BenchmarkRematch.Tpch
