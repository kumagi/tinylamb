import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# NonnegativeRevenueUpperBoundPruning

A pending revenue total cannot change the ten largest values when a valid upper bound is
strictly below at least ten known totals.

A top-ten aggregate can skip shipment rows for that order once the bound and the ten
qualifying totals are established.
-/

set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_NonnegativeRevenueUpperBoundPruning
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments
open TinylambOpt.ResearchGoals.QueryRewrites


theorem filter_l1_empty (x upper : Int) (l₁ : List Int)
    (hl1 : ∀ b ∈ l₁, (!decide (x ≤ b)) = true) (hxu : x ≤ upper) :
    l₁.filter (fun y => decide (upper < y)) = [] := by
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro b hb
  have hnb := hl1 b (List.mem_filter.mp hb).1
  have hnb' : ¬ x ≤ b := by simpa using hnb
  have hbx : b < x := by omega
  have hub : ¬ upper < b := by omega
  simp [hub] at hb

theorem filter_len_perm (p : Int → Bool) (l m : List Int) (h : l.Perm m) :
    (l.filter p).length = (m.filter p).length := by
  exact (h.filter p).length_eq


theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.NonnegativeRevenueUpperBoundPruning.Goal := by
  intro known pending upper hsum hcount
  simp [countWhere] at hcount
  obtain ⟨l₁, l₂, hms', hms, hl1⟩ := List.mergeSort_cons (le := fun a b : Int => decide (a ≤ b))
    (by intro a b c h1 h2; simp at *; omega)
    (by intro a b; simp; omega) pending.sum known
  have hperm := List.mergeSort_perm (le := (fun a b : Int => decide (a ≤ b))) known
  have hflen : (known.filter (fun y => decide (upper < y))).length =
      ((l₁ ++ l₂).filter (fun y => decide (upper < y))).length := by
    rw [← hms]
    exact (TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_NonnegativeRevenueUpperBoundPruning.filter_len_perm (fun y => decide (upper < y)) known.mergeSort known hperm).symm
  have hl1empty : l₁.filter (fun y => decide (upper < y)) = [] := by
    exact TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_NonnegativeRevenueUpperBoundPruning.filter_l1_empty pending.sum upper l₁ (by simpa using hl1) hsum
  have hl2len : 10 ≤ l₂.length := by
    rw [List.filter_append, hl1empty] at hflen
    simp at hflen
    have hfil : ((l₂.filter (fun y => decide (upper < y))).length) ≤ l₂.length :=
      List.length_filter_le (fun y => decide (upper < y)) l₂
    omega
  rw [hms', hms]
  simp only [List.reverse_append, List.reverse_cons, List.append_assoc]
  rw [List.take_append_of_le_length (by simpa using hl2len : 10 ≤ l₂.reverse.length)]
  rw [List.take_append_of_le_length (by simpa using hl2len : 10 ≤ l₂.reverse.length)]
end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_NonnegativeRevenueUpperBoundPruning
