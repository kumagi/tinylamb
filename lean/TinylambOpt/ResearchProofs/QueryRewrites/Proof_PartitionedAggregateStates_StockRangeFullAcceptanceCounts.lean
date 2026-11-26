import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_StockRangeFullAcceptanceCounts

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Zone-map page bounds can replace a per-row strict comparison: a page whose
maximum is below the sold price is wholly accepted, a page whose minimum is at
least the sold price is wholly rejected, and otherwise the exact per-row count
is used. All three cases agree with scanning the flattened pages.
-/

set_option maxHeartbeats 0

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- A page's three-way contribution equals its exact strict count. -/
theorem page_contribution (p : List Int) (sold : Int) :
    (if p.max?.any (· < sold) then p.length
     else if p.min?.any (sold ≤ ·) then 0 else countWhere p (· < sold)) =
      countWhere p (· < sold) := by
  by_cases hmax : p.max?.any (· < sold) = true
  · have hm : ∃ m, p.max? = some m ∧ (decide (m < sold) : Bool) = true :=
      (Option.any_eq_true (fun x => decide (x < sold)) p.max?).mp hmax
    rcases hm with ⟨m, hpm, hmdec⟩
    have hmlt : m < sold := of_decide_eq_true hmdec
    have hall : ∀ x ∈ p, (decide (x < sold) : Bool) = true := by
      intro x hx
      have hle : x ≤ m := (List.max?_eq_some_iff.mp hpm).2 x hx
      exact decide_eq_true (Int.lt_of_le_of_lt hle hmlt)
    have hf : p.filter (fun x => decide (x < sold)) = p := List.filter_eq_self.mpr hall
    simp [hmax, countWhere, hf]
  · simp only [hmax, Bool.false_eq_true, ↓reduceIte]
    by_cases hmin : p.min?.any (sold ≤ ·) = true
    · have hm : ∃ m, p.min? = some m ∧ (decide (sold ≤ m) : Bool) = true :=
        (Option.any_eq_true (fun x => decide (sold ≤ x)) p.min?).mp hmin
      rcases hm with ⟨m, hpm, hmdec⟩
      have hmsold : sold ≤ m := of_decide_eq_true hmdec
      have hf : p.filter (fun x => decide (x < sold)) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro x hx
        have hx' := List.mem_filter.mp hx
        have hle : m ≤ x := (List.min?_eq_some_iff.mp hpm).2 x hx'.1
        have hxlt : x < sold := of_decide_eq_true hx'.2
        omega
      simp [hmin, countWhere, hf]
    · simp [hmin]

/-- Strict counts add over a concatenation of pages. -/
theorem countWhere_flatten (pages : List (List Int)) (sold : Int) :
    countWhere pages.flatten (· < sold) =
      (pages.map (fun p => countWhere p (· < sold))).sum := by
  induction pages with
  | nil => simp [countWhere]
  | cons p ps ih => simp [countWhere, List.filter_append, Function.comp_def, ih]

theorem sound : StockRangeFullAcceptanceCounts.Goal := by
  intro pages sold
  rw [countWhere_flatten]
  induction pages with
  | nil => simp
  | cons p ps ih =>
      simp [page_contribution, Function.comp_def, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_StockRangeFullAcceptanceCounts
