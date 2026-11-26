import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_WeekdayPivotBySparseDateRuns

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
A sparse weekday pivot may consume each repeated date run once, weighting its
NULL-preserving subtotal by the run length. Expanding the run first and grouping
the expanded rows by weekday gives the same nullable SUM, while zero-length runs
contribute nothing to either side.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- Grouping distributes over concatenation of row lists. -/
theorem forKey_append {α β : Type} [BEq α] (xs ys : List (α × β)) (k : α) :
    forKey (xs ++ ys) k = forKey xs k ++ forKey ys k := by
  simp [forKey, List.filter_append, List.map_append]

/-- Grouping a repeated run yields either the repeated payloads or nothing. -/
theorem forKey_replicate {α β : Type} [BEq α] [LawfulBEq α]
    (r : α × β) (k : α) (n : Nat) :
    forKey (List.replicate n r) k = if r.1 == k then List.replicate n r.2 else [] := by
  by_cases h : r.1 == k
  · have heq : r.1 = k := beq_iff_eq.mp h
    subst k
    simp [forKey, h]
  · have hf : (List.replicate n r).filter (fun row => row.1 == k) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro row hr
      have hrow : (row.1 == k) = true := (List.mem_filter.mp hr).2
      have hmem : row ∈ List.replicate n r := (List.mem_filter.mp hr).1
      have hreq : row = r := List.eq_of_mem_replicate hmem
      have hrk : r.1 = k := by
        rw [← hreq]
        exact beq_iff_eq.mp hrow
      exact h (by simp [hrk])
    simp [forKey, h, hf]

/-- Expanding runs and grouping by weekday is a permutation of grouping the
    selected runs directly; zero-length runs are dropped from the right side. -/
theorem forKey_flatMap_replicate (runs : List ((Nat × Option Int) × Nat)) weekday :
    (forKey (runs.flatMap (fun (r, n) => List.replicate n r)) weekday).Perm
      ((runs.filter (fun r => r.1.1 == weekday && 0 < r.2)).flatMap
        (fun ((_, v), n) => List.replicate n v)) := by
  induction runs with
  | nil => simp [forKey]
  | cons run runs ih =>
      rcases run with ⟨⟨d, v⟩, n⟩
      simp only [List.flatMap_cons, forKey_append, forKey_replicate]
      by_cases hd : (d == weekday) = true
      · by_cases hn : 0 < n
        · simpa [hd, hn, List.Perm.append_left] using List.Perm.append_left (List.replicate n v) ih
        · have hn0 : n = 0 := by omega
          simpa [hd, hn0] using ih
      · simpa [hd] using ih

theorem sound : WeekdayPivotBySparseDateRuns.Goal := by
  intro runs weekday
  dsimp
  let selected := runs.filter (fun r => r.1.1 == weekday && 0 < r.2)
  have hperm := forKey_flatMap_replicate runs weekday
  rw [nsum_perm hperm]
  rw [List.flatMap_def]
  rw [← nsum_map_nsum]
  rw [List.map_map]
  congr 1
  apply List.map_congr_left
  intro r hr
  rcases r with ⟨⟨d, v⟩, n⟩
  have hn : n ≠ 0 := by
    have hr' : (((d == weekday) && decide (0 < n)) : Bool) = true := (List.mem_filter.mp hr).2
    have hpos : 0 < n := of_decide_eq_true ((Bool.and_eq_true_iff.mp hr').2)
    exact Nat.ne_of_gt hpos
  change nsum (List.replicate n v) = _
  rw [nsum_replicate]
  simp [hn, Int.mul_comm]
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_WeekdayPivotBySparseDateRuns
