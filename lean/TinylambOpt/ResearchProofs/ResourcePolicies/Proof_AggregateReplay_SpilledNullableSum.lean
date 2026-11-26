import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_SpilledNullableSum

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: flushing partitions of a nullable SUM keeps the exact sum
    and the non-NULL count, so the final SQL answer (including all-NULL groups)
    is unchanged. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- `state` is additive over concatenation of value lists. -/
theorem state_append (xs ys : List (Option Int)) :
    state (xs ++ ys) = combine (state xs) (state ys) := by
  simp [state, combine, List.filterMap_append, List.sum_append]

/-- Pair addition is associative. -/
theorem combine_assoc (x y z : State) : combine (combine x y) z = combine x (combine y z) := by
  rcases x with ⟨sx, cx⟩
  rcases y with ⟨sy, cy⟩
  rcases z with ⟨sz, cz⟩
  simp [combine, Int.add_assoc, Nat.add_assoc]

/-- Zero is a left identity for the accumulator. -/
theorem combine_zero (s : State) : combine (0, 0) s = s := by
  rcases s with ⟨ss, cs⟩
  simp [combine]

/-- Folding partition states from an accumulator accumulates the state of the
    concatenated partition payloads. -/
theorem fold_states {α : Type} [BEq α] (ps : List (List (α × Option Int))) (k : α) (acc : State) :
    (ps.map (fun rs => state (forKey rs k))).foldl combine acc =
      combine acc (state (forKey ps.flatten k)) := by
  induction ps generalizing acc with
  | nil => simp [forKey, state, combine]
  | cons rs ps ih =>
      simp only [List.map_cons, List.foldl_cons, List.flatten_cons]
      rw [show forKey (rs ++ ps.flatten) k = forKey rs k ++ forKey ps.flatten k by
        simp [forKey, List.filter_append, List.map_append]]
      rw [state_append]
      rw [ih]
      rw [combine_assoc]

/-- `answer` of a raw state equals `nsum` of its values. -/
theorem answer_state (vs : List (Option Int)) : answer (state vs) = nsum vs := by
  simp [answer, state, nsum]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.SpilledNullableSum.Goal := by
  intro ps k
  rw [fold_states]
  rw [combine_zero]
  exact answer_state (forKey ps.flatten k)
end TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_SpilledNullableSum
