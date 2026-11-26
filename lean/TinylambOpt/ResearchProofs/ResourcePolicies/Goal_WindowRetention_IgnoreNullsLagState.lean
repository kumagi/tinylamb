import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

/-!
`IgnoreNullsLagState`: with IGNORE NULLS the carried state holds only non-NULL
values, so composing two spill halos over `filterMap id` streams equals the halo
of the concatenated stream. SQL may copy a bounded non-NULL prefix rather than
all physical rows across a morsel boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_IgnoreNullsLagState

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

private theorem tail_compose {α : Type} (a b : List α) (w : Nat) :
    tailRows (tailRows a w ++ b) w = tailRows (a ++ b) w := by
  simp only [tailRows]
  generalize hA : a.drop (a.length - w) = A
  have hAl : A.length = min w a.length := by rw [← hA]; simp [List.length_drop]; omega
  by_cases hc : w ≤ A.length + b.length
  · generalize hP : a.take (a.length - w) = P
    have hPa : P ++ A = a := by rw [← hP, ← hA]; exact List.take_append_drop _ _
    rw [← hPa]
    simp only [List.length_append]
    have hidx : P.length + A.length + b.length - w = P.length + (A.length + b.length - w) := by
      omega
    rw [hidx]
    rw [List.append_assoc, List.drop_length_add_append]
  · have hnc : A.length + b.length < w := by omega
    rw [hAl] at hnc
    have halt : a.length < w := by omega
    have hA0 : A = a := by
      rw [← hA, Nat.sub_eq_zero_of_le (Nat.le_of_lt halt), List.drop_zero]
    rw [hA0]

theorem sound : IgnoreNullsLagState.Goal := by
  intro a b k
  rw [List.filterMap_append]
  exact tail_compose (a.filterMap id) (b.filterMap id) k

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_IgnoreNullsLagState
