import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_IgnoreNullsLagState

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
IGNORE NULLS lag state keeps only the last `k` non-NULL payloads. Because
filtering NULLs distributes over append, replacing the old morsel's kept payloads
by their trailing halo yields the same kept payloads as filtering the combined
stream, so a fixed-width non-NULL state suffices across a boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem halo_compose {α : Type} (a b : List α) (w : Nat) :
    TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.tailRows
        (TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.tailRows a w ++ b) w =
      TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.tailRows (a ++ b) w := by
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.tailRows
  simp only [List.length_append, List.length_drop]
  by_cases ha : a.length ≤ w
  · have hsub : a.length - w = 0 := by omega
    have hlenTail : a.length - (a.length - w) = a.length := by omega
    simp [hsub]
  · have haw : w < a.length := Nat.lt_of_not_ge ha
    have hlenTail : a.length - (a.length - w) = w := by omega
    rw [hlenTail]
    have hidxL : w + b.length - w = b.length := by omega
    rw [hidxL]
    have hidxR : a.length + b.length - w = (a.length - w) + b.length := by omega
    rw [hidxR, ← List.drop_drop]
    rw [List.drop_append_of_le_length (by omega : a.length - w ≤ a.length)]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.IgnoreNullsLagState.Goal := by
  intro a b k
  simpa [List.filterMap_append] using (halo_compose (a.filterMap id) (b.filterMap id) k)
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_IgnoreNullsLagState
