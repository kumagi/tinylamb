import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_ConsecutiveHaloReplacement

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
A trailing-halo replacement composes: replacing the prefix `a` by its last `w`
rows before appending `b` leaves the last `w` rows of `(a ++ b)` unchanged. This
is the spill-boundary rule that lets a bounded ROWS-window state discard older
partition history without changing later frames.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.ConsecutiveHaloReplacement.Goal := by
  intro a b w
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
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WindowRetention.Goal_ConsecutiveHaloReplacement
