import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_RestartSeekMatchesPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
Restart-based seeking may decode from the recorded anchor and then continue to
the requested row, provided the anchor is not past that row.

The proof keeps all frame lengths unchanged: restarting at a boundary merely
splits the prefix sum of the same length stream, so SQL sees the same offset.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RestartSeekMatchesPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem sound : RestartSeekMatchesPrefix.Goal := by
  intro ns s i hs hi
  let a := anchor s i
  have ha_le_i : a ≤ i := by
    dsimp [a, anchor]
    exact Nat.div_mul_le_self i s
  have htake : (ns.take i).take a = ns.take a := by
    rw [List.take_take]
    rw [Nat.min_eq_left ha_le_i]
  have hdrop : (ns.take i).drop a = (ns.drop a).take (i - a) := by
    exact List.drop_take
  have hsplit : ns.take i = ns.take a ++ (ns.drop a).take (i - a) := by
    have h := List.take_append_drop a (ns.take i)
    rw [← h, htake, hdrop]
  change offset ns a + ((ns.drop a).take (i - a)).sum = offset ns i
  unfold offset
  rw [hsplit, List.sum_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RestartSeekMatchesPrefix
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_RestartSeekMatchesPrefix
