import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_RestartSeekMatchesPrefix
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem sound : RestartSeekMatchesPrefix.Goal := by
  intro ns s i hs hi
  unfold seek anchor offset
  have ha : i / s * s ≤ i := Nat.div_mul_le_self i s
  have hsplit : ns.take i = ns.take (i / s * s) ++ (ns.drop (i / s * s)).take (i - i / s * s) := by
    rw [← List.take_add]
    congr 1
    omega
  rw [hsplit, List.sum_append]
end Nov26.Proof_RestartDirectory_RestartSeekMatchesPrefix
