import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.ConsecutiveHaloReplacement

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

theorem sound : ConsecutiveHaloReplacement.Goal := by
  intro a b w
  simp only [tailRows, List.length_append, List.length_drop]
  rw [List.drop_append, List.drop_append]
  simp only [List.length_drop]
  -- LHS: drop (a.length - (a.length-w) + b.length - w) (drop (a.length-w) a)
  --      ++ drop (a.length - (a.length-w) + b.length - w - (a.length - (a.length-w))) b
  -- RHS: drop (a.length + b.length - w) a
  --      ++ drop (a.length + b.length - w - a.length) b
  congr 1
  · -- left parts
    rw [List.drop_drop]
    congr 1
    omega
  · -- right parts
    congr 1
    omega

end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.ConsecutiveHaloReplacement
