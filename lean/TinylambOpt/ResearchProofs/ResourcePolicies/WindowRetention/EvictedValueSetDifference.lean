import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.EvictedValueSetDifference

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

theorem sound : Not EvictedValueSetDifference.Goal := by
  intro h
  have hp := h 1 [1] 2
  have hmem : (1 : Nat) ∈ (List.eraseDups [1, 2]) := by decide
  have hmem' : (1 : Nat) ∈ (List.eraseDups ((List.eraseDups [1, 1]).erase 1 ++ [2])) := hp.symm.mem_iff.mp hmem
  revert hmem'
  decide

end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.EvictedValueSetDifference
