import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.PeerKeysBoundBufferedRows

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

theorem sound : Not PeerKeysBoundBufferedRows.Goal := by
  intro h
  have := h [1, 1, 1] 1
  revert this
  decide

end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.PeerKeysBoundBufferedRows
