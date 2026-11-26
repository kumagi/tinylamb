import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_ManifestCutCannotExceedDurability
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem sound : ManifestCutCannotExceedDurability.Goal := by
  intro gs wal disk g h
  have hr : ready wal disk g = true := by
    have := List.find?_some h
    simpa [choose] using this
  simp only [ready, Bool.and_eq_true, decide_eq_true_eq, List.all_eq_true] at hr
  constructor
  · exact hr.1
  · intro f hf
    have := hr.2 f hf
    simpa [List.contains_iff_mem, decide_eq_true_eq] using this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_ManifestCutCannotExceedDurability
