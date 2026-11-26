import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_PinnedGenerationFileClosure
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem sound : PinnedGenerationFileClosure.Goal := by
  intro disk roots g w hg
  simp only [ready]
  congr 1
  rw [Bool.eq_iff_iff]
  simp only [List.all_eq_true, decide_eq_true_eq, List.contains_iff_mem]
  constructor
  · intro h f hf
    have hs : f ∈ sweep disk roots := h f hf
    simp only [sweep, List.mem_filter, decide_eq_true_eq] at hs
    exact hs.1
  · intro h f hf
    simp only [sweep, List.mem_filter, decide_eq_true_eq]
    refine ⟨h f hf, ?_⟩
    exact List.any_eq_true.mpr ⟨g, hg, by simpa [List.contains_iff_mem] using hf⟩
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_PinnedGenerationFileClosure
