import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.SingleEditDirectoryGrowth

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ SingleEditDirectoryGrowth.Goal := by
  intro h
  have := h [] [0, 0] 127 1 (by decide)
  revert this
  simp [directory, offset, streamBytes]
  decide

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.SingleEditDirectoryGrowth
