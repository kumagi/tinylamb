import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_VersionVacuum_EndpointSnapshotInterpolation
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
theorem refute : ¬ EndpointSnapshotInterpolation.Goal := by
  intro h
  let vs : List Version := [⟨5, some 1⟩, ⟨3, some 2⟩, ⟨1, some 3⟩]
  have h' := h vs 1 5 3 (by simp [ordered, vs]) (by decide) (by decide)
  simp [readAt, sparse, winner, vs] at h'
end Nov26.Proof_VersionVacuum_EndpointSnapshotInterpolation
