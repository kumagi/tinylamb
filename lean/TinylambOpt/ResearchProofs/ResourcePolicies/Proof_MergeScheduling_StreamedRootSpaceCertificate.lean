import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_StreamedRootSpaceCertificate

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `StreamedRootSpaceCertificate`: when each machine term already
    fits the cap, the streamed root exceeds the cap precisely when its final
    write-out does. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.StreamedRootSpaceCertificate.Goal := by
  intro a b cap ha hb hab
  simp only [TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.StreamedRootSpaceCertificate.Goal,
    peak, temp] at *
  constructor <;> intro hgt <;> omega
end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_StreamedRootSpaceCertificate
