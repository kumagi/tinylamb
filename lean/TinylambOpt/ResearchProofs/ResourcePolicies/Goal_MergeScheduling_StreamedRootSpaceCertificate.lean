import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.StreamedRootSpaceCertificate.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_StreamedRootSpaceCertificate

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem goal : StreamedRootSpaceCertificate.Goal := by
  intro a b cap ha hb hab
  simp only [StreamedRootSpaceCertificate.Goal, peak, temp, size] at *
  constructor
  · intro hgt
    -- hgt: cap < max(peak a, max(temp a+peak b, temp a+temp b+(size a+size b)))
    -- since peak a <= cap and temp a+peak b <= cap, third must be > cap
    omega
  · intro hgt
    -- third term > cap implies max > cap
    omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_StreamedRootSpaceCertificate
