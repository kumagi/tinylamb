import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.StreamedRootSpaceCertificate

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

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.StreamedRootSpaceCertificate
