import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SameBranch

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

/-- `pairs a b = Σ_{x∈a} b.count x`. Splitting `a` into the two stable branches
    of the tested bit splits the outer sum, while each element's count in `b`
    is unchanged: an element with the bit set is absent from the lower branch
    of `b` (and all its occurrences lie in the upper branch), and conversely. -/
theorem sound_SameBranch : SameBranch.Goal := by
  intro a b bit
  unfold pairs
  induction a with
  | nil => simp [lower, upper]
  | cons x xs ih =>
      have ih' := ih
      simp [lower, upper] at ih'
      by_cases hxt : x.testBit bit
      · simp [lower, upper, hxt]
        rw [ih']
        omega
      · have hxf : x.testBit bit = false := by
          cases h : x.testBit bit <;> simp_all
        simp [lower, upper, hxf]
        rw [ih']
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_SameBranch
