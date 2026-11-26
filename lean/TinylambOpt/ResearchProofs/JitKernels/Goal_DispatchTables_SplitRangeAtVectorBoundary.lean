import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! One CASE arm can be split at a vector boundary into two adjacent half-open
arms without changing any key's dispatch result. Both halves keep the original
result and their union is the original range, so surrounding arms keep their
first-match priority. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitRangeAtVectorBoundary

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem sound : SplitRangeAtVectorBoundary.Goal := by
  intro before a after mid x hlo hhi
  induction before with
  | nil =>
      simp only [pick, List.nil_append, List.find?_cons]
      by_cases hax : holds a x
      · have hax' : a.lo ≤ x ∧ x < a.hi := by simpa [holds] using hax
        by_cases hxm : x < mid
        · have hp : holds { a with hi := mid } x = true := by
            simp [holds, hxm]
            omega
          simp [hax, hp]
        · have hp : holds { a with hi := mid } x = false := by
            simp [holds]
            omega
          have hp2 : holds { a with lo := mid } x = true := by
            simp [holds]
            omega
          simp [hax, hp, hp2]
      · have hax' : ¬ (a.lo ≤ x ∧ x < a.hi) := by
          intro h
          exact hax (by simpa [holds] using h)
        have hp : holds { a with hi := mid } x = false := by
          simp [holds]
          omega
        have hp2 : holds { a with lo := mid } x = false := by
          simp [holds]
          omega
        simp [hax, hp, hp2]
  | cons b bs ih =>
      by_cases hb : holds b x
      · simp [pick, hb]
      · simpa [pick, hb] using ih

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitRangeAtVectorBoundary
