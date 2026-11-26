import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.ArrivalIndependentCohortWait

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem sound : ArrivalIndependentCohortWait.Goal := by
  intro q arrivals t ht
  simp only [waitTurns]
  have htake : (q ++ arrivals).takeWhile (· != t) = q.takeWhile (· != t) := by
    induction q with
    | nil => simp at ht
    | cons a as ih =>
      by_cases h : (a != t) = true
      · have ht' : t ∈ as := by
          rcases List.mem_cons.mp ht with rfl | ht'
          · simp at h
          · exact ht'
        simp [List.takeWhile_cons, h, ih ht']
      · simp [List.takeWhile_cons, h]
  rw [htake]

end TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.ArrivalIndependentCohortWait
