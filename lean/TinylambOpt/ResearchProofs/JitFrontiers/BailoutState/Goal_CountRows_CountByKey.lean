import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_CountRows_CountByKey

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem countAux (s : State) (rs : List Row) (k : Nat) :
    ((refRun CountRows.policy s rs).lookup k).getD 0 =
      (s.lookup k).getD 0 + (rs.countP (fun r => r.1 == k) : Int) := by
  induction rs generalizing s with
  | nil => simp [refRun]
  | cons r rs ih =>
      by_cases heq : r.1 == k
      · have hEq : r.1 = k := beq_iff_eq.mp heq
        have hk : CountRows.policy.key r = some k := by simp [CountRows.policy, hEq]
        rw [refRun_cons]
        rw [ih (step CountRows.policy s r)]
        rw [lookup_step_same CountRows.policy s r k hk]
        simp only [Option.getD_some, CountRows.policy]
        simp [heq]
        omega
      · have hne : k ≠ r.1 := by
          intro h
          subst h
          simp at heq
        have hk : CountRows.policy.key r = some r.1 := by simp [CountRows.policy]
        rw [refRun_cons]
        rw [ih (step CountRows.policy s r)]
        rw [lookup_step_other CountRows.policy s r k r.1 hk hne]
        simp [heq]


theorem sound : CountRows.CountByKey.Goal := by
  intro rs k
  simpa using countAux [] rs k

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_CountRows_CountByKey
