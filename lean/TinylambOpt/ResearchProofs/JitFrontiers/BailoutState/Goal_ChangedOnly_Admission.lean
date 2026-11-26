import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ChangedOnly_Admission

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem admissionAux (s : State) (rs : List Row) (k : Nat) :
    (refRun ChangedOnly.policy s rs).lookup k = some 1 ↔
      (s.lookup k = some 1) ∨ ∃ r ∈ rs, r.1 = k ∧ r.2 ≠ some 0 := by
  induction rs generalizing s with
  | nil => simp [refRun]
  | cons r rs ih =>
      cases r with
      | mk j v =>
          by_cases hv : v == some 0
          · have hvEq : v = some 0 := by simpa using hv
            subst v
            rw [refRun_cons]
            have hstep : step ChangedOnly.policy s (j, some 0) = s := by simp [step, ChangedOnly.policy]
            rw [hstep]
            simpa using ih s
          · have hvne : v ≠ some 0 := by simpa using hv
            have hk : ChangedOnly.policy.key (j, v) = some j := by simp [ChangedOnly.policy, hvne]
            by_cases hjk : j == k
            · have hjeq : j = k := beq_iff_eq.mp hjk
              subst j
              rw [refRun_cons]
              rw [ih (step ChangedOnly.policy s (k, v))]
              rw [lookup_step_same ChangedOnly.policy s (k, v) k hk]
              simp [ChangedOnly.policy, hvne]
            · have hne : k ≠ j := by
                intro h
                subst h
                simp at hjk
              rw [refRun_cons]
              rw [ih (step ChangedOnly.policy s (j, v))]
              rw [lookup_step_other ChangedOnly.policy s (j, v) k j hk hne]
              have hjneq : j ≠ k := fun h => hne h.symm
              simp [hvne, hjneq]


theorem sound : ChangedOnly.Admission.Goal := by
  intro k rs
  simpa using admissionAux [] rs k

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ChangedOnly_Admission
