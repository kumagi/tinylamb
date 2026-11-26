import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_FirstValue_FirstWins

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem firstValueKeep (rs : List Row) (k : Nat) : ∀ s : State,
    (s.lookup k).isSome → (refRun FirstValue.policy s rs).lookup k = s.lookup k := by
  induction rs with
  | nil => intro s hs; simp [refRun]
  | cons r rs ih =>
      intro s hs
      cases hkey : FirstValue.policy.key r with
      | none =>
          rw [refRun_cons]
          simp [step, hkey]
          rw [ih s hs]
      | some j =>
          by_cases hjk : j == k
          · have hjeq : j = k := beq_iff_eq.mp hjk
            subst j
            rw [refRun_cons]
            rw [ih (step FirstValue.policy s r)]
            · rw [lookup_step_same FirstValue.policy s r k hkey]
              cases hlk : s.lookup k with
              | none => rw [hlk] at hs; cases hs
              | some v => simp [FirstValue.policy]
            · rw [lookup_step_same FirstValue.policy s r k hkey]
              simp
          · have hne : k ≠ j := by
              intro h
              subst h
              simp at hjk
            rw [refRun_cons]
            have hl' := lookup_step_other FirstValue.policy s r k j hkey hne
            rw [ih (step FirstValue.policy s r) (by rw [hl']; exact hs)]
            exact hl'


theorem sound : FirstValue.FirstWins.Goal := by
  intro k x rs
  rw [refRun_cons]
  rw [firstValueKeep]
  · simp [step, FirstValue.policy]
  · simp [step, FirstValue.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_FirstValue_FirstWins
