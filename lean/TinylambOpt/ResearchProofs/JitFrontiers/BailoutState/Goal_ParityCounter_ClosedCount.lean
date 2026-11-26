import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ParityCounter_ClosedCount

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem parityAuxGen : ∀ (rs : List Row) (s : State) (f : Nat → Nat),
    (∀ k, (s.lookup k).getD 0 = ((f k % 2 : Nat) : Int)) →
    ∀ k, ((refRun ParityCounter.policy s rs).lookup k).getD 0 =
      (((f k + rs.countP (fun r => r.1 == k)) % 2 : Nat) : Int) := by
  intro rs
  induction rs with
  | nil =>
      intro s f hs k
      simp [refRun, hs]
  | cons r rs ih =>
      intro s f hs k
      cases r with
      | mk j v =>
          let f' : Nat → Nat := fun t => if t == j then f t + 1 else f t
          have hkrow : ParityCounter.policy.key (j, v) = some j := by simp [ParityCounter.policy]
          have hs' : ∀ t, ((step ParityCounter.policy s (j, v)).lookup t).getD 0 = ((f' t % 2 : Nat) : Int) := by
            intro t
            by_cases htj : t == j
            · have hteq : t = j := beq_iff_eq.mp htj
              subst t
              rw [lookup_step_same ParityCounter.policy s (j, v) j hkrow]
              simp [ParityCounter.policy, f', hs]
              have hmod : f j % 2 < 2 := Nat.mod_lt _ (by decide)
              omega
            · have hne : j ≠ t := by
                intro h
                subst h
                simp at htj
              rw [lookup_step_other ParityCounter.policy s (j, v) t j hkrow (fun h => hne h.symm)]
              have htne : t ≠ j := fun h => hne h.symm
              simp [f', htne, hs]
          rw [refRun_cons]
          rw [ih (step ParityCounter.policy s (j, v)) f' hs' k]
          by_cases hkj : j == k
          · have hkeq : j = k := beq_iff_eq.mp hkj
            subst j
            simp [f']
            omega
          · have hne : k ≠ j := by
              intro h
              subst h
              simp at hkj
            have hjneq : j ≠ k := fun h => hne h.symm
            simp [f', hne, hjneq]


theorem sound : ParityCounter.ClosedCount.Goal := by
  intro rs k
  have h := parityAuxGen rs [] (fun _ => 0) (by simp) k
  simpa using h

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ParityCounter_ClosedCount
