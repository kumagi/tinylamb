import TinylambOpt.ResearchGoals.Frontiers.SparseRecovery

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A03

open TinylambOpt.ResearchGoals.Frontiers.SparseRecovery

/-! Per-goal notes:
  * `BoundedMoments`: true — power sums 0..d determine a multiset of size <= d.
  * `TwoRootMask`: true — for xs = [a,b] the equation is 2*(p-a)*(p-b) = 0 over Int.
  * `SwapDecoder`: true — d = added-removed and q/d = added+removed.
  * `MonicFingerprint`: true — a monic degree-n polynomial is fixed by its n values.
  * `RationalDelta`: true — degree <= d products agreeing at d+1 points coincide.
-/

theorem TwoRootMask_sound : TinylambOpt.ResearchGoals.Frontiers.SparseRecovery.TwoRootMask.Goal := by
  intro xs p hlen
  match xs with
  | [] => simp at hlen
  | [a] => simp at hlen
  | a :: b :: [] =>
      simp [moment, List.mem_cons]
      constructor
      · intro hp
        rcases hp with rfl | rfl <;> grind
      · intro h
        have hI := congrArg (fun n : Nat => (n : Int)) h
        simp at hI
        have hf : (2 : Int) * (((p : Int) - (a : Int)) * ((p : Int) - (b : Int))) =
            ((2 : Int) * (p : Int) * (p : Int) + ((a : Int) + (b : Int)) ^ 2) -
              (2 * (p : Int) * ((a : Int) + (b : Int)) + ((a : Int) ^ 2 + (b : Int) ^ 2)) := by
          grind
        have hz : ((p : Int) - (a : Int)) * ((p : Int) - (b : Int)) = 0 := by
          omega
        have hpa : (p : Int) = a ∨ (p : Int) = b := by
          have := Int.mul_eq_zero.mp hz
          omega
        omega
  | a :: b :: c :: rest => simp at hlen

theorem SwapDecoder_sound : TinylambOpt.ResearchGoals.Frontiers.SparseRecovery.SwapDecoder.Goal := by
  intro base removed added hne
  dsimp
  simp [moment]
  let M2 : Int := ((base.map (fun x : Nat => x ^ 2)).sum : Int)
  have hd : ((added : Int) + (base.sum : Int) - ((removed : Int) + (base.sum : Int))) =
      (added : Int) - (removed : Int) := by
    omega
  have hq : (((added : Int) ^ 2 + M2) - ((removed : Int) ^ 2 + M2)) =
      ((added : Int) - (removed : Int)) * ((added : Int) + (removed : Int)) := by
    grind
  have hdnz : ((added : Int) - (removed : Int)) ≠ 0 := by
    intro h
    have hEq : (added : Int) = (removed : Int) := by omega
    have hn : added = removed := by omega
    exact hne hn.symm
  rw [hd, hq]
  have hdiv : (((added : Int) - (removed : Int)) * ((added : Int) + (removed : Int))) /
        ((added : Int) - (removed : Int)) = (added : Int) + (removed : Int) := by
    exact Int.mul_ediv_cancel_left _ hdnz
  rw [hdiv]
  constructor <;> grind

end TinylambOpt.ResearchProofs.NewGoalsBatch.A03
