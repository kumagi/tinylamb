import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_QuadraticFiniteDifferenceLoop

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem sound : TinylambOpt.ResearchGoals.JitKernels.InductionLowering.QuadraticFiniteDifferenceLoop.Goal := by
  intro a b c start stride n
  let p : Int → Int := fun x => poly a b c x
  let dd : Int := 2 * a * stride * stride
  let step : (Int × Int) → Nat → (Int × Int) := fun s _ => advance dd s
  let state : Nat → (Int × Int) := fun k =>
    (p (start + stride * (k : Int)),
      p (start + stride * ((k + 1 : Nat) : Int)) - p (start + stride * (k : Int)))
  have hstep : ∀ k : Nat, step (state k) k = state (k + 1) := by
    intro k
    ext <;> simp [step, advance, state, p, poly, dd] <;> grind
  have hboth : ∀ m : Nat,
      (List.range m).scanl step (state 0) = (List.range (m + 1)).map state ∧
      (List.range m).foldl step (state 0) = state m := by
    intro m
    induction m with
    | zero => simp [state, p]
    | succ m ih =>
        rcases ih with ⟨hscan, hfold⟩
        constructor
        · rw [List.range_succ, List.scanl_append, hscan, hfold]
          simp [List.scanl, hstep, List.range_succ, List.map_append]
        · rw [List.range_succ, List.foldl_append, hfold]
          simp [hstep]
  have hinit : (p start, p (start + stride) - p start) = state 0 := by
    simp [state, p]
  unfold TinylambOpt.ResearchGoals.JitKernels.InductionLowering.differences
  rw [hinit, (hboth n).1]
  simp [List.range_succ, state, p]

end TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_QuadraticFiniteDifferenceLoop
