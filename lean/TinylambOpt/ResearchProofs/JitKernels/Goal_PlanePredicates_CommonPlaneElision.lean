import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
A plane on which the threshold and the residual agree carries no comparison
information, so flipping it in both operands leaves the ballot unchanged. The
proof splits on whether the flipped plane lies inside the folded width and uses
a congruence result for the fold when it does not.

Proved

- `CommonPlaneElision` — a common plane may be elided from comparison code
  generation without changing the selected rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_CommonPlaneElision

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- The fold processes the new top plane last, which dominates the result. -/
theorem foldBits_succ (x c w : Nat) :
    foldBits x c (w + 1) = if bit x w == bit c w then foldBits x c w else bit c w := by
  simp [foldBits, List.range_succ]

/-- The fold only reads planes below its width. -/
theorem foldBits_congr {x c x' c' : Nat} {w : Nat}
    (hx : ∀ j < w, bit x j = bit x' j) (hc : ∀ j < w, bit c j = bit c' j) :
    foldBits x c w = foldBits x' c' w := by
  induction w with
  | zero => simp [foldBits]
  | succ w ih =>
      rw [foldBits_succ, foldBits_succ]
      have hxw := hx w (by omega)
      have hcw := hc w (by omega)
      rw [hxw, hcw]
      by_cases hcond : bit x' w == bit c' w
      · simp [hcond, ih (fun j hj => hx j (by omega)) (fun j hj => hc j (by omega))]
      · simp [hcond]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PlanePredicates.CommonPlaneElision.Goal := by
  intro x c j w h
  induction w with
  | zero => simp [foldBits]
  | succ w ih =>
      rw [foldBits_succ, foldBits_succ]
      by_cases hjw : j = w
      · subst j
        have h' : x.testBit w = c.testBit w := by simpa [bit] using h
        simp [bit, Nat.testBit_xor, h', ih]
      · have hb1 : bit (x ^^^ 2^j) w = bit x w := by
          simp [bit, Nat.testBit_xor, show ¬ j = w by exact hjw]
        have hb2 : bit (c ^^^ 2^j) w = bit c w := by
          simp [bit, Nat.testBit_xor, show ¬ j = w by exact hjw]
        rw [hb1, hb2]
        by_cases hcond : bit x w == bit c w
        · simp [hcond, ih]
        · simp [hcond]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_CommonPlaneElision
