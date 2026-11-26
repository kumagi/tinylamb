import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-! When no byte can equal one, a borrow cannot continue past a zero byte, so the
cheap subtract mask is exactly the zero-byte mask. This permits the per-lane
equality mask to replace the broader ballot for restricted alphabets. -/

set_option maxHeartbeats 0
set_option maxRecDepth 100000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_BorrowMasks_BorrowImpossibleNeighbor

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowImpossibleNeighbor

theorem sound : Goal := by
  intro x hno
  have hbyte : ∀ i < 8, x.extractLsb' (8*i) 8 ≠ (1 : BitVec 8) := by
    intro i hi heq
    have hlane : lane x i = 1 := by
      unfold lane
      rw [heq]
      rfl
    exact hno i hi hlane
  have h0 := hbyte 0 (by omega)
  have h1 := hbyte 1 (by omega)
  have h2 := hbyte 2 (by omega)
  have h3 := hbyte 3 (by omega)
  have h4 := hbyte 4 (by omega)
  have h5 := hbyte 5 (by omega)
  have h6 := hbyte 6 (by omega)
  have h7 := hbyte 7 (by omega)
  simp [fast, TinylambOpt.ResearchGoals.SwarDigitBallot.zeroBytes,
    ones, TinylambOpt.ResearchGoals.SwarDigitBallot.low7,
    TinylambOpt.ResearchGoals.SwarDigitBallot.high] at *
  bv_decide

end Nov26.Proof_BorrowMasks_BorrowImpossibleNeighbor
