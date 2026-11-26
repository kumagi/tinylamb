import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `SignedNarrow.BiasBoundary` — `∀ y, ir op (0x80000000,y)=none ∧ ir op
  (0xffffffff80000000,y)=some 0xffffffff80000000`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedNarrow_BiasBoundary

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises


theorem sound : SignedNarrow.BiasBoundary.Goal := by
  intro y
  constructor
  · unfold ir
    have hnot : ¬ (SignedNarrow.op.defined (0x80000000 : W) y = true) := by
      dsimp [SignedNarrow.op]
      native_decide
    rw [ite_eq_right hnot]
  · unfold ir
    have hpos : SignedNarrow.op.defined (0xffffffff80000000 : W) y = true := by
      dsimp [SignedNarrow.op]
      native_decide
    rw [ite_eq_left hpos]
    change some (signExt32 (0xffffffff80000000 : W)) = some (0xffffffff80000000 : W)
    congr

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedNarrow_BiasBoundary
