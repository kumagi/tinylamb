import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Refuted by counterexample

- `SignedProduct.Unguarded` — Aggressive hypothesis: a defined raw bit pattern can
  always be marked with the extra IR promise.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_Unguarded

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.SignedProduct


theorem refute : ¬ SignedProduct.Unguarded.Goal := by
  intro h
  have hbad : ir SignedProduct.op (0x7fffffffffffffff, 2) ≠ some ((0x7fffffffffffffff : W) * 2) := by native_decide
  exact hbad (h _)

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_Unguarded
