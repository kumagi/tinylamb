import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `NetworkBitOrder.BitReversal` — `∀ n w i b, let p := i*w+b; (layout.bit n w i b)%8 +
  p%8 = 7 ∧ layout.bit n w i b/8=p/8`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_BitReversal

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

-- Sanity checks on small inputs.
private def check (N W : Nat) : Bool :=
  (List.range N).all fun n => (List.range W).all fun w =>
    (List.range (3 * W + 3)).all fun i =>
      (List.range W).all fun b =>
        let p := i * w + b
        decide ((layout.bit n w i b) % 8 + p % 8 = 7 ∧ (layout.bit n w i b) / 8 = p / 8)

#eval check 6 9

theorem sound_BitReversal : NetworkBitOrder.BitReversal.Goal := by
  intro n w i b
  dsimp [NetworkBitOrder.layout, layout]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_BitReversal
