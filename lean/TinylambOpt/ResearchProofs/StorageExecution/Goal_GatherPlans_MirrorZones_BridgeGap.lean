import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Named results include `disproof_BridgeGap`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_BridgeGap

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

/-!
  NOTE: the goals module declares `BridgeGap.Goal` only inside
  `namespace Contiguous`; there is no `MirrorZones.BridgeGap.Goal`.
  The proposition below is the direct MirrorZones analogue of the
  Contiguous break-even rule: instantiate the one-parameter
  `Contiguous.price fee` with the two-parameter `MirrorZones.price zone fee`
  (the second parameter plays the role of the gap fee).
-/
private def BridgeGap : Prop :=
  ∀ zone fee x y, x < y →
    (price zone fee x y ≤ price zone fee x x + price zone fee y y ↔ y - x - 1 ≤ fee)

-- Truth probe on small inputs: the analogue is false (e.g. zone=0 fee=1 x=0 y=2).
#eval not ((List.range 4).all fun zone =>
  (List.range 4).all fun fee =>
  (List.range 5).all fun x =>
  (List.range 5).all fun y =>
    x < y → (price zone fee x y ≤ price zone fee x x + price zone fee y y ↔ y - x - 1 ≤ fee))

-- Explicit counterexample computation:
--   price 0 1 0 2 = 3 + 1*2 + 1 = 6, price 0 1 0 0 = price 0 1 2 2 = 2,
--   hence the left side is 6 <= 4 (false) while 2-0-1 <= 1 is true.
#eval price 0 1 0 2
#eval price 0 1 0 0 + price 0 1 2 2
#eval decide (price 0 1 0 2 ≤ price 0 1 0 0 + price 0 1 2 2 ↔ 2 - 0 - 1 ≤ 1)

theorem disproof_BridgeGap : ¬ BridgeGap := by
  intro h
  have hiff := h 0 1 0 2 (by omega)
  have hbad : ¬ (price 0 1 0 2 ≤ price 0 1 0 0 + price 0 1 2 2 ↔
      2 - 0 - 1 ≤ 1) := by native_decide
  exact hbad hiff

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_BridgeGap
