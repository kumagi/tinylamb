import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Named results include `disproof_PositiveGapRule`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_PositiveGapRule

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

/--!
  NOTE: `PositiveGapRule.Goal` is declared only inside `namespace Contiguous`;
  `MirrorZones` declares `JumpUnprofitable.Goal` and `BoundaryTax.Goal` instead.
  The proposition below is the direct MirrorZones analogue of the Contiguous
  closed form: instantiate the one-parameter `Contiguous.price fee` with the
  two-parameter `MirrorZones.price zone fee` (the second parameter plays the
  role of the gap fee) and leave the gap-only right-hand side unchanged.

  The analogue is false.  `MirrorZones.price` charges a fixed `+1` per extent
  and an extra `penalty` for every zone crossing, neither of which is captured
  by `min fee (b - a - 1)`.  A minimal counterexample is `zone = 1`,
  `fee = 1`, `xs = [1,2]`: bridging the gap from 1 to 2 crosses from zone 0 to
  zone 1, so both a single extent and two point reads cost 4, while the
  gap-only formula gives `2 + 1 + min 1 0 = 3`.
-/
private def PositiveGapRule : Prop :=
  ∀ zone fee xs, let ks := points xs
    let gaps := ks.zip (ks.drop 1)
    cost (price zone fee) (choose (price zone fee) xs) =
      ks.length + (if ks.isEmpty then 0 else fee) +
        (gaps.map (fun (a, b) => min fee (b - a - 1))).sum

-- Small-input truth probe: a counterexample exists already for zone=1, fee=1.
#eval not ((List.range 4).all fun zone =>
  (List.range 4).all fun fee =>
  (List.range 4).all fun x =>
  (List.range 4).all fun y =>
    x < y → cost (price zone fee) (choose (price zone fee) [x, y]) =
      (points [x, y]).length + (if (points [x, y]).isEmpty then 0 else fee) +
        (((points [x, y]).zip ((points [x, y]).drop 1)).map
          (fun (a, b) => min fee (b - a - 1))).sum)

-- Explicit computation of the counterexample.
#eval points [1, 2]
#eval covers (points [1, 2])
#eval cost (price 1 1) (choose (price 1 1) [1, 2])
#eval price 1 1 1 2
#eval price 1 1 1 1 + price 1 1 2 2
#eval (points [1, 2]).length + 1 + min 1 (2 - 1 - 1)

theorem disproof_PositiveGapRule : ¬ PositiveGapRule := by
  intro h
  have hEq := h 1 1 [1, 2]
  have hbad : ¬ (cost (price 1 1) (choose (price 1 1) [1, 2]) =
      (points [1, 2]).length +
        (if (points [1, 2]).isEmpty then 0 else 1) +
        (((points [1, 2]).zip ((points [1, 2]).drop 1)).map
          (fun (a, b) => min 1 (b - a - 1))).sum) := by
    native_decide
  exact hbad hEq

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_PositiveGapRule
