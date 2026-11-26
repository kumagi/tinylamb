import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Refuted by counterexample

- `Exchanges.GlobalPacking` — `∀ cap xs, 0 < cap → traffic cap xs = packets cap (xs.map
  Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_GlobalPacking

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

/-! Smallest counterexample: at capacity 4, one byte to each of two distinct
destinations needs two packets, whereas the total of two bytes fits in one. -/
#eval let xs : List Bucket := [(0, 1), (1, 1)]
      (traffic 4 xs, packets 4 (xs.map Prod.snd).sum)

theorem disproof_Exchanges_GlobalPacking : ¬ Exchanges.GlobalPacking.Goal := by
  intro h
  let xs : List Bucket := [(0, 1), (1, 1)]
  have hh := h 4 xs (by decide)
  have hneq : ¬ (Exchanges.traffic 4 xs =
      Exchanges.packets 4 (xs.map Prod.snd).sum) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_GlobalPacking
