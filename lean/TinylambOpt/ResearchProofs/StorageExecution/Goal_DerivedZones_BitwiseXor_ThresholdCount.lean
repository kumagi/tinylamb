import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
import TinylambOpt.ResearchProofs.StorageExecution.SE_DerivedZones_BitwiseXor

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `BitwiseXor.ThresholdCount` — Estimate the number of passing rows using only
  certified and possible pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_ThresholdCount

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

abbrev Page := List (Option Row) × Bounds × Bounds

/-! Small-input truth check: enumerate every row of a few covered pages and
verify the certified lower/upper page counts bracket the actual count for
every cut in [0,16]. (A separate exhaustive check of the underlying interval
certificate, over all rectangles and points in [0,16]^2, also passes.) -/
private def smallPage (a b : Bounds) : List (Option Row) :=
  (List.range 8).flatMap fun x =>
    (List.range 8).map fun y =>
      if (decide (a.lo ≤ x && x ≤ a.hi)) && (decide (b.lo ≤ y && y ≤ b.hi)) then some (x, y) else none

private def smallPages : List Page :=
  [ (smallPage ⟨0, 3⟩ ⟨0, 3⟩, ⟨0, 3⟩, ⟨0, 3⟩)
  , (smallPage ⟨1, 6⟩ ⟨2, 5⟩, ⟨1, 6⟩, ⟨2, 5⟩)
  , (smallPage ⟨4, 4⟩ ⟨3, 7⟩, ⟨4, 4⟩, ⟨3, 7⟩)
  , ([none, some (7, 7)], ⟨7, 7⟩, ⟨7, 7⟩)
  ]

private def checkCuts : Bool :=
  (List.range 17).all fun cut =>
    let n := fun (e : Page) => (e.1.filterMap id).length
    let actual := ((smallPages.map Prod.fst).flatten.filterMap
      (fun r => r.map (BitwiseXor.kernel.score 0))).countP (· ≥ cut)
    decide (((smallPages.filter (fun e => cut ≤ (BitwiseXor.kernel.bounds 0 e.2.1 e.2.2).lo)).map n).sum ≤ actual &&
      actual ≤ ((smallPages.filter (fun e => cut ≤ (BitwiseXor.kernel.bounds 0 e.2.1 e.2.2).hi)).map n).sum)

example : checkCuts = true := by native_decide

/-- The XOR interval certificate is sound, hence so is the threshold count. -/
theorem sound_ThresholdCount : BitwiseXor.ThresholdCount.Goal := by
  exact TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.BitwiseXor.sound_ThresholdCount

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_ThresholdCount
