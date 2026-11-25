import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_BucketRuns_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

/-! Brute-force sanity check: no counterexample for small configurations. -/
#eval do
  let mut bad : Array (Nat × Nat × Int × Int × Nat × Nat × Int × Int) := #[]
  for n in List.range 12 do
    for p in List.range 5 do
      for step in [-2, 0, 1, 3] do
        for lo in [-2, 0, 1, 3] do
          for mstep in [0, 1, 2, 3] do
            for mn in [0, 1, 2, 5] do
              let c : Config := ⟨n, 0, p, 0, step, 0, []⟩
              let m : Progression := ⟨lo, mstep, mn⟩
              let lhs := (((rows BucketRuns.layout c).filter (fun r : Nat × Int => !contains m r.1)).map Prod.snd).sum
              let rhs := ((BucketRuns.layout.lanes c).map (fun a : Lane => partialSum a a.n - deletedSum a m)).sum
              if lhs != rhs then bad := bad.push (n, p, step, lo, mstep, mn, lhs, rhs)
  pure bad.toList.length

theorem sound_BucketRuns_DeleteProgression : BucketRuns.DeleteProgression.Goal := by
  intro c mask
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.BucketRuns.Remaining.sound_DeleteProgression c mask

end TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_DeleteProgression
