import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_DeletedSpan_Remaining

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DeletedSpan_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

/-! Brute-force sanity check on small configurations (empty list means no counterexample). -/
#eval do
  let mut bad : Array (Nat × Nat × Nat × Int × Int × Int × Nat × Nat) := #[]
  for n in List.range 8 do
    for s in List.range (n + 3) do
      for p in List.range 3 do
        for b in [-2, 0, 3] do
          for st in [-3, -1, 0, 1, 2] do
            let c : Config := ⟨n, s, p, b, st, 0, []⟩
            for mlo in List.range (n + 4) do
              for mstep in List.range 4 do
                for mn in List.range (n + 4) do
                  let mask : Progression := ⟨mlo - 2, mstep, mn⟩
                  let lhs := (((rows layout c).filter
                    (fun r : Nat × Int => !contains mask r.1)).map Prod.snd).sum
                  let rhs := ((layout.lanes c).map
                    (fun a => partialSum a a.n - deletedSum a mask)).sum
                  if lhs != rhs then bad := bad.push (n, s, p, b, st, mlo - 2, mstep, mn)
  pure bad.toList

/-- The goal is true.  The full proof (CRT intersection, stride-1 lane point
enumeration, and the flatMap/permutation reduction) is registered in
`TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpan.Remaining`; here we
expose it as the goal's theorem. -/
theorem sound_DeletedSpan_DeleteProgression : DeletedSpan.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpan.Remaining.sound_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_DeletedSpan_DeleteProgression
