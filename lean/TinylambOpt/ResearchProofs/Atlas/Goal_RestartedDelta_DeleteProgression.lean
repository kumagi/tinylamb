import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E7

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RestartedDelta_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

/-! Exhaustive sanity check on small configurations (no counterexample). -/
#eval do
  let mut bad : Array (Nat × Nat × Int × Int × Int × Int × Nat × Nat) := #[]
  for n in List.range 9 do
    for sp in List.range 11 do
      for b in [-2, 0, 3] do
        for s in [-3, -1, 0, 1, 2] do
          for bi in [-2, 1] do
            let c : Config := ⟨n, sp, 0, b, s, bi, []⟩
            for mlo in [-3, 0, 2] do
              for mstep in List.range 5 do
                for mn in List.range 11 do
                  let mask : Progression := ⟨mlo, mstep, mn⟩
                  let lhs := (((rows layout c).filter (fun r : Nat × Int => !contains mask r.1)).map Prod.snd).sum
                  let rhs := ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum
                  if lhs != rhs then bad := bad.push (n, sp, b, s, bi, mlo, mstep, mn)
  pure bad.toList

/-- The goal is established by the fully proved lane-decomposition lemma
`Auto_EncodedPages_E7.RD.RD_DeleteProgression`, which applies the generic
`deleteProgression_of_perm` argument to the two stride-one lanes of
`RestartedDelta.layout`. -/
theorem sound_RestartedDelta_DeleteProgression :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E7.RD.RD_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_RestartedDelta_DeleteProgression
