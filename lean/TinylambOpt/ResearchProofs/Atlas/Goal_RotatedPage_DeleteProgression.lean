import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P8

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RotatedPage_DeleteProgression

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

/-! Brute-force sanity check over small configurations (n, split, base, step)
and small deletion progressions (lo, step, n). -/
#eval
  let bases : List Int := [-2, 0, 3]
  let steps : List Int := [-3, 0, 2]
  let mlos : List Int := [-3, 0, 2]
  let msteps : List Nat := [0, 1, 2, 3, 5]
  let mns : List Nat := [0, 1, 2, 3, 5]
  (List.range 9).all fun n =>
  (List.range 9).all fun split =>
  bases.all fun b =>
  steps.all fun st =>
  mlos.all fun lo =>
  msteps.all fun ms =>
  mns.all fun mn =>
    let c : Config := ⟨n, split, 1, b, st, 0, []⟩
    let mask : Progression := ⟨lo, ms, mn⟩
    (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum ==
      ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- The two RotatedPage lanes partition the physical rows: the first lane holds
the unrotated suffix `[s, n)` remapped to `[0, n-s)`, and the second lane holds
the wrapped prefix `[0, s)` remapped to `[n-s, n)`.  Deleting a progression of
physical ordinals therefore subtracts `deletedSum` from each lane's
`partialSum`, which is exactly the generic stride-one lane deletion lemma
proved for RotatedPage in `Auto_EncodedPages_P8`. -/
theorem sound_RotatedPage_DeleteProgression : RotatedPage.DeleteProgression.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_P8.sound_DeleteProgression

end TinylambOpt.ResearchProofs.Atlas.Goal_RotatedPage_DeleteProgression
