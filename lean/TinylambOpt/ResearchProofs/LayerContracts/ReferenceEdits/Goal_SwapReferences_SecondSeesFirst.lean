import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `SwapReferences.SecondSeesFirst` — `∀ s a b, a ≠ b → (finish model s a
  b).children.count (some b) = 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_SecondSeesFirst

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SwapReferences.SecondSeesFirst.Goal := by
  intro s a b hab
  let s1 := step s (.rewrite a (some b))
  have hc : (s1.children.map (fun x => if x = some b then some a else x)).count (some b) = 0 :=
    count_some_map_rewrite_src_lit s1.children b a (Ne.symm hab)
  simpa [finish, run, step, SwapReferences.model, s1, rwFun] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_SecondSeesFirst
