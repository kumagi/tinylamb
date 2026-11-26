import TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `LateSecondMeasure` — `∀ (gs : List Nat) k (approx exact payload : Nat → Int) (err :
  Nat), (∀ g ∈ gs, (exact g - approx g).natAbs ≤ err) → let kept := winners gs approx
  k (∀ x ∈ kept, ∀ y ∈ gs, y ∉ kept → approx y + 2*(err:Int) < approx x) → gs.Nodup →
  (winners gs exact k).map (fun g => (g,payload g)) = (winners kept exact k).map (fun
  g => (g,payload g))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_LateSecondMeasure

theorem sound : TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.LateSecondMeasure.Goal :=
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases.LateSecondMeasure_sound

end TinylambOpt.ResearchProofs.Frontiers.Goal_WorkloadBases_LateSecondMeasure
