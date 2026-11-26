import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Proved

- `GrantMonotonic` — A larger grant cannot raise the best candidate cost for a fixed
  group.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_GrantMonotonic

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_grant_mono (p : Plan) (n g h : Nat) (hgh : g ≤ h) :
    cost p n h ≤ cost p n g := by
  unfold cost
  have hsub : p.workingSet - h ≤ p.workingSet - g := Nat.sub_le_sub_left hgh p.workingSet
  have hmul : (p.workingSet - h) * p.spillRate ≤ (p.workingSet - g) * p.spillRate :=
    Nat.mul_le_mul_right p.spillRate hsub
  omega

theorem sound : GrantMonotonic.Goal := by
  intro ps n g h a b hgh hbestg hbesth
  change (List.map (fun p => cost p n g) ps).min? = some a at hbestg
  change (List.map (fun p => cost p n h) ps).min? = some b at hbesth
  have hgmem := (List.min?_eq_some_iff.mp hbestg).1
  have hble := (List.min?_eq_some_iff.mp hbesth).2
  rcases List.mem_map.mp hgmem with ⟨p0, hp0mem, hp0cost⟩
  have hcostgh := cost_grant_mono p0 n g h hgh
  have hb_le : b ≤ cost p0 n h := hble (cost p0 n h) (List.mem_map.mpr ⟨p0, hp0mem, rfl⟩)
  rw [hp0cost] at hcostgh
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_GrantMonotonic
