import TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.

Proved

- `MarginalEnvelope` — At a fixed grant, the optimal cost over a finite memo has
  decreasing marginal cost.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_MarginalEnvelope

open TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

private theorem cost_mid (p : Plan) (n g : Nat) :
    cost p n g + cost p (n+2) g = 2 * cost p (n+1) g := by
  unfold cost
  have h1 : p.perRow * (n+2) = p.perRow * n + 2 * p.perRow := by
    rw [Nat.mul_add]
    omega
  have h2 : p.perRow * (n+1) = p.perRow * n + p.perRow := by
    rw [Nat.mul_add]
    simp
  rw [h1, h2]
  omega

theorem sound : MarginalEnvelope.Goal := by
  intro ps n g a b c ha hb hc
  change (List.map (fun p => cost p n g) ps).min? = some a at ha
  change (List.map (fun p => cost p (n+1) g) ps).min? = some b at hb
  change (List.map (fun p => cost p (n+2) g) ps).min? = some c at hc
  have hbmem := (List.min?_eq_some_iff.mp hb).1
  rcases List.mem_map.mp hbmem with ⟨p1, hp1, hp1cost⟩
  have h0 := (List.min?_eq_some_iff.mp ha).2
  have h2 := (List.min?_eq_some_iff.mp hc).2
  have ha_le : a ≤ cost p1 n g := h0 (cost p1 n g) (List.mem_map.mpr ⟨p1, hp1, rfl⟩)
  have hc_le : c ≤ cost p1 (n+2) g := h2 (cost p1 (n+2) g) (List.mem_map.mpr ⟨p1, hp1, rfl⟩)
  have hlin := cost_mid p1 n g
  rw [hp1cost] at hlin
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PlanEnvelope_MarginalEnvelope
