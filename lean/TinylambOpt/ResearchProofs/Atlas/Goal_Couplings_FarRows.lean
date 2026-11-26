import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

/-!
A query can use a synopsis to avoid inspecting some input only when the claimed bounds
hold for the underlying data.

Proved

- `Couplings.FarRows` — `∀ rs d, (d+1) * rs.countP (fun (a,b) => d < (a-b).natAbs) ≤
  cost rs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_FarRows

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings

/-- Every row whose absolute difference exceeds `d` contributes at least `d + 1`
    to the total cost, so the number of such rows times `d + 1` is bounded by
    the whole cost. -/
theorem sound_Couplings_FarRows : Couplings.FarRows.Goal := by
  intro rs d
  induction rs with
  | nil =>
      simp [cost]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      dsimp [cost] at ih ⊢
      simp only [List.countP_cons, List.map_cons, List.sum_cons] at ih ⊢
      by_cases h : d < (a - b).natAbs
      · simp [h]
        rw [Nat.mul_succ]
        have hle : d + 1 ≤ (a - b).natAbs := by omega
        omega
      · simp [h]
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_FarRows
