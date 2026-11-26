import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_DispatchWindowOutputCap

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
A bounded dispatch window with one report per morsel and a known per-morsel output cap retains at most window times rows rows; distinct ids inside the window cannot exceed the window length.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem flatMap_length_le_rows (rs : List Report) (rows : Nat)
    (h : ∀ r ∈ rs, r.2.length ≤ rows) :
    (rs.flatMap Prod.snd).length ≤ rs.length * rows := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      have h1 : r.2.length ≤ rows := h r (by simp)
      have h2 : (rs.flatMap Prod.snd).length ≤ rs.length * rows :=
        ih (by intro x hx; exact h x (by simp [hx]))
      simp only [List.flatMap_cons, List.length_append, List.length_cons]
      rw [Nat.add_mul]
      simp only [Nat.one_mul]
      omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.DispatchWindowOutputCap.Goal := by
  intro rs start window rows hnodup hwin
  have hlen : rs.length ≤ window := by
    have hsub : rs.map Prod.fst ⊆ (List.range window).map (start + ·) := by
      intro i hi
      simp only [List.mem_map] at hi ⊢
      obtain ⟨r, hr, rfl⟩ := hi
      have hw := hwin r hr
      refine ⟨r.1 - start, ?_, ?_⟩
      · simp
        omega
      · omega
    have hle := List.Nodup.length_le_of_subset hnodup hsub
    simpa using hle
  have hflat : (rs.flatMap Prod.snd).length ≤ rs.length * rows :=
    flatMap_length_le_rows rs rows (by intro r hr; exact (hwin r hr).2.2)
  have hmul : rs.length * rows ≤ window * rows := Nat.mul_le_mul_right rows hlen
  exact Nat.le_trans hflat hmul
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MorselRetirement.Goal_DispatchWindowOutputCap
