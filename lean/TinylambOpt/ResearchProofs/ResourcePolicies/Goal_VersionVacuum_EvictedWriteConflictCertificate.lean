import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
/-!
Registers the checked result for `VersionVacuum.EvictedWriteConflictCertificate.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_EvictedWriteConflictCertificate
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

private theorem le_getD_max?_of_mem {l : List Nat} {x : Nat} (hx : x ∈ l) :
    x ≤ l.max?.getD 0 := by
  induction l with
  | nil => cases hx
  | cons y ys ih =>
    simp only [List.max?_cons]
    rcases List.mem_cons.mp hx with rfl | hx
    · cases h : ys.max? with
      | none => simp
      | some m =>
        simp only [Option.getD_some]
        exact Nat.le_max_left _ _
    · have := ih hx
      cases h : ys.max? with
      | none =>
        simp at h
        subst h
        cases hx
      | some m =>
        simp only [h, Option.getD_some] at this ⊢
        exact Nat.le_trans this (Nat.le_max_right _ _)

theorem sound : EvictedWriteConflictCertificate.Goal := by
  intro evicted s k hmax t ht
  have hmem : t ∈ evicted.map Prod.snd := List.mem_map.mpr ⟨(k, t), ht, rfl⟩
  exact Nat.le_trans (le_getD_max?_of_mem hmem) hmax
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_EvictedWriteConflictCertificate
