import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
# DemandedManufacturerWitnessCache

An item can be a witness for an outer manufacturer only when its own name is a
non-null name appearing in the demand list. Restricting the expensive item
classification to those demanded rows therefore preserves every `any`
witness for every requested name, including null requests (which never match).
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DemandedManufacturerWitnessCache

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

private theorem hit_symm {k : Option String} {n : Option String}
    (h : hit k n = true) : hit n k = true := by
  cases k with
  | none => simp [hit] at h
  | some ks =>
      cases n with
      | none => simp [hit] at h
      | some ns =>
          simp [hit] at h ⊢
          exact h.symm

theorem sound : DemandedManufacturerWitnessCache.Goal := by
  intro wanted items admit
  let demanded := items.filter (fun r => wanted.any (hit r.1))
  let mark := fun (rs : List (Option String × Nat)) k =>
    rs.any (fun r => hit k r.1 && admit r.2)
  have hdemand : ∀ k ∈ wanted, ∀ r ∈ items, hit k r.1 = true →
      wanted.any (hit r.1) = true := by
    intro k hk r hr hkr
    apply List.any_eq_true.mpr
    exact ⟨k, hk, hit_symm hkr⟩
  have hpoint : ∀ k ∈ wanted, mark demanded k = mark items k := by
    intro k hk
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      rcases List.any_eq_true.mp h with ⟨r, hr, hp⟩
      have hri : r ∈ items := (List.mem_filter.mp hr).1
      apply List.any_eq_true.mpr
      exact ⟨r, hri, hp⟩
    · intro h
      rcases List.any_eq_true.mp h with ⟨r, hri, hp⟩
      have hhit : hit k r.1 = true := (Bool.and_eq_true_iff.mp hp).1
      apply List.any_eq_true.mpr
      refine ⟨r, ?_, hp⟩
      simp [demanded, hri, hdemand k hk r hri hhit]
  have hmap : ∀ l : List (Option String), l ⊆ wanted →
      l.map (mark demanded) = l.map (mark items) := by
    intro l hl
    exact List.map_congr_left (by
      intro k hk
      exact hpoint k (hl hk))
  exact hmap wanted (by intro k hk; exact hk)

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_DemandedManufacturerWitnessCache
