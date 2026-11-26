import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse
/-!
Registers the checked result for `SlabReuse.CompactedArenaTraceAdmission.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_CompactedArenaTraceAdmission
open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse CompactedArenaTraceAdmission

private theorem place_single (c : Nat) (rs : List Nat) :
    (place [c] rs).isSome = true ↔ rs.sum ≤ c := by
  induction rs generalizing c with
  | nil => simp [place]
  | cons r rs ih =>
    simp only [place, List.foldlM_cons, alloc]
    by_cases hr : r ≤ c
    · simp only [hr, ite_true, Option.bind_some]
      -- goal is (place [c-r] rs).isSome = true ↔ (r+rs.sum) ≤ c
      have ih' := ih (c - r)
      simp only [place] at ih'
      constructor
      · intro h
        have hrs := ih'.mp h
        have hrc : r + rs.sum ≤ c := by
          have h1 : rs.sum + r ≤ c - r + r := Nat.add_le_add_right hrs r
          have h2 : c - r + r = c := Nat.sub_add_cancel hr
          rw [Nat.add_comm]
          rw [h2] at h1
          exact h1
        simpa [List.sum_cons] using hrc
      · intro h
        have h' : r + rs.sum ≤ c := by simpa [List.sum_cons] using h
        have hrs : rs.sum ≤ c - r := by
          have h1 : r + rs.sum ≤ c := h'
          omega
        exact ih'.mpr hrs
    · simp only [hr, ite_false, Option.isSome_none, Bool.false_eq_true, List.sum_cons]
      constructor
      · intro h; cases h
      · intro h; omega

theorem sound : Goal := by
  intro hs rs
  constructor
  · intro h
    have := (place_single hs.sum rs).mp h
    simpa using this
  · intro h
    have h' : rs.sum ≤ hs.sum := by simpa using h
    exact (place_single hs.sum rs).mpr h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_CompactedArenaTraceAdmission
