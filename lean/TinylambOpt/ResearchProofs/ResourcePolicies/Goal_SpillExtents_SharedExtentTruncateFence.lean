import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.SharedExtentTruncateFence.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_SharedExtentTruncateFence
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents SharedExtentTruncateFence

theorem sound : Goal := by
  intro segments limit
  constructor
  · intro h seg hseg id hid
    have hle : id + 1 ≤ roof segments.flatten := by
      unfold roof
      cases hmax : segments.flatten.max? with
      | none =>
        have hempty : segments.flatten = [] := List.max?_eq_none_iff.mp hmax
        have hmem : id ∈ segments.flatten := List.mem_flatten.mpr ⟨seg, hseg, hid⟩
        rw [hempty] at hmem
        cases hmem
      | some m =>
        simp only [hmax, Option.map_some, Option.getD_some]
        have hmem : id ∈ segments.flatten := List.mem_flatten.mpr ⟨seg, hseg, hid⟩
        have hm : ∀ b ∈ segments.flatten, b ≤ m := (List.max?_eq_some_iff.mp hmax).2
        have := hm id hmem
        omega
    omega
  · intro h
    unfold roof
    cases hmax : segments.flatten.max? with
    | none => simp
    | some m =>
      simp only [Option.map_some, Option.getD_some]
      have hmem : m ∈ segments.flatten := List.max?_mem hmax
      rcases List.mem_flatten.mp hmem with ⟨seg, hseg, hmid⟩
      have := h seg hseg m hmid
      omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_SharedExtentTruncateFence
