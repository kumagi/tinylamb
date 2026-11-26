import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse
/-!
Registers the checked result for `SlabReuse.LargestHoleSingleAdmission.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_LargestHoleSingleAdmission
open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse LargestHoleSingleAdmission

private theorem alloc_some_iff_exists (hs : List Nat) (n : Nat) :
    (∃ hs', alloc n hs = some hs') ↔ ∃ h ∈ hs, n ≤ h := by
  induction hs with
  | nil => simp [alloc]
  | cons h tl ih =>
    unfold alloc
    by_cases hn : n ≤ h
    · simp [hn]
    · simp only [hn, ite_false]
      constructor
      · intro ⟨hs', hhs'⟩
        match ha : alloc n tl with
        | none => simp [ha] at hhs'
        | some tl' =>
          have : ∃ h ∈ tl, n ≤ h := ih.mp ⟨tl', ha⟩
          obtain ⟨x, hx, hle⟩ := this
          exact ⟨x, List.Mem.tail h hx, hle⟩
      · intro ⟨x, hx, hle⟩
        cases hx with
        | head => exact absurd hle hn
        | tail _ hx =>
          obtain ⟨tl', htl'⟩ := ih.mpr ⟨x, hx, hle⟩
          exact ⟨h :: tl', by simp [htl']⟩

theorem sound : Goal := by
  intro hs n hn
  have hiff : (alloc n hs).isSome = true ↔ ∃ h ∈ hs, n ≤ h := by
    constructor
    · intro h
      obtain ⟨hs', hhs'⟩ := Option.isSome_iff_exists.mp h
      exact (alloc_some_iff_exists hs n).mp ⟨hs', hhs'⟩
    · intro h
      obtain ⟨hs', hhs'⟩ := (alloc_some_iff_exists hs n).mpr h
      simp [hhs']
  by_cases hle : n ≤ hs.max?.getD 0
  · rw [decide_eq_true_iff.mpr hle]
    rw [hiff]
    cases hm : hs.max? with
    | none => simp [hm] at hle; omega
    | some m =>
      have hmem : m ∈ hs := List.max?_mem hm
      simp only [hm, Option.getD_some] at hle
      exact ⟨m, hmem, hle⟩
  · rw [decide_eq_false hle]
    cases hsome : (alloc n hs).isSome
    · rfl
    · exfalso
      rw [hiff] at hsome
      obtain ⟨x, hx, hxle⟩ := hsome
      have hmax : x ≤ hs.max?.getD 0 := by
        cases hm : hs.max? with
        | none => simp [List.max?_eq_none_iff.mp hm] at hx
        | some m =>
          simp only [hm, Option.getD_some]
          exact (List.max?_eq_some_iff.mp hm).2 x hx
      exact hle (Nat.le_trans hxle hmax)
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_LargestHoleSingleAdmission
