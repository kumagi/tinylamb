import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.DuplicateConsistentRetirement.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_DuplicateConsistentRetirement
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.DuplicateConsistentRetirement

private theorem lookup_eq_of_perm_consistent {a b : List Report} {i : Nat}
    (hcons : consistent a) (hperm : a.Perm b) : lookup a i = lookup b i := by
  unfold lookup
  -- if find? finds in a, then that element exists in b; consistent means same snd
  cases ha : a.find? (fun r => r.1 == i) with
  | none =>
      -- i not found in a => not found in b (Perm preserves membership of matching elements)
      have hnone_b : b.find? (fun r => r.1 == i) = none := by
        rw [List.find?_eq_none]
        intro r hr
        have hra : r ∈ a := hperm.symm.mem_iff.mp hr
        have := List.find?_eq_none.mp ha r hra
        exact this
      simp [ha, hnone_b]
  | some ra =>
      have hmem_a : ra ∈ a := List.mem_of_find?_eq_some ha
      have hpred_a : ra.1 == i := by
        have h := List.find?_some ha
        simpa using h
      have hmem_b : ra ∈ b := hperm.mem_iff.mp hmem_a
      -- find? in b returns some rb with same id; consistent gives same snd
      cases hb : b.find? (fun r => r.1 == i) with
      | none =>
          have := List.find?_eq_none.mp hb ra hmem_b
          simp [hpred_a] at this
      | some rb =>
          have hmem_rb_b : rb ∈ b := List.mem_of_find?_eq_some hb
          have hpred_rb : rb.1 == i := by
            have h := List.find?_some hb
            simpa using h
          have hmem_rb_a : rb ∈ a := hperm.symm.mem_iff.mp hmem_rb_b
          -- both ra, rb in a, same id, consistent => same snd
          have hid : ra.1 = rb.1 := by
            simp at hpred_a hpred_rb; omega
          have hsnd : ra.2 = rb.2 := hcons ra hmem_a rb hmem_rb_a hid
          simp [ha, hb, hsnd]

theorem sound : Goal := by
  intro a b start count hcons hperm
  unfold emit readyIds
  have hlookup : ∀ i, lookup a i = lookup b i := fun i => lookup_eq_of_perm_consistent hcons hperm
  simp only [hlookup]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_DuplicateConsistentRetirement
