/-
Scratch proof for `DrainCutKeepsNewInvalidation.Goal`. A drain cut partitions
old work into a served prefix and a still-pending suffix. For a duplicate-free
old queue, an id is in the suffix exactly when it was old and not in the
served prefix; fresh invalidations remain pending independently of the cut.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_DrainCutKeepsNewInvalidation. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_DrainCutKeepsNewInvalidation

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- One occurrence cannot be both inside a take prefix and its drop suffix. -/
theorem not_mem_take_of_mem_drop {l : List Nat} (hnd : l.Nodup) {a : Nat} {n : Nat}
    (hd : a ∈ l.drop n) : a ∉ l.take n := by
  induction n generalizing l with
  | zero => simp
  | succ n ih =>
      cases l with
      | nil => simp at hd
      | cons x xs =>
          simp only [List.drop_succ_cons, List.take_succ_cons] at hd ⊢
          have hnd' := List.nodup_cons.mp hnd
          by_cases hax : a = x
          · subst a
            exact (hnd'.1 (List.mem_of_mem_drop hd)).elim
          · simp [hax, ih hnd'.2 hd]

/-- The cut keeps exactly old entries after the cut plus all fresh entries. -/
theorem sound : DrainCutKeepsNewInvalidation.Goal := by
  intro old fresh k n hnd
  simp only [queue, List.mem_eraseDups, List.mem_append]
  constructor
  · rintro (hd | hf)
    · left
      exact ⟨List.mem_of_mem_drop hd, not_mem_take_of_mem_drop hnd hd⟩
    · exact Or.inr hf
  · rintro (⟨hold, hnotake⟩ | hf)
    · left
      have hmem : k ∈ old.take n ++ old.drop n := by
        simpa [List.take_append_drop] using hold
      rcases List.mem_append.mp hmem with ht | hd
      · exact (hnotake ht).elim
      · exact hd
    · exact Or.inr hf

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_DrainCutKeepsNewInvalidation
