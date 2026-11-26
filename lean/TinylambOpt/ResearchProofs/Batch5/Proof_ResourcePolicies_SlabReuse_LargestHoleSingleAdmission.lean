import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_LargestHoleSingleAdmission. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_LargestHoleSingleAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


namespace SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- The maximum of a cons list is the max of the head and the tail maximum (with a zero default). -/
theorem max?_getD_cons (h : Nat) : ∀ t : List Nat,
    (h :: t).max?.getD 0 = max h (t.max?.getD 0) := by
  intro t
  induction t with
  | nil => simp [List.max?_cons]
  | cons x xs ih => simp [List.max?_cons]

/-- Unfolding the maximum of a cons list exposes the tail-elim form. -/
theorem cons_max_elim (h : Nat) (t : List Nat) :
    t.max?.elim h (max h) = max h (t.max?.getD 0) := by
  rw [← show (h :: t).max?.getD 0 = t.max?.elim h (max h) by simp [List.max?_cons]]
  exact max?_getD_cons h t

/-- A cons-list maximum dominates its head. -/
theorem head_le_cons_max (h : Nat) (t : List Nat) :
    h ≤ t.max?.elim h (max h) := by
  rw [cons_max_elim h t]
  exact Nat.le_max_left _ _

end SlabReuseProof

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-- A single request can be granted iff some hole is at least as large as the request. -/
theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.LargestHoleSingleAdmission.Goal := by
  intro hs n hn
  induction hs with
  | nil =>
      simp [alloc]
      omega
  | cons h hs ih =>
      by_cases hle : n ≤ h
      · simp [alloc, hle]
        exact Nat.le_trans hle (SlabReuseProof.head_le_cons_max h hs)
      · by_cases ht : n ≤ hs.max?.getD 0
        · have hit : (alloc n hs).isSome = true := by rw [ih]; simp [ht]
          simp [alloc, hle, hit]
          rw [SlabReuseProof.cons_max_elim h hs]
          exact Nat.le_trans ht (Nat.le_max_right h (hs.max?.getD 0))
        · have hit : (alloc n hs).isSome = false := by rw [ih]; simp [ht]
          simp [alloc, hle, hit]
          rw [SlabReuseProof.cons_max_elim h hs]
          exact Nat.max_lt.mpr ⟨by omega, by omega⟩
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_LargestHoleSingleAdmission
