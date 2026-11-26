/-
Scratch proof for `CoalescedRequestOrder.Goal`. Coalescing is the operation
that appends a batch of invalidations and keeps only first occurrences.
Removing duplicates twice around a concatenation is the same as removing them
once, so coalescing a batch and then a second batch equals coalescing their
concatenation. This is what allows two arrival waves to be merged without
changing which object generations remain pending.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_CoalescedRequestOrder. The theorem below
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

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_CoalescedRequestOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Membership, not multiplicity, decides which entries `removeAll` drops. -/
theorem removeAll_eraseDups (xs ys : List Nat) : xs.removeAll ys.eraseDups = xs.removeAll ys := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [List.cons_removeAll, ih]

/-- Erasing duplicates is idempotent. -/
theorem eraseDups_idem (l : List Nat) : l.eraseDups.eraseDups = l.eraseDups := by
  match l with
  | [] => simp
  | x :: xs =>
      have hfilt : ((xs.filter fun b => !b == x).eraseDups).filter (fun b => !b == x) =
          (xs.filter fun b => !b == x).eraseDups := by
        apply List.filter_eq_self.mpr
        intro a ha
        have ha' : a ∈ xs.filter (fun b => !b == x) := by simpa using ha
        exact (List.mem_filter.mp ha').2
      rw [List.eraseDups_cons, List.eraseDups_cons, hfilt]
      simpa using congrArg (fun l : List Nat => x :: l) (eraseDups_idem (xs.filter fun b => !b == x))
termination_by l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b : Nat => !b == x) xs)

/-- Removing duplicates of the left part before appending changes nothing. -/
theorem eraseDups_append_eraseDups (as bs : List Nat) :
    (as.eraseDups ++ bs).eraseDups = (as ++ bs).eraseDups := by
  rw [List.eraseDups_append, List.eraseDups_append, eraseDups_idem]
  rw [removeAll_eraseDups]

/-- Coalescing two successive batches equals coalescing their concatenation. -/
theorem sound : CoalescedRequestOrder.Goal := by
  intro old a b
  simp only [queue]
  rw [← List.append_assoc]
  rw [eraseDups_append_eraseDups]

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_CoalescedRequestOrder
