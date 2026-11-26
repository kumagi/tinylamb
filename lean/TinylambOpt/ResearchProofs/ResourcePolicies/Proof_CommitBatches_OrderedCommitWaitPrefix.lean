import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_OrderedCommitWaitPrefix. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_OrderedCommitWaitPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- If no element of a list satisfies a Boolean predicate, its filter is empty. -/
theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) : ∀ l : List α,
    (∀ x ∈ l, p x = false) → l.filter p = [] := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      intro h
      have ha : p a = false := h a (by simp)
      have ht : ∀ x ∈ l, p x = false := by intro x hx; exact h x (by simp [hx])
      simp [ha, ih ht]

/-- When enqueues respect end-offset order, all waiters that can be woken form
    a prefix of the queue; scanning until the first not-yet-durable waiter is
    therefore equivalent to filtering the whole queue. -/
theorem sound : OrderedCommitWaitPrefix.Goal := by
  intro ws d hp
  induction ws with
  | nil => simp [wake]
  | cons w ws ih =>
      rw [List.pairwise_cons] at hp
      rcases hp with ⟨hall, hpair⟩
      by_cases hwd : w.stop ≤ d
      · have hih := ih hpair
        simp [wake, hwd, hih]
      · have htail : ∀ x ∈ ws, decide (x.stop ≤ d) = false := by
          intro x hx
          have hge : w.stop ≤ x.stop := hall x hx
          simp [show ¬x.stop ≤ d by omega]
        have hfilt : ws.filter (fun x : Waiter => decide (x.stop ≤ d)) = [] :=
          filter_eq_nil_of_all_false (fun x : Waiter => decide (x.stop ≤ d)) ws htail
        simp [wake, hwd, hfilt]
end TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_OrderedCommitWaitPrefix
