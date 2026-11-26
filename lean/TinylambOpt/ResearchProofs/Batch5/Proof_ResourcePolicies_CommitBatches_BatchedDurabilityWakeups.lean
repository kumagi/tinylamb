import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_BatchedDurabilityWakeups. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_BatchedDurabilityWakeups

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- A Boolean predicate partitions a list into the satisfying and rejecting
    filters; this is the generic list fact behind the two-callback wake split. -/
theorem filter_append_not_perm {α : Type} (p : α → Bool) : ∀ l : List α,
    (l.filter p ++ l.filter (fun x => !(p x))).Perm l := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]
      · have hf : p a = false := by cases hp : p a <;> simp_all
        rw [show (List.filter p (a :: l) ++ List.filter (fun x => !p x) (a :: l)) =
            (List.filter p l ++ a :: List.filter (fun x => !p x) l) by simp [hf]]
        have hp : (List.filter p l ++ a :: List.filter (fun x => !p x) l).Perm
            (a :: (List.filter p l ++ List.filter (fun x => !p x) l)) := List.perm_middle
        exact hp.trans (List.Perm.cons a ih)

/-- Re-filtering with a weaker predicate leaves the already filtered list alone. -/
theorem filter_filter_imp {α : Type} (p q : α → Bool)
    (h : ∀ x, p x = true → q x = true) : ∀ l : List α, (l.filter p).filter q = l.filter p := by
  intro l
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true
      · have hq := h a hp
        simp [hp, hq, ih]
      · simp [hp, ih]

/-- `wake ws a` and `pending ws a` partition the waiter queue. -/
theorem wake_pending_perm (ws : List Waiter) (a : Nat) :
    (wake ws a ++ pending ws a).Perm ws := by
  have hp := filter_append_not_perm (fun w : Waiter => decide (w.stop ≤ a)) ws
  have hnot : (fun w : Waiter => decide (a < w.stop)) =
      (fun w : Waiter => !(decide (w.stop ≤ a))) := by
    funext w
    by_cases h : w.stop ≤ a
    · have hlt : ¬a < w.stop := by omega
      simp [h, hlt]
    · have hlt : a < w.stop := by omega
      simp [h, hlt]
  rw [← hnot] at hp
  simpa [wake, pending] using hp

/-- A waiter acknowledged by the earlier fsync remains acknowledged later. -/
theorem wake_wake_eq (ws : List Waiter) {a b : Nat} (hab : a ≤ b) :
    wake (wake ws a) b = wake ws a := by
  change (ws.filter (fun w : Waiter => decide (w.stop ≤ a))).filter
      (fun w : Waiter => decide (w.stop ≤ b)) =
    ws.filter (fun w : Waiter => decide (w.stop ≤ a))
  apply filter_filter_imp
  intro w hw
  have hs : w.stop ≤ a := by simpa using hw
  exact by simp [Nat.le_trans hs hab]

/-- Two fsync callbacks acknowledge the same multiset of waiters as one
    callback at the final durable offset: the first splits the queue at `a`
    and the second drains the pending remainder at `b`. -/
theorem sound : BatchedDurabilityWakeups.Goal := by
  intro ws a b hab
  have hp := (wake_pending_perm ws a).filter (fun w : Waiter => decide (w.stop ≤ b))
  have hfirst : (wake ws a).filter (fun w : Waiter => decide (w.stop ≤ b)) = wake ws a := by
    simpa [wake] using (wake_wake_eq ws hab)
  rw [List.filter_append, hfirst] at hp
  simpa [wake] using hp
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_BatchedDurabilityWakeups
