import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
`BatchedDurabilityWakeups` (PROVEN). For any waiter list `ws` and durable
offsets `a ≤ b`, waking waiters in two fsync callbacks — the first releasing
everything with `stop ≤ a`, the second releasing the still-pending waiters up
to `b` — releases exactly the same multiset of waiters as one callback to `b`.
The result is stated as a permutation so multiplicities are preserved:
`wake ws a ++ wake (pending ws a) b ~ wake ws b`. This lets the group-commit
thread acknowledge in batches across successive WAL flushes without ever
losing or duplicating an acknowledgement while preserving the modeled result.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches_BatchedDurabilityWakeups
open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- Moving a consed element from the end of an append to the front is a permutation. -/
private theorem perm_append_cons {α : Type} (l : List α) (a : α) (m : List α) :
    (l ++ (a :: m)).Perm (a :: (l ++ m)) := by
  induction l with
  | nil => exact List.Perm.refl _
  | cons z zs ih =>
    rw [List.cons_append, List.cons_append]
    exact List.Perm.trans (List.Perm.cons z ih) (List.Perm.swap a z (zs ++ m))

/-- Consing a waiter already durable at `d` keeps it at the head of the wake set. -/
private theorem wake_cons_le (x : Waiter) (xs : List Waiter) (d : Nat) (h : x.stop ≤ d) :
    wake (x :: xs) d = x :: wake xs d :=
  List.filter_cons_of_pos (by simpa using h)

/-- Consing a waiter not yet durable at `d` leaves the wake set unchanged. -/
private theorem wake_cons_gt (x : Waiter) (xs : List Waiter) (d : Nat) (h : ¬ (x.stop ≤ d)) :
    wake (x :: xs) d = wake xs d :=
  List.filter_cons_of_neg (by simpa using h)

/-- A waiter durable at `a` is absent from the pending list at `a`. -/
private theorem pend_cons_le (x : Waiter) (xs : List Waiter) (d : Nat) (h : x.stop ≤ d) :
    pending (x :: xs) d = pending xs d :=
  List.filter_cons_of_neg (by simpa using h)

/-- A waiter not durable at `a` heads the pending list at `a`. -/
private theorem pend_cons_gt (x : Waiter) (xs : List Waiter) (d : Nat) (h : ¬ (x.stop ≤ d)) :
    pending (x :: xs) d = x :: pending xs d :=
  List.filter_cons_of_pos (by simpa using h)

theorem sound : BatchedDurabilityWakeups.Goal := by
  intro ws
  induction ws with
  | nil => intro a b hab; exact List.Perm.refl _
  | cons x xs ih =>
    intro a b hab
    by_cases h1 : x.stop ≤ a
    · have h2 : x.stop ≤ b := Nat.le_trans h1 hab
      rw [wake_cons_le x xs a h1, pend_cons_le x xs a h1, wake_cons_le x xs b h2,
        List.cons_append]
      exact List.Perm.cons x (ih a b hab)
    · by_cases h2 : x.stop ≤ b
      · rw [wake_cons_gt x xs a h1, pend_cons_gt x xs a h1,
          wake_cons_le x (pending xs a) b h2, wake_cons_le x xs b h2]
        exact List.Perm.trans
          (perm_append_cons (wake xs a) x (wake (pending xs a) b))
          (List.Perm.cons x (ih a b hab))
      · rw [wake_cons_gt x xs a h1, pend_cons_gt x xs a h1,
          wake_cons_gt x (pending xs a) b h2, wake_cons_gt x xs b h2]
        exact ih a b hab

end TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches_BatchedDurabilityWakeups
