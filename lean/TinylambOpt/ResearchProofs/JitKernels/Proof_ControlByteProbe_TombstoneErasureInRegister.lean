import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# TombstoneErasureInRegister (proven)

Compacting out deleted lanes in registers before deciding the first live match
preserves the probe result: tombstones satisfy `!= .empty`, so erasing them
commutes with the `takeWhile (· != .empty)` scan window, and tombstones
evaluate to `none`, so removing them from the window cannot change
`findSome?`.  SQL execution may therefore physically compact tombstone-free
groups without changing any probe answer. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_TombstoneErasureInRegister
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private theorem findSome?_filter (k : Nat) (l : List Slot) (p : Slot → Bool)
    (h : ∀ x ∈ l, p x = false → value k x = none) :
    (l.filter p).findSome? (value k) = l.findSome? (value k) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    by_cases hp : p a = true
    · rw [List.filter_cons, if_pos hp, List.findSome?_cons, List.findSome?_cons,
          ih (fun x hx => h x (List.mem_cons_of_mem a hx))]
    · rw [List.filter_cons, if_neg hp, List.findSome?_cons,
          h a (List.mem_cons_self) (not_true_eq_false hp),
          ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem takeWhile_pos {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = true) : (a :: as).takeWhile p = a :: as.takeWhile p := by
  simp [List.takeWhile, h]

private theorem takeWhile_neg {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = false) : (a :: as).takeWhile p = [] := by
  simp [List.takeWhile, h]

private theorem filter_takeWhile {α : Type} (p q : α → Bool) :
    ∀ (l : List α), (∀ x ∈ l, q x = false → p x = true) →
      (l.filter q).takeWhile p = (l.takeWhile p).filter q
  | [], _ => rfl
  | a :: as, h => by
      have ha : ∀ x ∈ as, q x = false → p x = true := fun x hx => h x (List.mem_cons_of_mem a hx)
      by_cases hq : q a = true
      · by_cases hp : p a = true
        · rw [List.filter_cons, if_pos hq, takeWhile_pos hp, filter_takeWhile p q as ha,
              takeWhile_pos hp, List.filter_cons, if_pos hq]
        · have hpf : p a = false := not_true_eq_false hp
          rw [List.filter_cons, if_pos hq, takeWhile_neg hpf, takeWhile_neg hpf,
              List.filter_nil]
      · have hp : p a = true := h a List.mem_cons_self (not_true_eq_false hq)
        rw [List.filter_cons, if_neg hq, takeWhile_pos hp, filter_takeWhile p q as ha,
          List.filter_cons, if_neg hq]

private theorem not_tomb {x : Slot} (h : (x != .tomb) = false) : x = .tomb := by
  cases x with
  | empty => exact absurd h (by decide)
  | tomb => rfl
  | live t k v => exact absurd h (by simp)

theorem sound : TombstoneErasureInRegister.Goal := by
  intro xs k
  show ((xs.filter (· != .tomb)).takeWhile (· != .empty)).findSome? (value k)
      = (xs.takeWhile (· != .empty)).findSome? (value k)
  rw [filter_takeWhile (· != .empty) (· != .tomb) xs (by
    intro x hx hq
    cases x with
    | empty => exact absurd hq (by decide)
    | tomb => decide
    | live t k v => exact absurd hq (by simp))]
  refine findSome?_filter k (xs.takeWhile (· != .empty)) (· != .tomb) (by
    intro x hx hq
    rw [not_tomb hq]
    rfl)
