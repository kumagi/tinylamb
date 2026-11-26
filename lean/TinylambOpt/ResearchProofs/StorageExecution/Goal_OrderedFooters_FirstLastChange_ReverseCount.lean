import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_ReverseCount

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

-- Ground check on small inputs (all probes must return `true`).
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  (gen n).all (fun xs =>
    (agg.reference xs.reverse).map (fun r => r.2.2) ==
      (agg.reference xs).map (fun r => r.2.2))

#eval probe 0
#eval probe 1
#eval probe 2
#eval probe 3
#eval probe 4

-- Number of adjacent changes; this is the third component of `agg.reference`.
private def changes (xs : List Nat) : Nat :=
  (edges xs).countP (fun p : Nat × Nat => p.1 != p.2)

private theorem changes_cons_cons (x y : Nat) (rest : List Nat) :
    changes (x :: y :: rest) = (if x != y then 1 else 0) + changes (y :: rest) := by
  unfold changes edges
  simp [List.zip_cons_cons, List.countP_cons, Nat.add_comm]

private theorem changes_append_singleton (l : List Nat) (z : Nat) :
    changes (l ++ [z]) =
      changes l + (match l.getLast? with | none => 0 | some y => if y != z then 1 else 0) := by
  induction l with
  | nil => simp [changes, edges]
  | cons a l ih =>
      cases l with
      | nil =>
          simp [changes, edges, List.countP_cons]
      | cons b l =>
          have ih' : changes (b :: (l ++ [z])) =
              changes (b :: l) +
                (match (b :: l).getLast? with | none => 0 | some y => if y != z then 1 else 0) := by
            simpa only [List.cons_append] using ih
          rw [show (a :: b :: l) ++ [z] = a :: (b :: (l ++ [z])) by
            simp only [List.cons_append]]
          rw [changes_cons_cons a b (l ++ [z]), ih', changes_cons_cons a b l]
          simp [List.getLast?_cons_cons]
          omega

private theorem beq_comm_nat : ∀ x y : Nat, (x == y) = (y == x)
  | 0, 0 => rfl
  | 0, _ + 1 => by simp
  | _ + 1, 0 => by simp
  | x + 1, y + 1 => by simpa using beq_comm_nat x y

private theorem bne_comm_nat (x y : Nat) : (x != y) = (y != x) := by
  rw [bne, bne, beq_comm_nat]

private theorem changes_reverse : ∀ xs : List Nat, changes xs.reverse = changes xs
  | [] => by simp [changes, edges]
  | x :: xs => by
      rw [List.reverse_cons, changes_append_singleton, List.getLast?_reverse, changes_reverse xs]
      cases xs with
      | nil => simp [changes, edges]
      | cons y ys =>
          rw [changes_cons_cons]
          simp [bne_comm_nat]
          omega

theorem sound_ReverseCount : FirstLastChange.ReverseCount.Goal := by
  intro xs
  cases xs with
  | nil => simp [agg]
  | cons x xs =>
      simpa [agg, changes] using changes_reverse (x :: xs)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_ReverseCount
