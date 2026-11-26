import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_TicketJoinBlockCartesianMoments

For a fixed full join key, replicating each branch subtotal by the other two
cardinalities yields all three sums. The factorized block can be expanded
without materializing the Cartesian product.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_TicketJoinBlockCartesianMoments

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem total_flatMap {α β : Type} (xs : List α) (f : α → List β) (g : β → Int) :
    total (xs.flatMap f) g = total xs (fun x => total (f x) g) := by
  induction xs with
  | nil => simp [total]
  | cons x xs ih => simpa [total] using ih

private theorem total_const (xs : List Int) (k : Int) :
    total xs (fun _ => k) = (xs.length : Int) * k := by
  induction xs with
  | nil => simp [total]
  | cons x xs ih =>
      change k + total xs (fun _ => k) = ((x :: xs).length : Int) * k
      rw [ih]
      rw [show ((x :: xs).length : Int) = (xs.length : Int) + 1 by simp]
      rw [Int.add_mul]
      omega


private theorem inner_fst (c : List Int) (x y : Int) :
    (List.map (Prod.fst ∘ fun z : Int => (x, y, z)) c).sum = (c.length : Int) * x := by
  induction c with
  | nil => simp
  | cons z zs ih =>
      simp only [List.map_cons, Function.comp_apply, List.sum_cons, ih]
      rw [show ((z :: zs).length : Int) = (zs.length : Int) + 1 by simp]
      rw [Int.add_mul]
      omega

private theorem inner_snd_fst (c : List Int) (x y : Int) :
    (List.map ((fun p : Int × Int × Int => p.2.1) ∘ fun z : Int => (x, y, z)) c).sum =
      (c.length : Int) * y := by
  induction c with
  | nil => simp
  | cons z zs ih =>
      simp only [List.map_cons, Function.comp_apply, List.sum_cons, ih]
      rw [show ((z :: zs).length : Int) = (zs.length : Int) + 1 by simp]
      rw [Int.add_mul]
      omega

private theorem inner_snd_snd (c : List Int) (x y : Int) :
    (List.map ((fun p : Int × Int × Int => p.2.2) ∘ fun z : Int => (x, y, z)) c).sum = c.sum := by
  induction c with
  | nil => simp
  | cons z zs ih => simp [Function.comp_apply, ih]

private theorem sum_mul_const (xs : List Int) (k : Int) :
    (xs.map (fun x => x * k)).sum = xs.sum * k := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      rw [Int.add_mul]

private theorem sum_const' (xs : List Int) (k : Int) :
    (xs.map (fun _ => k)).sum = (xs.length : Int) * k := by
  simpa [total] using total_const xs k

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.TicketJoinBlockCartesianMoments.Goal := by
  intro s r c
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.TicketJoinBlockCartesianMoments.Goal]
  let bag := s.flatMap fun x => r.flatMap fun y => c.map fun z => (x, y, z)
  change (total bag Prod.fst, total bag (fun p => p.2.1), total bag (fun p => p.2.2)) =
    (s.sum * (r.length : Int) * c.length, r.sum * (s.length : Int) * c.length,
      c.sum * (s.length : Int) * r.length)
  have hfst : total bag Prod.fst = s.sum * (r.length : Int) * (c.length : Int) := by
    dsimp [bag]
    rw [total_flatMap]
    simp only [total_flatMap]
    simp [total, inner_fst, total_const, sum_mul_const, sum_const', Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  have hsnd : total bag (fun p => p.2.1) = r.sum * (s.length : Int) * (c.length : Int) := by
    dsimp [bag]
    rw [total_flatMap]
    simp only [total_flatMap]
    simp [total, inner_snd_fst, total_const, sum_mul_const, sum_const', Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  have hthd : total bag (fun p => p.2.2) = c.sum * (s.length : Int) * (r.length : Int) := by
    dsimp [bag]
    rw [total_flatMap]
    simp only [total_flatMap]
    simp [total, inner_snd_snd, total_const, sum_mul_const, sum_const', Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  exact Prod.ext hfst (Prod.ext hsnd hthd)

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_TicketJoinBlockCartesianMoments
