import TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema

/-!
# ContiguousFileRowIdTotals

For contiguous row ID runs, the row count and sum equal the closed-form progression
totals weighted by file occurrences in the manifest.

An executor with certified run bounds can compute COUNT and exact SUM by range. It needs
the count to recover SQL SUM's NULL result on empty input.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ContiguousFileRowIdTotals
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema


theorem sum_flatMap {α : Type} (l : List α) (f : α → List Nat) :
    (l.flatMap f).sum = (l.map (fun x => (f x).sum)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem sum_replicate (c x : Nat) : (List.replicate c x).sum = c * x := by
  induction c with
  | zero => simp
  | succ c ih => simp [ih, Nat.mul_add, Nat.add_mul, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]

theorem sum_map_mul_left (c : Nat) (l : List Nat) :
    (l.map (fun x => c * x)).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]

theorem sum_map_mul_add (c base : Nat) (l : List Nat) :
    (l.map (fun i => c * (base + i))).sum = c * (l.map (fun i => base + i)).sum := by
  have hmap : l.map (fun i => c * (base + i)) = (l.map (fun i => base + i)).map (fun x => c * x) := by
    rw [List.map_map]
    rfl
  rw [hmap, sum_map_mul_left]

theorem twice_sum_range (base n : Nat) :
    2 * ((List.range n).map (fun i => base + i)).sum = n * (2*base + n - 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      cases n with
      | zero => simp
      | succ n =>
          rw [List.range_succ, List.map_append, List.sum_append]
          simp only [List.map_singleton, List.sum_cons, List.sum_nil, Nat.add_zero]
          have hmul : 2 * (((List.range (n+1)).map (fun i => base+i)).sum + (base + (n+1))) =
              2 * ((List.range (n+1)).map (fun i => base+i)).sum + 2*(base+(n+1)) := by omega
          rw [hmul, ih]
          simp [Nat.mul_add, Nat.add_mul, Nat.mul_comm, Nat.mul_left_comm]
          omega

theorem sum_range_arith (base n : Nat) :
    ((List.range n).map (fun i => base + i)).sum = n * (2*base + n - 1) / 2 := by
  exact Nat.eq_div_of_mul_eq_right (by decide : (2:Nat) ≠ 0) (twice_sum_range base n)

theorem sum_const_range (c n : Nat) : ((List.range n).map (fun _ => c)).sum = c * n := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.range_succ, ih, Nat.mul_add, Nat.add_mul, Nat.mul_comm, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem length_run (c base n : Nat) :
    ((List.range n).flatMap fun i => List.replicate c (base + i)).length = c * n := by
  rw [List.length_flatMap]
  simp only [List.length_replicate, List.map_const]
  exact sum_const_range c n

theorem sum_run (c base n : Nat) :
    ((List.range n).flatMap fun i => List.replicate c (base + i)).sum =
      c * (n * (2 * base + n - 1) / 2) := by
  rw [sum_flatMap]
  have h1 : (List.range n).map (fun i => (List.replicate c (base+i)).sum) =
            (List.range n).map (fun i => c * (base+i)) := by
    apply List.map_congr_left
    intro i hi
    rw [sum_replicate]
  rw [h1]
  rw [sum_map_mul_add, sum_range_arith]


theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema.ContiguousFileRowIdTotals.Goal := by
  intro runs manifest
  dsimp
  apply Prod.ext
  · rw [List.length_flatMap]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rcases r with ⟨f, base, n⟩
    simpa [Nat.mul_comm] using TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ContiguousFileRowIdTotals.length_run (manifest.count f) base n
  · rw [TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ContiguousFileRowIdTotals.sum_flatMap]
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rcases r with ⟨f, base, n⟩
    simpa using TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ContiguousFileRowIdTotals.sum_run (manifest.count f) base n
end TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ContiguousFileRowIdTotals
