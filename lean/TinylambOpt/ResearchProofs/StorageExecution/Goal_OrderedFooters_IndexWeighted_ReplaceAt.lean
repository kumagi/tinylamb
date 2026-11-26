import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `IndexWeighted.ReplaceAt` — `∀ xs i x, i<xs.length → (agg.reference (xs.set i
  x)).2.2+i*(xs[i]?.getD 0)=(agg.reference xs).2.2+i*x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_ReplaceAt

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

-- Small-input truth check (both successful and out-of-range indices).
#eval let samples : List (List Nat) := [[], [0], [1, 2, 3, 4], [3, 0, 2], [0, 0, 0], [5, 1, 7, 2, 9]]
  samples.all (fun xs =>
    (List.range (xs.length + 2)).all (fun i =>
      (List.range 6).all (fun x =>
        decide (i < xs.length → (agg.reference (xs.set i x)).2.2 + i * (xs[i]?.getD 0) =
          (agg.reference xs).2.2 + i * x))))

private def moment (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, j) => j * v)).sum

private theorem moment_eq (xs : List Nat) : (agg.reference xs).2.2 = moment xs := by
  rfl

private theorem zipIdx_add (xs : List Nat) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, j) => (v, j + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem zipIdx_fst (xs : List Nat) (n : Nat) :
    (xs.zipIdx n).map (fun (v, _) => v) = xs := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_succ_weight (ps : List (Nat × Nat)) :
    (ps.map (fun (v, j) => (j + 1) * v)).sum =
      (ps.map (fun (v, j) => j * v)).sum + (ps.map (fun (v, _) => v)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨v, j⟩
      simp only [List.map_cons, List.sum_cons, ih]
      simp [Nat.succ_mul, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

-- Replacing the head does not change the weighted sum: its weight is zero,
-- and all tail indices shift by one.
private theorem moment_cons (a : Nat) (xs : List Nat) : moment (a :: xs) = moment xs + xs.sum := by
  unfold moment
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, Nat.zero_mul, Nat.zero_add]
  rw [show xs.zipIdx 1 = xs.zipIdx.map (fun (v, j) => (v, j + 1)) by
    simpa using zipIdx_add xs 0 1]
  rw [List.map_map]
  rw [← zipIdx_fst xs 0]
  simpa [Function.comp_def] using (sum_succ_weight xs.zipIdx)

private theorem sum_set (xs : List Nat) (i : Nat) (x : Nat) (hi : i < xs.length) :
    (xs.set i x).sum + xs[i]?.getD 0 = xs.sum + x := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons a xs ih =>
      cases i with
      | zero =>
          simp
          omega
      | succ j =>
          simp only [List.getElem?_cons_succ, List.set_cons_succ, List.length_cons,
            List.sum_cons, Nat.succ_lt_succ_iff] at hi ⊢
          have h := ih j hi
          omega

theorem sound_ReplaceAt : IndexWeighted.ReplaceAt.Goal := by
  intro xs i x hi
  rw [moment_eq, moment_eq]
  induction xs generalizing i with
  | nil => simp at hi
  | cons a xs ih =>
      cases i with
      | zero =>
          simp [moment_cons]
      | succ j =>
          simp only [List.getElem?_cons_succ, List.set_cons_succ, List.length_cons,
            Nat.succ_lt_succ_iff] at hi ⊢
          rw [moment_cons a (xs.set j x), moment_cons a xs]
          have hm := ih j hi
          have hs := sum_set xs j x hi
          simp only [Nat.succ_mul]
          omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_ReplaceAt
