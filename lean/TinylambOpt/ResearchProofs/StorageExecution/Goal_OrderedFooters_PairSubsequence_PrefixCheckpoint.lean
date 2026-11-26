import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

private def pc (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun p => if p.1 == 1 then (xs.drop (p.2 + 1)).count 2 else 0)).sum

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun p => (p.1, p.2 + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      rw [show n + m + 1 = (n + 1) + m by omega]
      rw [ih (n + 1)]
      simp

private theorem pc_cons (x : Nat) (xs : List Nat) :
    pc (x :: xs) = (if x == 1 then xs.count 2 else 0) + pc xs := by
  unfold pc
  rw [List.zipIdx_cons']
  simp only [List.map_cons, List.sum_cons, List.map_map, Function.comp_def, Prod.map,
    List.drop_succ_cons]
  rfl

private theorem pc_append_left (a b : List Nat) :
    (a.zipIdx.map (fun p => if p.1 == 1 then ((a ++ b).drop (p.2 + 1)).count 2 else 0)).sum =
      pc a + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [pc]
  | cons x xs ih =>
      rw [List.zipIdx_cons']
      simp only [List.map_cons, List.sum_cons, List.map_map, Function.comp_def, Prod.map,
        List.count_cons, id_eq]
      have htail :
          (xs.zipIdx.map (fun p => if p.1 == 1 then ((x :: xs ++ b).drop (p.2 + 1 + 1)).count 2 else 0)).sum =
          (xs.zipIdx.map (fun p => if p.1 == 1 then ((xs ++ b).drop (p.2 + 1)).count 2 else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro p hp
        simp [Nat.add_assoc]
      rw [htail, ih]
      rw [pc_cons]
      by_cases hx : x == 1
      · have hx1 : x = 1 := by simpa using hx
        simp [hx1, List.count_append, Nat.add_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · simp [hx]

private theorem pc_append_right (a b : List Nat) :
    ((b.zipIdx a.length).map (fun p => if p.1 == 1 then ((a ++ b).drop (p.2 + 1)).count 2 else 0)).sum =
      pc b := by
  induction a with
  | nil => simp [pc]
  | cons x xs ih =>
      rw [show (x :: xs).length = xs.length + 1 by simp]
      rw [zipIdx_add b xs.length 1]
      simp only [List.map_map, Function.comp_def]
      have htail :
          ((b.zipIdx xs.length).map (fun p => if p.1 == 1 then ((x :: xs ++ b).drop (p.2 + 1 + 1)).count 2 else 0)).sum =
          ((b.zipIdx xs.length).map (fun p => if p.1 == 1 then ((xs ++ b).drop (p.2 + 1)).count 2 else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro p hp
        simp [Nat.add_assoc]
      rw [htail, ih]

private theorem pc_append (a b : List Nat) : pc (a ++ b) = pc a + a.count 1 * b.count 2 + pc b := by
  unfold pc
  rw [List.zipIdx_append]
  simp only [List.map_append, List.sum_append]
  rw [show 0 + a.length = a.length by omega]
  rw [show (a.zipIdx.map (fun p => if p.1 == 1 then ((a ++ b).drop (p.2 + 1)).count 2 else 0)).sum =
      pc a + a.count 1 * b.count 2 from pc_append_left a b]
  rw [show ((b.zipIdx a.length).map (fun p => if p.1 == 1 then ((a ++ b).drop (p.2 + 1)).count 2 else 0)).sum =
      pc b from pc_append_right a b]
  unfold pc
  omega

private theorem stitchAgg : stitch agg := by
  intro a b
  change agg.join (agg.reference a) (agg.reference b) = agg.reference (a ++ b)
  simp only [agg, List.count_append]
  apply Prod.ext
  · rfl
  apply Prod.ext
  · rfl
  · change pc a + pc b + a.count 1 * b.count 2 = pc (a ++ b)
    rw [pc_append]
    omega

theorem sound_PrefixCheckpoint : PairSubsequence.PrefixCheckpoint.Goal := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.scanl_cons]
      rw [stitchAgg init p]
      rw [ih (init ++ p)]
      simp [List.range_succ_eq_map, Function.comp_def, List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_PrefixCheckpoint
