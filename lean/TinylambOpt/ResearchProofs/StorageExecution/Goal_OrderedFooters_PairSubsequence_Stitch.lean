import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_Stitch

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

private def pairFrom (xs : List Nat) (k : Nat) : Nat :=
  ((xs.zipIdx k).map (fun (v, i) => if v == 1 then (xs.drop (i + 1 - k)).count 2 else 0)).sum

private theorem drop_cons_of_le' (x : Nat) {xs : List Nat} {i k : Nat} (h : k + 1 ≤ i) :
    (x :: xs).drop (i + 1 - k) = xs.drop (i + 1 - (k + 1)) := by
  have h1 : i + 1 - k = (i + 1 - (k + 1)) + 1 := by omega
  rw [h1, List.drop_succ_cons]

private theorem pairFrom_shift (xs : List Nat) (k : Nat) : pairFrom xs (k + 1) = pairFrom xs k := by
  induction xs generalizing k with
  | nil => simp [pairFrom]
  | cons x xs ih =>
      unfold pairFrom at ih ⊢
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      have hhead1 : (x :: xs).drop ((k + 1) + 1 - (k + 1)) = xs := by
        have : (k + 1) + 1 - (k + 1) = 1 := by omega
        rw [this]
        simp
      have hhead2 : (x :: xs).drop (k + 1 - k) = xs := by
        have : k + 1 - k = 1 := by omega
        rw [this]
        simp
      rw [hhead1, hhead2]
      have htailL :
          List.map (fun p : Nat × Nat => if p.1 == 1 then ((x :: xs).drop (p.2 + 1 - (k + 1))).count 2 else 0)
              (xs.zipIdx ((k + 1) + 1)) =
          List.map (fun p : Nat × Nat => if p.1 == 1 then (xs.drop (p.2 + 1 - ((k + 1) + 1))).count 2 else 0)
              (xs.zipIdx ((k + 1) + 1)) := by
        apply List.map_congr_left
        intro p hp
        have hb : (k + 1) + 1 ≤ p.2 := (List.mem_zipIdx hp).1
        rw [drop_cons_of_le' x hb]
      have htailR :
          List.map (fun p : Nat × Nat => if p.1 == 1 then ((x :: xs).drop (p.2 + 1 - k)).count 2 else 0)
              (xs.zipIdx (k + 1)) =
          List.map (fun p : Nat × Nat => if p.1 == 1 then (xs.drop (p.2 + 1 - (k + 1))).count 2 else 0)
              (xs.zipIdx (k + 1)) := by
        apply List.map_congr_left
        intro p hp
        have hb : k + 1 ≤ p.2 := (List.mem_zipIdx hp).1
        rw [drop_cons_of_le' x hb]
      rw [htailL, htailR]
      exact congrArg (fun n => (if x == 1 then xs.count 2 else 0) + n) (ih (k + 1))

private def pairCount (xs : List Nat) : Nat := pairFrom xs 0

private theorem pairCount_cons (x : Nat) (xs : List Nat) :
    pairCount (x :: xs) = (if x == 1 then xs.count 2 else 0) + pairCount xs := by
  unfold pairCount
  unfold pairFrom
  simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
  have hhead : (x :: xs).drop (0 + 1 - 0) = xs := by simp
  rw [hhead]
  have htail :
      List.map (fun p : Nat × Nat => if p.1 == 1 then ((x :: xs).drop (p.2 + 1 - 0)).count 2 else 0)
          (xs.zipIdx (0 + 1)) =
      List.map (fun p : Nat × Nat => if p.1 == 1 then (xs.drop (p.2 + 1 - 1)).count 2 else 0)
          (xs.zipIdx 1) := by
    apply List.map_congr_left
    intro p hp
    have hb : 1 ≤ p.2 := (List.mem_zipIdx hp).1
    rw [show p.2 + 1 - 0 = p.2 + 1 by omega, List.drop_succ_cons]
    rw [show p.2 + 1 - 1 = p.2 by omega]
  rw [htail]
  change (if x == 1 then xs.count 2 else 0) + pairFrom xs 1 =
      (if x == 1 then xs.count 2 else 0) + pairFrom xs 0
  exact congrArg (fun n => (if x == 1 then xs.count 2 else 0) + n) (pairFrom_shift xs 0)

private theorem pairCount_append (a b : List Nat) :
    pairCount (a ++ b) = pairCount a + pairCount b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [pairCount, pairFrom]
  | cons x xs ih =>
      change pairCount (x :: (xs ++ b)) = pairCount (x :: xs) + pairCount b + (x :: xs).count 1 * b.count 2
      rw [pairCount_cons x (xs ++ b), pairCount_cons x xs, ih]
      simp only [List.count_append, List.count_cons]
      by_cases h : x == 1
      · simp [h, Nat.add_mul]
        omega
      · simp [h]

theorem sound_Stitch : PairSubsequence.Stitch.Goal := by
  intro a b
  change agg.join (agg.reference a) (agg.reference b) = agg.reference (a ++ b)
  simp [agg, List.count_append]
  have hp := pairCount_append a b
  change pairCount (a ++ b) = pairCount a + pairCount b + a.count 1 * b.count 2 at hp
  simpa [pairCount, pairFrom] using hp.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_Stitch
