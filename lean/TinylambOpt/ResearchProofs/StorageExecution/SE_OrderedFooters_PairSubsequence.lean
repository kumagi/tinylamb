import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.PairSubsequence
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

def P (xs : List Nat) : Nat :=
  (xs.zipIdx.map fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0).sum

theorem ref_eq (xs : List Nat) :
    PairSubsequence.agg.reference xs = (xs.count 1, xs.count 2, P xs) := rfl

theorem join_eq (p q : Nat × Nat × Nat) :
    PairSubsequence.agg.join p q = (p.1 + q.1, p.2.1 + q.2.1, p.2.2 + q.2.2 + p.1 * q.2.1) := rfl

private theorem zipIdx_succ (l : List Nat) :
    ∀ k, ((l.zipIdx k).map fun (v, i) => (v, i + 1)) = l.zipIdx (k + 1) := by
  induction l with
  | nil => intro k; rfl
  | cons y l ih =>
      intro k
      rw [List.zipIdx_cons, List.map_cons, ih (k + 1), List.zipIdx_cons]

private theorem P_cons (x : Nat) (l : List Nat) :
    P (x :: l) = P l + (if x == 1 then l.count 2 else 0) := by
  unfold P
  rw [List.zipIdx_cons (i := 0), List.map_cons, List.sum_cons]
  have hb : ((match (x, 0) with
    | (v, i) => if v == 1 then ((x :: l).drop (i + 1)).count 2 else 0) : Nat)
      = (if x == 1 then l.count 2 else 0) := rfl
  rw [hb]
  have hmap : ((l.zipIdx (0 + 1)).map fun (v, i) =>
        if v == 1 then ((x :: l).drop (i + 1)).count 2 else 0)
      = ((l.zipIdx 0).map fun (v, i) => if v == 1 then (l.drop (i + 1)).count 2 else 0) := by
    rw [← zipIdx_succ l 0, List.map_map]
    refine congrArg (List.map · l.zipIdx) ?_
    funext p
    rcases p with ⟨v, i⟩
    by_cases hv : v = 1 <;> simp [hv, List.drop_cons]
  rw [hmap]
  omega

theorem P_append (a b : List Nat) :
    P (a ++ b) = P a + P b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [P]
  | cons x t ih =>
      have hcc : ((x :: t : List Nat).count 1 : Nat) = t.count 1 + (if x == 1 then 1 else 0) := by
        by_cases hx : x = 1 <;> simp [hx]
      rw [show (x :: t) ++ b = x :: (t ++ b) from rfl, P_cons, P_cons, ih, hcc,
        List.count_append]
      by_cases hx : x = 1
      · simp [hx, Nat.add_mul, Nat.one_mul]
        omega
      · simp [hx]

theorem stitch_agg : stitch PairSubsequence.agg := by
  intro a b
  rw [ref_eq, ref_eq, ref_eq, join_eq]
  dsimp only
  rw [List.count_append, List.count_append, P_append]

theorem sound_Stitch : PairSubsequence.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : PairSubsequence.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : PairSubsequence.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : PairSubsequence.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : PairSubsequence.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : PairSubsequence.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : PairSubsequence.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : PairSubsequence.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem sound_ReversalComplement : PairSubsequence.ReversalComplement.Goal := by
  intro xs
  induction xs with
  | nil =>
      rw [ref_eq]
      rfl
  | cons x xs ih =>
      have hrev : (x :: xs : List Nat).reverse = xs.reverse ++ [x] := List.reverse_cons
      have hP1 : P ((x :: xs : List Nat).reverse)
          = P xs.reverse + xs.count 1 * (if x == 2 then 1 else 0) := by
        rw [hrev, P_append]
        have h1 : P ([x] : List Nat) = 0 := by
          unfold P
          rw [List.zipIdx_cons (i := 0), List.map_cons, List.zipIdx_nil, List.map_nil,
            List.sum_cons, List.sum_nil]
          by_cases hx : x = 1 <;> simp [hx]
        have h2 : (([x] : List Nat).count 2 : Nat) = if x == 2 then 1 else 0 := by
          by_cases hx : x = 2 <;> simp [hx]
        have h3 : (xs.reverse.count 1 : Nat) = xs.count 1 := List.count_reverse
        rw [h1, h2, h3]
        simp
      have hP2 : P (x :: xs) = P xs + (if x == 1 then xs.count 2 else 0) := P_cons x xs
      have hc1 : ((x :: xs : List Nat).count 1 : Nat) = xs.count 1 + (if x == 1 then 1 else 0) := by
        by_cases hx : x = 1 <;> simp [hx]
      have hc2 : ((x :: xs : List Nat).count 2 : Nat) = xs.count 2 + (if x == 2 then 1 else 0) := by
        by_cases hx : x = 2 <;> simp [hx]
      have hr : (PairSubsequence.agg.reference (x :: xs)).2.2 = P (x :: xs) := rfl
      have hrr : (PairSubsequence.agg.reference ((x :: xs : List Nat).reverse)).2.2
          = P ((x :: xs : List Nat).reverse) := rfl
      have hih : P xs + P ((xs : List Nat).reverse) = xs.count 1 * xs.count 2 := ih
      rw [hr, hrr]
      rw [hP1, hP2]
      rw [hc1, hc2]
      by_cases hx : x = 1
      · subst hx
        simp
        rw [Nat.add_mul, Nat.one_mul]
        omega
      · by_cases hy : x = 2
        · subst hy
          simp
          rw [Nat.mul_add, Nat.mul_one]
          omega
        · have hb : (x == 1) = false := by simp_all
          have hn : (x == 2) = false := by simp_all
          rw [hb, hn]
          simp
          omega

private theorem P_rep1 : ∀ n : Nat, P (List.replicate n 1) = 0 := by
  intro n
  induction n with
  | zero => simp [P]
  | succ n ih =>
      rw [List.replicate_succ, P_cons, ih]
      have h : (List.replicate n 1).count 2 = 0 := by
        rw [List.count_replicate]
        simp
      rw [h]
      simp

private theorem P_rep2 : ∀ n : Nat, P (List.replicate n 2) = 0 := by
  intro n
  induction n with
  | zero => simp [P]
  | succ n ih =>
      rw [List.replicate_succ, P_cons, ih]
      simp

theorem sound_BatchedMarkers : PairSubsequence.BatchedMarkers.Goal := by
  intro a b
  have hc1 : ((List.replicate a 1 ++ List.replicate b 2 : List Nat).count 1 : Nat) = a := by
    rw [List.count_append, List.count_replicate, List.count_replicate]
    simp
  have hc2 : ((List.replicate a 1 ++ List.replicate b 2 : List Nat).count 2 : Nat) = b := by
    rw [List.count_append, List.count_replicate, List.count_replicate]
    simp
  show PairSubsequence.agg.reference (List.replicate a 1 ++ List.replicate b 2) = (a, b, a * b)
  rw [ref_eq, P_append, P_rep1, P_rep2, hc1, hc2]
  simp
