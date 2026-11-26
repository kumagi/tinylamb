import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

/-! Ground check on small inputs (both probes return `true`). -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun p => ls.all (fun z =>
    let ps := [p]
    (List.range (a.length + 1)).all (fun lo =>
      (List.range 4).all (fun hi =>
        agg.join (agg.reference (a.drop lo))
            (agg.join (combine agg ps) (agg.reference (z.take hi))) ==
          agg.reference (((a ++ ps.flatten ++ z).drop lo).take
            (a.length - lo + ps.flatten.length + hi)))))))

private def probe2 (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun p => ls.all (fun q => ls.all (fun z =>
    let ps := [p, q]
    (List.range (a.length + 1)).all (fun lo =>
      (List.range 4).all (fun hi =>
        agg.join (agg.reference (a.drop lo))
            (agg.join (combine agg ps) (agg.reference (z.take hi))) ==
          agg.reference (((a ++ ps.flatten ++ z).drop lo).take
            (a.length - lo + ps.flatten.length + hi))))))))

#eval probe 1
#eval probe 2
#eval probe 3
#eval probe2 2

/-! FrameSlice is a generic consequence of stitch: the page summaries fold
into the summary of their concatenation, and the two ragged boundary pieces
concatenate to exactly the requested window. -/

section Generic

variable {σ : Type} (f : Fold σ)

private theorem foldl_reference (h : stitch f) (l : List (List Nat)) (acc : List Nat) :
    List.foldl f.join (f.reference acc) (l.map f.reference) =
      f.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem combine_reference (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  simpa [combine] using foldl_reference f h ps []

private theorem slice_concat (a p z : List Nat) (lo hi : Nat) (hlo : lo ≤ a.length) :
    a.drop lo ++ p ++ z.take hi =
      ((a ++ p ++ z).drop lo).take (a.length - lo + p.length + hi) := by
  rw [show a ++ p ++ z = a ++ (p ++ z) by simp only [List.append_assoc],
    List.drop_append_of_le_length hlo, List.take_append]
  have hd : (a.drop lo).length = a.length - lo := by simp
  rw [hd]
  have hN : a.length - lo + p.length + hi - (a.length - lo) = p.length + hi := by omega
  rw [hN, List.take_append]
  have hp : p.length + hi - p.length = hi := by omega
  rw [hp]
  have htakeA : List.take (a.length - lo + p.length + hi) (a.drop lo) = a.drop lo :=
    List.take_of_length_le (by simp [hd]; omega)
  have htakeP : List.take (p.length + hi) p = p :=
    List.take_of_length_le (by omega)
  rw [htakeA, htakeP]
  exact List.append_assoc _ _ _

private theorem frameSlice_of_stitch (h : stitch f) : frameSlice f := by
  intro a ps z lo hi hlo
  rw [combine_reference f h, h, h]
  refine congrArg f.reference ?_
  simpa only [List.append_assoc] using slice_concat a ps.flatten z lo hi hlo

end Generic

/-! Stitch for PairSubsequence: the ordered-pair count splits into the two
local counts plus the cross term `#1(left) * #2(right)`. -/

private def pairCount (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0)).sum

private theorem zipIdx_shift {α : Type} (xs : List α) (k j : Nat) :
    xs.zipIdx (k + j) = (xs.zipIdx k).map (fun p => (p.1, p.2 + j)) := by
  induction xs generalizing k with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      rw [show k + j + 1 = k + 1 + j by omega, ih]

private theorem pairCount_cons (x : Nat) (xs : List Nat) :
    pairCount (x :: xs) = (if x == 1 then xs.count 2 else 0) + pairCount xs := by
  simp only [pairCount, List.zipIdx_cons, List.map_cons, List.sum_cons]
  rw [show xs.zipIdx 1 = (xs.zipIdx).map (fun p => (p.1, p.2 + 1)) by
    simpa using zipIdx_shift xs 0 1]
  simp [Function.comp_def]

private theorem pairCount_append (a b : List Nat) :
    pairCount (a ++ b) = pairCount a + pairCount b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [pairCount]
  | cons x xs ih =>
      rw [List.cons_append, pairCount_cons, pairCount_cons, ih]
      by_cases h : x == 1 <;> simp [h, List.count_cons, Nat.succ_mul] <;> omega

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.count 1 + b.count 1, a.count 2 + b.count 2,
      pairCount a + pairCount b + a.count 1 * b.count 2) =
    ((a ++ b).count 1, (a ++ b).count 2, pairCount (a ++ b))
  rw [pairCount_append]
  simp [List.count_append]

theorem sound_FrameSlice : PairSubsequence.FrameSlice.Goal := by
  exact frameSlice_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_FrameSlice
