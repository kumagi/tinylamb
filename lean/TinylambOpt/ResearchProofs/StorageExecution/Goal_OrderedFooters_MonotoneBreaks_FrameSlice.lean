import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `MonotoneBreaks.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

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

#eval probe 2
#eval probe 3

private theorem getLastD_eq (b : List Nat) (t u : Nat) (hb : b ≠ []) :
    b.getLast?.getD t = b.getLast?.getD u := by
  simp [List.getLast?_eq_some_getLast hb]

private theorem getLast?_append_of_ne (a b : List Nat) (hb : b ≠ []) :
    (a ++ b).getLast? = b.getLast? := by
  simp [List.getLast?_append, List.getLast?_eq_some_getLast hb]

private theorem edges_append (xs b : List Nat) (x : Nat) (hb : b ≠ []) :
    edges ((x :: xs) ++ b) =
      edges (x :: xs) ++ ((xs.getLast?.getD x, b.head hb) :: edges b) := by
  induction xs generalizing b x with
  | nil =>
      cases b with
      | nil => contradiction
      | cons y ys => simp [edges]
  | cons y ys ih =>
      cases b with
      | nil => contradiction
      | cons z zs =>
          have hih := ih (z :: zs) y (by simp)
          simp [edges, List.getLast?_cons] at hih ⊢
          exact hih

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

/-! Stitch for MonotoneBreaks: endpoints compose, and the descending edges of
the concatenation consist of those of each side plus the bridging edge. -/

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          simp only [agg, List.head?_cons, Option.map_some, optionalJoin]
          simp only [List.cons_append, List.head?_cons, Option.map_some]
          have hlast : (x :: (xs ++ (y :: ys))).getLast?.getD x =
              (y :: ys).getLast?.getD y := by
            simp only [List.getLast?_cons, Option.getD_some]
            rw [getLast?_append_of_ne xs (y :: ys) (by simp)]
            simp [List.getLast?_cons]
          rw [hlast]
          have hedges : edges (x :: (xs ++ (y :: ys))) =
              edges (x :: xs) ++ ((xs.getLast?.getD x, y) :: edges (y :: ys)) := by
            simpa [List.cons_append] using edges_append xs (y :: ys) x (by simp)
          rw [hedges]
          simp [List.countP_append, List.countP_cons, List.getLast?_cons]
          by_cases hlt : y < xs.getLast?.getD x <;> simp [hlt] <;> omega

theorem sound_FrameSlice : MonotoneBreaks.FrameSlice.Goal := by
  exact frameSlice_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_FrameSlice
