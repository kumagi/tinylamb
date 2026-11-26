import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `Drawdown.TreeReduce` — Merge an arbitrary spill tree without visiting the raw rows
  again.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

private def lo (xs : List Nat) : Nat := xs.min?.getD 0
private def hi (xs : List Nat) : Nat := peak xs

-- A structurally recursive version of the drawdown component of `agg.reference`.
private def ddAlt : List Nat → Nat
  | [] => 0
  | x :: xs => max (x - lo (x :: xs)) (ddAlt xs)

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x :: xs) = max x (peak xs) := by
  unfold peak
  cases xs with
  | nil => simp
  | cons y ys => simp [List.max?_cons]

private theorem peak_append (a b : List Nat) : peak (a ++ b) = max (peak a) (peak b) := by
  induction a with
  | nil => simp [peak]
  | cons x xs ih => simp only [List.cons_append, peak_cons, ih]; omega

private theorem lo_cons_of_ne (x : Nat) (l : List Nat) (hl : l ≠ []) : lo (x :: l) = min x (lo l) := by
  cases l with
  | nil => cases hl rfl
  | cons y ys => simp [lo, List.min?_cons]

private theorem lo_snoc (a : List Nat) (x : Nat) (ha : a ≠ []) : lo (a ++ [x]) = min (lo a) x := by
  induction a with
  | nil => cases ha rfl
  | cons y ys ih =>
      cases ys with
      | nil => simp [lo, List.min?_cons]
      | cons z zs =>
          change lo (y :: ((z :: zs) ++ [x])) = min (lo (y :: z :: zs)) x
          rw [lo_cons_of_ne y ((z :: zs) ++ [x]) (by simp)]
          rw [ih (by simp)]
          rw [lo_cons_of_ne y (z :: zs) (by simp)]
          omega

private theorem lo_append_nonempty (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    lo (a ++ b) = min (lo a) (lo b) := by
  induction a with
  | nil => cases ha rfl
  | cons y ys ih =>
      cases ys with
      | nil =>
          change lo (y :: b) = min (lo [y]) (lo b)
          rw [lo_cons_of_ne y b hb]
          simp [lo]
      | cons z zs =>
          change lo (y :: ((z :: zs) ++ b)) = min (lo (y :: z :: zs)) (lo b)
          rw [lo_cons_of_ne y ((z :: zs) ++ b) (by simp)]
          rw [ih (by simp)]
          rw [lo_cons_of_ne y (z :: zs) (by simp)]
          omega

private theorem hi_snoc (a : List Nat) (x : Nat) : hi (a ++ [x]) = max (hi a) x := by
  rw [hi, hi, peak_append]
  simp [peak]

private theorem hi_append (a b : List Nat) : hi (a ++ b) = max (hi a) (hi b) := by
  change peak (a ++ b) = max (peak a) (peak b)
  exact peak_append a b

private theorem ddAlt_snoc (a : List Nat) (x : Nat) (ha : a ≠ []) :
    ddAlt (a ++ [x]) = max (ddAlt a) (hi a - x) := by
  induction a with
  | nil => cases ha rfl
  | cons y ys ih =>
      cases ys with
      | nil =>
          change ddAlt [y, x] = max (ddAlt [y]) (hi [y] - x)
          simp [ddAlt, hi, lo, peak, List.min?_cons]
          omega
      | cons z zs =>
          change max (y - lo (y :: ((z :: zs) ++ [x]))) (ddAlt ((z :: zs) ++ [x])) =
            max (max (y - lo (y :: z :: zs)) (ddAlt (z :: zs))) (hi (y :: z :: zs) - x)
          rw [lo_cons_of_ne y ((z :: zs) ++ [x]) (by simp)]
          rw [lo_snoc (z :: zs) x (by simp)]
          rw [ih (by simp)]
          rw [lo_cons_of_ne y (z :: zs) (by simp)]
          have hhi1 : hi (y :: z :: zs) = max y (hi (z :: zs)) := by
            rw [hi, hi, peak_cons]
          rw [hhi1]
          have hhi2 : hi (z :: zs) = max z (hi zs) := by
            rw [hi, hi, peak_cons]
          rw [hhi2]
          rw [hi]
          omega

private theorem ddAlt_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    ddAlt (a ++ b) = max (max (ddAlt a) (ddAlt b)) (hi a - lo b) := by
  induction b generalizing a with
  | nil => cases hb rfl
  | cons y bs ih =>
      cases bs with
      | nil =>
          rw [ddAlt_snoc a y ha]
          simp [ddAlt, lo]
      | cons z zs =>
          rw [show a ++ (y :: z :: zs) = (a ++ [y]) ++ (z :: zs) by simp [List.append_assoc]]
          rw [ih (a ++ [y]) (by simp [ha]) (by simp)]
          rw [ddAlt_snoc a y ha, hi_snoc]
          change max (max (max (ddAlt a) (hi a - y)) (ddAlt (z :: zs)))
              (max (hi a) y - lo (z :: zs)) =
            max (max (ddAlt a) (max (y - lo (y :: z :: zs)) (ddAlt (z :: zs))))
              (hi a - lo (y :: z :: zs))
          rw [lo_cons_of_ne y (z :: zs) (by simp)]
          omega

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun p => (p.1, p.2 + m)) := by
  induction xs generalizing n m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons]
      congr 1
      rw [show n + m + 1 = n + 1 + m by omega]
      exact ih (n + 1) m

private theorem min?_getD_eq_of_ne (l : List Nat) (hl : l ≠ []) (d e : Nat) :
    l.min?.getD d = l.min?.getD e := by
  cases l with
  | nil => cases hl rfl
  | cons y ys => simp

private theorem dd_eq_ddAlt (xs : List Nat) :
    peak (xs.zipIdx.map (fun p => p.1 - ((xs.drop p.2).min?.getD p.1))) = ddAlt xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      unfold ddAlt
      rw [show (x :: xs).zipIdx = (x, 0) :: xs.zipIdx 1 by simp [List.zipIdx_cons]]
      simp only [List.map_cons]
      rw [peak_cons]
      simp only [List.drop_zero]
      rw [min?_getD_eq_of_ne (x :: xs) (by simp) x 0]
      change max (x - lo (x :: xs))
          (peak (List.map (fun p => p.1 - ((x :: xs).drop p.2).min?.getD p.1) (xs.zipIdx 1))) =
        max (x - lo (x :: xs)) (ddAlt xs)
      congr 1
      have hshift : xs.zipIdx 1 = (xs.zipIdx).map (fun p => (p.1, p.2 + 1)) := by
        simpa using zipIdx_add xs 0 1
      rw [hshift]
      simp only [List.map_map]
      change peak (xs.zipIdx.map (fun p => p.1 - ((x :: xs).drop (p.2 + 1)).min?.getD p.1)) = ddAlt xs
      simp only [List.drop_succ_cons]
      exact ih

private theorem reference_of_ne (xs : List Nat) (hxs : xs ≠ []) :
    agg.reference xs = some (lo xs, hi xs, ddAlt xs) := by
  cases xs with
  | nil => cases hxs rfl
  | cons x xs =>
      simp only [agg, List.head?_cons, Option.map_some]
      rw [dd_eq_ddAlt]
      rfl

private theorem stitch_agg : stitch agg := by
  intro a b
  cases ha : a with
  | nil =>
      subst a
      simp [agg, optionalJoin]
  | cons ax as =>
      cases hb : b with
      | nil =>
          subst b
          simp [agg, optionalJoin]
      | cons bx bs =>
          subst a
          subst b
          change optionalJoin
            (fun (s t : Nat × Nat × Nat) =>
              (min s.1 t.1, max s.2.1 t.2.1, max (max s.2.2 t.2.2) (s.2.1 - t.1)))
            (agg.reference (ax :: as)) (agg.reference (bx :: bs)) =
            agg.reference ((ax :: as) ++ (bx :: bs))
          rw [reference_of_ne (ax :: as) (by simp),
            reference_of_ne (bx :: bs) (by simp),
            reference_of_ne ((ax :: as) ++ (bx :: bs)) (by simp)]
          simp only [optionalJoin, Option.some.injEq, Prod.mk.injEq]
          rw [lo_append_nonempty (ax :: as) (bx :: bs) (by simp) (by simp)]
          rw [hi_append]
          rw [ddAlt_append (ax :: as) (bx :: bs) (by simp) (by simp)]
          omega

private theorem treeReduce_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

theorem sound_TreeReduce : Drawdown.TreeReduce.Goal := by
  exact treeReduce_of_stitch agg stitch_agg

#guard agg.join (agg.reference [3, 1]) (agg.reference [4, 0, 2]) == agg.reference [3, 1, 4, 0, 2]
#guard rootState agg (.node (.leaf [3, 1, 2]) (.node (.leaf [0, 5]) (.leaf [1]))) ==
  agg.reference (plain (.node (.leaf [3, 1, 2]) (.node (.leaf [0, 5]) (.leaf [1]))))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_TreeReduce
