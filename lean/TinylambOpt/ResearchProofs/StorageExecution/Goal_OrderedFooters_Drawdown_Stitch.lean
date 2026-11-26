import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `Drawdown.Stitch` — Compute a page boundary correction using only the two serialized
  states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_Stitch

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

private def lo (xs : List Nat) : Nat := xs.min?.getD 0
private def hi (xs : List Nat) : Nat := peak xs

private def ddRaw (xs : List Nat) : List Nat :=
  xs.zipIdx.map (fun (x, i) => x - ((xs.drop i).min?.getD x))

private def dd (xs : List Nat) : Nat := peak (ddRaw xs)

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x :: xs) = max x (peak xs) := by
  unfold peak
  rw [List.max?_cons]
  cases h : xs.max? with
  | none => simp
  | some m => simp

private theorem hi_cons (x : Nat) (xs : List Nat) : hi (x :: xs) = max x (hi xs) := by
  exact peak_cons x xs

private theorem lo_singleton (x : Nat) : lo [x] = x := by
  simp [lo, List.min?_cons]

private theorem hi_singleton (x : Nat) : hi [x] = x := by
  simp [hi, peak, List.max?_cons]

private theorem lo_cons_ne (x : Nat) (xs : List Nat) (hxs : xs ≠ []) :
    lo (x :: xs) = min x (lo xs) := by
  cases h : xs.min? with
  | none =>
      have : xs = [] := by cases xs <;> simp_all
      contradiction
  | some m => simp [lo, List.min?_cons, h]

private theorem lo_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    lo (a ++ b) = min (lo a) (lo b) := by
  induction a generalizing b with
  | nil => contradiction
  | cons x xs ih =>
      by_cases hxs : xs = []
      · subst xs
        rw [List.singleton_append, lo_singleton, lo_cons_ne x b hb]
      · rw [List.cons_append, lo_cons_ne x (xs ++ b) (by simp [hxs])]
        rw [ih b hxs hb, lo_cons_ne x xs hxs]
        exact (Nat.min_assoc x (lo xs) (lo b)).symm

private theorem hi_append (a b : List Nat) : hi (a ++ b) = max (hi a) (hi b) := by
  induction a with
  | nil => simp [hi, peak]
  | cons x xs ih =>
      rw [List.cons_append, hi_cons, ih, hi_cons]
      exact (Nat.max_assoc x (hi xs) (hi b)).symm

private theorem drop_succ_cons (x : Nat) (xs : List Nat) (i : Nat) :
    (x :: xs).drop (i + 1) = xs.drop i := by
  simp

private theorem zipIdx_succ (xs : List Nat) (k : Nat) :
    xs.zipIdx (k + 1) = (xs.zipIdx k).map (fun p => (p.1, p.2 + 1)) := by
  induction xs generalizing k with
  | nil => simp [List.zipIdx]
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, List.map_cons]
      congr 1
      exact ih (k + 1)

private theorem ddRaw_cons (x : Nat) (xs : List Nat) :
    ddRaw (x :: xs) = (x - lo (x :: xs)) :: ddRaw xs := by
  unfold ddRaw
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.drop_zero]
  have hhead : x - (x :: xs).min?.getD x = x - lo (x :: xs) := by
    simp [lo, List.min?_cons]
  rw [hhead]
  rw [zipIdx_succ]
  simp only [List.map_map]
  simp

private theorem sub_min' (x a b : Nat) : x - min a b = max (x - a) (x - b) := by
  by_cases hab : a ≤ b
  · rw [Nat.min_eq_left hab, Nat.max_eq_left (by omega)]
  · have hba : b ≤ a := by omega
    rw [Nat.min_eq_right hba, Nat.max_eq_right (by omega)]

private theorem max_sub' (x y m : Nat) : max x y - m = max (x - m) (y - m) := by
  by_cases hxy : x ≤ y
  · rw [Nat.max_eq_right hxy, Nat.max_eq_right (by omega)]
  · have hyx : y ≤ x := by omega
    rw [Nat.max_eq_left hyx, Nat.max_eq_left (by omega)]

private theorem dd_cons_ne (x : Nat) (xs : List Nat) (hxs : xs ≠ []) :
    dd (x :: xs) = max (dd xs) (x - lo xs) := by
  rw [show dd (x :: xs) = peak ((x - lo (x :: xs)) :: ddRaw xs) by
    simp [dd, ddRaw_cons]]
  rw [peak_cons]
  have hlo : x - lo (x :: xs) = x - lo xs := by
    cases h : xs.min? with
    | none =>
        have : xs = [] := by cases xs <;> simp_all
        contradiction
    | some m =>
        simp [lo, List.min?_cons, h]
        by_cases hmx : m ≤ x
        · rw [Nat.min_eq_right hmx]
        · have hxm : x ≤ m := by omega
          rw [Nat.min_eq_left hxm, Nat.sub_self, Nat.sub_eq_zero_of_le hxm]
  rw [hlo]
  exact Nat.max_comm _ _

private theorem dd_singleton (x : Nat) : dd [x] = 0 := by
  simp [dd, ddRaw, List.zipIdx_cons, peak]

private theorem dd_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    dd (a ++ b) = max (max (dd a) (dd b)) (hi a - lo b) := by
  induction a generalizing b with
  | nil => contradiction
  | cons x xs ih =>
      by_cases hxs : xs = []
      · subst xs
        rw [List.singleton_append, dd_cons_ne x b hb, dd_singleton, hi_singleton]
        simp
      · rw [List.cons_append, dd_cons_ne x (xs ++ b) (by simp [hxs])]
        rw [ih b hxs hb, lo_append xs b hxs hb]
        rw [dd_cons_ne x xs hxs, hi_cons]
        rw [sub_min', max_sub']
        ac_rfl

private theorem ref_eq (xs : List Nat) :
    agg.reference xs = xs.head?.map (fun _ => (lo xs, hi xs, dd xs)) := by
  simp [agg, lo, hi, dd, ddRaw]

theorem sound_Stitch : Drawdown.Stitch.Goal := by
  intro a b
  rw [ref_eq a, ref_eq b, ref_eq (a ++ b)]
  simp only [agg]
  cases ha : a.head? with
  | none =>
      have ha0 : a = [] := by cases a <;> simp_all
      subst a
      simp [optionalJoin]
  | some _ =>
      cases hb : b.head? with
      | none =>
          have hb0 : b = [] := by cases b <;> simp_all
          subst b
          simp [ha, optionalJoin]
      | some _ =>
          have hane : a ≠ [] := by cases a <;> simp_all
          have hbne : b ≠ [] := by cases b <;> simp_all
          have hh : (a ++ b).head? = a.head? := by cases a <;> simp_all
          simp only [ha, hh, Option.map_some, optionalJoin]
          rw [lo_append a b hane hbne, hi_append a b, dd_append a b hane hbne]

#eval (Drawdown.agg.join (Drawdown.agg.reference [3, 1, 4]) (Drawdown.agg.reference [1, 5, 2]) ==
      Drawdown.agg.reference ([3, 1, 4] ++ [1, 5, 2]))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_Stitch
