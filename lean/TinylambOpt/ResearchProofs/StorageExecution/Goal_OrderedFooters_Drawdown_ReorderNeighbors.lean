import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `Drawdown.ReorderNeighbors` — Two neighboring states may be scheduled in reverse if
  they commute on their reachable values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

private def mn (xs : List Nat) : Nat := xs.min?.getD 0

private def dd (xs : List Nat) : Nat :=
  peak (xs.zipIdx.map (fun (x,i) => x - ((xs.drop i).min?.getD x)))

private theorem zipIdx_add (xs : List Nat) (k : Nat) :
    xs.zipIdx k = xs.zipIdx.map (fun p => (p.1, p.2 + k)) := by
  induction xs generalizing k with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons]
      rw [ih (k+1)]
      simp only [List.map_cons, List.cons.injEq, Nat.zero_add]
      constructor
      · simp
      · rw [ih 1]
        simp only [List.map_map]
        apply List.map_congr_left
        intro p hp
        simp
        omega

private theorem opt_max_elim (o : Option Nat) (a : Nat) :
    o.elim a (max a) = max a (o.getD 0) := by
  cases o <;> simp

private theorem dd_cons (x : Nat) (xs : List Nat) : dd (x::xs) = max (x - mn (x::xs)) (dd xs) := by
  unfold dd mn
  rw [List.zipIdx_cons]
  simp only [List.map_cons]
  rw [zipIdx_add]
  simp only [List.map_map]
  simp [peak, Function.comp_def, opt_max_elim]

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x::xs) = max x (peak xs) := by
  cases xs <;> simp [peak, List.max?_cons]

private theorem mn_cons_ne (x : Nat) {xs : List Nat} (h : xs ≠ []) :
    mn (x::xs) = min x (mn xs) := by
  cases xs with
  | nil => contradiction
  | cons y ys => simp [mn, List.min?_cons]

private theorem mn_cons_fold (x : Nat) (xs : List Nat) : mn (x::xs) = xs.foldl min x := by
  simp [mn]
  rw [List.foldl_min]
  cases h : xs.min? <;> simp

private theorem mn_cons_append (x : Nat) (xs ys : List Nat) (hy : ys ≠ []) :
    mn (x::(xs++ys)) = min (mn (x::xs)) (mn ys) := by
  rw [mn_cons_fold, List.foldl_append, mn_cons_fold, List.foldl_min]
  cases hys : ys.min? with
  | none => simp [List.min?_eq_none_iff] at hys; exact (hy hys).elim
  | some m => simp [mn, hys]

private theorem mn_append_ne {xs ys : List Nat} (hx : xs ≠ []) (hy : ys ≠ []) :
    mn (xs++ys) = min (mn xs) (mn ys) := by
  cases xs with
  | nil => contradiction
  | cons x xs => exact mn_cons_append x xs ys hy

private theorem peak_append (xs ys : List Nat) : peak (xs++ys) = max (peak xs) (peak ys) := by
  induction xs with
  | nil => simp [peak]
  | cons x xs ih =>
      simp only [List.cons_append]
      rw [peak_cons, ih, peak_cons]
      omega

private theorem sub_min_max (a b c : Nat) : a - min b c = max (a-b) (a-c) := by
  omega

private theorem dd_append (xs ys : List Nat) (hy : ys ≠ []) :
    dd (xs++ys) = max (max (dd xs) (dd ys)) (peak xs - mn ys) := by
  induction xs with
  | nil => simp [dd, peak, mn]
  | cons x xs ih =>
      simp only [List.cons_append]
      rw [dd_cons]
      rw [mn_cons_append x xs ys hy]
      rw [ih]
      rw [dd_cons]
      rw [peak_cons]
      rw [sub_min_max]
      omega

private theorem ref_cons (x : Nat) (xs : List Nat) :
    agg.reference (x::xs) = some (mn (x::xs), peak (x::xs), dd (x::xs)) := by
  simp [agg, mn, dd, peak]

private theorem ref_append (xs ys : List Nat) :
    agg.reference (xs ++ ys) = agg.join (agg.reference xs) (agg.reference ys) := by
  induction xs with
  | nil => simp [agg, optionalJoin]
  | cons x xs ih =>
      cases ys with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          simp only [List.cons_append]
          rw [ref_cons, ref_cons, ref_cons]
          simp [agg, optionalJoin, mn_cons_append, peak_cons, peak_append,
            dd_cons, dd_append, sub_min_max]
          omega

private theorem ref_left_ctx (z x y : List Nat) (h : agg.reference x = agg.reference y) :
    agg.reference (z++x) = agg.reference (z++y) := by
  induction z with
  | nil => simpa
  | cons w z ih =>
      simp only [List.cons_append]
      calc
        agg.reference (w::(z++x)) = agg.join (agg.reference [w]) (agg.reference (z++x)) := by
          simpa using (ref_append [w] (z++x))
        _ = agg.join (agg.reference [w]) (agg.reference (z++y)) := by rw [ih]
        _ = agg.reference (w::(z++y)) := by
          simpa using (ref_append [w] (z++y)).symm

private theorem ref_right_ctx (x y z : List Nat) (h : agg.reference x = agg.reference y) :
    agg.reference (x++z) = agg.reference (y++z) := by
  induction z generalizing x y with
  | nil => simpa
  | cons w z ih =>
      have hxw : agg.reference (x++[w]) = agg.reference (y++[w]) := by
        calc
          agg.reference (x++[w]) = agg.join (agg.reference x) (agg.reference [w]) := ref_append x [w]
          _ = agg.join (agg.reference y) (agg.reference [w]) := by rw [h]
          _ = agg.reference (y++[w]) := (ref_append y [w]).symm
      rw [show x++(w::z) = (x++[w])++z by simp,
          show y++(w::z) = (y++[w])++z by simp]
      exact ih (x++[w]) (y++[w]) hxw

theorem sound_ReorderNeighbors : Drawdown.ReorderNeighbors.Goal := by
  intro pre a b suf h
  have hab : agg.reference (a++b) = agg.reference (b++a) := by
    calc
      agg.reference (a++b) = agg.join (agg.reference a) (agg.reference b) := ref_append a b
      _ = agg.join (agg.reference b) (agg.reference a) := h
      _ = agg.reference (b++a) := (ref_append b a).symm
  have h1 : agg.reference (pre++(a++b)) = agg.reference (pre++(b++a)) :=
    ref_left_ctx pre (a++b) (b++a) hab
  have h2 : agg.reference ((pre++(a++b))++suf) = agg.reference ((pre++(b++a))++suf) :=
    ref_right_ctx (pre++(a++b)) (pre++(b++a)) suf h1
  simpa [List.append_assoc] using h2

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_ReorderNeighbors
