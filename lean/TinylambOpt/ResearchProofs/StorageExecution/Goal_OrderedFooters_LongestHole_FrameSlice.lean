import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `LongestHole.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

/-! FrameSlice for `LongestHole`: every join in the requested window can be
replaced by a reference on the corresponding concatenation, because `agg`
stitches.  The final list equality is just associativity of append. -/

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

section LongestHoleFold

private abbrev zerosHead (xs : List Nat) := (xs.takeWhile (fun x => x == 0)).length
private abbrev zerosTail (xs : List Nat) := (xs.reverse.takeWhile (fun x => x == 0)).length
private abbrev run (xs : List Nat) (i : Nat) := ((xs.drop i).takeWhile (fun x => x == 0)).length
private def runs (xs : List Nat) : List Nat :=
  (List.range (xs.length + 1)).map (fun i => run xs i)

private theorem longestZero_eq_peak_runs (xs : List Nat) :
    longestZero xs = peak (runs xs) := rfl

private theorem length_takeWhile_le (p : Nat → Bool) (xs : List Nat) :
    (xs.takeWhile p).length ≤ xs.length :=
  List.Sublist.length_le (List.takeWhile_sublist p)

private theorem mem_takeWhile_imp {p : Nat → Bool} {xs : List Nat} {x : Nat}
    (hx : x ∈ xs.takeWhile p) : p x = true := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [List.takeWhile_cons] at hx
      by_cases hy : p y = true
      · simp only [hy, ite_true, List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact hy
        · exact ih hx
      · simp [hy] at hx

private theorem takeWhile_all_true {p : Nat → Bool} (xs : List Nat)
    (h : ∀ x ∈ xs, p x = true) : xs.takeWhile p = xs := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_self_of_len {p : Nat → Bool} {xs : List Nat}
    (h : (xs.takeWhile p).length = xs.length) : xs.takeWhile p = xs :=
  List.Sublist.eq_of_length (List.takeWhile_sublist p) h

private theorem allZero_of_len {p : Nat → Bool} {xs : List Nat}
    (h : (xs.takeWhile p).length = xs.length) : ∀ x ∈ xs, p x = true := by
  intro x hx
  rw [← takeWhile_self_of_len h] at hx
  exact mem_takeWhile_imp hx

private theorem acc_le_foldl_max (xs : List Nat) (a : Nat) : a ≤ xs.foldl max a := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      exact Nat.le_trans (Nat.le_max_left a x) (ih (max a x))

private theorem mem_le_foldl_max {xs : List Nat} {a x : Nat} (hx : x ∈ xs) :
    x ≤ xs.foldl max a := by
  induction xs generalizing a with
  | nil => simp at hx
  | cons y ys ih =>
      rcases List.mem_cons.mp hx with hxy | hx
      · subst x
        exact Nat.le_trans (Nat.le_max_right a y) (acc_le_foldl_max ys (max a y))
      · exact ih hx

private theorem all_le_foldl_max (xs : List Nat) (a n : Nat)
    (ha : a ≤ n) (h : ∀ x ∈ xs, x ≤ n) : xs.foldl max a ≤ n := by
  induction xs generalizing a with
  | nil => exact ha
  | cons x xs ih =>
      exact ih (max a x) (Nat.max_le.mpr ⟨ha, h x (by simp)⟩)
        (fun y hy => h y (by simp [hy]))

private theorem peak_le {xs : List Nat} {n : Nat} (h : ∀ x ∈ xs, x ≤ n) : peak xs ≤ n := by
  cases xs with
  | nil => simp [peak]
  | cons x xs =>
      simpa [peak, List.max?] using
        (all_le_foldl_max xs x n (h x (by simp)) (fun y hy => h y (by simp [hy])))

private theorem le_peak_of_mem {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  cases xs with
  | nil => simp at hx
  | cons y ys =>
      rcases List.mem_cons.mp hx with hxy | hx
      · subst x
        simpa [peak, List.max?] using (acc_le_foldl_max ys y)
      · simpa [peak, List.max?] using (mem_le_foldl_max (xs := ys) (a := y) hx)

/-! If a suffix of `a` is entirely zero, its length is at most the number of
zeros at the end of `a`. -/
private theorem tailZeros_ge {a : List Nat} {i : Nat} (_hi : i ≤ a.length)
    (hall : ∀ x ∈ a.drop i, (x == 0) = true) :
    a.length - i ≤ zerosTail a := by
  let k := a.length - i
  have hrev : (a.drop i).reverse = a.reverse.take k := by
    simpa [k] using (List.reverse_drop (l := a) (i := i))
  have hallrev : ∀ x ∈ (a.drop i).reverse, (x == 0) = true := by
    intro x hx
    exact hall x (List.mem_reverse.mp hx)
  have halltk : ∀ x ∈ a.reverse.take k, (x == 0) = true := by
    intro x hx
    rw [← hrev] at hx
    exact hallrev x hx
  have hself : (a.reverse.take k).takeWhile (fun x => x == 0) = a.reverse.take k :=
    takeWhile_all_true _ halltk
  have htake : List.take k (a.reverse.takeWhile (fun x => x == 0)) = a.reverse.take k := by
    rw [List.take_takeWhile, hself]
  have hlen := congrArg List.length htake
  have hklen : k ≤ a.reverse.length := by
    simp [List.length_reverse, k]
  have hleft : (List.take k (a.reverse.takeWhile (fun x => x == 0))).length =
      min k (zerosTail a) := by
    simp [List.length_take]
  have hright : (a.reverse.take k).length = k := by
    rw [List.length_take, Nat.min_eq_left hklen]
  rw [hleft, hright] at hlen
  change min k ((a.reverse.takeWhile (fun x => x == 0)).length) = k at hlen
  change a.length - i ≤ (a.reverse.takeWhile (fun x => x == 0)).length
  omega

/-! Conversely, the last `zerosTail a` elements of `a` are all zero. -/
private theorem tailZeros_all (a : List Nat) :
    ∀ x ∈ a.drop (a.length - zerosTail a), (x == 0) = true := by
  let s := zerosTail a
  have hs : s = (a.reverse.takeWhile (fun x => x == 0)).length := rfl
  have hs_le_len : s ≤ a.length := by
    rw [hs]
    simpa [List.length_reverse] using
      (length_takeWhile_le (fun x => x == 0) a.reverse)
  have hrev : (a.drop (a.length - s)).reverse = a.reverse.take s := by
    rw [List.reverse_drop]
    congr 1
    omega
  have h1 : List.take s (a.reverse.takeWhile (fun x => x == 0)) =
      a.reverse.takeWhile (fun x => x == 0) :=
    List.take_of_length_le (by omega)
  have ht := List.take_takeWhile (p := fun x => x == 0) (i := s) (l := a.reverse)
  have heqTW : (a.reverse.take s).takeWhile (fun x => x == 0) =
      a.reverse.takeWhile (fun x => x == 0) := ht.symm.trans h1
  have hself : (a.reverse.take s).takeWhile (fun x => x == 0) = a.reverse.take s := by
    apply List.Sublist.eq_of_length (List.takeWhile_sublist (p := fun x => x == 0))
    rw [heqTW]
    have hs_le_rev : s ≤ a.reverse.length := by
      simpa [List.length_reverse] using hs_le_len
    have hrt : (a.reverse.take s).length = s := by
      rw [List.length_take, Nat.min_eq_left hs_le_rev]
    rw [hrt, ← hs]
  intro x hx
  have hxrev : x ∈ (a.drop (a.length - s)).reverse := List.mem_reverse.mpr hx
  rw [hrev] at hxrev
  rw [← hself] at hxrev
  exact mem_takeWhile_imp (p := fun y => y == 0) hxrev

/-! The longest zero run across a concatenation either stays in one side or
crosses the boundary, in which case its length is the tail run of the left
list plus the head run of the right list. -/
private theorem longestZero_append (a b : List Nat) :
    longestZero (a ++ b) =
      max (max (longestZero a) (longestZero b)) (zerosTail a + zerosHead b) := by
  apply Nat.le_antisymm
  · change peak (runs (a ++ b)) ≤
      max (max (longestZero a) (longestZero b)) (zerosTail a + zerosHead b)
    apply peak_le
    intro x hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hi_lt : i < (a ++ b).length + 1 := by
      simpa [List.mem_range] using hi
    have hi_le : i ≤ (a ++ b).length := by omega
    by_cases hia : i ≤ a.length
    · have hdrop : (a ++ b).drop i = a.drop i ++ b :=
        List.drop_append_of_le_length hia
      change (((a ++ b).drop i).takeWhile (fun x => x == 0)).length ≤
        max (max (longestZero a) (longestZero b)) (zerosTail a + zerosHead b)
      rw [hdrop, List.takeWhile_append]
      by_cases hcond : ((a.drop i).takeWhile (fun x => x == 0)).length = (a.drop i).length
      · rw [ite_eq_left hcond]
        have hall := allZero_of_len (p := fun x => x == 0) hcond
        have hle : (a.drop i).length ≤ zerosTail a := by
          simpa [List.length_drop] using tailZeros_ge hia hall
        have hle1 : (a.drop i).length + zerosHead b ≤ zerosTail a + zerosHead b := by omega
        simpa [List.length_append] using
          Nat.le_trans hle1 (Nat.le_max_right (max (longestZero a) (longestZero b)) (zerosTail a + zerosHead b))
      · rw [ite_eq_right hcond]
        have hmem : run a i ∈ runs a :=
          List.mem_map.mpr ⟨i, by simp [List.mem_range]; omega, rfl⟩
        have hle : run a i ≤ longestZero a := by
          rw [longestZero_eq_peak_runs]
          exact le_peak_of_mem hmem
        simpa [run] using Nat.le_trans hle (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
    · have hia' : a.length < i := by omega
      have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
        rw [List.drop_append]
        have hd : a.drop i = [] := List.drop_eq_nil_of_le (by omega)
        simp [hd]
      change (((a ++ b).drop i).takeWhile (fun x => x == 0)).length ≤
        max (max (longestZero a) (longestZero b)) (zerosTail a + zerosHead b)
      rw [hdrop]
      have hj : i - a.length < b.length + 1 := by
        simp [List.length_append] at hi
        omega
      have hmem : run b (i - a.length) ∈ runs b :=
        List.mem_map.mpr ⟨i - a.length, by simp [List.mem_range]; omega, rfl⟩
      have hle : run b (i - a.length) ≤ longestZero b := by
        rw [longestZero_eq_peak_runs]
        exact le_peak_of_mem hmem
      simpa [run] using Nat.le_trans hle (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · change peak (runs a) ≤ longestZero (a ++ b)
        apply peak_le
        intro x hx
        rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
        have hi_le : i ≤ a.length := by
          simp [List.mem_range] at hi
          omega
        have hdrop : (a ++ b).drop i = a.drop i ++ b :=
          List.drop_append_of_le_length hi_le
        have hmem : run (a ++ b) i ∈ runs (a ++ b) := by
          refine List.mem_map.mpr ⟨i, ?_, rfl⟩
          simp [List.mem_range, List.length_append]
          omega
        have hy_le : run (a ++ b) i ≤ longestZero (a ++ b) := by
          rw [longestZero_eq_peak_runs]
          exact le_peak_of_mem hmem
        have hrun_le : run a i ≤ run (a ++ b) i := by
          change ((a.drop i).takeWhile (fun x => x == 0)).length ≤
            (((a ++ b).drop i).takeWhile (fun x => x == 0)).length
          rw [hdrop, List.takeWhile_append]
          by_cases hcond : ((a.drop i).takeWhile (fun x => x == 0)).length = (a.drop i).length
          · rw [ite_eq_left hcond]
            have hself : (a.drop i).takeWhile (fun x => x == 0) = a.drop i :=
              takeWhile_self_of_len hcond
            rw [hself]
            simp [List.length_append]
          · rw [ite_eq_right hcond]
            exact Nat.le_refl _
        exact Nat.le_trans hrun_le hy_le
      · change peak (runs b) ≤ longestZero (a ++ b)
        apply peak_le
        intro x hx
        rcases List.mem_map.mp hx with ⟨j, hj, rfl⟩
        have hj_le : j ≤ b.length := by
          simp [List.mem_range] at hj
          omega
        have hdrop : (a ++ b).drop (a.length + j) = b.drop j := by
          simp [List.drop_append]
        have hmem : run (a ++ b) (a.length + j) ∈ runs (a ++ b) := by
          refine List.mem_map.mpr ⟨a.length + j, ?_, rfl⟩
          simp [List.mem_range, List.length_append]
          omega
        have hy_le : run (a ++ b) (a.length + j) ≤ longestZero (a ++ b) := by
          rw [longestZero_eq_peak_runs]
          exact le_peak_of_mem hmem
        simpa [run, hdrop] using hy_le
    · let i := a.length - zerosTail a
      have hdrop : (a ++ b).drop i = a.drop i ++ b := by
        rw [List.drop_append]
        simp [Nat.sub_eq_zero_of_le (by omega : i ≤ a.length)]
      have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
        simpa [i] using tailZeros_all a
      have htake : ((a.drop i ++ b).takeWhile (fun x => x == 0)).length =
          (a.drop i).length + zerosHead b := by
        rw [List.takeWhile_append]
        have hself : (a.drop i).takeWhile (fun x => x == 0) = a.drop i :=
          takeWhile_all_true _ hall
        simp [hself, List.length_append]
      have hmem : run (a ++ b) i ∈ runs (a ++ b) := by
        refine List.mem_map.mpr ⟨i, ?_, rfl⟩
        simp [List.mem_range, List.length_append]
        omega
      have hy_le : run (a ++ b) i ≤ longestZero (a ++ b) := by
        rw [longestZero_eq_peak_runs]
        exact le_peak_of_mem hmem
      simp only [run, hdrop] at hy_le
      rw [htake] at hy_le
      have hlen_drop : (a.drop i).length = zerosTail a := by
        have hsle : zerosTail a ≤ a.length := by
          simpa [List.length_reverse] using
            (length_takeWhile_le (fun x => x == 0) a.reverse)
        simp [List.length_drop, i]
        omega
      rw [hlen_drop] at hy_le
      exact hy_le

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg, List.length_append]
  refine Prod.ext rfl (Prod.ext ?_ (Prod.ext ?_ ?_))
  · by_cases hbool : (a.takeWhile (fun x => x == 0)).length == a.length
    · have ha : (a.takeWhile (fun x => x == 0)).length = a.length := by simpa using hbool
      have hself : a.takeWhile (fun x => x == 0) = a := takeWhile_self_of_len ha
      have htake : (a ++ b).takeWhile (fun x => x == 0) =
          a ++ b.takeWhile (fun x => x == 0) := by
        rw [List.takeWhile_append]
        simp [hself]
      simp [hbool, htake, List.length_append]
    · have ha : (a.takeWhile (fun x => x == 0)).length ≠ a.length := by
        intro h
        simp [h] at hbool
      have htake : (a ++ b).takeWhile (fun x => x == 0) =
          a.takeWhile (fun x => x == 0) := by
        rw [List.takeWhile_append]
        simp [ha]
      simp [hbool, htake]
  · change (if zerosTail b == b.length then b.length + zerosTail a else zerosTail b) =
      zerosTail (a ++ b)
    by_cases hbool : zerosTail b == b.length
    · have hb : zerosTail b = b.length := by simpa using hbool
      have hbrev : zerosTail b = b.reverse.length := by
        simpa [List.length_reverse] using hb
      have hself : b.reverse.takeWhile (fun x => x == 0) = b.reverse :=
        takeWhile_self_of_len hbrev
      have hrev : (a ++ b).reverse = b.reverse ++ a.reverse := by simp
      have htake : (a ++ b).reverse.takeWhile (fun x => x == 0) =
          b.reverse ++ a.reverse.takeWhile (fun x => x == 0) := by
        rw [hrev, List.takeWhile_append, ite_eq_left hbrev]
      have hlen : zerosTail (a ++ b) = b.length + zerosTail a := by
        have h := congrArg List.length htake
        simpa [zerosTail, zerosHead, List.length_append, List.length_reverse] using h
      simp [hbool, hlen]
    · have hb : zerosTail b ≠ b.length := by
        intro h
        simp [h] at hbool
      have hbrev : zerosTail b ≠ b.reverse.length := by
        intro h
        apply hb
        simpa [List.length_reverse] using h
      have hrev : (a ++ b).reverse = b.reverse ++ a.reverse := by simp
      have htake : (a ++ b).reverse.takeWhile (fun x => x == 0) =
          b.reverse.takeWhile (fun x => x == 0) := by
        rw [hrev, List.takeWhile_append, ite_eq_right hbrev]
      have hlen : zerosTail (a ++ b) = zerosTail b := by
        simpa [zerosTail] using congrArg List.length htake
      simp [hbool, hlen]
  · exact (longestZero_append a b).symm

end LongestHoleFold

theorem sound_FrameSlice : LongestHole.FrameSlice.Goal := by
  exact frameSlice_of_stitch _ stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_FrameSlice
