import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_WeightedOrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

/-!
`ExceptionOverlay.WeightedOrderStatistic.Goal` is **not** declared in
`TinylambOpt.ResearchGoals.StorageExecution.BagKernels`: the only
`WeightedOrderStatistic` goal in that module lives in `MultiplicityRuns`.
The statement proven below is the natural ExceptionOverlay analogue: a
default value `x` on `a` ordinals overridden by `y` on the next `b` ordinals
has weighted order statistics `x` for ranks `< a` and `y` afterwards.
The override list is `[(a, y), (a+1, y), ...]`, so the first patch wins on
each position and the overlay expands to `a` copies of `x` followed by
`b` copies of `y`.
-/

private def WeightedOrderStatistic.Goal : Prop :=
  ∀ x y a b i, x < y → i < a + b →
    quantile bag (a+b, x, (List.range b).map (fun j => (a+j, y))) i =
      some (if i < a then x else y)

private def mk (x y a b : Nat) : Nat × Nat × List (Nat × Nat) :=
  (a+b, x, (List.range b).map (fun j => (a+j, y)))

private theorem shift_range_not_mem (a b : Nat) :
    a+b ∉ (List.range b).map (fun j => a+j) := by
  simp only [List.mem_map, List.mem_range]
  rintro ⟨j, hj, h⟩
  omega

private theorem eraseDups_shift (a b : Nat) :
    ((List.range b).map (fun j => a + j)).eraseDups =
      (List.range b).map (fun j => a + j) := by
  induction b with
  | zero => simp
  | succ b ih =>
      rw [List.range_succ, List.map_append, List.eraseDups_append, ih]
      have hne : a + b ∉ (List.range b).map (fun j => a + j) := shift_range_not_mem a b
      simp [List.removeAll, List.eraseDups_cons]

private theorem filter_shift_lt (a b : Nat) :
    ((List.range b).map (fun j => a + j)).filter (fun i => i < a + b) =
      (List.range b).map (fun j => a + j) := by
  rw [List.filter_eq_self]
  intro v hv
  simp only [List.mem_map, List.mem_range] at hv
  rcases hv with ⟨j, hj, rfl⟩
  simp
  omega

private theorem lookup_shift_ge_none (a y b c : Nat) (hbc : b ≤ c) :
    List.lookup (a+c) ((List.range b).map (fun t => (a+t,y))) = none := by
  induction b generalizing c with
  | zero => simp
  | succ b ih =>
      rw [List.range_succ, List.map_append, List.lookup_append]
      rw [ih c (by omega)]
      have hcb : c ≠ b := by omega
      simp [hcb]

private theorem lookup_shift (a y b j : Nat) (hj : j < b) :
    List.lookup (a+j) ((List.range b).map (fun t => (a+t,y))) = some y := by
  induction b with
  | zero => omega
  | succ b ih =>
      by_cases hjb : j < b
      · rw [List.range_succ, List.map_append, List.lookup_append, ih hjb]
        simp
      · have hnone := lookup_shift_ge_none a y b j (by omega)
        rw [List.range_succ, List.map_append, List.lookup_append]
        rw [hnone]
        simp
        omega

private theorem mass_default (x y a b : Nat) (hxy : x < y) :
    bag.mass (mk x y a b) x = a := by
  have hne : y ≠ x := by omega
  have hcount : List.countP
      (fun i => List.lookup i ((List.range b).map (fun j => (a+j, y))) == some x)
      ((List.range b).map (fun j => a+j)) = 0 := by
    rw [List.countP_eq_zero]
    intro i hi
    simp only [List.mem_map, List.mem_range] at hi
    rcases hi with ⟨j, hj, rfl⟩
    rw [lookup_shift a y b j hj]
    simp [hne]
  simp [bag, mk, Function.comp_def, eraseDups_shift, filter_shift_lt, hcount]

private theorem mass_override (x y a b : Nat) (hxy : x < y) :
    bag.mass (mk x y a b) y = b := by
  have hne : y ≠ x := by omega
  have hforall : ∀ i ∈ ((List.range b).map (fun j => a+j)),
      (List.lookup i ((List.range b).map (fun j => (a+j, y))) == some y) = true := by
    intro i hi
    simp only [List.mem_map, List.mem_range] at hi
    rcases hi with ⟨j, hj, rfl⟩
    rw [lookup_shift a y b j hj]
    simp
  have hcount : List.countP
      (fun i => List.lookup i ((List.range b).map (fun j => (a+j, y))) == some y)
      ((List.range b).map (fun j => a+j)) = b := by
    simpa using (List.countP_eq_length.mpr hforall)
  simp [bag, mk, Function.comp_def, eraseDups_shift, filter_shift_lt, hne, hcount]

private theorem keys_mk (x y a b : Nat) :
    bag.keys (mk x y a b) = x :: (List.range b).map (fun _ => y) := by
  simp [bag, mk, List.map_map, Function.comp_def]

private theorem replicate_append_succ (y b : Nat) :
    List.replicate b y ++ [y] = List.replicate (b+1) y := by
  induction b with
  | zero => simp [List.replicate_succ]
  | succ b ih => simp [List.replicate_succ, ih]

private theorem map_const_range (y b : Nat) :
    (List.range b).map (fun _ => y) = List.replicate b y := by
  induction b with
  | zero => simp
  | succ b ih =>
      rw [List.range_succ, List.map_append, ih]
      simpa using (replicate_append_succ y b)

private theorem eraseDups_replicate_succ (y b : Nat) :
    (y :: List.replicate b y).eraseDups = [y] := by
  simp [List.eraseDups_cons]

private theorem eraseDups_keys_succ (x y b : Nat) (hxy : x < y) :
    (x :: (List.range (b+1)).map (fun _ => y)).eraseDups = [x, y] := by
  rw [map_const_range, List.replicate_succ, List.eraseDups_cons]
  simp [Nat.ne_of_gt hxy, eraseDups_replicate_succ]

private theorem below_x (x y a b : Nat) (hxy : x < y) :
    below bag (mk x y a b) x = a := by
  by_cases hb : b = 0
  · subst b
    simp [below, weighted, bag, mk, List.eraseDups_cons]
  · obtain ⟨b', rfl⟩ := Nat.exists_eq_succ_of_ne_zero hb
    have hkeys := keys_mk x y a (b'+1)
    have hdu := eraseDups_keys_succ x y b' hxy
    have hmx := mass_default x y a (b'+1) hxy
    have hmy := mass_override x y a (b'+1) hxy
    have hnyle : ¬ y ≤ x := by omega
    simp [below, weighted, hkeys, hdu, hmx, hmy, hnyle]

private theorem below_y (x y a b : Nat) (hxy : x < y) :
    below bag (mk x y a b) y = a + b := by
  by_cases hb : b = 0
  · subst b
    have hxle : x ≤ y := Nat.le_of_lt hxy
    simp [below, weighted, bag, mk, List.eraseDups_cons, hxle]
  · obtain ⟨b', rfl⟩ := Nat.exists_eq_succ_of_ne_zero hb
    have hkeys := keys_mk x y a (b'+1)
    have hdu := eraseDups_keys_succ x y b' hxy
    have hmx := mass_default x y a (b'+1) hxy
    have hmy := mass_override x y a (b'+1) hxy
    have hxle : x ≤ y := Nat.le_of_lt hxy
    simp [below, weighted, hkeys, hdu, hmx, hmy, hxle]

private theorem live_zero_b (x y a : Nat) (_hxy : x < y) (ha : 0 < a) :
    live bag (mk x y a 0) = [x] := by
  simp [live, bag, mk, List.eraseDups_cons, ha]

private theorem live_zero_a (x y b : Nat) (hxy : x < y) (hb : 0 < b) :
    live bag (mk x y 0 b) = [y] := by
  obtain ⟨b', rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hb)
  have hkeys := keys_mk x y 0 (b'+1)
  have hdu := eraseDups_keys_succ x y b' hxy
  have hmx := mass_default x y 0 (b'+1) hxy
  have hmy := mass_override x y 0 (b'+1) hxy
  have hbpp : 0 < b'+1 := by omega
  simp [live, hkeys, hdu, hmx, hmy, hbpp]

private theorem live_pos (x y a b : Nat) (hxy : x < y) (ha : 0 < a) (hb : 0 < b) :
    live bag (mk x y a b) = [x, y] := by
  obtain ⟨b', rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hb)
  have hkeys := keys_mk x y a (b'+1)
  have hdu := eraseDups_keys_succ x y b' hxy
  have hmx := mass_default x y a (b'+1) hxy
  have hmy := mass_override x y a (b'+1) hxy
  have hbpp : 0 < b'+1 := by omega
  simp [live, hkeys, hdu, hmx, hmy, ha, hbpp]

private theorem mergeSort_pair_of_lt (x y : Nat) (hxy : x < y) :
    [x, y].mergeSort = [x, y] := by
  have hnot : ¬ y < x := by omega
  simp [hnot]

theorem sound_WeightedOrderStatistic : WeightedOrderStatistic.Goal := by
  intro x y a b i hxy hi
  by_cases ha : a = 0
  · subst a
    have hbpos : 0 < b := by omega
    have hlt : i < b := by omega
    have hlive : live bag (0+b, x, (List.range b).map (fun j => (0+j, y))) = [y] := by
      simpa [mk] using (live_zero_a x y b hxy hbpos)
    have hbelow : below bag (b, x, (List.range b).map (fun j => (j, y))) y = b := by
      simpa [mk] using (below_y x y 0 b hxy)
    rw [quantile, hlive]
    simp [hbelow, hlt]
  · have hapos : 0 < a := Nat.pos_of_ne_zero ha
    by_cases hb : b = 0
    · subst b
      have hlt : i < a := by omega
      have hlive : live bag (a+0, x, (List.range 0).map (fun j => (a+j, y))) = [x] := by
        simpa [mk] using (live_zero_b x y a hxy hapos)
      have hbelow : below bag (a, x, ([] : List (Nat × Nat))) x = a := by
        simpa [mk] using (below_x x y a 0 hxy)
      rw [quantile, hlive]
      simp [hbelow, hlt]
    · have hbpos : 0 < b := Nat.pos_of_ne_zero hb
      have hlive : live bag (a+b, x, (List.range b).map (fun j => (a+j, y))) = [x, y] := by
        simpa [mk] using (live_pos x y a b hxy hapos hbpos)
      have hbelowx : below bag (a+b, x, (List.range b).map (fun j => (a+j, y))) x = a := by
        simpa [mk] using (below_x x y a b hxy)
      have hbelowy : below bag (a+b, x, (List.range b).map (fun j => (a+j, y))) y = a+b := by
        simpa [mk] using (below_y x y a b hxy)
      have hsort : [x, y].mergeSort = [x, y] := mergeSort_pair_of_lt x y hxy
      rw [quantile, hlive, hsort]
      by_cases hia : i < a
      · simp [hia, hbelowx]
      · simp [hia, hi, hbelowx, hbelowy]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_WeightedOrderStatistic
