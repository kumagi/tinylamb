import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.WeightedOrderStatistic` — `∀ x y a b i, x<y → i<a+b → quantile bag
  [(x,a),(y,b)] i = some (if i<a then x else y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_WeightedOrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

/-!
`DictionaryAliases.WeightedOrderStatistic.Goal` is **not** declared in
`TinylambOpt.ResearchGoals.StorageExecution.BagKernels`: the only
`WeightedOrderStatistic` goal in that module lives in `MultiplicityRuns`.
The statement proven below is the natural DictionaryAliases analogue: the
dictionary `[x, y]` with `x < y` and ordinals `0` / `1` repeated `a` / `b`
times has weighted order statistics `x` for ranks `< a` and `y` afterwards.
-/
private def WeightedOrderStatistic.Goal : Prop :=
  ∀ x y a b i, x < y → i < a + b →
    quantile bag ([x, y], List.replicate a 0 ++ List.replicate b 1) i =
      some (if i < a then x else y)

-- Sanity checks on small inputs (in both zero-multiplicity edge cases).
example : quantile bag ([10, 20], List.replicate 2 0 ++ List.replicate 3 1) 1 = some 10 := by
  native_decide
example : quantile bag ([10, 20], List.replicate 2 0 ++ List.replicate 3 1) 2 = some 20 := by
  native_decide
example : quantile bag ([10, 20], List.replicate 0 0 ++ List.replicate 3 1) 0 = some 20 := by
  native_decide
example : quantile bag ([10, 20], List.replicate 2 0 ++ List.replicate 0 1) 1 = some 10 := by
  native_decide

private theorem count_replicate_ne (v n w : Nat) (h : v ≠ w) :
    (List.replicate n v).count w = 0 := by
  rw [List.count_replicate]
  simp [beq_iff_eq, h]

private theorem keys_pair (x y a b : Nat) :
    bag.keys ([x, y], List.replicate a 0 ++ List.replicate b 1) = [x, y] := rfl

private theorem eraseDups_pair_of_lt (x y : Nat) (hxy : x < y) :
    [x, y].eraseDups = [x, y] := by
  simp [List.eraseDups_cons, Nat.ne_of_gt hxy]

private theorem mass_pair_left (x y a b : Nat) (hxy : x < y) :
    bag.mass ([x, y], List.replicate a 0 ++ List.replicate b 1) x = a := by
  simp [bag, Nat.ne_of_gt hxy, count_replicate_ne]

private theorem mass_pair_right (x y a b : Nat) (hxy : x < y) :
    bag.mass ([x, y], List.replicate a 0 ++ List.replicate b 1) y = b := by
  simp [bag, Nat.ne_of_lt hxy, count_replicate_ne]

private theorem below_left (x y a b : Nat) (hxy : x < y) :
    below bag ([x, y], List.replicate a 0 ++ List.replicate b 1) x = a := by
  have hkeys : bag.keys ([x, y], List.replicate a 0 ++ List.replicate b 1) = [x, y] := rfl
  have hdx : [x, y].eraseDups = [x, y] := eraseDups_pair_of_lt x y hxy
  have hmx := mass_pair_left x y a b hxy
  have hnyle : ¬ y ≤ x := by omega
  simp [below, weighted, hkeys, hdx, hmx, hnyle]

private theorem below_right (x y a b : Nat) (hxy : x < y) :
    below bag ([x, y], List.replicate a 0 ++ List.replicate b 1) y = a + b := by
  have hkeys : bag.keys ([x, y], List.replicate a 0 ++ List.replicate b 1) = [x, y] := rfl
  have hdx : [x, y].eraseDups = [x, y] := eraseDups_pair_of_lt x y hxy
  have hmx := mass_pair_left x y a b hxy
  have hmy := mass_pair_right x y a b hxy
  have hxle : x ≤ y := Nat.le_of_lt hxy
  simp [below, weighted, hkeys, hdx, hmx, hmy, hxle]

private theorem mergeSort_pair_of_lt (x y : Nat) (hxy : x < y) :
    [x, y].mergeSort (fun u v => decide (u ≤ v)) = [x, y] := by
  have hnot : ¬ y < x := by omega
  simp [hnot]

theorem sound_WeightedOrderStatistic : WeightedOrderStatistic.Goal := by
  intro x y a b i hxy hi
  by_cases ha : a = 0
  · subst a
    have hbpos : 0 < b := by omega
    have hlt : i < b := by omega
    have hkeys : bag.keys ([x, y], List.replicate b 1) = [x, y] := rfl
    have hdx : [x, y].eraseDups = [x, y] := eraseDups_pair_of_lt x y hxy
    have hmx : bag.mass ([x, y], List.replicate b 1) x = 0 := by
      simpa using mass_pair_left x y 0 b hxy
    have hmy : bag.mass ([x, y], List.replicate b 1) y = b := by
      simpa using mass_pair_right x y 0 b hxy
    have hlive : live bag ([x, y], List.replicate b 1) = [y] := by
      simp [live, hkeys, hdx, hmx, hmy, hbpos]
    have hbelow : below bag ([x, y], List.replicate b 1) y = b := by
      simpa using below_right x y 0 b hxy
    simp [quantile, hlive, hbelow, hlt]
  · have hapos : 0 < a := Nat.pos_of_ne_zero ha
    by_cases hb : b = 0
    · subst b
      have hlt : i < a := by omega
      have hkeys : bag.keys ([x, y], List.replicate a 0) = [x, y] := rfl
      have hdx : [x, y].eraseDups = [x, y] := eraseDups_pair_of_lt x y hxy
      have hmx : bag.mass ([x, y], List.replicate a 0) x = a := by
        simpa using mass_pair_left x y a 0 hxy
      have hmy : bag.mass ([x, y], List.replicate a 0) y = 0 := by
        simpa using mass_pair_right x y a 0 hxy
      have hlive : live bag ([x, y], List.replicate a 0) = [x] := by
        simp [live, hkeys, hdx, hmx, hmy, hapos]
      have hbelow : below bag ([x, y], List.replicate a 0) x = a := by
        simpa using below_left x y a 0 hxy
      simp [quantile, hlive, hbelow, hlt]
    · have hbpos : 0 < b := Nat.pos_of_ne_zero hb
      have hkeys : bag.keys ([x, y], List.replicate a 0 ++ List.replicate b 1) = [x, y] := rfl
      have hdx : [x, y].eraseDups = [x, y] := eraseDups_pair_of_lt x y hxy
      have hmx := mass_pair_left x y a b hxy
      have hmy := mass_pair_right x y a b hxy
      have hlive : live bag ([x, y], List.replicate a 0 ++ List.replicate b 1) = [x, y] := by
        simp [live, hkeys, hdx, hmx, hmy, hapos, hbpos]
      have hbelowx : below bag ([x, y], List.replicate a 0 ++ List.replicate b 1) x = a :=
        below_left x y a b hxy
      have hbelowy : below bag ([x, y], List.replicate a 0 ++ List.replicate b 1) y = a + b :=
        below_right x y a b hxy
      have hsort : [x, y].mergeSort (fun u v => decide (u ≤ v)) = [x, y] :=
        mergeSort_pair_of_lt x y hxy
      rw [quantile, hlive, hsort]
      by_cases hia : i < a
      · simp [hia, hbelowx]
      · simp [hia, hi, hbelowx, hbelowy]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_WeightedOrderStatistic
