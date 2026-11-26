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

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_WeightedOrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

private theorem eraseDups_pair_of_lt (x y : Nat) (hxy : x < y) :
    [x, y].eraseDups = [x, y] := by
  simp [List.eraseDups_cons, Nat.ne_of_gt hxy]

private theorem keys_pair (x y a b : Nat) : bag.keys [(x, a), (y, b)] = [x, y] := by
  rfl

private theorem mass_pair_left (x y a b : Nat) (hxy : x < y) :
    bag.mass [(x, a), (y, b)] x = a := by
  simp [bag, Nat.ne_of_gt hxy]

private theorem mass_pair_right (x y a b : Nat) (hxy : x < y) :
    bag.mass [(x, a), (y, b)] y = b := by
  simp [bag, Nat.ne_of_lt hxy]

private theorem mergeSort_pair_of_lt (x y : Nat) (hxy : x < y) :
    [x, y].mergeSort (fun u v => decide (u ≤ v)) = [x, y] := by
  have hnot : ¬ y < x := by omega
  simp [hnot]

theorem sound_WeightedOrderStatistic : WeightedOrderStatistic.Goal := by
  intro x y a b i hxy hi
  have hxle : x ≤ y := Nat.le_of_lt hxy
  have hnyle : ¬ y ≤ x := by omega
  have hkeys : bag.keys [(x, a), (y, b)] = [x, y] := keys_pair x y a b
  have hdx : [x, y].eraseDups = [x, y] := eraseDups_pair_of_lt x y hxy
  have hmx : bag.mass [(x, a), (y, b)] x = a := mass_pair_left x y a b hxy
  have hmy : bag.mass [(x, a), (y, b)] y = b := mass_pair_right x y a b hxy
  have hsort : [x, y].mergeSort (fun u v => decide (u ≤ v)) = [x, y] :=
    mergeSort_pair_of_lt x y hxy
  by_cases ha : a = 0
  · subst a
    have hbpos : 0 < b := by omega
    have hlt : i < b := by omega
    have hlive : live bag [(x, 0), (y, b)] = [y] := by
      simp [live, hkeys, hdx, hmx, hmy, hbpos]
    have hbelow : below bag [(x, 0), (y, b)] y = b := by
      simp [below, weighted, hkeys, hdx, hmx, hmy, hxle]
    simp [quantile, hlive, hbelow, hlt]
  · have hapos : 0 < a := Nat.pos_of_ne_zero ha
    by_cases hb : b = 0
    · subst b
      have hlt : i < a := by omega
      have hlive : live bag [(x, a), (y, 0)] = [x] := by
        simp [live, hkeys, hdx, hmx, hmy, hapos]
      have hbelow : below bag [(x, a), (y, 0)] x = a := by
        simp [below, weighted, hkeys, hdx, hmx, hmy, hnyle]
      simp [quantile, hlive, hbelow, hlt]
    · have hbpos : 0 < b := Nat.pos_of_ne_zero hb
      have hlive : live bag [(x, a), (y, b)] = [x, y] := by
        simp [live, hkeys, hdx, hmx, hmy, hapos, hbpos]
      have hbelowx : below bag [(x, a), (y, b)] x = a := by
        simp [below, weighted, hkeys, hdx, hmx, hmy, hnyle]
      have hbelowy : below bag [(x, a), (y, b)] y = a + b := by
        simp [below, weighted, hkeys, hdx, hmx, hmy, hxle]
      rw [quantile, hlive, hsort]
      by_cases hia : i < a
      · simp [hia, hbelowx]
      · simp [hia, hi, hbelowx, hbelowy]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_WeightedOrderStatistic
