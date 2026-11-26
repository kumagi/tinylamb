import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# WeekPairMultiplicityProduct

The number of matching date combinations for a week pair is the product of its two date
multiplicities.

The executor can carry a weight for the week pair and delay expanding identical output
rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.WeekPairMultiplicityProduct
theorem inner_count (nextYear : List Nat) (k w : Nat) :
    ((nextYear.filter (fun x => x == k + 52)).map (fun _ => k)).count w =
      if k == w then nextYear.count (k+52) else 0 := by
  rw [List.map_const']
  rw [List.count_replicate]
  by_cases h : k == w
  · have hkw : k = w := beq_iff_eq.mp h
    simp [h]
    rw [← List.count_eq_length_filter]
  · simp [h]

theorem block_count (firstYear nextYear : List Nat) (k w : Nat) :
    ((firstYear.filter (fun x => x == k)).flatMap (fun _ =>
      (nextYear.filter (fun x => x == k + 52)).map (fun _ => k))).count w =
      firstYear.count k * (if k == w then nextYear.count (k+52) else 0) := by
  induction firstYear with
  | nil => simp
  | cons a as ih =>
    by_cases ha : a == k
    · simp [ha, ih, inner_count, List.count_cons]
      rw [Nat.succ_mul]
      ac_rfl
    · simp [ha, ih, List.count_cons]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.WeekPairMultiplicityProduct.Goal := by
  intro weeks firstYear nextYear w
  dsimp
  induction weeks with
  | nil => simp
  | cons k ks ih =>
    simp only [List.flatMap_cons, List.count_append]
    rw [ih, block_count]
    by_cases hkw : k == w
    · have hkwEq : k = w := beq_iff_eq.mp hkw
      subst k
      simp [hkw, List.count_cons, Nat.succ_mul, Nat.add_mul]
      ac_rfl
    · simp [hkw, List.count_cons]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.WeekPairMultiplicityProduct
