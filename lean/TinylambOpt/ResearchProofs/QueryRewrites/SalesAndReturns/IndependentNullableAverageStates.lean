import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# IndependentNullableAverageStates

Independent counts of non-NULL inputs preserve the denominators of the three modeled
averages. The executor can merge partial states without rescanning joined rows.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.IndependentNullableAverageStates
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

abbrev Row := Option Int × Option Int × Option Int

theorem filterMap_replicate (f : Row → Option Int) (a : Row) : ∀ n : Nat,
    (List.replicate n a).filterMap f =
      match f a with
      | none => []
      | some v => List.replicate n v := by
  intro n
  induction n with
  | zero => cases f a <;> simp
  | succ n ih => cases h : f a <;> simp [h, ih]

theorem repAux (n : Nat) (f : Row → Option Int) : ∀ rs : List Row,
    (let vs := rs.flatMap (fun a => (List.replicate n a).filterMap f); (vs.sum, vs.length)) =
      (let vs := rs.filterMap f; ((n : Int) * vs.sum, n * vs.length)) := by
  intro rs
  induction rs with
  | nil => simp
  | cons r rs ih =>
      simp only [List.flatMap_cons]
      cases hfr : f r with
      | none =>
          simp [hfr] at ih ⊢
          exact ih
      | some v =>
          simp [hfr] at ih ⊢
          rcases ih with ⟨ih1, ih2⟩
          constructor
          · rw [Int.mul_add, ih1]
          · rw [Nat.mul_add, ih2]
            omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.IndependentNullableAverageStates.Goal := by
  intro rs n
  dsimp only
  apply List.map_congr_left
  intro f hf
  simpa [List.filterMap_flatMap] using repAux n f rs

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.IndependentNullableAverageStates

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.IndependentNullableAverageStates.sound
