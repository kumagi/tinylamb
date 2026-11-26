import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_RevenueBandEarlyPairFilter

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# RevenueBandEarlyPairFilter

Testing the reciprocal two-channel band before probing the third channel is
sound. The pair predicate does not depend on the third value, so filtering the
pair list first and filtering each probe list inside the flatMap produce the
same bag of accepted triples. The executor may skip third-channel probes for
pairs outside the band, including when the band is not transitive.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.RevenueBandEarlyPairFilter.Goal := by
  intro pairs third
  dsimp only
  let band := fun (a b : Nat) =>
    decide (9 * b ≤ 10 * a ∧ 10 * a ≤ 11 * b ∧ 9 * a ≤ 10 * b ∧ 10 * b ≤ 11 * a)
  change (pairs.filter (fun p => band p.1 p.2)).flatMap
      (fun p => third.filter (fun z => band p.1 z && band p.2 z)) =
    pairs.flatMap
      (fun p => third.filter (fun z => band p.1 p.2 && band p.1 z && band p.2 z))
  induction pairs with
  | nil => rfl
  | cons p pairs ih =>
      by_cases hp : band p.1 p.2 = true
      · simp [hp, ih]
      · have hpfalse : band p.1 p.2 = false := by
          cases h : band p.1 p.2 <;> simp_all
        simp [hpfalse, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_RevenueBandEarlyPairFilter
