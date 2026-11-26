import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
# RevenueBandEarlyPairFilter

The reciprocal band between a pair and a third-channel probe is evaluated
independently for each pair. Filtering the pairs first therefore only removes
whole `[]` contributions from the flat-map: for a rejected pair the right-hand
filter is false for every probe. SQL may apply the pairwise band check before
reading the third-channel list without changing the bag of accepted triples.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_RevenueBandEarlyPairFilter

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

theorem sound : RevenueBandEarlyPairFilter.Goal := by
  intro pairs third
  let band := fun (a b : Nat) => decide
    (9 * b ≤ 10 * a ∧ 10 * a ≤ 11 * b ∧ 9 * a ≤ 10 * b ∧ 10 * b ≤ 11 * a)
  let complete := fun (p : Nat × Nat) => third.filter (fun z => band p.1 z && band p.2 z)
  change (pairs.filter (fun p => band p.1 p.2)).flatMap complete =
    pairs.flatMap (fun p => third.filter (fun z => band p.1 p.2 && band p.1 z && band p.2 z))
  induction pairs with
  | nil => simp
  | cons p ps ih =>
      simp only [List.filter_cons, List.flatMap_cons]
      by_cases h : band p.1 p.2 = true
      · simp [complete, h, ih]
      · simp [complete, h, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_RevenueBandEarlyPairFilter
