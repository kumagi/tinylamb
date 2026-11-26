import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_OppositeNationBitIntersection

The two opposite trade directions are disjoint, so filtering their disjunction
and concatenating the two filtered lists give the same physical row order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_OppositeNationBitIntersection

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.OppositeNationBitIntersection.Goal := by
  intro ids supplier customer
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.OppositeNationBitIntersection.Goal]
  let dir : Bool → Nat → Bool := fun side i => supplier i == some side && customer i == some (!side)
  have hdisj : ∀ i, dir true i = true → dir false i = false := by
    intro i hi
    cases hs : supplier i with
    | none => simp [dir, hs] at hi
    | some sv =>
        cases hc : customer i with
        | none => simp [dir, hs, hc] at hi
        | some cv =>
            simp [dir, hs, hc] at hi ⊢
            rcases hi with ⟨hsv, _⟩
            subst sv
            simp
  change (List.filter (fun i => dir true i || dir false i) ids).Perm
    (List.filter (dir true) ids ++ List.filter (dir false) ids)
  induction ids with
  | nil => simp
  | cons i is ih =>
      by_cases h1 : dir true i = true
      · have h2 : dir false i = false := hdisj i h1
        simp [h1, h2, ih]
      · by_cases h2 : dir false i = true
        · have hp : (i :: (List.filter (dir true) is ++ List.filter (dir false) is)).Perm
              (List.filter (dir true) is ++ i :: List.filter (dir false) is) := List.perm_middle.symm
          simpa [h1, h2] using (List.Perm.cons i ih).trans hp
        · simp [h1, h2, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_OppositeNationBitIntersection
