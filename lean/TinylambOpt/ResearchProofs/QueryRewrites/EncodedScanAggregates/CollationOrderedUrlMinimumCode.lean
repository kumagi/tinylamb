import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

/-!
# CollationOrderedUrlMinimumCode

An order-preserving dictionary decodes the minimum accepted code to the
minimum matching URL, including empty groups. This allows min on encoded keys
to be answered by decoding the min key only.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.CollationOrderedUrlMinimumCode

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem foldl_min_mono (url : Nat → String) (hmono : ∀ a b, a ≤ b → url a ≤ url b)
    (a : Nat) (as : List Nat) :
    url (as.foldl min a) = (as.map url).foldl min (url a) := by
  induction as generalizing a with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.map_cons]
      rw [ih]
      congr 1
      by_cases hab : a ≤ b
      · have hab' := hmono a b hab
        rw [Nat.min_eq_left hab]
        simp [min, hab']
      · have hba : b ≤ a := Nat.le_of_not_ge hab
        have hba' := hmono b a hba
        rw [Nat.min_eq_right hba]
        simp only [min]
        split
        · rename_i h; exact (String.le_antisymm h hba').symm
        · rfl

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.CollationOrderedUrlMinimumCode.Goal := by
  intro ids url hmono
  cases ids with
  | nil => simp
  | cons a as =>
      simp only [List.min?, List.map_cons, Option.map_some]
      congr 1
      exact foldl_min_mono url hmono a as

#print axioms sound

end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.CollationOrderedUrlMinimumCode
