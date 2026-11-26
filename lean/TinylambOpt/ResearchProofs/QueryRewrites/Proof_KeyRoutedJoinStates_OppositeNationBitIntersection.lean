import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_OppositeNationBitIntersection. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_OppositeNationBitIntersection

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# OppositeNationBitIntersection

A supplier/customer pair cannot simultaneously satisfy
`(supplier = true, customer = false)` and `(supplier = false, customer = true)`.
The two trade directions are therefore disjoint, so filtering by their
disjunction and concatenating the two separate filters yield the same row
multiset. Revenue-bearing row IDs are not duplicated.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem krs_opposite_directions_disjoint (supplier customer : Nat → Option Bool) (i : Nat) :
    (supplier i == some true && customer i == some false) = true →
    (supplier i == some false && customer i == some true) = true → False := by
  intro ht hf
  cases hsup : supplier i with
  | none => simp [hsup] at ht
  | some sb =>
      cases sb <;> simp [hsup] at ht hf

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.OppositeNationBitIntersection.Goal := by
  intro ids supplier customer
  dsimp only
  let dir := fun side i => supplier i == some side && customer i == some (!side)
  induction ids with
  | nil => simp
  | cons i ids ih =>
      by_cases ht : dir true i
      · by_cases hf : dir false i
        · have ht' : (supplier i == some true && customer i == some false) = true := by simpa [dir] using ht
          have hf' : (supplier i == some false && customer i == some true) = true := by simpa [dir] using hf
          exact False.elim (krs_opposite_directions_disjoint supplier customer i ht' hf')
        · have ht' : (supplier i == some true && customer i == some false) = true := by simpa [dir] using ht
          have hf' : (supplier i == some false && customer i == some true) = false := by simpa [dir] using hf
          simpa [ht', hf', List.cons_append] using ih.cons i
      · by_cases hf : dir false i
        · have ht' : (supplier i == some true && customer i == some false) = false := by simpa [dir] using ht
          have hf' : (supplier i == some false && customer i == some true) = true := by simpa [dir] using hf
          simpa [ht', hf'] using (ih.cons i).trans List.perm_middle.symm
        · have ht' : (supplier i == some true && customer i == some false) = false := by simpa [dir] using ht
          have hf' : (supplier i == some false && customer i == some true) = false := by simpa [dir] using hf
          simpa [ht', hf'] using ih
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_OppositeNationBitIntersection
