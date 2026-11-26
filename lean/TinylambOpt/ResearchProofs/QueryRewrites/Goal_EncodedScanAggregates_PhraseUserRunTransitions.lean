import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false



/-!
Inside one sorted phrase posting, a distinct user is introduced exactly at
the first lane and at every adjacent change. Counting those transitions is
therefore the same as counting distinct users, including for an empty posting.
SQL may derive COUNT(DISTINCT user) from a run of encoded user ordinals.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_PhraseUserRunTransitions

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites (countWhere)

private theorem run_transition_count :
    ∀ (a : Nat) (xs : List Nat), xs.Pairwise (· ≤ ·) → (∀ x ∈ xs, a ≤ x) →
      (a :: xs).eraseDups.length =
        1 + countWhere ((a :: xs).zip ((a :: xs).drop 1)) (fun p : Nat × Nat => p.1 != p.2) := by
  intro a xs
  induction xs generalizing a with
  | nil =>
      intro hs hge
      simp [countWhere, List.eraseDups_cons]
  | cons b rest ih =>
      intro hs hge
      have hs' := List.pairwise_cons.mp hs
      have hb_le : a ≤ b := hge b (by simp)
      have hrest_pair : rest.Pairwise (· ≤ ·) := hs'.2
      have hb_rest : ∀ x ∈ rest, b ≤ x := hs'.1
      have ha_rest : ∀ x ∈ rest, a ≤ x := fun x hx => Nat.le_trans hb_le (hb_rest x hx)
      by_cases hab : a == b
      · have heq : a = b := beq_iff_eq.mp hab
        have ih' := ih a hrest_pair ha_rest
        have hf : (b :: rest).filter (fun x => !x == a) = rest.filter (fun x => !x == a) := by
          rw [List.filter_cons]
          simp [heq]
        rw [List.eraseDups_cons, hf]
        simp only [List.length_cons]
        have herase : (rest.filter (fun x => !x == a)).eraseDups.length =
            countWhere ((a :: rest).zip ((a :: rest).drop 1)) (fun p : Nat × Nat => p.1 != p.2) := by
          have hEq : (rest.filter (fun x => !x == a)).eraseDups.length + 1 =
              1 + countWhere ((a :: rest).zip ((a :: rest).drop 1)) (fun p : Nat × Nat => p.1 != p.2) := by
            simpa [List.eraseDups_cons, Nat.add_comm] using ih'
          omega
        rw [herase]
        simp [countWhere, heq]
        omega
      · have hba : (b == a) = false := by
          by_cases hba' : b == a
          · exfalso
            apply hab
            have heqba : b = a := beq_iff_eq.mp hba'
            simp [heqba]
          · simpa using hba'
        have hneBool : (a != b) = true := by simpa using hab
        have hf : (b :: rest).filter (fun x => !x == a) = b :: rest := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hxa_false : (x == a) = false := by
            by_cases hxa : x == a
            · exfalso
              have hxeq : x = a := beq_iff_eq.mp hxa
              have hbx : b ≤ x := by
                cases hx with
                | head => exact Nat.le_refl b
                | tail _ hmem => exact hb_rest x hmem
              have habEq : a = b := by omega
              exact hab (by simp [habEq])
            · simpa using hxa
          simp [hxa_false]
        rw [List.eraseDups_cons, hf]
        have ih' := ih b hrest_pair hb_rest
        simp only [List.length_cons]
        rw [ih']
        simp only [List.drop_succ_cons, List.drop_zero, List.zip_cons_cons]
        unfold countWhere
        simp [hneBool]
        omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.PhraseUserRunTransitions.Goal := by
  intro users hsorted
  cases users with
  | nil => simp [countWhere]
  | cons a xs =>
      have hs := List.pairwise_cons.mp hsorted
      exact run_transition_count a xs hs.2 hs.1
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_PhraseUserRunTransitions
