import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_ExceptionalShipDatesSubtractFromGroupState. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_ExceptionalShipDatesSubtractFromGroupState

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# ExceptionalShipDatesSubtractFromGroupState

The late rows (`date > cutoff`) and the early rows (`date <= cutoff`) form a
complementary partition of one return/status group. Filtering nullable amounts
after the partition splits the group's amount multiset in two, so subtracting
the late `SUM` and `COUNT` from the total reconstructs the eligible aggregate
state. Nat subtraction is safe because the counts are additively partitioned.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem krs_perm_sum_int {l₁ l₂ : List Int} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simpa using ih
  | swap x y l => simp; omega
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

theorem krs_amounts_partition (xs : List (Nat × Option Int)) (cutoff : Nat) :
    let amounts := fun (rs : List (Nat × Option Int)) => rs.filterMap Prod.snd
    (amounts xs).Perm (amounts (xs.filter (fun r => cutoff < r.1)) ++
      amounts (xs.filter (fun r => r.1 ≤ cutoff))) := by
  induction xs with
  | nil => simp
  | cons r xs ih =>
      rcases r with ⟨k, v⟩
      cases v with
      | none =>
          by_cases h : cutoff < k
          · have hn : ¬ k ≤ cutoff := by omega
            simpa [h, hn] using ih
          · have hle : k ≤ cutoff := by omega
            simpa [h, hle] using ih
      | some a =>
          by_cases h : cutoff < k
          · have hn : ¬ k ≤ cutoff := by omega
            simpa [h, hn] using ih.cons a
          · have hle : k ≤ cutoff := by omega
            exact (by
              have hc := ih.cons a
              simpa [h, hle] using hc.trans List.perm_middle.symm)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ExceptionalShipDatesSubtractFromGroupState.Goal := by
  intro xs cutoff
  dsimp
  have hp := krs_amounts_partition xs cutoff
  dsimp at hp
  have hsum : (xs.filterMap Prod.snd).sum =
      ((xs.filter (fun r => cutoff < r.1)).filterMap Prod.snd ++
        (xs.filter (fun r => r.1 ≤ cutoff)).filterMap Prod.snd).sum := krs_perm_sum_int hp
  have hlen := hp.length_eq
  rw [hsum, hlen]
  simp only [List.sum_append, List.length_append]
  apply Prod.ext <;> omega
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_ExceptionalShipDatesSubtractFromGroupState
