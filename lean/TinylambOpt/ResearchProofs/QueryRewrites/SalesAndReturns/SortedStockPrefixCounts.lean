import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# SortedStockPrefixCounts

In a sorted inventory quantity bucket, the boundary before a sale quantity gives the
number of smaller inventory rows. The executor can count those matches without comparing
each row.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SortedStockPrefixCounts
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem asc : ∀ (l : List Int) (sold : Int),
    l.Pairwise (fun a b => decide (a ≤ b) = true) →
    (l.takeWhile (fun x => decide (x < sold))).length =
      List.countP (fun x => decide (x < sold)) l := by
  intro l
  induction l with
  | nil => intro sold hp; simp [List.countP_eq_length_filter]
  | cons a t ih =>
      intro sold hp
      have hp' := List.pairwise_cons.mp hp
      by_cases hlt : a < sold
      · have ha : decide (a < sold) = true := by simp [hlt]
        simp only [List.takeWhile_cons]
        rw [if_pos ha]
        simp only [List.length_cons]
        rw [ih sold hp'.2]
        rw [List.countP_cons]
        simp [ha]
      · have ha : decide (a < sold) = false := by simp [hlt]
        have hnot : ∀ b ∈ t, ¬ decide (b < sold) = true := by
          intro b hb hbs
          have hab : decide (a ≤ b) = true := hp'.1 b hb
          have hab' : a ≤ b := by simpa using hab
          have hb' : b < sold := by simpa using hbs
          omega
        have hz : List.countP (fun x : Int => decide (x < sold)) t = 0 :=
          List.countP_eq_zero.mpr hnot
        simp only [List.takeWhile_cons]
        rw [if_neg (by simpa using ha : ¬ decide (a < sold) = true)]
        rw [List.countP_cons]
        simp [ha]
        simp [hz]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.SortedStockPrefixCounts.Goal := by
  intro stock sold
  dsimp only [countWhere]
  let le : Int → Int → Bool := fun a b => decide (a ≤ b)
  change ((stock.mergeSort le).takeWhile (fun x : Int => decide (x < sold))).length =
    (stock.filter (fun x : Int => decide (x < sold))).length
  have ht : ∀ a b c : Int, decide (a ≤ b) = true → decide (b ≤ c) = true → decide (a ≤ c) = true := by
    intro a b c hab hbc
    simp at hab hbc ⊢
    omega
  have hto : ∀ a b : Int, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    by_cases h : a ≤ b
    · simp [h]
    · have h' : b ≤ a := by omega
      simp [h, h']
  have hp : (stock.mergeSort le).Pairwise (fun a b => decide (a ≤ b) = true) :=
    List.pairwise_mergeSort (le := le) ht hto stock
  rw [asc _ sold hp]
  rw [← List.countP_eq_length_filter (p := fun x : Int => decide (x < sold)) (l := stock)]
  exact (List.mergeSort_perm stock le).countP_eq (fun x : Int => decide (x < sold))

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SortedStockPrefixCounts

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SortedStockPrefixCounts.sound
