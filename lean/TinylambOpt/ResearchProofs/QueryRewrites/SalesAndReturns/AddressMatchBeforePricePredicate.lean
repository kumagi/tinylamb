import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# AddressMatchBeforePricePredicate

An address that passes the ZIP or state test needs no sales-price comparison for the
modeled OR filter. The executor can evaluate the price condition only for the remaining
rows.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.AddressMatchBeforePricePredicate
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.AddressMatchBeforePricePredicate.Goal := by
  intro rows
  induction rows with
  | nil => simp
  | cons r rows ih =>
      rcases r with ⟨address, price⟩
      by_cases ha : address = true
      · subst address
        simp at ih ⊢
        exact ih
      · have haf : address = false := by cases address <;> simp_all
        subst address
        by_cases hp : price > 500
        · have hp' : decide (price > 500) = true := by simp [hp]
          simp [hp'] at ih ⊢
          exact (List.Perm.cons (false, price) ih).trans List.perm_middle.symm
        · have hp' : decide (price > 500) = false := by simp [hp]
          simp [hp'] at ih ⊢
          exact ih
