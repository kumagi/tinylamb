import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ChannelMonthlySlotSums

Combining monthly states in twelve slots preserves the modeled channel totals. The
executor can merge the fixed layout by month.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ChannelMonthlySlotSums
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem flatten_total {α : Type} (ps : List (List α)) (f : α → Int) :
    total ps.flatten f = total ps (fun p => total p f) := by
  dsimp only [total]
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.flatten_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      rw [ih]

theorem total_if_eq_filter {α : Type} [BEq α] [LawfulBEq α]
    (p : List (α × Int)) (month : α) :
    total p (fun r => if r.1 == month then r.2 else 0) =
      total (p.filter (fun r => r.1 == month)) Prod.snd := by
  induction p with
  | nil => simp [total]
  | cons r rs ih =>
      by_cases h : r.1 == month
      · simpa [total, h] using ih
      · simpa [total, h] using ih

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ChannelMonthlySlotSums.Goal := by
  intro pages month
  dsimp only
  rw [flatten_total]
  simp only [total]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  exact total_if_eq_filter p month

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ChannelMonthlySlotSums

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ChannelMonthlySlotSums.sound
