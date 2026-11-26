import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# MonthlyDeviationFromTotalAndCount

The group sum and number of contributing months determine the average used in the
modeled deviation test. The executor can evaluate it from aggregate state instead of
rereading window rows.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MonthlyDeviationFromTotalAndCount

theorem fm_flatten {α β : Type} (bs : List (List α)) (f : α → Option β) :
    bs.flatten.filterMap f = (bs.map (fun b => b.filterMap f)).flatten := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]

theorem sum_flatten : ∀ bs : List (List Int), bs.flatten.sum = (bs.map List.sum).sum := by
  intro bs
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]

theorem len_flatten {α : Type} : ∀ bs : List (List α), bs.flatten.length = (bs.map List.length).sum := by
  intro bs
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.MonthlyDeviationFromTotalAndCount.Goal := by
  intro blocks
  rw [fm_flatten, sum_flatten, len_flatten]
  rw [List.map_map, List.map_map]
  rfl

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MonthlyDeviationFromTotalAndCount

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MonthlyDeviationFromTotalAndCount.sound
