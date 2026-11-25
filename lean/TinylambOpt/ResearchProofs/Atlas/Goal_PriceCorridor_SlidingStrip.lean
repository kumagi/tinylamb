import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_SlidingStrip

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.PriceCorridor

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih]

private theorem sum_map_sub {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b - g b)).sum = (bs.map f).sum - (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih] <;> omega

theorem sound_PriceCorridor_SlidingStrip : SlidingStrip.Goal := by
  intro p rs
  dsimp only
  unfold scan
  rw [sum_filter_eq_sum_if, sum_filter_eq_sum_if, sum_filter_eq_sum_if,
    sum_filter_eq_sum_if]
  rw [← sum_map_sub]
  rw [← sum_map_sub]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases hylo : p.y - 2 ≤ r.1.y <;> by_cases hyhi : r.1.y ≤ p.y + 5 <;>
    by_cases hx4 : r.1.x = p.x + 4 <;> by_cases hx3 : r.1.x = p.x - 3 <;>
      simp [shape, hylo, hyhi, hx4, hx3] <;> omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PriceCorridor_SlidingStrip
