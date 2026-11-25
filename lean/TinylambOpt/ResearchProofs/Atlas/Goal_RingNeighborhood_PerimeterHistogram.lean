import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RingNeighborhood_PerimeterHistogram

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.RingNeighborhood

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) (g : Point × Int → Int) :
    ((rs.filter f).map g).sum = (rs.map (fun r => if f r then g r else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem sum_map_add {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b + g b)).sum = (bs.map f).sum + (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]; omega

private theorem sum_map_zero {β : Type} (bs : List β) : (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => exact (sum_map_zero bs).symm
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private theorem perimeter_row (p v : Point) (w : Int) :
    (if shape.qualifies p v then w else 0) =
      ([2,3,4,5].map (fun d => if max (v.x-p.x).natAbs (v.y-p.y).natAbs == d then w else 0)).sum := by
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  unfold shape
  dsimp only
  let d := max (v.x - p.x).natAbs (v.y - p.y).natAbs
  change (if 1 < d && d ≤ 5 then w else 0) =
    (if d == 2 then w else 0) + ((if d == 3 then w else 0) +
      ((if d == 4 then w else 0) + ((if d == 5 then w else 0) + 0)))
  by_cases h2 : d = 2 <;> by_cases h3 : d = 3 <;> by_cases h4 : d = 4 <;>
    by_cases h5 : d = 5 <;> by_cases hlo : 1 < d <;> by_cases hhi : d ≤ 5 <;>
      simp [h2, h3, h4, h5, hlo, hhi] <;> omega

private theorem rhs_eq (p : Point) (rs : Rows) :
    ([2,3,4,5].map (fun d =>
      ((rs.filter (fun r => max (r.1.x-p.x).natAbs (r.1.y-p.y).natAbs == d)).map Prod.snd).sum)).sum =
    ([2,3,4,5].map (fun d =>
      (rs.map (fun r => if max (r.1.x-p.x).natAbs (r.1.y-p.y).natAbs == d then r.2 else 0)).sum)).sum := by
  apply congrArg List.sum
  apply List.map_congr_left
  intro d hd
  exact sum_filter_eq_sum_if rs (fun r => max (r.1.x-p.x).natAbs (r.1.y-p.y).natAbs == d) Prod.snd

theorem sound_RingNeighborhood_PerimeterHistogram : RingNeighborhood.PerimeterHistogram.Goal := by
  intro p rs
  unfold scan
  rw [sum_filter_eq_sum_if rs (fun r => shape.qualifies p r.1) Prod.snd]
  rw [rhs_eq]
  rw [sum_swap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  exact perimeter_row p r.1 r.2

end TinylambOpt.ResearchProofs.Atlas.Goal_RingNeighborhood_PerimeterHistogram
