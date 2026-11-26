import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `EitherEndpointInside.SplitBoundaryDebt` — `∀ p m rs, p.x ≤ m → m ≤ p.y → scan shape
  p rs = scan shape ⟨p.x,m⟩ rs + scan shape ⟨m,p.y⟩ rs - ((rs.filter (fun r =>
  shape.qualifies ⟨p.x,m⟩ r.1 && shape.qualifies ⟨m,p.y⟩ r.1)).map Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EitherEndpointInside_SplitBoundaryDebt

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.EitherEndpointInside

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum = (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x - g x)).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem or_weight (a b : Bool) (w : Int) :
    (if a || b then w else 0) =
      (if a then w else 0) + (if b then w else 0) - (if a && b then w else 0) := by
  cases a <;> cases b <;> simp <;> omega

theorem sound_EitherEndpointInside_SplitBoundaryDebt :
    EitherEndpointInside.SplitBoundaryDebt.Goal := by
  intro p m rs hxm hmy
  rw [scan, scan, scan,
    sum_filter_eq_sum_if, sum_filter_eq_sum_if,
    sum_filter_eq_sum_if, sum_filter_eq_sum_if]
  rw [← sum_map_add, ← sum_map_sub]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  rcases r with ⟨v, w⟩
  have hqual : shape.qualifies p v =
      (shape.qualifies ⟨p.x, m⟩ v || shape.qualifies ⟨m, p.y⟩ v) := by
    by_cases hx1 : p.x ≤ v.x <;> by_cases hx2 : v.x < m <;>
      by_cases hx3 : m ≤ v.x <;> by_cases hx4 : v.x < p.y <;>
      by_cases hy1 : p.x ≤ v.y <;> by_cases hy2 : v.y < m <;>
      by_cases hy3 : m ≤ v.y <;> by_cases hy4 : v.y < p.y <;>
      simp [shape, hx1, hx2, hx3, hx4, hy1, hy2, hy3, hy4] <;> omega
  rw [hqual]
  exact or_weight (shape.qualifies ⟨p.x, m⟩ v) (shape.qualifies ⟨m, p.y⟩ v) w

end TinylambOpt.ResearchProofs.Atlas.Goal_EitherEndpointInside_SplitBoundaryDebt
