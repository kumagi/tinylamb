import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `PriorTradingDay.DailyState` — `∀ p rs, scan shape p rs = ([(1:Int),2,3].map (fun d
  => ((rs.filter (fun r => r.1.x == p.x-d)).map Prod.snd).sum)).sum + ((rs.filter
  (fun r => r.1.x == p.x && r.1.y < p.y)).map Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_DailyState

open TinylambOpt.ResearchGoals.Atlas.RangePlans

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool)
    (g : Point × Int → Int) :
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

private theorem scan_eq_sum_indicators (s : Shape) (p : Point) (rs : Rows) :
    scan s p rs = (rs.map (fun r => r.2 * (if s.qualifies p r.1 then (1 : Int) else 0))).sum := by
  unfold scan
  rw [sum_filter_eq_sum_if]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  by_cases h : s.qualifies p r.1 = true <;> simp [h]

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, ← sum_map_add]

private theorem sum_filters_swap {α : Type} (ds : List α) (rs : Rows)
    (f : α → Point × Int → Bool) :
    (ds.map (fun d => ((rs.filter (fun r => f d r)).map Prod.snd).sum)).sum =
      (rs.map (fun r => (ds.map (fun d => if f d r then r.2 else 0)).sum)).sum := by
  calc
    (ds.map (fun d => ((rs.filter (fun r => f d r)).map Prod.snd).sum)).sum =
        (ds.map (fun d =>
          (rs.map (fun r => if f d r then r.2 else 0)).sum)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro d hd
      exact sum_filter_eq_sum_if rs (fun r => f d r) Prod.snd
    _ = _ := sum_swap ds rs (fun d r => if f d r then r.2 else 0)

theorem sound_PriorTradingDay_DailyState : PriorTradingDay.DailyState.Goal := by
  intro p rs
  rw [scan_eq_sum_indicators]
  rw [sum_filters_swap [1, 2, 3] rs (fun d r => r.1.x == p.x - d),
    sum_filter_eq_sum_if rs (fun r => r.1.x == p.x && r.1.y < p.y) Prod.snd,
    ← sum_map_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro r hr
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  by_cases h1 : r.1.x = p.x - 1 <;> by_cases h2 : r.1.x = p.x - 2 <;>
    by_cases h3 : r.1.x = p.x - 3 <;> by_cases hsx : r.1.x = p.x <;>
    by_cases hsy : r.1.y < p.y <;> by_cases hlo : p.x - 3 ≤ r.1.x <;>
    by_cases hhi : r.1.x < p.x
  all_goals
    have h1b : (r.1.x == p.x - 1) = decide (r.1.x = p.x - 1) := rfl
    have h2b : (r.1.x == p.x - 2) = decide (r.1.x = p.x - 2) := rfl
    have h3b : (r.1.x == p.x - 3) = decide (r.1.x = p.x - 3) := rfl
    have hsxb : (r.1.x == p.x) = decide (r.1.x = p.x) := rfl
    rw [h1b, h2b, h3b, hsxb]
    all_goals first
      | rw [decide_eq_true h1]
      | rw [decide_eq_false h1]
    all_goals first
      | rw [decide_eq_true h2]
      | rw [decide_eq_false h2]
    all_goals first
      | rw [decide_eq_true h3]
      | rw [decide_eq_false h3]
    all_goals first
      | rw [decide_eq_true hsx]
      | rw [decide_eq_false hsx]
    all_goals simp only [PriorTradingDay.shape, hsxb]
    all_goals try rw [decide_eq_true hsx]
    all_goals try rw [decide_eq_false hsx]
    simp only [hsy, hlo, hhi, Bool.and_eq_true, decide_eq_true_eq,
      Bool.false_eq_true, false_and, and_false, and_true, if_false, if_true] <;> grind

end TinylambOpt.ResearchProofs.Atlas.Goal_PriorTradingDay_DailyState
