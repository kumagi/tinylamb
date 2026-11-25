import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ExactlyOneKey_SubtractiveGrouping

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ExactlyOneKey

-- scratch brute-force truth check
private def pts : List Point := (List.range 5).flatMap (fun i => (List.range 5).map (fun j => ⟨Int.ofNat i - 2, Int.ofNat j - 2⟩))
private def rows1 : List (Point × Int) := pts.flatMap (fun v => [(v, -2), (v, 1)])
private def rows2 : List Rows := [] :: rows1.map (fun r => [r]) ++ rows1.flatMap (fun r => rows1.map (fun s => [r, s]))
private def check (p : Point) (rs : Rows) : Bool :=
  scan shape p rs =
    ((rs.filter (fun r => r.1.x == p.x)).map Prod.snd).sum +
      ((rs.filter (fun r => r.1.y == p.y)).map Prod.snd).sum -
      2 * ((rs.filter (fun r => r.1 == p)).map Prod.snd).sum
#eval pts.all (fun p => rows2.all (fun rs => check p rs))

private theorem point_beq_eq (a b : Point) : (a == b) = (a.x == b.x && a.y == b.y) := by
  rfl

private theorem row_eq (p : Point) (a : Point × Int) :
    (if shape.qualifies p a.1 then a.2 else 0) =
      (if a.1.x == p.x then a.2 else 0) +
        (if a.1.y == p.y then a.2 else 0) -
        2 * (if a.1 == p then a.2 else 0) := by
  rcases a with ⟨v, w⟩
  by_cases hx : v.x == p.x <;> by_cases hy : v.y == p.y <;>
    simp [shape, hx, hy, point_beq_eq] <;> omega

private theorem sum_filter_map_cons (f : Point × Int → Bool) (a : Point × Int) (rs : Rows) :
    (((a :: rs).filter f).map Prod.snd).sum =
      (if f a then a.2 else 0) + ((rs.filter f).map Prod.snd).sum := by
  by_cases h : f a <;> simp [h]

theorem sound_ExactlyOneKey_SubtractiveGrouping : SubtractiveGrouping.Goal := by
  intro p rs
  induction rs with
  | nil => simp [scan]
  | cons a rs ih =>
      simp only [scan] at ih ⊢
      rw [sum_filter_map_cons, sum_filter_map_cons, sum_filter_map_cons,
        sum_filter_map_cons, ih, row_eq]
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_ExactlyOneKey_SubtractiveGrouping
