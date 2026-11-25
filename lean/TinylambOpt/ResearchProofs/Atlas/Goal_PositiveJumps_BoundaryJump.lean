import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_BoundaryJump

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private def edgeBridge (x y : Series) : List (Int × Int) :=
  (x.getLast?.bind (fun a => y.head?.map (fun b => (a, b)))).toList

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem adj_append (x y : Series) :
    adj x ++ edgeBridge x y ++ adj y = adj (x ++ y) := by
  induction x with
  | nil => simp [adj, edgeBridge]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [adj, edgeBridge]
          | cons b ys =>
              simp only [List.cons_append, List.nil_append, adj_cons₂]
              simp [adj, edgeBridge, List.getLast?_cons, List.head?_cons]
      | cons b bs =>
          simp only [List.cons_append]
          have hbridge : edgeBridge (a :: b :: bs) y = edgeBridge (b :: bs) y := by
            simp [edgeBridge, List.getLast?_cons]
          have hi : adj (b :: bs) ++ edgeBridge (b :: bs) y ++ adj y =
              adj (b :: (bs ++ y)) := by
            simpa only [List.cons_append] using ih
          rw [adj_cons₂, hbridge, adj_cons₂]
          simpa only [List.append_assoc, List.cons_append] using
            congrArg (fun l => (a, b) :: l) hi

private theorem seam_eq_bridge_sum (x y : Series) (f : Int → Int → Int) :
    seam x y f = ((edgeBridge x y).map (fun p : Int × Int => f p.1 p.2)).sum := by
  cases x <;> cases y <;> simp [seam, edgeBridge, List.getLast?_cons,
    List.head?_cons]

theorem sound_PositiveJumps_BoundaryJump : PositiveJumps.BoundaryJump.Goal := by
  intro x y
  unfold PositiveJumps.measure delta
  rw [← adj_append]
  simp only [List.map_append, List.map_map, Function.comp_def, List.sum_append]
  rw [seam_eq_bridge_sum]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_BoundaryJump
