import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_PageBoundary

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RunTransitions

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

private theorem bridge_countP_eq_seam (x y : Series) :
    ((edgeBridge x y).countP (fun (a, b) => a != b) : Int) =
      seam x y (fun a b => if a == b then 0 else 1) := by
  simp only [edgeBridge, seam]
  cases hxa : x.getLast? with
  | none => simp
  | some a =>
      cases hyb : y.head? with
      | none => simp
      | some b =>
          by_cases hab : a = b <;> simp [hab]

theorem sound_RunTransitions_PageBoundary : RunTransitions.PageBoundary.Goal := by
  intro x y
  unfold RunTransitions.measure
  rw [← adj_append x y]
  simp only [List.countP_append]
  push_cast
  rw [bridge_countP_eq_seam x y]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_PageBoundary
