import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_RemoveInterior

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: x, y of length <= 3 over [-2, 2] and a in [-2, 2].
    `0` means that no counterexample was found. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls : List (List Int) := lists 3
  let f : Int → Int → Int := fun u v => u * v
  let bad := ls.flatMap (fun x => ls.flatMap (fun y => vals.filter (fun a =>
    decide (LagProduct.measure (x ++ y) - LagProduct.measure (x ++ a :: y) !=
      seam x y f - seam x [a] f - seam [a] y f)) |>.map (fun a => (x, y, a))))
  pure bad.length

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

private theorem adj_insert (x y : Series) (z : Int) :
    adj (x ++ z :: y) = adj x ++ edgeBridge x [z] ++ edgeBridge [z] y ++ adj y := by
  have h1 := adj_append x (z :: y)
  have he : edgeBridge x (z :: y) = edgeBridge x [z] := by
    simp [edgeBridge]
  have h2 := adj_append [z] y
  have hz : adj (z :: y) = edgeBridge [z] y ++ adj y := by
    simpa [adj] using h2.symm
  rw [← h1, he, hz]
  simp only [List.append_assoc]

private theorem seam_eq_bridge_sum (x y : Series) (f : Int → Int → Int) :
    seam x y f = ((edgeBridge x y).map (fun p : Int × Int => f p.1 p.2)).sum := by
  cases x <;> cases y <;> simp [seam, edgeBridge, List.getLast?_cons, List.head?_cons]

private theorem measure_append (x y : Series) :
    LagProduct.measure (x ++ y) =
      LagProduct.measure x + seam x y (fun u v => u * v) + LagProduct.measure y := by
  unfold LagProduct.measure
  rw [← adj_append x y]
  simp only [List.map_append, List.sum_append]
  rw [seam_eq_bridge_sum x y (fun u v => u * v)]

private theorem measure_insert (x y : Series) (z : Int) :
    LagProduct.measure (x ++ z :: y) =
      LagProduct.measure x + seam x [z] (fun u v => u * v) +
        seam [z] y (fun u v => u * v) + LagProduct.measure y := by
  unfold LagProduct.measure
  rw [adj_insert]
  simp only [List.map_append, List.sum_append]
  rw [seam_eq_bridge_sum x [z] (fun u v => u * v)]
  rw [seam_eq_bridge_sum [z] y (fun u v => u * v)]

theorem sound_LagProduct_RemoveInterior : LagProduct.RemoveInterior.Goal := by
  intro x y a
  rw [measure_append x y, measure_insert x y a]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_RemoveInterior
