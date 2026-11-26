import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `QuadraticVariation.LocalEdit` — `∀ x y a b, measure (x++b::y)-measure (x++a::y) =
  seam x [b] (fun u v => (v-u)^2) - seam x [a] (fun u v => (v-u)^2) + seam [b] y (fun
  u v => (v-u)^2) - seam [a] y (fun u v => (v-u)^2)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_LocalEdit

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: x, y of length ≤ 3 over [-2, 2] and a, b in [-2, 2].
    `0` means no counterexample was found. -/
#eval do
  let vals : List Int := [-2, -1, 0, 1, 2]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls : List (List Int) := lists 3
  let f : Int → Int → Int := fun u v => (v - u)^2
  let bad := ls.flatMap (fun x => ls.flatMap (fun y => vals.flatMap (fun a => vals.filter (fun b =>
    decide (QuadraticVariation.measure (x ++ b :: y) - QuadraticVariation.measure (x ++ a :: y) !=
      seam x [b] f - seam x [a] f + seam [b] y f - seam [a] y f)) |>.map (fun b => (x, y, a, b)))))
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

private theorem delta_insert (x y : Series) (z : Int) :
    delta (x ++ z :: y) =
      delta x ++ (edgeBridge x [z]).map (fun p : Int × Int => p.2 - p.1)
        ++ (edgeBridge [z] y).map (fun p : Int × Int => p.2 - p.1)
        ++ delta y := by
  unfold delta
  rw [adj_insert]
  simp only [List.map_append]

private theorem sq_map_sub_sum (l : List (Int × Int)) :
    ((l.map (fun p : Int × Int => p.2 - p.1)).map (fun v : Int => v^2)).sum =
      (l.map (fun p : Int × Int => (p.2 - p.1)^2)).sum := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]

private theorem measure_insert (x y : Series) (z : Int) :
    QuadraticVariation.measure (x ++ z :: y) =
      QuadraticVariation.measure x
        + ((edgeBridge x [z]).map (fun p : Int × Int => (p.2 - p.1)^2)).sum
        + ((edgeBridge [z] y).map (fun p : Int × Int => (p.2 - p.1)^2)).sum
        + QuadraticVariation.measure y := by
  unfold QuadraticVariation.measure
  rw [delta_insert]
  simp only [List.map_append, List.sum_append]
  rw [sq_map_sub_sum, sq_map_sub_sum]

private theorem seam_eq_bridge_sum (x y : Series) (f : Int → Int → Int) :
    seam x y f = ((edgeBridge x y).map (fun p : Int × Int => f p.1 p.2)).sum := by
  cases x <;> cases y <;> simp [seam, edgeBridge, List.getLast?_cons, List.head?_cons]

theorem sound_QuadraticVariation_LocalEdit : QuadraticVariation.LocalEdit.Goal := by
  intro x y a b
  let f : Int → Int → Int := fun u v => (v - u)^2
  rw [measure_insert x y b, measure_insert x y a]
  rw [seam_eq_bridge_sum x [b] f, seam_eq_bridge_sum x [a] f,
      seam_eq_bridge_sum [b] y f, seam_eq_bridge_sum [a] y f]
  simp only [f]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_LocalEdit
