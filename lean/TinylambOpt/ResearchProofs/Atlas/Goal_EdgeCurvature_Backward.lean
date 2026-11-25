import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_Backward

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj_cons₂]

private theorem getLastD_reverse (x : Series) : x.reverse.getLast?.getD 0 = x.head?.getD 0 := by
  rw [List.getLast?_reverse]

private theorem delta_append_singleton (x : Series) (a : Int) (hx : x ≠ []) :
    delta (x ++ [a]) = delta x ++ [a - x.getLast?.getD 0] := by
  induction x with
  | nil => contradiction
  | cons b bs ih =>
      cases bs with
      | nil => simp [delta, adj]
      | cons c cs =>
          have hne : (c :: cs) ≠ [] := by simp
          have hi := ih hne
          change (c - b) :: delta ((c :: cs) ++ [a]) =
            (c - b) :: (delta (c :: cs) ++ [a - (b :: c :: cs).getLast?.getD 0])
          rw [hi]
          simp [List.getLast?_cons]

private theorem delta_reverse (x : Series) :
    delta x.reverse = (delta x).reverse.map (fun v => -v) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          rw [List.reverse_cons]
          rw [delta_append_singleton ((b :: bs).reverse) a (by simp)]
          rw [ih]
          rw [getLastD_reverse (b :: bs)]
          simp only [List.head?_cons, Option.getD_some]
          rw [delta_cons₂]
          simp only [List.map_cons, List.reverse_cons, List.map_append]
          have hneg : -(b - a) = a - b := by omega
          rw [hneg]
          simp

private theorem delta_map_neg (x : Series) :
    delta (x.map (fun v => -v)) = (delta x).map (fun v => -v) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          simp only [List.map_cons] at ih ⊢
          rw [delta_cons₂, ih]
          change (-b - -a) :: (delta (b :: bs)).map (fun v => -v) =
            (-(b - a)) :: (delta (b :: bs)).map (fun v => -v)
          congr 1
          omega

private theorem d2_reverse (x : Series) : delta (delta x.reverse) = (delta (delta x)).reverse := by
  rw [delta_reverse]
  rw [delta_map_neg]
  rw [delta_reverse]
  simp [List.map_map, Function.comp_def]

theorem sound_EdgeCurvature_Backward : EdgeCurvature.Backward.Goal := by
  intro x
  simp only [EdgeCurvature.measure, QuadraticVariation.measure]
  rw [d2_reverse]
  rw [List.map_reverse]
  exact List.sum_reverse _

end TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_Backward
