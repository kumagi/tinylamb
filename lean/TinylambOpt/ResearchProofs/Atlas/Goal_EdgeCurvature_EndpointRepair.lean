import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_EndpointRepair

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private def d2 (x : Series) : Series := delta (delta x)

private def tail2 (x : Series) : Series := (x.reverse.take 2).reverse

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj_cons₂]

private theorem d2_cons₃ (a b c : Int) (xs : Series) :
    d2 (a :: b :: c :: xs) = (c - 2 * b + a) :: d2 (b :: c :: xs) := by
  unfold d2
  rw [delta_cons₂ a b (c :: xs)]
  rw [delta_cons₂ b c xs]
  rw [delta_cons₂ (b - a) (c - b) (delta (c :: xs))]
  simp
  omega

private theorem d2_pair (a b : Int) : d2 [a, b] = [] := by
  simp [d2, delta, adj]

private theorem d2_take2 (y : Series) : d2 (y.take 2) = [] := by
  cases y with
  | nil => simp [d2, delta, adj]
  | cons b ys =>
      cases ys with
      | nil => simp [d2, delta, adj]
      | cons c cs => simp [d2, delta, adj]

private theorem d2_cons_append1 (a : Int) (y : Series) :
    d2 (a :: y) = d2 ([a] ++ y.take 2) ++ d2 y := by
  cases y with
  | nil => simp [d2, delta, adj]
  | cons b ys =>
      cases ys with
      | nil => simp [d2, delta, adj]
      | cons c cs =>
          rw [d2_cons₃ a b c cs]
          simp [d2_cons₃, d2_pair]

private theorem d2_cons_append2 (a b : Int) (y : Series) :
    d2 (a :: b :: y) = d2 ([a, b] ++ y.take 2) ++ d2 y := by
  cases y with
  | nil => simp [d2, delta, adj]
  | cons c ys =>
      cases ys with
      | nil => simp [d2, delta, adj]
      | cons d ds =>
          rw [d2_cons₃ a b c (d :: ds)]
          rw [d2_cons₃ b c d ds]
          simp [d2_cons₃, d2_pair]

private theorem tail2_cons₃ (a b c : Int) (cs : Series) :
    tail2 (a :: b :: c :: cs) = tail2 (b :: c :: cs) := by
  unfold tail2
  cases cs with
  | nil => simp
  | cons d ds =>
      cases ds with
      | nil => simp
      | cons e es =>
          simp only [List.reverse_cons]
          repeat rw [List.append_assoc]
          change (List.take 2 (es.reverse ++ [e, d, c, b, a])).reverse =
            (List.take 2 (es.reverse ++ [e, d, c, b])).reverse
          rw [show es.reverse ++ [e, d, c, b, a] = (es.reverse ++ [e, d]) ++ [c, b, a] by
            simp only [List.append_assoc, List.cons_append, List.nil_append]]
          rw [show es.reverse ++ [e, d, c, b] = (es.reverse ++ [e, d]) ++ [c, b] by
            simp only [List.append_assoc, List.cons_append, List.nil_append]]
          have hlen : 2 ≤ (es.reverse ++ [e, d]).length := by simp
          rw [List.take_append_of_le_length hlen]
          rw [List.take_append_of_le_length hlen]

private theorem d2_append (x y : Series) :
    d2 (x ++ y) = d2 x ++ d2 (tail2 x ++ y.take 2) ++ d2 y := by
  induction x with
  | nil =>
      simp only [List.nil_append]
      change d2 y = d2 [] ++ d2 (y.take 2) ++ d2 y
      rw [d2_take2]
      simp [d2, delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil =>
          simpa [tail2, d2, delta, adj] using d2_cons_append1 a y
      | cons b xs =>
          cases xs with
          | nil =>
              simpa [tail2, d2, delta, adj] using d2_cons_append2 a b y
          | cons c cs =>
              change d2 (a :: b :: c :: (cs ++ y)) =
                d2 (a :: b :: c :: cs) ++ d2 (tail2 (a :: b :: c :: cs) ++ y.take 2) ++ d2 y
              rw [d2_cons₃ a b c (cs ++ y)]
              have ih' : d2 (b :: c :: (cs ++ y)) =
                  d2 (b :: c :: cs) ++ d2 (tail2 (b :: c :: cs) ++ y.take 2) ++ d2 y := by
                simpa only [List.cons_append] using ih
              rw [ih']
              rw [d2_cons₃ a b c cs]
              rw [tail2_cons₃]
              simp only [List.cons_append, List.append_assoc]

theorem sound_EdgeCurvature_EndpointRepair : EdgeCurvature.EndpointRepair.Goal := by
  intro x y
  unfold EdgeCurvature.measure QuadraticVariation.measure
  change ((d2 (x ++ y)).map (fun v => v ^ 2)).sum =
    ((d2 x).map (fun v => v ^ 2)).sum + ((d2 y).map (fun v => v ^ 2)).sum +
      ((d2 (tail2 x ++ y.take 2)).map (fun v => v ^ 2)).sum
  rw [d2_append]
  simp only [List.map_append, List.sum_append]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_EndpointRepair
