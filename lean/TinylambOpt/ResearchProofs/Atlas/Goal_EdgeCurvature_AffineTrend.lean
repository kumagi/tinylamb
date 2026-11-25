import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_AffineTrend

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj_cons₂]

private def d2 (x : Series) : Series := delta (delta x)

private theorem d2_cons₃ (a b c : Int) (xs : Series) :
    d2 (a :: b :: c :: xs) = (c - 2*b + a) :: d2 (b :: c :: xs) := by
  unfold d2
  rw [delta_cons₂ a b (c :: xs)]
  rw [delta_cons₂ b c xs]
  rw [delta_cons₂ (b - a) (c - b) (delta (c :: xs))]
  simp
  omega

private theorem zipIdx_add (xs : Series) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private def shiftAffine (a c : Int) (x : Series) : Series :=
  x.zipIdx.map (fun (v, i) => v + a * (i : Int) + c)

private theorem shiftAffine_cons (a c : Int) (v : Int) (xs : Series) :
    shiftAffine a c (v :: xs) = (v + c) :: shiftAffine a (c + a) xs := by
  unfold shiftAffine
  simp only [List.zipIdx_cons, List.map_cons]
  have hzip := zipIdx_add xs 0 1
  simp only [Nat.zero_add] at hzip
  rw [hzip]
  simp only [List.map_map]
  congr 1
  · simp
  · apply List.map_congr_left
    intro p hp
    rcases p with ⟨w, i⟩
    dsimp
    rw [Int.mul_add]
    simp
    omega

private theorem d2_shiftAffine (a c : Int) (x : Series) :
    d2 (shiftAffine a c x) = d2 x := by
  induction x generalizing c with
  | nil => simp [d2, shiftAffine, delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [d2, shiftAffine, delta, adj]
      | cons w xs =>
          cases xs with
          | nil => simp [d2, shiftAffine, delta, adj]
          | cons u xs =>
              rw [shiftAffine_cons a c v (w :: u :: xs)]
              rw [shiftAffine_cons a (c + a) w (u :: xs)]
              rw [shiftAffine_cons a (c + a + a) u xs]
              rw [d2_cons₃]
              have htail := ih (c + a)
              rw [shiftAffine_cons a (c + a) w (u :: xs)] at htail
              rw [shiftAffine_cons a (c + a + a) u xs] at htail
              rw [htail]
              congr 1
              dsimp
              omega

theorem sound_EdgeCurvature_AffineTrend : EdgeCurvature.AffineTrend.Goal := by
  intro x a b
  change ((d2 (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b))).map (fun v => v ^ 2)).sum =
    ((d2 x).map (fun v => v ^ 2)).sum
  rw [show (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b)) = shiftAffine a b x by rfl]
  rw [d2_shiftAffine]

end TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_AffineTrend
