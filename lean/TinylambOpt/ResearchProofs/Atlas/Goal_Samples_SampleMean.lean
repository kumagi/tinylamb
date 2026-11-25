import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Samples_SampleMean

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

private theorem length_samples {α : Type} (xs : List α) :
    ∀ k, (samples k xs).length = choose xs.length k := by
  induction xs with
  | nil =>
      intro k
      cases k <;> simp [samples, choose]
  | cons x xs ih =>
      intro k
      cases k with
      | zero => simp [samples, choose]
      | succ k =>
          simp only [samples, List.length_append, List.length_map, ih, List.length_cons]
          rw [show choose (xs.length + 1) (k + 1) =
            choose xs.length k + choose xs.length (k + 1) by
              simp [choose]]
          omega

private theorem length_sums (xs : List Int) (k : Nat) :
    (sums xs k).length = choose xs.length k := by
  simp [sums, length_samples]

private theorem sums_cons (x : Int) (xs : List Int) (k : Nat) :
    sums (x :: xs) (k + 1) = sums xs (k + 1) ++ (sums xs k).map (x + ·) := by
  simp [sums, samples, List.map_append, List.map_map, Function.comp_def]

private theorem sum_map_add (x : Int) (ys : List Int) :
    (ys.map (x + ·)).sum = ys.sum + (ys.length : Int) * x := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      rw [show ((ys.length + 1 : Nat) : Int) = (ys.length : Int) + 1 by omega]
      rw [Int.add_mul]
      simp
      omega

/-- Key lemma: for every `k`, the total of all `k+1`-sample sums equals
`C(n-1, k) * xs.sum`. The `k = 0` and `n = 0` boundary cases are handled
separately; the induction step is Pascal's identity plus distributivity. -/
private theorem mean_aux (xs : List Int) : ∀ k,
    (sums xs (k + 1)).sum = (choose (xs.length - 1) k : Int) * xs.sum := by
  induction xs with
  | nil =>
      intro k
      simp [sums, samples]
  | cons x xs ih =>
      intro k
      cases k with
      | zero =>
          have h0 := ih 0
          simp [choose] at h0
          change (sums (x :: xs) 1).sum =
            (choose ((x :: xs).length - 1) 0 : Int) * (x :: xs).sum
          rw [sums_cons]
          rw [List.sum_append]
          rw [h0]
          simp [sums, samples, choose, Int.add_comm]
      | succ j =>
          cases xs with
          | nil => simp [sums, samples, choose]
          | cons y ys =>
              rw [sums_cons]
              rw [List.sum_append]
              rw [sum_map_add]
              rw [ih (j + 1)]
              rw [ih j]
              rw [length_sums]
              simp only [List.length_cons, List.sum_cons, Nat.add_sub_cancel_right]
              rw [show choose (ys.length + 1) (j + 1) =
                choose ys.length j + choose ys.length (j + 1) by
                  simp [choose]]
              rw [show ((choose ys.length j + choose ys.length (j + 1) : Nat) : Int) =
                  (choose ys.length j : Int) + (choose ys.length (j + 1) : Int) by omega]
              simp only [Int.mul_add, Int.add_mul]
              omega

theorem sound_Samples_SampleMean : Samples.SampleMean.Goal := by
  intro xs k hk hkn
  obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (by omega : k ≠ 0)
  subst k
  exact mean_aux xs j

end TinylambOpt.ResearchProofs.Atlas.Goal_Samples_SampleMean
