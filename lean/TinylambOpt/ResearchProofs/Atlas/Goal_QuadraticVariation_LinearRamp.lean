import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_LinearRamp

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: all n ≤ 8 and a, d ∈ [-4, 4].
    `false` means no counterexample was found. -/
#eval do
  let vals : List Int := [-4, -3, -2, -1, 0, 1, 2, 3, 4]
  let bad := (List.range 9).any (fun n =>
    vals.any (fun a => vals.any (fun d =>
      decide (QuadraticVariation.measure ((List.range n).map (fun (i : Nat) => a + d * i)) !=
        ((n - 1 : Nat) : Int) * d^2))))
  pure bad

private theorem lastD_cons_of_ne_nil (a : Int) {xs : Series} (hxs : xs ≠ []) :
    (a :: xs).getLast?.getD 0 = xs.getLast?.getD 0 := by
  rw [List.getLast?_cons_of_ne_nil hxs]

private theorem delta_append_singleton (x : Series) (z : Int) (hx : x ≠ []) :
    delta (x ++ [z]) = delta x ++ [z - x.getLast?.getD 0] := by
  induction x generalizing z with
  | nil => contradiction
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b ys =>
          have hne : b :: ys ≠ ([] : Series) := by simp
          have hih := ih z hne
          simp [delta, adj] at hih ⊢
          rw [hih]

private theorem measure_append_singleton (x : Series) (z : Int) (hx : x ≠ []) :
    QuadraticVariation.measure (x ++ [z]) =
      QuadraticVariation.measure x + (z - x.getLast?.getD 0)^2 := by
  unfold QuadraticVariation.measure
  rw [delta_append_singleton x z hx]
  rw [List.map_append, List.sum_append]
  simp

private theorem lastD_map_range (k : Nat) (a d : Int) :
    ((List.range (k + 1)).map (fun i : Nat => a + d * i)).getLast?.getD 0 =
      a + d * (k : Int) := by
  rw [List.range_succ, List.map_append, List.getLast?_append]
  simp

theorem sound_QuadraticVariation_LinearRamp : QuadraticVariation.LinearRamp.Goal := by
  intro n a d
  induction n with
  | zero => simp [QuadraticVariation.measure, delta, adj]
  | succ n ih =>
      cases n with
      | zero => simp [QuadraticVariation.measure, delta, adj]
      | succ k =>
          rw [List.range_succ, List.map_append]
          simp only [List.map_cons, List.map_nil]
          have hne : ((List.range (k + 1)).map (fun i : Nat => a + d * i)) ≠ ([] : Series) := by
            simp
          rw [measure_append_singleton _ _ hne]
          rw [ih, lastD_map_range]
          have hstep : (a + d * ((Nat.succ k : Nat) : Int)) - (a + d * (k : Int)) = d := by
            push_cast
            rw [Int.mul_add, Int.mul_one]
            omega
          rw [hstep]
          simp only [Nat.add_sub_cancel_right]
          push_cast
          rw [Int.add_mul, Int.one_mul]

end TinylambOpt.ResearchProofs.Atlas.Goal_QuadraticVariation_LinearRamp
