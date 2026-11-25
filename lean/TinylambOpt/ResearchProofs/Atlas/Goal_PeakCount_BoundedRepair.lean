import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundaryPeaks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundedRepair

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount
open TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundaryPeaks

-- Exhaustive sanity check: x, y of length <= 2 over [-2,2], a, b in [-2,2].
private def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun i =>
      (vecs n).map (fun xs => (Int.ofNat i - 2) :: xs))

#eval (vecs 2).all (fun x => (vecs 2).all (fun y =>
  (vecs 1).all (fun as => (vecs 1).all (fun bs =>
    match as, bs with
    | [a], [b] => decide ((M (x ++ a :: y) - M (x ++ b :: y)).natAbs ≤ 2)
    | _, _ => false))))

-- `M (p :: u) = M u + ind p u`: peeling one row leaves only the peak test
-- on the first three rows of `p :: u`, which is 0 or 1.
private def ind (p : Int) (u : List Int) : Int := M ([p] ++ u.take 2)

private theorem ind_cons (p : Int) (u : List Int) : M (p :: u) = M u + ind p u := by
  exact M_cons_one p u

private theorem M_nonneg (u : List Int) : 0 ≤ M u := by
  simp [M, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.measure]

private theorem ind_bounds (p : Int) (u : List Int) : 0 ≤ ind p u ∧ ind p u ≤ 1 := by
  constructor
  · exact M_nonneg _
  · unfold ind
    cases u with
    | nil => simp [M_one]
    | cons r rs =>
        cases rs with
        | nil => simp [M_two]
        | cons s ss =>
            change M [p, r, s] ≤ 1
            rw [measure_cons3 p r s []]
            simp [M_two]
            split <;> omega

theorem sound_PeakCount_BoundedRepair : BoundedRepair.Goal := by
  intro x y a b
  induction x generalizing y a b with
  | nil =>
      change (M (a :: y) - M (b :: y)).natAbs ≤ 2
      rw [ind_cons a y, ind_cons b y]
      rcases ind_bounds a y with ⟨ha0, ha1⟩
      rcases ind_bounds b y with ⟨hb0, hb1⟩
      omega
  | cons p xs ih =>
      cases xs with
      | nil =>
          change (M (p :: a :: y) - M (p :: b :: y)).natAbs ≤ 2
          rw [ind_cons p (a :: y), ind_cons p (b :: y), ind_cons a y, ind_cons b y]
          rcases ind_bounds p (a :: y) with ⟨ha0, ha1⟩
          rcases ind_bounds p (b :: y) with ⟨hb0, hb1⟩
          rcases ind_bounds a y with ⟨ha0', ha1'⟩
          rcases ind_bounds b y with ⟨hb0', hb1'⟩
          omega
      | cons q xs' =>
          cases xs' with
          | nil =>
              change (M (p :: q :: a :: y) - M (p :: q :: b :: y)).natAbs ≤ 2
              cases y with
              | nil =>
                  rw [measure_cons3 p q a [], measure_cons3 p q b []]
                  simp only [M, M_two] at *
                  split <;> split <;> omega
              | cons r ys =>
                  cases ys with
                  | nil =>
                      rw [measure_cons3 p q a [r], measure_cons3 p q b [r],
                        measure_cons3 q a r [], measure_cons3 q b r []]
                      simp only [M, M_two] at *
                      split <;> split <;> split <;> split <;> omega
                  | cons s zs =>
                      rw [measure_cons3 p q a (r :: s :: zs),
                        measure_cons3 p q b (r :: s :: zs),
                        measure_cons3 q a r (s :: zs),
                        measure_cons3 q b r (s :: zs),
                        measure_cons3 a r s zs,
                        measure_cons3 b r s zs]
                      simp only [M] at *
                      split <;> split <;> split <;> split <;> split <;> split <;>
                        (simp only [Bool.and_eq_true, decide_eq_true_eq] at *; omega)
          | cons r xs'' =>
              -- With at least three rows before the edited one, the two
              -- lists have the same first three rows, so the peeled peak
              -- test is identical and the induction hypothesis applies.
              change (M (p :: ((q :: r :: xs'') ++ a :: y)) -
                M (p :: ((q :: r :: xs'') ++ b :: y))).natAbs ≤ 2
              rw [ind_cons p ((q :: r :: xs'') ++ a :: y),
                ind_cons p ((q :: r :: xs'') ++ b :: y)]
              have hind : ind p ((q :: r :: xs'') ++ a :: y) =
                  ind p ((q :: r :: xs'') ++ b :: y) := by
                simp [ind]
              rw [hind]
              have hih := ih y a b
              simp only [M] at hih ⊢
              omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundedRepair
