import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundaryPeaks

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PeakCount.AdjacentCap` — `∀ x, measure x ≤ ((x.length-1)/2:Nat)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_AdjacentCap

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount

abbrev M := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.measure

-- Exhaustive sanity check: every list of length <= 6 over [-1,0,1]
-- respects the cap.
private def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 3).flatMap (fun i =>
      (vecs n).map (fun xs => (Int.ofNat i - 1) :: xs))

#eval! (List.range 7).all (fun n =>
  (vecs n).all (fun x =>
    decide (M x ≤ (((x.length - 1) / 2 : Nat) : Int))))

-- Whether position 1 is a strict peak (only possible from length 3 on).
private def peak1 : Series → Bool
  | [] => false
  | [_] => false
  | [_, _] => false
  | a :: b :: c :: _ => decide (a < b && c < b)

-- The boundary-peak file proves the exact three-term recurrence for `M`.
private theorem m_cons3_peak (a b c : Int) (xs : Series) :
    M (a :: b :: c :: xs) = (if peak1 (a :: b :: c :: xs) then 1 else 0) +
      M (b :: c :: xs) := by
  by_cases h : a < b && c < b
  · simp [peak1, h, Goal_PeakCount_BoundaryPeaks.measure_cons3]
  · simp [peak1, h, Goal_PeakCount_BoundaryPeaks.measure_cons3]

-- Stronger induction invariant: the cap, plus an even tighter cap when
-- position 1 is not a peak.  Adjacent positions cannot both be peaks, so
-- the tighter cap is exactly what is needed when a peak sits at position 1.
private theorem cap_pair (x : Series) :
    M x ≤ (((x.length - 1) / 2 : Nat) : Int) ∧
      (peak1 x = false → M x ≤ (((x.length - 2) / 2 : Nat) : Int)) := by
  induction x with
  | nil =>
      constructor
      · simp [Goal_PeakCount_BoundaryPeaks.M_nil]
      · intro h
        simp [Goal_PeakCount_BoundaryPeaks.M_nil]
  | cons a xs ih =>
      cases xs with
      | nil =>
          constructor
          · simp [Goal_PeakCount_BoundaryPeaks.M_one]
          · intro h
            simp [Goal_PeakCount_BoundaryPeaks.M_one]
      | cons b ys =>
          rcases ih with ⟨ihP, ihQ⟩
          constructor
          · by_cases hp : peak1 (a :: b :: ys) = true
            · cases ys with
              | nil => simp [peak1] at hp
              | cons c ys' =>
                  cases ys' with
                  | nil =>
                      have hpeak : peak1 [a, b, c] = true := by simpa using hp
                      rw [m_cons3_peak, if_pos hpeak]
                      simp [Goal_PeakCount_BoundaryPeaks.M_two]
                  | cons d ds =>
                      have hpeaktail : peak1 (b :: c :: d :: ds) = false := by
                        simp [peak1] at hp ⊢
                        omega
                      have hdecomp := m_cons3_peak a b c (d :: ds)
                      rw [hdecomp, if_pos hp]
                      have hq := ihQ hpeaktail
                      have hq' : M (b :: c :: d :: ds) ≤
                          (((ds.length + 1) / 2 : Nat) : Int) := by
                        simpa using hq
                      have htarget : ((((a :: b :: c :: d :: ds).length - 1) / 2 : Nat) : Int) =
                          (((ds.length + 3) / 2 : Nat) : Int) := by
                        simp
                      rw [htarget]
                      calc
                        1 + M (b :: c :: d :: ds) ≤
                            1 + (((ds.length + 1) / 2 : Nat) : Int) := by omega
                        _ = ((((ds.length + 1) / 2 + 1 : Nat) : Int)) := by
                          rw [Int.natCast_add, Int.natCast_one]
                          omega
                        _ ≤ (((ds.length + 3) / 2 : Nat) : Int) := by
                          apply (Int.ofNat_le).mpr
                          omega
            · cases ys with
              | nil => simp [Goal_PeakCount_BoundaryPeaks.M_two]
              | cons c ys' =>
                  cases ys' with
                  | nil =>
                      rw [m_cons3_peak, if_neg hp]
                      simp [Goal_PeakCount_BoundaryPeaks.M_two]
                  | cons d ds =>
                      have hdecomp := m_cons3_peak a b c (d :: ds)
                      rw [hdecomp, if_neg hp]
                      have hpP := ihP
                      have hpP' : M (b :: c :: d :: ds) ≤
                          (((ds.length + 2) / 2 : Nat) : Int) := by
                        simpa using hpP
                      have hle : (((ds.length + 2) / 2 : Nat) : Int) ≤
                          (((ds.length + 3) / 2 : Nat) : Int) := by
                        apply (Int.ofNat_le).mpr
                        omega
                      simpa using (by omega : M (b :: c :: d :: ds) ≤
                        (((ds.length + 3) / 2 : Nat) : Int))
          · intro hf
            cases ys with
            | nil => simp [Goal_PeakCount_BoundaryPeaks.M_two]
            | cons c ys' =>
                cases ys' with
                | nil =>
                    have hfnot : ¬ peak1 [a, b, c] = true := by simpa using hf
                    rw [m_cons3_peak, if_neg hfnot]
                    simp [Goal_PeakCount_BoundaryPeaks.M_two]
                | cons d ds =>
                    have hfnot : ¬ peak1 (a :: b :: c :: d :: ds) = true := by simpa using hf
                    have hdecomp := m_cons3_peak a b c (d :: ds)
                    rw [hdecomp, if_neg hfnot]
                    have hpP := ihP
                    have hpP' : M (b :: c :: d :: ds) ≤
                        (((ds.length + 2) / 2 : Nat) : Int) := by
                      simpa using hpP
                    simpa using hpP'

theorem sound_PeakCount_AdjacentCap : AdjacentCap.Goal := by
  intro x
  change M x ≤ (((x.length - 1) / 2 : Nat) : Int)
  exact (cap_pair x).1

end TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_AdjacentCap
