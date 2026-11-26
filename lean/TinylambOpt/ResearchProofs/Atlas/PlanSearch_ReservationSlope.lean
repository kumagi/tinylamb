import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `RiskEnvelope.ReservationSlope` — `∀ xs z a b, (loss xs (z+1) a b:Int) - loss xs z a
  b = (b:Int)*mass xs (· ≤ z) - (a:Int)*mass xs (· > z)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReservationSlope

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

private theorem cast_add_sub (P Q R S : Nat) :
    ((P + Q : Nat) : Int) - ((R + S : Nat) : Int) =
      ((P : Int) - (R : Int)) + ((Q : Int) - (S : Int)) := by
  omega

private theorem step_diff (x z : Int) (a b w : Nat) :
    ((w * (a * (x - (z + 1)).toNat + b * ((z + 1) - x).toNat) : Nat) : Int) -
      ((w * (a * (x - z).toNat + b * (z - x).toNat) : Nat) : Int) =
        if x ≤ z then ((b * w : Nat) : Int) else -((a * w : Nat) : Int) := by
  by_cases h : x ≤ z
  · simp only [h, ↓reduceIte]
    have hx1 : x - z ≤ 0 := by omega
    have hx2 : x - (z + 1) ≤ 0 := by omega
    have hz1 : 0 ≤ z - x := by omega
    have hz2 : 0 ≤ z + 1 - x := by omega
    rw [Int.toNat_of_nonpos hx1, Int.toNat_of_nonpos hx2]
    push_cast
    rw [Int.toNat_of_nonneg hz1, Int.toNat_of_nonneg hz2]
    grind
  · simp only [h, ↓reduceIte]
    have hx1 : 0 ≤ x - z := by omega
    have hx2 : 0 ≤ x - (z + 1) := by omega
    have hz1 : z - x ≤ 0 := by omega
    have hz2 : z + 1 - x ≤ 0 := by omega
    rw [Int.toNat_of_nonpos hz1, Int.toNat_of_nonpos hz2]
    push_cast
    rw [Int.toNat_of_nonneg hx1, Int.toNat_of_nonneg hx2]
    grind

theorem sound_ReservationSlope : RiskEnvelope.ReservationSlope.Goal := by
  intro xs z a b
  induction xs with
  | nil =>
      simp [RiskEnvelope.loss, RiskEnvelope.mass]
  | cons e xs ih =>
      rcases e with ⟨x, w⟩
      change
        (((w * (a * (x - (z + 1)).toNat + b * ((z + 1) - x).toNat)) +
              RiskEnvelope.loss xs (z + 1) a b : Nat) : Int) -
            (((w * (a * (x - z).toNat + b * (z - x).toNat)) +
              RiskEnvelope.loss xs z a b : Nat) : Int) =
          (b : Int) * RiskEnvelope.mass ((x, w) :: xs) (fun y => decide (y ≤ z)) -
            (a : Int) * RiskEnvelope.mass ((x, w) :: xs) (fun y => decide (y > z))
      rw [cast_add_sub]
      rw [step_diff, ih]
      by_cases hle : x ≤ z
      · have hgt : ¬ x > z := by omega
        simp [RiskEnvelope.mass, hle, hgt]
        grind
      · have hgt : x > z := by omega
        simp [RiskEnvelope.mass, hle, hgt]
        grind

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReservationSlope
