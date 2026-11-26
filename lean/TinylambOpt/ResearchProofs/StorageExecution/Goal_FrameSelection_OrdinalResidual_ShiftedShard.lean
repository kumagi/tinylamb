import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `OrdinalResidual.ShiftedShard` — `∀ (a b : List Nat), bitWidth (a ++ b.map
  (·+a.length)) = max (bitWidth a) (bitWidth b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_ShiftedShard

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

-- Residual with an explicit starting row ordinal.
private def residualsAt (k : Nat) (xs : List Nat) : List Nat :=
  (xs.zipIdx k).map (fun (v, i) => zig ((v : Int) - (i : Nat)))

private theorem residuals_eq (xs : List Nat) : residuals xs = residualsAt 0 xs := by
  rfl

private theorem residualsAt_cons (k x : Nat) (xs : List Nat) :
    residualsAt k (x :: xs) = zig ((x : Int) - (k : Nat)) :: residualsAt (k + 1) xs := by
  rfl

private theorem residualsAt_append (k : Nat) (a b : List Nat) :
    residualsAt k (a ++ b) = residualsAt k a ++ residualsAt (k + a.length) b := by
  unfold residualsAt
  rw [List.zipIdx_append, List.map_append]

-- Shifting every value by s while the row ordinals start at k ≥ s leaves the
-- residuals unchanged (absolute residuals at offset k - s).
private theorem residualsAt_map_add (b : List Nat) (k s : Nat) (h : s ≤ k) :
    residualsAt k (b.map (fun v => v + s)) = residualsAt (k - s) b := by
  induction b generalizing k with
  | nil => simp [residualsAt]
  | cons x xs ih =>
      have hk1 : s ≤ k + 1 := by omega
      have hadd : (k + 1) - s = (k - s) + 1 := by omega
      have hhead : zig (((x + s : Nat) : Int) - (k : Nat)) =
          zig ((x : Int) - ((k - s : Nat) : Int)) := by
        congr 1
        omega
      rw [List.map_cons]
      rw [residualsAt_cons]
      rw [residualsAt_cons]
      rw [hhead]
      congr 1
      simpa [hadd] using ih (k + 1) hk1

private theorem residuals_append_shift (k : Nat) (a b : List Nat) :
    residualsAt k (a ++ b.map (fun v => v + (k + a.length))) =
      residualsAt k a ++ residualsAt 0 b := by
  induction a generalizing k with
  | nil =>
      simpa [residualsAt] using residualsAt_map_add b k k (by omega)
  | cons x xs ih =>
      have hshift : k + (x :: xs).length = (k + 1) + xs.length := by
        simp [Nat.add_comm, Nat.add_left_comm]
      rw [hshift, List.cons_append, residualsAt_cons, residualsAt_cons]
      rw [ih (k + 1)]
      simp [List.cons_append]

private theorem peak_cons (x : Nat) (xs : List Nat) : peak (x :: xs) = max x (peak xs) := by
  unfold peak
  cases xs with
  | nil => simp
  | cons y ys => simp [List.max?_cons]

private theorem peak_append (xs ys : List Nat) : peak (xs ++ ys) = max (peak xs) (peak ys) := by
  induction xs with
  | nil => simp [peak]
  | cons x xs ih =>
      rw [List.cons_append, peak_cons, ih, peak_cons]
      rw [Nat.max_assoc]

private theorem log2_mono {a b : Nat} (h : a ≤ b) : a.log2 ≤ b.log2 := by
  by_cases ha : a = 0
  · subst a
    simp
  · have hb : b ≠ 0 := by omega
    by_cases hle : a.log2 ≤ b.log2
    · exact hle
    · exfalso
      have hlt' : b.log2 < a.log2 := by omega
      have hb_lt : b < 2 ^ a.log2 := (Nat.log2_lt hb).mp hlt'
      have ha_le : 2 ^ a.log2 ≤ a := (Nat.log2_eq_iff ha).mp rfl |>.1
      omega

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · simp [width, ha]
    by_cases hb : b = 0
    · subst b
      omega
    · simp [hb]
      have := log2_mono h
      omega

private theorem width_max (a b : Nat) : width (max a b) = max (width a) (width b) := by
  by_cases h : a ≤ b
  · rw [Nat.max_eq_right h]
    exact (Nat.max_eq_right (width_mono h)).symm
  · have hba : b ≤ a := by omega
    rw [Nat.max_eq_left hba]
    exact (Nat.max_eq_left (width_mono hba)).symm

theorem sound_ShiftedShard : OrdinalResidual.ShiftedShard.Goal := by
  intro a b
  unfold bitWidth
  have hres : residuals (a ++ b.map (fun v => v + a.length)) = residuals a ++ residuals b := by
    simpa [residuals_eq] using residuals_append_shift 0 a b
  rw [hres, peak_append]
  exact width_max (peak (residuals a)) (peak (residuals b))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_ShiftedShard
