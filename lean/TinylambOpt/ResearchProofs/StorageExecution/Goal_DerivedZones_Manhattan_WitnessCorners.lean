import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_WitnessCorners

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem gap_piecewise (x t : Nat) : gap x t = if x ≤ t then t - x else x - t := by
  unfold gap
  by_cases h : x ≤ t
  · simp [h]
  · simp [h, Nat.le_of_not_ge h]

private theorem max_add_right (a b c : Nat) : max (a + c) (b + c) = max a b + c := by
  by_cases h : a ≤ b
  · rw [Nat.max_eq_right (by omega : a + c ≤ b + c), Nat.max_eq_right h]
  · have h' : b ≤ a := by omega
    rw [Nat.max_eq_left (by omega : b + c ≤ a + c), Nat.max_eq_left h']

private theorem max_add_left (a b c : Nat) : max (a + b) (a + c) = a + max b c := by
  by_cases h : b ≤ c
  · rw [Nat.max_eq_right (by omega : a + b ≤ a + c), Nat.max_eq_right h]
  · have h' : c ≤ b := by omega
    rw [Nat.max_eq_left (by omega : a + c ≤ a + b), Nat.max_eq_left h']

private theorem endpoint_gap (lo hi t : Nat) (hlohi : lo ≤ hi) :
    max (hi - t) (t - lo) = max (gap lo t) (gap hi t) := by
  rw [gap_piecewise lo t, gap_piecewise hi t]
  by_cases hlo : lo ≤ t
  · by_cases hhi : hi ≤ t
    · simp [hlo, hhi]
      rw [Nat.max_eq_left (by omega : t - hi ≤ t - lo)]
    · simp [hlo, hhi]
      exact Nat.max_comm _ _
  · have hlo' : t ≤ lo := Nat.le_of_not_ge hlo
    by_cases hhi : hi ≤ t
    · omega
    · have hhi' : t ≤ hi := Nat.le_of_not_ge hhi
      simp [hlo, hhi]
      rw [Nat.max_eq_left (by omega : t - lo ≤ hi - t)]
      rw [Nat.max_eq_right (by omega : lo - t ≤ hi - t)]

theorem sound_WitnessCorners : Manhattan.WitnessCorners.Goal := by
  rintro t ⟨alo, ahi⟩ ⟨blo, bhi⟩ hala hblb
  change alo ≤ ahi at hala
  change blo ≤ bhi at hblb
  change max (ahi - t) (t - alo) + max (bhi - t) (t - blo) =
    peak ([alo, ahi].flatMap (fun x => [blo, bhi].map (fun y => gap x t + gap y t)))
  rw [endpoint_gap alo ahi t hala, endpoint_gap blo bhi t hblb]
  simp [peak]
  rw [← Nat.max_assoc]
  rw [max_add_left (gap alo t) (gap blo t) (gap bhi t)]
  rw [max_add_right (gap alo t) (gap ahi t) (max (gap blo t) (gap bhi t))]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_WitnessCorners
