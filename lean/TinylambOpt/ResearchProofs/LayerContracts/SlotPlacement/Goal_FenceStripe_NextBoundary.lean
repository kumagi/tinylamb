import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_NextBoundary

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

theorem fixF : ∀ r i, FenceStripe.model.legal r i = true → FenceStripe.model.snap r i = i := by
  intro r i h
  simp [FenceStripe.model] at h
  simp [FenceStripe.model, h]

theorem upF : UpSpec FenceStripe.model := by
  intro r i
  by_cases hleg : i % (r.stripe+1) + r.bytes ≤ r.stripe+1
  · simp [FenceStripe.model, hleg]
  · simp [FenceStripe.model, hleg]
    have hdecomp : (i / (r.stripe+1)) * (r.stripe+1) + i % (r.stripe+1) = i := by
      rw [← Nat.mul_comm]
      exact Nat.div_add_mod i (r.stripe+1)
    have hmodlt : i % (r.stripe+1) < r.stripe+1 := Nat.mod_lt _ (by omega)
    rw [Nat.succ_mul]
    omega

theorem beforeF : BeforeLegalSpec FenceStripe.model := by
  intro r i j hij hlegBool
  have hleg : j % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
    simpa [FenceStripe.model] using hlegBool
  by_cases hlegi : i % (r.stripe+1) + r.bytes ≤ r.stripe+1
  · simpa [FenceStripe.model, hlegi] using hij
  · simp [FenceStripe.model, hlegi]
    have hidec : (i / (r.stripe+1)) * (r.stripe+1) + i % (r.stripe+1) = i := by
      rw [← Nat.mul_comm]
      exact Nat.div_add_mod i (r.stripe+1)
    have hjdec : (j / (r.stripe+1)) * (r.stripe+1) + j % (r.stripe+1) = j := by
      rw [← Nat.mul_comm]
      exact Nat.div_add_mod j (r.stripe+1)
    have hdivle : i / (r.stripe+1) ≤ j / (r.stripe+1) := Nat.div_le_div_right hij
    by_cases hlt : j < (i / (r.stripe+1) + 1) * (r.stripe+1)
    · have hdivlt : j / (r.stripe+1) < i / (r.stripe+1) + 1 :=
        (Nat.div_lt_iff_lt_mul (by omega : 0 < r.stripe+1)).2 hlt
      have hdivge : j / (r.stripe+1) = i / (r.stripe+1) := by omega
      have heqmul : (j / (r.stripe+1)) * (r.stripe+1) = (i / (r.stripe+1)) * (r.stripe+1) := by rw [hdivge]
      omega
    · have hsucc : (i / (r.stripe+1) + 1) * (r.stripe+1) =
          (i / (r.stripe+1)) * (r.stripe+1) + (r.stripe+1) := by rw [Nat.succ_mul]
      omega

theorem snapLegalF : SnapLegalSpec FenceStripe.model := by
  intro r i j hij hlegBool
  have hleg : j % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
    simpa [FenceStripe.model] using hlegBool
  have hbytesle : r.bytes ≤ r.stripe+1 := by omega
  by_cases hlegi : i % (r.stripe+1) + r.bytes ≤ r.stripe+1
  · simp [FenceStripe.model, hlegi]
  · simp [FenceStripe.model, hlegi, hbytesle]

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.NextBoundary.Goal := by
  exact nextBoundary_of FenceStripe.model upF beforeF fixF snapLegalF

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_NextBoundary
