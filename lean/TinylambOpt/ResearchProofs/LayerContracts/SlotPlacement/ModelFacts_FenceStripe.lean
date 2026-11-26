import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts.FenceStripeFacts

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

theorem fix : ∀ r i, FenceStripe.model.legal r i = true → FenceStripe.model.snap r i = i := by
  intro r i h
  simp [FenceStripe.model] at h
  simp [FenceStripe.model, h]

theorem up : UpSpec FenceStripe.model := by
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

theorem before : BeforeLegalSpec FenceStripe.model := by
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

theorem snapLegal : SnapLegalSpec FenceStripe.model := by
  intro r i j hij hlegBool
  have hleg : j % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
    simpa [FenceStripe.model] using hlegBool
  have hbytesle : r.bytes ≤ r.stripe+1 := by omega
  by_cases hlegi : i % (r.stripe+1) + r.bytes ≤ r.stripe+1
  · simp [FenceStripe.model, hlegi]
  · simp [FenceStripe.model, hlegi, hbytesle]

theorem fpSub : FpSubSpec FenceStripe.model := by
  intro r n i hn x hx
  apply band_subset hn x hx

theorem legalMono : LegalMonoSpec FenceStripe.model := by
  intro r n i hn hleg
  simp [FenceStripe.model] at hleg ⊢
  omega

def prev : Req → Nat → Nat :=
  fun r i => if FenceStripe.model.legal r (i-1) = true then i-1 else i - r.bytes

/-! Arithmetic for the case where the byte before the first fit is illegal.
    Write `W = stripe+1`, `q = i/W`, and `i = W*q` (the cursor sits on a fence).
    The previous legal fence is `i - bytes`; its footprint ends at `i-1`. -/

theorem prevSpec : PrevSpec FenceStripe.model prev := by
  intro r cap u i hipos hfit hs0
  have hicap : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have hlegi : i % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
    have h := hfit
    simp [fits, FenceStripe.model] at h
    exact h.1.2
  have hbytesle : r.bytes ≤ r.stripe+1 := by omega
  by_cases hlegpBool : FenceStripe.model.legal r (i-1) = true
  · have hlegp : (i-1) % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
      simpa [FenceStripe.model] using hlegpBool
    dsimp [prev]
    simp [hlegpBool]
    refine ⟨by omega, ?_, ?_, ?_⟩
    · intro x hx
      simp [FenceStripe.model, band] at hx
      omega
    · intro x hx
      simp [FenceStripe.model, band] at hx
      rcases hx with ⟨k, hk, rfl⟩
      by_cases hk0 : k = 0
      · left
        omega
      · right
        simp [FenceStripe.model, band]
        refine ⟨k-1, ?_, by omega⟩
        omega
    · intro x hjx hxlt
      have hx : x = i - 1 := by omega
      subst x
      have hsnapi : FenceStripe.model.snap r i = i := by simp [FenceStripe.model, hlegi]
      rw [show i - 1 + 1 = i by omega]
      exact hsnapi
  · have hbytespos : 0 < r.bytes := by
      by_cases hb : r.bytes = 0
      · have hmodlt : (i-1) % (r.stripe+1) < r.stripe+1 := Nat.mod_lt _ (by omega)
        have hlegp' : FenceStripe.model.legal r (i-1) = true := by
          simp [FenceStripe.model, hb, Nat.le_of_lt hmodlt]
        exact (hlegpBool hlegp').elim
      · omega
    have hlegpNot : ¬ (i-1) % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
      simpa [FenceStripe.model] using hlegpBool
    have himod0 : i % (r.stripe+1) = 0 := by
      by_cases hm : i % (r.stripe+1) = 0
      · exact hm
      · have himodpos : 0 < i % (r.stripe+1) := Nat.pos_of_ne_zero hm
        have hqdec : (r.stripe+1) * (i/(r.stripe+1)) + i % (r.stripe+1) = i := Nat.div_add_mod i (r.stripe+1)
        have hqdec' : (i/(r.stripe+1)) * (r.stripe+1) + i % (r.stripe+1) = i := by
          rw [Nat.mul_comm]
          exact hqdec
        have hdivp : (i-1) / (r.stripe+1) = i / (r.stripe+1) := by
          apply Nat.div_eq_of_lt_le
          · omega
          · rw [Nat.succ_mul]
            have hmodlt : i % (r.stripe+1) < r.stripe+1 := Nat.mod_lt _ (by omega)
            omega
        have hmodp : (i-1) % (r.stripe+1) = i % (r.stripe+1) - 1 := by
          rw [Nat.mod_eq_sub (i-1) (r.stripe+1), Nat.mod_eq_sub i (r.stripe+1), hdivp]
          omega
        have hltmod : i % (r.stripe+1) - 1 + r.bytes < i % (r.stripe+1) + r.bytes :=
          Nat.add_lt_add_right (Nat.sub_lt himodpos (by omega)) r.bytes
        have hlegp' : (i-1) % (r.stripe+1) + r.bytes ≤ r.stripe+1 := by
          rw [hmodp]
          exact Nat.le_trans (Nat.le_of_lt hltmod) hlegi
        exact (hlegpNot hlegp').elim
    have hdecomp : (r.stripe+1) * (i / (r.stripe+1)) + i % (r.stripe+1) = i := Nat.div_add_mod i (r.stripe+1)
    rw [himod0] at hdecomp
    have hqpos : 0 < i / (r.stripe+1) := by
      by_cases hq : i / (r.stripe+1) = 0
      · rw [hq] at hdecomp
        simp at hdecomp
        omega
      · exact Nat.pos_of_ne_zero hq
    have hWq : (r.stripe+1) * (i / (r.stripe+1)) = i := by omega
    have hqW : (i / (r.stripe+1)) * (r.stripe+1) = i := by
      rw [Nat.mul_comm]
      exact hWq
    have hq1 : 1 ≤ i / (r.stripe+1) := Nat.succ_le_of_lt hqpos
    have hWle_mul : r.stripe+1 ≤ (i/(r.stripe+1)) * (r.stripe+1) := by
      simpa using Nat.mul_le_mul_right (r.stripe+1) hq1
    have hWle : r.stripe+1 ≤ i := by
      rw [← hqW]
      exact hWle_mul
    have hqm1 : (i / (r.stripe+1) - 1) * (r.stripe+1) = i - (r.stripe+1) := by
      rw [Nat.sub_mul, Nat.one_mul, hqW]
    have hWqm1 : (r.stripe+1) * (i / (r.stripe+1) - 1) = i - (r.stripe+1) := by
      rw [Nat.mul_comm]
      exact hqm1
    dsimp [prev]
    simp [hlegpBool]
    refine ⟨by omega, ?_, ?_, ?_, ?_⟩
    · have hdivj : (i - r.bytes) / (r.stripe+1) = i/(r.stripe+1) - 1 := by
        apply Nat.div_eq_of_lt_le
        · rw [hqm1]
          exact Nat.sub_le_sub_left hbytesle i
        · have hkq : i / (r.stripe+1) - 1 + 1 = i / (r.stripe+1) := by omega
          rw [hkq, hqW]
          omega
      have hmodj : (i - r.bytes) % (r.stripe+1) =
          (i - r.bytes) - (r.stripe+1) * ((i-r.bytes)/(r.stripe+1)) := Nat.mod_eq_sub _ _
      rw [hdivj, hWqm1] at hmodj
      have hsum : r.bytes + (i - (r.stripe+1)) = i - ((r.stripe+1) - r.bytes) := by omega
      have hsub : i - r.bytes - (i - (r.stripe+1)) = (r.stripe+1) - r.bytes := by
        rw [Nat.sub_sub, hsum]
        exact Nat.sub_sub_self (by omega : (r.stripe+1) - r.bytes ≤ i)
      simp [FenceStripe.model]
      rw [hmodj, hsub]
      omega
    · intro x hx
      simp [FenceStripe.model, band] at hx
      omega
    · intro x hx
      simp [FenceStripe.model, band] at hx
      rcases hx with ⟨k, hk, rfl⟩
      left
      omega
    · intro x hjx hxlt
      by_cases hxi : x + 1 = i
      · rw [hxi]
        simp [FenceStripe.model, hlegi]
      · let y := x + 1
        have hylo : i - r.bytes + 1 ≤ y := by omega
        have hylt : y < i := by omega
        have hdivy : y / (r.stripe+1) = i/(r.stripe+1) - 1 := by
          apply Nat.div_eq_of_lt_le
          · rw [hqm1]
            omega
          · have hkq : i / (r.stripe+1) - 1 + 1 = i / (r.stripe+1) := by omega
            rw [hkq, hqW]
            exact hylt
        have hmody : y % (r.stripe+1) =
            y - (r.stripe+1) * (y/(r.stripe+1)) := Nat.mod_eq_sub _ _
        rw [hdivy, hWqm1] at hmody
        have hden : ((r.stripe+1) - r.bytes + 1) + (i - (r.stripe+1)) ≤ y := by omega
        have hle : (r.stripe+1) - r.bytes + 1 ≤ y - (i - (r.stripe+1)) := Nat.le_sub_of_add_le hden
        have hsum : r.stripe+1 < y % (r.stripe+1) + r.bytes := by
          rw [hmody]
          omega
        change (if y % (r.stripe+1) + r.bytes ≤ r.stripe+1 then y
                else (y / (r.stripe+1) + 1) * (r.stripe+1)) = i
        rw [if_neg (by omega : ¬ y % (r.stripe+1) + r.bytes ≤ r.stripe+1)]
        rw [hdivy]
        have hkq : i / (r.stripe+1) - 1 + 1 = i / (r.stripe+1) := by omega
        rw [hkq, hqW]

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts.FenceStripeFacts
