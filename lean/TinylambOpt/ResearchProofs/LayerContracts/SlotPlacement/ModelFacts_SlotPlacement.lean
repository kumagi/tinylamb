import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `PhaseAligned.OldPhase` — `∀ r, model.snap r r.old = r.old`
- `PhaseAligned.Period` — `∀ r i, model.snap r (i+r.stripe+1) = model.snap r
  i+r.stripe+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

/-! -------------------------- Contiguous -------------------------- -/

namespace ContiguousFacts

def prev : Req → Nat → Nat := fun _ i => i - 1

theorem prevSpec : PrevSpec Contiguous.model prev := by
  intro r cap u i hipos hfit hs0
  have hi : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  dsimp [prev]
  refine ⟨by omega, by simp [Contiguous.model], ?_, ?_, ?_⟩
  · intro x hx
    simp [Contiguous.model, band] at hx
    omega
  · intro x hx
    simp [Contiguous.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk0 : k = 0
    · left
      omega
    · right
      simp [Contiguous.model, band]
      refine ⟨k-1, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    simp [Contiguous.model]
    omega

theorem fix : ∀ r i, Contiguous.model.legal r i = true → Contiguous.model.snap r i = i := by
  intro r i h
  rfl

theorem up : UpSpec Contiguous.model := by intro r i; exact Nat.le_refl i

theorem before : BeforeLegalSpec Contiguous.model := by
  intro r i j hij hleg
  simpa [Contiguous.model] using hij

theorem snapLegal : SnapLegalSpec Contiguous.model := by
  intro r i j hij hleg
  simp [Contiguous.model]

theorem fpSub : FpSubSpec Contiguous.model := by
  intro r n i hn x hx
  apply band_subset hn x hx

theorem legalMono : LegalMonoSpec Contiguous.model := by
  intro r n i hn hleg
  simp [Contiguous.model]

end ContiguousFacts

/-! -------------------------- TrailerWord -------------------------- -/

namespace TrailerWordFacts

def prev : Req → Nat → Nat := fun _ i => i - 1

theorem prevSpec : PrevSpec TrailerWord.model prev := by
  intro r cap u i hipos hfit hs0
  have hi : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  dsimp [prev]
  refine ⟨by omega, by simp [TrailerWord.model], ?_, ?_, ?_⟩
  · intro x hx
    simp [TrailerWord.model, band] at hx
    omega
  · intro x hx
    simp [TrailerWord.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk0 : k = 0
    · left
      omega
    · right
      simp [TrailerWord.model, band]
      refine ⟨k-1, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    simp [TrailerWord.model]
    omega

theorem fix : ∀ r i, TrailerWord.model.legal r i = true → TrailerWord.model.snap r i = i := by
  intro r i h
  simp [TrailerWord.model]

theorem up : UpSpec TrailerWord.model := by intro r i; exact Nat.le_refl i

theorem before : BeforeLegalSpec TrailerWord.model := by
  intro r i j hij hleg
  simp [TrailerWord.model]
  exact hij

theorem snapLegal : SnapLegalSpec TrailerWord.model := by
  intro r i j hij hleg
  simp [TrailerWord.model]

theorem fpSub : FpSubSpec TrailerWord.model := by
  intro r n i hn x hx
  apply band_subset (by change n + 4 ≤ r.bytes + 4; omega) x hx

theorem legalMono : LegalMonoSpec TrailerWord.model := by
  intro r n i hn hleg
  simp [TrailerWord.model]

end TrailerWordFacts

/-! -------------------------- ReserveHeader -------------------------- -/

namespace ReserveHeaderFacts

def prev : Req → Nat → Nat := fun _ i => i - 1

theorem prevSpec : PrevSpec ReserveHeader.model prev := by
  intro r cap u i hipos hfit hs0
  have hi : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have hlegi : r.stripe ≤ i := by
    have h := hfit
    simp [fits, ReserveHeader.model] at h
    exact h.1.2
  have hs0' : r.stripe ≠ i := by
    simpa [ReserveHeader.model] using hs0
  have hgt : r.stripe < i := by omega
  dsimp [prev]
  refine ⟨by omega, ?_, ?_, ?_, ?_⟩
  · simp [ReserveHeader.model]
    omega
  · intro x hx
    simp [ReserveHeader.model, band] at hx
    omega
  · intro x hx
    simp [ReserveHeader.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk0 : k = 0
    · left
      omega
    · right
      simp [ReserveHeader.model, band]
      refine ⟨k-1, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    simp [ReserveHeader.model]
    omega

theorem fix : ∀ r i, ReserveHeader.model.legal r i = true → ReserveHeader.model.snap r i = i := by
  intro r i h
  simp [ReserveHeader.model] at h
  simp [ReserveHeader.model, Nat.max_eq_right h]

theorem up : UpSpec ReserveHeader.model := by
  intro r i
  simp [ReserveHeader.model]
  exact Nat.le_max_right r.stripe i

theorem before : BeforeLegalSpec ReserveHeader.model := by
  intro r i j hij hleg
  simp [ReserveHeader.model] at hleg ⊢
  omega

theorem snapLegal : SnapLegalSpec ReserveHeader.model := by
  intro r i j hij hleg
  simp [ReserveHeader.model]
  exact Nat.le_max_left r.stripe i

theorem fpSub : FpSubSpec ReserveHeader.model := by
  intro r n i hn x hx
  apply band_subset hn x hx

theorem legalMono : LegalMonoSpec ReserveHeader.model := by
  intro r n i hn hleg
  simp [ReserveHeader.model] at hleg ⊢
  exact hleg

end ReserveHeaderFacts

/-! -------------------------- ForwardGrowth -------------------------- -/

namespace ForwardGrowthFacts

def prev : Req → Nat → Nat := fun _ i => i - 1

theorem prevSpec : PrevSpec ForwardGrowth.model prev := by
  intro r cap u i hipos hfit hs0
  have hi : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have hlegi : r.old ≤ i := by
    have h := hfit
    simp [fits, ForwardGrowth.model] at h
    exact h.1.2
  have hs0' : r.old ≠ i := by
    simpa [ForwardGrowth.model] using hs0
  have hgt : r.old < i := by omega
  dsimp [prev]
  refine ⟨by omega, ?_, ?_, ?_, ?_⟩
  · simp [ForwardGrowth.model]
    omega
  · intro x hx
    simp [ForwardGrowth.model, band] at hx
    omega
  · intro x hx
    simp [ForwardGrowth.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk0 : k = 0
    · left
      omega
    · right
      simp [ForwardGrowth.model, band]
      refine ⟨k-1, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    simp [ForwardGrowth.model]
    omega

theorem fix : ∀ r i, ForwardGrowth.model.legal r i = true → ForwardGrowth.model.snap r i = i := by
  intro r i h
  simp [ForwardGrowth.model] at h
  simp [ForwardGrowth.model, Nat.max_eq_right h]

theorem up : UpSpec ForwardGrowth.model := by
  intro r i
  simp [ForwardGrowth.model]
  exact Nat.le_max_right r.old i

theorem before : BeforeLegalSpec ForwardGrowth.model := by
  intro r i j hij hleg
  simp [ForwardGrowth.model] at hleg ⊢
  omega

theorem snapLegal : SnapLegalSpec ForwardGrowth.model := by
  intro r i j hij hleg
  simp [ForwardGrowth.model]
  exact Nat.le_max_left r.old i

theorem fpSub : FpSubSpec ForwardGrowth.model := by
  intro r n i hn x hx
  apply band_subset hn x hx

theorem legalMono : LegalMonoSpec ForwardGrowth.model := by
  intro r n i hn hleg
  simp [ForwardGrowth.model] at hleg ⊢
  exact hleg

end ForwardGrowthFacts

/-! AlignedWordFacts -/

namespace AlignedWordFacts


open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

def prevA : Req → Nat → Nat := fun _ i => i - 8

theorem prevSpecA : PrevSpec AlignedWord.model prevA := by
  intro r cap u i hipos hfit hs0
  have hicap : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have himod : i % 8 = 0 := by
    have h := hfit
    simp [fits, AlignedWord.model] at h
    exact h.1.2
  have hdecomp : 8 * (i / 8) + i % 8 = i := Nat.div_add_mod i 8
  rw [himod] at hdecomp
  have hi_eq : i = (i / 8) * 8 := by omega
  have hkpos : 0 < i / 8 := by
    have hs0' : i ≠ 0 := by intro h; apply hs0; simp [AlignedWord.model, align, h]
    omega
  dsimp [prevA]
  refine ⟨by omega, ?_, ?_, ?_, ?_⟩
  · have hj_eq : i - 8 = (i / 8 - 1) * 8 := by omega
    simp [AlignedWord.model, hj_eq, Nat.mul_comm, Nat.mul_mod_right]
  · intro x hx
    simp [AlignedWord.model, band] at hx
    omega
  · intro x hx
    simp [AlignedWord.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk8 : k < 8
    · left
      omega
    · right
      simp [AlignedWord.model, band]
      refine ⟨k-8, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    apply align_eq_of_between i 0 8 (i/8) (x+1) (by omega) (by omega)
    · simpa using hi_eq
    · exact hkpos
    · omega
    · omega

theorem fixA : ∀ r i, AlignedWord.model.legal r i = true → AlignedWord.model.snap r i = i := by
  intro r i h
  simp [AlignedWord.model] at h
  apply align_eq_of_mod i 0 8 (by omega) (by omega) h

theorem upA : UpSpec AlignedWord.model := by
  intro r i
  apply align_ge i 0 8
  omega

theorem beforeA : BeforeLegalSpec AlignedWord.model := by
  intro r i j hij hleg
  simp [AlignedWord.model] at hleg
  change align i 0 8 ≤ j
  have hmono := align_mono i j 0 8 hij (by omega)
  have hjalign : align j 0 8 = j := align_eq_of_mod j 0 8 (by omega) (by omega) hleg
  rw [hjalign] at hmono
  exact hmono

theorem snapLegalA : SnapLegalSpec AlignedWord.model := by
  intro r i j hij hleg
  simp [AlignedWord.model]
  apply align_mod i 0 8 (by omega) (by omega)

theorem fpSubA : FpSubSpec AlignedWord.model := by
  intro r n i hn x hx
  change x ∈ band i n at hx
  change x ∈ band i r.bytes
  apply band_subset hn x hx

theorem legalMonoA : LegalMonoSpec AlignedWord.model := by
  intro r n i hn hleg
  simp [AlignedWord.model] at hleg ⊢
  exact hleg


end AlignedWordFacts

/-! SlotGroupAndInterleaveFacts -/

namespace SlotGroupAndInterleaveFacts


open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

/-! SlotGroup -/

def prevG : Req → Nat → Nat := fun _ i => i - 4

theorem prevSpecG : PrevSpec SlotGroup.model prevG := by
  intro r cap u i hipos hfit hs0
  have hicap : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have himod : i % 4 = 0 := by
    have h := hfit
    simp [fits, SlotGroup.model] at h
    exact h.1.2
  have hdecomp : 4 * (i / 4) + i % 4 = i := Nat.div_add_mod i 4
  rw [himod] at hdecomp
  have hi_eq : i = (i / 4) * 4 := by omega
  have hkpos : 0 < i / 4 := by
    have hs0' : i ≠ 0 := by intro h; apply hs0; simp [SlotGroup.model, align, h]
    omega
  dsimp [prevG]
  refine ⟨by omega, ?_, ?_, ?_, ?_⟩
  · have hj_eq : i - 4 = (i / 4 - 1) * 4 := by omega
    simp [SlotGroup.model, hj_eq, Nat.mul_comm, Nat.mul_mod_right]
  · intro x hx
    simp [SlotGroup.model, band] at hx
    omega
  · intro x hx
    simp [SlotGroup.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk4 : k < 4
    · left
      omega
    · right
      simp [SlotGroup.model, band]
      refine ⟨k-4, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    apply align_eq_of_between i 0 4 (i/4) (x+1) (by omega) (by omega)
    · simpa using hi_eq
    · exact hkpos
    · omega
    · omega

theorem fixG : ∀ r i, SlotGroup.model.legal r i = true → SlotGroup.model.snap r i = i := by
  intro r i h
  simp [SlotGroup.model] at h
  apply align_eq_of_mod i 0 4 (by omega) (by omega) h

theorem upG : UpSpec SlotGroup.model := by
  intro r i
  apply align_ge i 0 4
  omega

theorem beforeG : BeforeLegalSpec SlotGroup.model := by
  intro r i j hij hleg
  simp [SlotGroup.model] at hleg
  change align i 0 4 ≤ j
  have hmono := align_mono i j 0 4 hij (by omega)
  have hjalign : align j 0 4 = j := align_eq_of_mod j 0 4 (by omega) (by omega) hleg
  rw [hjalign] at hmono
  exact hmono

theorem snapLegalG : SnapLegalSpec SlotGroup.model := by
  intro r i j hij hleg
  simp [SlotGroup.model]
  apply align_mod i 0 4 (by omega) (by omega)

theorem fpSubG : FpSubSpec SlotGroup.model := by
  intro r n i hn x hx
  change x ∈ band i (align n 0 4) at hx
  change x ∈ band i (align r.bytes 0 4)
  apply band_subset (align_mono n r.bytes 0 4 hn (by omega)) x hx

theorem legalMonoG : LegalMonoSpec SlotGroup.model := by
  intro r n i hn hleg
  simp [SlotGroup.model] at hleg ⊢
  exact hleg

/-! Interleave -/

def pI (r : Req) := r.old % 2

theorem snap0I (r : Req) : align 0 (pI r) 2 = pI r := by
  unfold align
  have hp : pI r < 2 := Nat.mod_lt _ (by omega)
  have hdiv : (0 - pI r + 2 - 1) / 2 = 0 := Nat.div_eq_of_lt (by omega)
  rw [hdiv]
  omega

def prevI : Req → Nat → Nat := fun r i => i - 2

theorem prevSpecI : PrevSpec Interleave.model prevI := by
  intro r cap u i hipos hfit hs0
  have hicap : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have himod : i % 2 = pI r := by
    have h := hfit
    simp [fits, Interleave.model, pI] at h
    exact h.1.2
  have hdecomp : 2 * (i / 2) + i % 2 = i := Nat.div_add_mod i 2
  rw [himod] at hdecomp
  have hi_eq : i = pI r + (i / 2) * 2 := by omega
  have hpne : pI r ≠ i := by
    intro h
    apply hs0
    change align 0 (pI r) 2 = i
    rw [snap0I r, h]
  have hkpos : 0 < i / 2 := by omega
  dsimp [prevI]
  refine ⟨by omega, ?_, ?_, ?_, ?_⟩
  · have hj_eq : i - 2 = pI r + ((i / 2) - 1) * 2 := by omega
    simp [Interleave.model, pI, hj_eq, Nat.add_mul_mod_self_right]
  · intro x hx
    simp [Interleave.model] at hx
    omega
  · intro x hx
    simp [Interleave.model] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hk0 : k = 0
    · left
      omega
    · right
      simp [Interleave.model]
      refine ⟨k-1, ?_, by omega⟩
      omega
  · intro x hjx hxlt
    apply align_eq_of_between i (pI r) 2 (i/2) (x+1) (by omega) (by omega)
    · exact hi_eq
    · exact hkpos
    · omega
    · omega

theorem fixI : ∀ r i, Interleave.model.legal r i = true → Interleave.model.snap r i = i := by
  intro r i h
  simp [Interleave.model, pI] at h
  apply align_eq_of_mod i (pI r) 2 (by omega) (Nat.mod_lt _ (by omega)) h

theorem upI : UpSpec Interleave.model := by
  intro r i
  apply align_ge i (pI r) 2
  omega

theorem beforeI : BeforeLegalSpec Interleave.model := by
  intro r i j hij hleg
  simp [Interleave.model, pI] at hleg
  change align i (pI r) 2 ≤ j
  have hmono := align_mono i j (pI r) 2 hij (by omega)
  have hjalign : align j (pI r) 2 = j :=
    align_eq_of_mod j (pI r) 2 (by omega) (Nat.mod_lt _ (by omega)) hleg
  rw [hjalign] at hmono
  exact hmono

theorem snapLegalI : SnapLegalSpec Interleave.model := by
  intro r i j hij hleg
  simp [Interleave.model, pI]
  apply align_mod i (pI r) 2 (by omega) (Nat.mod_lt _ (by omega))

theorem fpSubI : FpSubSpec Interleave.model := by
  intro r n i hn x hx
  simp [Interleave.model] at hx ⊢
  rcases hx with ⟨k, hk, rfl⟩
  exact ⟨k, by omega, rfl⟩

theorem legalMonoI : LegalMonoSpec Interleave.model := by
  intro r n i hn hleg
  simp [Interleave.model, pI] at hleg ⊢
  exact hleg


end SlotGroupAndInterleaveFacts

/-! PhaseAlignedFacts -/

namespace PhaseAlignedFacts


open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

theorem snap0P (r : Req) : align 0 (r.old % (r.stripe+1)) (r.stripe+1) = r.old % (r.stripe+1) := by
  unfold align
  have hp : r.old % (r.stripe+1) < r.stripe+1 := Nat.mod_lt _ (by omega)
  have hdiv : (0 - r.old % (r.stripe+1) + (r.stripe+1) - 1) / (r.stripe+1) = 0 :=
    Nat.div_eq_of_lt (by omega)
  rw [hdiv]
  omega

def prevP : Req → Nat → Nat := fun r i => i - (r.stripe+1)

theorem prevSpecP : PrevSpec PhaseAligned.model prevP := by
  intro r cap u i hipos hfit hs0
  have hicap : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have himod : i % (r.stripe+1) = r.old % (r.stripe+1) := by
    have h := hfit
    simp [fits, PhaseAligned.model] at h
    exact h.1.2
  have hdecomp : (r.stripe+1) * (i / (r.stripe+1)) + i % (r.stripe+1) = i :=
    Nat.div_add_mod i (r.stripe+1)
  rw [himod] at hdecomp
  rw [Nat.mul_comm (r.stripe+1) (i / (r.stripe+1))] at hdecomp
  have hi_eq : i = r.old % (r.stripe+1) + (i / (r.stripe+1)) * (r.stripe+1) := by omega
  have hpne : r.old % (r.stripe+1) ≠ i := by
    intro h
    apply hs0
    change align 0 (r.old % (r.stripe+1)) (r.stripe+1) = i
    rw [snap0P r, h]
  have hkpos : 0 < i / (r.stripe+1) := by
    by_cases hk : i / (r.stripe+1) = 0
    · have : i = r.old % (r.stripe+1) := by
        rw [hk] at hdecomp
        simpa using hdecomp.symm
      exact (hpne this.symm).elim
    · exact Nat.pos_of_ne_zero hk
  have hk1 : 1 ≤ i / (r.stripe+1) := Nat.succ_le_of_lt hkpos
  have hkWle : r.stripe+1 ≤ (i / (r.stripe+1)) * (r.stripe+1) := by
    simpa using Nat.mul_le_mul_right (r.stripe+1) hk1
  have hWle : r.stripe+1 ≤ i := by omega
  have hp_lt : r.old % (r.stripe+1) < r.stripe+1 := Nat.mod_lt _ (by omega)
  dsimp [prevP]
  refine ⟨by omega, ?_, ?_, ?_, ?_⟩
  · have hj_eq : i - (r.stripe+1) =
        r.old % (r.stripe+1) + ((i / (r.stripe+1)) - 1) * (r.stripe+1) := by
      rw [Nat.sub_mul, Nat.one_mul]
      omega
    simp [PhaseAligned.model]
    rw [hj_eq, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hp_lt]
  · intro x hx
    simp [PhaseAligned.model, band] at hx
    omega
  · intro x hx
    simp [PhaseAligned.model, band] at hx
    rcases hx with ⟨k, hk, rfl⟩
    by_cases hkW : k < r.stripe+1
    · left
      omega
    · right
      simp [PhaseAligned.model, band]
      refine ⟨k - (r.stripe+1), ?_, by omega⟩
      omega
  · intro x hjx hxlt
    apply align_eq_of_between i (r.old % (r.stripe+1)) (r.stripe+1) (i / (r.stripe+1)) (x+1)
    · omega
    · exact hp_lt
    · exact hi_eq
    · exact hkpos
    · omega
    · omega

theorem fixP : ∀ r i, PhaseAligned.model.legal r i = true → PhaseAligned.model.snap r i = i := by
  intro r i h
  simp [PhaseAligned.model] at h
  change align i (r.old % (r.stripe+1)) (r.stripe+1) = i
  apply align_eq_of_mod i (r.old % (r.stripe+1)) (r.stripe+1) (by omega)
    (Nat.mod_lt _ (by omega)) h

theorem upP : UpSpec PhaseAligned.model := by
  intro r i
  change i ≤ align i (r.old % (r.stripe+1)) (r.stripe+1)
  apply align_ge
  omega

theorem beforeP : BeforeLegalSpec PhaseAligned.model := by
  intro r i j hij hleg
  simp [PhaseAligned.model] at hleg
  change align i (r.old % (r.stripe+1)) (r.stripe+1) ≤ j
  have hmono := align_mono i j (r.old % (r.stripe+1)) (r.stripe+1) hij (by omega)
  have hjalign : align j (r.old % (r.stripe+1)) (r.stripe+1) = j :=
    align_eq_of_mod j (r.old % (r.stripe+1)) (r.stripe+1) (by omega)
      (Nat.mod_lt _ (by omega)) hleg
  rw [hjalign] at hmono
  exact hmono

theorem snapLegalP : SnapLegalSpec PhaseAligned.model := by
  intro r i j hij hleg
  simp [PhaseAligned.model]
  apply align_mod i (r.old % (r.stripe+1)) (r.stripe+1) (by omega)
    (Nat.mod_lt _ (by omega))

theorem fpSubP : FpSubSpec PhaseAligned.model := by
  intro r n i hn x hx
  apply band_subset hn x hx

theorem legalMonoP : LegalMonoSpec PhaseAligned.model := by
  intro r n i hn hleg
  simp [PhaseAligned.model] at hleg ⊢
  exact hleg

theorem oldPhase : PhaseAligned.OldPhase.Goal := by
  intro r
  change align r.old (r.old % (r.stripe+1)) (r.stripe+1) = r.old
  apply align_eq_of_mod r.old (r.old % (r.stripe+1)) (r.stripe+1) (by omega)
    (Nat.mod_lt _ (by omega)) (rfl)

theorem period : PhaseAligned.Period.Goal := by
  intro r i
  change align (i + (r.stripe+1)) (r.old % (r.stripe+1)) (r.stripe+1) =
    align i (r.old % (r.stripe+1)) (r.stripe+1) + (r.stripe+1)
  by_cases hi : r.old % (r.stripe+1) ≤ i
  · unfold align
    have hsub : i + (r.stripe+1) - r.old % (r.stripe+1) =
        (i - r.old % (r.stripe+1)) + (r.stripe+1) := by omega
    rw [hsub]
    have hs : (i - r.old % (r.stripe+1)) + (r.stripe+1) + (r.stripe+1) - 1 =
        (i - r.old % (r.stripe+1) + (r.stripe+1) - 1) + (r.stripe+1) := by omega
    rw [hs]
    have hdiv : (i - r.old % (r.stripe+1) + (r.stripe+1) - 1 + (r.stripe+1)) /
        (r.stripe+1) = (i - r.old % (r.stripe+1) + (r.stripe+1) - 1) / (r.stripe+1) + 1 := by
      simpa [Nat.one_mul] using Nat.add_mul_div_right
        (i - r.old % (r.stripe+1) + (r.stripe+1) - 1) 1 (by omega : 0 < r.stripe+1)
    rw [hdiv, Nat.succ_mul]
    omega
  · have hil : i < r.old % (r.stripe+1) := by omega
    have hp_lt : r.old % (r.stripe+1) < r.stripe+1 := Nat.mod_lt _ (by omega)
    unfold align
    have hdivi : (i - r.old % (r.stripe+1) + (r.stripe+1) - 1) / (r.stripe+1) = 0 :=
      Nat.div_eq_of_lt (by omega)
    have hdivj : (i + (r.stripe+1) - r.old % (r.stripe+1) + (r.stripe+1) - 1) /
        (r.stripe+1) = 1 := by
      apply Nat.div_eq_of_lt_le
      · rw [Nat.one_mul]
        omega
      · rw [Nat.succ_mul, Nat.one_mul]
        omega
    rw [hdivi, hdivj]
    omega


end PhaseAlignedFacts

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts
