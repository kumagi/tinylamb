import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.NullPolicy
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private def pickRow (i j : Nat) (vi vj : Int) : Row :=
  ((List.replicate (max i j + 1) none).set i (some vi)).set j (some vj)

private theorem get_left (i j : Nat) (vi vj : Int) (hij : i ≠ j) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickRow i j vi vj) i = Out.val vi := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hi : i < max i j + 1 := by omega
  have hji : j ≠ i := by omega
  simp [pickRow, hji, hi]

private theorem get_right (i j : Nat) (vi vj : Int) (hij : i ≠ j) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickRow i j vi vj) j = Out.val vj := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hj : j < max i j + 1 := by omega
  simp [pickRow, hij, hj]

private theorem get_self (i : Nat) (vi : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get ((List.replicate (i+1) none).set i (some vi)) i = Out.val vi := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  simp

private def pickLeft (i j : Nat) (v : Int) : Row := (List.replicate (max i j + 1) none).set i (some v)

private theorem get_pickLeft_left (i j : Nat) (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickLeft i j v) i = Out.val v := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hi : i < max i j + 1 := by omega
  simp [pickLeft, hi]

private theorem get_pickLeft_right (i j : Nat) (v : Int) (hij : i ≠ j) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickLeft i j v) j = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hj : j < max i j + 1 := by omega
  simp [pickLeft, hij, hj]

private theorem get_hole (i : Nat) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate (i+1) none) i = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  rw [List.getElem?_replicate, if_pos (by omega : i < i + 1)]

theorem sound : NullPolicy.KeyMinimal.Goal := by
  intro a b hkey
  unfold NullPolicy.shape at hkey ⊢
  dsimp at hkey ⊢
  simp at hkey
  by_cases hs : a.slot = b.slot
  · let r : Row := List.replicate (a.slot + 1) none
    have hg : get r a.slot = Out.null := get_hole a.slot
    have hg2 : get r b.slot = Out.null := by rw [← hs]; exact hg
    refine ⟨r, by simp, by simp, ?_⟩
    rw [hg, hg2]
    cases am : a.mode
    · cases bm : b.mode
      · exfalso
        apply hkey
        · omega
        · rw [am, bm]
        · simp [am, bm]
      · simp [am, bm]
    · cases bm : b.mode
      · simp [am, bm]
      · have hl : a.literal ≠ b.literal := by
          intro he
          apply hkey
          · omega
          · rw [am, bm]
          · simp [am, bm, he]
        simp [am, bm]
        exact hl
  · let r : Row := pickRow a.slot b.slot 0 1
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_left a.slot b.slot 0 1 hs]
    rw [get_right a.slot b.slot 0 1 hs]
    simp only []
    exact fun hh => absurd (Out.val.inj hh) (by omega)

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_KeyMinimal
