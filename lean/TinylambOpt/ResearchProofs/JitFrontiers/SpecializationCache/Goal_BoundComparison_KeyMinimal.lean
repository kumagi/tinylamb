import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_BoundComparison_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.BoundComparison
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

theorem sound : BoundComparison.KeyMinimal.Goal := by
  intro a b hkey
  unfold BoundComparison.shape at hkey ⊢
  dsimp at hkey ⊢
  simp at hkey
  by_cases hs : a.slot = b.slot
  · have hl : a.literal ≠ b.literal := hkey (by omega)
    let x := min a.literal b.literal
    let r : Row := (List.replicate (a.slot+1) none).set a.slot (some x)
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_self a.slot x]
    rw [← hs, get_self a.slot x]
    dsimp [x]
    by_cases hlt : min a.literal b.literal < a.literal
    · have hb : min a.literal b.literal = b.literal := by omega
      simp [mapV, hlt, hb]
      omega
    · have ha : min a.literal b.literal = a.literal := by omega
      have hltb : a.literal < b.literal := by omega
      simp [mapV, hlt, ha, hltb]
  · let r : Row := pickRow a.slot b.slot (a.literal - 1) b.literal
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_left a.slot b.slot (a.literal - 1) b.literal hs]
    rw [get_right a.slot b.slot (a.literal - 1) b.literal hs]
    simp [mapV]
    omega

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_BoundComparison_KeyMinimal
