import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `TruncateKey.KeyMinimal` — Aggressive claim: different keys have distinguishable
  behavior on a mutually eligible row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.TruncateKey
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

theorem sound : TruncateKey.KeyMinimal.Goal := by
  intro a b hkey
  unfold TruncateKey.shape at hkey ⊢
  dsimp at hkey ⊢
  simp at hkey
  by_cases hs : a.slot = b.slot
  · have hl : a.literal % 65536 ≠ b.literal % 65536 := hkey (by omega)
    let r : Row := (List.replicate (a.slot+1) none).set a.slot (some 0)
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_self a.slot 0]
    rw [← hs, get_self a.slot 0]
    simp [mapV, Int.add_emod, hl]
  · by_cases hl : a.literal % 65536 = b.literal % 65536
    · let r : Row := pickRow a.slot b.slot 0 1
      refine ⟨r, by simp, by simp, ?_⟩
      rw [get_left a.slot b.slot 0 1 hs]
      rw [get_right a.slot b.slot 0 1 hs]
      simp [mapV, Int.add_emod, hl]
      have hr0 : 0 ≤ a.literal % (65536:Int) := Int.emod_nonneg _ (by native_decide)
      have hrlt : a.literal % (65536:Int) < 65536 := Int.emod_lt_of_pos _ (by native_decide)
      omega
    · let r : Row := pickRow a.slot b.slot 0 0
      refine ⟨r, by simp, by simp, ?_⟩
      rw [get_left a.slot b.slot 0 0 hs]
      rw [get_right a.slot b.slot 0 0 hs]
      simp [mapV, Int.add_emod]
      exact hl

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_KeyMinimal
