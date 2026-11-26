import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.CheckedAffine
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

theorem refute : ¬ CheckedAffine.KeyMinimal.Goal := by
  let A : Cfg := ⟨0, 1, 0, false, 0, []⟩
  let B : Cfg := {A with scale := 18446744073709551617}
  apply not_keyMinimal_of CheckedAffine.shape A B
  · native_decide
  · intro r ha hb
    cases hget : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r A.slot with
    | null =>
        have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.null := by simpa [A] using hget
        simp [CheckedAffine.shape, A, B, hg, mapV]
    | fault =>
        have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.fault := by simpa [A] using hget
        simp [CheckedAffine.shape, A, B, hg, mapV]
    | val x =>
        have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.val x := by simpa [A] using hget
        unfold CheckedAffine.shape at hb
        dsimp at hb
        simp [A, B] at hb
        rw [hg] at hb
        simp only [fits, Bool.and_eq_true, decide_eq_true_eq] at hb
        have hlim : limit 64 = 9223372036854775808 := by native_decide
        rw [hlim] at hb
        have hx : x = 0 := by omega
        subst x
        simp [CheckedAffine.shape, A, B, hg, mapV]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_KeyMinimal
