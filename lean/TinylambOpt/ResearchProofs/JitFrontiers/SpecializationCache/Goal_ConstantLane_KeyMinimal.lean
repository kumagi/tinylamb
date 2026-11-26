import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Refuted by counterexample

- `ConstantLane.KeyMinimal` — Aggressive claim: different keys have distinguishable
  behavior on a mutually eligible row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ConstantLane
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

theorem refute : ¬ ConstantLane.KeyMinimal.Goal := by
  let A : Cfg := ⟨0, 0, 0, false, 0, []⟩
  let B : Cfg := {A with literal := 1}
  apply not_keyMinimal_of ConstantLane.shape A B
  · native_decide
  · intro r ha hb
    unfold ConstantLane.shape at ha hb
    dsimp at ha hb
    cases hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r A.slot with
    | null =>
        have hg0 : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.null := by simpa [A] using hg
        simp [A, B] at ha hb
        rw [hg0] at ha hb
        have hf : (Out.null == Out.val (0:Int)) = false := by rfl
        rw [hf] at ha
        cases ha
    | fault =>
        have hg0 : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.fault := by simpa [A] using hg
        simp [A, B] at ha hb
        rw [hg0] at ha hb
        have hf : (Out.fault == Out.val (0:Int)) = false := by rfl
        rw [hf] at ha
        cases ha
    | val n =>
        have hg0 : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.val n := by simpa [A] using hg
        simp [A, B] at ha hb
        rw [hg0] at ha hb
        have hba : (Out.val n == Out.val (0:Int)) = (n == 0) := by rfl
        have hbb : (Out.val n == Out.val (1:Int)) = (n == 1) := by rfl
        rw [hba] at ha
        rw [hbb] at hb
        have hna : n = 0 := beq_iff_eq.mp ha
        have hnb : n = 1 := beq_iff_eq.mp hb
        omega

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_KeyMinimal
