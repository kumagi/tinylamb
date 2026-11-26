import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `SignDirection.KeyMinimal` — Aggressive claim: different keys have distinguishable
  behavior on a mutually eligible row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.SignDirection
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

theorem sound : SignDirection.KeyMinimal.Goal := by
  intro a b hkey
  unfold SignDirection.shape at hkey ⊢
  dsimp at hkey ⊢
  simp at hkey
  by_cases hs : a.slot ≠ b.slot
  · let x := if a.mode then a.literal + 1 else a.literal - 1
    let r : Row := pickRow a.slot b.slot x b.literal
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_left a.slot b.slot x b.literal hs]
    rw [get_right a.slot b.slot x b.literal hs]
    cases am : a.mode
    · have hx : x = a.literal - 1 := by simp [x, am]
      cases bm : b.mode
      · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : a.literal - 1 < a.literal),
            decide_eq_false (by omega : ¬ (b.literal < b.literal))]
        decide
      · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : a.literal - 1 < a.literal),
            decide_eq_false (by omega : ¬ (b.literal > b.literal))]
        decide
    · have hx : x = a.literal + 1 := by simp [x, am]
      cases bm : b.mode
      · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : a.literal + 1 > a.literal),
            decide_eq_false (by omega : ¬ (b.literal < b.literal))]
        decide
      · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : a.literal + 1 > a.literal),
            decide_eq_false (by omega : ¬ (b.literal > b.literal))]
        decide
  · have heq : a.slot = b.slot := by omega
    cases am : a.mode
    · cases bm : b.mode
      · have hl : a.literal ≠ b.literal := by
          intro he
          apply hkey
          · omega
          · exact he
          · simp [am, bm]
        by_cases hlt : a.literal < b.literal
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some a.literal)
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot a.literal, ← heq, get_self a.slot a.literal]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_false (by omega : ¬ (a.literal < a.literal)),
              decide_eq_true (by omega : a.literal < b.literal)]
          decide
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some b.literal)
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot b.literal, ← heq, get_self a.slot b.literal]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_true (by omega : b.literal < a.literal),
              decide_eq_false (by omega : ¬ (b.literal < b.literal))]
          decide
      · by_cases hle : a.literal ≤ b.literal
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (a.literal - 1))
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot (a.literal - 1), ← heq, get_self a.slot (a.literal - 1)]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_true (by omega : a.literal - 1 < a.literal),
              decide_eq_false (by omega : ¬ (a.literal - 1 > b.literal))]
          decide
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some b.literal)
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot b.literal, ← heq, get_self a.slot b.literal]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_true (by omega : b.literal < a.literal),
              decide_eq_false (by omega : ¬ (b.literal > b.literal))]
          decide
    · cases bm : b.mode
      · by_cases hle : b.literal ≤ a.literal
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (b.literal - 1))
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot (b.literal - 1), ← heq, get_self a.slot (b.literal - 1)]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_false (by omega : ¬ (b.literal - 1 > a.literal)),
              decide_eq_true (by omega : b.literal - 1 < b.literal)]
          decide
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some a.literal)
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot a.literal, ← heq, get_self a.slot a.literal]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_false (by omega : ¬ (a.literal > a.literal)),
              decide_eq_true (by omega : a.literal < b.literal)]
          decide
      · have hl : a.literal ≠ b.literal := by
          intro he
          apply hkey
          · omega
          · exact he
          · simp [am, bm]
        by_cases hlt : a.literal < b.literal
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some b.literal)
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot b.literal, ← heq, get_self a.slot b.literal]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_true (by omega : b.literal > a.literal),
              decide_eq_false (by omega : ¬ (b.literal > b.literal))]
          decide
        · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some a.literal)
          refine ⟨r, by simp, by simp, ?_⟩
          rw [get_self a.slot a.literal, ← heq, get_self a.slot a.literal]
          simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
          rw [decide_eq_false (by omega : ¬ (a.literal > a.literal)),
              decide_eq_true (by omega : a.literal > b.literal)]
          decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_KeyMinimal
