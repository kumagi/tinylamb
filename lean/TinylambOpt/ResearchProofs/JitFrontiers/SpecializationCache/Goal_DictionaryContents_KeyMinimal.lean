import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_KeyMinimal

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryContents
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

private theorem get_repl_null (i n : Nat) (h : i < n) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate n none) i = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  simp [List.getElem?_replicate, h]

private theorem get_repl_fault (i n : Nat) (h : n ≤ i) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate n none) i = Out.fault := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  simp [List.getElem?_replicate, h]

private theorem out_ne_of_opt_ne {x y : Option Int} (h : x ≠ y) :
    (x.map Out.val).getD Out.null ≠ (y.map Out.val).getD Out.null := by
  cases x <;> cases y <;> simp_all

theorem sound : DictionaryContents.KeyMinimal.Goal := by
  intro a b hkey
  unfold DictionaryContents.shape at hkey ⊢
  dsimp at hkey ⊢
  simp at hkey
  by_cases hs : a.slot = b.slot
  · have ht : a.table ≠ b.table := hkey (by omega)
    have hex : ∃ i : Nat, a.table[i]? ≠ b.table[i]? := by
      by_cases hex' : ∃ i : Nat, a.table[i]? ≠ b.table[i]?
      · exact hex'
      · exfalso
        apply ht
        apply List.ext_getElem?
        intro i
        by_cases hi : a.table[i]? = b.table[i]?
        · exact hi
        · exact False.elim (hex' ⟨i, hi⟩)
    obtain ⟨i, hi⟩ := hex
    let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (i : Int))
    have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r a.slot = Out.val (i:Int) := by
      unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
      simp [r]
    have hgb : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r b.slot = Out.val (i:Int) := by
      rw [← hs]
      exact hg
    refine ⟨r, by simp, by simp, ?_⟩
    simp [dictionaryLoad, hg, hgb, List.getElem?_toArray]
    exact out_ne_of_opt_ne hi
  · let n := min a.slot b.slot + 1
    let r : Row := List.replicate n none
    have hcase : (a.slot < n ∧ n ≤ b.slot) ∨ (b.slot < n ∧ n ≤ a.slot) := by
      dsimp [n]
      omega
    rcases hcase with ⟨ha, hb⟩ | ⟨hb, ha⟩
    · have hga : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r a.slot = Out.null := by
        simpa [r] using get_repl_null a.slot n ha
      have hgb : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r b.slot = Out.fault := by
        simpa [r] using get_repl_fault b.slot n hb
      refine ⟨r, by simp, by simp, ?_⟩
      simp [dictionaryLoad, hga, hgb]
    · have hga : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r a.slot = Out.fault := by
        simpa [r] using get_repl_fault a.slot n ha
      have hgb : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r b.slot = Out.null := by
        simpa [r] using get_repl_null b.slot n hb
      refine ⟨r, by simp, by simp, ?_⟩
      simp [dictionaryLoad, hga, hgb]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_KeyMinimal
