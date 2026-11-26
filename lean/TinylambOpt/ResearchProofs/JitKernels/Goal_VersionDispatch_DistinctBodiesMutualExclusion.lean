import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Entries that accept the same word cannot clash, so they must name the same code; dispatch is invariant under permutation of such a chain. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_DistinctBodiesMutualExclusion

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.DistinctBodiesMutualExclusion

theorem bool_false_of_not {p : Bool} (h : ¬ p) : p = false := by
  cases p <;> simp_all

theorem find_cons_of_false {α : Type} (p : α → Bool) (a : α) (l : List α) (ha : ¬ p a) :
    (a :: l).find? p = l.find? p := by
  simp [List.find?, bool_false_of_not ha]

theorem dispatch_cons_of_false (e : Entry) (l : List Entry) (w : Word) (he : ¬ accepts e w) :
    dispatch (e :: l) w = dispatch l w := by
  rw [dispatch, dispatch, find_cons_of_false (accepts · w) e l he]

theorem dispatch_eq_of_all_codes (l : List Entry) (w : Word) (c : Nat)
    (hex : ∃ e ∈ l, accepts e w) (hall : ∀ e ∈ l, accepts e w → e.code = c) :
    dispatch l w = some c := by
  induction l with
  | nil => simp at hex
  | cons e l ih =>
      by_cases he : accepts e w
      · rw [show dispatch (e :: l) w = some e.code by simp [dispatch, he]]
        rw [hall e (by simp) he]
      · have hex' : ∃ f ∈ l, accepts f w := by
          rcases hex with ⟨f, hf, haf⟩
          simp only [List.mem_cons] at hf
          rcases hf with rfl | hf
          · exact False.elim (he haf)
          · exact ⟨f, hf, haf⟩
        have hall' : ∀ f ∈ l, accepts f w → f.code = c := by
          intro f hf haf
          exact hall f (by simp [hf]) haf
        rw [dispatch_cons_of_false e l w he]
        exact ih hex' hall'

theorem dispatch_none_of_all_false (l : List Entry) (w : Word) (h : ∀ e ∈ l, ¬ accepts e w) :
    dispatch l w = none := by
  induction l with
  | nil => simp [dispatch]
  | cons e l ih =>
      have he : ¬ accepts e w := h e (by simp)
      have ht : ∀ f ∈ l, ¬ accepts f w := by
        intro f hf
        exact h f (by simp [hf])
      rw [dispatch_cons_of_false e l w he]
      exact ih ht

theorem sound : DistinctBodiesMutualExclusion.Goal := by
  intro a b w hperm hcode
  have noClashOfAccepts : ∀ e f : Entry, accepts e w → accepts f w → !clash e f := by
    intro e f he hf
    simp [clash] at ⊢
    simp [accepts] at he hf
    bv_decide
  by_cases hexa : ∃ e ∈ a, accepts e w
  · rcases hexa with ⟨ea, hea_mem, hea⟩
    have hallA : ∀ e ∈ a, accepts e w → e.code = ea.code := by
      intro e he hacc
      exact (hcode ea hea_mem e he (noClashOfAccepts ea e hea hacc)).symm
    have hda : dispatch a w = some ea.code := dispatch_eq_of_all_codes a w ea.code ⟨ea, hea_mem, hea⟩ hallA
    have heab_mem : ea ∈ b := hperm.mem_iff.mp hea_mem
    have hexb : ∃ e ∈ b, accepts e w := ⟨ea, heab_mem, hea⟩
    have hallB : ∀ e ∈ b, accepts e w → e.code = ea.code := by
      intro e he hacc
      have hea' : e ∈ a := hperm.mem_iff.mpr he
      exact (hcode ea hea_mem e hea' (noClashOfAccepts ea e hea hacc)).symm
    have hdb : dispatch b w = some ea.code := dispatch_eq_of_all_codes b w ea.code hexb hallB
    rw [hda, hdb]
  · have hnoa : ∀ e ∈ a, ¬ accepts e w := by
      intro e he hacc
      exact hexa ⟨e, he, hacc⟩
    have hnob : ∀ e ∈ b, ¬ accepts e w := by
      intro e he hacc
      exact hnoa e (hperm.mem_iff.mpr he) hacc
    rw [dispatch_none_of_all_false a w hnoa, dispatch_none_of_all_false b w hnob]

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_DistinctBodiesMutualExclusion
