import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Two equal-body versions differing in exactly one masked bit can be replaced by one version with that bit unguarded. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_FoldNeighborVersions

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem accepts_iff_mask (e : Entry) (w : Word) :
    accepts e w = true ↔
      ∀ j, e.mask.getLsbD j = true → w.getLsbD j = e.value.getLsbD j := by
  constructor
  · intro h j hm
    have heq : w &&& e.mask = e.value &&& e.mask := by
      simpa [accepts, beq_iff_eq] using h
    have hb := congrArg (fun z : Word => z.getLsbD j) heq
    simpa [hm] using hb
  · intro h
    simp only [accepts, beq_iff_eq]
    apply BitVec.eq_of_getLsbD_eq
    intro j hj
    by_cases hm : e.mask.getLsbD j = true
    · simp [hm, h j hm]
    · simp [hm]

theorem shift_one_ne (i j : Nat) (hji : j ≠ i) :
    (((1 : Word) <<< i).getLsbD j) = false := by
  rw [BitVec.getLsbD_shiftLeft]
  simp only [Bool.and_eq_false_iff]
  by_cases hj64 : j < 64
  · simp [hj64]
    by_cases hlt : j < i
    · simp [hlt]
    · have hgt : i < j := by omega
      have hsub : j - i ≠ 0 := by omega
      simp [hlt, hsub]
  · simp [hj64]

theorem shift_one_eq (i : Nat) (hi : i < 64) :
    (((1 : Word) <<< i).getLsbD i) = true := by
  rw [BitVec.getLsbD_shiftLeft]
  simp [hi]

theorem value_eq_of_ne (a b : Entry) (i : Fin 64)
    (hdiff : a.value ^^^ b.value = (1 : Word) <<< i.val) (j : Nat)
    (hji : j ≠ i.val) : a.value.getLsbD j = b.value.getLsbD j := by
  have hx := congrArg (fun z : Word => z.getLsbD j) hdiff
  rw [BitVec.getLsbD_xor, shift_one_ne i.val j hji] at hx
  cases ha : a.value.getLsbD j <;> cases hb : b.value.getLsbD j <;> simp_all

theorem value_ne_at (a b : Entry) (i : Fin 64)
    (hdiff : a.value ^^^ b.value = (1 : Word) <<< i.val) :
    a.value.getLsbD i.val ≠ b.value.getLsbD i.val := by
  have hx := congrArg (fun z : Word => z.getLsbD i.val) hdiff
  rw [BitVec.getLsbD_xor] at hx
  simp [i.isLt] at hx
  cases ha : a.value.getLsbD i.val <;> cases hb : b.value.getLsbD i.val <;> simp_all

theorem merged_mask_true (a : Entry) (i : Fin 64) (j : Nat)
    (hj : (a.mask &&& ~~~((1 : Word) <<< i.val)).getLsbD j = true) :
    a.mask.getLsbD j = true ∧ j ≠ i.val := by
  constructor
  · have hp :
        a.mask.getLsbD j = true ∧ j < 64 ∧
          ((64 ≤ j ∨ j < i.val) ∨ ¬ j - i.val = 0) := by
      simpa [BitVec.getLsbD_and, BitVec.getLsbD_not] using hj
    exact hp.1
  · intro hji
    subst j
    have hf : (a.mask &&& ~~~((1 : Word) <<< i.val)).getLsbD i.val = false := by
      simp [BitVec.getLsbD_and, BitVec.getLsbD_not]
    simp at hj

theorem merged_mask_of (a : Entry) (i : Fin 64) (j : Nat)
    (hm : a.mask.getLsbD j = true) (hji : j ≠ i.val) :
    (a.mask &&& ~~~((1 : Word) <<< i.val)).getLsbD j = true := by
  have hj64 : j < 64 := BitVec.lt_of_getLsbD hm
  rw [BitVec.getLsbD_and, BitVec.getLsbD_not, shift_one_ne i.val j hji]
  simp [hm, hj64]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions.Goal := by
  intro a b i w hmask hcode hbit hdiff
  let c : Entry := {a with mask := a.mask &&& ~~~((1 : Word) <<< i.val)}
  have hcode' : a.code = c.code := by rfl
  have hcode'' : b.code = c.code := hcode.symm.trans hcode'
  change dispatch [a, b] w = dispatch [c] w
  by_cases ha : accepts a w
  · have hc : accepts c w = true := by
      rw [accepts_iff_mask]
      intro j hj
      rcases merged_mask_true a i j hj with ⟨hamask, hji⟩
      exact (accepts_iff_mask a w).1 ha j hamask
    simp [dispatch, ha, hc, hcode']
  · by_cases hb : accepts b w
    · have hc : accepts c w = true := by
        rw [accepts_iff_mask]
        intro j hj
        rcases merged_mask_true a i j hj with ⟨hamask, hji⟩
        have hbmask : b.mask.getLsbD j = true := by simpa [hmask] using hamask
        have hbw := (accepts_iff_mask b w).1 hb j hbmask
        have hval := value_eq_of_ne a b i hdiff j hji
        exact hbw.trans hval.symm
      simp [dispatch, ha, hb, hc, hcode'']
    · have hc : accepts c w = false := by
        cases hcv : accepts c w with
        | false => rfl
        | true =>
          have hct : accepts c w = true := hcv
          have hwi :
              w.getLsbD i.val = a.value.getLsbD i.val ∨
                w.getLsbD i.val = b.value.getLsbD i.val := by
            have hne := value_ne_at a b i hdiff
            cases hw : w.getLsbD i.val <;>
              cases hva : a.value.getLsbD i.val <;>
              cases hvb : b.value.getLsbD i.val <;> simp_all
          rcases hwi with hwa | hwb
          · have haccepts : accepts a w = true := by
              rw [accepts_iff_mask]
              intro j hj
              by_cases hji : j = i.val
              · subst j
                exact hwa
              · exact (accepts_iff_mask c w).1 hct j (merged_mask_of a i j hj hji)
            exact False.elim (ha haccepts)
          · have hbaccepts : accepts b w = true := by
              rw [accepts_iff_mask]
              intro j hj
              by_cases hji : j = i.val
              · subst j
                exact hwb
              · have hamask : a.mask.getLsbD j = true := by simpa [hmask] using hj
                have hcw := (accepts_iff_mask c w).1 hct j (merged_mask_of a i j hamask hji)
                have hval := value_eq_of_ne a b i hdiff j hji
                exact hcw.trans hval
            exact False.elim (hb hbaccepts)
      simp [dispatch, ha, hb, hc]

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_FoldNeighborVersions
