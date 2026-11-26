import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Two equal-body guards differing in exactly one masked bit can be merged by dropping that
bit from the mask: an incoming row matches either neighbor exactly when it matches the merged
guard, so the compiled dispatch chain returns the same code. -/
namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_FoldNeighborVersions
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch FoldNeighborVersions

/-- A guard accepts exactly the words whose masked bits equal the masked value. -/
private theorem acc_eq (e : Entry) (w : Word) :
    accepts e w = true ↔ (w &&& e.mask) = (e.value &&& e.mask) := by
  simp [accepts]

/-- The singleton bit vector `1 <<< i` tests exactly bit `i`. -/
private theorem bit_single (i : Fin 64) (k : Nat) :
    ((1 : Word) <<< i.val).getLsbD k = decide (i.val = k) := by
  rw [BitVec.getLsbD_shiftLeft]
  show ((decide (k < 64) && !decide (k < ↑i) && (1#64).getLsbD (k - ↑i))) = decide (↑i = k)
  rw [BitVec.getLsbD_one (w := 64)]
  rcases Nat.lt_trichotomy k i.val with hlt | heq | hgt
  · rw [decide_eq_true (by omega : k < 64), decide_eq_true hlt,
      decide_eq_true (by omega : k - i.val = 0), decide_eq_false (by omega : ¬ i.val = k)]
    simp
  · subst heq
    rw [decide_eq_true (by omega : i.val < 64), decide_eq_false (by omega : ¬ i.val < i.val),
        decide_eq_true (by omega : i.val - i.val = 0), decide_eq_true (rfl : i.val = i.val)]
    simp
  · rw [decide_eq_false (by omega : ¬ i.val = k)]
    by_cases hk : k < 64
    · rw [decide_eq_true hk, decide_eq_false (by omega : ¬ k < i.val),
        decide_eq_false (by omega : ¬ k - i.val = 0)]
      simp
    · rw [decide_eq_false hk]
      simp

/-- If two bit values xor to `D`, then below any mask they agree on every bit `D` does not
test. -/
private theorem agree_off (AV BV M D : Bool) (hx : (AV ^^ BV) = D) :
    ((AV && M) && !D) = ((BV && M) && !D) := by
  revert AV BV M D hx
  decide

/-- Two bits are equal when they agree everywhere `D` is false and `D` being true already
implies their equality. -/
private theorem bit_eq_of_agree (P Q D : Bool) (hce : (P && !D) = (Q && !D))
    (he : D = true → P = Q) : P = Q := by
  revert P Q D hce he
  decide

/--
`FoldNeighborVersions.Goal`: two compiled versions sharing one body whose guards differ in a
single masked bit can be coalesced by deleting that bit from the mask. Any input row that
triggers either neighbor also triggers the merged guard (and vice versa), so the dispatch
chain picks the same code regardless of which bit of the differing pair the row satisfies —
letting the code cache store one specialization instead of two.
-/
theorem sound : Goal := by
  intro a b i w hm hc hbit hxor
  let S : Word := (1 : Word) <<< i.val
  let s : Entry := {a with mask := a.mask &&& ~~~S}
  have hs_get : ∀ k, S.getLsbD k = decide (i.val = k) := bit_single i
  have hmask_eq : (b.value &&& a.mask) &&& ~~~S = (a.value &&& a.mask) &&& ~~~S := by
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro k hk
    have hx : (a.value.getLsbD k ^^ b.value.getLsbD k) = S.getLsbD k := by
      have hxb := congrArg (fun v : Word => v.getLsbD k) hxor
      rw [BitVec.getLsbD_xor] at hxb
      exact hxb
    simpa [BitVec.getLsbD_and, BitVec.getLsbD_not, hk] using (agree_off _ _ _ _ hx).symm
  have hacc : accepts s w = true ↔ (accepts a w = true ∨ accepts b w = true) := by
    rw [acc_eq s w, acc_eq a w, acc_eq b w]
    dsimp only [s]
    constructor
    · intro h
      rw [← BitVec.and_assoc, ← BitVec.and_assoc] at h
      by_cases g : w.getLsbD i.val = a.value.getLsbD i.val
      · left
        apply BitVec.eq_of_getLsbD_eq_iff.mpr
        intro k hk
        have hce : ((w &&& a.mask).getLsbD k && !S.getLsbD k)
            = ((a.value &&& a.mask).getLsbD k && !S.getLsbD k) := by
          simpa [BitVec.getLsbD_and, BitVec.getLsbD_not, hk] using
            congrArg (fun v : Word => v.getLsbD k) h
        have he : S.getLsbD k = true →
            (w &&& a.mask).getLsbD k = (a.value &&& a.mask).getLsbD k := by
          intro hd
          have h2 : decide (i.val = k) = true := by rw [← hs_get k]; exact hd
          have hik : i.val = k := of_decide_eq_true h2
          subst hik
          simp only [BitVec.getLsbD_and]
          exact congrArg (fun x => x && a.mask.getLsbD i.val) g
        exact bit_eq_of_agree _ _ _ hce he
      · right
        rw [← hm]
        apply BitVec.eq_of_getLsbD_eq_iff.mpr
        intro k hk
        have hce : ((w &&& a.mask).getLsbD k && !S.getLsbD k)
            = ((a.value &&& a.mask).getLsbD k && !S.getLsbD k) := by
          simpa [BitVec.getLsbD_and, BitVec.getLsbD_not, hk] using
            congrArg (fun v : Word => v.getLsbD k) h
        by_cases hd : S.getLsbD k = true
        · have h2 : decide (i.val = k) = true := by rw [← hs_get k]; exact hd
          have hik : i.val = k := of_decide_eq_true h2
          subst hik
          have hw : w.getLsbD i.val = !a.value.getLsbD i.val := by
            revert g
            cases w.getLsbD i.val <;> cases a.value.getLsbD i.val <;> simp_all
          have hbv : b.value.getLsbD i.val = !a.value.getLsbD i.val := by
            have hxb := congrArg (fun v : Word => v.getLsbD i.val) hxor
            rw [BitVec.getLsbD_xor] at hxb
            rw [show ((1 : Word) <<< i.val).getLsbD i.val = true from hd] at hxb
            revert hxb
            cases a.value.getLsbD i.val <;> cases b.value.getLsbD i.val <;> simp_all
          rw [BitVec.getLsbD_and, BitVec.getLsbD_and, hw, hbv]
        · have hkv : ¬ i.val = k := by
            intro hh; subst hh
            have h2 : decide (i.val = i.val) = true := decide_eq_true rfl
            rw [hs_get] at hd
            exact hd h2
          have hd' : S.getLsbD k = false := by rw [hs_get]; exact decide_eq_false hkv
          have hxe : a.value.getLsbD k = b.value.getLsbD k := by
            have hxb := congrArg (fun v : Word => v.getLsbD k) hxor
            rw [BitVec.getLsbD_xor] at hxb
            rw [show ((1 : Word) <<< i.val).getLsbD k = false from hd'] at hxb
            revert hxb
            cases a.value.getLsbD k <;> cases b.value.getLsbD k <;> simp_all
          rw [hd', Bool.not_false, Bool.and_true] at hce
          rw [hce, BitVec.getLsbD_and, BitVec.getLsbD_and, hxe]
          simp
    · rintro (ha | hb)
      · rw [← BitVec.and_assoc, ← BitVec.and_assoc]
        exact congrArg (fun v : Word => v &&& ~~~S) ha
      · rw [← BitVec.and_assoc, ← BitVec.and_assoc]
        rw [← hm] at hb
        rw [hb]
        exact hmask_eq
  by_cases ha : accepts a w = true
  · have hs : accepts s w = true := hacc.mpr (Or.inl ha)
    have hleft : dispatch [a, b] w = some a.code := by simp [dispatch, ha]
    have hright0 : dispatch [s] w = some s.code := by simp [dispatch, hs]
    have hcode : s.code = a.code := by simp [s]
    rw [hleft, hright0, hcode]
  · by_cases hb : accepts b w = true
    · have hs : accepts s w = true := hacc.mpr (Or.inr hb)
      have hleft : dispatch [a, b] w = some b.code := by simp [dispatch, ha, hb]
      have hright0 : dispatch [s] w = some s.code := by simp [dispatch, hs]
      have hcode : s.code = b.code := by simp [s, hc]
      rw [hleft, hright0, hcode]
    · have hs : ¬ accepts s w = true := by
        intro hs
        exact hb ((hacc.mp hs).resolve_left ha)
      have hleft : dispatch [a, b] w = none := by simp [dispatch, ha, hb]
      have hright : dispatch [s] w = none := by simp [dispatch, hs]
      rw [hleft, hright]

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_FoldNeighborVersions
