import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_BitGuardImpliesWeaker
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch BitGuardImpliesWeaker

/-- Case split on a Boolean-valued term as pure equations. -/
private theorem bool_cases (b : Bool) : b = true ∨ b = false := by
  cases b <;> simp

/-- Pure-Boolean core of guard subsumption (kernel-evaluated over all bit assignments). -/
private theorem mono_bool (A B W U V : Bool)
    (hinc : (A && B) = A)
    (hnc : (((V ^^ U) && A) && B) = false)
    (hb : (W && B) = (U && B)) : (W && A) = (V && A) := by
  revert A B W U V
  decide

/-- A bit ignored by `b`'s mask cannot be tested by `a`'s: flipping it preserves `b`'s accept
equation for `b.value` but breaks `a`'s. -/
private theorem flip_bool (A U V : Bool)
    (h1 : (U && A) = (V && A))
    (h2 : ((U ^^ true) && A) = (V && A)) : A = false := by
  revert A U V
  decide

/-- Where `a`'s mask tests a bit, acceptance of `b.value` forces the two values to agree there. -/
private theorem xor_contra (A B U V : Bool) (h1 : (U && A) = (V && A)) :
    (((V ^^ U) && A) && B) = false := by
  revert A B U V
  decide

/-- The single set bit of `1#64 <<< i` sits exactly at position `i`. -/
private theorem bit_single (i : Nat) (hi : i < 64) (k : Nat) (hk : k < 64) :
    ((1#64) <<< i).getLsbD k = decide (i = k) := by
  rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_one]
  have h1 : decide (k < 64) = true := decide_eq_true hk
  have h3 : decide (0 < 64) = true := rfl
  rcases Nat.lt_trichotomy k i with h | rfl | h
  · rw [h1, decide_eq_true h, h3, decide_eq_true (Nat.sub_eq_zero_of_le (Nat.le_of_lt h)),
      decide_eq_false (show ¬ i = k from by omega)]
    decide
  · rw [h1, h3, decide_eq_false (show ¬ k < k from by omega),
      decide_eq_true (show k - k = 0 by omega), decide_eq_true (show k = k from rfl)]
    decide
  · rw [h1, h3, decide_eq_false (show ¬ k < i from by omega),
      decide_eq_false (show ¬ k - i = 0 from by omega),
      decide_eq_false (show ¬ i = k from by omega)]
    decide

/-- A word matches its own guard. -/
private theorem accepts_own (e : Entry) : accepts e e.value := by
  simp [accepts]

/-- Flipping a bit that `b`'s mask ignores leaves `b.value` accepted by `b`. -/
private theorem accepts_flip (b : Entry) (i : Nat) (hi : i < 64)
    (hbm : b.mask.getLsbD i = false) :
    accepts b (b.value ^^^ (1#64 <<< i)) := by
  have key : (b.value ^^^ (1#64 <<< i)) &&& b.mask = b.value &&& b.mask := by
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro k hk
    rw [BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_xor, bit_single i hi k hk]
    by_cases h : i = k
    · subst h
      simp [hbm]
    · rw [decide_eq_false h, Bool.xor_false]
  show (b.value ^^^ (1#64 <<< i)) &&& b.mask == b.value &&& b.mask
  rw [key]
  simp

/--
`BitGuardImpliesWeaker.Goal`: `weaker a b` holds exactly when the guarded version `a` accepts
every word that `b` accepts, so a dispatch chain may drop the dominated second guard test behind
`a`. Forward: per bit, mask containment and value agreement on the intersection convert `b`'s
accept equation into `a`'s (`mono_bool`). Backward: the witnesses `b.value` and `b.value` with
bit `i` flipped (`flip_bool`) show `a`'s mask cannot contain a bit that `b`'s mask ignores, and
the values cannot disagree where both masks look (`xor_contra`).
-/
theorem sound : Goal := by
  intro a b
  constructor
  · intro hw w hb
    obtain ⟨hinc, hnc⟩ : a.mask &&& b.mask = a.mask
        ∧ ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
      simpa [weaker, clash] using hw
    have hb' : (w &&& b.mask) = (b.value &&& b.mask) := by simpa [accepts] using hb
    have key : (w &&& a.mask) = (a.value &&& a.mask) := by
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i _
      have hincb : (a.mask.getLsbD i && b.mask.getLsbD i) = a.mask.getLsbD i := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hinc
      have hncb : (((a.value.getLsbD i ^^ b.value.getLsbD i) && a.mask.getLsbD i)
          && b.mask.getLsbD i) = false := by
        simpa [BitVec.getLsbD_and, BitVec.getLsbD_xor] using
          congrArg (fun v : Word => v.getLsbD i) hnc
      have hbb : (w.getLsbD i && b.mask.getLsbD i) = (b.value.getLsbD i && b.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) hb'
      rw [BitVec.getLsbD_and, BitVec.getLsbD_and]
      exact mono_bool _ _ _ _ _ hincb hncb hbb
    simpa [accepts] using key
  · intro hall
    have h1eq : (b.value &&& a.mask) = (a.value &&& a.mask) := by
      simpa [accepts] using hall b.value (accepts_own b)
    have hinc : a.mask &&& b.mask = a.mask := by
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i hi
      have h1bit : (b.value.getLsbD i && a.mask.getLsbD i)
          = (a.value.getLsbD i && a.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) h1eq
      obtain hbm | hbm := bool_cases (b.mask.getLsbD i)
      · rw [BitVec.getLsbD_and, hbm, Bool.and_true]
      · have h2eq : ((b.value ^^^ (1#64 <<< i)) &&& a.mask) = (a.value &&& a.mask) := by
          simpa [accepts] using hall _ (accepts_flip b i hi hbm)
        have h2bit : ((b.value.getLsbD i ^^ true) && a.mask.getLsbD i)
            = (a.value.getLsbD i && a.mask.getLsbD i) := by
          have e2 := congrArg (fun v : Word => v.getLsbD i) h2eq
          rw [BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_xor,
            bit_single i hi i hi, decide_eq_true (show i = i from rfl)] at e2
          exact e2
        rw [BitVec.getLsbD_and, flip_bool _ _ _ h1bit h2bit, hbm]
        rfl
    have hnc : ((a.value ^^^ b.value) &&& a.mask &&& b.mask) = 0 := by
      apply BitVec.eq_of_getLsbD_eq_iff.mpr
      intro i _
      have h1bit : (b.value.getLsbD i && a.mask.getLsbD i)
          = (a.value.getLsbD i && a.mask.getLsbD i) := by
        simpa [BitVec.getLsbD_and] using congrArg (fun v : Word => v.getLsbD i) h1eq
      have h := xor_contra (a.mask.getLsbD i) (b.mask.getLsbD i)
        (b.value.getLsbD i) (a.value.getLsbD i) h1bit
      simp only [BitVec.getLsbD_and, BitVec.getLsbD_xor]
      rw [h]
      exact (BitVec.getLsbD_zero (w := 64) (i := i)).symm
    simp [weaker, clash, hinc, hnc]

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch_BitGuardImpliesWeaker
