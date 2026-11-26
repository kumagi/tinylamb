import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common


theorem conjunction_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Conjunction.test.emit a b).yes.getLsbD i = decide (observe Conjunction.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Conjunction.test, andBits, sqlAnd, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem conjunction_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Conjunction.test.emit a b).nul.getLsbD i = decide (observe Conjunction.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Conjunction.test, andBits, sqlAnd, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem conjunction_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Conjunction.test.emit a b).err.getLsbD i = decide (observe Conjunction.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Conjunction.test, andBits, sqlAnd, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem disjunction_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Disjunction.test.emit a b).yes.getLsbD i = decide (observe Disjunction.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Disjunction.test, andBits, negBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem disjunction_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Disjunction.test.emit a b).nul.getLsbD i = decide (observe Disjunction.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Disjunction.test, andBits, negBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem disjunction_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Disjunction.test.emit a b).err.getLsbD i = decide (observe Disjunction.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Disjunction.test, andBits, negBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem exclusiveor_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (ExclusiveOr.test.emit a b).yes.getLsbD i = decide (observe ExclusiveOr.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, ExclusiveOr.test, strictBits, strict, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem exclusiveor_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (ExclusiveOr.test.emit a b).nul.getLsbD i = decide (observe ExclusiveOr.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, ExclusiveOr.test, strictBits, strict, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem exclusiveor_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (ExclusiveOr.test.emit a b).err.getLsbD i = decide (observe ExclusiveOr.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, ExclusiveOr.test, strictBits, strict, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem boolequal_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (BoolEqual.test.emit a b).yes.getLsbD i = decide (observe BoolEqual.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, BoolEqual.test, strictBits, strict, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem boolequal_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (BoolEqual.test.emit a b).nul.getLsbD i = decide (observe BoolEqual.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, BoolEqual.test, strictBits, strict, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem boolequal_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (BoolEqual.test.emit a b).err.getLsbD i = decide (observe BoolEqual.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, BoolEqual.test, strictBits, strict, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem coalesce_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Coalesce.test.emit a b).yes.getLsbD i = decide (observe Coalesce.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Coalesce.test, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem coalesce_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Coalesce.test.emit a b).nul.getLsbD i = decide (observe Coalesce.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Coalesce.test, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem coalesce_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Coalesce.test.emit a b).err.getLsbD i = decide (observe Coalesce.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Coalesce.test, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem nullsafeequal_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (NullSafeEqual.test.emit a b).yes.getLsbD i = decide (observe NullSafeEqual.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, NullSafeEqual.test, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem nullsafeequal_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (NullSafeEqual.test.emit a b).nul.getLsbD i = decide (observe NullSafeEqual.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, NullSafeEqual.test, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem nullsafeequal_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (NullSafeEqual.test.emit a b).err.getLsbD i = decide (observe NullSafeEqual.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, NullSafeEqual.test, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem conditionalnull_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (ConditionalNull.test.emit a b).yes.getLsbD i = decide (observe ConditionalNull.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, ConditionalNull.test, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem conditionalnull_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (ConditionalNull.test.emit a b).nul.getLsbD i = decide (observe ConditionalNull.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, ConditionalNull.test, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem conditionalnull_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (ConditionalNull.test.emit a b).err.getLsbD i = decide (observe ConditionalNull.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, ConditionalNull.test, zeros_getElem, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem antipredicate_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (AntiPredicate.test.emit a b).yes.getLsbD i = decide (observe AntiPredicate.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, AntiPredicate.test, andBits, negBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem antipredicate_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (AntiPredicate.test.emit a b).nul.getLsbD i = decide (observe AntiPredicate.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, AntiPredicate.test, andBits, negBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem antipredicate_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (AntiPredicate.test.emit a b).err.getLsbD i = decide (observe AntiPredicate.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, AntiPredicate.test, andBits, negBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem implication_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Implication.test.emit a b).yes.getLsbD i = decide (observe Implication.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Implication.test, negBits, andBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem implication_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Implication.test.emit a b).nul.getLsbD i = decide (observe Implication.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Implication.test, negBits, andBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem implication_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (Implication.test.emit a b).err.getLsbD i = decide (observe Implication.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, Implication.test, negBits, andBits, zeros_getElem, sqlAnd, negate, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem ifmissingtest_yes (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (IfMissingTest.test.emit a b).yes.getLsbD i = decide (observe IfMissingTest.test a b i == .yes) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, IfMissingTest.test, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem ifmissingtest_nul (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (IfMissingTest.test.emit a b).nul.getLsbD i = decide (observe IfMissingTest.test a b i == .null) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, IfMissingTest.test, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

theorem ifmissingtest_err (a b : Bits) (ha : well a) (hb : well b) (i : Nat) (hi : i < 64) :
    (IfMissingTest.test.emit a b).err.getLsbD i = decide (observe IfMissingTest.test a b i == .fault) := by
  have h1 := congrArg (fun w : W => w.getLsbD i) ha.1
  have h2 := congrArg (fun w : W => w.getLsbD i) ha.2.1
  have h3 := congrArg (fun w : W => w.getLsbD i) ha.2.2
  have h4 := congrArg (fun w : W => w.getLsbD i) hb.1
  have h5 := congrArg (fun w : W => w.getLsbD i) hb.2.1
  have h6 := congrArg (fun w : W => w.getLsbD i) hb.2.2
  simp [BitVec.getLsbD_and] at h1 h2 h3 h4 h5 h6
  simp [observe, lane, IfMissingTest.test, zeros_getElem, BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_xor, hi]
  by_cases hye : a.yes.getLsbD i <;> by_cases hne : a.nul.getLsbD i <;> by_cases hfe : a.err.getLsbD i <;>
  by_cases hyb : b.yes.getLsbD i <;> by_cases hnb : b.nul.getLsbD i <;> by_cases hfb : b.err.getLsbD i <;> simp_all <;> native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes
