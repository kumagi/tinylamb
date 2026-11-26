import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-! Shared byte-mask lemmas for the JitKernels.BorrowMasks audit. They connect
the cheap borrow mask to the exact zero-byte mask at the level of the first and
last marked lane, the prefix membership test, and a replicated needle. SQL text
execution can use these facts to skip scalar equality checks without changing
which row is delivered as the next match. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.BorrowMasksLemmas

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.SwarDigitBallot

private theorem high_lsb (p : Nat) (hp : p < 64) :
    high.getLsbD p = decide (p % 8 = 7) := by
  exact (show ∀ p : Fin 64, high.getLsbD p.val = decide (p.val % 8 = 7) from by
    native_decide) ⟨p, hp⟩

theorem high_only_ctz_mod (m : W) (honly : m &&& ~~~high = 0) (hz : m ≠ 0) :
    m.ctz.toNat % 8 = 7 := by
  have hset : m.getLsbD m.ctz.toNat = true := BitVec.getLsbD_true_ctz_of_ne_zero hz
  have hp : m.ctz.toNat < 64 := BitVec.lt_of_getLsbD hset
  have hself : m &&& high = m := by bv_decide
  have hget := congrArg (fun z : W => z.getLsbD m.ctz.toNat) hself
  rw [BitVec.getLsbD_and, hset] at hget
  have hhigh : high.getLsbD m.ctz.toNat = true := by simpa using hget
  have hdec : decide (m.ctz.toNat % 8 = 7) = true := by
    rw [← high_lsb m.ctz.toNat hp]
    exact hhigh
  simpa using hdec

theorem marked_head_eq_ctz (m : W) (honly : m &&& ~~~high = 0) :
    (marked m).head? = if m = 0 then none else some (m.ctz.toNat / 8) := by
  by_cases hz : m = 0
  · simp [marked, hz]
  · have hset : m.getLsbD m.ctz.toNat = true := BitVec.getLsbD_true_ctz_of_ne_zero hz
    have hp : m.ctz.toNat < 64 := BitVec.lt_of_getLsbD hset
    have hmod := high_only_ctz_mod m honly hz
    let q := m.ctz.toNat / 8
    have hp_eq : m.ctz.toNat = 8 * q + 7 := by
      dsimp [q]
      omega
    have hq : q < 8 := by omega
    rw [marked, List.head?_filter]
    rw [ite_eq_right hz]
    change List.find? (fun i => m.getLsbD (8 * i + 7)) (List.range 8) = some q
    apply List.find?_range_eq_some.mpr
    refine ⟨?_, List.mem_range.mpr hq, ?_⟩
    · simpa [hp_eq] using hset
    · intro j hj
      have hjlt : 8 * j + 7 < m.ctz.toNat := by omega
      have hfalse : m.getLsbD (8 * j + 7) = false := BitVec.getLsbD_false_of_lt_ctz hjlt
      simp [hfalse]

theorem fast_high_only (x : W) : fast x &&& ~~~high = 0 := by
  simp [fast, ones, high]
  bv_decide

theorem zeroBytes_high_only (x : W) : zeroBytes x &&& ~~~high = 0 := by
  simp [zeroBytes, high, low7]
  bv_decide

theorem fast_zero_iff (x : W) : fast x = 0 ↔ zeroBytes x = 0 := by
  have h : (fast x == 0) = (zeroBytes x == 0) := by
    simp [fast, zeroBytes, ones, low7, high]
    bv_decide
  constructor <;> intro hx
  · simpa [hx] using h
  · simpa [hx] using h.symm

theorem fast_ctz_eq (x : W) : (fast x).ctz = (zeroBytes x).ctz := by
  simp [fast, zeroBytes, ones, low7, high]
  bv_decide

private theorem cases8 (i : Nat) (hi : i < 8) :
    i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by
  omega

theorem lane_eq_zero_iff (a : BitVec 8) : a.toNat = 0 ↔ a = 0 := by
  constructor
  · intro h; apply BitVec.eq_of_toNat_eq; simpa using h
  · intro h; simp [h]

theorem lane_eq_nat_iff (a b : BitVec 8) : a.toNat = b.toNat ↔ a = b := by
  constructor
  · intro h; exact BitVec.eq_of_toNat_eq h
  · intro h; simp [h]

theorem zeroBytes_lsb (y : W) (i : Nat) (hi : i < 8) :
    (zeroBytes y).getLsbD (8*i+7) = decide (y.extractLsb' (8*i) 8 = (0 : BitVec 8)) := by
  rcases cases8 i hi with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    simp [zeroBytes, low7] <;> bv_decide

theorem repeatByte_extract (b : BitVec 8) (i : Nat) (hi : i < 8) :
    (repeatByte b).extractLsb' (8*i) 8 = b := by
  rcases cases8 i hi with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    simp [repeatByte, ones] <;> bv_decide


theorem xor_eq_zero_iff_nat_eq (a b : BitVec 8) :
    decide (a ^^^ b = 0) = decide (a.toNat == b.toNat) := by
  by_cases h : a = b
  · subst b
    simp
  · have hn : a.toNat ≠ b.toNat := by
      intro hn
      exact h (BitVec.eq_of_toNat_eq hn)
    simp [h, hn]

theorem marked_zeroBytes_xor (x : W) (b : BitVec 8) :
    marked (zeroBytes (x ^^^ repeatByte b)) =
      (List.range 8).filter (fun i => lane x i == b.toNat) := by
  unfold marked
  apply List.filter_congr
  intro i hi
  have hi8 : i < 8 := List.mem_range.mp hi
  rw [zeroBytes_lsb (x ^^^ repeatByte b) i hi8]
  rw [BitVec.extractLsb'_xor, repeatByte_extract b i hi8]
  change decide ((x.extractLsb' (8*i) 8) ^^^ b = 0) = decide (lane x i == b.toNat)
  change decide ((x.extractLsb' (8*i) 8) ^^^ b = 0) =
    decide ((x.extractLsb' (8*i) 8).toNat == b.toNat)
  exact xor_eq_zero_iff_nat_eq (x.extractLsb' (8*i) 8) b

def prefixMask : Nat → W
  | 0 => 0
  | n+1 => prefixMask n ||| ((1 : W) <<< (8*n+7))

private theorem cases9 (n : Nat) (hn : n ≤ 8) :
    n = 0 ∨ n = 1 ∨ n = 2 ∨ n = 3 ∨ n = 4 ∨ n = 5 ∨ n = 6 ∨ n = 7 ∨ n = 8 := by
  omega

theorem marked_any_lt_eq_mask (m : W) (n : Nat) (hn : n ≤ 8) :
    (marked m).any (· < n) = ((m &&& prefixMask n) != 0) := by
  rcases cases9 n hn with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    simp [marked, prefixMask, List.range_succ, List.any_cons] <;> bv_decide

theorem prefix_fast_zero_iff (x : W) (n : Nat) (hn : n ≤ 8) :
    (fast x &&& prefixMask n == 0) = (zeroBytes x &&& prefixMask n == 0) := by
  rcases cases9 n hn with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    simp only [prefixMask, fast, zeroBytes, ones, low7, high] <;> bv_decide


theorem prefix_fast_any_eq (x : W) (n : Nat) (hn : n ≤ 8) :
    ((fast x &&& prefixMask n) != 0) = ((zeroBytes x &&& prefixMask n) != 0) := by
  exact congrArg Bool.not (prefix_fast_zero_iff x n hn)


theorem any_lt_of_mem_lt8 {l : List Nat} (hl : ∀ i ∈ l, i < 8) {n : Nat} (hn : 8 ≤ n) :
    l.any (· < n) = l.any (· < 8) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rcases List.any_eq_true.mp h with ⟨i, hi, hp⟩
    have hi8 : i < 8 := hl i hi
    exact List.any_eq_true.mpr ⟨i, hi, by simp [hi8]⟩
  · intro h
    rcases List.any_eq_true.mp h with ⟨i, hi, hp⟩
    have hi8 : i < 8 := hl i hi
    have hin : i < n := Nat.lt_of_lt_of_le hi8 hn
    exact List.any_eq_true.mpr ⟨i, hi, by simp [hin]⟩

theorem marked_mem_lt8 {m : W} {i : Nat} (hi : i ∈ marked m) : i < 8 := by
  unfold marked at hi
  exact List.mem_range.mp (List.mem_filter.mp hi).1

theorem marked_any_lt_eq_mask_all (m : W) (n : Nat) :
    (marked m).any (· < n) = ((m &&& prefixMask (min n 8)) != 0) := by
  by_cases hn : n ≤ 8
  · have hmin : min n 8 = n := Nat.min_eq_left hn
    simp [hmin, marked_any_lt_eq_mask m n hn]
  · have hn8 : 8 ≤ n := by omega
    have hmin : min n 8 = 8 := Nat.min_eq_right hn8
    rw [hmin]
    rw [any_lt_of_mem_lt8 (l := marked m) (fun i hi => marked_mem_lt8 (m := m) hi) hn8]
    exact marked_any_lt_eq_mask m 8 (by omega)

end Nov26.BorrowMasksLemmas
