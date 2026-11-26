import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/-
Proves `BorrowAnyByteHit`: the subtract-and-mask word test is zero exactly
when no byte is zero. A SQL literal scan may take the cheap word-wide branch
only when this equivalence holds, without changing the modeled miss result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_BorrowAnyByteHit

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

theorem boolNatEq0 (b : BitVec 8) : (b.toNat == 0) = (b == 0) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hb : b = 0 := BitVec.toNat_inj.mp (by simpa using h)
    simp [hb]
  · intro h
    have hb : b = 0 := by simpa using h
    simp [hb]

theorem boolNatEq1 (b : BitVec 8) : (b.toNat == 1) = (b == 1) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hb : b = 1 := BitVec.toNat_inj.mp (by simpa using h)
    simp [hb]
  · intro h
    have hb : b = 1 := by simpa using h
    simp [hb]

theorem boolNatEqBVec (b c : BitVec 8) : (b.toNat == c.toNat) = (b == c) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hbc : b = c := BitVec.toNat_inj.mp (by simpa using h)
    simp [hbc]
  · intro h
    have hbc : b = c := by simpa using h
    simp [hbc]

theorem lane_bit0 (x : W) (i : Nat) : (lane x i == 0) = (x.extractLsb' (8*i) 8 == 0) := by
  unfold lane; exact boolNatEq0 _

theorem lane_bitBV (x : W) (i : Nat) (b : BitVec 8) :
    (lane x i == b.toNat) = (x.extractLsb' (8*i) 8 == b) := by
  unfold lane; exact boolNatEqBVec _ _

theorem find_range_eq {p q : Nat → Bool} {n : Nat}
    (hqp : ∀ i, i < n → q i = true → p i = true)
    (hpq : ∀ i, i < n → p i = true → ∃ j, j < n ∧ j ≤ i ∧ q j = true) :
    (List.range n).find? p = (List.range n).find? q := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.find?_append, List.find?_append]
      have ih' := ih
        (by intro i hi hq; exact hqp i (by omega) hq)
        (by intro i hi hp; rcases hpq i (by omega) hp with ⟨j, hjn, hji, hqj⟩; exact ⟨j, by omega, hji, hqj⟩)
      cases hfp : (List.range n).find? p with
      | some k =>
          have hqf : (List.range n).find? q = some k := ih'.symm.trans hfp
          rw [hqf]
          simp
      | none =>
          have hpnone : ∀ i, i ∈ List.range n → ¬ p i = true := List.find?_eq_none.mp hfp
          have hqnone : ∀ i, i < n → ¬ q i = true := by
            intro i hi hqi
            exact hpnone i (List.mem_range.mpr hi) (hqp i (by omega) hqi)
          have hqf : (List.range n).find? q = none := List.find?_eq_none.mpr (by
            intro i hi; exact hqnone i (List.mem_range.mp hi))
          rw [hqf]
          have hniff : p n = q n := by
            apply Bool.eq_iff_iff.mpr
            constructor
            · intro hpn
              rcases hpq n (by omega) hpn with ⟨j, hjn, hji, hqj⟩
              by_cases hjl : j < n
              · exact False.elim (hqnone j hjl hqj)
              · have hje : j = n := by omega
                subst j
                exact hqj
            · intro hqn
              exact hqp n (by omega) hqn
          simp [hniff]

def zeroUpTo (x : W) : Nat → Bool
  | 0 => (zeroBytes x).getLsbD 7
  | n + 1 => zeroUpTo x n || (zeroBytes x).getLsbD (8*(n+1)+7)

theorem zeroUpTo_iff (x : W) : ∀ i, zeroUpTo x i = true ↔ ∃ j, j ≤ i ∧ (zeroBytes x).getLsbD (8*j+7) = true := by
  intro i
  induction i with
  | zero => simp [zeroUpTo]
  | succ i ih =>
      constructor
      · intro h
        have h' := Bool.or_eq_true_iff.mp (by simpa [zeroUpTo] using h)
        rcases h' with hz | hn
        · rcases (ih.mp hz) with ⟨j, hji, hj⟩
          exact ⟨j, by omega, hj⟩
        · exact ⟨i+1, by omega, hn⟩
      · rintro ⟨j, hji, hj⟩
        by_cases hji' : j = i + 1
        · subst j
          exact Bool.or_eq_true_iff.mpr (Or.inr hj)
        · exact Bool.or_eq_true_iff.mpr (Or.inl (ih.mpr ⟨j, by omega, hj⟩))

theorem zero_imp_fast (x : W) (i : Nat) (hi : i < 8) :
    (zeroBytes x).getLsbD (8*i+7) = true → (fast x).getLsbD (8*i+7) = true := by
  have c : i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by omega
  rcases c with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  all_goals
    unfold fast zeroBytes ones high low7
    bv_decide

theorem fast_imp_zeroUpTo (x : W) (i : Nat) (hi : i < 8) :
    (fast x).getLsbD (8*i+7) = true → zeroUpTo x i = true := by
  have c : i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by omega
  rcases c with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  all_goals
    simp only [fast, zeroBytes, ones, high, low7, zeroUpTo]
    bv_decide

theorem zero_lane_char (x : W) (i : Nat) (hi : i < 8) :
    (zeroBytes x).getLsbD (8*i+7) = (lane x i == 0) := by
  have c : i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by omega
  rcases c with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  all_goals
    unfold zeroBytes low7
    simp [lane_bit0]
    bv_decide

theorem xor_repeat_lane_zero (x : W) (b : BitVec 8) (i : Nat) (hi : i < 8) :
    (lane (x ^^^ repeatByte b) i == 0) = (lane x i == b.toNat) := by
  have c : i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by omega
  rcases c with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  all_goals
    rw [lane_bit0, lane_bitBV]
    simp [repeatByte, ones]
    bv_decide

theorem any_range_prefix_eq {p q : Nat → Bool} {m n : Nat} (_hmn : m ≤ n)
    (hqp : ∀ i, i < n → q i = true → p i = true)
    (hpq : ∀ i, i < n → p i = true → ∃ j, j < n ∧ j ≤ i ∧ q j = true) :
    ((List.range n).filter p).any (fun i => decide (i < m)) =
    ((List.range n).filter q).any (fun i => decide (i < m)) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [List.any_eq_true] at h ⊢
    rcases h with ⟨i, hi, him⟩
    rw [List.mem_filter] at hi
    rcases hi with ⟨hir, hpi⟩
    have hin : i < n := List.mem_range.mp hir
    have him' : i < m := by simpa using him
    rcases hpq i hin hpi with ⟨j, hjn, hji, hqj⟩
    refine ⟨j, ?_, ?_⟩
    · rw [List.mem_filter]
      exact ⟨List.mem_range.mpr hjn, hqj⟩
    · simp [show j < m by omega]
  · intro h
    rw [List.any_eq_true] at h ⊢
    rcases h with ⟨i, hi, him⟩
    rw [List.mem_filter] at hi
    rcases hi with ⟨hir, hqi⟩
    have hin : i < n := List.mem_range.mp hir
    have him' : i < m := by simpa using him
    refine ⟨i, ?_, him⟩
    rw [List.mem_filter]
    exact ⟨hir, hqp i hin hqi⟩

theorem any_lt_of_gt8 {l : List Nat} (hl : ∀ i ∈ l, i < 8) (hn : 8 < n) :
    l.any (fun i => decide (i < n)) = l.any (fun i => decide (i < 8)) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [List.any_eq_true] at h ⊢
    rcases h with ⟨i, hi, hit⟩
    refine ⟨i, hi, ?_⟩
    simp [hl i hi]
  · intro h
    rw [List.any_eq_true] at h ⊢
    rcases h with ⟨i, hi, hit⟩
    refine ⟨i, hi, ?_⟩
    have hi8 : i < 8 := hl i hi
    simp [show i < n by omega]

theorem proof : BorrowAnyByteHit.Goal := by
  unfold BorrowAnyByteHit.Goal fast zeroBytes ones high low7
  intro x
  bv_decide

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks.Goal_BorrowAnyByteHit
