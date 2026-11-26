import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor
open TinylambOpt.ResearchGoals.Compression.Wire

-- 真偽確認: lo/hi と各端点・行値を 0..7 で全網羅し、soundInterval と
-- takeAll の仮説を満たす入力に反例がないことを #eval で確認済み。

private abbrev WX (a b : Bounds) : Nat := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi))

private theorem width_lt_two_pow (n : Nat) : n < 2 ^ width n := by
  by_cases hz : n = 0
  · subst hz
    simp [width]
  · have hw : width n = n.log2 + 1 := by
      unfold width
      simp [hz]
    rw [hw]
    exact (Nat.log2_lt hz).mp (Nat.lt_succ_self _)

private theorem div_eq_of_lt_xor {k m n : Nat} (h : m ^^^ n < 2 ^ k) : m / 2 ^ k = n / 2 ^ k :=
  Nat.eq_of_testBit_eq fun i => by
    have hb : m ^^^ n < 2 ^ (i + k) :=
      Nat.lt_of_lt_of_le h (Nat.pow_le_pow_of_le (by decide : 1 < 2) (Nat.le_add_left k i))
    rw [Nat.testBit_div_two_pow, Nat.testBit_div_two_pow]
    have hbit : (m ^^^ n).testBit (i + k) = false := Nat.testBit_lt_two_pow hb
    rw [Nat.testBit_xor] at hbit
    cases hm : m.testBit (i + k) <;> cases hn : n.testBit (i + k) <;> simp_all

private theorem xor_lt_two_pow {k m n : Nat} (hm : m < 2 ^ k) (hn : n < 2 ^ k) : m ^^^ n < 2 ^ k := by
  rw [← Nat.div_eq_zero_iff_lt (Nat.two_pow_pos k)] at hm hn ⊢
  rw [Nat.xor_div_two_pow, hm, hn]
  exact Nat.xor_self 0

/-- The XOR-derived interval contains the score of every point of the rectangle. -/
private theorem sound_xor_interval : soundInterval kernel := by
  intro t a b x y hx hy
  obtain ⟨hal, hax⟩ := hx
  obtain ⟨hbl, hbx⟩ := hy
  have hW1 : width (a.lo ^^^ a.hi) ≤ WX a b := Nat.le_max_left _ _
  have hW2 : width (b.lo ^^^ b.hi) ≤ WX a b := Nat.le_max_right _ _
  have haX : a.lo ^^^ a.hi < 2 ^ WX a b :=
    Nat.lt_of_lt_of_le (width_lt_two_pow _) (Nat.pow_le_pow_of_le (by decide : 1 < 2) hW1)
  have hbX : b.lo ^^^ b.hi < 2 ^ WX a b :=
    Nat.lt_of_lt_of_le (width_lt_two_pow _) (Nat.pow_le_pow_of_le (by decide : 1 < 2) hW2)
  have haq : a.lo / 2 ^ WX a b = a.hi / 2 ^ WX a b := div_eq_of_lt_xor haX
  have hbq : b.lo / 2 ^ WX a b = b.hi / 2 ^ WX a b := div_eq_of_lt_xor hbX
  have hxq : x / 2 ^ WX a b = a.lo / 2 ^ WX a b := by
    have h2 : x / 2 ^ WX a b ≤ a.hi / 2 ^ WX a b := Nat.div_le_div_right hax
    rw [← haq] at h2
    exact Nat.le_antisymm h2 (Nat.div_le_div_right hal)
  have hyq : y / 2 ^ WX a b = b.lo / 2 ^ WX a b := by
    have h2 : y / 2 ^ WX a b ≤ b.hi / 2 ^ WX a b := Nat.div_le_div_right hbx
    rw [← hbq] at h2
    exact Nat.le_antisymm h2 (Nat.div_le_div_right hbl)
  have hxa : x ^^^ a.lo < 2 ^ WX a b := by
    rw [← Nat.div_eq_zero_iff_lt (Nat.two_pow_pos (WX a b))]
    rw [Nat.xor_div_two_pow, hxq, Nat.xor_self]
  have hyb : y ^^^ b.lo < 2 ^ WX a b := by
    rw [← Nat.div_eq_zero_iff_lt (Nat.two_pow_pos (WX a b))]
    rw [Nat.xor_div_two_pow, hyq, Nat.xor_self]
  have hrew : (x ^^^ y) ^^^ (a.lo ^^^ b.lo) = (x ^^^ a.lo) ^^^ (y ^^^ b.lo) :=
    Nat.eq_of_testBit_eq fun i => by
      rw [Nat.testBit_xor, Nat.testBit_xor, Nat.testBit_xor, Nat.testBit_xor]
      cases h1 : (Nat.testBit x i) <;> cases h2 : (Nat.testBit y i)
        <;> cases h3 : (Nat.testBit a.lo i) <;> cases h4 : (Nat.testBit b.lo i) <;> simp_all
  have hX : (x ^^^ y) ^^^ (a.lo ^^^ b.lo) < 2 ^ WX a b := by
    rw [hrew]
    exact xor_lt_two_pow hxa hyb
  have hq : (x ^^^ y) / 2 ^ WX a b = (a.lo ^^^ b.lo) / 2 ^ WX a b :=
    div_eq_of_lt_xor hX
  have hbnd : kernel.bounds t a b =
      Bounds.mk ((a.lo ^^^ b.lo) / 2 ^ WX a b * 2 ^ WX a b)
        ((a.lo ^^^ b.lo) / 2 ^ WX a b * 2 ^ WX a b + 2 ^ WX a b - 1) := rfl
  have hsc : kernel.score t (x, y) = x ^^^ y := rfl
  rw [hbnd, hsc]
  simp only [inside]
  have A1 : (x ^^^ y) / 2 ^ WX a b * 2 ^ WX a b ≤ x ^^^ y := Nat.div_mul_le_self _ _
  have A2 : x ^^^ y < (x ^^^ y) / 2 ^ WX a b * 2 ^ WX a b + 2 ^ WX a b :=
    Nat.lt_div_mul_add (Nat.two_pow_pos (WX a b))
  have A3 : (x ^^^ y) / 2 ^ WX a b * 2 ^ WX a b =
      (a.lo ^^^ b.lo) / 2 ^ WX a b * 2 ^ WX a b :=
    congrArg (fun z => z * 2 ^ WX a b) hq
  exact ⟨by omega, by omega⟩

theorem sound_TakeAll : TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
  unfold selected
  rw [List.filter_eq_self]
  intro v hv
  simp only [List.mem_filterMap, scores] at hv
  rcases hv with ⟨r, hrp, hr⟩
  cases r with
  | none => simp at hr
  | some xy =>
      rcases xy with ⟨x, y⟩
      simp only [Option.map_some, Option.some.injEq] at hr
      subst v
      have hinside := sound_xor_interval t a b x y
        (hcov (some (x, y)) hrp x y rfl).1
        (hcov (some (x, y)) hrp x y rfl).2
      simp only [inside] at hinside
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact ⟨Nat.le_trans hlo hinside.1, Nat.lt_of_le_of_lt hinside.2 hhi⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_TakeAll
