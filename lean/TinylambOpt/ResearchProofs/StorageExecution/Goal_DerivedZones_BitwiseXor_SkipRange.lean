import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Truth check on small inputs: for every rectangle inside [0,8)^2 and every
range [lo,hi), either the certified interval allows the range, or the full
rectangle page has no selected score. -/
private def smallPage (a b : Bounds) : List (Option Row) :=
  (List.range 8).flatMap fun x =>
    (List.range 8).map fun y =>
      if (decide (a.lo ≤ x ∧ x ≤ a.hi)) && (decide (b.lo ≤ y ∧ y ≤ b.hi)) then some (x, y) else none

private def checkSmall : Bool :=
  (List.range 8).all fun alo =>
  (List.range 8).all fun ahi =>
  (List.range 8).all fun blo =>
  (List.range 8).all fun bhi =>
  (List.range 8).all fun lo =>
  (List.range 8).all fun hi =>
    let a : Bounds := ⟨alo, ahi⟩
    let b : Bounds := ⟨blo, bhi⟩
    allows (kernel.bounds 0 a b) lo hi || (selected kernel 0 (smallPage a b) lo hi == [])

example : checkSmall = true := by native_decide

/-- `width n ≤ w` implies `n` fits in `w` bits. -/
private theorem lt_two_pow_of_width_le {n w : Nat} (h : width n ≤ w) : n < 2 ^ w := by
  by_cases hn : n = 0
  · subst n
    exact Nat.two_pow_pos w
  · have hw : n.log2 + 1 ≤ w := by simpa [width, hn] using h
    exact (Nat.log2_lt hn).mp (by omega)

private theorem eq_of_xor_eq_zero {a b : Nat} (h : a ^^^ b = 0) : a = b := by
  calc
    a = a ^^^ 0 := (Nat.xor_zero a).symm
    _ = a ^^^ (b ^^^ b) := by rw [Nat.xor_self b]
    _ = (a ^^^ b) ^^^ b := (Nat.xor_assoc a b b).symm
    _ = b := by rw [h, Nat.zero_xor]

private theorem div_eq_of_xor_lt {a b w : Nat} (h : a ^^^ b < 2 ^ w) :
    a / 2 ^ w = b / 2 ^ w := by
  apply eq_of_xor_eq_zero
  rw [← Nat.xor_div_two_pow]
  exact Nat.div_eq_of_lt h

/-- The high bits of XOR on a rectangle are determined by the corner
`(a.lo, b.lo)`, so the computed interval is sound. -/
private theorem soundInterval_bitwiseXor : soundInterval kernel := by
  intro t a b x y hax hby
  let w := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi))
  let base := ((a.lo ^^^ b.lo) / 2 ^ w) * 2 ^ w
  change base ≤ x ^^^ y ∧ x ^^^ y ≤ base + 2 ^ w - 1
  have hd : 0 < 2 ^ w := Nat.two_pow_pos w
  have hwa : width (a.lo ^^^ a.hi) ≤ w := Nat.le_max_left _ _
  have hwb : width (b.lo ^^^ b.hi) ≤ w := Nat.le_max_right _ _
  have hqax : a.lo / 2 ^ w = a.hi / 2 ^ w :=
    div_eq_of_xor_lt (lt_two_pow_of_width_le hwa)
  have hqby : b.lo / 2 ^ w = b.hi / 2 ^ w :=
    div_eq_of_xor_lt (lt_two_pow_of_width_le hwb)
  have hqxx : a.lo / 2 ^ w = x / 2 ^ w :=
    Nat.le_antisymm (Nat.div_le_div_right (c := 2 ^ w) hax.1)
      (by rw [hqax]; exact Nat.div_le_div_right (c := 2 ^ w) hax.2)
  have hqyy : b.lo / 2 ^ w = y / 2 ^ w :=
    Nat.le_antisymm (Nat.div_le_div_right (c := 2 ^ w) hby.1)
      (by rw [hqby]; exact Nat.div_le_div_right (c := 2 ^ w) hby.2)
  have hq : (x ^^^ y) / 2 ^ w = base / 2 ^ w := by
    rw [Nat.xor_div_two_pow, ← hqxx, ← hqyy]
    change (a.lo / 2 ^ w) ^^^ (b.lo / 2 ^ w) =
      (((a.lo ^^^ b.lo) / 2 ^ w) * 2 ^ w) / 2 ^ w
    rw [Nat.mul_div_left _ hd]
    exact (Nat.xor_div_two_pow (a := a.lo) (b := b.lo) (n := w)).symm
  have hm : (x ^^^ y) % 2 ^ w < 2 ^ w := by
    rw [Nat.xor_mod_two_pow]
    exact Nat.xor_lt_two_pow (Nat.mod_lt x hd) (Nat.mod_lt y hd)
  have hbase : base = (base / 2 ^ w) * 2 ^ w := by
    simp [base, Nat.mul_div_left _ hd]
  have hdec := Nat.div_add_mod (x ^^^ y) (2 ^ w)
  rw [hq, Nat.mul_comm (2 ^ w) (base / 2 ^ w)] at hdec
  constructor
  · rw [hbase]
    omega
  · rw [hbase]
    omega

/-- Every value of `scores` comes from a non-NULL row of the page. -/
private theorem score_mem {f : Kernel} {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores f t p) :
    ∃ r ∈ p, ∃ x y, r = some (x, y) ∧ v = f.score t (x, y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

/-- A certified scalar interval justifies dropping a page for a range test. -/
private theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc hn
  apply List.filter_eq_nil_iff.mpr
  intro v hv hpass
  have hpred : (lo ≤ v && v < hi) = true := hpass
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, hvxy⟩
  have hcov := hc r hr x y hrxy
  have hb : inside (f.bounds t a b) v := by
    simpa [hvxy] using hs t a b x y hcov.1 hcov.2
  simp only [inside] at hb
  simp [allows] at hn
  omega

theorem sound_SkipRange : BitwiseXor.SkipRange.Goal :=
  skipRange_of_sound soundInterval_bitwiseXor

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SkipRange
