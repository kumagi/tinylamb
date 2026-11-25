import TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.C_Wire_B

open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Wire.BitPackBreakEven
`byteCount x = (x+7)/8` is `⌈x/8⌉`; the strict inequality
`overhead + ⌈rows*bits/8⌉ < rows*oldBytes` is equivalent to
`rows*bits ≤ 8*(rows*oldBytes - overhead - 1)` because `⌈y/8⌉ ≤ k ↔ y ≤ 8k`
for `y = rows*bits + 7`. `omega` discharges it after `byteCount` is unfolded
(it abstracts the nonlinear products `rows*bits`, `rows*oldBytes` as atoms). -/

theorem BitPackBreakEven_sound :
    TinylambOpt.ResearchGoals.Compression.Wire.BitPackBreakEven.Goal := by
  intro rows bits oldBytes overhead h
  simp only [byteCount]
  omega

/-! ## Wire.OneBitSaving
`n*w = n*(w-1) + n` for `w ≥ 1`, so `byteCount (n*w) = ((n*(w-1)+7) + n)/8`.
For any `a, n`, `(a+n)/8 ≥ a/8 + n/8` (write `a+7 = 8q+r`, `0≤r<8`; then
`(a+n+7)/8 = q + (r+n)/8 ≥ q + n/8`). The hypotheses `8 ≤ n`, `0 < w` are
stronger than needed; only `0 < w` is used to get `n*(w-1)+n = n*w`. -/

theorem OneBitSaving_sound :
    TinylambOpt.ResearchGoals.Compression.Wire.OneBitSaving.Goal := by
  intro n w _hn hw
  have hmul : n * (w - 1) + n = n * w := by
    obtain ⟨k, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (by omega : w ≠ 0)
    rw [Nat.succ_sub_one, Nat.mul_succ]
  simp only [byteCount]
  omega

/-! ## Wire.SplitPenalty
`byteCount` is `⌈·/8⌉`: splitting a bit string into two pieces changes the
byte count by at most one in either direction. Both bounds are linear
(in `a`, `b`) once `byteCount` is unfolded, so `omega` closes both halves. -/

theorem SplitPenalty_sound :
    TinylambOpt.ResearchGoals.Compression.Wire.SplitPenalty.Goal := by
  intro a b
  simp only [byteCount]
  constructor <;> omega

/-! ## Wire.AlignmentDebt
`align unit n` rounds `n` up to a multiple of `unit`. Key facts for
`0 < unit` (proved from `align unit n = ((n+unit-1)/unit)*unit` via
`Nat.div_add_mod`/`Nat.mod_lt`):
- `align_ge`: `n ≤ align unit n`;
- `align_le`: `align unit n ≤ n + unit - 1`;
- `align_dvd`: `unit ∣ align unit n`;
- `align_mono`: monotonicity;
- `align_fixed`: `align unit m = m` when `unit ∣ m`;
- `align_add_le`: subadditivity `align (a+b) ≤ align a + align b`;
- `align_pair`: `align a + align b ≤ align (a+b) + unit`
  (the difference is a multiple of `unit` below `2*unit`).

List induction then gives `Σ align(sᵢ) ≤ align(Σ sᵢ) + (k-1)*unit`
(the singleton tail is handled separately because the pair bound's `+unit`
slack does not fit `(k-1)*unit` at `k = 1`), and `Nat.sub_le_iff_le_add`
turns it into the required subtraction form. -/

theorem align_ge {unit : Nat} (hu : 0 < unit) (n : Nat) : n ≤ align unit n := by
  have hdiv := Nat.div_add_mod (n + unit - 1) unit
  rw [Nat.mul_comm unit ((n + unit - 1) / unit)] at hdiv
  have hmod := Nat.mod_lt (n + unit - 1) hu
  unfold align
  omega

theorem align_le {unit : Nat} (hu : 0 < unit) (n : Nat) :
    align unit n ≤ n + unit - 1 := by
  have hdiv := Nat.div_add_mod (n + unit - 1) unit
  rw [Nat.mul_comm unit ((n + unit - 1) / unit)] at hdiv
  have hmod := Nat.mod_lt (n + unit - 1) hu
  unfold align
  omega

theorem align_dvd {unit : Nat} (n : Nat) : unit ∣ align unit n := by
  unfold align
  exact ⟨(n + unit - 1) / unit, by rw [Nat.mul_comm]⟩

theorem align_mono {unit : Nat} {a b : Nat} (h : a ≤ b) :
    align unit a ≤ align unit b := by
  unfold align
  apply Nat.mul_le_mul_right
  apply Nat.div_le_div_right
  have h1 : a + unit ≤ b + unit := Nat.add_le_add_right h unit
  exact Nat.sub_le_sub_right h1 1

theorem align_fixed {unit : Nat} (hu : 0 < unit) {m : Nat}
    (hm : unit ∣ m) : align unit m = m := by
  obtain ⟨t, rfl⟩ := hm
  unfold align
  have h1 : unit * t + unit - 1 = unit * t + (unit - 1) := by omega
  rw [h1, Nat.add_comm (unit * t) (unit - 1), Nat.add_mul_div_left _ _ hu]
  rw [Nat.div_eq_of_lt (by omega : unit - 1 < unit), Nat.zero_add]
  exact Nat.mul_comm t unit

theorem align_add_le {unit : Nat} (hu : 0 < unit) (a b : Nat) :
    align unit (a + b) ≤ align unit a + align unit b := by
  have h3 : a + b ≤ align unit a + align unit b :=
    Nat.add_le_add (align_ge hu a) (align_ge hu b)
  have h4 := align_mono (unit := unit) h3
  rw [align_fixed hu (Nat.dvd_add (align_dvd a) (align_dvd b))] at h4
  exact h4

theorem align_pair {unit : Nat} (hu : 0 < unit) (a b : Nat) :
    align unit a + align unit b ≤ align unit (a + b) + unit := by
  have hdvd : unit ∣ (align unit a + align unit b) - align unit (a + b) :=
    Nat.dvd_sub (Nat.dvd_add (align_dvd a) (align_dvd b)) (align_dvd (a + b))
  have hge := align_ge hu (a + b)
  have hle1 := align_le hu a
  have hle2 := align_le hu b
  have hsum : align unit a + align unit b ≤ a + b + 2 * unit - 2 := by omega
  have hd_le : (align unit a + align unit b) - align unit (a + b) ≤
      2 * unit - 2 := by
    have h1 := Nat.sub_le_sub_left hge (align unit a + align unit b)
    have h2 := Nat.sub_le_sub_right hsum (a + b)
    omega
  obtain ⟨t, ht⟩ := hdvd
  rw [ht] at hd_le
  have hlt : unit * t < unit * 2 := by omega
  have ht2 : t < 2 := (Nat.mul_lt_mul_left hu).mp hlt
  have ht1 : t ≤ 1 := by omega
  have hd_le_u : (align unit a + align unit b) - align unit (a + b) ≤ unit := by
    rw [ht]
    have hmul : unit * t ≤ unit * 1 := Nat.mul_le_mul_left unit ht1
    rw [Nat.mul_one] at hmul
    omega
  rw [Nat.sub_le_iff_le_add] at hd_le_u
  omega

theorem align_list_bound {unit : Nat} (hu : 0 < unit) (sizes : List Nat) :
    (sizes.map (align unit)).sum ≤
      align unit sizes.sum + (sizes.length - 1) * unit := by
  induction sizes with
  | nil => simp
  | cons x xs ih =>
    cases xs with
    | nil => simp
    | cons y ys =>
      simp only [List.map_cons, List.sum_cons, List.length_cons,
        Nat.add_sub_cancel] at ih ⊢
      have hp := align_pair hu x (y + ys.sum)
      have hlen : (ys.length + 1) * unit = ys.length * unit + unit := by
        rw [Nat.add_mul, Nat.one_mul]
      omega

theorem AlignmentDebt_sound :
    TinylambOpt.ResearchGoals.Compression.Wire.AlignmentDebt.Goal := by
  intro unit sizes hu
  have h := align_list_bound hu sizes
  rw [Nat.sub_le_iff_le_add]
  omega

/-! ## Wire.MeanWidth — REFUTED

The goal claims the bit-packed encoding is unchanged when the lane width is
chosen from the *mean* value `xs.sum / xs.length` instead of the peak.
Counterexample `xs = [1, 2]`: the mean is `3/2 = 1`, so `width 1 = 1`,
while `peak = 2` gives `width 2 = 2`. Then
`pack 1 [1,2] = [1]` but `pack 2 [1,2] = [9]` — the encodings differ
(`word 1 [1,2] = 1`, `word 2 [1,2] = 9`, both one byte). A single large
outlier inflates the peak width while the mean stays small, so mean-based
width selection loses information; the goal is false. -/

theorem MeanWidth_disproof :
    ¬ TinylambOpt.ResearchGoals.Compression.Wire.MeanWidth.Goal := by
  intro h
  have h2 := h [1, 2] (by decide)
  revert h2
  decide

end TinylambOpt.ResearchProofs.DevinBatch.C_Wire_B
