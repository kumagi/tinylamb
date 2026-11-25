import TinylambOpt.ResearchGoals.Compression.Predictors

/-
  SideResidue predicts the current value from an affine function of the
  reference (side) column.  The predictor never reads the history, so a
  truncated restart seed is indistinguishable from the full one, and adding a
  whole period to the reference column leaves every residue unchanged.
-/

namespace TinylambOpt.ResearchProofs.Compression.Predictors.SideResidue

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.SideResidue

set_option maxHeartbeats 0
set_option maxRecDepth 10000

abbrev P := TinylambOpt.ResearchGoals.Compression.Predictors.SideResidue.predict

/-! ## self-contained Wire charge lemmas -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_of_bound (n W : Nat) (hn : n ≠ 0) (h : n < 2 ^ W) : width n ≤ W := by
  rw [width_eq n hn]
  have hl : n.log2 < W := (Nat.log2_lt hn).mpr h
  omega

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have hw7 : (width n + 6) / 7 ≤ k := (Nat.max_le.mp h).2
    have hw : width n ≤ 7 * k := by
      have hh := (Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)).mp hw7
      omega
    calc n < 2 ^ width n := lt_two_pow_width n
      _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by decide : 0 < 2) hw
      _ = 128 ^ k := by
        have h128 : (128 : Nat) = 2 ^ 7 := by decide
        rw [h128, ← Nat.pow_mul]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · have hw : width n ≤ 7 * k := by
        by_cases hn : n = 0
        · subst hn
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          exact width_le_of_bound n (7 * k) hn hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem ubytes_pos (n : Nat) : 0 < ubytes n := by
  unfold ubytes
  omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbp : 0 < ubytes b := ubytes_pos b
  have hbw : b < 128 ^ ubytes b := (ubytes_le_iff b (ubytes b) hbp).mp (Nat.le_refl _)
  exact (ubytes_le_iff a (ubytes b) hbp).mpr (Nat.lt_of_le_of_lt h hbw)

private theorem log2_div_128 (n : Nat) (hn : 128 ≤ n) :
    Nat.log2 n = Nat.log2 (n / 128) + 7 := by
  let q := n / 128
  have hq : q ≠ 0 := by dsimp [q]; omega
  have hlow : 2 ^ (Nat.log2 q + 7) ≤ n := by
    rw [Nat.pow_add]
    change 2 ^ Nat.log2 q * 2 ^ 7 ≤ n
    rw [show (2 : Nat) ^ 7 = 128 by decide]
    have hqle : 2 ^ Nat.log2 q ≤ q := Nat.log2_self_le hq
    have hmul : q * 128 ≤ n := by dsimp [q]; exact Nat.div_mul_le_self n 128
    exact Nat.le_trans (Nat.mul_le_mul_right 128 hqle) hmul
  have hloglow : Nat.log2 q + 7 ≤ Nat.log2 n :=
    (Nat.le_log2 (by omega : n ≠ 0)).mpr hlow
  have hhigh : n < 2 ^ (Nat.log2 q + 8) := by
    have hqbound : q < 2 ^ (Nat.log2 q + 1) := Nat.lt_log2_self
    have hq1 : q + 1 ≤ 2 ^ (Nat.log2 q + 1) := Nat.succ_le_of_lt hqbound
    have hnlt : n < 128 * (q + 1) := by
      dsimp [q]
      simpa [Nat.mul_comm] using Nat.lt_mul_div_succ n (by decide : 0 < 128)
    have hmul : 128 * (q + 1) ≤ 128 * 2 ^ (Nat.log2 q + 1) := Nat.mul_le_mul_left 128 hq1
    have hp : 128 * 2 ^ (Nat.log2 q + 1) = 2 ^ (Nat.log2 q + 8) := by
      calc
        128 * 2 ^ (Nat.log2 q + 1) = 2 ^ 7 * 2 ^ (Nat.log2 q + 1) := by
          rw [show (128 : Nat) = 2 ^ 7 by decide]
        _ = 2 ^ (7 + (Nat.log2 q + 1)) := (Nat.pow_add 2 7 (Nat.log2 q + 1)).symm
        _ = 2 ^ (Nat.log2 q + 8) := by congr 1; omega
    exact Nat.lt_of_lt_of_le hnlt (by simpa [hp] using hmul)
  have hloghigh : Nat.log2 n < Nat.log2 q + 8 :=
    (Nat.log2_lt (by omega : n ≠ 0)).mpr (by simpa [q] using hhigh)
  change Nat.log2 n = Nat.log2 q + 7
  omega

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
        unfold ubytes width
        by_cases hn : n == 0
        · simp [hn]
        · simp [hn]
          have hn0 : n ≠ 0 := by simpa using hn
          have hloglt : Nat.log2 n < 7 :=
            (Nat.log2_lt hn0).mpr (by
              change n < 2 ^ 7
              rw [show (2 : Nat) ^ 7 = 128 by decide]
              exact hlt)
          omega
      · have hn128 : 128 ≤ n := by omega
        have hdivlt : n / 128 < n := by omega
        have hdivlen : (uleb (n / 128)).length = ubytes (n / 128) := ih (n / 128) hdivlt
        rw [uleb]
        simp [hlt, hdivlen]
        unfold ubytes width
        have hn0 : n ≠ 0 := by omega
        have hnq : n / 128 ≠ 0 := by omega
        simp [hn0, hnq]
        have hlogeq := log2_div_128 n hn128
        rw [hlogeq]
        omega

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

/-! ## generic encode/restore helpers -/

private theorem encode_restore (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int) :
    ∀ xs, restore p side i h (encode p side i h xs) = xs := by
  intro xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      simp only [encode, restore]
      have hfirst : p i h side + unzig (zig (x - p i h side)) = x := by
        rw [unzig_zig]
        omega
      rw [hfirst, ih]

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h : List Int) :
    ∀ xs, (encode p side i h xs).length = xs.length := by
  intro xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem encode_lt_of_certificate (p : Predict) (side : Nat → Int) (i : Nat)
    (h : List Int) (xs : List Int) (w : Nat) (hc : certificate p side i h xs w) :
    ∀ e ∈ encode p side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      dsimp [certificate] at hc
      have hhead : zig (x - p i h side) < 2 ^ w := by
        have hh := hc 0 (by simp)
        simpa using hh
      have hctail : certificate p side (i + 1) (x :: h) xs w := by
        intro j hj
        have hc1 := hc (j + 1) (by simpa using Nat.succ_lt_succ hj)
        simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm,
          List.take_succ_cons, List.reverse_cons, List.append_assoc] using hc1
      have htail := ih (i + 1) (x :: h) hctail
      simp only [encode, List.mem_cons]
      intro e he
      rcases he with rfl | he
      · exact hhead
      · exact htail e he

private theorem history_irrelevant (p : Predict) (side : Nat → Int)
    (hind : ∀ i h, p i h side = p i [] side) :
    ∀ (i : Nat) (h h' : List Int) (xs : List Int),
      encode p side i h xs = encode p side i h' xs := by
  intro i h h' xs
  induction xs generalizing i h h' with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      rw [hind i h, hind i h']
      rw [ih (i + 1) (x :: h) (x :: h')]

/-! ## the four SideResidue goals -/

theorem sound_ResidualRoundTrip : ResidualRoundTrip.Goal := by
  intro base period side i h xs
  exact encode_restore (P base period) side i h xs

theorem sound_ByteBudget : ByteBudget.Goal := by
  intro base period side i h xs w extra oldBytes hc hlt
  let p := P base period
  let es := encode p side i h xs
  have hall : ∀ e ∈ es, e < 2 ^ w := by
    dsimp [es, p]
    exact encode_lt_of_certificate p side i h xs w (by simpa [p] using hc)
  have hpowpos : 0 < 2 ^ w := Nat.pow_pos Nat.zero_lt_two
  have hpeak_lt : peak es < 2 ^ w := by
    by_cases hnil : es = []
    · have hp0 : peak es = 0 := by simp [peak, hnil]
      rw [hp0]
      exact hpowpos
    · obtain ⟨m, hm⟩ := max?_some_of_ne_nil hnil
      have hmprops := List.max?_eq_some_iff.mp hm
      have hpe : peak es = m := by simp [peak, hm]
      rw [hpe]
      exact hall m hmprops.1
  have hwle : width (peak es) ≤ w := by
    by_cases hz : peak es = 0
    · rw [hz]
      simp [width]
    · exact width_le_of_bound (peak es) w hz hpeak_lt
  have hlen : es.length = xs.length := by
    dsimp [es]
    exact encode_length p side i h xs
  have hframe : (frame es).length ≤ quote xs.length w := by
    simp only [frame, quote, List.length_append, List.length_cons, List.length_nil, Nat.zero_add]
    rw [uleb_length es.length]
    rw [uleb_length (width (peak es))]
    rw [pack_length]
    rw [hlen]
    have hub : ubytes (width (peak es)) ≤ ubytes w := ubytes_mono hwle
    have hmul : xs.length * width (peak es) ≤ xs.length * w := Nat.mul_le_mul_left _ hwle
    have hbc := byteCount_mono hmul
    omega
  change extra + (frame es).length < oldBytes
  omega

theorem sound_RestartSeed : RestartSeed.Goal := by
  intro base period side i h xs
  have hind : ∀ j (hh : List Int), P base period j hh side = P base period j [] side := by
    intro j hh
    rfl
  exact history_irrelevant (P base period) side hind i (h.take 0) h xs

theorem sound_PeriodShift : PeriodShift.Goal := by
  intro base period side i h xs k
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      have hmod : (side i + k * ((period : Int) + 1)) % ((period : Int) + 1) =
          side i % ((period : Int) + 1) := by
        exact Int.add_mul_emod_self_right (side i) k ((period : Int) + 1)
      have hp : predict base period i h (fun j => side j + k * ((period + 1 : Nat) : Int)) =
          predict base period i h side := by
        dsimp [predict]
        rw [hmod]
      rw [hp]
      rw [ih (i + 1) (x :: h)]

end TinylambOpt.ResearchProofs.Compression.Predictors.SideResidue
