import TinylambOpt.ResearchGoals.Compression.Predictors

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Ordinal

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.Ordinal

set_option maxHeartbeats 0

/-! ## Shared helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      simp [zig, unzig]
      omega

private theorem roundtrip_general (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) :
    restore p side i h (encode p side i h xs) = xs := by
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode, restore]
      rw [unzig_zig]
      have hx : p i h side + (x - p i h side) = x := by omega
      rw [hx]
      rw [ih]

private theorem encode_ordinal_hist_indep (base slope : Int) (side : Nat → Int) (i : Nat)
    (h1 h2 : List Int) (xs : List Int) :
    encode (predict base slope) side i h1 xs = encode (predict base slope) side i h2 xs := by
  induction xs generalizing i h1 h2 with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode]
      have hp : (predict base slope) i h1 side = (predict base slope) i h2 side := by
        simp [predict]
      rw [hp]
      rw [ih (i+1) (x::h1) (x::h2)]

private theorem encode_len (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) :
    (encode p side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => rfl
  | cons x xs ih =>
      simp only [encode, List.length_cons]
      rw [ih]

private theorem encode_residuals_lt (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int)
    (w : Nat)
    (hcert : ∀ j < xs.length,
      zig ((xs[j]?.getD 0) - p (i+j) ((xs.take j).reverse ++ h) side) < 2^w) :
    ∀ e ∈ encode p side i h xs, e < 2^w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      have hhead : zig (x - p i h side) < 2^w := by
        have hh := hcert 0 (by simp)
        simpa using hh
      have htailcert : ∀ j < xs.length,
          zig ((xs[j]?.getD 0) - p (i+1+j) ((xs.take j).reverse ++ (x::h)) side) < 2^w := by
        intro j hj
        have hh := hcert (j+1) (by simp [hj])
        have hidx : (x::xs)[j+1]?.getD 0 = xs[j]?.getD 0 := by simp
        have htk : ((x::xs).take (j+1)).reverse ++ h = (xs.take j).reverse ++ (x::h) := by
          rw [List.take_succ_cons, List.reverse_cons]
          simp [List.append_assoc]
        have hiadd : i + (j+1) = i+1+j := by omega
        rw [hidx, htk, hiadd] at hh
        exact hh
      have htail := ih (i+1) (x::h) htailcert
      simp only [encode, List.mem_cons]
      intro e he
      rcases he with rfl | he
      · exact hhead
      · exact htail e he

/-! ## Width / varint arithmetic -/

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs hne
  cases xs with
  | nil => contradiction
  | cons a as =>
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem peak_lt_of_all_lt (xs : List Nat) (w : Nat)
    (h : ∀ e ∈ xs, e < 2^w) : peak xs < 2^w := by
  cases xs with
  | nil =>
      simp [peak, Nat.pow_pos]
  | cons x xs =>
      have hne : (x::xs) ≠ [] := by simp
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
      have hmprops := List.max?_eq_some_iff.mp hm
      have hp : peak (x::xs) = m := by simp [peak, hm]
      rw [hp]
      exact h m hmprops.1

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem width_le_of_lt (n w : Nat) (h : n < 2^w) : width n ≤ w := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · exact (width_le_iff n w hn).mpr h

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
          exact (width_le_iff n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by unfold ubytes; omega
  have hb_lt : b < 128 ^ (ubytes b) := (ubytes_le_iff b (ubytes b) hbpos).mp (by omega)
  have ha_lt : a < 128 ^ (ubytes b) := Nat.lt_of_le_of_lt h hb_lt
  exact (ubytes_le_iff a (ubytes b) hbpos).mpr ha_lt

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_one (n : Nat) : (uleb n).length ≤ 1 ↔ n < 128 := by
  by_cases hn : n < 128
  · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
    rw [h1]
    simp [hn]
  · have hlen : 2 ≤ (uleb n).length := by
      rw [uleb_len_succ n hn]
      have := uleb_len_pos (n / 128)
      omega
    constructor
    · intro h; omega
    · intro h; omega

private theorem uleb_len_le_succ (n k : Nat) :
    (uleb n).length ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      simpa using uleb_len_le_one n
  | succ k ih =>
      by_cases hn : n < 128
      · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
        have hlen1 : (uleb n).length = 1 := by rw [h1]; simp
        constructor
        · intro _
          calc n < 128 := hn
            _ ≤ 128 ^ (k + 1 + 1) := by
              have hk1 : 1 ≤ k + 1 + 1 := by omega
              simpa using Nat.pow_le_pow_right (by decide : 0 < 128) hk1
        · intro _
          rw [hlen1]
          omega
      · rw [uleb_len_succ n hn]
        have hih := ih (n / 128)
        constructor
        · intro h
          have hd : (uleb (n / 128)).length ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := hih.mp hd
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)] at hdiv
          calc n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

private theorem uleb_len_le_ubytes (n : Nat) : (uleb n).length ≤ ubytes n := by
  have hpos : 0 < ubytes n := by unfold ubytes; omega
  exact (uleb_len_le_iff n (ubytes n) hpos).mpr
    ((ubytes_le_iff n (ubytes n) hpos).mp (by omega))

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

/-! ## The four Ordinal goals -/

theorem sound_ResidualRoundTrip : Ordinal.ResidualRoundTrip.Goal := by
  intro base slope side i h xs
  exact roundtrip_general (predict base slope) side i h xs

theorem sound_ByteBudget : Ordinal.ByteBudget.Goal := by
  intro base slope side i h xs w extra oldBytes hcert hlt
  dsimp [quote] at hlt
  let es := encode (predict base slope) side i h xs
  have hlt_es : ∀ e ∈ es, e < 2^w := by
    dsimp [es]
    exact encode_residuals_lt (predict base slope) side i h xs w hcert
  have hpeak : peak es < 2^w := peak_lt_of_all_lt es w hlt_es
  have hwp : width (peak es) ≤ w := width_le_of_lt _ _ hpeak
  have hpacklen : (pack (width (peak es)) es).length =
      byteCount (es.length * width (peak es)) := by
    simp [pack, little]
  have hes_len : es.length = xs.length := by
    dsimp [es]
    exact encode_len (predict base slope) side i h xs
  have hub : ubytes (width (peak es)) ≤ ubytes w := ubytes_mono hwp
  have huleb_n : (uleb es.length).length ≤ ubytes xs.length := by
    rw [hes_len]
    exact uleb_len_le_ubytes xs.length
  have huleb_w : (uleb (width (peak es))).length ≤ ubytes w :=
    Nat.le_trans (uleb_len_le_ubytes (width (peak es))) hub
  have hbc : byteCount (es.length * width (peak es)) ≤ byteCount (xs.length * w) := by
    apply byteCount_mono
    rw [hes_len]
    exact Nat.mul_le_mul_left xs.length hwp
  change extra + (frame es).length < oldBytes
  change extra + ([0] ++ uleb es.length ++ uleb (width (peak es)) ++
    pack (width (peak es)) es).length < oldBytes
  simp only [List.length_append, List.length_cons, List.length_nil, hpacklen]
  omega

theorem sound_RestartSeed : Ordinal.RestartSeed.Goal := by
  intro base slope side i h xs
  exact encode_ordinal_hist_indep base slope side i (h.take 0) h xs

theorem sound_AbsoluteSeek : Ordinal.AbsoluteSeek.Goal := by
  intro base slope side i n
  induction n generalizing i with
  | zero => simp [encode]
  | succ n ih =>
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map]
      simp only [encode]
      have hzero : (base + slope * (↑(i + 0) : Int)) -
          (predict base slope) i [] side = 0 := by
        simp [predict]
      rw [hzero]
      simp [zig, List.replicate_succ]
      rw [encode_ordinal_hist_indep base slope side (i+1) _ []]
      simpa [Function.comp_def, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
        using ih (i+1)

end TinylambOpt.ResearchProofs.Compression.Predictors.Ordinal
