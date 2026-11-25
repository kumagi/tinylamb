import TinylambOpt.ResearchGoals.Compression.Predictors

namespace TinylambOpt.ResearchProofs.Compression.Predictors.Bucket

open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.Bucket
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0

/-! ## Generic codec helpers -/

private theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  cases x with
  | ofNat n => simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem restore_encode (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int), restore p side i h (encode p side i h xs) = xs := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode, restore]
  | cons x xs ih =>
      dsimp [encode, restore]
      rw [unzig_zig]
      have hx : p i h side + (x - p i h side) = x := by omega
      rw [hx]
      rw [ih]

private theorem encode_len (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int), (encode p side i h xs).length = xs.length := by
  intro i h xs
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

/-- The `Bucket` predictor ignores its history argument. -/
private theorem encode_hist_irrelevant (base step : Int) (period : Nat) (side : Nat → Int) :
    ∀ (i : Nat) (h1 h2 : List Int) (xs : List Int),
      encode (predict base step period) side i h1 xs = encode (predict base step period) side i h2 xs := by
  intro i h1 h2 xs
  induction xs generalizing i h1 h2 with
  | nil => simp [encode]
  | cons x xs ih =>
      simp only [encode]
      have hh : zig (x - predict base step period i h1 side) =
          zig (x - predict base step period i h2 side) := by rfl
      rw [hh, ih (i+1) (x::h1) (x::h2)]

/-- Every residual emitted by `encode` satisfies the certificate bound. -/
private theorem encode_all_lt (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int) (w : Nat),
      (∀ j < xs.length, zig ((xs[j]?.getD 0) - p (i+j) ((xs.take j).reverse ++ h) side) < 2^w) →
      ∀ e ∈ encode p side i h xs, e < 2^w := by
  intro i h xs w hcert
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      intro e he
      simp only [encode, List.mem_cons] at he
      rcases he with rfl | he
      · have hc := hcert 0 (by simp)
        simpa using hc
      · apply ih (i+1) (x::h) ?_ e he
        intro j hj
        have hc := hcert (j+1) (by simp [hj])
        simpa [List.take_succ_cons, List.reverse_cons, Nat.add_assoc, Nat.add_comm,
          Nat.add_left_comm] using hc

/-! ## Wire charge helpers -/

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

private theorem width_le_of_lt_pow (n W : Nat) (h : n < 2^W) : width n ≤ W := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · exact (width_le_iff n W hn).mpr h

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

private theorem ubytes_pos (n : Nat) : 0 < ubytes n := by
  unfold ubytes
  omega

private theorem ubytes_mono_le {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  apply (ubytes_le_iff a (ubytes b) (ubytes_pos b)).mpr
  have hb : b < 128 ^ ubytes b :=
    (ubytes_le_iff b (ubytes b) (ubytes_pos b)).mp (by omega)
  omega

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
  have hubpos : 0 < ubytes n := ubytes_pos n
  exact (uleb_len_le_iff n (ubytes n) hubpos).mpr
    ((ubytes_le_iff n (ubytes n) hubpos).mp (by omega))

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem pack_len (w : Nat) (xs : List Nat) : (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem peak_lt_of_all_lt (xs : List Nat) {M : Nat} (hM : 0 < M)
    (h : ∀ e ∈ xs, e < M) : peak xs < M := by
  cases xs with
  | nil => simpa [peak] using hM
  | cons x xs =>
      have hxM : x < M := h x (by simp)
      cases hm : xs.max? with
      | none =>
          simpa [peak, hm] using hxM
      | some m =>
          have hm_mem : m ∈ xs := (List.max?_eq_some_iff.mp hm).1
          have hmM : m < M := h m (by simp [hm_mem])
          have hmm : max x m < M := by omega
          simp [peak, hm, hmm]

/-- The frame built from residuals that all fit in `w` bits is no larger than
    the quote charged for width `w` (header, varints and padded payload). -/
private theorem frame_len_le_quote (es : List Nat) (n w' w : Nat)
    (hlen : es.length = n) (hw' : w' = width (peak es)) (hwle : w' ≤ w) :
    (frame es).length ≤ quote n w := by
  rw [show frame es = [0] ++ uleb es.length ++ uleb w' ++ pack w' es by
    dsimp [frame]
    rw [hw']]
  rw [show quote n w = 1 + ubytes n + ubytes w + byteCount (n*w) by rfl]
  simp only [List.length_append, List.length_cons, List.length_nil]
  rw [pack_len]
  have h1 : (uleb es.length).length ≤ ubytes n := by
    rw [hlen]
    exact uleb_len_le_ubytes n
  have h2 : (uleb w').length ≤ ubytes w :=
    Nat.le_trans (uleb_len_le_ubytes w') (ubytes_mono_le hwle)
  have h3 : byteCount (es.length * w') ≤ byteCount (n*w) := by
    rw [hlen]
    exact byteCount_mono (Nat.mul_le_mul_left n hwle)
  omega

/-! ## Run-crossing helpers -/

private theorem cons_map_succ_eq_map_append (f : Nat → Int) (start n : Nat) :
    f start :: (List.range n).map (fun j => f (start+1+j)) =
      (List.range n).map (fun j => f (start+j)) ++ [f (start+n)] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [← List.cons_append, ih]
      have heq : f (start + 1 + n) = f (start + (n+1)) := by
        congr 1
        omega
      rw [heq]
      simp [List.map_append]

private theorem restore_zeros (base step : Int) (period : Nat) (side : Nat → Int) :
    ∀ (start : Nat) (h : List Int) (n : Nat),
      restore (predict base step period) side start h (List.replicate n 0) =
        (List.range n).map (fun j : Nat => predict base step period (start+j) h side) := by
  intro start h n
  induction n generalizing start h with
  | zero => simp [restore]
  | succ n ih =>
      simp only [List.replicate_succ, restore]
      have hun : unzig 0 = 0 := by simp [unzig]
      rw [hun]
      have hz : predict base step period start h side + 0 = predict base step period start h side := by omega
      rw [hz]
      rw [ih]
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      exact cons_map_succ_eq_map_append (fun j : Nat => predict base step period j h side) start n

/-! ## Goals -/

theorem sound_ResidualRoundTrip :
    TinylambOpt.ResearchGoals.Compression.Predictors.Bucket.ResidualRoundTrip.Goal := by
  intro base step period side i h xs
  exact restore_encode (predict base step period) side i h xs

theorem sound_ByteBudget :
    TinylambOpt.ResearchGoals.Compression.Predictors.Bucket.ByteBudget.Goal := by
  intro base step period side i h xs w extra oldBytes hcert hq
  let p : Predict := predict base step period
  let es : List Nat := encode p side i h xs
  have hall : ∀ e ∈ es, e < 2^w := by
    dsimp [es, p]
    exact encode_all_lt (predict base step period) side i h xs w hcert
  have hwpos : 0 < 2^w := by exact Nat.pow_pos (by decide : 0 < 2)
  have hp : peak es < 2^w := peak_lt_of_all_lt es hwpos hall
  have hwle : width (peak es) ≤ w := width_le_of_lt_pow (peak es) w hp
  have hframe : (frame es).length ≤ quote xs.length w := by
    apply frame_len_le_quote es xs.length (width (peak es)) w
    · dsimp [es, p]
      exact encode_len (predict base step period) side i h xs
    · rfl
    · exact hwle
  simpa [es, p] using Nat.lt_of_le_of_lt (Nat.add_le_add_left hframe extra) hq

theorem sound_RestartSeed :
    TinylambOpt.ResearchGoals.Compression.Predictors.Bucket.RestartSeed.Goal := by
  intro base step period side i h xs
  simpa using encode_hist_irrelevant base step period side i (h.take 0) h xs

theorem sound_RunCrossing :
    TinylambOpt.ResearchGoals.Compression.Predictors.Bucket.RunCrossing.Goal := by
  intro base step period side i n
  simpa [predict] using restore_zeros base step period side i [] n

end TinylambOpt.ResearchProofs.Compression.Predictors.Bucket
