import TinylambOpt.ResearchGoals.Compression.Predictors

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `SideAffine.ByteBudget` — `∀ base slope side i h xs w extra oldBytes, certificate
  (predict base slope) side i h xs w → extra + quote xs.length w < oldBytes → extra +
  (frame (encode (predict base slope) side i h xs)).length < oldBytes`
- `SideAffine.ProportionalColumns` — `∀ base slope side i n, encode (predict base
  slope) side i [] ((List.range n).map (fun j => base+slope*side (i+j))) =
  List.replicate n 0`
- `SideAffine.ResidualRoundTrip` — `∀ base slope side i h xs, restore (predict base
  slope) side i h (encode (predict base slope) side i h xs) = xs`
- `SideAffine.RestartSeed` — `∀ base slope side i h xs, encode (predict base slope)
  side i (h.take (0)) xs = encode (predict base slope) side i h xs`
-/

namespace TinylambOpt.ResearchProofs.Compression.Predictors.SideAffine

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Predictors
open TinylambOpt.ResearchGoals.Compression.Predictors.SideAffine

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## zig / encode / restore helpers -/

private theorem unzig_zig (z : Int) : unzig (zig z) = z := by
  cases z with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      simp [zig, unzig]
      omega

private theorem encode_length (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) :
    (encode p side i h xs).length = xs.length := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih => simp [encode, ih]

private theorem restore_encode (p : Predict) (side : Nat → Int) :
    ∀ (i : Nat) (h xs : List Int), restore p side i h (encode p side i h xs) = xs
  | _, _, [] => by simp [encode, restore]
  | i, h, x :: xs => by
      simp only [encode, restore]
      rw [unzig_zig]
      have hx : p i h side + (x - p i h side) = x := by omega
      rw [hx]
      rw [restore_encode p side (i + 1) (x :: h) xs]

/-! ## wire arithmetic helpers -/

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

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  apply (ubytes_le_iff a (ubytes b) (ubytes_pos b)).mpr
  have hb : b < 128 ^ ubytes b := (ubytes_le_iff b (ubytes b) (ubytes_pos b)).mp (Nat.le_refl _)
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
  apply (uleb_len_le_iff n (ubytes n) (ubytes_pos n)).mpr
  exact (ubytes_le_iff n (ubytes n) (ubytes_pos n)).mp (Nat.le_refl _)

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

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  have hxsne : xs ≠ [] := by
    intro hnil
    subst xs
    simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hxsne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hxm : x ≤ m := hmprops.2 x hx
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hxm

private theorem width_peak_le_of_bound {xs : List Nat} {w : Nat}
    (h : ∀ x ∈ xs, x < 2 ^ w) : width (peak xs) ≤ w := by
  by_cases hnil : xs = []
  · subst xs
    simp [peak, width]
  · obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hnil
    have hmprops := List.max?_eq_some_iff.mp hm
    have hm_lt : m < 2 ^ w := h m hmprops.1
    have hp : peak xs = m := by simp [peak, hm]
    rw [hp]
    by_cases hm0 : m = 0
    · simp [width, hm0]
    · exact (width_le_iff m w hm0).mpr hm_lt

/-! ## SideAffine-specific helpers -/

private theorem encode_history_irrelevant (base slope : Int) (side : Nat → Int) :
    ∀ (i : Nat) (h h' : List Int) (xs : List Int),
      encode (predict base slope) side i h xs = encode (predict base slope) side i h' xs
  | _, _, _, [] => by simp [encode]
  | i, h, h', x :: xs => by
      simp only [encode]
      simp [predict]
      rw [encode_history_irrelevant base slope side (i + 1) (x :: h) (x :: h') xs]

private theorem encode_append_affine (base slope : Int) (side : Nat → Int) :
    ∀ (i : Nat) (xs ys : List Int),
      encode (predict base slope) side i [] (xs ++ ys) =
        encode (predict base slope) side i [] xs ++
          encode (predict base slope) side (i + xs.length) [] ys
  | i, [], ys => by simp [encode]
  | i, x :: xs, ys => by
      simp only [List.cons_append, encode, List.length_cons]
      rw [encode_history_irrelevant base slope side (i + 1) [x] [] (xs ++ ys)]
      rw [encode_history_irrelevant base slope side (i + 1) [x] [] xs]
      rw [encode_append_affine base slope side (i + 1) xs ys]
      simp [Nat.add_comm, Nat.add_left_comm]

private theorem encode_bounded (base slope : Int) (side : Nat → Int) (i : Nat) (h : List Int)
    (xs : List Int) (w : Nat)
    (hcert : ∀ j < xs.length, zig ((xs[j]?.getD 0) - (base + slope * side (i + j))) < 2 ^ w) :
    ∀ e ∈ encode (predict base slope) side i h xs, e < 2 ^ w := by
  induction xs generalizing i h with
  | nil => simp [encode]
  | cons x xs ih =>
      intro e he
      simp only [encode, List.mem_cons] at he
      rcases he with rfl | he
      · simpa [predict] using hcert 0 (by simp)
      · apply (ih (i := i + 1) (h := x :: h))
        · intro j hj
          have hc := hcert (j + 1) (by simpa using Nat.succ_lt_succ hj)
          simpa [predict, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hc
        · exact he

private theorem frame_length_le_quote (es : List Nat) (n w : Nat) (hlen : es.length = n)
    (hw : width (peak es) ≤ w) : (frame es).length ≤ quote n w := by
  have h1 : (uleb es.length).length ≤ ubytes es.length := uleb_len_le_ubytes es.length
  have h2 : (uleb (width (peak es))).length ≤ ubytes (width (peak es)) :=
    uleb_len_le_ubytes (width (peak es))
  have h3 : (pack (width (peak es)) es).length = byteCount (es.length * width (peak es)) := by
    simp [pack, little]
  have h4 : ubytes (width (peak es)) ≤ ubytes w := ubytes_mono hw
  have h5 : byteCount (es.length * width (peak es)) ≤ byteCount (n * w) := by
    rw [hlen]
    exact byteCount_mono (Nat.mul_le_mul_left n hw)
  dsimp [frame, quote]
  rw [hlen] at h1 ⊢
  simp only [List.length_append]
  rw [h3]
  omega

private theorem replicate_succ_append (n : Nat) (a : Nat) :
    List.replicate n a ++ [a] = List.replicate (n + 1) a := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih]

/-! ## goals -/

theorem sound_ResidualRoundTrip :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideAffine.ResidualRoundTrip.Goal := by
  intro base slope side i h xs
  exact restore_encode (predict base slope) side i h xs

theorem sound_ByteBudget :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideAffine.ByteBudget.Goal := by
  intro base slope side i h xs w extra oldBytes hcert hlt
  let es := encode (predict base slope) side i h xs
  have hcert' : ∀ j < xs.length,
      zig ((xs[j]?.getD 0) - (base + slope * side (i + j))) < 2 ^ w := by
    intro j hj
    simpa [certificate, predict] using hcert j hj
  have hb : ∀ e ∈ es, e < 2 ^ w := by
    dsimp [es]
    exact encode_bounded base slope side i h xs w hcert'
  have hw : width (peak es) ≤ w := width_peak_le_of_bound hb
  have hlen : es.length = xs.length := by
    dsimp [es]
    exact encode_length (predict base slope) side i h xs
  have hf : (frame es).length ≤ quote xs.length w := frame_length_le_quote es xs.length w hlen hw
  have hsum : extra + (frame es).length ≤ extra + quote xs.length w := Nat.add_le_add_left hf extra
  simpa [es] using Nat.lt_of_le_of_lt hsum hlt

theorem sound_RestartSeed :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideAffine.RestartSeed.Goal := by
  intro base slope side i h xs
  exact encode_history_irrelevant base slope side i (h.take 0) h xs

theorem sound_ProportionalColumns :
    TinylambOpt.ResearchGoals.Compression.Predictors.SideAffine.ProportionalColumns.Goal := by
  intro base slope side i n
  induction n generalizing i with
  | zero => simp [encode]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.map_singleton]
      rw [encode_append_affine base slope side i
        ((List.range n).map (fun j => base + slope * side (i + j)))
        [base + slope * side (i + n)]]
      simp only [List.length_map, List.length_range]
      rw [ih i]
      have hzero : encode (predict base slope) side (i + n) []
          [base + slope * side (i + n)] = [0] := by
        simp [encode, predict, zig]
      rw [hzero]
      exact replicate_succ_append n 0

end TinylambOpt.ResearchProofs.Compression.Predictors.SideAffine
