import TinylambOpt.ResearchGoals.Compression.Wire

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved (17 claims, including the following)

- `AlignmentDebt` — `∀ unit (sizes : List Nat), 0 < unit → (sizes.map (align unit)).sum
  - align unit sizes.sum ≤ (sizes.length-1)*unit`
- `BitPackBreakEven` — `∀ rows bits oldBytes overhead, overhead < rows*oldBytes →
  (overhead + byteCount (rows*bits) < rows*oldBytes ↔ rows*bits ≤
  8*(rows*oldBytes-overhead-1))`
- `BitSplice` — `∀ w a b, pack w (a++b) = little (fromLittle (pack w a) +
  2^(w*a.length)*fromLittle (pack w b)) (byteCount ((a.length+b.length)*w))`

Refuted by counterexample

- `MeanWidth` — `∀ xs : List Nat, xs ≠ [] → pack (width (xs.sum/xs.length)) xs = pack
  (width (peak xs)) xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Wire

open TinylambOpt.ResearchGoals.Compression.Wire

/-! Proofs and concrete counterexamples for all 18 Wire goals. -/

/-! ## shared arithmetic helpers -/

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

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem byteCount_le_of_le (bits b : Nat) (h : bits ≤ 8 * b) : byteCount bits ≤ b := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem bits_le_of_byteCount_le (bits b : Nat) (h : byteCount bits ≤ b) : bits ≤ 8 * b := by
  exact Nat.le_trans (bits_le_byteCount bits) (Nat.mul_le_mul_left 8 h)

private theorem byteCount_add_le (a b : Nat) : byteCount (a + b) ≤ byteCount a + byteCount b := by
  unfold byteCount
  have ha := Nat.mod_add_div (a + 7) 8
  have hb := Nat.mod_add_div (b + 7) 8
  have hab := Nat.mod_add_div (a + b + 7) 8
  have hama := Nat.mod_lt (a + 7) (by decide : 0 < 8)
  have hbmb := Nat.mod_lt (b + 7) (by decide : 0 < 8)
  have habmb := Nat.mod_lt (a + b + 7) (by decide : 0 < 8)
  omega

private theorem byteCount_add_ge (a b : Nat) : byteCount a + byteCount b ≤ byteCount (a + b) + 1 := by
  unfold byteCount
  have ha := Nat.mod_add_div (a + 7) 8
  have hb := Nat.mod_add_div (b + 7) 8
  have hab := Nat.mod_add_div (a + b + 7) 8
  have hama := Nat.mod_lt (a + 7) (by decide : 0 < 8)
  have hbmb := Nat.mod_lt (b + 7) (by decide : 0 < 8)
  have habmb := Nat.mod_lt (a + b + 7) (by decide : 0 < 8)
  omega

private theorem byteCount_add_mul_eight (x q : Nat) : byteCount (x + 8 * q) = byteCount x + q := by
  unfold byteCount
  have hx := Nat.mod_add_div (x + 7) 8
  omega

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem align_add (u x y : Nat) (hu : 0 < u) : align u (x + y) + u ≥ align u x + align u y := by
  let A := x + u - 1
  let B := y + u - 1
  let C := x + y + u - 1
  have hsum : A + B = C + (u - 1) := by dsimp [A, B, C]; omega
  have hAB : (A + B) / u = A / u + B / u + (if u ≤ A % u + B % u then 1 else 0) := Nat.add_div hu
  have hle1 : A / u + B / u ≤ (A + B) / u := by rw [hAB]; split <;> omega
  have hC : (C + (u - 1)) / u = C / u + (u - 1) / u + (if u ≤ C % u + (u - 1) % u then 1 else 0) := Nat.add_div hu
  have hum1 : (u - 1) / u = 0 := Nat.div_eq_of_lt (by omega)
  have hle2 : (A + B) / u ≤ C / u + 1 := by
    rw [hsum, hC, hum1]
    split <;> omega
  have hdiv_le : A / u + B / u ≤ C / u + 1 := Nat.le_trans hle1 hle2
  have hmul := Nat.mul_le_mul_left u hdiv_le
  simp only [Nat.mul_add, Nat.mul_one] at hmul
  simpa [align, A, B, C, Nat.mul_comm] using hmul

private theorem mul_pred_succ (n u : Nat) (hn : 0 < n) : (n - 1) * u + u = n * u := by
  calc (n - 1) * u + u = (n - 1) * u + 1 * u := by rw [Nat.one_mul]
    _ = ((n - 1) + 1) * u := by rw [Nat.add_mul]
    _ = n * u := by congr 1; omega

/-! ## ULEB helpers -/

private theorem readU_uleb_fuel (n fuel : Nat) (tail : Bytes) (hf : n < 128 ^ fuel) :
    readU (uleb n ++ tail) = some (n, tail) := by
  induction fuel generalizing n tail with
  | zero =>
      have hn : n = 0 := by omega
      subst hn
      simp [uleb, readU]
  | succ fuel ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt, readU]
      · have hmod : 128 + n % 128 < 256 := by omega
        have hdiv : n / 128 < 128 ^ fuel := by
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
          simpa [Nat.pow_succ, Nat.mul_comm] using hf
        have hrec := ih (n / 128) tail hdiv
        rw [uleb]
        simp only [hlt, ite_false, List.cons_append, readU]
        have hnotlt : ¬ (128 + n % 128 < 128) := by omega
        simp only [hnotlt, ite_false, hmod, ite_true]
        rw [hrec]
        simp only [Option.map_some, Option.some.injEq]
        refine Prod.ext ?_ ?_
        · simp; omega
        · rfl

private theorem readU_uleb (n : Nat) (tail : Bytes) : readU (uleb n ++ tail) = some (n, tail) := by
  have hf : n < 128 ^ (n + 1) := by
    have h2 : n < 128 ^ n := by
      calc n < 2 ^ n := Nat.lt_two_pow_self
        _ ≤ 128 ^ n := Nat.pow_le_pow_left (by omega) _
    calc n < 128 ^ n := h2
      _ ≤ 128 ^ n * 128 := Nat.le_mul_of_pos_right _ (by omega)
      _ = 128 ^ (n + 1) := by rw [Nat.pow_succ]
  exact readU_uleb_fuel n (n + 1) tail hf

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

/-! ## DelimitedInteger -/

theorem sound_DelimitedInteger : DelimitedInteger.Goal := by
  intro n tail
  exact readU_uleb n tail

/-! ## VarintCharge -/

theorem sound_VarintCharge : VarintCharge.Goal := by
  intro n
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · have hle : (uleb n).length ≤ ubytes n :=
      (uleb_len_le_iff n (ubytes n) hubpos).mpr
        ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
    exact hle
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

/-! ## WidthCliff -/

theorem sound_WidthCliff : WidthCliff.Goal := by
  intro n k hk
  exact ubytes_le_iff n k hk

/-! ## SignedInverse -/

theorem sound_SignedInverse : SignedInverse.Goal := by
  intro x
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

/-! ## SignedCliff -/

private theorem zig_lt_iff (x : Int) (q : Nat) :
    zig x < 2 * q ↔ -((q : Nat) : Int) ≤ x ∧ x < (q : Nat) := by
  cases x with
  | ofNat n =>
      simp [zig]
  | negSucc n =>
      simp [zig]
      omega

private theorem zig_lt_pow_iff (x : Int) (k : Nat) (hk : 0 < k) :
    zig x < 128 ^ k ↔
      -((128 ^ k / 2 : Nat) : Int) ≤ x ∧ x < ((128 ^ k / 2 : Nat) : Int) := by
  obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  subst hj
  have hpow : 128 ^ (j + 1) = 2 * (64 * 128 ^ j) := by
    rw [Nat.pow_succ]
    omega
  have hdiv : 128 ^ (j + 1) / 2 = 64 * 128 ^ j := by
    rw [hpow]
    omega
  rw [hdiv, hpow]
  exact zig_lt_iff x (64 * 128 ^ j)

theorem sound_SignedCliff : SignedCliff.Goal := by
  intro x k hk
  rw [sbytes, ubytes_le_iff (zig x) k hk]
  exact zig_lt_pow_iff x k hk

/-! ## MaxWordBytes -/

theorem sound_MaxWordBytes : MaxWordBytes.Goal := by
  intro n hn
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · omega
  · by_cases h0 : n = 0
    · subst h0
      simp [width]
    · have hlog : Nat.log2 n < 64 := (Nat.log2_lt h0).mpr hn
      have hw : width n ≤ 64 := by
        unfold width
        simp [h0]
        omega
      have hdiv : (width n + 6) / 7 ≤ (64 + 6) / 7 := Nat.div_le_div_right (by omega)
      have h64 : (64 + 6) / 7 = 10 := by decide
      omega

/-! ## PrefixFree -/

theorem sound_PrefixFree : PrefixFree.Goal := by
  intro a b tail hpre
  have hp : (uleb a) <+: (uleb b) := (List.isPrefixOf_iff_prefix).mp hpre
  obtain ⟨rest, hrest⟩ := hp
  have hb := readU_uleb b tail
  have ha : readU (uleb b ++ tail) = some (a, rest ++ tail) := by
    rw [← hrest, List.append_assoc]
    exact readU_uleb a (rest ++ tail)
  have hpair : (b, tail) = (a, rest ++ tail) := by
    have : some (b, tail) = some (a, rest ++ tail) := by rw [← hb, ha]
    exact Option.some.inj this
  have hba : b = a := (Prod.ext_iff.mp hpair).1
  simpa [hba] using readU_uleb a tail


/-! ## base-256 and bit-packing helpers -/

private theorem fromLittle_append_singleton (l : Bytes) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256 ^ l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m * 256) = v % m + m * ((v / m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256 * t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hsum_lt : r + m * s < m * 256 := by
    have hm_le : m * s ≤ m * 255 := Nat.mul_le_mul_left m hs_le
    omega
  have hrewrite : v = (r + m * s) + (m * 256) * t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m * 256) = r + m * s := by
    calc
      v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
      _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
      _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v / m) % 256) = r + m * s := by rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v / 256 ^ j) % 256)) = v % 256 ^ k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256 ^ k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem word_lt_pow (w : Nat) (xs : List Nat) : word w xs < 2 ^ (xs.length * w) := by
  induction xs with
  | nil => simp [word]
  | cons x xs ih =>
      have hxmod : x % 2 ^ w < 2 ^ w := Nat.mod_lt x (Nat.two_pow_pos w)
      have hsum : x % 2 ^ w + 2 ^ w * word w xs < 2 ^ w * (word w xs + 1) := by
        rw [Nat.mul_succ, Nat.add_comm (2 ^ w * word w xs) (2 ^ w)]
        exact Nat.add_lt_add_right hxmod _
      have hle : 2 ^ w * (word w xs + 1) ≤ 2 ^ w * 2 ^ (xs.length * w) :=
        Nat.mul_le_mul_left (2 ^ w) (Nat.succ_le_of_lt ih)
      have hp_exp : (xs.length + 1) * w = w + xs.length * w := by
        rw [Nat.add_mul, Nat.one_mul]
        omega
      have hp : 2 ^ ((xs.length + 1) * w) = 2 ^ w * 2 ^ (xs.length * w) := by
        rw [hp_exp, Nat.pow_add]
      rw [word]
      change x % 2 ^ w + 2 ^ w * word w xs < 2 ^ ((xs.length + 1) * w)
      rw [hp]
      exact Nat.lt_of_lt_of_le hsum hle

private theorem fromLittle_pack (w : Nat) (xs : List Nat) :
    fromLittle (pack w xs) = word w xs := by
  unfold pack little
  rw [fromLittle_little]
  have hlt := word_lt_pow w xs
  have hb := bits_le_byteCount (xs.length * w)
  have hpow : 256 ^ byteCount (xs.length * w) = 2 ^ (8 * byteCount (xs.length * w)) := by
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  apply Nat.mod_eq_of_lt
  rw [hpow]
  exact Nat.lt_of_lt_of_le hlt (Nat.pow_le_pow_right (by decide : 0 < 2) hb)

private theorem word_shift (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) :
    word w xs / 2 ^ w = word w (List.drop 1 xs) := by
  cases xs with
  | nil => simp [word]
  | cons x xs =>
      have hx : x < 2 ^ w := h x (by simp)
      have hxmod : x % 2 ^ w = x := Nat.mod_eq_of_lt hx
      have hxdiv : x / 2 ^ w = 0 := Nat.div_eq_of_lt hx
      rw [word, hxmod]
      rw [Nat.add_comm, Nat.mul_add_div (Nat.two_pow_pos w) (word w xs) x, hxdiv, Nat.add_zero]
      simp

private theorem word_digit (w i : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w)
    (hi : i < xs.length) : (word w xs / 2 ^ (i * w)) % 2 ^ w = xs[i] := by
  induction i generalizing xs with
  | zero =>
      simp only [Nat.zero_mul, Nat.pow_zero, Nat.div_one]
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have hx : x < 2 ^ w := h x (by simp)
          simp [word, Nat.mod_eq_of_lt hx]
  | succ i ih =>
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have ht : ∀ y ∈ xs, y < 2 ^ w := by
            intro y hy
            exact h y (by simp [hy])
          have hp_exp : (i + 1) * w = w + i * w := by
            rw [Nat.add_mul, Nat.one_mul]
            omega
          have hp : 2 ^ ((i + 1) * w) = 2 ^ w * 2 ^ (i * w) := by
            rw [hp_exp, Nat.pow_add]
          rw [hp, ← Nat.div_div_eq_div_mul, word_shift w (x :: xs) h]
          exact ih xs ht (Nat.lt_of_succ_lt_succ (by simpa using hi))

private theorem word_append (w : Nat) (a b : List Nat) :
    word w (a ++ b) = word w a + 2 ^ (w * a.length) * word w b := by
  induction a with
  | nil => simp [word]
  | cons x xs ih =>
      rw [List.cons_append, word, ih, word]
      have hp_exp : w * (xs.length + 1) = w + w * xs.length := by
        rw [Nat.mul_add, Nat.mul_one]
        omega
      have hp : 2 ^ (w * (xs.length + 1)) = 2 ^ w * 2 ^ (w * xs.length) := by
        rw [hp_exp, Nat.pow_add]
      rw [List.length_cons, hp]
      simp only [Nat.mul_add, Nat.add_assoc]
      ac_rfl

/-! ## PackedInverse -/

theorem sound_PackedInverse : PackedInverse.Goal := by
  intro w xs hbound
  have hfl : fromLittle (pack w xs) = word w xs := fromLittle_pack w xs
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_map]
  by_cases hi : i < xs.length
  · rw [List.getElem?_range hi, List.getElem?_eq_getElem hi]
    simp only [Option.map_some, Option.some.injEq]
    change (fromLittle (pack w xs) / 2 ^ (i * w)) % 2 ^ w = xs[i]
    rw [hfl]
    exact word_digit w i xs hbound hi
  · have hle : xs.length ≤ i := by omega
    have hl : (List.range xs.length).length ≤ i := by simpa using hle
    rw [List.getElem?_eq_none hl, List.getElem?_eq_none hle]
    simp


private theorem fromLittle_take_mod (bs : Bytes) (m : Nat) (hbs : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.take m) = fromLittle bs % 256 ^ m := by
  induction bs generalizing m with
  | nil => simp [fromLittle, Nat.zero_mod]
  | cons b bs ih =>
      cases m with
      | zero => simp [fromLittle, Nat.mod_one]
      | succ m =>
          have hb : b < 256 := hbs b (by simp)
          have hbs' : ∀ x ∈ bs, x < 256 := by
            intro x hx
            exact hbs x (by simp [hx])
          simp only [List.take_succ_cons, fromLittle]
          rw [ih m hbs']
          have hMpos : 0 < 256 ^ m := Nat.pow_pos (by decide : 0 < 256)
          have hpowsucc : 256 ^ (m + 1) = 256 ^ m * 256 := by rw [Nat.pow_succ]
          rw [hpowsucc]
          have hlt : b + 256 * (fromLittle bs % 256 ^ m) < 256 ^ m * 256 := by
            have hmodlt := Nat.mod_lt (fromLittle bs) hMpos
            omega
          have hdecomp : b + 256 * fromLittle bs =
              (b + 256 * (fromLittle bs % 256 ^ m)) + (256 ^ m * 256) * (fromLittle bs / 256 ^ m) := by
            have hmad := Nat.mod_add_div (fromLittle bs) (256 ^ m)
            calc
              b + 256 * fromLittle bs
                  = b + 256 * (fromLittle bs % 256 ^ m + 256 ^ m * (fromLittle bs / 256 ^ m)) := by rw [hmad]
              _ = b + (256 * (fromLittle bs % 256 ^ m) + 256 * (256 ^ m * (fromLittle bs / 256 ^ m))) := by rw [Nat.mul_add]
              _ = (b + 256 * (fromLittle bs % 256 ^ m)) + 256 * (256 ^ m * (fromLittle bs / 256 ^ m)) := by rw [Nat.add_assoc]
              _ = (b + 256 * (fromLittle bs % 256 ^ m)) + (256 ^ m * 256) * (fromLittle bs / 256 ^ m) := by ac_rfl
          rw [hdecomp, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hlt]

private theorem fromLittle_drop_div (bs : Bytes) (s : Nat) (hbs : ∀ b ∈ bs, b < 256) :
    fromLittle (bs.drop s) = fromLittle bs / 256 ^ s := by
  induction s generalizing bs with
  | zero => simp
  | succ s ih =>
      cases bs with
      | nil => simp [fromLittle, Nat.zero_div]
      | cons b bs =>
          have hb : b < 256 := hbs b (by simp)
          have hbs' : ∀ x ∈ bs, x < 256 := by
            intro x hx
            exact hbs x (by simp [hx])
          simp only [List.drop_succ_cons, fromLittle]
          rw [ih bs hbs']
          rw [Nat.add_comm b (256 * fromLittle bs)]
          rw [Nat.pow_succ, Nat.mul_comm (256 ^ s) 256, ← Nat.div_div_eq_div_mul]
          rw [Nat.add_comm (256 * fromLittle bs) b]
          rw [Nat.add_mul_div_left b (fromLittle bs) (by decide : 0 < 256)]
          rw [Nat.div_eq_of_lt hb, Nat.zero_add]

/-! ## LocalRead -/

theorem sound_LocalRead : LocalRead.Goal := by
  intro bs w i hbs
  by_cases hw0 : w = 0
  · subst w
    simp [TinylambOpt.ResearchGoals.Compression.Wire.takeAt,
      TinylambOpt.ResearchGoals.Compression.Wire.get, Nat.mod_one]
  · let start := i * w / 8
    let shift := i * w % 8
    let m := byteCount (shift + w)
    have hfl : fromLittle ((bs.drop start).take m) =
        (fromLittle bs / 256 ^ start) % 256 ^ m := by
      rw [fromLittle_take_mod _ _ (by
        intro b hb
        exact hbs b (List.mem_of_mem_drop hb))]
      rw [fromLittle_drop_div _ _ hbs]
    change (fromLittle ((bs.drop start).take m) / 2 ^ shift) % 2 ^ w =
      (fromLittle bs / 2 ^ (i * w)) % 2 ^ w
    rw [hfl]
    have hbm := bits_le_byteCount (shift + w)
    have h8m_ge_shift : shift ≤ 8 * m := by
      dsimp [m] at hbm ⊢
      omega
    have h8m_ge_w : w ≤ 8 * m - shift := by
      dsimp [m] at hbm ⊢
      omega
    have hpow256 : 256 ^ m = 2 ^ (8 * m) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, Nat.pow_mul]
    let q := fromLittle bs / 256 ^ start
    let r := q % 256 ^ m
    let t := q / 256 ^ m
    have hq : q = r + 256 ^ m * t := by
      dsimp [r, t]
      exact (Nat.mod_add_div q (256 ^ m)).symm
    have hdivq : q / 2 ^ shift = r / 2 ^ shift + 2 ^ (8 * m - shift) * t := by
      rw [hq, hpow256]
      have hfactor : 2 ^ (8 * m) = 2 ^ shift * 2 ^ (8 * m - shift) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      rw [hfactor, Nat.mul_assoc]
      exact Nat.add_mul_div_left r (2 ^ (8 * m - shift) * t) (Nat.two_pow_pos shift)
    have hmod_eq : (q / 2 ^ shift) % 2 ^ w = (r / 2 ^ shift) % 2 ^ w := by
      rw [hdivq]
      have hfactor : 2 ^ (8 * m - shift) * t = 2 ^ w * (2 ^ (8 * m - shift - w) * t) := by
        have hexp : w + (8 * m - shift - w) = 8 * m - shift := by omega
        calc
          2 ^ (8 * m - shift) * t = 2 ^ (w + (8 * m - shift - w)) * t := by rw [hexp]
          _ = (2 ^ w * 2 ^ (8 * m - shift - w)) * t := by rw [Nat.pow_add]
          _ = 2 ^ w * (2 ^ (8 * m - shift - w) * t) := by rw [Nat.mul_assoc]
      rw [hfactor]
      exact Nat.add_mul_mod_self_left (r / 2 ^ shift) (2 ^ w)
        (2 ^ (8 * m - shift - w) * t)
    have h256start : 256 ^ start = 2 ^ (8 * start) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, Nat.pow_mul]
    have hqv : q / 2 ^ shift = fromLittle bs / 2 ^ (i * w) := by
      dsimp [q]
      rw [h256start, Nat.div_div_eq_div_mul]
      have hp : 2 ^ (8 * start) * 2 ^ shift = 2 ^ (i * w) := by
        rw [← Nat.pow_add]
        congr 1
        dsimp [start, shift]
        have hmad := Nat.div_add_mod (i * w) 8
        omega
      rw [hp]
    rw [← hmod_eq, hqv]

/-! ## PackedCharge -/

theorem sound_PackedCharge : PackedCharge.Goal := by
  intro w xs
  simp [pack, little]

/-! ## BitSplice -/

theorem sound_BitSplice : BitSplice.Goal := by
  intro w a b
  have hfa : fromLittle (pack w a) = word w a := fromLittle_pack w a
  have hfb : fromLittle (pack w b) = word w b := fromLittle_pack w b
  rw [hfa, hfb]
  unfold pack
  rw [List.length_append, word_append]


/-! ## PaddingDebt -/

theorem sound_PaddingDebt : PaddingDebt.Goal := by
  intro sizes
  have hstrong : (sizes.map byteCount).sum ≤ byteCount sizes.sum + (sizes.length - 1) := by
    induction sizes with
    | nil => simp [byteCount]
    | cons a as ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        by_cases hn : as.length = 0
        · have asnil : as = [] := List.eq_nil_of_length_eq_zero hn
          subst as
          simp [byteCount]
        · have hstep := byteCount_add_ge a as.sum
          have hbound := Nat.add_le_add_left ih (byteCount a)
          have hpred : (as.length - 1) + 1 = as.length := by omega
          have htarget : byteCount a + (byteCount as.sum + (as.length - 1)) ≤
              byteCount (a + as.sum) + as.length := by omega
          exact Nat.le_trans hbound htarget
  omega

/-! ## MinimumWidth -/

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

theorem sound_MinimumWidth : MinimumWidth.Goal := by
  intro xs w
  constructor
  · intro h
    cases xs with
    | nil => simp [peak, width]
    | cons a as =>
        obtain ⟨m, hm⟩ := max?_some_of_ne_nil (by simp : (a :: as) ≠ [])
        have hmprops := List.max?_eq_some_iff.mp hm
        have hm_lt : m < 2 ^ w := h m hmprops.1
        have hp : peak (a :: as) = m := by simp [peak, hm]
        rw [hp]
        by_cases hm0 : m = 0
        · simp [hm0, width]
        · exact (width_le_iff m w hm0).mpr hm_lt
  · intro hw x hx
    have hxp : x ≤ peak xs := le_peak hx
    have hpk : peak xs < 2 ^ w := by
      by_cases hp0 : peak xs = 0
      · rw [hp0]
        exact Nat.pow_pos (by decide : 0 < 2)
      · exact (width_le_iff (peak xs) w hp0).mp hw
    exact Nat.lt_of_le_of_lt hxp hpk

/-! ## BitPackBreakEven -/

theorem sound_BitPackBreakEven : BitPackBreakEven.Goal := by
  intro rows bits oldBytes overhead hlt
  let T := rows * oldBytes
  constructor
  · intro h
    have hc : byteCount (rows * bits) ≤ T - overhead - 1 := by
      dsimp [T] at hlt h ⊢
      omega
    have hres := bits_le_of_byteCount_le (rows * bits) (T - overhead - 1) hc
    simpa [T] using hres
  · intro h
    have hc : byteCount (rows * bits) ≤ T - overhead - 1 := by
      apply byteCount_le_of_le
      simpa [T] using h
    omega

/-! ## OneBitSaving -/

theorem sound_OneBitSaving : OneBitSaving.Goal := by
  intro n w hn hw
  have hwpred : (w - 1) + 1 = w := by omega
  have hdecomp : n = n % 8 + 8 * (n / 8) := (Nat.mod_add_div n 8).symm
  calc
    byteCount (n * (w - 1)) + n / 8
        = byteCount (n * (w - 1) + 8 * (n / 8)) := by rw [byteCount_add_mul_eight]
    _ ≤ byteCount (n * (w - 1) + 8 * (n / 8) + n % 8) := by
        apply byteCount_mono
        omega
    _ = byteCount (n * w) := by
        have hw_eq : n * w = n * (w - 1) + n := by
          calc
            n * w = n * ((w - 1) + 1) := by rw [hwpred]
            _ = n * (w - 1) + n := by rw [Nat.mul_add, Nat.mul_one]
        congr 1
        calc
          n * (w - 1) + 8 * (n / 8) + n % 8 = n * (w - 1) + (8 * (n / 8) + n % 8) := by omega
          _ = n * (w - 1) + n := by rw [Nat.add_comm (8 * (n / 8)) (n % 8), ← hdecomp]
          _ = n * w := hw_eq.symm

/-! ## SplitPenalty -/

theorem sound_SplitPenalty : SplitPenalty.Goal := by
  intro a b
  exact ⟨byteCount_add_le a b, byteCount_add_ge a b⟩

/-! ## AlignmentDebt -/

theorem sound_AlignmentDebt : AlignmentDebt.Goal := by
  intro unit sizes hu
  have hstrong : (sizes.map (align unit)).sum ≤
      align unit sizes.sum + (sizes.length - 1) * unit := by
    induction sizes with
    | nil => simp [align]
    | cons a as ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        have hstep := align_add unit a as.sum hu
        by_cases hn : as.length = 0
        · have asnil : as = [] := List.eq_nil_of_length_eq_zero hn
          subst as
          simp [align]
        · have hnpos : 0 < as.length := Nat.pos_of_ne_zero hn
          have hbound : align unit a + (List.map (align unit) as).sum ≤
              align unit a + (align unit as.sum + (as.length - 1) * unit) :=
            Nat.add_le_add_left ih _
          have htarget : align unit a + (align unit as.sum + (as.length - 1) * unit) ≤
              align unit (a + as.sum) + as.length * unit := by
            rw [← mul_pred_succ as.length unit hnpos]
            omega
          have hlen : as.length + 1 - 1 = as.length := by omega
          rw [hlen]
          exact Nat.le_trans hbound htarget
  omega

/-! ## MeanWidth (false) -/

-- Concrete counterexample: xs = [0, 1].  The average is 0, so
-- `width (xs.sum / xs.length) = 0` and `pack 0 [0,1] = []`; the peak is 1, so
-- `width (peak xs) = 1` and `pack 1 [0,1] = [2]`.  Thus the two packs differ.
theorem refutation_MeanWidth : ¬ MeanWidth.Goal := by
  intro h
  have hbad :
      pack (width (([0, 1] : List Nat).sum / ([0, 1] : List Nat).length)) ([0, 1] : List Nat) =
        pack (width (peak ([0, 1] : List Nat))) ([0, 1] : List Nat) :=
    h ([0, 1]) (by decide)
  have hne :
      pack (width (([0, 1] : List Nat).sum / ([0, 1] : List Nat).length)) ([0, 1] : List Nat) ≠
        pack (width (peak ([0, 1] : List Nat))) ([0, 1] : List Nat) := by
    native_decide
  exact hne hbad

end TinylambOpt.ResearchProofs.Compression.Wire

-- C++ references to this model or its proof.
-- table/tlcs_compression.hpp
-- table/tlcs_file.cpp
-- table/tlcs_file.hpp
