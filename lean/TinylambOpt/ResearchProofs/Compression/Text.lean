import TinylambOpt.ResearchGoals.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Text

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Text
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp (common)

/-! Proofs and counterexamples for the text-compression goals.

Wire-level facts (ULEB charge/decoding, bit packing) are reproduced here as
private helpers so this module does not depend on another proof module. -/

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
    calc
      n < 2 ^ width n := lt_two_pow_width n
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
        · simp
          omega
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
    · intro h
      omega
    · intro h
      omega

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
          calc
            n < 128 := hn
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
          calc
            n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc
              n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

private theorem uleb_len (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  apply Nat.le_antisymm
  · have hle : (uleb n).length ≤ ubytes n :=
      (uleb_len_le_iff n (ubytes n) hubpos).mpr
        ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
    exact hle
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

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
  calc
    v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
    _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
    _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
    _ = v % m + m * ((v / m) % 256) := by rfl

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

private theorem fromLittle_little_four (n : Nat) (hn : n < 2 ^ 32) :
    fromLittle (little n 4) = n := by
  unfold little
  rw [fromLittle_little]
  have h256 : (256 : Nat) ^ 4 = 2 ^ 32 := by decide
  rw [h256]
  exact Nat.mod_eq_of_lt hn

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

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have h := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  omega

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

private theorem packed_inverse (w : Nat) (xs : List Nat) (hbound : ∀ x ∈ xs, x < 2 ^ w) :
    (List.range xs.length).map (get (pack w xs) w) = xs := by
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

private theorem common_le_left (a b : Bytes) : common a b ≤ a.length := by
  induction a generalizing b with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases h : x == y
          · simp [common, h]
            have := ih ys
            omega
          · simp [common, h]

private theorem common_le_right (a b : Bytes) : common a b ≤ b.length := by
  induction a generalizing b with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases h : x == y
          · simp [common, h]
            have := ih ys
            omega
          · simp [common, h]

private theorem take_common_append_drop (a b : Bytes) :
    a.take (common a b) ++ b.drop (common a b) = b := by
  induction a generalizing b with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases h : x == y
          · simp only [common, h, ↓reduceIte]
            rw [Nat.add_comm 1 (common xs ys)]
            simp only [List.take_succ_cons, List.drop_succ_cons]
            change x :: (List.take (common xs ys) xs ++ List.drop (common xs ys) ys) = y :: ys
            rw [ih ys]
            exact congrArg (fun z => z :: ys) (by simpa using h)
          · simp [common, h]

private theorem lengths_length (ws : Words) :
    (lengths ws).length = (ws.map (fun w => ubytes w.length)).sum + ws.flatten.length := by
  induction ws with
  | nil => simp [lengths]
  | cons w ws ih =>
      rw [lengths, List.flatMap_cons]
      rw [List.length_append, List.length_append, uleb_len]
      change ubytes w.length + w.length + (lengths ws).length =
        (List.map (fun x => ubytes x.length) (w :: ws)).sum + (w :: ws).flatten.length
      rw [ih]
      simp only [List.flatten_cons, List.length_append, List.map_cons, List.sum_cons]
      omega

private theorem prefix_sum_le (ws : Words) (i : Nat) :
    ((ws.take i).map List.length).sum ≤ ws.flatten.length := by
  induction ws generalizing i with
  | nil => simp
  | cons w ws ih =>
      cases i with
      | zero => simp
      | succ i =>
          simp only [List.take_succ_cons, List.map_cons, List.sum_cons]
          have h := ih i
          simp only [List.flatten_cons, List.length_append]
          omega

private theorem idx_lt_pow_of_width (d : Words) (i : Nat) (hi : i < d.length) :
    i < 2 ^ width (d.length - 1) := by
  have hdpos : 0 < d.length := by omega
  by_cases hd1 : d.length = 1
  · simp [hd1, width] at hi ⊢
    omega
  · have hmpos : 0 < d.length - 1 := by omega
    have hm : d.length - 1 < 2 ^ ((d.length - 1).log2 + 1) := Nat.lt_log2_self
    have hw : width (d.length - 1) = (d.length - 1).log2 + 1 := by
      simp [width, Nat.ne_of_gt hmpos]
    rw [hw]
    have : i ≤ d.length - 1 := by omega
    omega

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem frontBody_length (ws : Words) :
    let d := dictionary ws
    (frontBody ws).length = ubytes d.length + (entries [] d).length +
      byteCount (ws.length * width (d.length - 1)) := by
  dsimp only
  unfold frontBody
  simp [uleb_len, List.length_append, pack_length, Nat.add_assoc]

/-! ## Word-stream codecs -/

theorem StreamOffsets_sound : StreamOffsets.Goal := by
  intro ws tail
  induction ws generalizing tail with
  | nil => simp [lengths, readLengths]
  | cons w ws ih =>
      change readLengths (ws.length + 1)
          ((uleb w.length ++ w ++ lengths ws) ++ tail) = some (w :: ws, tail)
      have hr : readU (uleb w.length ++ (w ++ (lengths ws ++ tail))) =
          some (w.length, w ++ (lengths ws ++ tail)) := by
        simpa [List.append_assoc] using readU_uleb w.length (w ++ (lengths ws ++ tail))
      have hdrop : (w ++ (lengths ws ++ tail)).drop w.length = lengths ws ++ tail := by
        simp
      simp [readLengths, List.append_assoc, hr, hdrop, ih]

theorem OffsetSaving_sound : OffsetSaving.Goal := by
  intro ws overhead
  rw [lengths_length]
  have hs : streamBytes (ws.map List.length) = (ws.map (fun w => ubytes w.length)).sum := by
    simp [streamBytes, List.map_map, Function.comp_def]
  rw [hs]
  unfold raw
  rw [List.length_flatten]
  omega

theorem LengthSeek_sound : LengthSeek.Goal := by
  intro ws i hi
  induction ws generalizing i with
  | nil => simp at hi
  | cons w ws ih =>
      cases i with
      | zero => simp
      | succ i =>
          simp only [List.take_succ_cons, List.map_cons, List.sum_cons, List.flatten_cons]
          have h := ih i (by simpa using hi)
          let P := (List.map List.length (List.take i ws)).sum
          have hd : List.drop (w.length + P) w = [] := by
            apply List.eq_nil_of_length_eq_zero
            rw [List.length_drop]
            omega
          have hdrop : List.drop (w.length + P) (w ++ ws.flatten) = List.drop P ws.flatten := by
            rw [List.drop_append, hd]
            simp
          rw [hdrop]
          simpa [P] using h

theorem FrontInverse_sound : FrontInverse.Goal := by
  intro prev ws tail
  induction ws generalizing prev tail with
  | nil => simp [entries, readEntries]
  | cons w ws ih =>
      let k := common prev w
      let suffix := w.drop k
      have hkprev : k ≤ prev.length := by simpa [k] using common_le_left prev w
      have hkprev' : common prev w ≤ List.length prev := common_le_left prev w
      have hdrop : (suffix ++ entries w ws ++ tail).drop suffix.length = entries w ws ++ tail := by
        simp
      have htake : (suffix ++ entries w ws ++ tail).take suffix.length = suffix := by
        simp
      have hwrecon : prev.take k ++ suffix = w := by
        simpa [k, suffix] using take_common_append_drop prev w
      have hr1 : readU (uleb (common prev w) ++ (uleb (List.length w - common prev w) ++
          (List.drop (common prev w) w ++ (entries w ws ++ tail)))) =
          some (common prev w, uleb (List.length w - common prev w) ++
            (List.drop (common prev w) w ++ (entries w ws ++ tail))) := by
        simpa using readU_uleb (common prev w)
          (uleb (List.length w - common prev w) ++ (List.drop (common prev w) w ++ (entries w ws ++ tail)))
      have hr2 : readU (uleb (List.length w - common prev w) ++
          (List.drop (common prev w) w ++ (entries w ws ++ tail))) =
          some (List.length w - common prev w, List.drop (common prev w) w ++ (entries w ws ++ tail)) := by
        simpa using readU_uleb (List.length w - common prev w)
          (List.drop (common prev w) w ++ (entries w ws ++ tail))
      have hdrop' : (List.drop (common prev w) w ++ (entries w ws ++ tail)).drop
          (List.length w - common prev w) = entries w ws ++ tail := by
        simp
      have htake' : (List.drop (common prev w) w ++ (entries w ws ++ tail)).take
          (List.length w - common prev w) = List.drop (common prev w) w := by
        simp
      have hwrecon' : prev.take (common prev w) ++ List.drop (common prev w) w = w := by
        exact take_common_append_drop prev w
      simp only [entries]
      simp [entry, readEntries, hr1, hr2, hkprev', hdrop', htake', hwrecon', ih]

theorem FrontEntryCharge_sound : FrontEntryCharge.Goal := by
  intro a b
  dsimp only
  let k := common a b
  have hk : k ≤ b.length := by simpa [k] using common_le_right a b
  simp [entry, uleb_len, List.length_drop]
  omega

private theorem mapM_range_eq_some (f : Nat → Option Bytes) : ∀ xs : Words,
    (∀ i (hi : i < xs.length), f i = some xs[i]) → (List.range xs.length).mapM f = some xs
  | [], _ => by simp
  | w :: ws, h => by
      have h0 : f 0 = some w := by
        rw [h 0 (by simp)]
        simp
      have hrest : ∀ i (hi : i < ws.length), f (i + 1) = some ws[i] := by
        intro i hi
        rw [h (i + 1) (by simpa using Nat.succ_lt_succ hi)]
        simp
      change List.mapM f (List.range (ws.length + 1)) = some (w :: ws)
      rw [List.range_succ_eq_map]
      have hrest' : ∀ i (hi : i < ws.length), (f ∘ Nat.succ) i = some ws[i] := by
        intro i hi
        simpa [Function.comp_def] using hrest i hi
      have hrec := mapM_range_eq_some (f ∘ Nat.succ) ws hrest'
      simp [h0, hrec]

theorem FrontPageInverse_sound : FrontPageInverse.Goal := by
  intro ws
  let d := dictionary ws
  let ids := ws.map d.idxOf
  let w := width (d.length - 1)
  have hmem : ∀ x ∈ ws, x ∈ d := by
    intro x hx
    simp [d, dictionary, List.mem_mergeSort, List.mem_eraseDups, hx]
  have hidxlt : ∀ x ∈ ws, d.idxOf x < d.length := by
    intro x hx
    exact List.idxOf_lt_length_of_mem (hmem x hx)
  have hbound : ∀ x ∈ ids, x < 2 ^ w := by
    intro x hx
    rcases List.mem_map.mp hx with ⟨y, hy, rfl⟩
    exact idx_lt_pow_of_width d (d.idxOf y) (hidxlt y hy)
  have hpk := packed_inverse w ids hbound
  have hlen_ids : ids.length = ws.length := by simp [ids]
  have hread : readEntries d.length [] (entries [] d ++ pack w ids) = some (d, pack w ids) :=
    FrontInverse_sound [] d (pack w ids)
  have hget : ∀ i (hi : i < ws.length), d[get (pack w ids) w i]? = some (ws[i]'hi) := by
    intro i hi
    have hi' : i < ids.length := by simpa [hlen_ids] using hi
    have hdigit : get (pack w ids) w i = ids[i] := by
      have hpk_i := congrArg (fun l => l[i]?) hpk
      rw [List.getElem?_map, List.getElem?_range hi'] at hpk_i
      simpa [List.getElem?_eq_getElem hi'] using hpk_i
    have hmemi : ws[i]'hi ∈ ws := List.getElem_mem hi
    have hidxt : d.idxOf (ws[i]'hi) < d.length := hidxlt (ws[i]'hi) hmemi
    have hidx_get : ids[i] = d.idxOf (ws[i]'hi) := by
      change (ws.map (fun a => d.idxOf a))[i] = d.idxOf (ws[i]'hi)
      simp
    rw [hdigit, hidx_get]
    change d[d.idxOf (ws[i]'hi)]? = some (ws[i]'hi)
    rw [List.getElem?_eq_getElem hidxt, List.getElem_idxOf hidxt]
  have hmap : (List.range ws.length).mapM (fun i => d[get (pack w ids) w i]?) = some ws :=
    mapM_range_eq_some (fun i => d[get (pack w ids) w i]?) ws hget
  have hr : readU (frontBody ws) = some (d.length, entries [] d ++ pack w ids) := by
    simpa [frontBody, d, ids, w, List.append_assoc] using
      (readU_uleb d.length (entries [] d ++ pack w ids))
  unfold readBody
  simp [hr, hread, w, ids, hmap, pack_length]

theorem FrontPageCharge_sound : FrontPageCharge.Goal := by
  intro ws
  simp [packet, little, frontBody_length]
  omega

/-! ## Frames and dictionary accounting -/

private theorem packet_header (tag n : Nat) (body : Bytes) :
    ((packet tag n body).drop 1).take 4 = little n 4 ∧
    ((packet tag n body).drop 5).take 4 = little body.length 4 ∧
    (packet tag n body).drop 9 = body ∧
    (packet tag n body)[0]? = some tag ∧
    9 ≤ (packet tag n body).length := by
  have h1 : (little n 4).length = 4 := by simp [little]
  have h2 : (little body.length 4).length = 4 := by simp [little]
  have hd1 : (packet tag n body).drop 1 = little n 4 ++ (little body.length 4 ++ body) := by
    simp [packet]
  have hd5 : (packet tag n body).drop 5 = little body.length 4 ++ body := by
    change List.drop (1 + 4) (packet tag n body) = little body.length 4 ++ body
    rw [← List.drop_drop, hd1]
    simp [h1]
  have hd9 : (packet tag n body).drop 9 = body := by
    change List.drop (1 + 8) (packet tag n body) = body
    rw [← List.drop_drop, hd1]
    simp [h1, h2, List.drop_append]
  refine ⟨?_, ?_, hd9, ?_, ?_⟩
  · rw [hd1]
    simp [h1]
  · rw [hd5]
    simp [h2]
  · simp [packet]
  · simp [packet, little]
    omega

theorem FramedInverse_sound : FramedInverse.Goal := by
  intro ws hn hb
  have hh := packet_header 1 ws.length (frontBody ws)
  have hinv : readBody ws.length (frontBody ws) = some ws := FrontPageInverse_sound ws
  have hh1 : ((packet 1 ws.length (frontBody ws)).tail).take 4 = little ws.length 4 := by
    simpa [List.tail] using hh.1
  unfold readPacket
  simp [hh1, hh.2.1, hh.2.2.1, hh.2.2.2.1, hh.2.2.2.2,
    fromLittle_little_four ws.length hn, fromLittle_little_four (frontBody ws).length hb, hinv]

private theorem packet_take_header (tag n : Nat) (body : Bytes) (k : Nat) (hk : 9 ≤ k)
    (hkle : k ≤ (packet tag n body).length) :
    (((packet tag n body).take k).drop 5).take 4 = little body.length 4 ∧
    ((packet tag n body).take k).drop 9 = body.take (k - 9) ∧
    ((packet tag n body).take k).length = k := by
  have h2 : (little body.length 4).length = 4 := by simp [little]
  have hh := packet_header tag n body
  constructor
  · rw [List.drop_take]
    simp only [List.take_take]
    have hmin : min 4 (k - 5) = 4 := by omega
    rw [hmin, hh.2.1]
  constructor
  · rw [List.drop_take, hh.2.2.1]
  · rw [List.length_take]
    exact Nat.min_eq_left hkle

theorem TruncatedFrame_sound : TruncatedFrame.Goal := by
  intro ws n hb frame hnlt
  let body := frontBody ws
  have hfrlen : frame.length = 9 + body.length := by
    change (packet 1 ws.length body).length = 9 + body.length
    simp [packet, little]
    omega
  by_cases h9 : n < 9
  · have hmin : min n frame.length = n := Nat.min_eq_left (Nat.le_of_lt hnlt)
    have hshort : (frame.take n).length < 9 := by
      rw [List.length_take, hmin]
      exact h9
    have hcond : min n frame.length < 9 ∨ ¬ (frame.take n)[0]? = some 1 := by
      left
      rw [hmin]
      exact h9
    simp [readPacket, hcond]
  · have hk : 9 ≤ n := Nat.le_of_not_gt h9
    have hle : n ≤ (packet 1 ws.length body).length := by
      simpa [frame] using Nat.le_of_lt hnlt
    have hh := packet_take_header 1 ws.length body n hk hle
    have hbnd : body.length < 2 ^ 32 := by simpa [body] using hb
    have hsize : fromLittle (((frame.take n).drop 5).take 4) = body.length := by
      change fromLittle ((((packet 1 ws.length body).take n).drop 5).take 4) = body.length
      rw [hh.1]
      exact fromLittle_little_four body.length hbnd
    have hblen : ((frame.take n).drop 9).length = n - 9 := by
      rw [hh.2.1, List.length_take]
      omega
    have hsmall : n - 9 < body.length := by
      rw [hfrlen] at hnlt
      omega
    have hne : body.length != ((frame.take n).drop 9).length := by
      simpa [hblen] using (Nat.ne_of_lt hsmall).symm
    have hminlen : min n (packet 1 ws.length (frontBody ws)).length = n := Nat.min_eq_left hle
    have htag : ((packet 1 ws.length (frontBody ws)).take n)[0]? = some 1 := by
      simp [packet, show 0 < n by omega]
    have hne0 : (frontBody ws).length != min n (packet 1 ws.length (frontBody ws)).length - 9 := by
      simpa [body] using hne
    have hne' : (frontBody ws).length != n - 9 := by simpa [hminlen] using hne0
    simp [readPacket, frame, body, hminlen, htag, hsize, hblen, hne']

private theorem removeAll_eq_nil_of_subset (xs ys : Words) (hsub : ∀ y ∈ ys, y ∈ xs) :
    ys.removeAll xs = [] := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys =>
          have : y ∈ ([] : Words) := hsub y (by simp)
          simp at this
  | cons a as ih =>
      rw [List.removeAll_cons]
      apply ih
      intro z hz
      have hz' : z ∈ (a :: as) := hsub z (List.mem_filter.mp hz).1
      have hzne : z ≠ a := by
        have hf := (List.mem_filter.mp hz).2
        intro hza
        subst z
        simp at hf
      simpa [hzne] using hz'

private theorem eraseDups_append_of_subset (xs ys : Words)
    (hsub : ∀ y ∈ ys, y ∈ xs) : (xs ++ ys).eraseDups = xs.eraseDups := by
  rw [List.eraseDups_append, removeAll_eq_nil_of_subset xs ys hsub]
  simp

private theorem eraseDups_replicate (ws : Words) (n : Nat) (hn : 0 < n) :
    ((List.replicate n ws).flatten).eraseDups = ws.eraseDups := by
  cases n with
  | zero => omega
  | succ n =>
      change (ws ++ (List.replicate n ws).flatten).eraseDups = ws.eraseDups
      apply eraseDups_append_of_subset
      intro y hy
      rcases List.mem_flatten.mp hy with ⟨l, hl, hyl⟩
      have hl' : l = ws := (List.mem_replicate.mp hl).2
      simpa [hl'] using hyl

private theorem dictionary_replicate (ws : Words) (n : Nat) (hn : 0 < n) :
    dictionary ((List.replicate n ws).flatten) = dictionary ws := by
  simp [dictionary, eraseDups_replicate ws n hn]

private theorem eraseDups_replicate_one (w : Bytes) (n : Nat) (hn : 0 < n) :
    (List.replicate n w).eraseDups = [w] := by
  cases n with
  | zero => omega
  | succ n =>
      simp [List.replicate_succ, List.eraseDups_cons]

private theorem pack_zero (xs : List Nat) : pack 0 xs = [] := by
  simp [pack, little, byteCount]

theorem ReplicatedDictionary_sound : ReplicatedDictionary.Goal := by
  intro ws n hn
  let repeated := (List.replicate n ws).flatten
  let d := dictionary ws
  have hdict : dictionary repeated = d := by simp [repeated, d, dictionary_replicate ws n hn]
  change (frontBody repeated).length = ubytes d.length + (entries [] d).length +
    byteCount (n * ws.length * width (d.length - 1))
  rw [frontBody_length repeated, hdict]
  simp [repeated, List.length_flatten]

theorem SingleTerm_sound : SingleTerm.Goal := by
  intro w n hn
  have hdict : dictionary (List.replicate n w) = [w] := by
    simp [dictionary, eraseDups_replicate_one w n hn]
  have hdict1 : dictionary [w] = [w] := by
    simp [dictionary, List.eraseDups_cons]
  have hw0 : width 0 = 0 := by simp [width]
  simp [frontBody, hdict, hdict1, hw0, pack_zero]

private theorem getLast_getD_of_ne_nil (l : Words) (hne : l ≠ []) (a b : Bytes) :
    l.getLast?.getD a = l.getLast?.getD b := by
  cases l with
  | nil => simp at hne
  | cons x xs =>
      cases xs with
      | nil => simp
      | cons y ys =>
          simpa using getLast_getD_of_ne_nil (y :: ys) (by simp) a b

private theorem entries_append (prev : Bytes) (pre post : Words) :
    entries prev (pre ++ post) = entries prev pre ++ entries (pre.getLast?.getD prev) post := by
  induction pre generalizing prev with
  | nil => simp [entries]
  | cons w ws ih =>
      cases ws with
      | nil => simp [entries]
      | cons w' ws' =>
          have hlast : (w' :: ws').getLast?.getD w = (w' :: ws').getLast?.getD prev :=
            getLast_getD_of_ne_nil (w' :: ws') (by simp) w prev
          have hi := ih w
          calc
            entries prev (w :: w' :: ws' ++ post)
                = entry prev w ++ entries w ((w' :: ws') ++ post) := by rfl
            _ = entry prev w ++ (entries w (w' :: ws') ++
                  entries ((w' :: ws').getLast?.getD w) post) := by rw [hi]
            _ = entries prev (w :: w' :: ws') ++
                  entries ((w' :: ws').getLast?.getD prev) post := by
                    simp [entries, hlast, List.append_assoc]
            _ = entries prev (w :: w' :: ws') ++
                  entries ((w :: w' :: ws').getLast?.getD prev) post := by simp

private theorem entries_cons_sub (p q w : Bytes) (ws : Words) :
    ((entries p (w :: ws)).length : Int) - (entries q (w :: ws)).length =
      ((entry p w).length : Int) - (entry q w).length := by
  simp [entries]
  omega

theorem ShardDictionaryBudget_sound : ShardDictionaryBudget.Goal := by
  intro a b
  change ((frontBody (a ++ b)).length : Int) - (frontBody a).length - (frontBody b).length =
    ((ubytes (dictionary (a ++ b)).length + (entries [] (dictionary (a ++ b))).length : Nat) : Int) -
      ((ubytes (dictionary a).length + (entries [] (dictionary a)).length : Nat) : Int) -
      ((ubytes (dictionary b).length + (entries [] (dictionary b)).length : Nat) : Int) +
      (byteCount ((a ++ b).length * width ((dictionary (a ++ b)).length - 1)) : Int) -
      (byteCount (a.length * width ((dictionary a).length - 1)) : Int) -
      (byteCount (b.length * width ((dictionary b).length - 1)) : Int)
  rw [frontBody_length (a ++ b), frontBody_length a, frontBody_length b]
  omega

theorem AdjacentExchange_sound : AdjacentExchange.Goal := by
  intro prev a b tail
  cases tail with
  | nil =>
      simp [entries]
      omega
  | cons w ws =>
      have hs := entries_cons_sub b a w ws
      simp [entries] at hs ⊢
      omega

theorem RestartCharge_sound : RestartCharge.Goal := by
  intro prev pre post
  dsimp only
  let last := pre.getLast?.getD prev
  have happ : entries prev (pre ++ post) = entries prev pre ++ entries last post := by
    simpa [last] using entries_append prev pre post
  cases post with
  | nil =>
      simp [entries]
  | cons w ws =>
      have hs := entries_cons_sub [] last w ws
      simp [happ, entries, last] at hs ⊢
      omega

theorem SuffixDictionary_sound : SuffixDictionary.Goal := by
  intro ws
  have h' : readBody ws.length (frontBody (ws.map List.reverse)) =
      some (ws.map List.reverse) := by
    simpa using FrontPageInverse_sound (ws.map List.reverse)
  rw [h']
  simp [Function.comp_def]

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hbpos : 0 < ubytes b := by
    unfold ubytes
    exact Nat.lt_of_lt_of_le Nat.zero_lt_one (Nat.le_max_left 1 _)
  apply (ubytes_le_iff a (ubytes b) hbpos).mpr
  exact Nat.lt_of_le_of_lt h ((ubytes_le_iff b (ubytes b) hbpos).mp (Nat.le_refl _))

private theorem flatten_prepend_length (stem : Bytes) (tails : Words) :
    (tails.map (stem ++ ·)).flatten.length = stem.length * tails.length + tails.flatten.length := by
  induction tails with
  | nil => simp
  | cons t ts ih =>
      simp [ih, List.length_append, Nat.mul_succ]
      omega

private theorem sum_ubytes_prepend (stem : Bytes) : ∀ tails : Words,
    (tails.map (fun t => ubytes t.length)).sum ≤
      (tails.map (fun t => ubytes (stem.length + t.length))).sum
  | [] => by simp
  | t :: ts => by
      simp
      have h := sum_ubytes_prepend stem ts
      have hh : ubytes t.length ≤ ubytes (stem.length + t.length) := ubytes_mono (by omega)
      omega

theorem CommonStemBudget_sound : CommonStemBudget.Goal := by
  intro stem tails
  dsimp only
  rw [lengths_length, lengths_length, flatten_prepend_length]
  have hs := sum_ubytes_prepend stem tails
  have hcomm : tails.length * stem.length = stem.length * tails.length := Nat.mul_comm _ _
  simp [List.map_map, Function.comp_def, List.length_append] at hs ⊢
  omega

theorem EmptyTerm_sound : EmptyTerm.Goal := by
  intro n tail
  simpa using FrontInverse_sound [] (List.replicate n []) tail

theorem EndOffsetWidth_sound : EndOffsetWidth.Goal := by
  intro ws w
  constructor
  · intro h
    have hw := h ws.length (Nat.le_refl _)
    simpa using hw
  · intro h i hi
    exact Nat.lt_of_le_of_lt (prefix_sum_le ws i) h

/-! ## Disjoint token substitution -/

private theorem expand_no_token (pat s : Bytes) (h : ∀ x ∈ s, x ≠ 256) :
    expand pat s = s := by
  induction s with
  | nil => simp [expand]
  | cons x xs ih =>
      have hx : x ≠ 256 := h x (by simp)
      have hxs : ∀ y ∈ xs, y ≠ 256 := by
        intro y hy
        exact h y (by simp [hy])
      simpa [expand, hx] using ih hxs

private theorem token_inverse_fuel : ∀ (fuel : Nat) (pat s : Bytes),
    (∀ x ∈ s, x < 256) → expand pat (symbols pat fuel s) = s := by
  intro fuel
  induction fuel with
  | zero =>
      intro pat s h
      simp [symbols]
      apply expand_no_token
      intro x hx
      have := h x hx
      omega
  | succ fuel ih =>
      intro pat s h
      cases s with
      | nil => simp [symbols, expand]
      | cons x xs =>
          by_cases hm : pat ≠ [] && pat.isPrefixOf (x :: xs)
          · have hparts := Bool.and_eq_true_iff.mp hm
            have hne : pat ≠ [] := of_decide_eq_true hparts.1
            have hpref : pat.isPrefixOf (x :: xs) := hparts.2
            obtain ⟨rest, hrest⟩ := (List.isPrefixOf_iff_prefix).mp hpref
            have hmprop : ¬ pat = [] ∧ pat <+: x :: xs := by
              exact ⟨hne, List.isPrefixOf_iff_prefix.mp hpref⟩
            have hdrop : (x :: xs).drop pat.length = rest := by
              rw [← hrest]
              simp
            have hrestbytes : ∀ y ∈ rest, y < 256 := by
              intro y hy
              apply h
              rw [← hrest]
              simp [hy]
            have hrec := ih pat rest hrestbytes
            have hrec' : List.flatMap (fun z => if z = 256 then pat else [z])
                (symbols pat fuel rest) = rest := by
              simpa [expand] using hrec
            simp [symbols, hmprop, hdrop, expand, hrec', hrest]
          · have hmprop : ¬ (¬ pat = [] ∧ pat <+: x :: xs) := by
              intro hp
              apply hm
              rw [Bool.and_eq_true_iff]
              constructor
              · simp [hp.1]
              · exact List.isPrefixOf_iff_prefix.mpr hp.2
            have hx : x < 256 := h x (by simp)
            have hxs : ∀ y ∈ xs, y < 256 := by
              intro y hy
              exact h y (by simp [hy])
            have hrec := ih pat xs hxs
            have hrec' : List.flatMap (fun z => if z = 256 then pat else [z])
                (symbols pat fuel xs) = xs := by
              simpa [expand] using hrec
            have hxne : x ≠ 256 := by omega
            simp [symbols, hmprop, expand, hxne, hrec']

theorem TokenInverse_sound : TokenInverse.Goal := by
  intro pat s fuel hbytes
  exact token_inverse_fuel fuel pat s hbytes

private theorem count_256_eq_zero (s : Bytes) (h : ∀ x ∈ s, x < 256) : s.count 256 = 0 := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      have hxne : x ≠ 256 := by
        have := h x (by simp)
        omega
      have hxs : ∀ y ∈ xs, y < 256 := by
        intro y hy
        exact h y (by simp [hy])
      simp [ih hxs, hxne]

private theorem symbols_length_fuel : ∀ (fuel : Nat) (pat s : Bytes),
    (∀ x ∈ s, x < 256) →
    (symbols pat fuel s).length + (symbols pat fuel s).count 256 * (pat.length - 1) = s.length := by
  intro fuel
  induction fuel with
  | zero =>
      intro pat s h
      simp [symbols, count_256_eq_zero s h]
  | succ fuel ih =>
      intro pat s h
      cases s with
      | nil => simp [symbols]
      | cons x xs =>
          by_cases hm : pat ≠ [] && pat.isPrefixOf (x :: xs)
          · have hparts := Bool.and_eq_true_iff.mp hm
            have hne : pat ≠ [] := of_decide_eq_true hparts.1
            have hpref : pat.isPrefixOf (x :: xs) := hparts.2
            obtain ⟨rest, hrest⟩ := (List.isPrefixOf_iff_prefix).mp hpref
            have hmprop : ¬ pat = [] ∧ pat <+: x :: xs := by
              exact ⟨hne, List.isPrefixOf_iff_prefix.mp hpref⟩
            have hdrop : (x :: xs).drop pat.length = rest := by
              rw [← hrest]
              simp
            have hrestbytes : ∀ y ∈ rest, y < 256 := by
              intro y hy
              apply h
              rw [← hrest]
              simp [hy]
            have hrec := ih pat rest hrestbytes
            have hL : 1 ≤ pat.length := by
              have := List.ne_nil_iff_length_pos.mp hne
              omega
            have hlenrest : pat.length + rest.length = xs.length + 1 := by
              have := congrArg List.length hrest
              simpa using this
            simp [symbols, hmprop, hdrop, Nat.add_mul]
            omega
          · have hmprop : ¬ (¬ pat = [] ∧ pat <+: x :: xs) := by
              intro hp
              apply hm
              rw [Bool.and_eq_true_iff]
              constructor
              · simp [hp.1]
              · exact List.isPrefixOf_iff_prefix.mpr hp.2
            have hx : x < 256 := h x (by simp)
            have hxs : ∀ y ∈ xs, y < 256 := by
              intro y hy
              exact h y (by simp [hy])
            have hrec := ih pat xs hxs
            have hxne : x ≠ 256 := by omega
            simp [symbols, hmprop, hxne]
            have hgoal := congrArg (fun n => n + 1) hrec
            omega

theorem TokenBreakEven_sound : TokenBreakEven.Goal := by
  intro pat s fee hpat hbytes
  dsimp only
  have hlen := symbols_length_fuel s.length pat s hbytes
  omega

-- Counterexample: pat = [0,0], s = [0,0,0].  Greedy substitution emits
-- [256,0] (2 bytes); there are 2 overlapping hits, so the claimed identity
-- gives 2 + 2*1 = 4, but s.length = 3.  Overlapping occurrences are counted
-- twice by allHits even though the disjoint parse consumes the middle byte.
theorem OverlappingSavings_disproof : ¬ OverlappingSavings.Goal := by
  intro h
  have hh := h ([0, 0] : Bytes) ([0, 0, 0] : Bytes) (by decide : 1 < ([0, 0] : Bytes).length)
  have hbad : ¬ ((symbols ([0, 0] : Bytes) ([0, 0, 0] : Bytes).length ([0, 0, 0] : Bytes)).length +
      (List.range ([0, 0, 0] : Bytes).length).countP
        (fun i => ([0, 0] : Bytes).isPrefixOf (List.drop i ([0, 0, 0] : Bytes))) *
        (([0, 0] : Bytes).length - 1) = ([0, 0, 0] : Bytes).length) := by
    native_decide
  exact hbad hh

private def pc (x y : Nat) : Bytes → Nat
  | [] => 0
  | [_] => 0
  | a :: b :: rest => (if a == x && b == y then 1 else 0) + pc x y (b :: rest)

private theorem symbols_nil (pat : Bytes) (fuel : Nat) : symbols pat fuel [] = [] := by
  cases fuel <;> simp [symbols]

private theorem symbols_count_pc : ∀ (fuel : Nat) (x y : Nat), x ≠ y → x < 256 → y < 256 →
    ∀ s : Bytes, s.length ≤ fuel →
      (symbols [x, y] fuel s).count 256 = s.count 256 + pc x y s := by
  intro fuel
  induction fuel with
  | zero =>
      intro x y hxy hx hy s hlen
      have hs : s = [] := List.eq_nil_of_length_eq_zero (Nat.eq_zero_of_le_zero hlen)
      subst s
      simp [symbols, pc]
  | succ fuel ih =>
      intro x y hxy hx hy s hlen
      cases s with
      | nil => simp [symbols, pc]
      | cons z zs =>
          cases zs with
          | nil =>
              have hno : ¬ ([x, y] : Bytes).isPrefixOf [z] := by simp
              simp [symbols, hno, symbols_nil, pc]
          | cons w rest =>
              simp only [List.length_cons] at hlen
              by_cases hm : ([x, y] : Bytes).isPrefixOf (z :: w :: rest)
              · have hparts : x = z ∧ y = w := by simpa using hm
                have hz : z = x := hparts.1.symm
                have hw : w = y := hparts.2.symm
                subst z
                subst w
                have hfuel : rest.length ≤ fuel := by omega
                have hrec := ih x y hxy hx hy rest hfuel
                have hpc_skip : pc x y (y :: rest) = pc x y rest := by
                  cases rest with
                  | nil => simp [pc]
                  | cons r rs => simp [pc, hxy.symm]
                have hpc_full : pc x y (x :: y :: rest) = 1 + pc x y (y :: rest) := by
                  simp [pc]
                simp [symbols, hm, hpc_skip, hpc_full, hrec,
                  show x ≠ 256 by omega, show y ≠ 256 by omega]
                omega
              · have hpref_false : ¬ ([x, y] : Bytes) <+: z :: w :: rest := by
                  intro hp
                  apply hm
                  exact List.isPrefixOf_iff_prefix.mpr hp
                have hzf : ¬ (z = x ∧ w = y) := by
                  intro hp
                  apply hpref_false
                  rw [hp.1, hp.2]
                  simp
                have hfuel : (w :: rest).length ≤ fuel := by
                  apply Nat.le_of_succ_le_succ
                  simpa [Nat.add_assoc] using hlen
                have hrec := ih x y hxy hx hy (w :: rest) hfuel
                simp only [symbols, List.count_cons, pc]
                have hx256 : x ≠ 256 := by omega
                have hzf' : ¬ (x = z ∧ y = w) := by
                  intro hp
                  exact hzf ⟨hp.1.symm, hp.2.symm⟩
                have hcw : (w :: rest).count 256 = (if w = 256 then 1 else 0) + rest.count 256 := by
                  by_cases hw256 : w = 256 <;> simp [hw256] <;> omega
                by_cases hz256 : z = 256
                · simp [hz256, hx256, hx256.symm, hrec, hcw]
                  omega
                · simp [hz256, hzf, hzf', hrec, hcw]
                  omega

private theorem pc_append (x y : Nat) (_hxy : x ≠ y) (a b : Bytes) :
    pc x y (a ++ b) = pc x y a + pc x y b +
      (if a.getLast? == some x && b.head? == some y then 1 else 0) := by
  induction a with
  | nil => simp [pc]
  | cons z zs ih =>
      cases zs with
      | nil =>
          cases b with
          | nil => simp [pc]
          | cons w ws =>
              simp [pc]
              omega
      | cons w ws =>
          have hi := ih
          change (if z == x && w == y then 1 else 0) + pc x y (w :: ws ++ b) =
            ((if z == x && w == y then 1 else 0) + pc x y (w :: ws)) + pc x y b +
              (if (w :: ws).getLast? == some x && b.head? == some y then 1 else 0)
          rw [hi]
          omega

theorem BoundaryToken_sound : BoundaryToken.Goal := by
  intro a b x y hx hy hxy
  have hab := symbols_count_pc (a ++ b).length x y hxy hx hy (a ++ b) (Nat.le_refl _)
  have ha := symbols_count_pc a.length x y hxy hx hy a (Nat.le_refl _)
  have hb := symbols_count_pc b.length x y hxy hx hy b (Nat.le_refl _)
  have hp := pc_append x y hxy a b
  have hcount : (a ++ b).count 256 = a.count 256 + b.count 256 := by simp
  omega

end TinylambOpt.ResearchProofs.Compression.Text
