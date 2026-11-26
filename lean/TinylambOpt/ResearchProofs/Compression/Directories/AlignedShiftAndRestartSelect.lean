import TinylambOpt.ResearchGoals.Compression.Directories

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `AlignedShift` — `∀ w (a b : List Nat), 2^w ∣ a.sum → (positions w (ends
  (a++b))).drop a.length = (positions w (ends b)).map (· + (a.length + a.sum/2^w))`
- `AnchorScanBound` — `∀ w sizes a i cap, a ≤ i → i < sizes.length → (∀ x ∈ sizes, x ≤
  cap) → let ps := positions w (ends sizes) (ps[i]?.getD 0)-(ps[a]?.getD 0) ≤ (i-a) +
  ((i-a)*cap+2^w-1)/2^w`
- `ResetHighBase` — `∀ w a b, directoryBytes w (ends a) + directoryBytes w (ends b) ≤
  directoryBytes w (ends (a++b)) + 2`
- `RestartSelect` — `∀ w sizes stride i, 0 < stride → i < sizes.length → let xs := ends
  sizes let ps := positions w xs let anchor := ((anchors stride ps)[i/stride]?).getD
  0 seekFrom (high w xs) anchor (i%stride) = ps[i]?`
-/

namespace TinylambOpt.ResearchProofs.Compression.Directories.AlignedShiftAndRestartSelect

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Directories

set_option maxHeartbeats 0

/-! ## cumulative-sum (directory) helpers -/

private def cums (acc : Nat) (xs : List Nat) : List Nat := (xs.scanl (· + ·) acc).drop 1

private theorem cums_cons (acc x : Nat) (xs : List Nat) :
    cums acc (x :: xs) = (acc + x) :: cums (acc + x) xs := by
  unfold cums
  cases xs with
  | nil => simp [List.scanl]
  | cons y ys => simp [List.scanl]

private theorem cums_length (acc : Nat) (xs : List Nat) : (cums acc xs).length = xs.length := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih => simp [cums_cons, ih]

private theorem cums_get? (acc : Nat) (xs : List Nat) (i : Nat) :
    (cums acc xs)[i]? = if i < xs.length then some (acc + (xs.take (i + 1)).sum) else none := by
  induction i generalizing xs acc with
  | zero =>
      cases xs with
      | nil => simp [cums]
      | cons x xs => simp [cums_cons]
  | succ i ih =>
      cases xs with
      | nil => simp [cums]
      | cons x xs =>
          rw [cums_cons, List.getElem?_cons_succ, ih (acc + x) xs]
          have hsucc : i + 1 < (x :: xs).length ↔ i < xs.length := by simp
          by_cases hi : i < xs.length
          · have hs : i + 1 < (x :: xs).length := hsucc.mpr hi
            simp [hi, hs, List.take_succ_cons]
            omega
          · have hs : ¬ i + 1 < (x :: xs).length := by simpa using hi
            simp [hi, hs]

private theorem ends_length (sizes : List Nat) : (ends sizes).length = sizes.length := by
  unfold ends
  exact cums_length 0 sizes

private theorem ends_get? (sizes : List Nat) (i : Nat) :
    (ends sizes)[i]? = if i < sizes.length then some ((sizes.take (i + 1)).sum) else none := by
  change (cums 0 sizes)[i]? = if i < sizes.length then some ((sizes.take (i + 1)).sum) else none
  simpa using (cums_get? 0 sizes i)

private def endAt (sizes : List Nat) (i : Nat) := (ends sizes)[i]?.getD 0

private theorem endAt_eq (sizes : List Nat) (i : Nat) (hi : i < sizes.length) :
    endAt sizes i = (sizes.take (i + 1)).sum := by
  unfold endAt
  rw [ends_get?]
  simp [hi]

private theorem cums_append (acc : Nat) (a b : List Nat) :
    cums acc (a ++ b) = cums acc a ++ cums (acc + a.sum) b := by
  induction a generalizing acc with
  | nil => simp [cums]
  | cons x xs ih =>
      rw [List.cons_append, cums_cons]
      rw [ih (acc + x)]
      simp only [cums_cons, List.sum_cons, List.cons_append]
      rw [show acc + (x + xs.sum) = acc + x + xs.sum by omega]

private theorem cums_shift (acc : Nat) (xs : List Nat) :
    cums acc xs = (cums 0 xs).map (fun v => v + acc) := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih =>
      rw [cums_cons, cums_cons, ih (acc + x)]
      simp only [Nat.zero_add]
      rw [ih x]
      simp only [List.map_cons]
      rw [List.cons.injEq]
      constructor
      · omega
      · rw [List.map_map]
        apply List.map_congr_left
        intro v hv
        simp only [Function.comp_apply]
        omega

private theorem ends_append (a b : List Nat) :
    ends (a ++ b) = ends a ++ (ends b).map (fun v => v + a.sum) := by
  change cums 0 (a ++ b) = cums 0 a ++ (cums 0 b).map (fun v => v + a.sum)
  rw [cums_append]
  simp only [Nat.zero_add, Nat.add_zero]
  rw [cums_shift a.sum b]

private theorem sum_take_mono (xs : List Nat) {i j : Nat} (hij : i ≤ j) :
    (xs.take i).sum ≤ (xs.take j).sum := by
  induction xs generalizing i j with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i =>
          cases j with
          | zero => omega
          | succ j =>
              simp only [List.take_succ_cons, List.sum_cons]
              exact Nat.add_le_add_left (ih (by omega)) x

private theorem endAt_monotone (sizes : List Nat) {i j : Nat} (hij : i ≤ j)
    (hj : j < sizes.length) : endAt sizes i ≤ endAt sizes j := by
  have hi : i < sizes.length := by omega
  rw [endAt_eq sizes i hi, endAt_eq sizes j hj]
  exact sum_take_mono sizes (by omega)

private theorem endAt_succ (sizes : List Nat) (i : Nat) (hi : i + 1 < sizes.length) :
    endAt sizes (i + 1) = endAt sizes i + sizes[i + 1]'hi := by
  rw [endAt_eq sizes (i + 1) hi, endAt_eq sizes i (by omega)]
  rw [show sizes.take (i + 1 + 1) = sizes.take (i + 1) ++ [sizes[i + 1]'hi] by
    simpa [Nat.add_assoc] using (List.take_succ (l := sizes) (i := i + 1))]
  simp only [List.sum_append, List.sum_singleton]

private theorem endAt_sub_bound (sizes : List Nat) (a i cap : Nat)
    (hai : a ≤ i) (hi : i < sizes.length) (hcap : ∀ x ∈ sizes, x ≤ cap) :
    endAt sizes i - endAt sizes a ≤ (i - a) * cap := by
  induction i generalizing a with
  | zero =>
      have ha : a = 0 := by omega
      subst a
      simp
  | succ i ih =>
      by_cases haeq : a = i + 1
      · subst a
        simp
      · have hai' : a ≤ i := by omega
        have hi' : i < sizes.length := by omega
        have hrec := ih a hai' hi'
        rw [endAt_succ sizes i hi]
        have hcap_i : sizes[i + 1]'hi ≤ cap := hcap _ (List.getElem_mem hi)
        have hmono_ab : endAt sizes a ≤ endAt sizes i := endAt_monotone sizes hai' hi'
        have hsubadd : endAt sizes i + sizes[i + 1]'hi - endAt sizes a ≤
            (endAt sizes i - endAt sizes a) + sizes[i + 1]'hi := by omega
        have hia : i + 1 - a = (i - a) + 1 := by omega
        rw [hia, Nat.add_mul, Nat.one_mul]
        omega

/-! ## positions helpers -/

private theorem positions_length (w : Nat) (xs : List Nat) : (positions w xs).length = xs.length := by
  simp [positions]

private theorem positions_get? (w : Nat) (xs : List Nat) (i : Nat) :
    (positions w xs)[i]? = if i < xs.length then some (xs[i]?.getD 0 / 2^w + i) else none := by
  unfold positions
  rw [List.getElem?_map]
  by_cases hi : i < xs.length
  · have hz : i < (xs.zipIdx).length := by simpa [List.length_zipIdx] using hi
    rw [List.getElem?_eq_getElem hz, List.getElem_zipIdx hz]
    simp [hi]
  · have hz : (xs.zipIdx).length ≤ i := by simpa [List.length_zipIdx] using hi
    rw [List.getElem?_eq_none hz]
    simp [hi]

private def posAt (w : Nat) (xs : List Nat) (i : Nat) := (positions w xs)[i]?.getD 0

private theorem posAt_eq (w : Nat) (xs : List Nat) (i : Nat) (hi : i < xs.length) :
    posAt w xs i = xs[i]?.getD 0 / 2^w + i := by
  unfold posAt
  rw [positions_get?]
  simp [hi]

private theorem posAt_strict (w : Nat) (sizes : List Nat) {i j : Nat}
    (hij : i < j) (hj : j < sizes.length) :
    posAt w (ends sizes) i < posAt w (ends sizes) j := by
  have hi : i < sizes.length := by omega
  rw [posAt_eq w (ends sizes) i (by simpa [ends_length] using hi),
    posAt_eq w (ends sizes) j (by simpa [ends_length] using hj)]
  simp only [ends_get?]
  simp [hi, hj]
  have hle := sum_take_mono sizes (by omega : i + 1 ≤ j + 1)
  have hdiv := Nat.div_le_div_right (c := 2^w) hle
  omega

private theorem positions_pairwise (w : Nat) (sizes : List Nat) :
    (positions w (ends sizes)).Pairwise (· < ·) := by
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have hj' : j < sizes.length := by simpa [positions_length, ends_length] using hj
  have hi' : i < (positions w (ends sizes)).length := by omega
  have hjpos : j < (positions w (ends sizes)).length := hj
  have hv_i := List.getElem_mem hi'
  have hv_j := List.getElem_mem hjpos
  have hiv : (positions w (ends sizes))[i] = posAt w (ends sizes) i := by
    unfold posAt
    rw [List.getElem?_eq_getElem hi']
    rfl
  have hjv : (positions w (ends sizes))[j] = posAt w (ends sizes) j := by
    unfold posAt
    rw [List.getElem?_eq_getElem hjpos]
    rfl
  rw [hiv, hjv]
  exact posAt_strict w sizes hij hj'

private theorem max?_some_of_ne_nil : ∀ {l : List Nat}, l ≠ [] → ∃ m, l.max? = some m := by
  intro l hne
  cases l with
  | nil => contradiction
  | cons a as =>
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem le_peak {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ peak l := by
  have hne : l ≠ [] := by intro hnil; subst l; simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hle := hmprops.2 x hx
  have hp : peak l = m := by simp [peak, hm]
  rw [hp]
  exact hle

private theorem peak_mem {l : List Nat} (hne : l ≠ []) : peak l ∈ l := by
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hp : peak l = m := by simp [peak, hm]
  rw [hp]
  exact hmprops.1

private theorem endAt_le_peak (sizes : List Nat) (i : Nat) :
    endAt sizes i ≤ peak (ends sizes) := by
  unfold endAt
  by_cases hi : i < (ends sizes).length
  · rw [List.getElem?_eq_getElem hi]
    simp
    exact le_peak (List.getElem_mem hi)
  · rw [List.getElem?_eq_none (by omega : (ends sizes).length ≤ i)]
    simp

private theorem positions_lt_highBits (w : Nat) (sizes : List Nat) {p : Nat}
    (hp : p ∈ positions w (ends sizes)) : p < highBits w (ends sizes) := by
  rcases List.mem_iff_getElem.mp hp with ⟨i, hi, hival⟩
  have hi' : i < (ends sizes).length := by simpa [positions_length] using hi
  have hget : (positions w (ends sizes))[i] = posAt w (ends sizes) i := by
    unfold posAt
    rw [List.getElem?_eq_getElem hi]
    rfl
  have hpe := posAt_eq w (ends sizes) i hi'
  have hpval : p = endAt sizes i / 2^w + i := by
    rw [← hival, hget]
    exact hpe
  have hend_le := endAt_le_peak sizes i
  have hdiv : endAt sizes i / 2^w ≤ peak (ends sizes) / 2^w := Nat.div_le_div_right hend_le
  unfold highBits
  rw [hpval]
  omega

/-! ## bit packing helpers -/

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

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem fromLittle_pack (w : Nat) (xs : List Nat) :
    fromLittle (pack w xs) = word w xs := by
  unfold pack little
  rw [fromLittle_little]
  have hlt := word_lt_pow w xs
  have hb : xs.length * w ≤ 8 * byteCount (xs.length * w) := bits_le_byteCount (xs.length * w)
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
          exact ih xs ht (by simpa using hi)

private theorem word_digit_out (w i : Nat) (xs : List Nat) (hi : xs.length ≤ i) :
    (word w xs / 2 ^ (i * w)) % 2 ^ w = 0 := by
  have hlt := word_lt_pow w xs
  have hpowexp : xs.length * w ≤ i * w := Nat.mul_le_mul_right w hi
  have hpow : 2 ^ (xs.length * w) ≤ 2 ^ (i * w) := Nat.pow_le_pow_right (by decide : 0 < 2) hpowexp
  have hdiv : word w xs / 2 ^ (i * w) = 0 := Nat.div_eq_of_lt (Nat.lt_of_lt_of_le hlt hpow)
  rw [hdiv, Nat.zero_mod]

/-! ## the high bit-vector and select helpers -/

private theorem get_high (w : Nat) (xs : List Nat) (j : Nat) :
    get (high w xs) 1 j =
      if j < highBits w xs then (if (positions w xs).contains j then 1 else 0) else 0 := by
  unfold high
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get
  rw [fromLittle_pack]
  let bits := (List.range (highBits w xs)).map
    (fun j => if (positions w xs).contains j then 1 else 0)
  change (word 1 bits / 2 ^ (j * 1)) % 2 ^ 1 = _
  by_cases hj : j < highBits w xs
  · have hbnd : ∀ v ∈ bits, v < 2 ^ 1 := by
      intro v hv
      dsimp [bits] at hv
      rcases List.mem_map.mp hv with ⟨k, hk, rfl⟩
      by_cases hm : k ∈ positions w xs <;> simp [List.contains_eq_mem, hm]
    have hbitlen : bits.length = highBits w xs := by simp [bits]
    have hjbits : j < bits.length := by rw [hbitlen]; exact hj
    have hd := word_digit 1 j bits hbnd hjbits
    rw [hd]
    simp [bits, List.getElem?_eq_getElem hjbits, hj]
  · have hout := word_digit_out 1 j bits (by simpa [bits] using hj)
    rw [hout]
    simp [hj]

private theorem count_eq_one_of_pairwise {ps : List Nat} {a : Nat}
    (hinc : ps.Pairwise (· < ·)) (ha : a ∈ ps) : ps.count a = 1 := by
  induction ps with
  | nil => simp at ha
  | cons x xs ih =>
      simp only [List.count_cons]
      rcases List.mem_cons.mp ha with rfl | hmem
      · have hnot : ¬ a ∈ xs := by
          intro h
          have hlt := (List.pairwise_cons.mp hinc).1 a h
          omega
        rw [List.count_eq_zero_of_not_mem hnot]
        simp
      · have hinc' := (List.pairwise_cons.mp hinc).2
        have hne : ¬ x == a := by
          intro heq
          have heq' : x = a := by simpa using heq
          subst x
          have hlt := (List.pairwise_cons.mp hinc).1 a hmem
          omega
        rw [if_neg hne]
        rw [ih hinc' hmem]

private theorem range_filter_mem_eq_of_sorted (ps : List Nat) (N : Nat)
    (hinc : ps.Pairwise (· < ·)) (hlt : ∀ p ∈ ps, p < N) :
    (List.range N).filter (fun j => ps.contains j) = ps := by
  have hperm : ((List.range N).filter (fun j => ps.contains j)).Perm ps := by
    rw [List.perm_iff_count]
    intro a
    by_cases ha : a ∈ ps
    · have hcnt : ps.count a = 1 := count_eq_one_of_pairwise hinc ha
      rw [List.count_filter (by simpa [List.contains_eq_mem] using ha)]
      rw [List.count_range, hcnt]
      simp [hlt a ha]
    · have hcnt : ps.count a = 0 := List.count_eq_zero_of_not_mem ha
      rw [hcnt]
      apply List.count_eq_zero_of_not_mem
      intro hfa
      have := (List.mem_filter.mp hfa).2
      exact ha (by simpa [List.contains_eq_mem] using this)
  have hinc_filter : ((List.range N).filter (fun j => ps.contains j)).Pairwise (· < ·) := by
    rw [List.pairwise_filter, List.pairwise_iff_getElem]
    intro i j hi hj hij hpi hpj
    rw [List.getElem_range (by simpa using hi), List.getElem_range (by simpa using hj)]
    exact hij
  exact List.Perm.eq_of_pairwise (by
    intro a b ha hb hab hba
    omega) hinc_filter hinc hperm

private theorem ones_high (w : Nat) (sizes : List Nat) :
    ones (high w (ends sizes)) = positions w (ends sizes) := by
  let xs := ends sizes
  let bs := high w xs
  have hbslen : bs.length = byteCount (highBits w xs) := by
    simp [bs, high, pack, little]
  have hbnd : ∀ p ∈ positions w xs, p < 8 * bs.length := by
    intro p hp
    have hp1 : p < highBits w xs := by simpa [xs] using positions_lt_highBits w sizes hp
    have hb : highBits w xs ≤ 8 * byteCount (highBits w xs) :=
      bits_le_byteCount (highBits w xs)
    rw [hbslen]
    omega
  have hfiltered := range_filter_mem_eq_of_sorted (positions w xs) (8 * bs.length)
    (positions_pairwise w sizes) hbnd
  unfold ones
  have hpred : (fun j => get bs 1 j == 1) = (fun j => (positions w xs).contains j) := by
    funext j
    by_cases hj : j < highBits w xs
    · have hg := get_high w xs j
      change get bs 1 j = _ at hg
      rw [hg]
      by_cases hmem : j ∈ positions w xs <;> simp [hj, hmem]
    · have hg := get_high w xs j
      change get bs 1 j = _ at hg
      rw [hg]
      have hnot : (positions w xs).contains j = false := by
        simp only [List.contains_eq_mem, decide_eq_false_iff_not]
        intro hm
        have hp : j < highBits w xs := by
          simpa [xs] using positions_lt_highBits w sizes (by simpa [xs] using hm)
        omega
      have hnotmem : ¬ j ∈ positions w xs := by
        simpa [List.contains_eq_mem] using hnot
      simp [hj, hnotmem]
  rw [hpred]
  exact hfiltered

private theorem seekFrom_ones (bs : Bytes) (start rank : Nat) (hstart : start ≤ 8 * bs.length) :
    seekFrom bs start rank = ((ones bs).filter (fun j => start ≤ j))[rank]? := by
  let N := 8 * bs.length
  let scan := (List.range (N - start)).map (fun j => start + j)
  have hscan_def : scan = List.range' start (N - start) := by
    simp [scan, List.range'_eq_map_range]
  have hscan_sorted : scan.Pairwise (· < ·) := by
    rw [hscan_def, List.pairwise_iff_getElem]
    intro i j hi hj hij
    rw [List.getElem_range' (by simpa [List.length_range'] using hi),
      List.getElem_range' (by simpa [List.length_range'] using hj)]
    omega
  have hscan_lt : ∀ p ∈ scan, p < N := by
    intro p hp
    rw [hscan_def, List.mem_range'] at hp
    rcases hp with ⟨i, hi, rfl⟩
    omega
  have hscan_filt := range_filter_mem_eq_of_sorted scan N hscan_sorted hscan_lt
  have hpred (j : Nat) (hjN : j < N) : scan.contains j = decide (start ≤ j) := by
    rw [List.contains_eq_mem]
    by_cases hs : start ≤ j
    · simp [hs]
      rw [hscan_def, List.mem_range']
      refine ⟨j - start, ?_, ?_⟩
      · omega
      · omega
    · simp [hs]
      intro hmem
      rw [hscan_def, List.mem_range'] at hmem
      rcases hmem with ⟨k, hk, rfl⟩
      omega
  have hscan_eq : scan = (List.range N).filter (fun j => start ≤ j) := by
    rw [← hscan_filt]
    apply List.filter_congr
    intro j hj
    have hjN : j < N := by simpa [List.mem_range] using hj
    exact hpred j hjN
  unfold seekFrom ones
  change (scan.filter (fun j => get bs 1 j == 1))[rank]? =
    (((List.range N).filter (fun j => get bs 1 j == 1)).filter (fun j => start ≤ j))[rank]?
  rw [hscan_eq]
  rw [List.filter_filter]
  rw [List.filter_filter]
  congr 2
  funext j
  by_cases hs : start ≤ j <;> by_cases hp : get bs 1 j == 1 <;> simp [hs, hp, Bool.and_comm]

private theorem zipIdx_add (xs : List Nat) (k : Nat) :
    xs.zipIdx k = (xs.zipIdx 0).map (fun pi => (pi.1, k + pi.2)) := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.zipIdx_succ, ih]
      rw [List.map_map]
      apply List.map_congr_left
      intro pi hpi
      simp only [Function.comp_apply]
      congr 1
      omega

private theorem filter_zipIdx_mod_eq (stride : Nat) (xs : List Nat) :
    ((xs.zipIdx stride).filter (fun pi => pi.2 % stride == 0)).map Prod.fst =
      ((xs.zipIdx 0).filter (fun pi => pi.2 % stride == 0)).map Prod.fst := by
  rw [zipIdx_add]
  rw [List.filter_map]
  rw [List.map_map]
  have hfst : (Prod.fst ∘ fun pi : Nat × Nat => (pi.1, stride + pi.2)) = Prod.fst := by
    funext pi
    rfl
  rw [hfst]
  congr 1
  apply List.filter_congr
  intro pi hpi
  simp only [Function.comp_apply]
  have hm : (stride + pi.2) % stride = pi.2 % stride := by
    rw [Nat.add_comm, Nat.add_mod]
    simp
  rw [hm]

private theorem filter_zipIdx_small (stride : Nat) (hstride : 0 < stride) (xs : List Nat)
    (hlen : xs.length = stride) :
    ((xs.zipIdx 0).filter (fun pi => pi.2 % stride == 0)).map Prod.fst = [xs[0]] := by
  cases xs with
  | nil => simp at hlen; omega
  | cons x xs =>
      have hlen' : xs.length + 1 = stride := by simpa using hlen
      have htail : xs.length = stride - 1 := by omega
      rw [List.zipIdx_cons]
      simp only [List.filter_cons, List.map_cons]
      have hzero : (0 : Nat) % stride == 0 := by
        rw [Nat.mod_eq_of_lt hstride]
        rfl
      rw [if_pos hzero]
      have hnil : (xs.zipIdx 1).filter (fun pi => pi.2 % stride == 0) = [] := by
        rw [List.filter_eq_nil_iff]
        intro pi hpi hp
        rcases List.mem_iff_getElem.mp hpi with ⟨i, hi, hi_val⟩
        have hi_len : i < xs.length := by simpa [List.length_zipIdx] using hi
        have hidx : pi.2 = 1 + i := by
          rw [← hi_val, List.getElem_zipIdx hi]
        have hlt : 1 + i < stride := by omega
        have hmod : (1 + i) % stride = 1 + i := Nat.mod_eq_of_lt hlt
        rw [hidx, hmod] at hp
        simp at hp
      rw [hnil]
      simp

private theorem zipIdx_filter_nth (stride : Nat) (hstride : 0 < stride) (ps : List Nat)
    (j : Nat) (hj : j * stride < ps.length) :
    (((ps.zipIdx 0).filter (fun pi => pi.2 % stride == 0)).map Prod.fst)[j]? = ps[j * stride]? := by
  induction j generalizing ps with
  | zero =>
      have hne : ps ≠ [] := by
        intro hnil
        subst ps
        simp at hj
      cases ps with
      | nil => contradiction
      | cons p ps =>
          rw [List.zipIdx_cons]
          simp only [List.filter_cons, List.map_cons, List.getElem?_cons_zero]
          have hz : (0 : Nat) % stride == 0 := by simp
          rw [if_pos hz]
          simp
  | succ j ih =>
      have hle_stride : stride ≤ ps.length := by
        have hle1 : (j + 1) * stride ≤ ps.length := Nat.le_of_lt hj
        have hsle : stride ≤ (j + 1) * stride := by
          calc
            stride = 1 * stride := by rw [Nat.one_mul]
            _ ≤ (j + 1) * stride := Nat.mul_le_mul_right stride (by omega)
        omega
      let pre := ps.take stride
      let rest := ps.drop stride
      have ps_eq : ps = pre ++ rest := by
        simpa [pre, rest] using (List.take_append_drop stride ps).symm
      have hprelen : pre.length = stride := by
        dsimp [pre]
        rw [List.length_take, Nat.min_eq_left hle_stride]
      have hrestlen : rest.length = ps.length - stride := by simp [rest]
      have hjrest : j * stride < rest.length := by
        have hmul : (j + 1) * stride = stride + j * stride := by
          rw [Nat.add_mul, Nat.one_mul]
          ac_rfl
        have hsmall : j * stride + stride = (j + 1) * stride := by
          rw [hmul]
          omega
        have hsmall_lt : j * stride + stride < ps.length := by
          rw [hsmall]
          exact hj
        omega
      rw [ps_eq]
      rw [List.zipIdx_append]
      rw [List.filter_append, List.map_append]
      have hpre := filter_zipIdx_small stride hstride pre hprelen
      rw [hpre]
      have hidx : 0 + pre.length = stride := by omega
      rw [hidx]
      rw [filter_zipIdx_mod_eq stride rest]
      rw [List.getElem?_append]
      simp
      rw [← List.getElem?_map]
      have hrec := ih rest hjrest
      rw [hrec]
      dsimp [rest]
      rw [List.getElem?_drop]
      have hidx : stride + j * stride = (j + 1) * stride := by
        rw [Nat.add_mul, Nat.one_mul]
        ac_rfl
      rw [hidx]
      rw [← ps_eq]

private theorem filterMap_eq_filter_map_offset (stride off : Nat) (ps : List Nat) :
    (ps.zipIdx off).filterMap (fun pi => if pi.2 % stride == 0 then some pi.1 else none) =
      ((ps.zipIdx off).filter (fun pi => pi.2 % stride == 0)).map Prod.fst := by
  induction ps generalizing off with
  | nil => simp
  | cons p ps ih =>
      have ih' := ih (off + 1)
      rw [List.zipIdx_cons]
      simp only [List.filterMap_cons, List.filter_cons, List.map_cons]
      by_cases h : off % stride == 0
      · rw [if_pos h, if_pos h]
        exact congrArg (List.cons p) ih'
      · rw [if_neg h, if_neg h]
        exact ih'

private theorem anchors_eq_filter (stride : Nat) (ps : List Nat) :
    anchors stride ps = ((ps.zipIdx 0).filter (fun pi => pi.2 % stride == 0)).map Prod.fst := by
  unfold anchors
  simpa using (filterMap_eq_filter_map_offset stride 0 ps)

private theorem anchors_nth (stride : Nat) (hstride : 0 < stride) (ps : List Nat)
    (j : Nat) (hj : j * stride < ps.length) :
    (anchors stride ps)[j]? = ps[j * stride]? := by
  rw [anchors_eq_filter]
  exact zipIdx_filter_nth stride hstride ps j hj

private theorem filter_ge_drop (ps : List Nat) (hinc : ps.Pairwise (· < ·))
    (j : Nat) (hj : j < ps.length) :
    ps.filter (fun p => decide (ps[j] ≤ p)) = ps.drop j := by
  induction j generalizing ps with
  | zero =>
      have hle : ∀ p ∈ ps, ps[0] ≤ p := by
        intro p hp
        rcases List.mem_iff_getElem.mp hp with ⟨i, hi, rfl⟩
        by_cases h0 : i = 0
        · subst i
          simp
        · have hlt := (List.pairwise_iff_getElem.mp hinc) 0 i (by simpa using (Nat.zero_lt_of_lt hi)) hi (Nat.pos_of_ne_zero h0)
          omega
      rw [List.filter_eq_self.mpr]
      · simp
      · intro p hp
        exact decide_eq_true (hle p hp)
  | succ j ih =>
      cases ps with
      | nil => simp at hj
      | cons p ps =>
          have hj' : j < ps.length := by simpa using hj
          have hinc_tail : ps.Pairwise (· < ·) := (List.pairwise_cons.mp hinc).2
          have htailval : (p :: ps)[j + 1] = ps[j] := by
            rw [List.getElem_cons_succ]
          have hlt_head : p < ps[j] := by
            have htmp : p < (p :: ps)[j + 1] := by
              rw [List.getElem_cons_succ]
              exact (List.pairwise_iff_getElem.mp hinc) 0 (j + 1) (by simp) (by simpa using hj) (by omega)
            simpa [htailval] using htmp
          have hfail : decide ((p :: ps)[j + 1] ≤ p) = false := by
            rw [htailval]
            simp [show ¬ ps[j] ≤ p by omega]
          rw [List.filter_cons]
          rw [hfail]
          rw [List.drop_succ_cons]
          have hpred : (fun x => decide ((p :: ps)[j + 1] ≤ x)) = (fun x => decide (ps[j] ≤ x)) := by
            funext x
            rw [htailval]
          rw [hpred]
          exact ih ps hinc_tail hj'

/-! ## byteCount helpers -/

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem byteCount_add_le_add_one (a b : Nat) :
    byteCount a + byteCount b ≤ byteCount (a + b) + 1 := by
  unfold byteCount
  have ha := Nat.mod_add_div (a + 7) 8
  have hb := Nat.mod_add_div (b + 7) 8
  have hab := Nat.mod_add_div (a + b + 7) 8
  have hama := Nat.mod_lt (a + 7) (by decide : 0 < 8)
  have hbmb := Nat.mod_lt (b + 7) (by decide : 0 < 8)
  have habmb := Nat.mod_lt (a + b + 7) (by decide : 0 < 8)
  omega

private theorem directoryBytes_eq (w : Nat) (xs : List Nat) :
    directoryBytes w xs = byteCount (xs.length * w) + byteCount (xs.length + peak xs / 2^w) := by
  simp [directoryBytes, highBits, high, pack, little]
  omega

private theorem div_add_le_div (x y d : Nat) (hd : 0 < d) : x / d + y / d ≤ (x + y) / d := by
  rw [Nat.add_div hd]
  omega

private theorem peak_append_ge (l₁ l₂ : List Nat) : peak l₂ ≤ peak (l₁ ++ l₂) := by
  by_cases h₂ : l₂ = []
  · subst l₂
    simp [peak]
  · have hm := peak_mem h₂
    exact le_peak (by simp [hm])

private theorem endAt_le_sum (xs : List Nat) (i : Nat) (hi : i < xs.length) :
    endAt xs i ≤ xs.sum := by
  unfold endAt
  rw [ends_get?]
  simp only [if_pos hi, Option.getD_some]
  simpa using (sum_take_mono xs (i := i + 1) (j := xs.length) (by omega))

private theorem peak_ends_le_sum (xs : List Nat) : peak (ends xs) ≤ xs.sum := by
  cases xs with
  | nil => simp [ends, cums, peak]
  | cons x xs =>
      have hall : ∀ p ∈ ends (x :: xs), p ≤ (x :: xs).sum := by
        intro p hp
        rcases List.mem_iff_getElem.mp hp with ⟨i, hi, rfl⟩
        have hval : (ends (x :: xs))[i] = endAt (x :: xs) i := by
          unfold endAt
          rw [List.getElem?_eq_getElem (by simpa [ends_length] using hi)]
          rfl
        rw [hval]
        exact endAt_le_sum (x :: xs) i (by simpa [ends_length] using hi)
      have hne : ends (x :: xs) ≠ [] := by
        intro hnil
        have hlen0 : (ends (x :: xs)).length = 0 := List.length_eq_zero_iff.mpr hnil
        simpa [ends, cums_length] using hlen0
      obtain ⟨m, hm⟩ := max?_some_of_ne_nil hne
      have hmprops := List.max?_eq_some_iff.mp hm
      have hp : peak (ends (x :: xs)) = m := by simp [peak, hm]
      rw [hp]
      exact hall m hmprops.1

private theorem peak_ends_append (a b : List Nat) :
    a.sum + peak (ends b) ≤ peak (ends (a ++ b)) := by
  by_cases hb : b = []
  · subst b
    simp only [List.append_nil]
    cases a with
    | nil => simp [ends, cums, peak]
    | cons x xs =>
        have hi_len : (x :: xs).length - 1 < (x :: xs).length := by simp
        have hget := ends_get? (x :: xs) ((x :: xs).length - 1)
        have htake : (x :: xs).take ((x :: xs).length - 1 + 1) = x :: xs := by
          have heq : (x :: xs).length - 1 + 1 = (x :: xs).length := by omega
          rw [heq, List.take_length]
        rw [if_pos hi_len, htake] at hget
        simp at hget
        have hi_ends : (x :: xs).length - 1 < (ends (x :: xs)).length := by
          simpa [ends_length] using hi_len
        have hget' : (ends (x :: xs))[((x :: xs).length - 1)]? = some (x + xs.sum) := by
          simpa using hget
        rw [List.getElem?_eq_getElem hi_ends] at hget'
        have hval : (ends (x :: xs))[((x :: xs).length - 1)] = (x :: xs).sum := Option.some.inj hget'
        have hmem : (x :: xs).sum ∈ ends (x :: xs) := by
          rw [← hval]
          exact List.getElem_mem (by simpa [ends_length] using hi_len)
        exact le_peak hmem
  · have hm : peak (ends b) ∈ ends b := peak_mem (by
        intro hnil
        apply hb
        apply List.eq_nil_of_length_eq_zero
        have hlen0 : (ends b).length = 0 := List.length_eq_zero_iff.mpr hnil
        simpa [ends, cums_length] using hlen0)
    have hmem : peak (ends b) + a.sum ∈ (ends b).map (fun v => v + a.sum) := by
      exact List.mem_map.mpr ⟨_, hm, rfl⟩
    have hmem2 : peak (ends b) + a.sum ∈ ends (a ++ b) := by
      rw [ends_append]
      exact List.mem_append.mpr (Or.inr hmem)
    have hle := le_peak hmem2
    omega

private theorem div_sub_div_le (x y d : Nat) (hd : 0 < d) (hyx : y ≤ x) :
    x / d - y / d ≤ (x - y + d - 1) / d := by
  rw [Nat.le_div_iff_mul_le hd]
  let q := x / d - y / d
  change q * d ≤ x - y + d - 1
  have hqyqx : y / d ≤ x / d := Nat.div_le_div_right hyx
  have hqx : x / d = q + y / d := by dsimp [q]; omega
  have hbase : x = (x / d) * d + x % d := by
    have h := Nat.div_add_mod x d
    rw [Nat.mul_comm d (x / d)] at h
    exact h.symm
  have hxdec : x = q * d + (y / d) * d + x % d := by
    calc
      x = (x / d) * d + x % d := hbase
      _ = (q + y / d) * d + x % d := by rw [hqx]
      _ = q * d + (y / d) * d + x % d := by rw [Nat.add_mul]
  have hx_ge : q * d + (y / d) * d ≤ x := by
    rw [hxdec]
    omega
  have hydec : y = (y / d) * d + y % d := by
    have h := Nat.div_add_mod y d
    rw [Nat.mul_comm d (y / d)] at h
    exact h.symm
  have hry : y % d ≤ d - 1 := by
    have := Nat.mod_lt y hd
    omega
  have hy_le : y ≤ (y / d) * d + (d - 1) := by
    calc
      y = (y / d) * d + y % d := hydec
      _ ≤ (y / d) * d + (d - 1) := Nat.add_le_add_left hry _
  have hxy : y + (x - y) = x := Nat.add_sub_of_le hyx
  omega

theorem sound_AlignedShift :
    TinylambOpt.ResearchGoals.Compression.Directories.AlignedShift.Goal := by
  intro w a b hdiv
  obtain ⟨q, hq⟩ := hdiv
  have hsumdiv : a.sum / 2^w = q := by
    rw [hq, Nat.mul_div_right _ (Nat.two_pow_pos w)]
  apply List.ext_getElem?
  intro i
  by_cases hib : i < b.length
  · rw [List.getElem?_drop, List.getElem?_map]
    have hi_ab : a.length + i < (a ++ b).length := by simp; omega
    have hi_ab_ends : a.length + i < (ends (a ++ b)).length := by simpa [ends_length] using hi_ab
    have hi_b_ends : i < (ends b).length := by simpa [ends_length] using hib
    rw [positions_get? w (ends (a ++ b)) (a.length + i), positions_get? w (ends b) i]
    simp [hi_ab_ends, hi_b_ends]
    have hidx : (ends (a ++ b))[a.length + i]'(by simpa [ends_length] using hi_ab) =
        (ends b)[i]'(by simpa [ends_length] using hib) + a.sum := by
      have hAB := ends_get? (a ++ b) (a.length + i)
      rw [if_pos hi_ab] at hAB
      rw [List.getElem?_eq_getElem hi_ab_ends] at hAB
      have hB := ends_get? b i
      rw [if_pos hib] at hB
      rw [List.getElem?_eq_getElem hi_b_ends] at hB
      rw [Option.some.inj hAB, Option.some.inj hB]
      rw [show (a ++ b).take (a.length + i + 1) = a ++ b.take (i + 1) by
        rw [List.take_append]
        have hle : a.length ≤ a.length + i + 1 := by omega
        rw [List.take_of_length_le hle]
        have hsub : a.length + i + 1 - a.length = i + 1 := by omega
        rw [hsub]]
      simp only [List.sum_append]
      omega
    rw [hidx, hq]
    have hdivsum : ((ends b)[i]'(by simpa [ends_length] using hib) + 2^w * q) / 2^w =
        (ends b)[i]'(by simpa [ends_length] using hib) / 2^w + q := by
      rw [Nat.add_mul_div_left _ q (Nat.two_pow_pos w)]
    have hmuldiv : (2^w * q) / 2^w = q := Nat.mul_div_right q (Nat.two_pow_pos w)
    rw [hdivsum]
    rw [hmuldiv]
    omega
  · have hlenb : b.length ≤ i := by omega
    have hlen_ab_pos : (positions w (ends (a ++ b))).length ≤ a.length + i := by
      simp [positions, ends_length]
      omega
    have hlen_b_pos : (positions w (ends b)).length ≤ i := by simpa [positions, ends_length] using hlenb
    rw [List.getElem?_map]
    rw [List.getElem?_drop]
    rw [positions_get? w (ends (a ++ b)) (a.length + i), positions_get? w (ends b) i]
    have hlen_ab' : (a ++ b).length ≤ a.length + i := by
      simpa [positions, ends_length] using hlen_ab_pos
    have hlen_b' : b.length ≤ i := by
      simpa [positions, ends_length] using hlen_b_pos
    have hnotab : ¬ a.length + i < (ends (a ++ b)).length := by
      simp only [ends_length]
      omega
    have hnotb : ¬ i < (ends b).length := by
      simp only [ends_length]
      omega
    simp [hnotab, hnotb]

theorem sound_AnchorScanBound :
    TinylambOpt.ResearchGoals.Compression.Directories.AnchorScanBound.Goal := by
  intro w sizes a i cap hai hi hcap
  let ps := positions w (ends sizes)
  have hi_ends : i < (ends sizes).length := by simpa [ends_length] using hi
  have ha_ends : a < (ends sizes).length := by omega
  have hps_i : posAt w (ends sizes) i = endAt sizes i / 2^w + i := by
    simpa [endAt] using posAt_eq w (ends sizes) i hi_ends
  have hps_a : posAt w (ends sizes) a = endAt sizes a / 2^w + a := by
    simpa [endAt] using posAt_eq w (ends sizes) a ha_ends
  have hmono := endAt_monotone sizes hai hi
  have hsub := endAt_sub_bound sizes a i cap hai hi hcap
  have hdpos : 0 < 2^w := Nat.two_pow_pos w
  have hdivdiff := div_sub_div_le (endAt sizes i) (endAt sizes a) (2^w) hdpos hmono
  have hdivmono : ((endAt sizes i - endAt sizes a + 2^w - 1) / 2^w) ≤
      ((i - a) * cap + 2^w - 1) / 2^w := by
    exact Nat.div_le_div_right (by omega)
  change posAt w (ends sizes) i - posAt w (ends sizes) a ≤
    (i - a) + ((i - a) * cap + 2^w - 1) / 2^w
  rw [hps_i, hps_a]
  have hd1 : endAt sizes a / 2^w ≤ endAt sizes i / 2^w := Nat.div_le_div_right hmono
  omega

theorem sound_ResetHighBase :
    TinylambOpt.ResearchGoals.Compression.Directories.ResetHighBase.Goal := by
  intro w a b
  rw [directoryBytes_eq, directoryBytes_eq, directoryBytes_eq]
  simp only [ends_length]
  let d := 2^w
  let hiA := a.length + peak (ends a) / d
  let hiB := b.length + peak (ends b) / d
  let hiAB := (a ++ b).length + peak (ends (a ++ b)) / d
  have hdpos : 0 < d := by dsimp [d]; exact Nat.two_pow_pos w
  have hpeakDiv : peak (ends a) / d + peak (ends b) / d ≤ peak (ends (a ++ b)) / d := by
    calc
      peak (ends a) / d + peak (ends b) / d ≤ (peak (ends a) + peak (ends b)) / d :=
        div_add_le_div _ _ _ hdpos
      _ ≤ (a.sum + peak (ends b)) / d := by
        exact Nat.div_le_div_right (Nat.add_le_add_right (peak_ends_le_sum a) _)
      _ ≤ peak (ends (a ++ b)) / d := by
        exact Nat.div_le_div_right (peak_ends_append a b)
  have hhi : hiA + hiB ≤ hiAB := by
    dsimp [hiA, hiB, hiAB]
    rw [List.length_append]
    omega
  have hlow : byteCount (a.length * w) + byteCount (b.length * w) ≤
      byteCount ((a ++ b).length * w) + 1 := by
    rw [List.length_append, Nat.add_mul]
    exact byteCount_add_le_add_one _ _
  have hhigh : byteCount hiA + byteCount hiB ≤ byteCount hiAB + 1 :=
    Nat.le_trans (byteCount_add_le_add_one hiA hiB)
      (Nat.add_le_add_right (byteCount_mono hhi) 1)
  dsimp [hiA, hiB, hiAB] at hhigh
  dsimp [d] at hhigh
  omega

theorem sound_RestartSelect :
    TinylambOpt.ResearchGoals.Compression.Directories.RestartSelect.Goal := by
  intro w sizes stride i hstride hi
  let xs := ends sizes
  let ps := positions w xs
  let j0 := i / stride
  have hlenps : ps.length = sizes.length := by dsimp [ps, xs]; simp [positions, ends_length]
  have hj0mul : j0 * stride ≤ i := Nat.div_mul_le_self i stride
  have hj0lt : j0 * stride < ps.length := by omega
  have hanchor : ((anchors stride ps)[j0]?).getD 0 = ps[j0 * stride] := by
    have han := anchors_nth stride hstride ps j0 hj0lt
    rw [han, List.getElem?_eq_getElem hj0lt]
    simp
  have hbslen : (high w xs).length = byteCount (highBits w xs) := by
    simp [high, pack, little]
  have hbits : highBits w xs ≤ 8 * (high w xs).length := by
    rw [hbslen]
    exact bits_le_byteCount (highBits w xs)
  have hpslt : ps[j0 * stride] < highBits w xs := by
    apply positions_lt_highBits w sizes
    dsimp [ps]
    exact List.getElem_mem hj0lt
  have hstart : ps[j0 * stride] ≤ 8 * (high w xs).length := by omega
  change seekFrom (high w (ends sizes))
    ((anchors stride (positions w (ends sizes)))[i / stride]?.getD 0) (i % stride) =
      (positions w (ends sizes))[i]?
  rw [hanchor]
  rw [seekFrom_ones (high w xs) (ps[j0 * stride]) (i % stride) hstart]
  rw [ones_high]
  have hpinc : ps.Pairwise (· < ·) := by
    dsimp [ps]
    exact positions_pairwise w sizes
  have hj0len : j0 < ps.length := by
    by_cases h0 : j0 = 0
    · have hzero : j0 * stride = 0 := by simp [h0]
      omega
    · have hsge : 1 ≤ stride := by omega
      have hsle : j0 ≤ j0 * stride := by
        calc
          j0 = j0 * 1 := by rw [Nat.mul_one]
          _ ≤ j0 * stride := Nat.mul_le_mul_left j0 hsge
      omega
  have hfilt := filter_ge_drop ps hpinc (j0 * stride) hj0lt
  have hfilt' : (positions w (ends sizes)).filter
      (fun p => decide (ps[j0 * stride] ≤ p)) =
      (positions w (ends sizes)).drop (j0 * stride) := by simpa [ps] using hfilt
  rw [hfilt']
  rw [List.getElem?_drop]
  have hsum : j0 * stride + i % stride = i := by
    dsimp [j0]
    have h := Nat.div_add_mod i stride
    rw [Nat.mul_comm stride (i / stride)] at h
    exact h
  rw [hsum]

end TinylambOpt.ResearchProofs.Compression.Directories.AlignedShiftAndRestartSelect
