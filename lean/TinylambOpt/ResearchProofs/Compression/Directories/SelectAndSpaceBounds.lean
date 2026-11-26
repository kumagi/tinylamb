import TinylambOpt.ResearchGoals.Compression.Directories
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `EndSelect` — `∀ w sizes i, let xs := ends sizes lookup w i (high w xs) (pack w xs) =
  xs[i]?`
- `HighPlaneBudget` — `∀ sizes, let xs := ends sizes highBits (suggested xs) xs ≤
  2*sizes.length`
- `PhysicalCharge` — `∀ w sizes, let xs := ends sizes directoryBytes w xs = byteCount
  (sizes.length*w) + byteCount (sizes.length+sizes.sum/2^w)`
- `SliceBySelect` — `∀ w (rows : List Bytes) i, i < rows.length → let xs := ends
  (rows.map List.length) let atEnd := fun j => (lookup w j (high w xs) (pack w
  xs)).getD 0 let start := if i == 0 then 0 else atEnd (i-1) (rows.flatten.drop
  start).take (atEnd i-start) = rows[i]?.getD []`
- `UnarySelect` — `∀ w sizes, ones (high w (ends sizes)) = positions w (ends sizes)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Directories.SelectAndSpaceBounds

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Directories
open TinylambOpt.ResearchProofs.Compression.Wire

/-! ## Small arithmetic / byte-count helpers -/

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem ends_length (sizes : List Nat) : (ends sizes).length = sizes.length := by
  simp [ends]

/-! ## base-256 decoding (copied from the Wire proof module) -/

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

/-- Like `word_digit`, but without the `x < 2^w` premise: the low word of
`word w xs` at lane `i` is always `xs[i] % 2^w`. -/
private theorem word_shift_unbounded (w : Nat) (xs : List Nat) :
    word w xs / 2 ^ w = word w (xs.drop 1) := by
  cases xs with
  | nil => simp [word]
  | cons x xs =>
      rw [word]
      have hxmod : x % 2 ^ w < 2 ^ w := Nat.mod_lt _ (Nat.two_pow_pos w)
      rw [Nat.add_comm]
      rw [Nat.mul_add_div (Nat.two_pow_pos w) (word w xs) (x % 2 ^ w)]
      simp [Nat.div_eq_of_lt hxmod]

private theorem word_digit_unbounded (w i : Nat) (xs : List Nat) (hi : i < xs.length) :
    (word w xs / 2 ^ (i * w)) % 2 ^ w = xs[i] % 2 ^ w := by
  induction i generalizing xs with
  | zero =>
      simp only [Nat.zero_mul, Nat.pow_zero, Nat.div_one]
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          rw [List.getElem_cons_zero]
          rw [word]
          simpa [Nat.mod_eq_of_lt (Nat.mod_lt x (Nat.two_pow_pos w))] using
            (Nat.add_mul_mod_self_left (x % 2 ^ w) (2 ^ w) (word w xs))
  | succ i ih =>
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have hp_exp : (i + 1) * w = w + i * w := by
            rw [Nat.add_mul, Nat.one_mul]
            omega
          have hp : 2 ^ ((i + 1) * w) = 2 ^ w * 2 ^ (i * w) := by
            rw [hp_exp, Nat.pow_add]
          rw [hp, ← Nat.div_div_eq_div_mul, word_shift_unbounded]
          exact ih xs (Nat.lt_of_succ_lt_succ (by simpa using hi))

/-- The little-endian byte stream is canonical, so the local-window read
agrees with the direct bit read. -/
private theorem takeAt_pack (w : Nat) (xs : List Nat) (i : Nat) :
    takeAt (pack w xs) w i = get (pack w xs) w i := by
  apply sound_LocalRead
  intro b hb
  simp only [pack, little, List.mem_map, List.mem_range] at hb
  obtain ⟨j, hj, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide : 0 < 256)

/-! ## Unary bit plane -/

private theorem get_pack1_eq (bits : List Nat) (hbits : ∀ b ∈ bits, b < 2) (j : Nat)
    (hj : j < bits.length) : get (pack 1 bits) 1 j = bits[j] := by
  have hpi := sound_PackedInverse 1 bits hbits
  have hget : ((List.range bits.length).map (get (pack 1 bits) 1))[j]? = some (bits[j]) := by
    rw [hpi]
    simp [hj]
  have hget' : ((List.range bits.length).map (get (pack 1 bits) 1))[j]? =
      some (get (pack 1 bits) 1 j) := by simp [hj]
  rw [hget'] at hget
  exact Option.some.inj hget

private theorem get_pack1_ge (bits : List Nat) (j : Nat) (hj : bits.length ≤ j) :
    get (pack 1 bits) 1 j = 0 := by
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get
  rw [fromLittle_pack]
  have hlt := word_lt_pow 1 bits
  simp only [Nat.mul_one] at hlt
  simp only [Nat.mul_one]
  have hp : 2 ^ bits.length ≤ 2 ^ j := Nat.pow_le_pow_right (by decide : 0 < 2) hj
  have hdiv : word 1 bits / 2 ^ j = 0 := Nat.div_eq_of_lt (Nat.lt_of_lt_of_le hlt hp)
  rw [hdiv]

private theorem ones_pack1_range (bits : List Nat) (hbits : ∀ b ∈ bits, b < 2) :
    ones (pack 1 bits) = (List.range bits.length).filter (fun j => bits[j]? == some 1) := by
  unfold ones
  have hlenpack : (pack 1 bits).length = byteCount bits.length := by
    simpa using (sound_PackedCharge 1 bits)
  rw [hlenpack]
  have hbig : bits.length ≤ 8 * byteCount bits.length := bits_le_byteCount bits.length
  obtain ⟨d, hd⟩ := Nat.exists_eq_add_of_le hbig
  rw [hd, List.range_add, List.filter_append]
  have htailnil :
      (List.filter (fun j => get (pack 1 bits) 1 j == 1)
        ((List.range d).map (fun x => bits.length + x))) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro j hj
    simp only [List.mem_map, List.mem_range] at hj
    obtain ⟨x, hx, rfl⟩ := hj
    have hjge : bits.length ≤ bits.length + x := by omega
    rw [get_pack1_ge bits (bits.length + x) hjge]
    simp
  rw [htailnil, List.append_nil]
  apply List.filter_congr
  intro j hj
  have hjlen : j < bits.length := List.mem_range.mp hj
  rw [get_pack1_eq bits hbits j hjlen]
  simp [hjlen]

/-- Selecting the set bits of a unary plane built from a sorted, bounded set
of positions returns exactly those positions. -/
private theorem filter_range_contains_off (off L : Nat) (ps : List Nat)
    (hpair : ps.Pairwise (· < ·))
    (hge : ∀ q ∈ ps, off ≤ q)
    (hltL : ∀ q ∈ ps, q < L) :
    ((List.range (L - off)).map (fun x => off + x)).filter (fun j => ps.contains j) = ps := by
  induction ps generalizing off with
  | nil => simp
  | cons p rest ih =>
      have hpge : off ≤ p := hge p (by simp)
      have hpltL : p < L := hltL p (by simp)
      have hpgt : ∀ q ∈ rest, p < q := (List.pairwise_cons.mp hpair).1
      have hrestpair : rest.Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
      have hLoff : L - off = (p - off) + (L - p) := by omega
      rw [hLoff, List.range_add, List.map_append, List.filter_append]
      have hfirst :
          ((List.range (p - off)).map (fun x => off + x)).filter
            (fun j => (p :: rest).contains j) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro j hj
        simp only [List.mem_map, List.mem_range] at hj
        obtain ⟨x, hx, rfl⟩ := hj
        have hnep : off + x ≠ p := by omega
        have hnmem : off + x ∉ rest := by
          intro hm
          have := hpgt (off + x) hm
          omega
        simp [hnep, hnmem]
      rw [hfirst, List.nil_append]
      have hmap :
          (List.map (fun x => off + x) (List.map (fun x => p - off + x) (List.range (L - p)))) =
            (List.range (L - p)).map (fun x => p + x) := by
        rw [List.map_map]
        apply List.map_congr_left
        intro x hx
        change off + ((p - off) + x) = p + x
        omega
      rw [hmap]
      have hM : L - p = (L - (p + 1)) + 1 := by omega
      rw [hM, List.range_succ_eq_map, List.map_cons]
      simp only [Nat.add_zero, List.filter_cons]
      simp
      have hcomp :
          List.map ((fun x => p + x) ∘ Nat.succ) (List.range (L - (p + 1))) =
            (List.range (L - (p + 1))).map (fun x => (p + 1) + x) := by
        apply List.map_congr_left
        intro x hx
        change p + Nat.succ x = (p + 1) + x
        omega
      rw [hcomp]
      rw [← ih (off := p + 1) hrestpair
        (by intro q hq; have := hpgt q hq; omega)
        (by intro q hq; exact hltL q (by simp [hq]))]
      apply List.filter_congr
      intro j hj
      simp only [List.mem_map, List.mem_range] at hj
      obtain ⟨x, hx, rfl⟩ := hj
      have hneq : ¬ p + 1 + x = p := by omega
      simp [hneq, hx]

private theorem filter_range_contains_eq (L : Nat) (ps : List Nat)
    (hpair : ps.Pairwise (· < ·)) (hbound : ∀ q ∈ ps, q < L) :
    (List.range L).filter (fun j => ps.contains j) = ps := by
  have h := filter_range_contains_off 0 L ps hpair (by intro q hq; omega) hbound
  simpa using h

/-! ## End offsets: monotonicity, peak and positions -/

private theorem endsFrom_ge (sizes : List Nat) (acc : Nat) :
    ∀ y ∈ sizes.scanl (· + ·) acc, acc ≤ y := by
  induction sizes generalizing acc with
  | nil => simp
  | cons x xs ih =>
      simp only [List.scanl_cons, List.mem_cons]
      intro y hy
      rcases hy with rfl | hy
      · omega
      · exact Nat.le_trans (by omega : acc ≤ acc + x) (ih (acc + x) y hy)

private theorem scanl_pairwise_le (sizes : List Nat) (acc : Nat) :
    (sizes.scanl (· + ·) acc).Pairwise (· ≤ ·) := by
  induction sizes generalizing acc with
  | nil => simp
  | cons x xs ih =>
      simp only [List.scanl_cons]
      rw [List.pairwise_cons]
      constructor
      · intro y hy
        exact Nat.le_trans (by omega : acc ≤ acc + x) (endsFrom_ge xs (acc + x) y hy)
      · exact ih (acc + x)

private theorem ends_pairwise_le (sizes : List Nat) : (ends sizes).Pairwise (· ≤ ·) := by
  exact (scanl_pairwise_le sizes 0).drop

private theorem positions_get? (w i : Nat) (xs : List Nat) :
    (positions w xs)[i]? = (xs[i]?).map (fun x => x / 2 ^ w + i) := by
  unfold positions
  simp [List.getElem?_zipIdx, Function.comp_def]

private theorem positions_pairwise_lt (w : Nat) {xs : List Nat}
    (hmono : xs.Pairwise (· ≤ ·)) : (positions w xs).Pairwise (· < ·) := by
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have hxi : i < xs.length := by simpa [positions] using hi
  have hxj : j < xs.length := by simpa [positions] using hj
  have hle : xs[i] ≤ xs[j] := (List.pairwise_iff_getElem.mp hmono) i j hxi hxj hij
  have hdivle : xs[i] / 2 ^ w ≤ xs[j] / 2 ^ w := Nat.div_le_div_right hle
  have hpi : (positions w xs)[i] = xs[i] / 2 ^ w + i := by
    have h := positions_get? w i xs
    rw [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hxi] at h
    simpa using h
  have hpj : (positions w xs)[j] = xs[j] / 2 ^ w + j := by
    have h := positions_get? w j xs
    rw [List.getElem?_eq_getElem hj, List.getElem?_eq_getElem hxj] at h
    simpa using h
  rw [hpi, hpj]
  omega

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

private theorem positions_lt_highBits (w : Nat) (sizes : List Nat) :
    ∀ q ∈ positions w (ends sizes), q < highBits w (ends sizes) := by
  intro q hq
  unfold positions at hq
  rw [List.mem_map] at hq
  rcases hq with ⟨⟨x, i⟩, hp, rfl⟩
  simp only [Prod.fst, Prod.snd]
  have hpi := List.mem_zipIdx hp
  have hi : i < (ends sizes).length := by simpa using hpi.2.1
  have hx : x = (ends sizes)[i] := by simpa using hpi.2.2
  rw [hx]
  have hpeak := le_peak (List.getElem_mem hi)
  have hdivle : (ends sizes)[i] / 2 ^ w ≤ peak (ends sizes) / 2 ^ w :=
    Nat.div_le_div_right hpeak
  unfold highBits
  have hlen := ends_length sizes
  omega

private theorem ones_high_eq_positions (w : Nat) (sizes : List Nat) :
    ones (high w (ends sizes)) = positions w (ends sizes) := by
  let xs := ends sizes
  let ps := positions w xs
  let L := highBits w xs
  let bits := (List.range L).map (fun j => if ps.contains j then 1 else 0)
  have hbits : ∀ b ∈ bits, b < 2 := by
    intro b hb
    simp only [bits, List.mem_map, List.mem_range] at hb
    obtain ⟨j, hj, rfl⟩ := hb
    split <;> omega
  have hhi : high w xs = pack 1 bits := by rfl
  rw [hhi]
  rw [ones_pack1_range bits hbits]
  have hblen : bits.length = L := by simp [bits]
  rw [hblen]
  have hfilter : (List.range L).filter (fun j => bits[j]? == some 1) =
      (List.range L).filter (fun j => ps.contains j) := by
    apply List.filter_congr
    intro j hj
    have hjL : j < L := List.mem_range.mp hj
    have hb : bits[j]? = some (if ps.contains j then 1 else 0) := by simp [bits, hjL]
    rw [hb]
    by_cases hp : ps.contains j
    · have hp' : j ∈ ps := by simpa using hp
      simp [hp']
    · have hp' : j ∉ ps := by simpa using hp
      simp [hp']
  rw [hfilter]
  simpa [ps, L, xs] using
    filter_range_contains_eq L ps (positions_pairwise_lt w (ends_pairwise_le sizes))
      (positions_lt_highBits w sizes)

/-- ## End offsets as prefix sums -/

private theorem ends_cons (x : Nat) (xs : List Nat) :
    ends (x :: xs) = x :: (ends xs).map (fun y => x + y) := by
  have hscan : ∀ (sizes : List Nat) (acc : Nat),
      sizes.scanl (· + ·) acc = (sizes.scanl (· + ·) 0).map (fun y => acc + y) := by
    intro sizes
    induction sizes with
    | nil => intro acc; simp
    | cons a as ih =>
        intro acc
        simp only [List.scanl_cons, List.map_cons]
        rw [ih (acc + a)]
        congr 1
        rw [ih (0 + a), List.map_map]
        apply List.map_congr_left
        intro y hy
        change acc + a + y = acc + ((0 + a) + y)
        omega
  unfold ends
  simp only [List.scanl_cons, List.drop_succ_cons, List.drop_zero]
  rw [hscan xs (0 + x)]
  cases xs with
  | nil => simp
  | cons a as => simp [List.scanl_cons]

private theorem ends_get_eq_sum_take (sizes : List Nat) (j : Nat) (hj : j < sizes.length) :
    (ends sizes)[j]'(by simpa [ends_length] using hj) = (sizes.take (j + 1)).sum := by
  induction sizes generalizing j with
  | nil => simp at hj
  | cons x xs ih =>
      cases j with
      | zero => simp [ends, List.scanl_cons]
      | succ j =>
          have hj' : j < xs.length := by simpa using hj
          have hi1 : j < (ends xs).length := by simpa [ends_length] using hj'
          have hi : (ends xs)[j]'(hi1) = (xs.take (j + 1)).sum := ih j hj'
          have hget : (ends (x :: xs))[j + 1]'(by simpa [ends_length] using hj) =
              x + (ends xs)[j]'(hi1) := by
            simp [ends_cons]
          rw [hget, hi]
          change x + (List.take (j + 1) xs).sum = (List.take (j + 2) (x :: xs)).sum
          have htake : List.take (j + 2) (x :: xs) = x :: List.take (j + 1) xs := by
            rw [List.take_cons (by omega : 0 < j + 2)]
            congr 1
          rw [htake]
          simp [List.sum_cons]

private theorem scanl_le_sum (sizes : List Nat) (acc : Nat) :
    ∀ y ∈ sizes.scanl (· + ·) acc, y ≤ acc + sizes.sum := by
  induction sizes generalizing acc with
  | nil => simp
  | cons x xs ih =>
      simp only [List.scanl_cons, List.mem_cons]
      intro y hy
      rcases hy with hEq | hmem
      · subst y
        have hmemacc : acc + x ∈ xs.scanl (· + ·) (acc + x) := by
          cases xs <;> simp [List.scanl_cons]
        have h := ih (acc + x) (acc + x) hmemacc
        simpa [Nat.add_assoc] using h
      · have h := ih (acc + x) y hmem
        simpa [Nat.add_assoc] using h

private theorem sum_mem_scanl (sizes : List Nat) (acc : Nat) :
    acc + sizes.sum ∈ sizes.scanl (· + ·) acc := by
  induction sizes generalizing acc with
  | nil => simp
  | cons x xs ih =>
      simp only [List.scanl_cons, List.mem_cons]
      right
      have h := ih (acc + x)
      simpa [Nat.add_assoc] using h

private theorem peak_ends (sizes : List Nat) : peak (ends sizes) = sizes.sum := by
  cases sizes with
  | nil => simp [ends, peak]
  | cons x xs =>
      have hle : ∀ y ∈ ends (x :: xs), y ≤ (x :: xs).sum := by
        intro y hy
        have hmem0 : y ∈ (x :: xs).scanl (· + ·) 0 := by
          simp [ends, List.scanl_cons] at hy ⊢
          exact Or.inr hy
        have h := scanl_le_sum (x :: xs) 0 y hmem0
        simpa using h
      have hmem : (x :: xs).sum ∈ ends (x :: xs) := by
        have h0 : (x :: xs).sum ∈ (x :: xs).scanl (· + ·) 0 := by
          simpa using sum_mem_scanl (x :: xs) 0
        simp only [List.scanl_cons, List.mem_cons] at h0
        rcases h0 with hzero | htail
        · have hmemx : x ∈ (x :: xs).scanl (· + ·) 0 := by
            simp only [List.scanl_cons, List.mem_cons]
            right
            cases xs <;> simp [List.scanl_cons]
          have hxle : x ≤ (x :: xs).sum := by
            have h := scanl_le_sum (x :: xs) 0 x hmemx
            simpa using h
          have hx0 : x = 0 := by omega
          have hxmem : x ∈ ends (x :: xs) := by
            cases xs with
            | nil => simp [ends]
            | cons y ys => simp [ends, List.scanl_cons]
          rw [hzero]
          simpa [hx0] using hxmem
        · simpa [ends] using htail
      have hmx : (ends (x :: xs)).max? = some (x :: xs).sum :=
        (List.max?_eq_some_iff).2 ⟨hmem, hle⟩
      simp [peak, hmx]

private theorem flatten_slice (rows : List Bytes) (i : Nat) (hi : i < rows.length) :
    (rows.flatten.drop ((rows.take i).map List.length).sum).take (rows[i].length) = rows[i] := by
  induction i generalizing rows with
  | zero =>
      cases rows with
      | nil => simp at hi
      | cons r rs => simp
  | succ i ih =>
      cases rows with
      | nil => simp at hi
      | cons r rs =>
          have hi' : i < rs.length := by simpa using hi
          have ih' := ih rs hi'
          have hdrop : (((r :: rs).take (i + 1)).map List.length).sum =
              r.length + ((rs.take i).map List.length).sum := by
            simp [List.take_succ_cons]
          simp only [List.flatten_cons]
          rw [hdrop, List.drop_append]
          have hle : r.length ≤ r.length + ((rs.take i).map List.length).sum := by omega
          rw [List.drop_eq_nil_of_le hle]
          simp only [List.nil_append, Nat.add_sub_cancel_left]
          simpa [List.getElem_cons_succ] using ih'

private theorem sum_take_succ (xs : List Nat) (j : Nat) (hj : j < xs.length) :
    (xs.take (j + 1)).sum = (xs.take j).sum + xs[j] := by
  have ht : xs.take (j + 1) = xs.take j ++ [xs[j]] := by
    rw [List.take_succ]
    rw [List.getElem?_eq_getElem hj]
    rfl
  rw [ht, List.sum_append]
  simp

/-! ## The five assigned goals -/

theorem sound_UnarySelect : UnarySelect.Goal := by
  intro w sizes
  exact ones_high_eq_positions w sizes

theorem sound_EndSelect : EndSelect.Goal := by
  intro w sizes i
  dsimp
  let xs := ends sizes
  change lookup w i (high w xs) (pack w xs) = xs[i]?
  have hone : ones (high w xs) = positions w xs := ones_high_eq_positions w sizes
  unfold lookup
  rw [hone]
  by_cases hi : i < xs.length
  · have hpos : (positions w xs)[i]? = some (xs[i] / 2 ^ w + i) := by
      rw [positions_get?]
      simp [hi]
    rw [hpos]
    simp only [Option.map_some]
    have hdiff : xs[i] / 2 ^ w + i - i = xs[i] / 2 ^ w := Nat.add_sub_cancel_right (xs[i] / 2 ^ w) i
    rw [hdiff, takeAt_pack]
    unfold TinylambOpt.ResearchGoals.Compression.Wire.get
    rw [fromLittle_pack]
    rw [word_digit_unbounded w i xs hi]
    rw [List.getElem?_eq_getElem hi]
    have hmad := Nat.div_add_mod (xs[i]) (2 ^ w)
    simp only [Option.some.injEq]
    simpa [Nat.mul_comm] using hmad
  · have hlen : xs.length ≤ i := by omega
    rw [List.getElem?_eq_none hlen]
    have hposlen : (positions w xs).length = xs.length := by simp [positions]
    rw [List.getElem?_eq_none (by omega : (positions w xs).length ≤ i)]
    simp [hlen]

theorem sound_SliceBySelect : SliceBySelect.Goal := by
  intro w rows i hi
  dsimp
  let sizes := rows.map List.length
  let xs := ends sizes
  change
    (rows.flatten.drop (if i == 0 then 0 else
      (lookup w (i - 1) (high w xs) (pack w xs)).getD 0)).take
      ((lookup w i (high w xs) (pack w xs)).getD 0 -
        (if i == 0 then 0 else (lookup w (i - 1) (high w xs) (pack w xs)).getD 0)) =
      rows[i]?.getD []
  have hlookup : ∀ j, lookup w j (high w xs) (pack w xs) = xs[j]? := by
    intro j
    have hl := sound_EndSelect w sizes j
    dsimp at hl
    simpa [xs] using hl
  have hA : ∀ j, j < rows.length → (xs[j]?).getD 0 = (sizes.take (j + 1)).sum := by
    intro j hj
    have hjx : j < xs.length := by simpa [xs, ends_length, sizes] using hj
    have h1 : (xs[j]?).getD 0 = xs[j] := by
      rw [List.getElem?_eq_getElem hjx]
      rfl
    rw [h1]
    have hs := ends_get_eq_sum_take sizes j (by simpa [sizes] using hj)
    simpa [xs] using hs
  cases i with
  | zero =>
      have hend : (lookup w 0 (high w xs) (pack w xs)).getD 0 = (sizes.take 1).sum := by
        rw [hlookup 0, hA 0 hi]
      rw [hend]
      simp
      have hs1 : (sizes.take 1).sum = rows[0].length := by
        cases rows with
        | nil => simp at hi
        | cons r rs => simp [sizes]
      rw [hs1]
      simpa [List.getElem?_eq_getElem hi] using (flatten_slice rows 0 hi)
  | succ i =>
      have hisucc : i + 1 < rows.length := by simpa using hi
      have hi' : i < rows.length := by omega
      have hstart : (lookup w i (high w xs) (pack w xs)).getD 0 = (sizes.take (i + 1)).sum := by
        rw [hlookup i]
        exact hA i hi'
      have hend : (lookup w (i + 1) (high w xs) (pack w xs)).getD 0 = (sizes.take (i + 2)).sum := by
        rw [hlookup (i + 1)]
        exact hA (i + 1) hisucc
      simp [hstart, hend]
      have hdiff : (sizes.take (i + 2)).sum - (sizes.take (i + 1)).sum = rows[i + 1].length := by
        have hslen : sizes.length = rows.length := by simp [sizes]
        have hsj : i + 1 < sizes.length := by simpa [hslen] using hisucc
        have hst := sum_take_succ sizes (i + 1) hsj
        rw [hst]
        have hsz : sizes[i + 1] = rows[i + 1].length := by simp [sizes]
        rw [hsz]
        omega
      rw [hdiff]
      have hdrop : (sizes.take (i + 1)).sum = ((rows.take (i + 1)).map List.length).sum := by
        simp [sizes]
      rw [hdrop]
      simpa [List.getElem?_eq_getElem hisucc] using (flatten_slice rows (i + 1) hisucc)

theorem sound_PhysicalCharge : PhysicalCharge.Goal := by
  intro w sizes
  dsimp
  unfold directoryBytes
  rw [sound_PackedCharge w (ends sizes)]
  have hhigh : (high w (ends sizes)).length =
      byteCount (sizes.length + sizes.sum / 2 ^ w) := by
    unfold high
    rw [sound_PackedCharge]
    simp [highBits, peak_ends, ends_length, Nat.mul_one]
  rw [hhigh]
  simp [ends, Nat.add_comm]

theorem sound_HighPlaneBudget : HighPlaneBudget.Goal := by
  intro sizes
  dsimp only
  rw [highBits]
  have hlen : (ends sizes).length = sizes.length := ends_length sizes
  have hpk : peak (ends sizes) = sizes.sum := peak_ends sizes
  rw [hlen, hpk]
  dsimp [suggested]
  rw [hpk, hlen]
  cases sizes with
  | nil => simp [width]
  | cons x xs =>
      let n := (x :: xs).length
      let m := (x :: xs).sum
      let a := m / n
      let w := width a
      have hn : 0 < n := by dsimp [n]; simp
      have hm_bound : m < n * 2 ^ w := by
        by_cases ha : a = 0
        · have hmltn : m < n := by
            have hz := (Nat.div_eq_zero_iff).mp ha
            omega
          have hw0 : w = 0 := by dsimp [w]; simp [ha, width]
          rw [hw0]
          simpa using hmltn
        · have hapos : 0 < a := Nat.pos_of_ne_zero ha
          have hw : w = a.log2 + 1 := by dsimp [w]; simp [width, ha]
          have hpow : a < 2 ^ w := by rw [hw]; exact Nat.lt_log2_self
          have ha1 : a + 1 ≤ 2 ^ w := by omega
          have hdivmod : m = a * n + m % n := by
            dsimp [a]
            rw [Nat.mul_comm]
            exact (Nat.div_add_mod m n).symm
          have hmod : m % n < n := Nat.mod_lt m hn
          have hle1 : a * n + n ≤ n * 2 ^ w := by
            have hh := Nat.mul_le_mul_right n ha1
            simpa [Nat.mul_add, Nat.mul_comm] using hh
          have hlt1 : m < a * n + n := by rw [hdivmod]; omega
          exact Nat.lt_of_lt_of_le hlt1 hle1
      have hdiv : m / 2 ^ w ≤ n := by
        have hlt : m / 2 ^ w < n := (Nat.div_lt_iff_lt_mul (Nat.two_pow_pos w)).2 hm_bound
        omega
      change n + m / 2 ^ w ≤ 2 * n
      omega

end TinylambOpt.ResearchProofs.Compression.Directories.SelectAndSpaceBounds
