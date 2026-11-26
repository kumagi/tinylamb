import TinylambOpt.ResearchGoals.Compression.TemporalText

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved

- `AlignedAppend` — `∀ a b first second, a.length%8 = 0 → stream a = some first →
  stream b = some second → stream (a++b) = some (first++second)`
- `PackedSeek` — `∀ ws bs i, stream ws = some bs → i < ws.length → some (extract bs i)
  = ws[i]?`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.TemporalText.PackedSeekAndAlignedAppend

open TinylambOpt.ResearchGoals.Compression.TemporalText
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Text (Words)

/-! Proofs for `PackedSeek.Goal` and `AlignedAppend.Goal`.

The bit-level helpers (`fromLittle_little`, `word_digit`, `takeAt = get`,
`word_append`, `little` concatenation) are adapted verbatim from the private
lemmas in `ResearchProofs.Compression.Wire`; the ASCII/decimal helpers
(`digits_spec`, `digits_inj`, `digits_paint`, `value_digits` and friends) are
adapted verbatim from `ResearchProofs.Compression.TemporalText`, because those
proof files are private and cannot be imported. -/

/-! ## bit-level helpers (from ResearchProofs.Compression.Wire) -/

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega
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

private theorem takeAt_eq_get : ∀ (bs : Bytes) (w i : Nat), (∀ b ∈ bs, b < 256) →
    takeAt bs w i = TinylambOpt.ResearchGoals.Compression.Wire.get bs w i := by
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

/-! ## point reads and byte bounds -/

private theorem little_lt (n k : Nat) {b : Nat} (hb : b ∈ little n k) : b < 256 := by
  unfold little at hb
  rcases List.mem_map.mp hb with ⟨i, -, rfl⟩
  exact Nat.mod_lt _ (by decide : 0 < 256)

private theorem pack_lt (w : Nat) (xs : List Nat) {b : Nat} (hb : b ∈ pack w xs) :
    b < 256 := by
  unfold pack at hb
  exact little_lt _ _ hb

private theorem get_pack (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w)
    {i : Nat} (hi : i < xs.length) :
    TinylambOpt.ResearchGoals.Compression.Wire.get (pack w xs) w i = xs[i] := by
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get
  rw [fromLittle_pack]
  exact word_digit w i xs h hi

/-! ## concatenating packed blocks at a byte boundary -/

private theorem little_succ (n k : Nat) :
    little n (k + 1) = n % 256 :: little (n / 256) k := by
  unfold little
  rw [List.range_succ_eq_map, List.map_cons, List.map_map]
  congr 1
  · simp
  · apply List.map_congr_left
    intro i hi
    have hdiv : n / 256 ^ i.succ = (n / 256) / 256 ^ i := by
      rw [Nat.pow_succ']
      exact (Nat.div_div_eq_div_mul n 256 (256 ^ i)).symm
    show (n / 256 ^ i.succ) % 256 = ((n / 256) / 256 ^ i) % 256
    rw [hdiv]

private theorem little_concat (ba bb A B : Nat)
    (hA : A < 256 ^ ba) (hB : B < 256 ^ bb) :
    little (A + 256 ^ ba * B) (ba + bb) = little A ba ++ little B bb := by
  induction ba generalizing bb A B with
  | zero =>
      have hA0 : A = 0 := by
        have hlt : A < 1 := by simpa using hA
        exact Nat.lt_one_iff.mp hlt
      subst A
      simp [little]
  | succ ba ih =>
      rw [show ba + 1 + bb = ba + bb + 1 from by omega]
      have hL : little (A + 256 ^ (ba + 1) * B) (ba + bb + 1) =
          (A + 256 ^ (ba + 1) * B) % 256 ::
            little ((A + 256 ^ (ba + 1) * B) / 256) (ba + bb) := little_succ _ _
      rw [hL]
      have hR : little A (ba + 1) = A % 256 :: little (A / 256) ba := little_succ _ _
      rw [hR, List.cons_append]
      have hNmod : (A + 256 ^ (ba + 1) * B) % 256 = A % 256 := by
        have hpow : 256 ^ (ba + 1) = 256 * 256 ^ ba := Nat.pow_succ'
        rw [hpow]
        have hassoc : (256 * 256 ^ ba) * B = 256 * (256 ^ ba * B) :=
          Nat.mul_assoc _ _ _
        rw [hassoc]
        exact Nat.add_mul_mod_self_left A 256 (256 ^ ba * B)
      rw [hNmod]
      congr 1
      have hNdiv : (A + 256 ^ (ba + 1) * B) / 256 = A / 256 + 256 ^ ba * B := by
        have hpow : 256 ^ (ba + 1) = 256 * 256 ^ ba := Nat.pow_succ'
        rw [hpow]
        have hassoc : (256 * 256 ^ ba) * B = 256 * (256 ^ ba * B) :=
          Nat.mul_assoc _ _ _
        rw [hassoc]
        exact Nat.add_mul_div_left A (256 ^ ba * B) (by decide : 0 < 256)
      rw [hNdiv]
      have hA' : A / 256 < 256 ^ ba := by
        have hpow : 256 ^ (ba + 1) = 256 ^ ba * 256 := Nat.pow_succ 256 ba
        rw [hpow] at hA
        exact (Nat.div_lt_iff_lt_mul (by decide : 0 < 256)).mpr hA
      exact ih _ _ _ hA' hB

private theorem pack_append_47 {xs ys : List Nat} (h : xs.length % 8 = 0) :
    pack 47 (xs ++ ys) = pack 47 xs ++ pack 47 ys := by
  have hmod : (xs.length * 47) % 8 = 0 := by omega
  have h8 : 8 * byteCount (xs.length * 47) = xs.length * 47 := by
    unfold byteCount
    omega
  have hbc : byteCount ((xs.length + ys.length) * 47) =
      byteCount (xs.length * 47) + byteCount (ys.length * 47) := by
    unfold byteCount
    rw [show (xs.length + ys.length) * 47 = xs.length * 47 + ys.length * 47 from by omega]
    omega
  have hb1 : word 47 xs < 256 ^ byteCount (xs.length * 47) := by
    have h1 := word_lt_pow 47 xs
    have hb := bits_le_byteCount (xs.length * 47)
    have h3 : 2 ^ (xs.length * 47) ≤ 256 ^ byteCount (xs.length * 47) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, ← Nat.pow_mul]
      exact Nat.pow_le_pow_right (by decide : 0 < 2) hb
    exact Nat.lt_of_lt_of_le h1 h3
  have hb2 : word 47 ys < 256 ^ byteCount (ys.length * 47) := by
    have h1 := word_lt_pow 47 ys
    have hb := bits_le_byteCount (ys.length * 47)
    have h3 : 2 ^ (ys.length * 47) ≤ 256 ^ byteCount (ys.length * 47) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, ← Nat.pow_mul]
      exact Nat.pow_le_pow_right (by decide : 0 < 2) hb
    exact Nat.lt_of_lt_of_le h1 h3
  have hpow : 2 ^ (47 * xs.length) = 256 ^ byteCount (xs.length * 47) := by
    have hmul : 47 * xs.length = xs.length * 47 := Nat.mul_comm _ _
    rw [hmul]
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, ← Nat.pow_mul, h8]
  unfold pack
  rw [List.length_append, word_append, hbc, hpow]
  exact little_concat _ _ _ _ hb1 hb2

/-! ## ASCII parser and decimal helpers (from ResearchProofs.Compression.TemporalText) -/

private theorem digits_spec (fmt : List (Option Nat)) (s : Bytes) (ds : List Nat)
    (h : digits fmt s = some ds) :
    s.length = fmt.length ∧ ds.length = (fmt.filter (fun x => x.isNone)).length ∧
      ∀ d ∈ ds, d < 10 := by
  induction fmt generalizing s ds with
  | nil =>
      cases s with
      | nil =>
          simp [digits] at h
          subst ds
          simp
      | cons b bs => simp [digits] at h
  | cons f fmt ih =>
      cases s with
      | nil => simp [digits] at h
      | cons b bs =>
          cases f with
          | none =>
              simp [digits] at h
              rcases h with ⟨hcond, ds', hd, rfl⟩
              have ih' := ih bs ds' hd
              simp only [List.length_cons, List.filter_cons, Option.isNone_none, ite_true,
                List.length_cons]
              constructor
              · omega
              constructor
              · omega
              · intro d hdmem
                simp only [List.mem_cons] at hdmem
                rcases hdmem with rfl | hdmem
                · omega
                · exact ih'.2.2 d hdmem
          | some t =>
              simp [digits] at h
              rcases h with ⟨ht, hd⟩
              have ih' := ih bs ds hd
              simpa [Option.isNone_some] using ih'
private theorem digits_inj (fmt : List (Option Nat)) :
    ∀ s₁ s₂ ds, digits fmt s₁ = some ds → digits fmt s₂ = some ds → s₁ = s₂ := by
  induction fmt with
  | nil =>
      intro s₁ s₂ ds h₁ h₂
      cases s₁ <;> cases s₂ <;> simp [digits] at h₁ h₂ ⊢
  | cons f fmt ih =>
      intro s₁ s₂ ds h₁ h₂
      cases s₁ with
      | nil => simp [digits] at h₁
      | cons b₁ bs₁ =>
          cases s₂ with
          | nil => simp [digits] at h₂
          | cons b₂ bs₂ =>
              cases f with
              | none =>
                  simp [digits] at h₁ h₂
                  rcases h₁ with ⟨hb₁, ds₁, hd₁, heq₁⟩
                  rcases h₂ with ⟨hb₂, ds₂, hd₂, heq₂⟩
                  have hhead : b₁ - 48 = b₂ - 48 := by
                    have := congrArg List.head? (heq₁.trans heq₂.symm)
                    simpa using this
                  have htail : ds₁ = ds₂ := by
                    have := congrArg List.tail? (heq₁.trans heq₂.symm)
                    simpa using this
                  have hb : b₁ = b₂ := by omega
                  rw [hb, ih bs₁ bs₂ ds₁ hd₁ (by simpa [htail] using hd₂)]
              | some t =>
                  simp [digits] at h₁ h₂
                  rcases h₁ with ⟨hb₁, hd₁⟩
                  rcases h₂ with ⟨hb₂, hd₂⟩
                  rw [← hb₁, ← hb₂]
                  rw [ih bs₁ bs₂ ds hd₁ hd₂]
private theorem digits_paint (fmt : List (Option Nat)) (ds : List Nat)
    (hlen : ds.length = (fmt.filter (fun x => x.isNone)).length)
    (hlt : ∀ d ∈ ds, d < 10) : digits fmt (paint fmt ds) = some ds := by
  induction fmt generalizing ds with
  | nil =>
      have hdnil : ds = [] := List.eq_nil_of_length_eq_zero (by simpa using hlen)
      subst ds
      rfl
  | cons f fmt ih =>
      cases f with
      | none =>
          cases ds with
          | nil => simp at hlen
          | cons d ds' =>
              have ih' : digits fmt (paint fmt ds') = some ds' := by
                apply ih ds'
                · simpa using hlen
                · intro x hx; exact hlt x (by simp [hx])
              simp [digits, paint, ih']
              have hdlt := hlt d (by simp)
              omega
      | some t =>
          simp [digits, paint]
          apply ih ds
          · simpa using hlen
          · exact hlt
private theorem valueAux_eq (ds : List Nat) (acc : Nat) :
    ds.foldl (fun n d => 10 * n + d) acc = acc * 10 ^ ds.length + value ds := by
  induction ds generalizing acc with
  | nil =>
      change acc = acc * 1 + 0
      omega
  | cons d ds ih =>
      rw [List.foldl_cons]
      unfold value
      rw [List.foldl_cons]
      change ds.foldl (fun n x => 10 * n + x) (10 * acc + d) =
        acc * 10 ^ (ds.length + 1) + ds.foldl (fun n x => 10 * n + x) (10 * 0 + d)
      simp only [Nat.mul_zero, Nat.zero_add]
      rw [ih (10 * acc + d), ih d]
      rw [Nat.pow_succ']
      simp only [Nat.add_mul, Nat.mul_assoc]
      ac_rfl

private theorem value_cons (d : Nat) (ds : List Nat) :
    value (d :: ds) = d * 10 ^ ds.length + value ds := by
  change ds.foldl (fun n d => 10 * n + d) (10 * 0 + d) = d * 10 ^ ds.length + value ds
  simp only [Nat.mul_zero, Nat.zero_add]
  rw [valueAux_eq]

private theorem value_lt_pow (ds : List Nat) (hlt : ∀ d ∈ ds, d < 10) :
    value ds < 10 ^ ds.length := by
  induction ds with
  | nil => simp [value]
  | cons d ds ih =>
      have hd : d < 10 := hlt d (by simp)
      have hi := ih (fun x hx => hlt x (by simp [hx]))
      rw [value_cons]
      calc
        d * 10 ^ ds.length + value ds < d * 10 ^ ds.length + 10 ^ ds.length :=
          Nat.add_lt_add_left hi _
        _ = (d + 1) * 10 ^ ds.length := by rw [Nat.succ_mul]
        _ ≤ 10 * 10 ^ ds.length := Nat.mul_le_mul_right _ (Nat.succ_le_of_lt hd)
        _ = 10 ^ (ds.length + 1) := by rw [Nat.pow_succ']
private def lval : List Nat → Nat
  | [] => 0
  | d :: ds => d + 10 * lval ds

private theorem lval_append_singleton (l : List Nat) (x : Nat) :
    lval (l ++ [x]) = lval l + 10 ^ l.length * x := by
  induction l with
  | nil => simp [lval]
  | cons a as ih =>
      simp only [List.cons_append, lval, List.length_cons, ih]
      rw [Nat.mul_add, Nat.pow_succ']
      ac_rfl

private theorem value_rev (ds : List Nat) : value ds = lval ds.reverse := by
  induction ds with
  | nil => rfl
  | cons d ds ih =>
      rw [value_cons, ih]
      rw [List.reverse_cons]
      rw [lval_append_singleton]
      simp only [List.length_reverse]
      ac_rfl

private theorem lval_digits_lt (v n : Nat) :
    lval ((List.range n).map (fun i => v / 10 ^ i % 10)) = v % 10 ^ n := by
  induction n with
  | zero => simp [lval, Nat.mod_one]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.map_singleton, lval_append_singleton, ih]
      simp only [List.length_map, List.length_range]
      rw [Nat.pow_succ']
      let p := 10 ^ n
      let d := v / p % 10
      let q := v / p / 10
      have hp : 0 < p := Nat.pow_pos (by decide : 0 < 10)
      have hr : v % p < p := Nat.mod_lt _ hp
      have hdlt : d < 10 := Nat.mod_lt _ (by decide : 0 < 10)
      have hlt : v % p + p * d < 10 * p := by
        have hdle : d ≤ 9 := by omega
        have hpdle : p * d ≤ p * 9 := Nat.mul_le_mul_left p hdle
        calc
          v % p + p * d < p + p * 9 := Nat.add_lt_add_of_lt_of_le hr hpdle
          _ = 10 * p := by
            rw [Nat.mul_comm p 9, Nat.add_comm, ← Nat.succ_mul]
      have hv1 : v = v % p + p * (v / p) := (Nat.mod_add_div v p).symm
      have hv2 : v / p = d + 10 * q := by
        dsimp [d, q]
        exact (Nat.mod_add_div (v / p) 10).symm
      have hrewrite : v = (v % p + p * d) + (10 * p) * q := by
        calc
          v = v % p + p * (v / p) := hv1
          _ = v % p + p * (d + 10 * q) := by rw [hv2]
          _ = (v % p + p * d) + (10 * p) * q := by
            rw [Nat.mul_add]
            ac_rfl
      calc
        v % p + p * d = (v % p + p * d) % (10 * p) := (Nat.mod_eq_of_lt hlt).symm
        _ = ((v % p + p * d) + (10 * p) * q) % (10 * p) := by
          rw [Nat.add_mul_mod_self_left]
        _ = v % (10 * p) := by rw [← hrewrite]

private theorem value_decDigits (v : Nat) (hv : v < 10 ^ 14) :
    value ((List.range 14).reverse.map (fun i => v / 10 ^ i % 10)) = v := by
  rw [value_rev]
  have hrev : ((List.range 14).reverse.map (fun i => v / 10 ^ i % 10)).reverse =
      (List.range 14).map (fun i => v / 10 ^ i % 10) := by
    rw [List.map_reverse, List.reverse_reverse]
  rw [hrev, lval_digits_lt]
  exact Nat.mod_eq_of_lt hv

private theorem lval_digit (l : List Nat) (hlt : ∀ d ∈ l, d < 10) :
    ∀ i, i < l.length → lval l / 10 ^ i % 10 = l[i]?.getD 0 := by
  induction l with
  | nil => simp
  | cons d rest ih =>
      intro i hi
      cases i with
      | zero =>
          simp only [lval, List.getElem?_cons_zero, Option.getD_some, Nat.pow_zero, Nat.div_one]
          rw [Nat.add_mul_mod_self_left]
          exact Nat.mod_eq_of_lt (hlt d (by simp))
      | succ i =>
          have hi' : i < rest.length := by simpa using hi
          have hd : d < 10 := hlt d (by simp)
          have hdiv10 : (d + 10 * lval rest) / 10 = lval rest := by
            rw [Nat.mul_comm 10 (lval rest)]
            rw [Nat.add_mul_div_right _ _ (by decide : 0 < 10)]
            rw [Nat.div_eq_of_lt hd]
            omega
          rw [Nat.pow_succ']
          rw [← Nat.div_div_eq_div_mul]
          change ((d + 10 * lval rest) / 10) / 10 ^ i % 10 = (d :: rest)[i + 1]?.getD 0
          rw [hdiv10]
          simpa [List.getElem?_cons_succ] using
            ih (fun x hx => hlt x (by simp [hx])) i hi'

private theorem map_range_getD (l : List Nat) :
    (List.range l.length).map (fun i => l[i]?.getD 0) = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map]
      simp only [List.getElem?_cons_zero, Option.getD_some, List.cons.injEq]
      constructor
      · trivial
      · have hmap :
            List.map ((fun i => (a :: as)[i]?.getD 0) ∘ Nat.succ) (List.range as.length) =
              List.map (fun i => as[i]?.getD 0) (List.range as.length) := by
            apply List.map_congr_left
            intro i hi
            simp only [Function.comp_apply, List.getElem?_cons_succ]
        rw [hmap, ih]

private theorem value_digits (ds : List Nat) (hlt : ∀ d ∈ ds, d < 10) :
    ((List.range ds.length).reverse.map (fun i => value ds / 10 ^ i % 10)) = ds := by
  let l := ds.reverse
  have hl : ∀ d ∈ l, d < 10 := by
    intro d hd
    exact hlt d (by simpa [l] using hd)
  calc
    (List.range ds.length).reverse.map (fun i => value ds / 10 ^ i % 10)
        = (List.range l.length).reverse.map (fun i => lval l / 10 ^ i % 10) := by
            simp only [l, List.length_reverse]
            congr 2
            funext i
            rw [value_rev]
    _ = (List.range l.length).reverse.map (fun i => l[i]?.getD 0) := by
            apply List.map_congr_left
            intro i hi
            have hiR : i ∈ List.range l.length := by simpa using (List.mem_reverse.mp hi)
            exact lval_digit l hl i (List.mem_range.mp hiR)
    _ = ((List.range l.length).map (fun i => l[i]?.getD 0)).reverse := by
            rw [List.map_reverse]
    _ = l.reverse := by rw [map_range_getD]
    _ = ds := by simp [l]

/-! ## mapM over Option -/

private theorem mapM_length {α β : Type} (f : α → Option β) :
    ∀ (l : List α) (vs : List β), l.mapM f = some vs → vs.length = l.length := by
  intro l
  induction l with
  | nil =>
      intro vs h
      simp at h
      subst vs
      rfl
  | cons a as ih =>
      intro vs h
      simp only [List.mapM_cons] at h
      cases hf : f a with
      | none => simp [hf] at h
      | some x =>
          cases hm : as.mapM f with
          | none => simp [hf, hm] at h
          | some xs =>
              simp [hf, hm] at h
              subst vs
              simp [ih xs hm]

private theorem mapM_get {α β : Type} (f : α → Option β) :
    ∀ (l : List α) (vs : List β), l.mapM f = some vs →
      ∀ i : Nat, (l[i]?).map f = (vs[i]?).map some := by
  intro l
  induction l with
  | nil =>
      intro vs h i
      simp at h
      subst vs
      simp
  | cons a as ih =>
      intro vs h i
      simp only [List.mapM_cons] at h
      cases hf : f a with
      | none => simp [hf] at h
      | some x =>
          cases hm : as.mapM f with
          | none => simp [hf, hm] at h
          | some xs =>
              simp [hf, hm] at h
              subst vs
              cases i with
              | zero => simp [hf]
              | succ i => simpa [List.getElem?_cons_succ] using ih xs hm i

private theorem mapM_append {α β : Type} (f : α → Option β) (l₁ l₂ : List α)
    (v₁ v₂ : List β) (h₁ : l₁.mapM f = some v₁) (h₂ : l₂.mapM f = some v₂) :
    (l₁ ++ l₂).mapM f = some (v₁ ++ v₂) := by
  rw [List.mapM_append, h₁, h₂]
  rfl

/-! ## temporal-text glue -/

private theorem digit_count14 : (layout.filter (fun x => x.isNone)).length = 14 := by
  simp [layout]

private theorem code_value_bound {w : Bytes} {v : Nat} (h : code w = some v) :
    v < 10 ^ 14 := by
  unfold code at h
  cases hd : digits layout w with
  | none => simp [hd] at h
  | some ds =>
      simp only [hd, Option.map_some, Option.some.injEq] at h
      have hspec := digits_spec layout w ds hd
      have hlen : ds.length = 14 := hspec.2.1.trans digit_count14
      have hlt := value_lt_pow ds hspec.2.2
      rw [hlen] at hlt
      omega

private theorem code_value_lt {w : Bytes} {v : Nat} (h : code w = some v) :
    v < 2 ^ 47 := by
  have hb := code_value_bound h
  have hpow : 10 ^ 14 < 2 ^ 47 := by decide
  exact Nat.lt_trans hb hpow

private theorem spell_code {w : Bytes} {v : Nat} (h : code w = some v) :
    spell v = w := by
  unfold code at h
  cases hd : digits layout w with
  | none => simp [hd] at h
  | some ds =>
      simp only [hd, Option.map_some, Option.some.injEq] at h
      have hspec := digits_spec layout w ds hd
      have hlen : ds.length = 14 := hspec.2.1.trans digit_count14
      have hpaint : digits layout (paint layout ds) = some ds :=
        digits_paint layout ds hspec.2.1 hspec.2.2
      have hw : w = paint layout ds := digits_inj layout w (paint layout ds) ds hd hpaint
      have hvd := value_digits ds hspec.2.2
      rw [hlen] at hvd
      have hspell : spell (value ds) = paint layout ds := by
        show paint layout ((List.range 14).reverse.map (fun i => value ds / 10 ^ i % 10)) =
          paint layout ds
        rw [hvd]
      rw [← h, hspell, hw]

/-! ## goals -/

theorem PackedSeek_sound :
    TinylambOpt.ResearchGoals.Compression.TemporalText.PackedSeek.Goal := by
  intro ws bs i hstream hi
  unfold stream at hstream
  cases hm : ws.mapM code with
  | none => simp [hm] at hstream
  | some vs =>
      have hpack : pack 47 vs = bs := by simpa [hm] using hstream
      have hlen : vs.length = ws.length := mapM_length code ws vs hm
      have hvi : i < vs.length := by omega
      have hcode_i : code ws[i] = some vs[i] := by
        have hopt := mapM_get code ws vs hm i
        simpa [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hvi] using hopt
      have hbound : ∀ x ∈ vs, x < 2 ^ 47 := by
        intro x hx
        rcases List.mem_iff_getElem.mp hx with ⟨j, hj, rfl⟩
        have hwj : j < ws.length := by omega
        have hcj : code ws[j] = some vs[j] := by
          have hopt := mapM_get code ws vs hm j
          simpa [List.getElem?_eq_getElem hwj, List.getElem?_eq_getElem hj] using hopt
        exact code_value_lt hcj
      have hbytes : ∀ b ∈ bs, b < 256 := by
        intro b hb
        rw [← hpack] at hb
        unfold pack at hb
        exact little_lt _ _ hb
      have htake : takeAt bs 47 i = vs[i] := by
        rw [takeAt_eq_get bs 47 i hbytes, ← hpack]
        exact get_pack 47 vs hbound hvi
      have hmain : extract bs i = ws[i] := by
        unfold extract
        rw [htake]
        exact spell_code hcode_i
      rw [hmain, List.getElem?_eq_getElem hi]

theorem AlignedAppend_sound :
    TinylambOpt.ResearchGoals.Compression.TemporalText.AlignedAppend.Goal := by
  intro a b first second ha hsa hsb
  unfold stream at hsa hsb
  cases hma : a.mapM code with
  | none => simp [hma] at hsa
  | some va =>
      have hpa : pack 47 va = first := by simpa [hma] using hsa
      have hla : va.length = a.length := mapM_length code a va hma
      cases hmb : b.mapM code with
      | none => simp [hmb] at hsb
      | some vb =>
          have hpb : pack 47 vb = second := by simpa [hmb] using hsb
          have hlb : vb.length = b.length := mapM_length code b vb hmb
          have hab : (a ++ b).mapM code = some (va ++ vb) :=
            mapM_append code a b va vb hma hmb
          unfold stream
          rw [hab]
          simp only [Option.map_some, Option.some.injEq]
          rw [← hpa, ← hpb]
          have hvl : va.length % 8 = 0 := by omega
          exact pack_append_47 hvl
end TinylambOpt.ResearchProofs.Compression.TemporalText.PackedSeekAndAlignedAppend
