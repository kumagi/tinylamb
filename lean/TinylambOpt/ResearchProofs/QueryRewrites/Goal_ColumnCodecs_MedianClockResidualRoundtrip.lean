import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
import TinylambOpt.ResearchGoals.Compression.Wire

/-!
# MedianClockResidualRoundtrip

For equal-length timestamp columns with residuals that fit the chosen width, the per-row
median and the selected signed residual reconstruct the timestamp at any valid index. A
column reader can recover one timestamp without expanding all three columns.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_MedianClockResidualRoundtrip

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
open TinylambOpt.ResearchGoals.Compression.Wire

theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

theorem fromLittle_append_singleton (l : Bytes) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256 ^ l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
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

theorem fromLittle_little (v k : Nat) :
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

theorem word_lt_pow (w : Nat) (xs : List Nat) : word w xs < 2 ^ (xs.length * w) := by
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

theorem fromLittle_pack (w : Nat) (xs : List Nat) :
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

theorem word_shift (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) :
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

theorem word_digit (w i : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w)
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

theorem fromLittle_take_mod (bs : Bytes) (m : Nat) (hbs : ∀ b ∈ bs, b < 256) :
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

theorem fromLittle_drop_div (bs : Bytes) (s : Nat) (hbs : ∀ b ∈ bs, b < 256) :
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

theorem little_lt (n k : Nat) : ∀ b ∈ little n k, b < 256 := by
  intro b hb
  simp only [little, List.mem_map] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

theorem takeAt_pack (w : Nat) (xs : List Nat) (hx : ∀ x ∈ xs, x < 2 ^ w) (j y : Nat)
    (hj : xs[j]? = some y) : takeAt (pack w xs) w j = y := by
  have hinv : ∀ w xs, (∀ x ∈ xs, x < 2^w) →
      (List.range xs.length).map (get (pack w xs) w) = xs :=
    sound_PackedInverse
  have hloc : ∀ bs w i, (∀ b ∈ bs, b < 256) → takeAt bs w i = get bs w i :=
    sound_LocalRead
  rw [hloc _ _ _ (by intro b hb; exact little_lt _ _ b hb)]
  have hlt : j < xs.length := (List.getElem?_eq_some_iff.mp hj).1
  have h := hinv w xs hx
  have h3 := congrArg (fun l => l[j]?) h
  simp [hlt, hj] at h3
  rw [h3]
  exact (List.getElem?_eq_some_iff.mp hj).2


theorem sound_SignedInverse : SignedInverse.Goal := by
  intro x
  cases x with
  | ofNat n =>
      simp [zig, unzig]
  | negSucc n =>
      have h : (2 * n + 1) % 2 = 1 := by omega
      simp [zig, unzig, h]
      omega

private theorem getElem?_some {α : Type} (l : List α) (i : Nat) (h : i < l.length) :
    ∃ v, l[i]? = some v := by
  induction l generalizing i with
  | nil => simp at h
  | cons a as ih =>
    cases i with
    | zero => exact ⟨a, by simp⟩
    | succ j =>
      have h' : j < as.length := by
        simp only [List.length_cons] at h
        omega
      exact ih j h'

private theorem zip_getElem? {α β : Type} : ∀ (xs : List α) (ys : List β) (i : Nat) {x y},
    xs[i]? = some x → ys[i]? = some y → (xs.zip ys)[i]? = some (x, y) := by
  intro xs
  induction xs with
  | nil => intro ys i x y hx _; cases hx
  | cons a as ih =>
    intro ys i x y hx hy
    cases ys with
    | nil => cases hy
    | cons b bs =>
      cases i with
      | zero =>
        simp only [List.getElem?_cons_zero] at hx hy
        have eax := Option.some.inj hx
        have eby := Option.some.inj hy
        subst eax; subst eby
        simp
      | succ j =>
        simp only [List.getElem?_cons_succ] at hx hy ⊢
        exact ih bs j hx hy

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs.MedianClockResidualRoundtrip.Goal := by
  intro a b c w i hab hbc hi
  intro anchors
  have hlenbc : (b.zip c).length = a.length := by
    have hmin : min b.length c.length = b.length := by omega
    rw [List.length_zip, hmin, hab]
  have hanch : anchors.length = a.length := by
    dsimp only [anchors]
    rw [List.length_map, List.length_zip, hlenbc]
    omega
  have hi' : i < anchors.length := by omega
  have key : ∀ col ∈ [a, b, c], col.length = a.length →
      (∀ d ∈ (col.zip anchors).map (fun (v, k) => zig (v - k)), d < 2 ^ w) →
      (anchors[i]?.getD 0)
          + unzig (takeAt (pack w ((col.zip anchors).map (fun (v, k) => zig (v - k)))) w i)
        = col[i]?.getD 0 := by
    intro col hcol hlen hres
    have hicol : i < col.length := by omega
    obtain ⟨v, hv⟩ := getElem?_some col i hicol
    obtain ⟨k, hk⟩ := getElem?_some anchors i hi'
    have hzip : (col.zip anchors)[i]? = some (v, k) := zip_getElem? _ _ _ hv hk
    have hri : ((col.zip anchors).map (fun (t : Int × Int) => zig (t.1 - t.2)))[i]?
        = some (zig (v - k)) := by
      rw [List.getElem?_map, hzip]
      simp
    have ht : takeAt (pack w ((col.zip anchors).map (fun (t : Int × Int) =>
        zig (t.1 - t.2)))) w i = zig (v - k) :=
      takeAt_pack w _ hres i _ hri
    have hgoal : (k : Int) + unzig (takeAt (pack w ((col.zip anchors).map
        (fun (t : Int × Int) => zig (t.1 - t.2)))) w i) = v := by
      rw [ht, sound_SignedInverse]
      omega
    simpa [hv, hk] using hgoal
  intro col hcol residual hres
  have hc : col = a ∨ col = b ∨ col = c := by simpa using hcol
  rcases hc with h | h | h
  · exact key col hcol (by rw [h]) hres
  · exact key col hcol (by rw [h]; exact hab.symm) hres
  · exact key col hcol (by rw [h]; exact (hbc.symm.trans hab.symm)) hres
end TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_MedianClockResidualRoundtrip


-- C++ references to this model or its proof.
-- table/rematch_storage_codecs.hpp
