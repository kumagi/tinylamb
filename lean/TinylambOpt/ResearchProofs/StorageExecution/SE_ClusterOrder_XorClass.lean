import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved (12 claims, including the following)

- `LocalRead` — `∀ bs w i, (∀ b ∈ bs, b < 256) → takeAt bs w i = get bs w i`
- `PackedCharge` — `∀ w xs, (pack w xs).length = byteCount (xs.length*w)`
- `PackedInverse` — `∀ w xs, (∀ x ∈ xs, x < 2^w) → (List.range xs.length).map (get
  (pack w xs) w) = xs`

Refuted by counterexample

- `XorClass.TwoColumnWin` — Aggressive hypothesis: the combined RLE payload always
  compensates for the permutation on nonconstant pages.
-/
set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.StorageExecution.ClusterOrder.XorClass
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire
attribute [local instance] boolRelToRel

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

/-! ## PackedInverse -/

theorem SEK_packed_inverse : PackedInverse.Goal := by
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

theorem SEK_local_read : LocalRead.Goal := by
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

theorem SEK_packed_charge : PackedCharge.Goal := by
  intro w xs
  simp [pack, little]


def leq2 (a b : Row) : Bool := a.1 < b.1 || (a.1 == b.1 && a.2 ≤ b.2)
def leK (key : Key) (r r' : Row) : Bool := leq2 (key r) (key r')

theorem leq2_iff (a b : Row) :
    leq2 a b = true ↔ (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)) := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  by_cases h1 : a1 < b1
  · simp [leq2, h1]
  · by_cases h2 : a1 = b1
    · by_cases h3 : a2 ≤ b2 <;> simp [leq2, h2, h3]
    · simp [leq2, h1, h2]

theorem leq2_true {a b : Row} (h : leq2 a b = true) : a.1 ≤ b.1 := by
  rcases (leq2_iff a b).mp h with h | ⟨e, _⟩
  · exact Nat.le_of_lt h
  · exact e ▸ Nat.le_refl _

theorem leq2_total (a b : Row) : (leq2 a b || leq2 b a) = true := by
  rw [Bool.or_eq_true, leq2_iff, leq2_iff]
  rcases Nat.lt_trichotomy a.1 b.1 with h | h | h
  · exact Or.inl (Or.inl h)
  · by_cases hc : a.2 ≤ b.2
    · exact Or.inl (Or.inr ⟨h, hc⟩)
    · exact Or.inr (Or.inr ⟨h ▸ rfl, Nat.le_of_not_le hc⟩)
  · exact Or.inr (Or.inl h)

theorem leq2_trans :
    ∀ a b c : Row, leq2 a b = true → leq2 b c = true → leq2 a c = true := by
  intro a b c h₁ h₂
  rw [leq2_iff] at h₁ h₂ ⊢
  rcases h₁ with h₁ | ⟨e₁, l₁⟩
  · rcases h₂ with h₂ | ⟨e₂, l₂⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases h₂ with h₂ | ⟨e₂, l₂⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, by omega⟩

theorem leq2_antisym {a b : Row} (h₁ : leq2 a b = true) (h₂ : leq2 b a = true) : a = b := by
  obtain ⟨a1, a2⟩ := a
  obtain ⟨b1, b2⟩ := b
  rw [leq2_iff] at h₁ h₂
  rcases h₁ with h₁ | ⟨e₁, l₁⟩
  · rcases h₂ with h₂ | ⟨e₂, l₂⟩ <;> simp only [Prod.mk.injEq] <;> omega
  · rcases h₂ with h₂ | ⟨e₂, l₂⟩
    · exact absurd h₂ (Nat.not_lt_of_le (Nat.le_of_eq e₁))
    · simp only [Prod.mk.injEq]; omega

theorem leK_trans (key : Key) :
    ∀ a b c : Row, leK key a b = true → leK key b c = true → leK key a c = true := by
  intro a b c h₁ h₂
  simpa only [leK] using leq2_trans (key a) (key b) (key c) h₁ h₂

theorem leK_total (key : Key) : ∀ a b : Row, (leK key a b || leK key b a) = true := by
  intro a b
  simpa only [leK] using leq2_total (key a) (key b)

theorem leK_self (key : Key) (r : Row) : leK key r r = true :=
  (leq2_iff (key r) (key r)).mpr (Or.inr ⟨rfl, Nat.le_refl _⟩)

theorem leK_antisym (key : Key) {x y : Row}
    (h₁ : leK key x y = true) (h₂ : leK key y x = true) : key x = key y :=
  leq2_antisym h₁ h₂

/-- The comparison hidden inside `perm` is exactly `zipIdxLE` of the key order. -/
theorem perm_cmp_eq (key : Key) :
    (fun (p q : Row × Nat) =>
      let a := key p.1
      let b := key q.1
      a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && p.2 ≤ q.2)))) =
      List.zipIdxLE (leK key) := by
  funext ⟨x, i⟩ ⟨y, j⟩
  rw [Bool.eq_iff_iff]
  simp only [List.zipIdxLE, leK, leq2_iff, decide_eq_true_eq, Bool.ite_eq_true_distrib,
    Bool.and_eq_true, Bool.or_eq_true]
  rcases Nat.lt_trichotomy (key x).1 (key y).1 with h | h | h <;>
    rcases Nat.lt_trichotomy (key x).2 (key y).2 with h' | h' | h' <;>
      by_cases hij : (key x).2 ≤ (key y).2 <;>
        simp [h, h', hij, Nat.lt_irrefl, Nat.not_lt, Nat.le_of_lt] <;> omega

theorem k_map_congr {α β : Type} {l : List α} {f g : α → β} (h : ∀ a ∈ l, f a = g a) :
    l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    rw [List.map_cons, List.map_cons, h a (List.mem_cons_self),
      ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

theorem perm_eq_merge (key : Key) (rs : List Row) :
    perm key rs = ((rs.zipIdx).mergeSort (List.zipIdxLE (leK key))).map Prod.snd := by
  show ((rs.zipIdx).mergeSort
        (fun ⟨x, i⟩ ⟨y, j⟩ =>
          let a := key x
          let b := key y
          a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && i ≤ j))))).map Prod.snd = _
  rw [perm_cmp_eq key]

theorem phys_eq_sort (key : Key) (rs : List Row) :
    physical key rs = rs.mergeSort (leK key) := by
  show ((perm key rs).map (fun i => rs[i]?.getD (0, 0))) = _
  rw [perm_eq_merge key rs, List.map_map]
  have hmap : ((rs.zipIdx).mergeSort (List.zipIdxLE (leK key))).map
      ((fun i => rs[i]?.getD (0, 0)) ∘ Prod.snd) =
      ((rs.zipIdx).mergeSort (List.zipIdxLE (leK key))).map Prod.fst := by
    refine k_map_congr (fun p hp => ?_)
    rw [List.mem_mergeSort] at hp
    obtain ⟨v, i⟩ := p
    rw [List.mem_zipIdx_iff_getElem?] at hp
    cases hs : rs[i]? with
    | some w =>
      have hw : w = v := Option.some.inj (hs ▸ hp)
      simp [hs, hw]
    | none => rw [hs] at hp; exact absurd hp (by simp)
  rw [hmap, List.mergeSort_zipIdx]

theorem perm_perm_range (key : Key) (rs : List Row) :
    (perm key rs).Perm (List.range rs.length) := by
  rw [perm_eq_merge]
  have h : ((List.zipIdx rs).mergeSort (List.zipIdxLE (leK key))).Perm (List.zipIdx rs) :=
    List.mergeSort_perm _ _
  refine (List.Perm.map Prod.snd h).trans ?_
  rw [List.zipIdx_map_snd 0, ← List.range_eq_range']

theorem perm_length (key : Key) (rs : List Row) : (perm key rs).length = rs.length :=
  (perm_perm_range key rs).length_eq.trans List.length_range

theorem perm_nodup (key : Key) (rs : List Row) : (perm key rs).Nodup :=
  (perm_perm_range key rs).symm.nodup List.nodup_range

theorem perm_mem (key : Key) (rs : List Row) (i : Nat) :
    i ∈ perm key rs ↔ i < rs.length := by
  rw [(perm_perm_range key rs).mem_iff]
  exact List.mem_range

theorem physical_perm (key : Key) (rs : List Row) :
    (physical key rs).Perm rs := by
  rw [phys_eq_sort]
  exact List.mergeSort_perm _ _

theorem sorted_col (key : Key) (rs : List Row) :
    (physical key rs).Pairwise (fun x y => (key x).1 ≤ (key y).1) := by
  rw [phys_eq_sort]
  refine List.Pairwise.imp (fun h => ?_)
      (List.pairwise_mergeSort (leK_trans key) (leK_total key) rs)
  simpa [leK] using leq2_true h

theorem pairwise_col_sort (key : Key) (rs : List Row) (f : Row → Nat)
    (hf : ∀ r, f r = (key r).1) :
    ((physical key rs).map f).Pairwise (· ≤ ·) := by
  rw [phys_eq_sort]
  have h1 : (rs.mergeSort (leK key)).Pairwise (fun x y => (key x).1 ≤ (key y).1) := by
    refine List.Pairwise.imp (fun h => ?_)
      (List.pairwise_mergeSort (leK_trans key) (leK_total key) rs)
    simpa [leK] using leq2_true h
  have h2 : (rs.mergeSort (leK key)).map f =
      (rs.mergeSort (leK key)).map (fun r => (key r).1) := k_map_congr (fun r _ => hf r)
  rw [h2, List.pairwise_map]
  exact h1

theorem takeWhile_countP (s l : List Row) (f : Row → Nat)
    (w : s.Pairwise (fun x y => f x ≤ f y)) (pp : s.Perm l) (v : Nat) :
    (s.takeWhile (fun x => f x < v)).length = l.countP (fun x => f x < v) := by
  have key : ∀ (u : List Row), u.Pairwise (fun x y => f x ≤ f y) →
      (u.takeWhile (fun x => f x < v)).length = u.countP (fun x => f x < v) := by
    intro u
    induction u with
    | nil => intro _; rfl
    | cons a t ih =>
      intro wu
      have wt : t.Pairwise (fun x y => f x ≤ f y) := (List.pairwise_cons.1 wu).2
      by_cases ha : f a < v
      · rw [List.takeWhile_cons_of_pos (p := fun x => f x < v) (a := a) (by simpa using ha),
          List.countP_cons_of_pos (p := fun x => f x < v) (a := a) (by simpa using ha),
          List.length_cons, ih wt]
      · rw [List.takeWhile_cons_of_neg (p := fun x => f x < v) (a := a) (by simpa using ha),
          List.length_nil, eq_comm, List.countP_eq_zero]
        intro x hx hpx
        rcases List.mem_cons.1 hx with rfl | hx'
        · exact ha (by simpa using hpx)
        · have hle : f a ≤ f x := (List.pairwise_cons.1 wu).1 x hx'
          have hlt : f x < v := by simpa using hpx
          omega
  exact (key s w).trans (pp.countP_eq _)

theorem takeWhile_eqP_countP (s l : List Row) (f : Row → Nat)
    (w : s.Pairwise (fun x y => f x ≤ f y)) (pp : s.Perm l) :
    (s.takeWhile (fun x => f x == 0)).length = l.countP (fun x => f x == 0) := by
  have h1 : ∀ (u : List Row), u.Pairwise (fun x y => f x ≤ f y) →
      (u.takeWhile (fun x => f x == 0)).length = u.countP (fun x => f x == 0) := by
    intro u
    induction u with
    | nil => intro _; rfl
    | cons a t ih =>
      intro wu
      have wt : t.Pairwise (fun x y => f x ≤ f y) := (List.pairwise_cons.1 wu).2
      by_cases hz : f a == 0
      · rw [List.takeWhile_cons_of_pos (p := fun x => f x == 0) (a := a) hz,
          List.countP_cons_of_pos (p := fun x => f x == 0) (a := a) hz,
          List.length_cons, ih wt]
      · rw [List.takeWhile_cons_of_neg (p := fun x => f x == 0) (a := a) hz,
          List.length_nil, eq_comm, List.countP_eq_zero]
        intro x hx hpx
        have ha0 : f a ≠ 0 := by simpa using hz
        rcases List.mem_cons.1 hx with rfl | hx'
        · exact ha0 (by simpa using hpx)
        · have hle : f a ≤ f x := (List.pairwise_cons.1 wu).1 x hx'
          have h0 : f x = 0 := by simpa using hpx
          omega
  exact (h1 s w).trans (pp.countP_eq (fun x => f x == 0))

theorem count_dropWhile_takeWhile (s l : List Nat) (v : Nat)
    (w : s.Pairwise (· ≤ ·)) (pp : s.Perm l) :
    (s.dropWhile (· < v) |>.takeWhile (· == v)).length = l.count v := by
  have aux : ∀ (m : List Nat), m.Pairwise (· ≤ ·) → (∀ x ∈ m, v ≤ x) →
      (m.takeWhile (· == v)).length = m.count v := by
    intro m
    induction m with
    | nil => intro _ _; rfl
    | cons a t ih =>
      intro w' hall
      have ha : v ≤ a := hall a (by simp)
      by_cases heq : a = v
      · rw [heq]
        rw [List.takeWhile_cons_of_pos (p := fun x => x == v) (a := v) (by simp),
          List.count_cons_self, List.length_cons]
        rw [ih ((List.pairwise_cons.1 w').2) (fun x hx => hall x (by simp [hx]))]
      · rw [List.count_cons_of_ne (by simpa using heq),
          List.takeWhile_cons_of_neg (p := fun x => x == v) (a := a) (by simpa using heq),
          List.length_nil, eq_comm]
        show List.countP (fun x => x == v) t = 0
        rw [List.countP_eq_zero]
        intro x hx hpx
        have hav : v < a := Nat.lt_of_le_of_ne ha (fun h => heq h.symm)
        have hle : a ≤ x := (List.pairwise_cons.1 w').1 x hx
        have hxv : x = v := by simpa using hpx
        omega
  have h1 : ∀ (m : List Nat), m.Pairwise (· ≤ ·) →
      ((m.dropWhile (· < v)).takeWhile (· == v)).length = m.count v := by
    intro m
    induction m with
    | nil => intro _; rfl
    | cons a t ih =>
      intro wm
      have wt : t.Pairwise (· ≤ ·) := (List.pairwise_cons.1 wm).2
      by_cases ha : a < v
      · rw [List.dropWhile_cons_of_pos (p := fun x => x < v) (a := a) (by simpa using ha),
          List.count_cons_of_ne (by omega)]
        exact ih wt
      · rw [List.dropWhile_cons_of_neg (p := fun x => x < v) (a := a) (by simpa using ha)]
        exact aux (a :: t) wm (fun x hx => by
          rcases List.mem_cons.1 hx with rfl | hx'
          · omega
          · have hle : a ≤ x := (List.pairwise_cons.1 wm).1 x hx'
            omega)
  exact (h1 s w).trans (pp.countP_eq (fun x => x == v))

/-! Bit-packing sidecar round trip. -/

theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  cases n with
  | zero => simp [width]
  | succ n =>
    have hw : width (n + 1) = (n + 1).log2 + 1 := by simp [width]
    rw [hw]
    exact Nat.lt_log2_self

theorem pack_bytes_lt (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  rw [show pack w xs = (List.range (byteCount (xs.length * w))).map
      (fun i => (word w xs / 256 ^ i) % 256) from rfl] at hb
  rw [List.mem_map] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

theorem packed_getD (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) (i : Nat)
    (hi : i < xs.length) :
    get (pack w xs) w i = xs[i]?.getD 0 := by
  have hinv := SEK_packed_inverse w xs h
  have h4 := congrArg (fun l : List Nat => l[i]?) hinv
  rw [← h4, List.getElem?_map, List.getElem?_range hi]
  rfl

theorem ordinal_eq (key : Key) (rs : List Row) (i : Nat) (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]?.getD 0 := by
  have hpc : i < (perm key rs).length := by rw [perm_length]; exact hi
  have hbound : ∀ x ∈ perm key rs, x < 2 ^ width (rs.length - 1) := by
    intro x hx
    have hlen : x < rs.length := perm_mem key rs x |>.mp hx
    have hw := lt_two_pow_width (rs.length - 1)
    omega
  have hget : takeAt (side key rs) (width (rs.length - 1)) i =
      get (pack (width (rs.length - 1)) (perm key rs)) (width (rs.length - 1)) i :=
    SEK_local_read _ _ _ (pack_bytes_lt _ _)
  show ordinal key rs i = _
  rw [show ordinal key rs i = takeAt (side key rs) (width (rs.length - 1)) i from rfl, hget,
    packed_getD _ _ hbound i hpc, List.getElem?_eq_getElem hpc]

theorem side_len (key : Key) (rs : List Row) :
    (side key rs).length = orderBytes rs.length := by
  show (pack (width (rs.length - 1)) (perm key rs)).length = _
  rw [SEK_packed_charge, orderBytes, perm_length]

/-! Generic list helpers. -/

def kpred (p : Row → Bool) : Row × Nat → Option Nat := fun (r, j) => if p r then some j else none

theorem kpred_some (p : Row → Bool) (a : Row) (s : Nat) (h : p a = true) :
    kpred p (a, s) = some s := by
  show (if p a = true then some s else none) = some s
  simp [h]

theorem kpred_none (p : Row → Bool) (a : Row) (s : Nat) (h : p a = false) :
    kpred p (a, s) = none := by
  show (if p a = true then some s else none) = none
  simp [h]

theorem map_range_getD {α : Type} (l : List α) (d : α) :
    (List.range l.length).map (fun i => l[i]?.getD d) = l := by
  refine List.ext_getElem? (fun i => ?_)
  rcases Nat.lt_or_ge i l.length with h | h
  · have h1 : ((List.range l.length).map (fun i => l[i]?.getD d))[i]? = some (l[i]?.getD d) := by
      rw [List.getElem?_map, List.getElem?_range h]
      rfl
    rw [h1, List.getElem?_eq_getElem h]
    rfl
  · have h2 : ((List.range l.length).map (fun i => l[i]?.getD d))[i]? = none := by
      rw [List.getElem?_eq_none (l := (List.range l.length).map (fun i => l[i]?.getD d)) (i := i)]
      rw [List.length_map, List.length_range]
      exact h
    rw [h2, List.getElem?_eq_none (l := l) (i := i) h]

theorem map_eq_map_range_getD {α β : Type} (f : α → β) (l : List α) (d : α) :
    l.map f = (List.range l.length).map (fun i => f (l[i]?.getD d)) := by
  refine List.ext_getElem? (fun i => ?_)
  rcases Nat.lt_or_ge i l.length with h | h
  · have h1 : ((List.range l.length).map (fun i => f (l[i]?.getD d)))[i]? =
      some (f (l[i]?.getD d)) := by
      rw [List.getElem?_map, List.getElem?_range h]
      rfl
    rw [List.getElem?_map, List.getElem?_eq_getElem h, h1, List.getElem?_eq_getElem h]
    rfl
  · have h2 : (l.map f)[i]? = none := by
      rw [List.getElem?_eq_none (l := l.map f) (i := i)]
      rw [List.length_map]
      exact h
    have h3 : ((List.range l.length).map (fun i => f (l[i]?.getD d)))[i]? = none := by
      rw [List.getElem?_eq_none (l := (List.range l.length).map (fun i => f (l[i]?.getD d)))
        (i := i)]
      rw [List.length_map, List.length_range]
      exact h
    exact h2.trans h3.symm

theorem zipIdx_eq_map {α : Type} (l : List α) (s : Nat) (d : α) :
    l.zipIdx s = (List.range l.length).map (fun i => (l[i]?.getD d, s + i)) := by
  refine List.ext_getElem? (fun j => ?_)
  rcases Nat.lt_or_ge j l.length with hj | hj
  · have h1 : (l.zipIdx s)[j]? = Option.map (fun a => (a, s + j)) l[j]? :=
      List.getElem?_zipIdx (l := l) (i := s) (j := j)
    have h3 : (List.range l.length)[j]? = some j := List.getElem?_range hj
    rw [h1, List.getElem?_map, h3, Option.map_some, List.getElem?_eq_getElem hj]
    rfl
  · have h1 : (l.zipIdx s)[j]? = none := by
      rw [List.getElem?_eq_none (l := l.zipIdx s) (i := j)]
      rw [List.length_zipIdx]
      exact hj
    have h2 : ((List.range l.length).map (fun i => (l[i]?.getD d, s + i)))[j]? = none := by
      rw [List.getElem?_eq_none
        (l := (List.range l.length).map (fun i => (l[i]?.getD d, s + i))) (i := j)]
      rw [List.length_map, List.length_range]
      exact hj
    exact h1.trans h2.symm

theorem zipIdx_append (a b : List Row) :
    ∀ s, (a ++ b).zipIdx s = a.zipIdx s ++ b.zipIdx (s + a.length) := by
  induction a with
  | nil => intro s; simp
  | cons x t ih =>
    intro s
    show (x :: (t ++ b)).zipIdx s = (x :: t).zipIdx s ++ b.zipIdx (s + (x :: t).length)
    simp only [List.zipIdx_cons, List.length_cons]
    rw [ih (s + 1)]
    refine congrArg (List.cons (x, s))
      (congrArg (HAppend.hAppend (List.zipIdx t (s + 1)))
        (congrArg (fun y => List.zipIdx b y) ?_))
    omega

theorem snd_ge_zipIdx (l : List α) : ∀ (s : Nat) p, p ∈ l.zipIdx s → s ≤ p.2 := by
  induction l with
  | nil => intro s p hp; exact absurd hp (by simp)
  | cons a t ih =>
    intro s p hp
    rw [List.zipIdx_cons] at hp
    rcases List.mem_cons.1 hp with heq | hpt
    · obtain ⟨rfl, rfl⟩ := heq
      exact Nat.le_refl _
    · have h2 := ih (s + 1) p hpt
      omega

theorem snd_ge_filterMap (l : List Row) :
    ∀ (s : Nat) (p : Row → Bool) x, x ∈ (l.zipIdx s).filterMap (kpred p) → s ≤ x := by
  induction l with
  | nil => intro s p x hx; exact absurd hx (by simp)
  | cons a t ih =>
    intro s p x hx
    rw [List.zipIdx_cons] at hx
    by_cases hq : p a
    · have hfa := kpred_some p a s (by simpa using hq)
      rw [List.filterMap_cons_some (f := kpred p) hfa] at hx
      rcases List.mem_cons.1 hx with heq | hrest
      · subst heq
        exact Nat.le_refl _
      · have h2 := ih (s + 1) p x hrest
        omega
    · have hfa := kpred_none p a s (by simpa using hq)
      rw [List.filterMap_cons_none (f := kpred p) hfa] at hx
      have h2 := ih (s + 1) p x hx
      omega

theorem zipIdx_filterMap_props (l : List Row) :
    ∀ (s : Nat) (p : Row → Bool),
      List.Pairwise (· ≤ ·) ((l.zipIdx s).filterMap (kpred p)) ∧
      ((l.zipIdx s).filterMap (kpred p)).Nodup ∧
      ∀ x ∈ (l.zipIdx s).filterMap (kpred p), s ≤ x := by
  induction l with
  | nil => intro s p; simp
  | cons a t ih =>
    intro s p
    obtain ⟨w₁, w₂, w₃⟩ := ih (s + 1) p
    by_cases hq : p a
    · have hfa := kpred_some p a s (by simpa using hq)
      rw [List.zipIdx_cons, List.filterMap_cons_some (f := kpred p) hfa]
      refine ⟨?_, ?_, ?_⟩
      · refine List.pairwise_cons.2 ⟨fun x hx =>
          Nat.le_of_succ_le (w₃ x hx), w₁⟩
      · exact List.nodup_cons.2 ⟨fun h => absurd (w₃ s h) (by omega), w₂⟩
      · intro x hx
        rcases List.mem_cons.1 hx with heq | hx'
        · subst heq
          exact Nat.le_refl _
        · have h2 := w₃ x hx'
          omega
    · have hfa := kpred_none p a s (by simpa using hq)
      rw [List.zipIdx_cons, List.filterMap_cons_none (f := kpred p) hfa]
      exact ⟨w₁, w₂, fun x hx => by have h2 := w₃ x hx; omega⟩

theorem gather_filter_aux (l : List Row) :
    ∀ (s : Nat) (p : Row → Bool),
      List.map (fun j => l[j - s]?.getD (0, 0)) ((l.zipIdx s).filterMap (kpred p)) =
        List.filter p l := by
  induction l with
  | nil => intro s p; simp
  | cons a t ih =>
    intro s p
    by_cases hq : p a
    · have hfa := kpred_some p a s (by simpa using hq)
      rw [List.zipIdx_cons, List.filterMap_cons_some (f := kpred p) hfa, List.map_cons,
        List.filter_cons_of_pos (by simpa using hq)]
      rw [List.cons.injEq]
      refine ⟨by simp, ?_⟩
      refine (List.map_congr_left (fun j hj => by
        have hge : s + 1 ≤ j := snd_ge_filterMap t (s + 1) p j hj
        have h2 : j - s = (j - (s + 1)) + 1 := by omega
        rw [h2, List.getElem?_cons_succ])).trans (ih (s + 1) p)
    · have hfa := kpred_none p a s (by simpa using hq)
      rw [List.zipIdx_cons, List.filterMap_cons_none (f := kpred p) hfa,
        List.filter_cons_of_neg (by simpa using hq)]
      refine (List.map_congr_left (fun j hj => by
        have hge : s + 1 ≤ j := snd_ge_filterMap t (s + 1) p j hj
        have h2 : j - s = (j - (s + 1)) + 1 := by omega
        rw [h2, List.getElem?_cons_succ])).trans (ih (s + 1) p)

theorem gather_filter (rs : List Row) (p : Row → Bool) :
    List.map (fun j => rs[j]?.getD (0, 0)) ((rs.zipIdx).filterMap (kpred p)) = rs.filter p := by
  have := gather_filter_aux rs 0 p
  simpa using this

theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ a ∈ l, f a = g a) : List.filterMap f l = List.filterMap g l := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    rw [List.filterMap_cons, List.filterMap_cons, h a (by simp)]
    have hi := ih (fun x hx => h x (List.mem_cons_of_mem a hx))
    rcases h' : g a with _|b
    · simpa [h'] using hi
    · simpa [h'] using hi

theorem List.filter_congr' {α : Type} {l : List α} {p q : α → Bool}
    (h : ∀ a ∈ l, p a = q a) : List.filter p l = List.filter q l := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    rw [List.filter_cons, List.filter_cons, h a (by simp)]
    have hi := ih (fun x hx => h x (List.mem_cons_of_mem a hx))
    rcases h' : q a with _|_
    · simpa [h'] using hi
    · simpa [h'] using hi

theorem nodup_idxOf_eq {l : List Nat} (hn : l.Nodup) :
    ∀ {k : Nat} (hk : k < l.length), l.idxOf (l[k]'hk) = k := by
  induction l with
  | nil => intro k hk; simp at hk
  | cons a t ih =>
    intro k hk
    obtain ⟨hn1, hn2⟩ := List.nodup_cons.1 hn
    cases k with
    | zero =>
      have hv0 : (a :: t)[0]'hk = a := rfl
      rw [hv0]
      exact List.idxOf_cons_self
    | succ m =>
      have hlt : m < t.length := by simpa using hk
      have hv : (a :: t)[m + 1]'hk = t[m]'hlt := rfl
      rw [List.idxOf_cons, if_neg ?_, hv, ih hn2 hlt]
      · intro hh
        have ha : a = (a :: t)[m + 1] := by simpa using hh
        have hmem : (a :: t)[m + 1] ∈ t := by
          show t[m] ∈ t
          exact List.getElem_mem hlt
        exact hn1 (ha ▸ hmem)

theorem set_map_idxOf (rs : List Row) (i : Nat) (v : Row) (ids : List Nat)
    (hn : ids.Nodup) (hi : i ∈ ids) (hlt : ∀ j ∈ ids, j < rs.length) :
    (List.map (fun j => rs[j]?.getD (0, 0)) ids).set (ids.idxOf i) v =
      List.map (fun j => (rs.set i v)[j]?.getD (0, 0)) ids := by
  have hidx : ids.idxOf i < ids.length := List.idxOf_lt_length_of_mem hi
  have hge : ids[ids.idxOf i] = i := List.getElem_idxOf hidx
  have him : i < rs.length := hlt i hi
  have hlen : (List.map (fun j => rs[j]?.getD (0, 0)) ids).length = ids.length :=
    List.length_map _
  have hlen2 : (List.map (fun j => (rs.set i v)[j]?.getD (0, 0)) ids).length = ids.length :=
    List.length_map _
  refine List.ext_getElem? (fun k => ?_)
  by_cases hk : k < ids.length
  · have hk' : k < (List.map (fun j => rs[j]?.getD (0, 0)) ids).length := by rw [hlen]; exact hk
    have hk2 : k < (List.map (fun j => (rs.set i v)[j]?.getD (0, 0)) ids).length :=
      by rw [hlen2]; exact hk
    by_cases hke : ids.idxOf i = k
    · subst hke
      have hL : ((List.map (fun j => rs[j]?.getD (0, 0)) ids).set (ids.idxOf i) v)[ids.idxOf i]?
          = some v := by
        rw [List.getElem?_set, if_pos rfl, if_pos (by rw [hlen]; exact hidx)]
      have hR : (List.map (fun j => (rs.set i v)[j]?.getD (0, 0)) ids)[ids.idxOf i]? = some v := by
        rw [List.getElem?_eq_getElem (by rw [hlen2]; exact hidx), List.getElem_map, hge,
          List.getElem?_set_self him]
        simp
      rw [hL, hR]
    · rw [List.getElem?_set, if_neg hke, List.getElem?_eq_getElem hk',
        List.getElem?_eq_getElem hk2, List.getElem_map, List.getElem_map]
      have he : ¬(i = ids[k]'hk) := by
        intro hh
        have h2 : ids.idxOf (ids[k]'hk) = k := nodup_idxOf_eq hn hk
        rw [← hh] at h2
        exact hke h2
      simp only [List.getElem?_set, if_neg he]
  · rw [List.getElem?_set]
    by_cases hke : ids.idxOf i = k
    · exact absurd (hke ▸ hidx) hk
    · rw [if_neg hke, List.getElem?_eq_none
          (l := List.map (fun j => rs[j]?.getD (0, 0)) ids) (i := k)
          (by rw [hlen]; exact Nat.le_of_not_lt hk),
        List.getElem?_eq_none (l := List.map (fun j => (rs.set i v)[j]?.getD (0, 0)) ids)
          (i := k) (by rw [hlen2]; exact Nat.le_of_not_lt hk)]

theorem zip_map_map2 {α β γ : Type} (l : List α) (f : α → β) (g : α → γ) :
    (List.map f l).zip (List.map g l) = List.map (fun x => (f x, g x)) l := by
  induction l with
  | nil => rfl
  | cons a t ih => simpa using congrArg (fun r => (f a, g a) :: r) ih

theorem perm_mem_iff {α : Type} {u v : List α} :
    List.Perm u v → ∀ (a : α), a ∈ u ↔ a ∈ v := by
  intro pp
  induction pp with
  | nil => intro a; simp
  | cons b l ih => intro a; simp [List.mem_cons, ih]
  | swap b c l => intro a; simp [List.mem_cons, or_comm, or_left_comm]
  | trans _ _ h₁ h₂ => intro a; exact (h₁ a).trans (h₂ a)

theorem List.filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ a ∈ l, f a = g a) : List.filterMap f l = List.filterMap g l := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    rw [List.filterMap_cons, List.filterMap_cons, h a (by simp),
      ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

theorem filter_map_comm {α β : Type} (g : α → β) (q : β → Bool) :
    ∀ l : List α, List.filter q (l.map g) = (List.filter (fun a => q (g a)) l).map g := by
  intro l
  induction l with
  | nil => rfl
  | cons a t ih =>
    by_cases h : q (g a)
    · rw [List.map_cons, List.filter_cons, if_pos h, List.filter_cons, if_pos h, ih,
        List.map_cons]
    · rw [List.map_cons, List.filter_cons, if_neg h, List.filter_cons, if_neg h, ih]

theorem zip_succ {α β : Type} (xs : List α) : ∀ (ys : List β), xs.length = ys.length →
    ∀ (a : α) (b : β), (xs ++ [a]).zip (ys ++ [b]) = xs.zip ys ++ [(a, b)] := by
  induction xs with
  | nil =>
      intro ys hlen a b
      cases ys with
      | nil => rfl
      | cons y t => exact absurd hlen (by simp)
  | cons x t ih =>
      intro ys hlen a b
      cases ys with
      | nil => exact absurd hlen (by simp)
      | cons y t' =>
          have hlen' : t.length = t'.length := by simpa using hlen
          show (x, y) :: (t ++ [a]).zip (t' ++ [b]) = (x, y) :: (t.zip t' ++ [(a, b)])
          rw [ih t' hlen' a b]

theorem zip_map_range {α β : Type} (f : Nat → α) (g : Nat → β) :
    ∀ n : Nat, (List.range n).map (fun i => (f i, g i)) =
      ((List.range n).map f).zip ((List.range n).map g) := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih =>
      have key1 : List.map f (List.range n ++ [n]) = List.map f (List.range n) ++ [f n] := by
        rw [List.map_append, List.map_singleton]
      have key2 : List.map g (List.range n ++ [n]) = List.map g (List.range n) ++ [g n] := by
        rw [List.map_append, List.map_singleton]
      rw [List.range_succ, key1, key2, List.map_append, ih,
        zip_succ _ _ (by rw [List.length_map, List.length_map]), List.map_singleton]

theorem filterMap_if_some_id {α : Type} (p : α → Bool) :
    ∀ l : List α, List.filterMap (fun a => if p a then some a else none) l = List.filter p l := by
  intro l
  induction l with
  | nil => rfl
  | cons a t ih =>
    by_cases h : p a
    · rw [List.filterMap_cons, List.filter_cons, ih]
      simp [h]
    · rw [List.filterMap_cons, List.filter_cons, ih]
      simp [h]

theorem snd_lt_zipIdx (l : List α) : ∀ (s : Nat) p, p ∈ l.zipIdx s → p.2 < s + l.length := by
  induction l with
  | nil => intro s p hp; exact absurd hp (by simp)
  | cons a t ih =>
    intro s p hp
    rw [List.zipIdx_cons] at hp
    rcases List.mem_cons.1 hp with heq | hpt
    · subst heq
      show s < s + (a :: t).length
      rw [List.length_cons]
      omega
    · have h2 := ih (s + 1) p hpt
      have hlen : (a :: t).length = t.length + 1 := rfl
      omega

theorem nodup_zipIdx (l : List α) : ∀ (s : Nat), (l.zipIdx s).Nodup := by
  induction l with
  | nil => intro s; simp
  | cons a t ih =>
    intro s
    rw [List.zipIdx_cons]
    refine List.nodup_cons.2 ⟨fun h => ?_, ih (s + 1)⟩
    have hge : s + 1 ≤ (a, s).2 := snd_ge_zipIdx t (s + 1) (a, s) h
    omega

theorem sorted_unique_nat : ∀ (a b : List Nat), a.Perm b →
    List.Pairwise (· ≤ ·) a → List.Pairwise (· ≤ ·) b → a = b := by
  intro a
  induction a with
  | nil =>
    intro b hp _ _
    have : b = [] := List.perm_nil.mp hp.symm
    subst this
    rfl
  | cons x t ih =>
    intro b hp ha hb
    cases b with
    | nil => exact absurd hp.symm.length_eq (by simp)
    | cons y u =>
      have hx : x ∈ y :: u := hp.mem_iff.mp (by simp)
      have hy : y ∈ x :: t := hp.symm.mem_iff.mp (by simp)
      have hhead : x = y := by
        rcases List.mem_cons.1 hx with h | hx'
        · exact h
        · rcases List.mem_cons.1 hy with h | hy'
          · exact h.symm
          · have h1 := (List.pairwise_cons.1 ha).1 y hy'
            have h2 := (List.pairwise_cons.1 hb).1 x hx'
            exact Nat.le_antisymm h1 h2
      subst hhead
      obtain ⟨_, hper⟩ := List.cons_perm_iff_perm_erase.mp hp
      rw [List.erase_cons, if_pos (beq_self_eq_true x)] at hper
      refine congrArg (List.cons x) (ih u hper ?_ ?_)
      · exact (List.pairwise_cons.1 ha).2
      · exact (List.pairwise_cons.1 hb).2

theorem zLE_tag_antisym (key : Key) (x y : Row × Nat)
    (h₁ : List.zipIdxLE (leK key) x y = true) (h₂ : List.zipIdxLE (leK key) y x = true) :
    x.2 = y.2 := by
  by_cases e1 : leK key x.1 y.1
  · by_cases e2 : leK key y.1 x.1
    · rw [List.zipIdxLE, if_pos e1, if_pos e2] at h₁
      rw [List.zipIdxLE, if_pos e2, if_pos e1] at h₂
      simp only [decide_eq_true_eq] at h₁ h₂
      omega
    · rw [List.zipIdxLE, if_pos e1, if_neg e2] at h₁
      rw [List.zipIdxLE, if_neg e2] at h₂
      exact absurd h₂ (by simp)
  · rw [List.zipIdxLE, if_neg e1] at h₁
    exact absurd h₁ (by simp)

/-- Two lists tagged by distinct ordinals, both sorted for the tagged key order
of `perm`/`physical`, with the same elements, are equal. -/
theorem sorted_unique_zLE (key : Key) : ∀ (a b : List (Row × Nat)),
    (a.map Prod.snd).Nodup →
    List.Pairwise (List.zipIdxLE (leK key)) a →
    List.Pairwise (List.zipIdxLE (leK key)) b → a.Perm b → a = b := by
  intro a
  induction a with
  | nil =>
    intro b _ _ _ pp
    have : b = [] := List.perm_nil.mp pp.symm
    subst this
    rfl
  | cons x t ih =>
    intro b hn wa wb pp
    cases b with
    | nil => exact absurd pp.symm.length_eq (by simp)
    | cons y u =>
      rw [List.map_cons, List.nodup_cons] at hn
      obtain ⟨hn1, hn2⟩ := hn
      have hy : y ∈ x :: t := pp.symm.mem_iff.mp (by simp)
      have hhead : x = y := by
        obtain hyx | hyt := List.mem_cons.1 hy
        · exact hyx.symm
        · have hxy := (List.pairwise_cons.1 wa).1 y hyt
          have hx : x ∈ y :: u := pp.mem_iff.mp (by simp)
          obtain hxy2 | hxu := List.mem_cons.1 hx
          · exact hxy2
          · have hyx := (List.pairwise_cons.1 wb).1 x hxu
            have htag : x.2 = y.2 := zLE_tag_antisym key x y hxy hyx
            have hmem : x.2 ∈ t.map Prod.snd := by
              rw [htag]
              exact List.mem_map.2 ⟨y, hyt, rfl⟩
            exact absurd hmem hn1
      subst hhead
      obtain ⟨_, hper⟩ := List.cons_perm_iff_perm_erase.mp pp
      rw [List.erase_cons, if_pos (beq_self_eq_true x)] at hper
      refine congrArg (List.cons x) (ih u hn2 ?_ ?_ hper)
      · exact (List.pairwise_cons.1 wa).2
      · exact (List.pairwise_cons.1 wb).2

/-- Two lists of pairs with distinct first components, both sorted by the first
component, with the same elements, are equal. -/
theorem sorted_unique_fst : ∀ (a b : List (Nat × Nat)),
    (a.map Prod.fst).Nodup →
    List.Pairwise (fun x y : Nat × Nat => x.1 ≤ y.1) a →
    List.Pairwise (fun x y : Nat × Nat => x.1 ≤ y.1) b → a.Perm b → a = b := by
  intro a
  induction a with
  | nil =>
    intro b _ _ _ pp
    have : b = [] := List.perm_nil.mp pp.symm
    subst this
    rfl
  | cons x t ih =>
    intro b hn wa wb pp
    cases b with
    | nil => exact absurd pp.symm.length_eq (by simp)
    | cons y u =>
      rw [List.map_cons, List.nodup_cons] at hn
      obtain ⟨hn1, hn2⟩ := hn
      have hy : y ∈ x :: t := pp.symm.mem_iff.mp (by simp)
      have hhead : x = y := by
        obtain hyx | hyt := List.mem_cons.1 hy
        · exact hyx.symm
        · have h1 : x.1 ≤ y.1 := (List.pairwise_cons.1 wa).1 y hyt
          have hx : x ∈ y :: u := pp.mem_iff.mp (by simp)
          obtain hxy2 | hxu := List.mem_cons.1 hx
          · exact hxy2
          · have h2 : y.1 ≤ x.1 := (List.pairwise_cons.1 wb).1 x hxu
            have hfst : x.1 = y.1 := Nat.le_antisymm h1 h2
            have hmem : x.1 ∈ t.map Prod.fst := by
              rw [hfst]
              exact List.mem_map.2 ⟨y, hyt, rfl⟩
            exact absurd hmem hn1
      subst hhead
      obtain ⟨_, hper⟩ := List.cons_perm_iff_perm_erase.mp pp
      rw [List.erase_cons, if_pos (beq_self_eq_true x)] at hper
      refine congrArg (List.cons x) (ih u hn2 ?_ ?_ hper)
      · exact (List.pairwise_cons.1 wa).2
      · exact (List.pairwise_cons.1 wb).2

theorem zLE_shift (key : Key) (x y : Row × Nat) (m : Nat) :
    List.zipIdxLE (leK key) (x.1, x.2 + m) (y.1, y.2 + m) =
      List.zipIdxLE (leK key) x y := by
  rw [List.zipIdxLE, List.zipIdxLE]
  by_cases h1 : leK key x.1 y.1 <;> by_cases h2 : leK key y.1 x.1 <;>
    simp [h1, h2, Nat.add_le_add_iff_right]

theorem pairwise_map_shift (key : Key) (l : List (Row × Nat)) (m : Nat)
    (w : List.Pairwise (List.zipIdxLE (leK key)) l) :
    List.Pairwise (List.zipIdxLE (leK key)) (List.map (fun p : Row × Nat => (p.1, p.2 + m)) l) := by
  refine List.pairwise_map.2 (List.Pairwise.imp ?_ w)
  intro a b h
  show List.zipIdxLE (leK key) (a.1, a.2 + m) (b.1, b.2 + m) = true
  rw [zLE_shift key a b m]
  exact h

theorem pairwise_map_range_fst {β : Type} (g : Nat → β) : ∀ n : Nat,
    List.Pairwise (fun x y : Nat × β => x.1 ≤ y.1) ((List.range n).map (fun m => (m, g m))) := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.map_append, List.map_singleton]
    refine List.pairwise_append.mpr ⟨ih, ?_, ?_⟩
    · simp
    · intro x hx y hy
      obtain ⟨m, hm, rfl⟩ := List.mem_map.1 hx
      rw [List.mem_singleton] at hy
      subst hy
      exact Nat.le_of_lt (List.mem_range.1 hm)


theorem getD_of_lt {α : Type} (l : List α) (d : α) {i : Nat} (h : i < l.length) :
    l[i]?.getD d = l[i]'h := by
  rw [List.getElem?_eq_getElem (l := l) (i := i) h]
  rfl

theorem getD_of_ge {α : Type} (l : List α) (d : α) {i : Nat} (h : ¬ i < l.length) :
    l[i]?.getD d = d := by
  rw [List.getElem?_eq_none (l := l) (i := i) (Nat.le_of_not_lt h)]
  rfl

theorem filterMap_id_some (q : Nat → Bool) : ∀ l : List Nat,
    List.filterMap (fun w => if q w then some w else none) l = List.filter q l := by
  intro l
  induction l with
  | nil => rfl
  | cons a t ih =>
    by_cases h : q a
    · simp [h, ih]
    · simp [h, ih]

theorem pairwise_le_range : ∀ n : Nat, List.Pairwise (· ≤ ·) (List.range n) := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.pairwise_append]
    refine ⟨ih, List.pairwise_singleton (· ≤ ·) n, ?_⟩
    intro x hx y hy
    rw [List.mem_singleton] at hy
    subst hy
    exact Nat.le_of_lt (List.mem_range.1 hx)

theorem nodup_filter {l : List Nat} (h : l.Nodup) (q : Nat → Bool) :
    (List.filter q l).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
    obtain ⟨ha, ht⟩ := List.nodup_cons.1 h
    by_cases hq : q a
    · rw [List.filter_cons_of_pos hq]
      refine List.nodup_cons.2 ⟨fun hmem => ?_, ih ht⟩
      rw [List.mem_filter] at hmem
      exact ha hmem.1
    · rw [List.filter_cons_of_neg hq]
      exact ih ht

theorem nodup_map_inj {α : Type} {l : List α} (h : l.Nodup) (f : α → Nat)
    (hf : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) : (List.map f l).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
    obtain ⟨hna, ht⟩ := List.nodup_cons.1 h
    have iht : (List.map f t).Nodup :=
      ih ht (fun x hx y hy he => hf x (List.mem_cons.2 (Or.inr hx)) y
        (List.mem_cons.2 (Or.inr hy)) he)
    rw [List.map_cons, List.nodup_cons]
    refine ⟨fun hmem => ?_, iht⟩
    rw [List.mem_map] at hmem
    obtain ⟨x, hx, he⟩ := hmem
    have : x = a := hf x (List.mem_cons.2 (Or.inr hx)) a List.mem_cons_self he
    subst this
    exact hna hx

theorem perm_val_lt (key : Key) (rs : List Row) (i : Nat) (hi : i < (perm key rs).length) :
    (perm key rs)[i]'hi < rs.length :=
  (perm_mem key rs _).mp (List.getElem_mem hi)

theorem perm_map_range_getD (key : Key) (rs : List Row) :
    (List.range rs.length).map (fun i => (perm key rs)[i]?.getD 0) = perm key rs := by
  have hlen : (perm key rs).length = rs.length := perm_length key rs
  refine List.ext_getElem? (fun i => ?_)
  rcases Nat.lt_or_ge i rs.length with hi | hi
  · have hpc : i < (perm key rs).length := by rw [hlen]; exact hi
    have h1 : ((List.range rs.length).map (fun i => (perm key rs)[i]?.getD 0))[i]? =
        some ((perm key rs)[i]?.getD 0) := by
      rw [List.getElem?_map, List.getElem?_range hi]
      rfl
    rw [h1, getD_of_lt _ _ hpc, ← List.getElem?_eq_getElem (l := perm key rs) (i := i) hpc]
  · have h2 : ((List.range rs.length).map (fun i => (perm key rs)[i]?.getD 0))[i]? = none := by
      rw [List.getElem?_eq_none
        (l := (List.range rs.length).map (fun i => (perm key rs)[i]?.getD 0)) (i := i)]
      rw [List.length_map, List.length_range]
      exact hi
    rw [h2, List.getElem?_eq_none (l := perm key rs) (i := i)]
    rw [hlen]
    exact hi

theorem physical_length (key : Key) (rs : List Row) :
    (physical key rs).length = rs.length := by
  show ((perm key rs).map (fun i => rs[i]?.getD (0, 0))).length = _
  rw [List.length_map, perm_length]

theorem physical_getD (key : Key) (rs : List Row) (i : Nat)
    (hi : i < (perm key rs).length) :
    (physical key rs)[i]?.getD (0, 0) = rs[(perm key rs)[i]'hi]?.getD (0, 0) := by
  cases h : (perm key rs)[i]? with
  | some v =>
    have hv : (perm key rs)[i]'hi = v := by
      have := List.getElem?_eq_getElem (l := perm key rs) (i := i) hi
      rw [h] at this
      exact Option.some.inj this.symm
    subst hv
    show (List.map (fun j => rs[j]?.getD (0, 0)) (perm key rs))[i]?.getD (0, 0) = _
    rw [List.getElem?_map (l := perm key rs) (i := i), h]
    rfl
  | none =>
    exact absurd h (by
      rw [List.getElem?_eq_getElem (l := perm key rs) (i := i) hi]
      simp)

theorem physical_getD2 (key : Key) (rs : List Row) (i : Nat)
    (hi : i < (perm key rs).length) :
    (physical key rs)[i]?.getD (0, 0) = rs[(perm key rs)[i]?.getD 0]?.getD (0, 0) := by
  rw [getD_of_lt (perm key rs) 0 hi, physical_getD key rs i hi]

theorem lateGather_pt (key : Key) (rs : List Row) (project : Row → Nat) (i : Nat)
    (hi : i < rs.length) :
    (ordinal key rs i, ((physical key rs).map project)[i]?.getD 0) =
      ((perm key rs)[i]?.getD 0, project (rs[(perm key rs)[i]?.getD 0]?.getD (0, 0))) := by
  have hpc : i < (perm key rs).length := by rw [perm_length]; exact hi
  cases h : (perm key rs)[i]? with
  | some v =>
    have h3 : ((physical key rs).map project)[i]?.getD 0 = project (rs[v]?.getD (0, 0)) := by
      show ((List.map (fun j => rs[j]?.getD (0, 0)) (perm key rs)).map project)[i]?.getD 0 = _
      rw [List.getElem?_map, List.getElem?_map (l := perm key rs) (i := i), h]
      rfl
    rw [ordinal_eq key rs i hi, h]
    exact Prod.ext rfl h3
  | none =>
    exact absurd h (by
      rw [List.getElem?_eq_getElem (l := perm key rs) (i := i) hpc]
      simp)

theorem lateGather_gen (key : Key) (rs : List Row) (project : Row → Nat) :
    unravel key rs ((physical key rs).map project) = rs.map project := by
  have hgm := perm_map_range_getD key rs
  show (((List.range rs.length).map (fun i => (ordinal key rs i, ((physical key rs).map project)[i]?.getD 0))).mergeSort (fun a b => a.1 ≤ b.1)).map Prod.snd = rs.map project
  rw [k_map_congr (fun i hi => lateGather_pt key rs project i (List.mem_range.1 hi))]
  have hF : (List.range rs.length).map (fun i => ((perm key rs)[i]?.getD 0,
      project (rs[(perm key rs)[i]?.getD 0]?.getD (0, 0)))) =
      (perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))) := by
    have hfg : (fun i => ((perm key rs)[i]?.getD 0,
        project (rs[(perm key rs)[i]?.getD 0]?.getD (0, 0)))) =
        (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))) ∘
        (fun i : Nat => (perm key rs)[i]?.getD 0) := rfl
    rw [hfg, ← List.map_map, hgm]
  rw [hF]
  have hms : List.Perm
      (List.map Prod.fst
        (List.mergeSort
          ((perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))))
          (fun a b => a.1 ≤ b.1)))
      (List.map Prod.fst ((perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))))) :=
    List.Perm.map Prod.fst (List.mergeSort_perm _ _)
  have hmapfst : List.map Prod.fst
      ((perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0))))) = perm key rs := by
    rw [List.map_map]
    show List.map (fun w : Nat => (w, project (rs[w]?.getD (0, 0))).1) (perm key rs) = _
    exact (k_map_congr (fun w _ => rfl)).symm.trans (List.map_id _)
  have hnd : List.Nodup
      (List.map Prod.fst
        (List.mergeSort
          ((perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))))
          (fun a b => a.1 ≤ b.1))) := by
    have h2 : List.Nodup
        (List.map Prod.fst ((perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))))) := by
      rw [hmapfst]
      exact perm_nodup key rs
    exact hms.symm.nodup h2
  have hsort : List.mergeSort
      ((perm key rs).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))))
      (fun a b => a.1 ≤ b.1) =
      (List.range rs.length).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0)))) := by
    refine sorted_unique_fst _ _ hnd ?wa ?wb ?pp
    · refine (List.pairwise_mergeSort
        (fun (a b c : Nat × Nat) (h₁ : decide (a.1 ≤ b.1) = true)
          (h₂ : decide (b.1 ≤ c.1) = true) =>
          decide_eq_true (Nat.le_trans (of_decide_eq_true h₁) (of_decide_eq_true h₂)))
        (fun a b : Nat × Nat => by
          by_cases h : a.1 ≤ b.1
          · simp [h]
          · simp [h, Nat.le_of_not_le h]) _).imp
        (fun h => of_decide_eq_true h)
    · exact pairwise_map_range_fst (fun m => project (rs[m]?.getD (0, 0))) rs.length
    · exact (List.mergeSort_perm _ _).trans (List.Perm.map _ (perm_perm_range key rs))
  rw [hsort]
  have hsnd : List.map Prod.snd
      ((List.range rs.length).map (fun w : Nat => (w, project (rs[w]?.getD (0, 0))))) =
      rs.map project := by
    rw [List.map_map]
    show List.map (fun w : Nat => (w, project (rs[w]?.getD (0, 0))).2) (List.range rs.length) = _
    have h1 : (fun w : Nat => (w, project (rs[w]?.getD (0, 0))).2) =
        project ∘ (fun w : Nat => rs[w]?.getD (0, 0)) := rfl
    rw [h1, ← List.map_map, map_range_getD (l := rs) (d := (0, 0))]
  exact hsnd

theorem stableLimit_gen (key : Key) (rs : List Row) (p : Row → Bool) (k : Nat) :
    let ids := ((physical key rs).zipIdx.filterMap
      (fun (r, i) => if p r then some (ordinal key rs i) else none)).mergeSort
      (fun a b => a ≤ b)
    (ids.take k).map (fun j => rs[j]?.getD (0, 0)) = (rs.filter p).take k := by
  show ((((physical key rs).zipIdx.filterMap (fun (r, i) => if p r then some (ordinal key rs i) else none)).mergeSort (fun a b => a ≤ b)).take k).map (fun j => rs[j]?.getD (0, 0)) = (rs.filter p).take k
  have hpcall : ∀ i, i ∈ List.range rs.length →
      ((fun x => match x with
          | (r, i) => if p r then some (ordinal key rs i) else none)
          ((physical key rs)[i]?.getD (0, 0), 0 + i)) =
        (if p (rs[(perm key rs)[i]?.getD 0]?.getD (0, 0))
          then some ((perm key rs)[i]?.getD 0) else none) := by
    intro i hi
    have hlt : i < rs.length := List.mem_range.1 hi
    have hpc : i < (perm key rs).length := by rw [perm_length]; exact hlt
    rw [show (fun x => match x with
          | (r, i) => if p r then some (ordinal key rs i) else none)
          ((physical key rs)[i]?.getD (0, 0), 0 + i) =
        (if p ((physical key rs)[i]?.getD (0, 0)) then some (ordinal key rs (0 + i))
          else none) from rfl,
      Nat.zero_add, ordinal_eq key rs i hlt, physical_getD2 key rs i hpc]
  have hF : (physical key rs).zipIdx.filterMap
      (fun (r, i) => if p r then some (ordinal key rs i) else none) =
      List.filter (fun w => p (rs[w]?.getD (0, 0))) (perm key rs) := by
    rw [zipIdx_eq_map (l := physical key rs) (s := 0) (d := (0, 0)), physical_length key rs,
      List.filterMap_map]
    refine (List.filterMap_congr' hpcall).trans ?rest
    have h2 : (fun i : Nat => if p (rs[(perm key rs)[i]?.getD 0]?.getD (0, 0))
        then some ((perm key rs)[i]?.getD 0) else none) =
        (fun w : Nat => if p (rs[w]?.getD (0, 0)) then some w else none) ∘
        (fun i : Nat => (perm key rs)[i]?.getD 0) := rfl
    rw [h2, ← List.filterMap_map, perm_map_range_getD key rs]
    have h3 : (fun w : Nat => if p (rs[w]?.getD (0, 0)) then some w else none) =
        (fun w : Nat => if (fun v => p (rs[v]?.getD (0, 0))) w then some w else none) := rfl
    rw [h3, filterMap_id_some]
  have hsort : List.mergeSort
      (List.filter (fun w => p (rs[w]?.getD (0, 0))) (perm key rs))
      (fun a b => a ≤ b) =
      List.filter (fun w => p (rs[w]?.getD (0, 0))) (List.range rs.length) := by
    refine sorted_unique_nat _ _ ?pp ?pa ?pb
    · exact (List.mergeSort_perm _ _).trans (List.Perm.filter _ (perm_perm_range key rs))
    · refine (List.pairwise_mergeSort
        (fun (a b c : Nat) (h₁ : decide (a ≤ b) = true) (h₂ : decide (b ≤ c) = true) =>
          decide_eq_true (Nat.le_trans (of_decide_eq_true h₁) (of_decide_eq_true h₂)))
        (fun a b : Nat => by
          by_cases h : a ≤ b
          · simp [h]
          · simp [h, Nat.le_of_not_le h]) _).imp
        (fun h => of_decide_eq_true h)
    · exact List.Pairwise.filter _ (pairwise_le_range rs.length)
  rw [hF, hsort]
  rw [List.map_take]
  have hcoll : (List.filter (fun w => p (rs[w]?.getD (0, 0))) (List.range rs.length)).map (fun j => rs[j]?.getD (0, 0)) = rs.filter p := by
    have h1 : rs = (List.range rs.length).map (fun j => rs[j]?.getD (0, 0)) :=
      (map_range_getD (l := rs) (d := (0, 0))).symm
    rw [show rs.filter p =
        (List.map (fun j => rs[j]?.getD (0, 0)) (List.range rs.length)).filter p from
        congrArg (fun s => s.filter p) h1,
      List.filter_map]
    rfl
  rw [hcoll]

theorem sharedDelete_gen (key : Key) (rs : List Row) (ds : List Nat) :
    (((physical key rs).zipIdx.filter (fun (_,i) => !ds.contains (ordinal key rs i))).map Prod.fst).Perm
     ((rs.zipIdx.filter (fun (_,i) => !ds.contains i)).map Prod.fst) := by
  have hL : ((physical key rs).zipIdx.filter
      (fun (_,i) => !ds.contains (ordinal key rs i))).map Prod.fst =
      (List.filter (fun i => !ds.contains ((perm key rs)[i]?.getD 0)) (List.range rs.length)).map
        (fun i => (physical key rs)[i]?.getD (0, 0)) := by
    rw [zipIdx_eq_map (l := physical key rs) (s := 0) (d := (0, 0)), filter_map_comm,
      List.map_map, physical_length key rs]
    refine congrArg (List.map (fun i => (physical key rs)[i]?.getD (0, 0)))
      (List.filter_congr (fun i hi => by
        simp [Nat.zero_add, ordinal_eq key rs i (List.mem_range.1 hi)]))
  have h1 : List.map (fun i => (physical key rs)[i]?.getD (0, 0))
      (List.filter (fun i => !ds.contains ((perm key rs)[i]?.getD 0)) (List.range rs.length)) =
      List.map (fun i => rs[(perm key rs)[i]?.getD 0]?.getD (0, 0))
        (List.filter (fun i => !ds.contains ((perm key rs)[i]?.getD 0))
          (List.range rs.length)) :=
    k_map_congr (fun i hi =>
      physical_getD2 key rs i
        (by rw [perm_length]; exact List.mem_range.1 ((List.mem_filter.1 hi).1)))
  have h2 : List.filter (fun w => !ds.contains w) (perm key rs) =
      List.map (fun i => (perm key rs)[i]?.getD 0)
        (List.filter (fun i => !ds.contains ((perm key rs)[i]?.getD 0))
          (List.range rs.length)) := by
    rw [← perm_map_range_getD key rs, filter_map_comm, perm_map_range_getD key rs]
  have hL2 : (List.filter (fun i => !ds.contains ((perm key rs)[i]?.getD 0))
      (List.range rs.length)).map (fun i => (physical key rs)[i]?.getD (0, 0)) =
      (List.filter (fun w => !ds.contains w) (perm key rs)).map
        (fun w => rs[w]?.getD (0, 0)) := by
    rw [h2, List.map_map]
    refine k_map_congr (fun i hi => ?_)
    show (physical key rs)[i]?.getD (0, 0) = rs[(perm key rs)[i]?.getD 0]?.getD (0, 0)
    exact physical_getD2 key rs i
      (by rw [perm_length]; exact List.mem_range.1 ((List.mem_filter.1 hi).1))
  have hR : ((rs.zipIdx).filter (fun (_,i) => !ds.contains i)).map Prod.fst =
      (List.filter (fun i => !ds.contains i) (List.range rs.length)).map
        (fun i => rs[i]?.getD (0, 0)) := by
    rw [zipIdx_eq_map (l := rs) (s := 0) (d := (0, 0)), filter_map_comm, List.map_map]
    refine congrArg (List.map (fun i => rs[i]?.getD (0, 0)))
      (List.filter_congr (fun i _ => by rw [Nat.zero_add]))
  rw [hL, hL2, hR]
  exact List.Perm.map (fun w => rs[w]?.getD (0, 0))
    (List.Perm.filter (fun w => !ds.contains w) (perm_perm_range key rs))

theorem payloadAlignment_genN (key : Key) (rs : List Row) (payload : List Nat)
    (hlen : rs.length = payload.length) :
    ((physical key rs).zip ((perm key rs).map (fun i => payload[i]?.getD 0))).Perm
      (rs.zip payload) := by
  have h1 : physical key rs = (perm key rs).map (fun i => rs[i]?.getD (0, 0)) := rfl
  rw [h1, zip_map_map2 (perm key rs) _ _]
  have h2 : (List.range rs.length).map (fun i => (rs[i]?.getD (0, 0), payload[i]?.getD 0)) =
      rs.zip payload := by
    rw [← zip_map_map2 (List.range rs.length) _ _, map_range_getD (l := rs) (d := (0, 0)),
      hlen,
      map_range_getD (l := payload) (d := (0 : Nat))]
  exact (List.Perm.map _ (perm_perm_range key rs)).trans (List.Perm.of_eq h2)

theorem beq_self_true (x : Nat) : (x == x) = true := by
  rw [show (x == x) = Nat.beq x x from rfl, Nat.beq_eq]

theorem idxOf_lt_length (l : List Nat) (a : Nat) (ha : a ∈ l) : l.idxOf a < l.length := by
  induction l with
  | nil => exact absurd ha (by simp)
  | cons x t ih =>
      cases hxa : (x == a) with
      | true =>
          rw [List.idxOf_cons, hxa]
          show 0 < (x :: t).length
          exact Nat.succ_pos _
      | false =>
          rw [List.idxOf_cons, hxa]
          show t.idxOf a + 1 < (x :: t).length
          have ht : a ∈ t := by
            rcases List.mem_cons.1 ha with haxx | hat
            · rw [haxx] at hxa
              rw [beq_self_true x] at hxa
              simp at hxa
            · exact hat
          exact Nat.add_lt_add_right (ih ht) 1

theorem idxOf_eq_of_getElem (l : List Nat) (a : Nat) (nd : l.Nodup) (k : Nat)
    (hk : k < l.length) (h : l[k] = a) : l.idxOf a = k := by
  induction l generalizing k with
  | nil => exact absurd hk (by simp)
  | cons x t ih =>
      cases hxa : (x == a) with
      | true =>
          have hxa' : x = a := Nat.eq_of_beq_eq_true hxa
          cases k with
          | zero => rw [List.idxOf_cons, hxa]; simp
          | succ j =>
              exfalso
              have hj : j < t.length := by simpa using hk
              have hat : a ∈ t := by
                rw [← h, List.getElem_cons_succ x t j hk]
                exact List.getElem_mem (l := t) (n := j) hj
              exact (List.nodup_cons.mp nd).1 (by rw [hxa']; exact hat)
      | false =>
          rw [List.idxOf_cons, hxa]
          show t.idxOf a + 1 = k
          cases k with
          | zero =>
              exfalso
              exact absurd (by simpa using h)
                (fun hh => by rw [hh] at hxa; rw [beq_self_true a] at hxa; simp at hxa)
          | succ j =>
              have hj : j < t.length := by simpa using hk
              rw [ih (List.nodup_cons.mp nd).2 j hj
                (by rw [← List.getElem_cons_succ x t j hk]; exact h)]

theorem sidecarBudget_gen (key : Key) : ∀ rs,
    (layoutBytes key rs < both rs ↔
      both (physical key rs) + orderBytes rs.length + 1 < both rs) := by
  intro rs
  show (1 + (side key rs).length + both (physical key rs) < both rs) ↔ _
  rw [side_len key rs]
  omega

theorem patchByOrdinal_gen (key : Key) : ∀ rs i v, i < rs.length →
    let ids := perm key rs
    (physical key rs).set (ids.idxOf i) v = ids.map (fun j => (rs.set i v)[j]?.getD (0, 0)) := by
  intro rs i v hi
  show ((perm key rs).map (fun j => rs[j]?.getD (0, 0))).set ((perm key rs).idxOf i) v = _
  refine List.ext_getElem (by rw [List.length_set, List.length_map, perm_length,
    List.length_map, perm_length]) (fun k hs hxg => ?_)
  have hb : (perm key rs).idxOf i < (perm key rs).length :=
    idxOf_lt_length _ _ ((perm_mem key rs i).mpr hi)
  have hsmap : k < (List.map (fun j => rs[j]?.getD (0, 0)) (perm key rs)).length := by
    rw [← List.length_set]
    exact hs
  rw [List.getElem_set (l := (perm key rs).map (fun j => rs[j]?.getD (0, 0))) (i := (perm key rs).idxOf i)
    (a := v) (j := k) hs]
  rw [List.getElem_map (fun j => rs[j]?.getD (0, 0)) (l := perm key rs) (i := k) (h := hsmap)]
  rw [List.getElem_map (fun j => (rs.set i v)[j]?.getD (0, 0)) (l := perm key rs) (i := k) (h := hxg)]
  rw [List.length_map (fun j => rs[j]?.getD (0, 0))] at hsmap
  by_cases hp : (perm key rs).idxOf i = k
  · subst hp
    rw [if_pos rfl, List.getElem_idxOf hb]
    have : (rs.set i v)[i]? = some v := by
      rw [List.getElem?_set]
      simp [hi]
    rw [this]
    rfl
  · rw [if_neg hp]
    have hne : (perm key rs)[k] ≠ i := by
      intro heq
      have := idxOf_eq_of_getElem (perm key rs) i (perm_nodup key rs) k hsmap heq
      exact hp this
    rw [show (rs.set i v)[(perm key rs)[k]]? = rs[(perm key rs)[k]]? from by
      rw [List.getElem?_set, if_neg (fun hh => hne hh.symm)]]

/-! Chunk F: mergeShards via tagged sorted-permutation uniqueness. -/

theorem phys_sort_tagged (key : Key) (l : List Row) :
    ((l.zipIdx).mergeSort (List.zipIdxLE (leK key))).map Prod.fst = physical key l :=
  (List.mergeSort_zipIdx (le := leK key)).trans (phys_eq_sort key l).symm

theorem shift_fst (m : Nat) (l : List (Row × Nat)) :
    List.map Prod.fst (List.map (fun p : Row × Nat => (p.1, p.2 + m)) l) = l.map Prod.fst := by
  induction l with
  | nil => rfl
  | cons p t ih => rw [List.map_cons, List.map_cons, ih, List.map_cons]

theorem zipIdx_shift {α : Type} : ∀ (b : List α) (s n : Nat),
    b.zipIdx (s + n) = (b.zipIdx s).map (fun p : α × Nat => (p.1, p.2 + n)) := by
  intro b
  induction b with
  | nil => intro s n; rfl
  | cons a t ih =>
    intro s n
    have e1 : (a :: t).zipIdx (s + n) = (a, s + n) :: t.zipIdx (s + n + 1) := rfl
    have e2 : (a :: t).zipIdx s = (a, s) :: t.zipIdx (s + 1) := rfl
    have e3 : s + n + 1 = s + 1 + n := by omega
    rw [e1, e2, List.map_cons, e3, ih]

theorem shift_mergeSort (key : Key) (m : Nat) (l : List (Row × Nat)) :
    (List.map (fun p : Row × Nat => (p.1, p.2 + m)) l).mergeSort (List.zipIdxLE (leK key)) =
      List.map (fun p : Row × Nat => (p.1, p.2 + m)) (l.mergeSort (List.zipIdxLE (leK key))) := by
  rw [← List.map_mergeSort (s := List.zipIdxLE (leK key))
    (fun a _ b _ => (zLE_shift key a b m).symm)]

theorem mergeShards_gen (key : Key) (a b : List Row) :
    List.merge (physical key a) (physical key b)
      (fun x y => let a := key x; let b := key y;
        a.1 < b.1 || (a.1 == b.1 && a.2 ≤ b.2)) =
      physical key (a ++ b) := by
  show List.merge (physical key a) (physical key b) (leK key) = physical key (a ++ b)
  let z := List.zipIdxLE (leK key)
  let X := a.zipIdx
  let Y := (b.zipIdx).map (fun p : Row × Nat => (p.1, p.2 + a.length))
  have hpa : physical key a = (X.mergeSort z).map Prod.fst := (phys_sort_tagged key a).symm
  have hpb : physical key b = (Y.mergeSort z).map Prod.fst := by
    rw [shift_mergeSort key a.length b.zipIdx, shift_fst]
    exact (phys_sort_tagged key b).symm
  have happ : (a ++ b).zipIdx = X ++ Y := by
    rw [List.zipIdx_append,
      zipIdx_shift (b := b) (s := 0) (n := a.length)]
  have hpab : physical key (a ++ b) = ((X ++ Y).mergeSort z).map Prod.fst := by
    rw [← happ, phys_sort_tagged]
  rw [hpa, hpb, hpab]
  have htag : ∀ x y, x ∈ X.mergeSort z → y ∈ Y.mergeSort z → x.2 ≤ y.2 := by
    intro x y hx hy
    have h1 := snd_lt_zipIdx a 0 x (List.mem_mergeSort.1 hx)
    rw [List.mem_mergeSort, List.mem_map] at hy
    obtain ⟨q, hq, rfl⟩ := hy
    show x.2 ≤ q.2 + a.length
    omega
  rw [← List.merge_stable (le := leK key) _ _ htag]
  congr 1
  have hwX : List.Pairwise z (X.mergeSort z) :=
    List.pairwise_mergeSort (List.zipIdxLE_trans (leK_trans key))
      (List.zipIdxLE_total (leK_total key)) X
  have hwY : List.Pairwise z (Y.mergeSort z) := by
    rw [shift_mergeSort key a.length b.zipIdx]
    refine pairwise_map_shift key (b.zipIdx.mergeSort z) a.length ?_
    exact List.pairwise_mergeSort (List.zipIdxLE_trans (leK_trans key))
      (List.zipIdxLE_total (leK_total key)) b.zipIdx
  have hpermXY : List.Perm ((X.mergeSort z).merge (Y.mergeSort z) z) (X ++ Y) :=
    (List.merge_perm_append z).trans
      (List.Perm.append (List.mergeSort_perm _ _) (List.mergeSort_perm _ _))
  have hndXY : ((X ++ Y).map Prod.snd).Nodup := by
    rw [← happ, List.zipIdx_map_snd 0]
    exact List.nodup_range' 1 (by decide)
  have hndM : (List.map Prod.snd ((X.mergeSort z).merge (Y.mergeSort z) z)).Nodup :=
    (List.Perm.map Prod.snd hpermXY).symm.nodup hndXY
  refine sorted_unique_zLE key _ _ hndM ?_ ?_ ?_
  · exact List.pairwise_merge (List.zipIdxLE_trans (leK_trans key))
      (List.zipIdxLE_total (leK_total key)) _ _ hwX hwY
  · exact List.pairwise_mergeSort (List.zipIdxLE_trans (leK_trans key))
      (List.zipIdxLE_total (leK_total key)) (X ++ Y)
  · exact hpermXY.trans (List.mergeSort_perm _ _).symm

/-! Chunk G: twoColumnWin refutation with the sorted 2-row witness. -/

theorem twoColumnWin_not (key : Key) (hperm : perm key [(0, 0), (1, 1)] = [0, 1]) :
    ¬ twoColumnWin key := by
  intro h
  have hpe : physical key [(0, 0), (1, 1)] = [(0, 0), (1, 1)] := by
    unfold physical
    rw [hperm]
    decide
  have hsb : (side key [(0, 0), (1, 1)]).length = 1 := by
    unfold side
    rw [hperm]
    native_decide
  have h1 := h [(0, 0), (1, 1)]
    (by native_decide : 1 < (List.eraseDups [(0, 0), (1, 1)] : List Row).length)
  rw [layoutBytes, hpe, hsb] at h1
  omega

/-! Part X: xor / Gray-code helpers (GrayPair, XorClass models). -/

def gg (x : Nat) : Nat := x ^^^ x / 2

theorem xor_decomp (a b : Nat) :
    a ^^^ b = (a % 2 + b % 2) % 2 + 2 * (a / 2 ^^^ b / 2) := by
  have hp : a % 2 ^^^ b % 2 = (a % 2 + b % 2) % 2 := by
    have ha : a % 2 = 0 ∨ a % 2 = 1 := by omega
    have hb : b % 2 = 0 ∨ b % 2 = 1 := by omega
    rcases ha with h | h <;> rcases hb with h' | h' <;> rw [h, h'] <;> rfl
  calc a ^^^ b
      = 2 * ((a ^^^ b) / 2) + (a ^^^ b) % 2 := (Nat.div_add_mod (a ^^^ b) 2).symm
    _ = 2 * (a / 2 ^^^ b / 2) + (a % 2 ^^^ b % 2) := by
      have hm : (a ^^^ b) % 2 = a % 2 ^^^ b % 2 := by
        have := Nat.xor_mod_two_pow (n := 1) (a := a) (b := b)
        simpa using this
      rw [Nat.xor_div_two, hm]
    _ = 2 * (a / 2 ^^^ b / 2) + (a % 2 + b % 2) % 2 := by rw [hp]
    _ = (a % 2 + b % 2) % 2 + 2 * (a / 2 ^^^ b / 2) := Nat.add_comm _ _

theorem xor_eq_zero_aux :
    ∀ (k : Nat) (a b : Nat), a + b ≤ k → a ^^^ b = 0 → a = b := by
  intro k
  induction k with
  | zero => intro a b _ _; omega
  | succ k ih =>
    intro a b hab h
    rw [xor_decomp] at h
    have hq : a / 2 ^^^ b / 2 = 0 := by omega
    have hpar : a % 2 = b % 2 := by
      have h0 : (a % 2 + b % 2) % 2 = 0 := by omega
      have ha : a % 2 = 0 ∨ a % 2 = 1 := by omega
      have hb : b % 2 = 0 ∨ b % 2 = 1 := by omega
      rcases ha with h1 | h1 <;> rcases hb with h2 | h2 <;> simp [h1, h2] at h0 ⊢ <;> omega
    have hle : a / 2 + b / 2 ≤ k := by omega
    have := ih (a / 2) (b / 2) hle hq
    omega

theorem xor_eq_zero (a b : Nat) : a ^^^ b = 0 ↔ a = b := by
  constructor
  · exact xor_eq_zero_aux (a + b) a b (by omega)
  · intro h
    subst h
    exact Nat.xor_self a

theorem gg_eq_zero (n : Nat) : gg n = 0 → n = 0 := by
  intro h
  have := (xor_eq_zero n (n / 2)).mp h
  omega

theorem gg_double (q : Nat) :
    gg (2 * q) = (if q % 2 = 1 then 1 else 0) + 2 * gg q := by
  unfold gg
  have h3 : (2 * q) / 2 = q := by omega
  rw [h3]
  have h : (2 * q) ^^^ q = q % 2 + 2 * (q ^^^ q / 2) := by
    rw [xor_decomp]
    have h1 : (2 * q) % 2 = 0 := by omega
    rw [h1, h3, Nat.zero_add, Nat.mod_mod]
  rw [h]
  by_cases hq : q % 2 = 1 <;> simp [hq] <;> omega

theorem gg_double_add_one (q : Nat) :
    gg (2 * q + 1) = (if q % 2 = 1 then 0 else 1) + 2 * gg q := by
  unfold gg
  have h2 : (2 * q + 1) / 2 = q := by omega
  rw [h2]
  have h : (2 * q + 1) ^^^ q = (1 + q % 2) % 2 + 2 * (q ^^^ q / 2) := by
    rw [xor_decomp]
    have h1 : (2 * q + 1) % 2 = 1 := by omega
    rw [h1, h2]
  rw [h]
  by_cases hq : q % 2 = 1 <;> simp [hq] <;> omega

theorem xor_two (a b : Nat) : (2 * a) ^^^ (2 * b) = 2 * (a ^^^ b) := by
  rw [xor_decomp]
  have h1 : (2 * a) % 2 = 0 := by omega
  have h2 : (2 * b) % 2 = 0 := by omega
  have h3 : (2 * a) / 2 = a := by omega
  have h4 : (2 * b) / 2 = b := by omega
  rw [h1, h2, h3, h4]
  omega

theorem xor_two_add_one (a b : Nat) : (1 + 2 * a) ^^^ (1 + 2 * b) = 2 * (a ^^^ b) := by
  rw [xor_decomp]
  have h1 : (1 + 2 * a) % 2 = 1 := by omega
  have h2 : (1 + 2 * b) % 2 = 1 := by omega
  have h3 : (1 + 2 * a) / 2 = a := by omega
  have h4 : (1 + 2 * b) / 2 = b := by omega
  rw [h1, h2, h3, h4]
  rw [show (1 + 1) % 2 = 0 by decide, Nat.zero_add]

theorem gg_xor_odd (q : Nat) : gg (2 * q) ^^^ gg (2 * q + 1) = 1 := by
  rw [gg_double, gg_double_add_one]
  by_cases hq : q % 2 = 1
  · rw [if_pos hq, if_pos hq, Nat.zero_add]
    rw [xor_decomp]
    have h1 : (1 + 2 * gg q) % 2 = 1 := by omega
    have h2 : (2 * gg q) % 2 = 0 := by omega
    have h3 : (1 + 2 * gg q) / 2 = gg q := by omega
    have h4 : (2 * gg q) / 2 = gg q := by omega
    rw [h1, h2, h3, h4, Nat.xor_self, Nat.mul_zero]
  · rw [if_neg hq, if_neg hq, Nat.zero_add]
    rw [xor_decomp]
    have h1 : (2 * gg q) % 2 = 0 := by omega
    have h2 : (1 + 2 * gg q) % 2 = 1 := by omega
    have h3 : (2 * gg q) / 2 = gg q := by omega
    have h4 : (1 + 2 * gg q) / 2 = gg q := by omega
    rw [h1, h2, h3, h4, Nat.xor_self, Nat.mul_zero]

theorem gg_xor_even (q : Nat) :
    gg (2 * q + 1) ^^^ gg (2 * q + 2) = 2 * (gg q ^^^ gg (q + 1)) := by
  have hrw : 2 * q + 2 = 2 * (q + 1) := by omega
  rw [hrw, gg_double_add_one, gg_double]
  by_cases hq : q % 2 = 1
  · rw [if_pos hq, if_neg (by omega : ¬(q + 1) % 2 = 1),
      Nat.zero_add, Nat.zero_add]
    exact xor_two (gg q) (gg (q + 1))
  · rw [if_neg hq, if_pos (by omega : (q + 1) % 2 = 1)]
    exact xor_two_add_one (gg q) (gg (q + 1))

theorem parity_trich (x : Nat) :
    x = 0 ∨ (∃ q, x = 2 * q + 1) ∨ (∃ q, x = 2 * q + 2) := by
  match x with
  | 0 => exact Or.inl rfl
  | Nat.succ x0 =>
    rcases (by omega : x0 % 2 = 0 ∨ x0 % 2 = 1) with hp | hp
    · exact Or.inr (Or.inl ⟨x0 / 2, by omega⟩)
    · exact Or.inr (Or.inr ⟨x0 / 2, by omega⟩)

theorem gg_pair_aux : ∀ k x : Nat, x ≤ k → ∃ b, gg x ^^^ gg (x + 1) = 2 ^ b := by
  intro k
  induction k with
  | zero =>
    intro x hx
    have h0 : x = 0 := by omega
    subst h0
    exact ⟨0, by unfold gg; rfl⟩
  | succ k ih =>
    intro x hx
    rcases parity_trich x with h | h | h
    · subst h
      exact ⟨0, by unfold gg; rfl⟩
    · rcases h with ⟨q, rfl⟩
      obtain ⟨b, hb⟩ := ih q (by omega)
      refine ⟨b + 1, ?_⟩
      rw [gg_xor_even, hb, Nat.mul_comm, Nat.pow_succ]
    · rcases h with ⟨q, rfl⟩
      have he : 2 * q + 2 = 2 * (q + 1) := by omega
      rw [he, gg_xor_odd]
      exact ⟨0, rfl⟩

theorem gg_d : ∀ x : Nat, ∃ b, gg x ^^^ gg (x + 1) = 2 ^ b :=
  fun x => gg_pair_aux x x (by omega)

theorem gg_inj_aux : ∀ k x a : Nat, x ≤ k → a ≤ k → gg x = gg a → x = a := by
  intro k
  induction k with
  | zero => intro x a hx ha _; omega
  | succ k ih =>
    intro x a hx ha hg
    have e0 : gg 0 = 0 := by unfold gg; rfl
    rcases parity_trich x with h | h | h
    · subst h
      rw [e0] at hg
      exact (gg_eq_zero a hg.symm).symm
    · rcases h with ⟨q, rfl⟩
      rw [gg_double_add_one] at hg
      rcases parity_trich a with h2 | h2 | h2
      · subst h2
        rw [e0] at hg
        have hq0 : gg q = 0 := by omega
        have he : q = 0 := gg_eq_zero q hq0
        rcases (by omega : q % 2 = 0 ∨ q % 2 = 1) with hq | hq
        · subst he
          simp [hq] at hg
        · subst he
          omega
      · rcases h2 with ⟨p, rfl⟩
        rw [gg_double_add_one] at hg
        by_cases hq : q % 2 = 1 <;> by_cases hp : p % 2 = 1 <;> simp [hq, hp] at hg ⊢ <;>
          (try omega) <;>
          (try { have h1 : gg q = gg p := by omega
                 have h3 : q = p := ih q p (by omega) (by omega) h1
                 omega })
      · rcases h2 with ⟨p, rfl⟩
        rw [show 2 * p + 2 = 2 * (p + 1) by omega, gg_double] at hg
        by_cases hq : q % 2 = 1 <;> by_cases hp : (p + 1) % 2 = 1 <;> simp [hq, hp] at hg ⊢ <;>
          (try omega) <;>
          (try { have h1 : gg q = gg (p + 1) := by omega
                 have h3 : q = p + 1 := ih q (p + 1) (by omega) (by omega) h1
                 omega })
    · rcases h with ⟨q, rfl⟩
      rw [show 2 * q + 2 = 2 * (q + 1) by omega, gg_double] at hg
      rcases parity_trich a with h2 | h2 | h2
      · subst h2
        rw [e0] at hg
        have hq0 : gg (q + 1) = 0 := by omega
        have hz : q + 1 = 0 := gg_eq_zero (q + 1) hq0
        omega
      · rcases h2 with ⟨p, rfl⟩
        rw [gg_double_add_one] at hg
        by_cases hq : (q + 1) % 2 = 1 <;> by_cases hp : p % 2 = 1 <;> simp [hq, hp] at hg ⊢ <;>
          (try omega) <;>
          (try { have h1 : gg (q + 1) = gg p := by omega
                 have h3 : q + 1 = p := ih (q + 1) p (by omega) (by omega) h1
                 omega })
      · rcases h2 with ⟨p, rfl⟩
        rw [show 2 * p + 2 = 2 * (p + 1) by omega, gg_double] at hg
        by_cases hq : (q + 1) % 2 = 1 <;> by_cases hp : (p + 1) % 2 = 1 <;> simp [hq, hp] at hg ⊢ <;>
          (try omega) <;>
          (try { have h1 : gg (q + 1) = gg (p + 1) := by omega
                 have h3 : q + 1 = p + 1 := ih (q + 1) (p + 1) (by omega) (by omega) h1
                 omega })

theorem gg_inj (x a : Nat) (h : gg x = gg a) : x = a :=
  gg_inj_aux (x + a) x a (by omega) (by omega) h

theorem key_gg_injective (x a : Row) (h : (x.1 ^^^ x.1 / 2, x.2) = (a.1 ^^^ a.1 / 2, a.2)) :
    x = a := by
  cases x with
  | mk x1 x2 =>
    cases a with
    | mk a1 a2 =>
      have h1 : gg x1 = gg a1 := by simpa [gg] using congrArg Prod.fst h
      have h2 : x2 = a2 := congrArg Prod.snd h
      rw [Prod.mk.injEq]
      exact ⟨gg_inj x1 a1 h1, h2⟩

theorem beqT {α : Type} [BEq α] [LawfulBEq α] {a b : α} (h : a = b) : (a == b) = true := by
  subst h
  exact beq_self_eq_true _

theorem beqF {α : Type} [BEq α] [LawfulBEq α] {a b : α} (h : ¬(a = b)) : (a == b) = false := by
  rw [Bool.eq_false_iff]
  intro hh
  exact h (eq_of_beq hh)

theorem count_map_inj {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β] {l : List α} (f : α → β) (a : α)
    (hf : Function.Injective f) : (l.map f).count (f a) = l.count a := by
  induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.map_cons, List.count_cons, List.count_cons, ih]
    have hb : (f x == f a) = (x == a) := by
      by_cases h : (x == a)
      · have hxy : x = a := eq_of_beq h
        subst hxy
        rw [beq_self_eq_true, beq_self_eq_true]
      · have hfa : ¬(f x = f a) := fun he => h (beqT (hf he))
        rw [beqF hfa, beqF (fun he => h (beqT he))]
    cases hx : (x == a) <;> simp [hx, hb]

theorem count_map_perm_eq {α β : Type} [BEq β] [LawfulBEq β] {l m : List α}
    (h : l.Perm m) (f : α → β) (a : β) :
    (l.map f).count a = (m.map f).count a := by
  rw [List.count_eq_countP, List.count_eq_countP]
  exact List.Perm.countP_eq (fun x => x == a) (List.Perm.map f h)

theorem zeroXor (a b : Nat) : (a^^^b == (0 : Nat)) = (a == b) := by
  by_cases hab : a = b
  · subst hab
    rw [Nat.xor_self, beq_self_eq_true, beq_self_eq_true]
  · have h1 : (a^^^b == (0 : Nat)) = false := by
      cases h : (a^^^b == (0 : Nat))
      · first | rfl | (exfalso; exact hab ((xor_eq_zero a b).mp (Nat.eq_of_beq_eq_true h)))
      · first | rfl | (exfalso; exact hab ((xor_eq_zero a b).mp (Nat.eq_of_beq_eq_true h)))
    have h2 : (a == b) = false := by
      cases h : (a == b)
      · first | rfl | (exfalso; exact hab (Nat.eq_of_beq_eq_true h))
      · first | rfl | (exfalso; exact hab (Nat.eq_of_beq_eq_true h))
    rw [h1, h2]

theorem sound_LateGather : XorClass.LateGather.Goal := lateGather_gen XorClass.key
theorem sound_StableLimit : XorClass.StableLimit.Goal := stableLimit_gen XorClass.key
theorem sound_SharedDelete : XorClass.SharedDelete.Goal := sharedDelete_gen XorClass.key
theorem sound_PayloadAlignment : XorClass.PayloadAlignment.Goal := payloadAlignment_genN XorClass.key
theorem sound_SidecarBudget : XorClass.SidecarBudget.Goal := sidecarBudget_gen XorClass.key
theorem sound_PatchByOrdinal : XorClass.PatchByOrdinal.Goal := patchByOrdinal_gen XorClass.key
theorem sound_MergeShards : XorClass.MergeShards.Goal := mergeShards_gen XorClass.key
theorem disproof_TwoColumnWin : ¬XorClass.TwoColumnWin.Goal :=
  twoColumnWin_not XorClass.key (by native_decide : perm XorClass.key [(0, 0), (1, 1)] = [0, 1])

theorem sound_ZeroClass : XorClass.ZeroClass.Goal := by
  intro rs
  refine Eq.trans
    (takeWhile_eqP_countP (physical XorClass.key rs) rs (fun p => (XorClass.key p).1)
      (List.pairwise_map.mp
        (pairwise_col_sort XorClass.key rs (fun p => (XorClass.key p).1) (fun _ => rfl)))
      (physical_perm XorClass.key rs))
    ?_
  refine congrArg (fun g : Row → Bool => rs.countP g) ?_
  funext p
  rcases p with ⟨a, b⟩
  show ((XorClass.key (a, b)).fst == 0) = (a == b)
  rw [show (XorClass.key (a, b)).fst = a ^^^ b from rfl]
  exact zeroXor a b

theorem sound_DecodeRight : XorClass.DecodeRight.Goal := by
  intro x y
  show (x ^^^ y) ^^^ x = y
  rw [Nat.xor_comm x y, Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

end TinylambOpt.ResearchProofs.StorageExecution.ClusterOrder.XorClass
