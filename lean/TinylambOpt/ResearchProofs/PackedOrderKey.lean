import TinylambOpt.ResearchGoals.PackedOrderKey

namespace TinylambOpt.ResearchProofs.PackedOrderKey

open TinylambOpt.ResearchGoals.PackedOrderKey

/-- The normalized major-key component of `encode` for a `some` major, as a raw 16-bit value. -/
private def P (x : BitVec 16) : Nat := (~~~(x ^^^ 0x8000)).toNat

private theorem P_lt_two_pow (x : BitVec 16) : P x < 2 ^ 16 := by
  unfold P
  exact BitVec.isLt _

private theorem decide_congr' {p q : Prop} [Decidable p] [Decidable q] (h : p ↔ q) :
    decide p = decide q := by
  by_cases hp : p
  · by_cases hq : q
    · rw [decide_eq_true hp, decide_eq_true hq]
    · exact absurd (h.mp hp) hq
  · by_cases hq : q
    · exact absurd (h.mpr hq) hp
    · rw [decide_eq_false hp, decide_eq_false hq]

/-- Xor with a power of two whose bit is unset just sets it. -/
private theorem xor_two_pow_eq_or {x i : Nat} (h : x < 2 ^ i) :
    x ^^^ 2 ^ i = x ||| 2 ^ i := by
  refine Nat.eq_of_testBit_eq (fun j => ?_)
  rw [Nat.testBit_xor, Nat.testBit_or, Nat.testBit_two_pow]
  cases hd : decide (i = j) with
  | true =>
      have hij : i = j := of_decide_eq_true hd
      subst hij
      have htb : x.testBit i = false := Nat.testBit_lt_two_pow h
      rw [htb]
      rfl
  | false =>
      simp

/-- Xor with a power of two clears the bit that `|||` had just set. -/
private theorem or_two_pow_xor {y i : Nat} (h : y < 2 ^ i) :
    (y ||| 2 ^ i) ^^^ 2 ^ i = y := by
  refine Nat.eq_of_testBit_eq (fun j => ?_)
  rw [Nat.testBit_xor, Nat.testBit_or, Nat.testBit_two_pow]
  cases hd : decide (i = j) with
  | true =>
      have hij : i = j := of_decide_eq_true hd
      subst hij
      have htb : y.testBit i = false := Nat.testBit_lt_two_pow h
      rw [htb]
      rfl
  | false =>
      simp

private theorem add_two_pow_eq_or {b i : Nat} (h : b < 2 ^ i) :
    b + 2 ^ i = b ||| 2 ^ i := by
  have := Nat.two_pow_add_eq_or_of_lt (i := i) h 1
  simp only [Nat.mul_one] at this
  rw [Nat.add_comm, Nat.or_comm, this]

private theorem or_two_pow_eq_add {b i : Nat} (h : b < 2 ^ i) :
    b ||| 2 ^ i = b + 2 ^ i := (add_two_pow_eq_or h).symm

/-- The core flip: flipping the sign bit turns the signed value into the unsigned
value shifted by `2^15`, so reversed unsigned order equals signed order. -/
private theorem xor_flip_int (x : BitVec 16) :
    ((x.toNat ^^^ 2 ^ 15 : Nat) : Int) = x.toInt + 2 ^ 15 := by
  by_cases hmsb : x.msb = true
  · have hge := BitVec.le_toNat_of_msb_true hmsb
    have hlt : x.toNat < 2 ^ 16 := BitVec.isLt x
    obtain ⟨y, hy, heq⟩ : ∃ y, y < 2 ^ 15 ∧ x.toNat = y + 2 ^ 15 :=
      ⟨x.toNat - 2 ^ 15, by omega, by omega⟩
    have hxor : x.toNat ^^^ 2 ^ 15 = y := by
      rw [heq, add_two_pow_eq_or hy, or_two_pow_xor hy]
    have hti : x.toInt = (x.toNat : Int) - (2 ^ 16 : Nat) := by
      rw [BitVec.toInt_eq_msb_cond, ite_eq_left hmsb]
    rw [hxor, hti, heq]
    omega
  · have hf : x.msb = false := by
      cases hm : x.msb with
      | true => exact absurd hm hmsb
      | false => rfl
    have hlt : x.toNat < 2 ^ 15 := BitVec.toNat_lt_of_msb_false hf
    have hti : x.toInt = (x.toNat : Int) := BitVec.toInt_eq_toNat_of_msb hf
    rw [xor_two_pow_eq_or hlt, or_two_pow_eq_add hlt, hti]
    omega

private theorem P_eq (x : BitVec 16) : P x = 2 ^ 16 - 1 - (x.toNat ^^^ 2 ^ 15) := by
  have h8 : ((0x8000 : BitVec 16).toNat) = 2 ^ 15 := by decide
  unfold P
  rw [BitVec.toNat_not, BitVec.toNat_xor, h8]

/-- Unsigned comparison of normalized majors mirrors the reversed signed comparison. -/
private theorem P_lt_iff {x y : BitVec 16} : P x < P y ↔ y.toInt < x.toInt := by
  have hx : x.toNat ^^^ 2 ^ 15 < 2 ^ 16 :=
    Nat.xor_lt_two_pow (n := 16) (BitVec.isLt x) (by decide)
  have hy : y.toNat ^^^ 2 ^ 15 < 2 ^ 16 :=
    Nat.xor_lt_two_pow (n := 16) (BitVec.isLt y) (by decide)
  have ex := xor_flip_int x
  have ey := xor_flip_int y
  rw [P_eq, P_eq]
  refine ⟨fun h => ?_, fun h => ?_⟩ <;> omega

private theorem P_inj {x y : BitVec 16} (h : P x = P y) : x = y := by
  apply BitVec.toInt_inj.mp
  have h1 := @P_lt_iff x y
  have h2 := @P_lt_iff y x
  rw [h] at h1
  rw [← h] at h2
  omega

/-- The packed 33-bit value of a row with a non-NULL major. -/
private theorem encode_toNat_some (x m : BitVec 16) :
    (encode ⟨some x, m⟩).toNat = P x * 2 ^ 16 + m.toNat := by
  show ((BitVec.ofNat 1 (Option.isNone (some x)).toNat : BitVec 1) ++ ~~~(x ^^^ 0x8000) ++ m).toNat
      = P x * 2 ^ 16 + m.toNat
  rw [BitVec.toNat_append_eq_mul_add, BitVec.toNat_append_eq_mul_add]
  have ht : ((BitVec.ofNat 1 (Option.isNone (some x)).toNat : BitVec 1)).toNat = 0 := by simp
  rw [ht]
  simp only [P]
  omega

/-- The packed 33-bit value of a row with a NULL major. -/
private theorem encode_toNat_none (m : BitVec 16) :
    (encode ⟨none, m⟩).toNat = 2 ^ 32 + m.toNat := by
  show ((BitVec.ofNat 1 (Option.isNone (none : Option (BitVec 16))).toNat : BitVec 1)
      ++ (0 : BitVec 16) ++ m).toNat = 2 ^ 32 + m.toNat
  rw [BitVec.toNat_append_eq_mul_add, BitVec.toNat_append_eq_mul_add]
  have ht : ((BitVec.ofNat 1 (Option.isNone (none : Option (BitVec 16))).toNat : BitVec 1)).toNat
      = 1 := by decide
  have hz : ((0 : BitVec 16)).toNat = 0 := rfl
  rw [ht, hz]

/-- The packed unsigned comparison for two non-NULL majors: strict less on the
packed value is the lexicographic `before` condition. -/
private theorem some_some_lt (x y xmin bmin : BitVec 16) :
    (P x * 2 ^ 16 + xmin.toNat < P y * 2 ^ 16 + bmin.toNat) ↔
      (x ≠ y ∧ P x < P y) ∨ (x = y ∧ xmin.toNat < bmin.toNat) := by
  constructor
  · intro h
    by_cases hyx : x = y
    · subst y
      exact Or.inr ⟨rfl, by omega⟩
    · refine Or.inl ⟨hyx, ?_⟩
      by_cases hlt : P x < P y
      · exact hlt
      · have hge : P y ≤ P x := by omega
        have hn : P x ≠ P y := fun he => hyx (P_inj he)
        have ha : xmin.toNat < 2 ^ 16 := BitVec.isLt xmin
        have hb : bmin.toNat < 2 ^ 16 := BitVec.isLt bmin
        omega
  · intro hd
    cases hd with
    | inl hn =>
        obtain ⟨hne, hlt⟩ := hn
        have hx : P x < 2 ^ 16 := P_lt_two_pow x
        have hy : P y < 2 ^ 16 := P_lt_two_pow y
        have ha : xmin.toNat < 2 ^ 16 := BitVec.isLt xmin
        have hb : bmin.toNat < 2 ^ 16 := BitVec.isLt bmin
        omega
    | inr he =>
        obtain ⟨heq, hlt⟩ := he
        subst heq
        omega

/-- The unsigned comparison of two packed non-NULL rows matches `before`'s
some/some branch (with the tie-break on the minor key). -/
private theorem cmp_some_some (x y xmin bmin : BitVec 16) :
    decide (P x * 2 ^ 16 + xmin.toNat < P y * 2 ^ 16 + bmin.toNat)
      = (if x = y then decide (xmin.toNat < bmin.toNat) else decide (x.toInt > y.toInt)) := by
  by_cases hyx : x = y
  · subst y
    rw [ite_eq_left rfl]
    exact decide_congr' (by omega)
  · rw [ite_eq_right hyx]
    refine (decide_congr' ⟨?_, ?_⟩).symm
    · intro h
      exact (some_some_lt x y xmin bmin).mpr (Or.inl ⟨hyx, P_lt_iff.mpr h⟩)
    · intro h
      cases (some_some_lt x y xmin bmin).mp h with
      | inl hn => exact P_lt_iff.mp hn.2
      | inr he => exact absurd he.1 hyx

theorem sound : TinylambOpt.ResearchGoals.PackedOrderKey.Goal := by
  intro a b
  rcases a with ⟨am, xmin⟩
  rcases b with ⟨bm, bmin⟩
  cases am
  case none =>
    cases bm
    case none =>
      constructor
      · simp only [before]
        rw [encode_toNat_none, encode_toNat_none]
        exact decide_congr' (by omega)
      · constructor
        · intro h
          have h := congrArg (fun v : BitVec 33 => v.toNat) h
          simp only [encode_toNat_none] at h
          exact ⟨rfl, BitVec.toNat_inj.mp (Nat.add_left_cancel h)⟩
        · rintro ⟨_, h2⟩
          subst h2
          rfl
    case some y =>
      constructor
      · simp only [before]
        rw [encode_toNat_none, encode_toNat_some]
        exact (decide_eq_false (by
          have hp : P y < 2 ^ 16 := P_lt_two_pow y
          have hb : bmin.toNat < 2 ^ 16 := BitVec.isLt bmin
          omega)).symm
      · constructor
        · intro h
          have h := congrArg (fun v : BitVec 33 => v.toNat) h
          simp only [encode_toNat_none, encode_toNat_some] at h
          have hp : P y < 2 ^ 16 := P_lt_two_pow y
          have hb : bmin.toNat < 2 ^ 16 := BitVec.isLt bmin
          exact (by omega : False).elim
        · rintro ⟨h1, _⟩
          exact absurd h1 (by simp)
  case some x =>
    cases bm
    case none =>
      constructor
      · simp only [before]
        rw [encode_toNat_some, encode_toNat_none]
        exact (decide_eq_true (by
          have hp : P x < 2 ^ 16 := P_lt_two_pow x
          omega)).symm
      · constructor
        · intro h
          have h := congrArg (fun v : BitVec 33 => v.toNat) h
          simp only [encode_toNat_some, encode_toNat_none] at h
          have hp : P x < 2 ^ 16 := P_lt_two_pow x
          exact (by omega : False).elim
        · rintro ⟨h1, _⟩
          exact absurd h1 (by simp)
    case some y =>
      constructor
      · simp only [before]
        rw [encode_toNat_some, encode_toNat_some]
        exact (cmp_some_some x y xmin bmin).symm
      · constructor
        · intro h
          have h := congrArg (fun v : BitVec 33 => v.toNat) h
          simp only [encode_toNat_some] at h
          have hp : P x < 2 ^ 16 := P_lt_two_pow x
          have hq : P y < 2 ^ 16 := P_lt_two_pow y
          have ha : xmin.toNat < 2 ^ 16 := BitVec.isLt xmin
          have hb : bmin.toNat < 2 ^ 16 := BitVec.isLt bmin
          have h1 : P x = P y ∧ xmin.toNat = bmin.toNat := by omega
          exact ⟨by rw [P_inj h1.1], BitVec.toNat_inj.mp h1.2⟩
        · rintro ⟨h1, h2⟩
          dsimp only at h1 h2
          have hxy := Option.some.inj h1
          subst hxy
          subst h2
          rfl

end TinylambOpt.ResearchProofs.PackedOrderKey
