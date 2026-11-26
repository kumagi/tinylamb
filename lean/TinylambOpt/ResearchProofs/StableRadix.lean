import Std
import TinylambOpt.ResearchGoals.StableRadix

/-!
Proved

- `Goal` — `∀ rows, radix rows = rows.mergeSort before`

The reference model is defined in `TinylambOpt.ResearchGoals.StableRadix`.
-/
open TinylambOpt.ResearchGoals.StableRadix

namespace TinylambOpt.ResearchProofs.StableRadix

def code (r : Row) : Nat :=
  match r.key with
  | none => 2^64
  | some x => x.toBitVec.toNat ^^^ (2^63 - 1)

theorem code_testBit (r : Row) (i : Nat) (hi : i < 65) :
    (code r).testBit i = digit r i := by
  rcases r with ⟨key, payload⟩
  cases key with
  | none =>
      simp only [code, digit]
      by_cases h : i = 64
      · subst i
        change (2^64).testBit 64 = (64 == 64)
        rw [Nat.testBit_two_pow]
        decide
      · have hlt : i < 64 := by omega
        change (2^64).testBit i = (i == 64)
        rw [Nat.testBit_two_pow]
        have h' : 64 ≠ i := by omega
        simp [h, h']
  | some x =>
      simp only [code, digit]
      by_cases h64 : i = 64
      · subst i
        have hxlt : x.toBitVec.toNat < 2^64 := x.toBitVec.isLt
        have hmlt : 2^63 - 1 < 2^64 := by decide
        have hxor : x.toBitVec.toNat ^^^ (2^63 - 1) < 2^64 := Nat.xor_lt_two_pow hxlt hmlt
        change (x.toBitVec.toNat ^^^ (2^63 - 1)).testBit 64 = false
        exact Nat.testBit_lt_two_pow hxor
      · have hi64 : i < 64 := by omega
        by_cases h63 : i = 63
        · subst i
          change (x.toBitVec.toNat ^^^ (2^63 - 1)).testBit 63 = x.toBitVec.getLsbD 63
          rw [Nat.testBit_xor, Nat.testBit_two_pow_sub_one]
          simp
          rfl
        · have hilt63 : i < 63 := by omega
          simp [h64, h63]
          have hmaskbit : Nat.testBit 9223372036854775807 i = true := by
            change Nat.testBit (2^63 - 1) i = true
            rw [Nat.testBit_two_pow_sub_one]
            simp [hilt63]
          rw [hmaskbit]
          simp
          rfl

theorem code_lt_pow65 (r : Row) : code r < 2^65 := by
  rcases r with ⟨key, payload⟩
  cases key with
  | none => change 2^64 < 2^65; decide
  | some x =>
      have hx : x.toBitVec.toNat < 2^64 := x.toBitVec.isLt
      have hx65 : x.toBitVec.toNat < 2^65 := by
        have hp : 2^64 < 2^65 := by decide
        omega
      have hm65 : 2^63 - 1 < 2^65 := by decide
      have hxo : x.toBitVec.toNat ^^^ (2^63 - 1) < 2^65 := Nat.xor_lt_two_pow hx65 hm65
      simpa [code] using hxo

theorem code_some_lt_pow64 (x : Int64) : code ⟨some x, 0⟩ < 2^64 := by
  have hx : x.toBitVec.toNat < 2^64 := x.toBitVec.isLt
  have hm : 2^63 - 1 < 2^64 := by decide
  have hxo : x.toBitVec.toNat ^^^ (2^63 - 1) < 2^64 := Nat.xor_lt_two_pow hx hm
  simpa [code] using hxo

theorem flip_low (x : Nat) (hx : x < 2^63) :
    x ^^^ (2^63 - 1) = 2^63 - (x + 1) := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_xor, Nat.testBit_two_pow_sub_one]
  rw [Nat.testBit_two_pow_sub_succ hx]
  by_cases hi : i < 63
  · simp [hi]
  · have hxi : x.testBit i = false := by
      apply Nat.testBit_lt_two_pow
      have hp : 2^63 ≤ 2^i := Nat.pow_le_pow_right (by decide : 0 < 2) (by omega)
      omega
    simp [hi, hxi]

theorem xor_two_pow_add (low mask : Nat) (hlow : low < 2^63) (hmask : mask < 2^63) :
    (2^63 + low) ^^^ mask = 2^63 + (low ^^^ mask) := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_xor, Nat.testBit_two_pow_mul_add 1 hlow]
  rw [Nat.testBit_two_pow_mul_add 1 (Nat.xor_lt_two_pow hlow hmask)]
  rw [Nat.testBit_xor]
  by_cases hi : i < 63
  · simp [hi]
  · have hmi : mask.testBit i = false := by
      apply Nat.testBit_lt_two_pow
      have hp : 2^63 ≤ 2^i := Nat.pow_le_pow_right (by decide : 0 < 2) (by omega)
      omega
    simp [hi, hmi]

theorem flip_high (x : Nat) (hlo : 2^63 ≤ x) (hx : x < 2^64) :
    x ^^^ (2^63 - 1) = 2^63 + ((x - 2^63) ^^^ (2^63 - 1)) := by
  have hlow : x - 2^63 < 2^63 := by omega
  have hxdecomp : x = 2^63 + (x - 2^63) := by omega
  calc
    x ^^^ (2^63 - 1) = (2^63 + (x - 2^63)) ^^^ (2^63 - 1) :=
      congrArg (fun t : Nat => t ^^^ (2^63 - 1)) hxdecomp
    _ = 2^63 + ((x - 2^63) ^^^ (2^63 - 1)) :=
      xor_two_pow_add (x - 2^63) (2^63 - 1) hlow (by decide)

theorem flip_bit63 (x : Nat) (hlo : 2^63 ≤ x) (hx : x < 2^64) :
    (x ^^^ (2^63 - 1)).testBit 63 = true := by
  rw [Nat.testBit_xor, Nat.testBit_two_pow_sub_one]
  have hxbit : x.testBit 63 = true :=
    Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hlo hx
  simp [hxbit]

theorem signed_flip_order (a b : Nat) (ha : a < 2^64) (hb : b < 2^64) :
    ((if b < 2^63 then (b : Int) else (b : Int) - (2^64 : Nat)) ≤
      (if a < 2^63 then (a : Int) else (a : Int) - (2^64 : Nat))) ↔
    ((a ^^^ (2^63 - 1)) ≤ (b ^^^ (2^63 - 1))) := by
  by_cases ha63 : a < 2^63
  · by_cases hb63 : b < 2^63
    · rw [flip_low a ha63, flip_low b hb63]
      simp [ha63, hb63]
      omega
    · have hfa_lt : (a ^^^ (2^63 - 1)) < 2^63 := by
        rw [flip_low a ha63]
        omega
      have hfb_ge : 2^63 ≤ (b ^^^ (2^63 - 1)) :=
        Nat.ge_two_pow_of_testBit (flip_bit63 b (by omega) hb)
      constructor
      · intro _
        omega
      · intro _
        simp [ha63, hb63]
        omega
  · by_cases hb63 : b < 2^63
    · have hfa_ge : 2^63 ≤ (a ^^^ (2^63 - 1)) :=
        Nat.ge_two_pow_of_testBit (flip_bit63 a (by omega) ha)
      have hfb_lt : (b ^^^ (2^63 - 1)) < 2^63 := by
        rw [flip_low b hb63]
        omega
      constructor
      · intro h
        simp [ha63, hb63] at h
        omega
      · intro _
        omega
    · have hage : 2^63 ≤ a := Nat.le_of_not_gt ha63
      have hbge : 2^63 ≤ b := Nat.le_of_not_gt hb63
      have hp64 : 2^64 = 2^63 + 2^63 := by
        rw [show 64 = 63 + 1 by decide, Nat.pow_succ]
      have hla : a - 2^63 < 2^63 := by
        apply (Nat.sub_lt_iff_lt_add hage).2
        rw [← hp64]
        exact ha
      have hlb : b - 2^63 < 2^63 := by
        apply (Nat.sub_lt_iff_lt_add hbge).2
        rw [← hp64]
        exact hb
      rw [flip_high a (by omega) ha, flip_high b (by omega) hb]
      rw [flip_low (a - 2^63) hla, flip_low (b - 2^63) hlb]
      simp [ha63, hb63]
      constructor <;> intro h <;> omega

theorem bv_code_order (u v : BitVec 64) :
    (v.sle u) = (((0#1 ++ u) ^^^ (((1 : BitVec 65) <<< 63) - 1)).ule
                  ((0#1 ++ v) ^^^ (((1 : BitVec 65) <<< 63) - 1))) := by
  rw [BitVec.sle_eq_decide, BitVec.ule_eq_decide]
  have hround (x : BitVec 64) :
      (((0#1 ++ x) ^^^ (((1 : BitVec 65) <<< 63) - 1)).toNat) =
        x.toNat ^^^ (2^63 - 1) := by
    simp
  rw [hround u, hround v]
  rw [decide_eq_decide]
  have hu : u.toNat < 2^64 := BitVec.isLt u
  have hv : v.toNat < 2^64 := BitVec.isLt v
  have htu : u.toInt =
      (if u.toNat < 2^63 then (u.toNat : Int) else (u.toNat : Int) - (2^64 : Nat)) := by
    rw [BitVec.toInt_eq_msb_cond, BitVec.msb_eq_decide]
    by_cases h : u.toNat < 2^63
    · have hnot : ¬ 2 ^ (64 - 1) ≤ u.toNat := by omega
      simp [h, hnot]
    · have hge : 2 ^ (64 - 1) ≤ u.toNat := by omega
      simp [h, hge]
  have htv : v.toInt =
      (if v.toNat < 2^63 then (v.toNat : Int) else (v.toNat : Int) - (2^64 : Nat)) := by
    rw [BitVec.toInt_eq_msb_cond, BitVec.msb_eq_decide]
    by_cases h : v.toNat < 2^63
    · have hnot : ¬ 2 ^ (64 - 1) ≤ v.toNat := by omega
      simp [h, hnot]
    · have hge : 2 ^ (64 - 1) ≤ v.toNat := by omega
      simp [h, hge]
  rw [htu, htv]
  exact signed_flip_order u.toNat v.toNat hu hv

theorem before_iff_code_le (a b : Row) : before a b = true ↔ code a ≤ code b := by
  rcases a with ⟨keya, pa⟩
  rcases b with ⟨keyb, pb⟩
  cases keya with
  | none =>
      cases keyb with
      | none => simp [before, code]
      | some y =>
          have hy : code ⟨some y, pb⟩ < 2^64 := code_some_lt_pow64 y
          have hnot : ¬ 2^64 ≤ code ⟨some y, pb⟩ := by omega
          constructor
          · intro hb; simp [before] at hb
          · intro hle; exact (hnot hle).elim
  | some x =>
      cases keyb with
      | none =>
          have hx : code ⟨some x, pa⟩ < 2^64 := code_some_lt_pow64 x
          have hnone : code ⟨none, pb⟩ = 2^64 := rfl
          rw [hnone]
          constructor
          · intro hb; omega
          · intro hc; simp [before]
      | some y =>
          have h := bv_code_order x.toBitVec y.toBitVec
          have hsle : y.toBitVec.sle x.toBitVec = decide (y.toInt ≤ x.toInt) := BitVec.sle_eq_decide
          have hule : (((0#1 ++ x.toBitVec) ^^^ (((1 : BitVec 65) <<< 63) - 1)).ule
                  ((0#1 ++ y.toBitVec) ^^^ (((1 : BitVec 65) <<< 63) - 1))) = decide (code ⟨some x, pa⟩ ≤ code ⟨some y, pb⟩) := by
            simp [code, BitVec.ule_eq_decide]
            rfl
          simp [before] 
          -- goal: decide (x.toInt >= y.toInt) = true -> code... 
          constructor
          · intro hb
            have hb' : y.toInt ≤ x.toInt := by omega
            have hs : y.toBitVec.sle x.toBitVec = true := by rw [hsle]; simp [hb']
            have hu : (((0#1 ++ x.toBitVec) ^^^ (((1 : BitVec 65) <<< 63) - 1)).ule
                  ((0#1 ++ y.toBitVec) ^^^ (((1 : BitVec 65) <<< 63) - 1))) = true := h ▸ hs
            rw [hule] at hu
            simpa using hu
          · intro hc
            have hu : (((0#1 ++ x.toBitVec) ^^^ (((1 : BitVec 65) <<< 63) - 1)).ule
                  ((0#1 ++ y.toBitVec) ^^^ (((1 : BitVec 65) <<< 63) - 1))) = true := by
              rw [hule]; simp [hc]
            have hs : y.toBitVec.sle x.toBitVec = true := by
              -- from h : sle = ule
              rw [h]
              exact hu
            rw [hsle] at hs
            simpa using hs


abbrev IRow := Row × Nat

def lowCode (r : Row) : Nat → Nat
  | 0 => if digit r 0 then 1 else 0
  | k+1 => lowCode r k + (if digit r (k+1) then 2^(k+1) else 0)

theorem lowCode_bound (r : Row) (k : Nat) : lowCode r k < 2^(k+1) := by
  induction k with
  | zero =>
      simp [lowCode]
      cases digit r 0 <;> simp
  | succ k ih =>
      simp [lowCode]
      by_cases h : digit r (k+1)
      · simp [h]
        rw [show k + 2 = (k+1).succ by omega, Nat.pow_succ]
        omega
      · simp [h]
        rw [show k + 2 = (k+1).succ by omega, Nat.pow_succ]
        omega

theorem lowCode_lt_pow_of_false (r : Row) (k : Nat) (h : digit r k = false) :
    lowCode r k < 2^k := by
  cases k with
  | zero => simp [lowCode, h]
  | succ k =>
      simp [lowCode, h]
      exact lowCode_bound r k

theorem lowCode_ge_pow_of_true (r : Row) (k : Nat) (h : digit r k = true) :
    2^k ≤ lowCode r k := by
  cases k with
  | zero => simp [lowCode, h]
  | succ k =>
      simp [lowCode, h]

def LowLE (k : Nat) (x y : IRow) : Prop :=
  lowCode x.1 k < lowCode y.1 k ∨ (lowCode x.1 k = lowCode y.1 k ∧ x.2 ≤ y.2)

def sortOrd : Nat → IRow → IRow → Prop
  | 0 => fun x y => x.2 ≤ y.2
  | k+1 => LowLE k

theorem same_digit_imp (k : Nat) {a b : IRow}
    (ha : digit a.1 k = false) (hb : digit b.1 k = false)
    (h : sortOrd k a b) : LowLE k a b := by
  cases k with
  | zero =>
      simpa [sortOrd, LowLE, lowCode, ha, hb] using h
  | succ k' =>
      simp only [sortOrd] at h
      simp [LowLE, lowCode, ha, hb]
      -- h : LowLE k' a b
      rcases h with hlt | ⟨heq, hind⟩
      · left; omega
      · right; constructor <;> omega

theorem same_digit_imp_true (k : Nat) {a b : IRow}
    (ha : digit a.1 k = true) (hb : digit b.1 k = true)
    (h : sortOrd k a b) : LowLE k a b := by
  cases k with
  | zero =>
      simpa [sortOrd, LowLE, lowCode, ha, hb] using h
  | succ k' =>
      simp only [sortOrd] at h
      simp [LowLE, lowCode, ha, hb]
      rcases h with hlt | ⟨heq, hind⟩
      · left; omega
      · right; constructor <;> omega

theorem cross_imp (k : Nat) {a b : IRow}
    (ha : digit a.1 k = false) (hb : digit b.1 k = true) : LowLE k a b := by
  left
  have hla := lowCode_lt_pow_of_false a.1 k ha
  have hgb := lowCode_ge_pow_of_true b.1 k hb
  omega

def ipass (l : List IRow) (i : Nat) : List IRow :=
  let (ones, zeros) := l.partition (fun r => digit r.1 i)
  zeros ++ ones

theorem pairwise_imp_mem {l : List IRow} {P : IRow → Prop} {R S : IRow → IRow → Prop}
    (hP : ∀ x, x ∈ l → P x)
    (himp : ∀ {a b}, P a → P b → R a b → S a b)
    (hs : l.Pairwise R) : l.Pairwise S := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp only [List.pairwise_cons] at hs ⊢
      constructor
      · intro b hb
        exact himp (hP a (by simp)) (hP b (by simp [hb])) (hs.1 b hb)
      · exact ih (fun x hx => hP x (by simp [hx])) hs.2

theorem ipass_perm (l : List IRow) (i : Nat) : (ipass l i).Perm l := by
  simp only [ipass, List.partition_eq_filter_filter]
  change (l.filter (fun r => !(digit r.1 i)) ++ l.filter (fun r => digit r.1 i)).Perm l
  simpa using (List.filter_append_perm (fun r => !(digit r.1 i)) l)

theorem ipass_sorted (k : Nat) (l : List IRow) (hs : l.Pairwise (sortOrd k)) :
    (ipass l k).Pairwise (LowLE k) := by
  simp only [ipass, List.partition_eq_filter_filter]
  rw [List.pairwise_append]
  constructor
  · -- zeros (digit k = false)
    have hsub : (l.filter (fun r => !(digit r.1 k))).Sublist l := List.filter_sublist
    have hpairs : (l.filter (fun r => !(digit r.1 k))).Pairwise (sortOrd k) :=
      List.Pairwise.sublist hsub hs
    apply pairwise_imp_mem (P := fun r : IRow => digit r.1 k = false)
    · intro x hx
      simpa using (List.mem_filter.mp hx).2
    · intro a b ha hb hab
      exact same_digit_imp k ha hb hab
    · exact hpairs
  constructor
  · -- ones
    have hsub : (l.filter (fun r => digit r.1 k)).Sublist l := List.filter_sublist
    have hpairs : (l.filter (fun r => digit r.1 k)).Pairwise (sortOrd k) :=
      List.Pairwise.sublist hsub hs
    apply pairwise_imp_mem (P := fun r : IRow => digit r.1 k = true)
    · intro x hx
      simpa using (List.mem_filter.mp hx).2
    · intro a b ha hb hab
      exact same_digit_imp_true k ha hb hab
    · exact hpairs
  · -- cross
    intro a ha b hb
    simp at ha hb
    exact cross_imp k ha.2 hb.2



theorem lowCode_step' (r : Row) (k : Nat) :
    lowCode r (k+1) = 2^(k+1) * (if digit r (k+1) then 1 else 0) + lowCode r k := by
  by_cases h : digit r (k+1) <;> simp [lowCode, h, Nat.add_comm]

theorem lowCode_testBit (r : Row) (k i : Nat) (hi : i ≤ k) :
    (lowCode r k).testBit i = digit r i := by
  induction k with
  | zero =>
      have hi0 : i = 0 := by omega
      subst i
      simp [lowCode]
      cases digit r 0 <;> simp
  | succ k ih =>
      by_cases hik : i ≤ k
      · rw [lowCode_step']
        rw [Nat.testBit_two_pow_mul_add (if digit r (k+1) then 1 else 0) (lowCode_bound r k) i]
        have hi_lt : i < k + 1 := by omega
        simpa [hi_lt] using (ih hik)
      · have hi_eq : i = k+1 := by omega
        subst i
        rw [lowCode_step']
        rw [Nat.testBit_two_pow_mul_add (if digit r (k+1) then 1 else 0) (lowCode_bound r k) (k+1)]
        simp
        cases digit r (k+1) <;> simp

theorem lowCode_eq_code (r : Row) : lowCode r 64 = code r := by
  apply Nat.eq_of_testBit_eq
  intro i
  by_cases hi : i < 65
  · rw [lowCode_testBit r 64 i (by omega), code_testBit r i hi]
  · have hpow : 2^65 ≤ 2^i := Nat.pow_le_pow_right (by decide : 2 > 0) (by omega)
    have h1 : (lowCode r 64).testBit i = false := by
      apply Nat.testBit_lt_two_pow
      exact Nat.lt_of_lt_of_le (by simpa using (lowCode_bound r 64)) hpow
    have h2 : (code r).testBit i = false := by
      apply Nat.testBit_lt_two_pow
      exact Nat.lt_of_lt_of_le (code_lt_pow65 r) hpow
    rw [h1, h2]

abbrev leIdx := List.zipIdxLE before

theorem lowLE_iff_leIdx (x y : IRow) : LowLE 64 x y ↔ leIdx x y = true := by
  have hbxy := before_iff_code_le x.1 y.1
  have hbyx := before_iff_code_le y.1 x.1
  by_cases hxy : before x.1 y.1 = true
  · by_cases hyx : before y.1 x.1 = true
    · have hcode : lowCode x.1 64 = lowCode y.1 64 := by
        rw [lowCode_eq_code, lowCode_eq_code]
        exact Nat.le_antisymm (hbxy.1 hxy) (hbyx.1 hyx)
      simp [leIdx, List.zipIdxLE, hxy, hyx, LowLE, hcode]
    · have hlt : lowCode x.1 64 < lowCode y.1 64 := by
        rw [lowCode_eq_code, lowCode_eq_code]
        have hle := hbxy.1 hxy
        have hnle : ¬ code y.1 ≤ code x.1 := by
          intro hc
          exact hyx (hbyx.2 hc)
        omega
      simp [leIdx, List.zipIdxLE, hxy, hyx, LowLE, hlt]
  · have hnle : ¬ code x.1 ≤ code y.1 := by
      intro hc
      exact hxy (hbxy.2 hc)
    have hgyx : code y.1 < code x.1 := by omega
    have hlt : lowCode y.1 64 < lowCode x.1 64 := by
      rwa [lowCode_eq_code, lowCode_eq_code]
    have hnotLow : ¬ LowLE 64 x y := by
      intro hh
      rcases hh with hltxy | ⟨heq, _⟩
      · omega
      · omega
    have hnotIdx : leIdx x y ≠ true := by
      simp [leIdx, List.zipIdxLE, hxy]
    constructor
    · intro hh; exact (hnotLow hh).elim
    · intro hh; exact (hnotIdx hh).elim


theorem map_fst_ipass (l : List IRow) (i : Nat) :
    (ipass l i).map Prod.fst = pass (l.map Prod.fst) i := by
  simp only [ipass, pass, List.partition_eq_filter_filter]
  rw [List.filter_map, List.filter_map]
  change (l.filter (fun r => !(digit r.1 i)) ++ l.filter (fun r => digit r.1 i)).map Prod.fst =
    (l.filter (fun r => !(digit r.1 i))).map Prod.fst ++
      (l.filter (fun r => digit r.1 i)).map Prod.fst
  simp

def iradixUpTo (rows : List Row) : Nat → List IRow
  | 0 => rows.zipIdx
  | k+1 => ipass (iradixUpTo rows k) k

theorem map_fst_iradixUpTo (rows : List Row) : ∀ k : Nat,
    (iradixUpTo rows k).map Prod.fst = (List.range k).foldl pass rows
  | 0 => by simp [iradixUpTo]
  | k+1 => by
      simp [iradixUpTo, List.range_succ, map_fst_ipass, map_fst_iradixUpTo rows k]

theorem iradixUpTo_perm (rows : List Row) : ∀ k : Nat,
    (iradixUpTo rows k).Perm rows.zipIdx
  | 0 => by simp [iradixUpTo]
  | k+1 => by
      exact (ipass_perm (iradixUpTo rows k) k).trans (iradixUpTo_perm rows k)

theorem zipIdx_pairwise_from (rows : List Row) (n : Nat) :
    (rows.zipIdx n).Pairwise (fun x y : IRow => x.2 ≤ y.2) := by
  induction rows generalizing n with
  | nil => simp
  | cons a t ih =>
      rw [List.zipIdx_cons]
      simp only [List.pairwise_cons]
      constructor
      · intro b hb
        have hbn := (List.mem_zipIdx hb).1
        omega
      · exact ih (n+1)

theorem iradixUpTo_sorted (rows : List Row) : ∀ k : Nat,
    (iradixUpTo rows k).Pairwise (sortOrd k)
  | 0 => by
      simpa [iradixUpTo, sortOrd] using (zipIdx_pairwise_from rows 0)
  | k+1 => by
      simpa [iradixUpTo, sortOrd] using
        (ipass_sorted k (iradixUpTo rows k) (iradixUpTo_sorted rows k))

theorem before_trans (a b c : Row) (hab : before a b = true) (hbc : before b c = true) :
    before a c = true := by
  rw [before_iff_code_le] at hab hbc ⊢
  omega

theorem before_total (a b : Row) : (before a b || before b a) = true := by
  have hle := before_iff_code_le a b
  have hle' := before_iff_code_le b a
  by_cases hab : before a b
  · simp [hab]
  · have hnle : ¬ code a ≤ code b := by
      intro hc
      exact hab (hle.2 hc)
    have hcb : code b ≤ code a := by omega
    have hbb : before b a = true := hle'.2 hcb
    simp [hab, hbb]

theorem leIdx_trans (a b c : IRow) (hab : leIdx a b = true) (hbc : leIdx b c = true) :
    leIdx a c = true :=
  List.zipIdxLE_trans before_trans a b c hab hbc

theorem leIdx_total (a b : IRow) : (leIdx a b || leIdx b a) = true :=
  List.zipIdxLE_total before_total a b

theorem leIdx_antisymm (rows : List Row) {a b : IRow}
    (ha : a ∈ rows.zipIdx) (hb : b ∈ rows.zipIdx)
    (hab : leIdx a b = true) (hba : leIdx b a = true) : a = b := by
  have hbxy := before_iff_code_le a.1 b.1
  have hbyx := before_iff_code_le b.1 a.1
  by_cases hxy : before a.1 b.1 = true
  · by_cases hyx : before b.1 a.1 = true
    · have hiab : a.2 ≤ b.2 := by simpa [leIdx, List.zipIdxLE, hxy, hyx] using hab
      have hiba : b.2 ≤ a.2 := by simpa [leIdx, List.zipIdxLE, hyx, hxy] using hba
      have hij : a.2 = b.2 := Nat.le_antisymm hiab hiba
      have ha' := List.mem_zipIdx ha
      have hb' := List.mem_zipIdx hb
      cases a with | mk ar ai =>
      cases b with | mk br bi =>
        simp at hij
        subst bi
        simp_all
    · have hnle : ¬ code b.1 ≤ code a.1 := by
        intro hc
        exact hyx (hbyx.2 hc)
      have hbfa : before b.1 a.1 = false := by simp [hyx]
      simp [leIdx, List.zipIdxLE, hbfa] at hba
  · have hnle : ¬ code a.1 ≤ code b.1 := by
      intro hc
      exact hxy (hbxy.2 hc)
    simp [leIdx, List.zipIdxLE, hxy] at hab

theorem sound : TinylambOpt.ResearchGoals.StableRadix.Goal := by
  intro rows
  have hperm1 : (iradixUpTo rows 65).Perm rows.zipIdx := iradixUpTo_perm rows 65
  have hsorted1 : (iradixUpTo rows 65).Pairwise (fun x y => leIdx x y = true) := by
    have hs := iradixUpTo_sorted rows 65
    exact hs.imp (fun h => (lowLE_iff_leIdx _ _).1 h)
  have hsorted2 : (rows.zipIdx.mergeSort leIdx).Pairwise (fun x y => leIdx x y = true) :=
    List.pairwise_mergeSort leIdx_trans leIdx_total rows.zipIdx
  have hperm2 : (rows.zipIdx.mergeSort leIdx).Perm rows.zipIdx :=
    List.mergeSort_perm rows.zipIdx leIdx
  have heq : iradixUpTo rows 65 = rows.zipIdx.mergeSort leIdx := by
    apply List.Perm.eq_of_pairwise
    · intro a b ha hb hab hba
      exact leIdx_antisymm rows (hperm1.mem_iff.mp ha) (hperm2.mem_iff.mp hb) hab hba
    · exact hsorted1
    · exact hsorted2
    · exact hperm1.trans hperm2.symm
  calc
    radix rows = (iradixUpTo rows 65).map Prod.fst := by
      symm
      simpa [radix] using (map_fst_iradixUpTo rows 65)
    _ = (rows.zipIdx.mergeSort leIdx).map Prod.fst := by rw [heq]
    _ = rows.mergeSort before := by
      simpa [leIdx] using (List.mergeSort_zipIdx (l := rows) (le := before))

end TinylambOpt.ResearchProofs.StableRadix

-- C++ references to this model or its proof.
-- executor/detail/columnar_tlcs_fast_path.cpp
-- executor/detail/ooc/external_sorter.cpp
-- executor/detail/relational_sort_limit.cpp
-- executor/detail/typed_stream_aggregate_dict.cpp
-- executor/detail/typed_stream_aggregate_partitioned.cpp
-- executor/relational_regression_test_part11.cpp
-- executor/sort.cpp
