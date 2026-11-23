import TinylambOpt.ResearchGoals.PackedRadix2x32BatchAgg

namespace TinylambOpt.ResearchProofs.PackedRadix2x32BatchAgg

open TinylambOpt.ResearchGoals.PackedRadix2x32BatchAgg

/-! ## Small helpers -/

private theorem ite_congr_bool {c₁ c₂ : Bool} {p q : Bool} (w : c₁ = c₂) :
    (if c₁ then p else q) = (if c₂ then p else q) := by
  subst w
  rfl

private def s2 (x y : Option Int) : Option Int :=
  match x, y with
  | some a, some b => some (a + b)
  | some a, none => some a
  | none, some b => some b
  | none, none => none

private theorem s2_comm (x y : Option Int) : s2 x y = s2 y x := by
  cases x <;> cases y <;> simp [s2] <;> omega

private theorem s2_assoc (x y z : Option Int) : s2 (s2 x y) z = s2 x (s2 y z) := by
  cases x <;> cases y <;> cases z <;> simp [s2] <;> omega

private theorem s2_none_left (x : Option Int) : s2 none x = x := by
  cases x <;> rfl

private theorem s2_none_right (x : Option Int) : s2 x none = x := by
  cases x <;> rfl

private theorem s2_rot4 (a b c d : Option Int) :
    s2 (s2 a b) (s2 c d) = s2 (s2 a c) (s2 b d) := by
  cases a <;> cases b <;> cases c <;> cases d <;> simp [s2] <;> omega

private theorem bump_eq_s2 {α : Type} [DecidableEq α] (k j : α) (v : Int) (x : Option Int) :
    (if k == j then match x with
      | some a => some (a + v)
      | none => some v
    else x) =
      if k == j then s2 x (some v) else x := by
  cases h : (k == j) <;> cases x <;> simp [s2, h]
/-! ## Nat / BitVec bitwise arithmetic for the 2×32 lane split -/

private theorem testBit_eq_false_of_ge {x i n : Nat} (h : x < 2 ^ n) (w : n ≤ i) :
    x.testBit i = false :=
  Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le h (Nat.pow_le_pow_of_le (by omega : 1 < 2) w))

private theorem twoPow32_eq : twoPow32 = (4294967296 : Int) := rfl

private theorem pow32_eq : (2 : Nat) ^ 32 = 4294967296 := by
  decide

private theorem bit_hi (a1 a2 : Nat) (h1 : a1 < 2 ^ 32) (h2 : a2 < 2 ^ 32) :
    (((BitVec.ofNat 64 a1 <<< 32) ||| BitVec.ofNat 64 a2) >>> 32).toNat = a1 := by
  have h6 : 2 ^ 32 * 2 ^ 32 = 2 ^ 64 := by
    rw [← Nat.pow_add]
  have hm1 : a1 % 2 ^ 64 = a1 := Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le h1 (by omega))
  have hm2 : a2 % 2 ^ 64 = a2 := Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le h2 (by omega))
  have hb : a1 * 2 ^ 32 % 2 ^ 64 = a1 * 2 ^ 32 :=
    Nat.mod_eq_of_lt (by
      rw [← h6]
      exact Nat.mul_lt_mul_of_pos_right h1 (Nat.pow_pos (by omega : (0 : Nat) < 2)))
  have key : ∀ i, ((a1 * 2 ^ 32 ||| a2) >>> 32).testBit i = a1.testBit i := by
    intro i
    rw [Nat.shiftRight_eq_div_pow, Nat.testBit_div_two_pow, Nat.testBit_or,
      Nat.testBit_mul_two_pow,
      testBit_eq_false_of_ge h2 (by omega : 32 ≤ i + 32)]
    simp
  rw [BitVec.toNat_ushiftRight, BitVec.toNat_or, BitVec.toNat_shiftLeft,
    BitVec.toNat_ofNat, BitVec.toNat_ofNat, Nat.shiftLeft_eq, hm1, hb, hm2]
  exact Nat.eq_of_testBit_eq key

private theorem bit_lo (a1 a2 : Nat) (h2 : a2 < 2 ^ 32) :
    (((BitVec.ofNat 64 a1 <<< 32) ||| BitVec.ofNat 64 a2) &&& 0xFFFFFFFF#64).toNat = a2 := by
  have hm2 : a2 % 2 ^ 64 = a2 := Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le h2 (by omega))
  have hm : (0xFFFFFFFF#64 : BitVec 64).toNat = 2 ^ 32 - 1 := by decide
  have key : ∀ j, ((a1 % 2 ^ 64 * 2 ^ 32 % 2 ^ 64 ||| a2 % 2 ^ 64) &&& (2 ^ 32 - 1)).testBit j =
      (a2 % 2 ^ 32).testBit j := by
    intro j
    simp only [Nat.testBit_and, Nat.testBit_two_pow_sub_one, Nat.testBit_or,
      Nat.testBit_mod_two_pow, Nat.testBit_mul_two_pow]
    rcases Nat.lt_or_ge j 32 with hj | hj
    · have e64 : (decide (j < 64)) = true := by
        rw [decide_eq_true_eq]
        exact Nat.lt_trans hj (by omega)
      have ege : (decide (32 ≤ j)) = false := by simp [hj]
      have esub : (decide (j - 32 < 64)) = true := by
        rw [decide_eq_true_eq]
        omega
      have e32 : (decide (j < 32)) = true := by
        rw [decide_eq_true_eq]
        exact hj
      simp [e64, ege, esub, e32]
    · rw [show (decide (j < 32)) = false from by simp [hj]]
      simp [testBit_eq_false_of_ge h2 hj, hj]
  rw [BitVec.toNat_and, BitVec.toNat_or, BitVec.toNat_shiftLeft, BitVec.toNat_ofNat,
    BitVec.toNat_ofNat, hm, Nat.shiftLeft_eq]
  exact (Nat.eq_of_testBit_eq key).trans (Nat.mod_eq_of_lt h2)

/-! ## Part 1: `unpack` inverts `tryPack` -/

private theorem unpack_of_tryPack :
    ∀ (lo1 lo2 k1 k2 : Int) (pk : BitVec 64),
      tryPack lo1 lo2 (some k1, some k2) = some pk → unpack lo1 lo2 pk = (k1, k2) := by
  intro lo1 lo2 k1 k2 pk h
  simp only [tryPack] at h
  split at h
  · rename_i hc
    obtain ⟨ha1, hba1, ha2, hba2⟩ := of_decide_eq_true hc
    have hpk : BitVec.ofNat 64 (k1 - lo1).toNat <<< 32 ||| BitVec.ofNat 64 (k2 - lo2).toNat = pk :=
      by simpa using h
    subst hpk
    rw [twoPow32_eq] at hba1 hba2
    have h1 : (k1 - lo1).toNat < 2 ^ 32 := by
      rw [pow32_eq]
      omega
    have h2 : (k2 - lo2).toNat < 2 ^ 32 := by
      rw [pow32_eq]
      omega
    have e1 : (Int.ofNat (k1 - lo1).toNat : Int) = k1 - lo1 := Int.toNat_of_nonneg ha1
    have e2 : (Int.ofNat (k2 - lo2).toNat : Int) = k2 - lo2 := Int.toNat_of_nonneg ha2
    simp only [unpack]
    rw [bit_hi _ _ h1 h2, bit_lo _ _ h2, Prod.mk.injEq]
    refine ⟨?_, ?_⟩
    · rw [e1]
      omega
    · rw [e2]
      omega
  · exact absurd h (by simp)

/-! ## Assoc-list machinery -/

private def stepAgg (lo1 lo2 : Int) (st : WorkerAgg) (k1 : Option Int) (k2 : Option Int)
    (v : Int) : WorkerAgg :=
  match tryPack lo1 lo2 (k1, k2) with
  | some pk => { st with fast := addAssoc st.fast pk v }
  | none => { st with fallback := addAssoc st.fallback (k1, k2) v }

private def workerFold (lo1 lo2 : Int) (rows : List RawRow) (st : WorkerAgg) : WorkerAgg :=
  rows.foldl (fun st (k1, k2, v) => stepAgg lo1 lo2 st k1 k2 v) st

private theorem workerAccum_eq (lo1 lo2 : Int) (rows : List RawRow) :
    workerAccum lo1 lo2 rows = workerFold lo1 lo2 rows {} := rfl

private def stepRef (acc : List (GroupKey × Int)) (r : RawRow) : List (GroupKey × Int) :=
  match r with
  | (k1, k2, v) => addAssoc acc (k1, k2) v

private theorem refGroupedSum_eq (rows : List RawRow) :
    refGroupedSum rows = rows.foldl stepRef [] := rfl

private def dkey (lo1 lo2 : Int) (pk : BitVec 64) : GroupKey :=
  let (k1, k2) := unpack lo1 lo2 pk
  (some k1, some k2)

private def decodeFast (lo1 lo2 : Int) (fast : List (BitVec 64 × Int)) :
    List (GroupKey × Int) :=
  fast.map fun (pk, s) => (dkey lo1 lo2 pk, s)

private def validFast (lo1 lo2 : Int) (fast : List (BitVec 64 × Int)) : Prop :=
  ∀ pk, pk ∈ fast.map Prod.fst → tryPack lo1 lo2 (dkey lo1 lo2 pk) = some pk

private def validFb (lo1 lo2 : Int) (fb : List (GroupKey × Int)) : Prop :=
  ∀ k, k ∈ fb.map Prod.fst → tryPack lo1 lo2 k = none

private theorem beq_true_mp {α : Type} [DecidableEq α] {a b : α} (h : (a == b) = true) :
    a = b := by
  rw [beq_iff_eq] at h
  exact h

private theorem beq_false_not {α : Type} [DecidableEq α] {a b : α} (h : (a == b) = false) :
    ¬ (a = b) := by
  intro he
  have htrue : (a == b) = true := by
    rw [beq_iff_eq]
    exact he
  exact absurd (htrue.symm.trans h) (by decide)

private theorem beq_false_not' {α : Type} [DecidableEq α] {a b : α} (h : (a == b) = false) :
    a ≠ b := fun he => beq_false_not h he

private theorem beq_ne {α : Type} [DecidableEq α] {a b : α} (h : ¬ (a = b)) :
    (a == b) = false := by
  cases h' : (a == b)
  · rfl
  · exact absurd (beq_true_mp h') h

private theorem lookup_addAssoc {α : Type} [DecidableEq α]
    (m : List (α × Int)) (k : α) (v : Int) (j : α) :
    lookupAssoc (addAssoc m k v) j =
      if k == j then s2 (lookupAssoc m j) (some v) else lookupAssoc m j := by
  induction m with
  | nil =>
      cases h : (k == j) <;> simp [addAssoc, lookupAssoc, s2, h]
  | cons p s ih =>
      cases p with
      | mk k₂ s₂ =>
          rw [addAssoc]
          by_cases hkk : k = j
          · -- k = j: RHS unfolds to s2 (lookupAssoc m j) (some v)
            rw [if_pos (beq_iff_eq.mpr hkk)]
            by_cases h₂k : k₂ = k
            · -- key replaced in place
              rw [if_pos (beq_iff_eq.mpr h₂k), lookupAssoc, lookupAssoc,
                  if_pos (beq_iff_eq.mpr (h₂k.trans hkk)),
                  if_pos (beq_iff_eq.mpr (h₂k.trans hkk))]
              exact rfl
            · -- k₂ ≠ k; since k = j this means k₂ ≠ j
              rw [if_neg (fun hc => h₂k (beq_iff_eq.mp hc)), lookupAssoc, ih,
                  if_pos (beq_iff_eq.mpr hkk), lookupAssoc]
              cases hz : (k₂ == j)
              · exact rfl
              · exact absurd ((beq_true_mp hz).trans hkk.symm) h₂k
          · -- k ≠ j: RHS keeps lookupAssoc m j
            rw [if_neg (fun hc => hkk (beq_iff_eq.mp hc))]
            by_cases h₂k : k₂ = k
            · -- replaced in place, and k₂ ≠ j since k ≠ j
              rw [if_pos (beq_iff_eq.mpr h₂k), lookupAssoc, lookupAssoc]
              cases hz : (k₂ == j)
              · exact rfl
              · exact absurd (h₂k ▸ beq_true_mp hz) hkk
            · -- untouched head
              rw [if_neg (fun hc => h₂k (beq_iff_eq.mp hc)), lookupAssoc, ih,
                  if_neg (fun hc => hkk (beq_iff_eq.mp hc)), lookupAssoc]

private def foldSum {α : Type} [DecidableEq α] (b : List (α × Int)) (j : α) : Option Int :=
  b.foldr (fun kv acc => s2 (if kv.1 == j then some kv.2 else none) acc) none

private theorem foldSum_cons {α : Type} [DecidableEq α] (kv : α × Int)
    (m : List (α × Int)) (j : α) :
    foldSum (kv :: m) j = s2 (if kv.1 == j then some kv.2 else none) (foldSum m j) := rfl

private theorem lookup_mergeAssoc {α : Type} [DecidableEq α]
    (b : List (α × Int)) (acc : List (α × Int)) (j : α) :
    lookupAssoc (mergeAssoc acc b) j = s2 (lookupAssoc acc j) (foldSum b j) := by
  induction b generalizing acc with
  | nil =>
      have h₁ : mergeAssoc acc [] = acc := rfl
      have h₂ : foldSum [] j = none := rfl
      rw [h₁, h₂, s2_none_right]
  | cons kv rest ih =>
      obtain ⟨k, v⟩ := kv
      have hstep : mergeAssoc acc ((k, v) :: rest) = mergeAssoc (addAssoc acc k v) rest := rfl
      rw [hstep, ih, lookup_addAssoc]
      cases h : (k == j)
      · have hfs : foldSum ((k, v) :: rest) j = s2 none (foldSum rest j) := by
          show s2 (if (k == j) = true then some v else none) (foldSum rest j) = _
          rw [if_neg (by simpa using h)]
        rw [hfs]
        simp only [s2_none_left]
        exact rfl
      · have hfs : foldSum ((k, v) :: rest) j = s2 (some v) (foldSum rest j) := by
          show s2 (if (k == j) = true then some v else none) (foldSum rest j) = _
          rw [if_pos h]
        rw [hfs]
        exact s2_assoc _ _ _

private theorem mem_addAssoc_fst {α : Type} [DecidableEq α]
    (m : List (α × Int)) (k : α) (v : Int) (j : α) :
    j ∈ (addAssoc m k v).map Prod.fst → j ∈ m.map Prod.fst ∨ j = k := by
  induction m with
  | nil =>
      intro h
      have h' : (addAssoc [] k v).map Prod.fst = [k] := rfl
      rw [h'] at h
      simp only [List.mem_cons, List.not_mem_nil, or_false] at h
      exact Or.inr h
  | cons p s ih =>
      cases p with
      | mk k₂ s₂ =>
          intro h
          cases hc : (k₂ == k)
          · rw [addAssoc, if_neg (by simpa using hc)] at h
            simp only [List.mem_cons, List.map_cons] at h ⊢
            rcases h with rfl | h'
            · exact Or.inl (Or.inl rfl)
            · rcases ih h' with him | hik
              · exact Or.inl (Or.inr him)
              · exact Or.inr hik
          · rw [addAssoc, if_pos hc] at h
            simp only [List.mem_cons, List.map_cons] at h ⊢
            rcases h with rfl | him
            · exact Or.inl (Or.inl rfl)
            · exact Or.inl (Or.inr him)

private theorem mem_mergeAssoc_fst {α : Type} [DecidableEq α]
    (b : List (α × Int)) (a : List (α × Int)) (j : α) :
    j ∈ (mergeAssoc a b).map Prod.fst → j ∈ a.map Prod.fst ∨ j ∈ b.map Prod.fst := by
  induction b generalizing a with
  | nil =>
      intro h
      exact Or.inl h
  | cons kv rest ih =>
      obtain ⟨k, v⟩ := kv
      have hstep : mergeAssoc a ((k, v) :: rest) = mergeAssoc (addAssoc a k v) rest := rfl
      intro h
      rw [hstep] at h
      rcases ih (addAssoc a k v) h with him | hj
      · rcases mem_addAssoc_fst a k v j him with h1 | h2
        · exact Or.inl h1
        · exact Or.inr (by simp [h2])
      · exact Or.inr (by simp [hj])

/-! ## foldSum / refGroupedSum semantics -/

private theorem foldSum_append {α : Type} [DecidableEq α] (a b : List (α × Int)) (j : α) :
    foldSum (a ++ b) j = s2 (foldSum a j) (foldSum b j) := by
  induction a generalizing b with
  | nil =>
      show foldSum b j = s2 none (foldSum b j)
      exact (s2_none_left _).symm
  | cons kv rest ih =>
      obtain ⟨k, v⟩ := kv
      have h₁ : foldSum ((k, v) :: rest ++ b) j =
          s2 (if k == j then some v else none) (foldSum (rest ++ b) j) := rfl
      have h₂ : foldSum ((k, v) :: rest) j =
          s2 (if k == j then some v else none) (foldSum rest j) := rfl
      rw [h₁, h₂, ih, s2_assoc]

private theorem foldSum_none_of_ne {α : Type} [DecidableEq α] (b : List (α × Int)) (j : α)
    (h : ∀ kv ∈ b, kv.1 ≠ j) : foldSum b j = none := by
  induction b with
  | nil => rfl
  | cons kv rest ih =>
      obtain ⟨k, v⟩ := kv
      have h₁ : foldSum ((k, v) :: rest) j =
          s2 (if k == j then some v else none) (foldSum rest j) := rfl
      have hne : ¬ ((k == j) = true) := fun hc =>
        h (k, v) (List.Mem.head _) (beq_iff_eq.mp hc)
      rw [h₁, if_neg hne, s2_none_left,
        ih (fun kv' hk => h kv' (List.mem_cons_of_mem _ hk))]

private def mapRaw (rows : List RawRow) : List (GroupKey × Int) :=
  rows.map fun (k1, k2, v) => ((k1, k2), v)

private theorem mapRaw_append (a b : List RawRow) :
    mapRaw (a ++ b) = mapRaw a ++ mapRaw b := by
  rw [mapRaw, mapRaw, mapRaw, List.map_append]

private theorem s2_swap3 (a b c : Option Int) :
    s2 a (s2 b c) = s2 (s2 c a) b := by
  rw [s2_comm b c, ← s2_assoc, s2_comm a c]

private theorem refLookup (rows : List RawRow) (acc : List (GroupKey × Int)) (g : GroupKey) :
    lookupAssoc (rows.foldl stepRef acc) g =
      s2 (foldSum (mapRaw rows) g) (lookupAssoc acc g) := by
  induction rows generalizing acc with
  | nil =>
      show lookupAssoc acc g = s2 (foldSum ([] : List (GroupKey × Int)) g) (lookupAssoc acc g)
      have h : foldSum ([] : List (GroupKey × Int)) g = none := rfl
      rw [h, s2_none_left]
  | cons r rest ih =>
      obtain ⟨k1, k2, v⟩ := r
      have h₁ : ((k1, k2, v) :: rest).foldl stepRef acc =
          rest.foldl stepRef (addAssoc acc (k1, k2) v) := rfl
      have h₂ : mapRaw ((k1, k2, v) :: rest) = ((k1, k2), v) :: mapRaw rest := rfl
      rw [h₁, ih, lookup_addAssoc]
      simp only [beq_iff_eq]
      by_cases hprop : (k1, k2) = g
      · rw [if_pos hprop, h₂, foldSum_cons]
        simp only [beq_iff_eq]
        simp only [if_pos hprop]
        exact s2_swap3 _ _ _
      · rw [if_neg hprop, h₂, foldSum_cons]
        simp only [beq_iff_eq]
        simp only [if_neg hprop, s2_none_left]

/-! ## tryPack / dkey bridges -/

private theorem tryPack_some_form (lo1 lo2 : Int) (g : GroupKey) (pk : BitVec 64)
    (h : tryPack lo1 lo2 g = some pk) : ∃ a b : Int, g = (some a, some b) := by
  cases g with
  | mk x y =>
      cases x with
      | none => exact absurd h (by cases y <;> simp [tryPack])
      | some a =>
          cases y with
          | none => exact absurd h (by simp [tryPack])
          | some b => exact ⟨a, b, by simp⟩

private theorem pack_inj (lo1 lo2 : Int) (g₁ g₂ : GroupKey) (pk : BitVec 64)
    (h₁ : tryPack lo1 lo2 g₁ = some pk) (h₂ : tryPack lo1 lo2 g₂ = some pk) : g₁ = g₂ := by
  obtain ⟨a₁, b₁, hg₁⟩ := tryPack_some_form lo1 lo2 g₁ pk h₁
  obtain ⟨a₂, b₂, hg₂⟩ := tryPack_some_form lo1 lo2 g₂ pk h₂
  have u₁ : unpack lo1 lo2 pk = (a₁, b₁) :=
    unpack_of_tryPack lo1 lo2 a₁ b₁ pk (by rw [← hg₁]; exact h₁)
  have u₂ : unpack lo1 lo2 pk = (a₂, b₂) :=
    unpack_of_tryPack lo1 lo2 a₂ b₂ pk (by rw [← hg₂]; exact h₂)
  have hab : (a₁, b₁) = (a₂, b₂) := u₁.symm.trans u₂
  rw [hg₁, hg₂]
  exact congrArg (fun p : Int × Int => (some p.1, some p.2)) hab

private theorem dkey_of_pack (lo1 lo2 : Int) (g : GroupKey) (pk : BitVec 64)
    (hT : tryPack lo1 lo2 g = some pk) : dkey lo1 lo2 pk = g := by
  obtain ⟨a, b, hg⟩ := tryPack_some_form lo1 lo2 g pk hT
  have u : unpack lo1 lo2 pk = (a, b) :=
    unpack_of_tryPack lo1 lo2 a b pk (by rw [← hg]; exact hT)
  have h₁ : dkey lo1 lo2 pk = (some (unpack lo1 lo2 pk).1, some (unpack lo1 lo2 pk).2) := rfl
  rw [hg, h₁, u]

private theorem prov_some (lo1 lo2 : Int) (k : GroupKey) (pk : BitVec 64)
    (hT : tryPack lo1 lo2 k = some pk) : tryPack lo1 lo2 (dkey lo1 lo2 pk) = some pk := by
  rw [dkey_of_pack lo1 lo2 k pk hT]
  exact hT

/-! ## Validity is preserved by addAssoc / mergeAssoc / worker folds -/

private theorem validFast_nil (lo1 lo2 : Int) : validFast lo1 lo2 [] := by
  intro pk h
  exact absurd h (by simp)

private theorem validFb_nil (lo1 lo2 : Int) : validFb lo1 lo2 [] := by
  intro k h
  exact absurd h (by simp)

private theorem validFast_addAssoc (lo1 lo2 : Int) (fast : List (BitVec 64 × Int))
    (pk : BitVec 64) (v : Int) (h1 : validFast lo1 lo2 fast)
    (h2 : tryPack lo1 lo2 (dkey lo1 lo2 pk) = some pk) :
    validFast lo1 lo2 (addAssoc fast pk v) := by
  intro j hj
  rcases mem_addAssoc_fst fast pk v j hj with hj' | he
  · exact h1 j hj'
  · rw [he]
    exact h2

private theorem validFb_addAssoc (lo1 lo2 : Int) (fb : List (GroupKey × Int))
    (k : GroupKey) (v : Int) (h1 : validFb lo1 lo2 fb)
    (h2 : tryPack lo1 lo2 k = none) : validFb lo1 lo2 (addAssoc fb k v) := by
  intro j hj
  rcases mem_addAssoc_fst fb k v j hj with hj' | he
  · exact h1 j hj'
  · rw [he]
    exact h2

private theorem validFast_mergeAssoc (lo1 lo2 : Int) (a b : List (BitVec 64 × Int))
    (ha : validFast lo1 lo2 a) (hb : validFast lo1 lo2 b) :
    validFast lo1 lo2 (mergeAssoc a b) := by
  intro j hj
  rcases mem_mergeAssoc_fst b a j hj with ha' | hb'
  · exact ha j ha'
  · exact hb j hb'

private theorem validFb_mergeAssoc (lo1 lo2 : Int) (a b : List (GroupKey × Int))
    (ha : validFb lo1 lo2 a) (hb : validFb lo1 lo2 b) :
    validFb lo1 lo2 (mergeAssoc a b) := by
  intro j hj
  rcases mem_mergeAssoc_fst b a j hj with ha' | hb'
  · exact ha j ha'
  · exact hb j hb'

private theorem valid_worker (lo1 lo2 : Int) :
    ∀ rows : List RawRow, ∀ st : WorkerAgg, validFast lo1 lo2 st.fast →
      validFb lo1 lo2 st.fallback →
      validFast lo1 lo2 (workerFold lo1 lo2 rows st).fast ∧
        validFb lo1 lo2 (workerFold lo1 lo2 rows st).fallback := by
  intro rows
  induction rows with
  | nil => exact fun st h1 h2 => ⟨h1, h2⟩
  | cons r rest ih =>
      intro st hfa hfb
      obtain ⟨k1, k2, v⟩ := r
      have hstep : workerFold lo1 lo2 ((k1, k2, v) :: rest) st =
          workerFold lo1 lo2 rest (stepAgg lo1 lo2 st k1 k2 v) := rfl
      rw [hstep]
      cases hT : (tryPack lo1 lo2 (k1, k2)) with
      | some pk =>
          have hsf : (stepAgg lo1 lo2 st k1 k2 v).fast = addAssoc st.fast pk v := by
            show (match (tryPack lo1 lo2 (k1, k2) : Option (BitVec 64)) with
              | some pk' => { st with fast := addAssoc st.fast pk' v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fast = _
            rw [hT]
          have hsb : (stepAgg lo1 lo2 st k1 k2 v).fallback = st.fallback := by
            show (match (tryPack lo1 lo2 (k1, k2) : Option (BitVec 64)) with
              | some pk' => { st with fast := addAssoc st.fast pk' v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fallback = _
            rw [hT]
          have hprov : tryPack lo1 lo2 (dkey lo1 lo2 pk) = some pk :=
            prov_some lo1 lo2 (k1, k2) pk hT
          obtain ⟨i1, i2⟩ := ih (stepAgg lo1 lo2 st k1 k2 v)
            (by rw [hsf]; exact validFast_addAssoc lo1 lo2 st.fast pk v hfa hprov)
            (by rw [hsb]; exact hfb)
          exact ⟨i1, i2⟩
      | none =>
          have hsf : (stepAgg lo1 lo2 st k1 k2 v).fast = st.fast := by
            show (match (tryPack lo1 lo2 (k1, k2) : Option (BitVec 64)) with
              | some pk' => { st with fast := addAssoc st.fast pk' v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fast = _
            rw [hT]
          have hsb : (stepAgg lo1 lo2 st k1 k2 v).fallback =
              addAssoc st.fallback (k1, k2) v := by
            show (match (tryPack lo1 lo2 (k1, k2) : Option (BitVec 64)) with
              | some pk' => { st with fast := addAssoc st.fast pk' v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fallback = _
            rw [hT]
          obtain ⟨i1, i2⟩ := ih (stepAgg lo1 lo2 st k1 k2 v) (by rw [hsf]; exact hfa)
            (by rw [hsb]; exact validFb_addAssoc lo1 lo2 st.fallback (k1, k2) v hfb hT)
          exact ⟨i1, i2⟩

private theorem worker_valid (lo1 lo2 : Int) (rows : List RawRow) :
    validFast lo1 lo2 (workerAccum lo1 lo2 rows).fast ∧
      validFb lo1 lo2 (workerAccum lo1 lo2 rows).fallback := by
  rw [workerAccum_eq]
  exact valid_worker lo1 lo2 rows {} (validFast_nil lo1 lo2) (validFb_nil lo1 lo2)

private theorem valid_aggs (lo1 lo2 : Int) :
    ∀ ags : List WorkerAgg, (∀ a ∈ ags, validFast lo1 lo2 a.fast ∧ validFb lo1 lo2 a.fallback) →
      ∀ af : List (BitVec 64 × Int), ∀ fb : List (GroupKey × Int),
        validFast lo1 lo2 af → validFb lo1 lo2 fb →
        validFast lo1 lo2 (ags.foldl (fun acc a => mergeAssoc acc a.fast) af) ∧
          validFb lo1 lo2 (ags.foldl (fun acc a => mergeAssoc acc a.fallback) fb) := by
  intro ags
  induction ags with
  | nil => exact fun _ af fb h1 h2 => ⟨h1, h2⟩
  | cons a rest ih =>
      intro h af fb hfa hfb
      have hf : validFast lo1 lo2 a.fast ∧ validFb lo1 lo2 a.fallback :=
        h a (by simp)
      have h₁ : (a :: rest).foldl (fun acc x => mergeAssoc acc x.fast) af =
          rest.foldl (fun acc x => mergeAssoc acc x.fast) (mergeAssoc af a.fast) := rfl
      have h₂ : (a :: rest).foldl (fun acc x => mergeAssoc acc x.fallback) fb =
          rest.foldl (fun acc x => mergeAssoc acc x.fallback) (mergeAssoc fb a.fallback) := rfl
      obtain ⟨i1, i2⟩ := ih (fun x hx => h x (by simp only [List.mem_cons]; exact Or.inr hx))
        (mergeAssoc af a.fast) (mergeAssoc fb a.fallback)
        (validFast_mergeAssoc lo1 lo2 af a.fast hfa hf.1)
        (validFb_mergeAssoc lo1 lo2 fb a.fallback hfb hf.2)
      exact ⟨by rw [h₁]; exact i1, by rw [h₂]; exact i2⟩
/-! ## foldSum through addAssoc / worker folds / merge folds -/

private theorem foldSum_addAssoc {α : Type} [DecidableEq α] (m : List (α × Int)) (k : α)
    (v : Int) (j : α) :
    foldSum (addAssoc m k v) j = s2 (foldSum m j) (if k == j then some v else none) := by
  induction m with
  | nil =>
      have h₁ : foldSum (addAssoc ([] : List (α × Int)) k v) j =
          s2 (if k == j then some v else none) (foldSum ([] : List (α × Int)) j) := rfl
      have hz : foldSum ([] : List (α × Int)) j = none := rfl
      rw [h₁, hz, s2_none_right, s2_none_left]
  | cons p s ih =>
      cases p with
      | mk k₂ s₂ =>
          rw [addAssoc]
          have hL : foldSum ((k₂, s₂ + v) :: s) j =
              s2 (if k₂ == j then some (s₂ + v) else none) (foldSum s j) := rfl
          have hL' : foldSum ((k₂, s₂) :: addAssoc s k v) j =
              s2 (if k₂ == j then some s₂ else none) (foldSum (addAssoc s k v) j) := rfl
          have hR : foldSum ((k₂, s₂) :: s) j =
              s2 (if k₂ == j then some s₂ else none) (foldSum s j) := rfl
          cases h1 : (k₂ == k)
          · rw [if_neg (by simpa using h1), hL', ih, hR, s2_assoc]
          · have h1p : k₂ = k := beq_true_mp h1
            show foldSum ((k₂, s₂ + v) :: s) j =
              s2 (foldSum ((k₂, s₂) :: s) j) (if k == j then some v else none)
            rw [hL, hR]
            cases hz : (k₂ == j)
            · have hkj' : (k == j) = false := by rw [← h1p]; exact hz
              rw [hkj']
              have hf : ¬ (false = true) := by simp
              simp only [if_neg hf, s2_none_left, s2_none_right]
            · have hkj' : (k == j) = true := by rw [← h1p]; exact hz
              rw [hkj']
              cases hx : foldSum s j <;> simp [s2, hx] <;> omega

private theorem foldSum_worker_some (lo1 lo2 : Int) (g : GroupKey) (pk : BitVec 64)
    (hT : tryPack lo1 lo2 g = some pk) :
    ∀ rows : List RawRow, ∀ st : WorkerAgg,
      foldSum (workerFold lo1 lo2 rows st).fast pk =
        s2 (foldSum (mapRaw rows) g) (foldSum st.fast pk) := by
  intro rows
  induction rows with
  | nil =>
      intro st
      show foldSum st.fast pk = s2 (foldSum ([] : List (GroupKey × Int)) g) (foldSum st.fast pk)
      have h : foldSum ([] : List (GroupKey × Int)) g = none := rfl
      rw [h, s2_none_left]
  | cons r rest ih =>
      intro st
      obtain ⟨k1, k2, v⟩ := r
      have hstep : workerFold lo1 lo2 ((k1, k2, v) :: rest) st =
          workerFold lo1 lo2 rest (stepAgg lo1 lo2 st k1 k2 v) := rfl
      have h₂ : mapRaw ((k1, k2, v) :: rest) = ((k1, k2), v) :: mapRaw rest := rfl
      rw [hstep, h₂, foldSum_cons, ih]
      simp only [beq_iff_eq]
      cases hT' : (tryPack lo1 lo2 (k1, k2)) with
      | some pk' =>
          have hsf : (stepAgg lo1 lo2 st k1 k2 v).fast = addAssoc st.fast pk' v := by
            show (match (tryPack lo1 lo2 (k1, k2)) with
              | some q => { st with fast := addAssoc st.fast q v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fast = _
            rw [hT']
          rw [hsf, foldSum_addAssoc]
          simp only [beq_iff_eq]
          by_cases hc : pk' = pk
          · have hgg : (k1, k2) = g :=
              pack_inj lo1 lo2 (k1, k2) g pk (hT'.trans (congrArg some hc)) hT
            simp only [if_pos hc, if_pos hgg]
            exact s2_swap3 _ _ _
          · have hne : ¬ ((k1, k2) = g) := fun he =>
              hc (Option.some.inj ((hT'.symm.trans (congrArg (tryPack lo1 lo2) he)).trans hT))
            simp only [if_neg hc, if_neg hne, s2_none_right, s2_none_left]
      | none =>
          have hsf : (stepAgg lo1 lo2 st k1 k2 v).fast = st.fast := by
            show (match (tryPack lo1 lo2 (k1, k2)) with
              | some q => { st with fast := addAssoc st.fast q v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fast = _
            rw [hT']
          have hne : ¬ ((k1, k2) = g) := fun he => by
            have h : none = some pk :=
              (hT'.symm.trans (congrArg (tryPack lo1 lo2) he)).trans hT
            exact absurd h (by simp)
          rw [hsf]
          simp only [if_neg hne, s2_none_left]

private theorem foldSum_worker_none (lo1 lo2 : Int) (g : GroupKey) (hg : tryPack lo1 lo2 g = none) :
    ∀ rows : List RawRow, ∀ st : WorkerAgg,
      foldSum (workerFold lo1 lo2 rows st).fallback g =
        s2 (foldSum (mapRaw rows) g) (foldSum st.fallback g) := by
  intro rows
  induction rows with
  | nil =>
      intro st
      show foldSum st.fallback g =
        s2 (foldSum ([] : List (GroupKey × Int)) g) (foldSum st.fallback g)
      have h : foldSum ([] : List (GroupKey × Int)) g = none := rfl
      rw [h, s2_none_left]
  | cons r rest ih =>
      intro st
      obtain ⟨k1, k2, v⟩ := r
      have hstep : workerFold lo1 lo2 ((k1, k2, v) :: rest) st =
          workerFold lo1 lo2 rest (stepAgg lo1 lo2 st k1 k2 v) := rfl
      have h₂ : mapRaw ((k1, k2, v) :: rest) = ((k1, k2), v) :: mapRaw rest := rfl
      rw [hstep, h₂, foldSum_cons, ih]
      simp only [beq_iff_eq]
      cases hT' : (tryPack lo1 lo2 (k1, k2)) with
      | some pk' =>
          have hsb : (stepAgg lo1 lo2 st k1 k2 v).fallback = st.fallback := by
            show (match (tryPack lo1 lo2 (k1, k2)) with
              | some q => { st with fast := addAssoc st.fast q v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fallback = _
            rw [hT']
          have hne : ¬ ((k1, k2) = g) := fun he => by
            have h : some pk' = none :=
              (hT'.symm.trans (congrArg (tryPack lo1 lo2) he)).trans hg
            exact absurd h (by simp)
          rw [hsb]
          simp only [if_neg hne, s2_none_left]
      | none =>
          have hsb : (stepAgg lo1 lo2 st k1 k2 v).fallback =
              addAssoc st.fallback (k1, k2) v := by
            show (match (tryPack lo1 lo2 (k1, k2)) with
              | some q => { st with fast := addAssoc st.fast q v }
              | none => { st with fallback := addAssoc st.fallback (k1, k2) v }).fallback = _
            rw [hT']
          rw [hsb, foldSum_addAssoc]
          simp only [beq_iff_eq]
          by_cases hprop : (k1, k2) = g
          · simp only [if_pos hprop]
            exact s2_swap3 _ _ _
          · simp only [if_neg hprop, s2_none_right, s2_none_left]

/-! ## Merge folds and decode -/

private theorem mergeFold_fast (lo1 lo2 : Int) (pk : BitVec 64) (ags : List WorkerAgg)
    (acc : List (BitVec 64 × Int)) :
    lookupAssoc (ags.foldl (fun a w => mergeAssoc a w.fast) acc) pk =
      s2 (foldSum ((ags.map fun w => w.fast).flatten) pk) (lookupAssoc acc pk) := by
  induction ags generalizing acc with
  | nil =>
      show lookupAssoc acc pk =
        s2 (foldSum ([] : List (BitVec 64 × Int)) pk) (lookupAssoc acc pk)
      have h : foldSum ([] : List (BitVec 64 × Int)) pk = none := rfl
      rw [h, s2_none_left]
  | cons a rest ih =>
      have h₁ : (a :: rest).foldl (fun x w => mergeAssoc x w.fast) acc =
          rest.foldl (fun x w => mergeAssoc x w.fast) (mergeAssoc acc a.fast) := rfl
      have h₂ : ((a :: rest).map fun w => w.fast).flatten =
          a.fast ++ ((rest.map fun w => w.fast).flatten) := rfl
      rw [h₁, h₂, ih, foldSum_append, lookup_mergeAssoc, s2_swap3]

private theorem mergeFold_fb (lo1 lo2 : Int) (g : GroupKey) (ags : List WorkerAgg)
    (acc : List (GroupKey × Int)) :
    lookupAssoc (ags.foldl (fun a w => mergeAssoc a w.fallback) acc) g =
      s2 (foldSum ((ags.map fun w => w.fallback).flatten) g) (lookupAssoc acc g) := by
  induction ags generalizing acc with
  | nil =>
      show lookupAssoc acc g =
        s2 (foldSum ([] : List (GroupKey × Int)) g) (lookupAssoc acc g)
      have h : foldSum ([] : List (GroupKey × Int)) g = none := rfl
      rw [h, s2_none_left]
  | cons a rest ih =>
      have h₁ : (a :: rest).foldl (fun x w => mergeAssoc x w.fallback) acc =
          rest.foldl (fun x w => mergeAssoc x w.fallback) (mergeAssoc acc a.fallback) := rfl
      have h₂ : ((a :: rest).map fun w => w.fallback).flatten =
          a.fallback ++ ((rest.map fun w => w.fallback).flatten) := rfl
      rw [h₁, h₂, ih, foldSum_append, lookup_mergeAssoc, s2_swap3]

private theorem foldSum_fast_join (lo1 lo2 : Int) (g : GroupKey) (pk : BitVec 64)
    (hT : tryPack lo1 lo2 g = some pk) :
    ∀ ws : List (List RawRow),
      foldSum ((ws.map fun w => (workerAccum lo1 lo2 w).fast).flatten) pk =
        foldSum (mapRaw ws.flatten) g := by
  intro ws
  induction ws with
  | nil => rfl
  | cons w rest ih =>
      have h₁ : ((w :: rest).map fun x => (workerAccum lo1 lo2 x).fast).flatten =
          (workerAccum lo1 lo2 w).fast ++
            ((rest.map fun x => (workerAccum lo1 lo2 x).fast).flatten) := rfl
      have h₂ : mapRaw ((w :: rest).flatten) = mapRaw w ++ mapRaw rest.flatten :=
        (mapRaw_append w rest.flatten)
      have h₃ : foldSum (workerAccum lo1 lo2 w).fast pk = foldSum (mapRaw w) g := by
        have h₄ := foldSum_worker_some lo1 lo2 g pk hT w {}
        have h₅ : ({} : WorkerAgg).fast = [] := rfl
        rw [h₅, show foldSum ([] : List (BitVec 64 × Int)) pk = none from rfl, s2_none_right] at h₄
        rw [congrArg (fun l : List (BitVec 64 × Int) => foldSum l pk)
          (congrArg (fun s : WorkerAgg => s.fast) (workerAccum_eq lo1 lo2 w))]
        exact h₄
      rw [h₁, h₂, foldSum_append, foldSum_append, h₃, ih]

private theorem foldSum_fb_join (lo1 lo2 : Int) (g : GroupKey) (hg : tryPack lo1 lo2 g = none) :
    ∀ ws : List (List RawRow),
      foldSum ((ws.map fun w => (workerAccum lo1 lo2 w).fallback).flatten) g =
        foldSum (mapRaw ws.flatten) g := by
  intro ws
  induction ws with
  | nil => rfl
  | cons w rest ih =>
      have h₁ : ((w :: rest).map fun x => (workerAccum lo1 lo2 x).fallback).flatten =
          (workerAccum lo1 lo2 w).fallback ++
            ((rest.map fun x => (workerAccum lo1 lo2 x).fallback).flatten) := rfl
      have h₂ : mapRaw ((w :: rest).flatten) = mapRaw w ++ mapRaw rest.flatten :=
        (mapRaw_append w rest.flatten)
      have h₃ : foldSum (workerAccum lo1 lo2 w).fallback g = foldSum (mapRaw w) g := by
        have h₄ := foldSum_worker_none lo1 lo2 g hg w {}
        have h₅ : ({} : WorkerAgg).fallback = [] := rfl
        rw [h₅, show foldSum ([] : List (GroupKey × Int)) g = none from rfl, s2_none_right] at h₄
        rw [congrArg (fun l : List (GroupKey × Int) => foldSum l g)
          (congrArg (fun s : WorkerAgg => s.fallback) (workerAccum_eq lo1 lo2 w))]
        exact h₄
      rw [h₁, h₂, foldSum_append, foldSum_append, h₃, ih]

private theorem lookup_decode_some (lo1 lo2 : Int) (g : GroupKey) (pk : BitVec 64)
    (hT : tryPack lo1 lo2 g = some pk) (fast : List (BitVec 64 × Int))
    (hv : validFast lo1 lo2 fast) :
    lookupAssoc (decodeFast lo1 lo2 fast) g = lookupAssoc fast pk := by
  have hdpk : dkey lo1 lo2 pk = g := dkey_of_pack lo1 lo2 g pk hT
  induction fast with
  | nil => rfl
  | cons ps rest ih =>
      obtain ⟨pk', s⟩ := ps
      have hmem : tryPack lo1 lo2 (dkey lo1 lo2 pk') = some pk' :=
        hv pk' (List.mem_map.2 ⟨(pk', s), List.Mem.head _, rfl⟩)
      have hstep : decodeFast lo1 lo2 ((pk', s) :: rest) =
          (dkey lo1 lo2 pk', s) :: decodeFast lo1 lo2 rest := rfl
      have ihv : validFast lo1 lo2 rest := by
        intro j hj
        obtain ⟨y, hy, hxy⟩ := List.mem_map.1 hj
        exact hv j (List.mem_map.2 ⟨y, List.Mem.tail _ hy, hxy⟩)
      rw [hstep, lookupAssoc, lookupAssoc]
      simp only [beq_iff_eq]
      by_cases hc : pk' = pk
      · have hgk : dkey lo1 lo2 pk' = g := (congrArg (dkey lo1 lo2) hc).trans hdpk
        rw [if_pos hc, if_pos hgk]
      · have hneg : ¬ (dkey lo1 lo2 pk' = g) := fun he => by
          have h : some pk' = some pk :=
            (hmem.symm.trans (congrArg (tryPack lo1 lo2) he)).trans hT
          exact hc (Option.some.inj h)
        rw [if_neg hc, if_neg hneg, ih ihv]

private theorem lookup_decode_none (lo1 lo2 : Int) (g : GroupKey)
    (hg : tryPack lo1 lo2 g = none) (fast : List (BitVec 64 × Int))
    (hv : validFast lo1 lo2 fast) : lookupAssoc (decodeFast lo1 lo2 fast) g = none := by
  induction fast with
  | nil => rfl
  | cons ps rest ih =>
      obtain ⟨pk', s⟩ := ps
      have hmem : tryPack lo1 lo2 (dkey lo1 lo2 pk') = some pk' :=
        hv pk' (List.mem_map.2 ⟨(pk', s), List.Mem.head _, rfl⟩)
      have hstep : decodeFast lo1 lo2 ((pk', s) :: rest) =
          (dkey lo1 lo2 pk', s) :: decodeFast lo1 lo2 rest := rfl
      have hne : ¬ (dkey lo1 lo2 pk' = g) := fun he => by
        have h : none = some pk' := (hg.symm.trans (congrArg (tryPack lo1 lo2) he.symm)).trans hmem
        exact absurd h (by simp)
      have ihv : validFast lo1 lo2 rest := by
        intro j hj
        obtain ⟨y, hy, hxy⟩ := List.mem_map.1 hj
        exact hv j (List.mem_map.2 ⟨y, List.Mem.tail _ hy, hxy⟩)
      rw [hstep, lookupAssoc]
      simp only [beq_iff_eq]
      rw [if_neg hne, ih ihv]
/-! ## Assembly of Goal parts 2 and 3 -/

private def fastM (lo1 lo2 : Int) (ws : List (List RawRow)) : List (BitVec 64 × Int) :=
  (ws.map (workerAccum lo1 lo2)).foldl (fun acc w => mergeAssoc acc w.fast) []

private def fbM (lo1 lo2 : Int) (ws : List (List RawRow)) : List (GroupKey × Int) :=
  (ws.map (workerAccum lo1 lo2)).foldl (fun acc w => mergeAssoc acc w.fallback) []

private theorem fastFlatten_bridge (lo1 lo2 : Int) (ws : List (List RawRow)) :
    ((ws.map (workerAccum lo1 lo2)).map fun w => w.fast).flatten =
      (ws.map fun w => (workerAccum lo1 lo2 w).fast).flatten := by
  induction ws with
  | nil => rfl
  | cons a t ih =>
      show (workerAccum lo1 lo2 a).fast ++
          ((t.map (workerAccum lo1 lo2)).map fun w => w.fast).flatten =
        (workerAccum lo1 lo2 a).fast ++ (t.map fun w => (workerAccum lo1 lo2 w).fast).flatten
      rw [ih]

private theorem fbFlatten_bridge (lo1 lo2 : Int) (ws : List (List RawRow)) :
    ((ws.map (workerAccum lo1 lo2)).map fun w => w.fallback).flatten =
      (ws.map fun w => (workerAccum lo1 lo2 w).fallback).flatten := by
  induction ws with
  | nil => rfl
  | cons a t ih =>
      show (workerAccum lo1 lo2 a).fallback ++
          ((t.map (workerAccum lo1 lo2)).map fun w => w.fallback).flatten =
        (workerAccum lo1 lo2 a).fallback ++
          (t.map fun w => (workerAccum lo1 lo2 w).fallback).flatten
      rw [ih]

private theorem merged_eq (lo1 lo2 : Int) (ws : List (List RawRow)) :
    mergedGroupAgg lo1 lo2 ws = (decodeFast lo1 lo2 (fastM lo1 lo2 ws), fbM lo1 lo2 ws) := rfl

private theorem validF_merged (lo1 lo2 : Int) (workers : List (List RawRow)) :
    validFast lo1 lo2 (fastM lo1 lo2 workers) ∧ validFb lo1 lo2 (fbM lo1 lo2 workers) :=
  valid_aggs lo1 lo2 (workers.map (workerAccum lo1 lo2))
    (fun a ha => by obtain ⟨w, hw, rfl⟩ := List.mem_map.1 ha; exact worker_valid lo1 lo2 w)
    [] [] (validFast_nil lo1 lo2) (validFb_nil lo1 lo2)

private theorem sound_part2 : ∀ (lo1 lo2 : Int) (workers : List (List RawRow)),
    let (fastDecoded, fbMerged) := mergedGroupAgg lo1 lo2 workers
    ∀ p ∈ fastDecoded, ∀ q ∈ fbMerged, p.1 ≠ q.1 := by
  intro lo1 lo2 workers p hp q hq he
  obtain ⟨vfm, vbm⟩ := validF_merged lo1 lo2 workers
  obtain ⟨x, hx, rfl⟩ := List.mem_map.1 hp
  cases x with
  | mk pk s =>
      have htry : tryPack lo1 lo2 (dkey lo1 lo2 pk) = some pk :=
        vfm pk (List.mem_map.2 ⟨(pk, s), hx, rfl⟩)
      have htry' : tryPack lo1 lo2 q.1 = none :=
        vbm q.1 (List.mem_map.2 ⟨q, hq, rfl⟩)
      have hk : dkey lo1 lo2 pk = q.1 := he
      have h : some pk = none := by rw [← htry, ← htry', ← hk]
      exact absurd h (by simp)

private theorem sound_part3 : ∀ (lo1 lo2 : Int) (workers : List (List RawRow)) (g : GroupKey),
    queryMergedSum lo1 lo2 workers g = lookupAssoc (refGroupedSum workers.flatten) g := by
  intro lo1 lo2 workers g
  obtain ⟨vfm, _⟩ := validF_merged lo1 lo2 workers
  have hR : lookupAssoc (refGroupedSum workers.flatten) g = foldSum (mapRaw workers.flatten) g := by
    rw [refGroupedSum_eq, refLookup]
    have h : lookupAssoc ([] : List (GroupKey × Int)) g = none := rfl
    rw [h, s2_none_right]
  cases hT : tryPack lo1 lo2 g with
  | some pk =>
      have hfa : lookupAssoc (fastM lo1 lo2 workers) pk = foldSum (mapRaw workers.flatten) g := by
        unfold fastM
        rw [mergeFold_fast lo1 lo2 pk (workers.map (workerAccum lo1 lo2)) [],
          show lookupAssoc ([] : List (BitVec 64 × Int)) pk = none from rfl, s2_none_right,
          fastFlatten_bridge lo1 lo2 workers,
          foldSum_fast_join lo1 lo2 g pk hT workers]
      have hdec : lookupAssoc (decodeFast lo1 lo2 (fastM lo1 lo2 workers)) g =
          foldSum (mapRaw workers.flatten) g := by
        rw [lookup_decode_some lo1 lo2 g pk hT (fastM lo1 lo2 workers) vfm, hfa]
      have hfb : lookupAssoc (fbM lo1 lo2 workers) g = none := by
        unfold fbM
        rw [mergeFold_fb lo1 lo2 g (workers.map (workerAccum lo1 lo2)) [],
          show lookupAssoc ([] : List (GroupKey × Int)) g = none from rfl, s2_none_right]
        refine foldSum_none_of_ne _ _ ?_
        intro kv hk heq
        obtain ⟨fl, hfl, hkin⟩ := List.mem_flatten.1 hk
        obtain ⟨w, hw, rfl⟩ := List.mem_map.1 hfl
        obtain ⟨w', hw', rfl⟩ := List.mem_map.1 hw
        obtain ⟨_, vbm'⟩ := worker_valid lo1 lo2 w'
        have htry : tryPack lo1 lo2 kv.1 = none :=
          vbm' kv.1 (List.mem_map.2 ⟨kv, hkin, rfl⟩)
        have h : none = some pk := by rw [← htry, heq, hT]
        exact absurd h (by simp)
      have hmain : queryMergedSum lo1 lo2 workers g = foldSum (mapRaw workers.flatten) g := by
        show (match lookupAssoc (decodeFast lo1 lo2 (fastM lo1 lo2 workers)) g with
          | some s => some s
          | none => lookupAssoc (fbM lo1 lo2 workers) g) = foldSum (mapRaw workers.flatten) g
        rw [hdec, hfb]
        cases hS : foldSum (mapRaw workers.flatten) g <;> simp [hS]
      exact hmain.trans hR.symm
  | none =>
      have hdec : lookupAssoc (decodeFast lo1 lo2 (fastM lo1 lo2 workers)) g = none :=
        lookup_decode_none lo1 lo2 g hT (fastM lo1 lo2 workers) vfm
      have hfb : lookupAssoc (fbM lo1 lo2 workers) g = foldSum (mapRaw workers.flatten) g := by
        unfold fbM
        rw [mergeFold_fb lo1 lo2 g (workers.map (workerAccum lo1 lo2)) [],
          show lookupAssoc ([] : List (GroupKey × Int)) g = none from rfl, s2_none_right,
          fbFlatten_bridge lo1 lo2 workers,
          foldSum_fb_join lo1 lo2 g hT workers]
      have hmain : queryMergedSum lo1 lo2 workers g = foldSum (mapRaw workers.flatten) g := by
        show (match lookupAssoc (decodeFast lo1 lo2 (fastM lo1 lo2 workers)) g with
          | some s => some s
          | none => lookupAssoc (fbM lo1 lo2 workers) g) = foldSum (mapRaw workers.flatten) g
        rw [hdec, hfb]
      exact hmain.trans hR.symm

theorem sound : TinylambOpt.ResearchGoals.PackedRadix2x32BatchAgg.Goal :=
  ⟨unpack_of_tryPack, sound_part2, sound_part3⟩

end TinylambOpt.ResearchProofs.PackedRadix2x32BatchAgg
