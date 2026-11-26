/-
Proves the PDEP deposit-soundness goal: `deposit n src mask` deposits the
lowest `n` source bits of `src` (consumed LSB-first) onto the set-bit lanes
of `mask`. The proof shows, by induction on `n`, that the set-bit lanes of
`deposit n src mask` are exactly the `mask`-lanes of rank `< n` whose
corresponding source bit is set, and finishes the `n = 64` case by removing
the vacuous rank bound.
-/
import Std
import TinylambOpt.ResearchGoals.SelectionMaskDeposit

open TinylambOpt.ResearchGoals.SelectionMaskDeposit

namespace TinylambOpt.ResearchProofs.SelectionMaskDeposit

private theorem bv_false {w : W} {i : Nat} (h : ¬ w.getLsbD i) : w.getLsbD i = false := by
  cases v : w.getLsbD i with
  | false => rfl
  | true => exact absurd v h

private theorem getLsbD_lt {w : W} {i : Nat} (h : w.getLsbD i = true) : i < 64 := by
  have h1 : (2 : Nat) ^ i ≤ w.toNat := Nat.ge_two_pow_of_testBit h
  have h2 : w.toNat < 2 ^ 64 := BitVec.isLt w
  rcases Nat.lt_trichotomy i 64 with hi | hi | hi
  · exact hi
  · have h3 : (2 : Nat) ^ 64 ≤ 2 ^ i :=
      Nat.pow_le_pow_right (by decide : (0 : Nat) < 2) (by omega : 64 ≤ i)
    omega
  · have h3 : (2 : Nat) ^ 64 ≤ 2 ^ i :=
      Nat.pow_le_pow_right (by decide : (0 : Nat) < 2) (by omega : 64 ≤ i)
    omega

private theorem lanes_mem {w : W} {i : Nat} (h64 : i < 64) (h : w.getLsbD i = true) :
    i ∈ lanes w :=
  List.mem_filter.mpr ⟨List.mem_range.mpr h64, h⟩

private theorem getLsbD_eq_mem_lanes (w : W) (i : Nat) :
    w.getLsbD i = decide (i ∈ lanes w) := by
  by_cases hb : i ∈ lanes w
  · rw [decide_eq_true hb]
    exact (List.mem_filter.mp hb).2
  · rw [decide_eq_false hb]
    cases v : w.getLsbD i with
    | false => rfl
    | true => exact absurd (lanes_mem (getLsbD_lt v) v) hb

private theorem lanes_eq_nil_iff (w : W) : lanes w = [] ↔ w = 0 := by
  constructor
  · intro hl
    refine BitVec.eq_of_getLsbD_eq_iff.mpr (fun i hi => ?_)
    rw [show ((0 : W).getLsbD i) = false from BitVec.getLsbD_zero]
    rw [getLsbD_eq_mem_lanes]
    simp [hl]
  · intro hw
    subst hw
    refine List.filter_eq_nil_iff.mpr (fun b _ => ?_)
    simp [BitVec.getLsbD_zero]

private theorem lanes_pairwise (w : W) : List.Pairwise (fun a b => a < b) (lanes w) := by
  have h := List.pairwise_lt_range (n := 64)
  simpa [lanes] using List.Pairwise.filter w.getLsbD h

/-- Clearing the lowest set bit: `(m &&& (m-1))` has all of `m`'s bits except
the lowest one, given `k` witnesses that lowest bit. -/
private theorem getLsbD_sub_and (m : W) (k : Nat)
    (hlow : ∀ j, j < k → m.getLsbD j = false) (hbit : m.getLsbD k = true) :
    ∀ j, (m &&& (m - 1)).getLsbD j = (m.getLsbD j && decide (j ≠ k)) := by
  have hmt : ∀ x : Nat, m.getLsbD x = m.toNat.testBit x := fun _ => rfl
  have hlow' : ∀ j, j < k → m.toNat.testBit j = false := by
    intro j hj
    rw [← hmt]; exact hlow j hj
  have hbit' : m.toNat.testBit k = true := by rw [← hmt]; exact hbit
  have hmod : m.toNat % 2 ^ (k + 1) = 2 ^ k := by
    refine Nat.eq_of_testBit_eq (fun i => ?_)
    rw [Nat.testBit_mod_two_pow, Nat.testBit_two_pow]
    by_cases h : i < k + 1
    · rw [decide_eq_true h, Bool.true_and]
      rcases Nat.lt_or_eq_of_le (Nat.le_of_lt_succ h) with hlt | rfl
      · rw [hlow' i hlt]
        exact (decide_eq_false (by omega : ¬ k = i)).symm
      · rw [decide_eq_true (rfl : i = i)]
        exact hbit'
    · rw [decide_eq_false h, Bool.false_and]
      exact (decide_eq_false (by omega : ¬ k = i)).symm
  have hpos : 1 ≤ m.toNat := by
    have h1 : (2 : Nat) ^ k ≤ m.toNat := Nat.ge_two_pow_of_testBit hbit'
    have h2 : 1 ≤ (2 : Nat) ^ k := Nat.one_le_two_pow
    omega
  have hdiv := Nat.div_add_mod m.toNat (2 ^ (k + 1))
  have hmt2 : m.toNat = 2 ^ (k + 1) * (m.toNat / 2 ^ (k + 1)) + 2 ^ k := by omega
  have hsub : m.toNat - 1 = 2 ^ (k + 1) * (m.toNat / 2 ^ (k + 1)) + (2 ^ k - 1) := by omega
  have hto : (m - 1).toNat = m.toNat - 1 := by
    have h1 := BitVec.toNat_sub (x := m) (y := 1)
    rw [show (BitVec.toNat (1 : W)) = 1 from BitVec.toNat_one (w := 64) (by decide)] at h1
    have h2 : (2 : Nat) ^ 64 - 1 + m.toNat = m.toNat - 1 + 2 ^ 64 := by omega
    have hlt : m.toNat - 1 < 2 ^ 64 := by
      have := BitVec.isLt m
      omega
    rw [h1, h2, Nat.add_mod, Nat.mod_self, Nat.add_zero]
    omega
  intro j
  have hb_lt : j < k → (m - 1).getLsbD j = true := by
    intro hj
    show (m - 1).toNat.testBit j = true
    rw [hto, hsub]
    rw [Nat.testBit_two_pow_mul_add (m.toNat / 2 ^ (k + 1))
      (by have := Nat.one_le_two_pow (n := k); omega : 2 ^ k - 1 < 2 ^ (k + 1)) j]
    rw [if_pos (by omega : j < k + 1)]
    rw [Nat.testBit_two_pow_sub_one]
    exact decide_eq_true hj
  have hb_eq : (m - 1).getLsbD k = false := by
    show (m - 1).toNat.testBit k = false
    rw [hto, hsub]
    rw [Nat.testBit_two_pow_mul_add (m.toNat / 2 ^ (k + 1))
      (by have := Nat.one_le_two_pow (n := k); omega : 2 ^ k - 1 < 2 ^ (k + 1)) k]
    rw [if_pos (Nat.lt_succ_self k)]
    rw [Nat.testBit_two_pow_sub_one]
    exact decide_eq_false (Nat.lt_irrefl k)
  have hmbits : m.toNat.testBit j = (2 ^ (k + 1) * (m.toNat / 2 ^ (k + 1)) + 2 ^ k).testBit j :=
    congrArg (fun x => x.testBit j) hmt2
  have hb_gt : k < j → (m - 1).getLsbD j = m.getLsbD j := by
    intro hj
    show (m - 1).toNat.testBit j = m.toNat.testBit j
    rw [hto, hsub, hmbits]
    rw [Nat.testBit_two_pow_mul_add (m.toNat / 2 ^ (k + 1))
      (by have := Nat.one_le_two_pow (n := k); omega : 2 ^ k - 1 < 2 ^ (k + 1)) j,
      Nat.testBit_two_pow_mul_add (m.toNat / 2 ^ (k + 1))
      (Nat.pow_lt_pow_right (by decide : (1 : Nat) < 2) (Nat.lt_succ_self k)) j]
    rw [if_neg (by omega : ¬ j < k + 1), if_neg (by omega : ¬ j < k + 1)]
  rcases Nat.lt_trichotomy j k with hlt | heq | hgt
  · rw [BitVec.getLsbD_and, hb_lt hlt, hlow j hlt, Bool.false_and, Bool.false_and]
  · subst heq
    rw [BitVec.getLsbD_and, hb_eq, hbit, Bool.true_and, Bool.true_and,
      decide_eq_false (by omega : ¬ (j ≠ j))]
  · rw [BitVec.getLsbD_and, hb_gt hgt]
    rw [decide_eq_true (Nat.ne_of_gt hgt), Bool.and_true, Bool.and_self]

private theorem getLsbD_one_and (src : W) (j : Nat) :
    (src &&& 1).getLsbD j = (decide (j = 0) && src.getLsbD 0) := by
  by_cases hj : j = 0
  · subst j
    simp
  · simp [hj]

/-- The `chosen` mask keeps the low bit only when source bit 0 is set. -/
private theorem chosen_if (src low : W) :
    low &&& (-(src &&& 1)) = if src.getLsbD 0 = true then low else 0 := by
  refine BitVec.eq_of_getLsbD_eq_iff.mpr (fun i hi => ?_)
  rw [BitVec.getLsbD_and, BitVec.getLsbD_neg, getLsbD_one_and, decide_eq_true hi]
  by_cases hb : src.getLsbD 0 = true
  · rw [if_pos hb]
    by_cases hi0 : i = 0
    · subst i
      rw [decide_eq_false (by simp : ¬ ∃ j, j < 0 ∧ (src &&& 1).getLsbD j = true)]
      simp [hb]
    · have hex : ∃ j, j < i ∧ (src &&& 1).getLsbD j = true := by
        refine ⟨0, by omega, ?_⟩
        rw [getLsbD_one_and, hb]
        simp
      rw [decide_eq_true hex]
      simp [hi0]
  · rw [if_neg hb]
    have hno : ¬ ∃ j, j < i ∧ (src &&& 1).getLsbD j = true := by
      rintro ⟨j, hj, hjt⟩
      rw [getLsbD_one_and] at hjt
      have hj0 : j = 0 := by
        cases h : decide (j = 0) <;> simp_all
      subst j
      simp_all
    rw [decide_eq_false hno]
    have hsrc0 : src[0] = false := by
      rw [← BitVec.getLsbD_eq_getElem (show 0 < 64 by decide)]
      exact bv_false hb
    simp [hsrc0]

/-- A nonzero vector's lanes split into the lowest set bit and strictly larger rest. -/
private theorem lanes_decomp (m : W) (hm : ¬ m = 0) :
    ∃ (k : Nat) (rest : List Nat),
      lanes m = k :: rest ∧
      (∀ x ∈ rest, k < x) ∧
      (∀ j, j < k → m.getLsbD j = false) ∧
      m.getLsbD k = true := by
  have hnnil : lanes m ≠ [] := fun h => hm ((lanes_eq_nil_iff m).mp h)
  obtain ⟨k, rest, hcons⟩ : ∃ (k : Nat) (rest : List Nat), lanes m = k :: rest := by
    cases h : lanes m with
    | nil => exact absurd h hnnil
    | cons k rest => exact ⟨k, rest, rfl⟩
  have hpair := lanes_pairwise m
  rw [hcons] at hpair
  have hrest : ∀ b ∈ rest, k < b := by
    cases hpair with
    | cons h1 _ => exact h1
  have hkb : m.getLsbD k = true := by
    have hmem : k ∈ lanes m := by rw [hcons]; exact List.mem_cons_self
    exact (List.mem_filter.mp hmem).2
  have hlow : ∀ j, j < k → m.getLsbD j = false := by
    intro j hj
    cases v : m.getLsbD j with
    | false => rfl
    | true =>
      have hmem : j ∈ lanes m := lanes_mem (getLsbD_lt v) v
      rw [hcons, List.mem_cons] at hmem
      cases hmem with
      | inl heq => subst heq; exact absurd hj (Nat.lt_irrefl _)
      | inr hx => exact absurd (hrest j hx) (Nat.not_lt_of_ge (Nat.le_of_lt hj))
  exact ⟨k, rest, hcons, hrest, hlow, hkb⟩

private theorem lowbit_xor (m : W) : m &&& (-m) = m ^^^ (m &&& (m - 1)) := by
  by_cases hm : m = 0
  · subst m
    rfl
  · obtain ⟨k, rest, hcons, hrest, hlow, hkb⟩ := lanes_decomp m hm
    have hsub := getLsbD_sub_and m k hlow hkb
    refine BitVec.eq_of_getLsbD_eq_iff.mpr (fun i hi => ?_)
    rw [show (m &&& -m).getLsbD i = (m.getLsbD i && (-m).getLsbD i) from BitVec.getLsbD_and]
    rw [show (m ^^^ (m &&& (m - 1))).getLsbD i =
        (m.getLsbD i ^^ (m &&& (m - 1)).getLsbD i) from BitVec.getLsbD_xor]
    rw [hsub]
    rw [BitVec.getLsbD_neg, decide_eq_true hi]
    by_cases hik : i = k
    · subst i
      have hno : ¬ ∃ j, j < k ∧ m.getLsbD j = true := by
        rintro ⟨j, hj, hjt⟩
        rw [hlow j hj] at hjt
        contradiction
      rw [decide_eq_false hno, hkb]
      simp
    · by_cases hiklt : i < k
      · have hmi : m.getLsbD i = false := hlow i hiklt
        have hno : ¬ ∃ j, j < i ∧ m.getLsbD j = true := by
          rintro ⟨j, hj, hjt⟩
          rw [hlow j (by omega)] at hjt
          contradiction
        rw [decide_eq_false hno, hmi]
        simp
      · have hikgt : k < i := by omega
        have hex : ∃ j, j < i ∧ m.getLsbD j = true := ⟨k, hikgt, hkb⟩
        rw [decide_eq_true hex, decide_eq_true hik]
        cases m.getLsbD i <;> simp

/-- The low bit `m &&& -m` is the singleton `{k}`. -/
private theorem getLsbD_lowbit (m : W) (k : Nat)
    (hlow : ∀ j, j < k → m.getLsbD j = false) (hbit : m.getLsbD k = true) :
    ∀ j, (m &&& -m).getLsbD j = decide (j = k) := by
  have hsub := getLsbD_sub_and m k hlow hbit
  intro j
  rw [lowbit_xor, BitVec.getLsbD_xor, hsub]
  cases h : m.getLsbD j with
  | true =>
    by_cases hj : j = k
    · subst hj
      rw [decide_eq_false (by omega : ¬ (j ≠ j)), decide_eq_true (rfl : j = j),
        Bool.and_false, Bool.xor_false]
    · rw [decide_eq_true hj, Bool.true_and, decide_eq_false hj, Bool.xor_self]
  | false =>
    by_cases hj : j = k
    · subst hj
      rw [h] at hbit
      exact absurd hbit Bool.false_ne_true
    · rw [decide_eq_true hj, Bool.false_and, decide_eq_false hj, Bool.false_xor]

/-- The lowest passing index splits a filtered `List.range`. -/
private theorem filter_lowbit_range : ∀ (p q : Nat → Bool) (k n : Nat),
    k < n → p k = true → q k = false →
    (∀ j, j < n → j ≠ k → p j = q j) → (∀ j, q j = true → k < j) →
    List.filter p (List.range n) = k :: List.filter q (List.range n) := by
  intro p q k n
  induction n with
  | zero =>
    intro hk
    exact absurd hk (Nat.not_lt_zero _)
  | succ n ih =>
    intro hk hp hq hpq hlow
    rw [List.range_succ]
    by_cases hkn : k < n
    · have hrec := ih (by omega) hp hq (fun j hj hjk => hpq j (by omega) hjk) hlow
      rw [List.filter_append, List.filter_append, hrec,
        show List.filter p [n] = List.filter q [n] from by
          rw [List.filter_cons, List.filter_cons, List.filter_nil, List.filter_nil,
            hpq n (by omega) (by omega)]]
      simp
    · have hkn2 : n = k := by omega
      subst hkn2
      have hfp : List.filter p (List.range n) = [] := by
        refine List.eq_nil_iff_forall_not_mem.mpr (fun x hx => ?_)
        have h1 := List.mem_filter.mp hx
        have hx' : x < n := List.mem_range.mp h1.1
        have hqx : q x = true := by
          have hpx : p x = q x := hpq x (by omega) (by omega)
          rw [← hpx]
          exact h1.2
        exact absurd (hlow x hqx) (by omega : ¬ (n < x))
      have hfq : List.filter q (List.range n) = [] := by
        refine List.eq_nil_iff_forall_not_mem.mpr (fun x hx => ?_)
        have h1 := List.mem_filter.mp hx
        have hx' : x < n := List.mem_range.mp h1.1
        exact absurd (hlow x h1.2) (by omega : ¬ (n < x))
      rw [List.filter_append, List.filter_append, hfp, hfq,
        List.filter_cons, List.filter_nil, List.filter_cons, List.filter_nil, hp, hq]
      simp

/-- A strictly increasing list of indices is recovered by filtering its range. -/
private theorem filter_mem_range : ∀ (l : List Nat),
    (∀ x ∈ l, x < 64) → List.Pairwise (fun a b => a < b) l →
    List.filter (fun b => decide (b ∈ l)) (List.range 64) = l := by
  intro l
  induction l with
  | nil => intro _ _; rfl
  | cons a as ih =>
    intro hlt hord
    have ha : a < 64 := hlt a List.mem_cons_self
    have hgt : ∀ x ∈ as, a < x := by
      cases hord with
      | cons h1 _ => exact h1
    have hnas : ¬ a ∈ as := fun hx => Nat.lt_irrefl a (hgt a hx)
    have hfc : List.filter (fun b => decide (b ∈ a :: as)) (List.range 64)
        = List.filter (fun b => decide (b = a) || decide (b ∈ as)) (List.range 64) := by
      refine List.filter_congr (fun b _ => ?_)
      simp [List.mem_cons]
    rw [hfc]
    have hbnd : ∀ x ∈ as, x < 64 := fun x hx => hlt x (List.mem_cons_of_mem a hx)
    have hpair' := hord.tail
    have hmain :=
      filter_lowbit_range (fun b => decide (b = a) || decide (b ∈ as))
        (fun b => decide (b ∈ as)) a 64 ha (by simp)
        (decide_eq_false hnas)
        (fun j hj hjk => by rw [decide_eq_false hjk, Bool.false_or])
        (fun j hj => hgt j (of_decide_eq_true hj))
    rw [hmain, ih hbnd hpair']

/-- Shifting both the rank bound and the source right by one preserves the map. -/
private theorem bridge : ∀ (rest : List Nat) (k m : Nat) (src : W),
    List.map Prod.fst
        (List.filter (fun p => decide (p.2 < m) && src.getLsbD p.2) (List.zipIdx rest (k + 1))) =
    List.map Prod.fst
        (List.filter (fun p => decide (p.2 < m - 1) && (src >>> 1).getLsbD p.2) (List.zipIdx rest k)) := by
  intro rest
  induction rest with
  | nil => intro k m src; rfl
  | cons a rest ih =>
    intro k m src
    have hshr : (src >>> 1).getLsbD k = src.getLsbD (k + 1) :=
      (BitVec.getLsbD_ushiftRight (x := src) (i := 1) (j := k)).trans
        (congrArg src.getLsbD (by omega : 1 + k = k + 1))
    have hd2 : decide (k + 1 < m) = decide (k < m - 1) := by
      have hd : (k + 1 < m) = (k < m - 1) :=
        propext ⟨fun hh => by omega, fun hh => by omega⟩
      simp [hd]
    have he : (decide ((a, k + 1).2 < m) && src.getLsbD (a, k + 1).2)
        = (decide ((a, k).2 < m - 1) && (src >>> 1).getLsbD (a, k).2) := by
      show (decide (k + 1 < m) && src.getLsbD (k + 1))
          = (decide (k < m - 1) && (src >>> 1).getLsbD k)
      rw [hd2, hshr]
    rw [List.zipIdx_cons, List.zipIdx_cons, List.filter_cons, List.filter_cons, he]
    cases hdec : (decide ((a, k).2 < m - 1) && (src >>> 1).getLsbD (a, k).2) with
    | true =>
      rw [if_pos (rfl : (true : Bool) = true), if_pos (rfl : (true : Bool) = true),
        List.map_cons, List.map_cons]
      exact congrArg (List.cons (a, k + 1).fst) (ih (k + 1) m src)
    | false =>
      rw [if_neg Bool.false_ne_true, if_neg Bool.false_ne_true]
      exact ih (k + 1) m src

/-- Main invariant: the lanes of `deposit n src mask` are the lanes of `mask`
whose rank is below `n` and whose rank bit is set in `src`. -/
private theorem lanes_deposit : ∀ (n : Nat) (src mask : W),
    lanes (deposit n src mask) =
      List.map Prod.fst
        (List.filter (fun p => decide (p.2 < n) && src.getLsbD p.2) (List.zipIdx (lanes mask) 0)) := by
  intro n
  induction n with
  | zero =>
    intro src mask
    have hz : deposit 0 src mask = 0 := rfl
    rw [hz]
    have hn : lanes (0 : W) = [] := (lanes_eq_nil_iff (0 : W)).mpr rfl
    rw [hn]
    simp
  | succ n ih =>
    intro src mask
    have hnil : lanes (0 : W) = [] := (lanes_eq_nil_iff (0 : W)).mpr rfl
    by_cases hmk : mask = 0
    · subst hmk
      have hstep : deposit (n + 1) src 0 = 0 := by
        rw [deposit]
        rw [if_pos (show (0 : W) = 0 from rfl)]
      rw [hstep, hnil]
      simp
    · obtain ⟨k, rest, hcons, hrest, hlow, hkb⟩ := lanes_decomp mask hmk
      have hk64 : k < 64 := getLsbD_lt hkb
      have hrest64 : ∀ x ∈ rest, x < 64 := by
        intro x hx
        have : x ∈ lanes mask := by rw [hcons]; exact List.mem_cons_of_mem k hx
        exact List.mem_range.mp (List.mem_filter.mp this).1
      have hpairr : List.Pairwise (fun a b => a < b) rest := by
        have hp2 := lanes_pairwise mask
        rw [hcons] at hp2
        cases hp2 with
        | cons _ h2 => exact h2
      have hmask1 : lanes (mask &&& (mask - 1)) = rest := by
        have hfc : List.filter (fun b => (mask &&& (mask - 1)).getLsbD b) (List.range 64)
            = List.filter (fun b => decide (b ∈ rest)) (List.range 64) := by
          refine List.filter_congr (fun b hb => ?_)
          have hb64 : b < 64 := List.mem_range.mp hb
          rw [getLsbD_sub_and mask k hlow hkb b]
          cases hbv : mask.getLsbD b with
          | true =>
            rw [Bool.true_and]
            have hbmem : b = k ∨ b ∈ rest := by
              have hmem : b ∈ lanes mask := lanes_mem hb64 hbv
              rw [hcons, List.mem_cons] at hmem
              exact hmem
            cases hbmem with
            | inl heq =>
              subst heq
              rw [decide_eq_false (by omega : ¬ (b ≠ b)),
                decide_eq_false (fun h => Nat.lt_irrefl b (hrest b h))]
            | inr hx =>
              rw [decide_eq_true (Nat.ne_of_gt (hrest b hx)), decide_eq_true hx]
          | false =>
            rw [Bool.false_and, decide_eq_false]
            intro hbr
            have h2 : mask.getLsbD b = true := by
              have hmem : b ∈ lanes mask := by rw [hcons]; exact List.mem_cons_of_mem k hbr
              exact (List.mem_filter.mp hmem).2
            rw [hbv] at h2
            cases h2
        show List.filter (fun b => (mask &&& (mask - 1)).getLsbD b) (List.range 64) = rest
        rw [hfc]
        exact filter_mem_range rest hrest64 hpairr
      have hrec :
          lanes (deposit n (src >>> 1) (mask &&& (mask - 1))) =
            List.map Prod.fst
              (List.filter (fun p => decide (p.2 < n + 1) && src.getLsbD p.2)
                (List.zipIdx rest 1)) := by
        rw [ih, hmask1]
        rw [show (fun (p : Nat × Nat) => decide (p.2 < n) && (src >>> 1).getLsbD p.2)
            = (fun (p : Nat × Nat) => decide (p.2 < n + 1 - 1) && (src >>> 1).getLsbD p.2) from by
              funext p
              rw [Nat.add_sub_cancel]]
        rw [← bridge rest 0 (n + 1) src]
      have hgt_rec : ∀ x ∈ lanes (deposit n (src >>> 1) (mask &&& (mask - 1))), k < x := by
        intro x hx
        rw [hrec] at hx
        obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hx
        have hpf := List.mem_filter.mp hp
        have hzip : (p.1, p.2) ∈ List.zipIdx rest 1 := by simpa using hpf.1
        obtain ⟨h1, h2, h3⟩ := List.mem_zipIdx hzip
        rw [h3]
        exact hrest _ (List.get_mem rest ⟨p.2 - 1, by omega⟩)
      have hex :
          List.filter (fun b => decide (b = k) || (deposit n (src >>> 1) (mask &&& (mask - 1))).getLsbD b)
              (List.range 64)
            = k :: lanes (deposit n (src >>> 1) (mask &&& (mask - 1))) := by
        refine filter_lowbit_range _ _ k 64 hk64 (by simp) ?_
          (fun j hj hjk => by rw [decide_eq_false hjk, Bool.false_or]) ?_
        · cases v : (deposit n (src >>> 1) (mask &&& (mask - 1))).getLsbD k with
          | false => rfl
          | true =>
            have hmem : k ∈ lanes (deposit n (src >>> 1) (mask &&& (mask - 1))) :=
              lanes_mem hk64 v
            exact absurd hmem (fun h => Nat.lt_irrefl k (hgt_rec k h))
        · intro j hj
          exact hgt_rec j (lanes_mem (getLsbD_lt hj) hj)
      cases hbs : src.getLsbD 0 with
      | true =>
        have hstep : deposit (n + 1) src mask =
            ((mask &&& -mask) ||| deposit n (src >>> 1) (mask &&& (mask - 1))) := by
          rw [deposit, if_neg hmk]
          simp only [chosen_if, if_pos hbs]
        rw [hstep]
        show List.filter (fun b => ((mask &&& -mask) ||| deposit n (src >>> 1) (mask &&& (mask - 1))).getLsbD b)
            (List.range 64) = _
        rw [show (fun b => ((mask &&& -mask) ||| deposit n (src >>> 1) (mask &&& (mask - 1))).getLsbD b)
              = (fun b => decide (b = k) || (deposit n (src >>> 1) (mask &&& (mask - 1))).getLsbD b) from by
            funext b
            rw [BitVec.getLsbD_or, getLsbD_lowbit mask k hlow hkb b]]
        rw [hex, hrec]
        rw [hcons, List.zipIdx_cons, List.filter_cons]
        have hguard : (decide ((k, 0).2 < n + 1) && src.getLsbD (k, 0).2) = true := by
          show decide (0 < Nat.succ n) && src.getLsbD 0 = true
          rw [decide_eq_true (Nat.succ_pos n), hbs, Bool.true_and]
          rfl
        rw [hguard, if_pos rfl, List.map_cons]
      | false =>
        have hstep : deposit (n + 1) src mask =
            deposit n (src >>> 1) (mask &&& (mask - 1)) := by
          rw [deposit, if_neg hmk]
          simp only [chosen_if, if_neg (show ¬(src.getLsbD 0 = true) from
            by rw [hbs]; exact Bool.false_ne_true)]
          exact BitVec.zero_or (x := deposit n (src >>> 1) (mask &&& (mask - 1)))
        rw [hstep, ih, hmask1]
        rw [show (fun (p : Nat × Nat) => decide (p.2 < n) && (src >>> 1).getLsbD p.2)
            = (fun (p : Nat × Nat) => decide (p.2 < n + 1 - 1) && (src >>> 1).getLsbD p.2) from by
              funext p
              rw [Nat.add_sub_cancel]]
        rw [← bridge rest 0 (n + 1) src]
        rw [hcons, List.zipIdx_cons, List.filter_cons]
        have hguard : (decide ((k, 0).2 < n + 1) && src.getLsbD (k, 0).2) = false := by
          show (decide (0 < Nat.succ n) && src.getLsbD 0) = false
          rw [decide_eq_true (Nat.succ_pos n), hbs, Bool.true_and]
        rw [hguard]
        simp

theorem sound : TinylambOpt.ResearchGoals.SelectionMaskDeposit.Goal := by
  intro parent child
  rw [lanes_deposit 64 child parent]
  refine congrArg (List.map Prod.fst) (List.filter_congr ?_)
  intro p hp
  have hlen : (lanes parent).length ≤ 64 := by
    show (List.filter parent.getLsbD (List.range 64)).length ≤ 64
    calc (List.filter parent.getLsbD (List.range 64)).length
        ≤ (List.range 64).length := List.length_filter_le parent.getLsbD (List.range 64)
      _ = 64 := List.length_range
  have hlt : p.2 < (lanes parent).length := by
    simpa using List.snd_lt_of_mem_zipIdx hp
  have : p.2 < 64 := Nat.lt_of_lt_of_le hlt hlen
  simp [this]

end TinylambOpt.ResearchProofs.SelectionMaskDeposit
