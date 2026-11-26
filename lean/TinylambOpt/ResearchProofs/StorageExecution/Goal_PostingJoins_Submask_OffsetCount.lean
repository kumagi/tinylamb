import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Submask.OffsetCount` — Two lower-bound offsets per interval compute the exact join
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_OffsetCount

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask

/-! Self-contained certificate for `Submask.OffsetCount`.

The binary lower bound on a sorted right-key column cuts exactly at a key
boundary, so each singleton posting `[y,y+1)` contributes the multiplicity of
the right key `y`.  The fold that builds `submasks x` enumerates exactly the
submasks of `x` (each once), hence the sum of those multiplicities is precisely
the number of right keys accepted by `s.test`. -/

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) : ys.Pairwise (· ≤ ·) := hv.1
private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat} (hy : y ∈ ys) : y < u := hv.2 y hy

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, List.getElem?_eq_getElem hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, List.getElem?_eq_none (by omega : xs.length ≤ i)]

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by omega
      subst hi
      exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs.toList.getD mid 0 = xs[mid] := by
          rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
          simp [Nat.pow_succ, Nat.mul_comm]
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        change Cut xs.toList v
            (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
              else searchBound xs v fuel lo mid) ∧
          lo ≤ (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
              else searchBound xs v fuel lo mid) ∧
          (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
              else searchBound xs v fuel lo mid) ≤ hi
        rw [hval]
        by_cases hv : xs[mid] < v
        · rw [if_pos hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  have hpair := hs.rel_getElem_of_lt hjlen hmidlen hjm
                  rw [← getD_eq_getElem hjlen, ← getD_eq_getElem hmidlen] at hpair
                  exact hpair
                rw [hlistval] at hrel
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, by omega, hub⟩
        · rw [if_neg hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              omega
            · have hmj : mid < j := by omega
              by_cases hjh : hi ≤ j
              · exact habove j hjh hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  have hpair := hs.rel_getElem_of_lt hmidlen hjlen hmj
                  rw [← getD_eq_getElem hmidlen, ← getD_eq_getElem hjlen] at hpair
                  exact hpair
                rw [hlistval] at hrel
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          have hres := ih lo mid hml (by omega) hf' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) :=
      (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]; omega
    rw [hpow]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen
        have hjl : j < ys.length := by simpa using hjlen
        omega)
    (by simpa using hs)
  simpa using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  let k := lower ys v
  have hle : k ≤ ys.length := by simpa [k] using lower_le_length hs v
  have htake : (ys.take k).filter (fun x => decide (x < v)) = ys.take k := by
    apply List.filter_eq_self.mpr
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq]
    have hiys : i < ys.length := by omega
    have hik : i < lower ys v := by
      dsimp [k] at hi ⊢
      omega
    have hlt := lower_lt_of_lt hs v hik
    rw [getD_eq_getElem hiys] at hlt
    simpa [k] using hlt
  have hdrop : (ys.drop k).filter (fun x => decide (x < v)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq]
    intro hlt
    have hiys : k + i < ys.length := by
      have hlen : i < (ys.drop k).length := hi
      rw [List.length_drop] at hlen
      omega
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) (by simpa [k] using hiys)
    rw [getD_eq_getElem hiys] at hge
    rw [List.getElem_drop] at hlt
    omega
  have hfull_list : ys.filter (fun x => decide (x < v)) =
      (ys.take k).filter (fun x => decide (x < v)) := by
    calc
      ys.filter (fun x => decide (x < v)) =
          (ys.take k ++ ys.drop k).filter (fun x => decide (x < v)) := by rw [List.take_append_drop]
      _ = (ys.take k).filter (fun x => decide (x < v)) ++
          (ys.drop k).filter (fun x => decide (x < v)) := by rw [List.filter_append]
      _ = (ys.take k).filter (fun x => decide (x < v)) := by rw [hdrop, List.append_nil]
  calc
    lower ys v = (ys.take k).length := by simp [k, List.length_take, Nat.min_eq_left hle]
    _ = ((ys.take k).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [hfull_list]

private theorem countP_lt_split {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.countP (fun y => decide (y < b)) =
      ys.countP (fun y => decide (y < a)) + ys.countP (fun y => decide (a ≤ y && y < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs'] <;> omega
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil_a : xs.countP (fun y => decide (y < a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy
            have hxy := List.rel_of_pairwise_cons hs hy
            simp [show ¬ y < a by omega]
          simp [hxa, hxb, hax, hnil_a, ih hs'] <;> omega
        · have hnil_a : xs.countP (fun y => decide (y < a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy
            have hxy := List.rel_of_pairwise_cons hs hy
            simp [show ¬ y < a by omega]
          have hnil_ab : xs.countP (fun y => decide (a ≤ y && y < b)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy
            have hxy := List.rel_of_pairwise_cons hs hy
            simpa using (show ¬ (a ≤ y ∧ y < b) by omega)
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs'] <;> omega

private theorem lower_interval_count {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = ys.countP (fun y => decide (a ≤ y && y < b)) := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a,
    ← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
  rw [countP_lt_split hs hab]
  omega

private theorem countP_or_of_disjoint {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    xs.countP p + xs.countP q = xs.countP (fun x => p x || q x) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have htail : ∀ y ∈ xs, p y = true → q y = false := fun y hy => hdisj y (by simp [hy])
      have hih := ih htail
      by_cases hp : p x = true
      · have hq : q x = false := hdisj x (by simp) hp
        simp [hp, hq] at hih ⊢
        omega
      · by_cases hq : q x = true
        · simp [hp, hq] at hih ⊢
          omega
        · simp [hp, hq] at hih ⊢
          omega

private theorem lower_singleton_count {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a : Nat) :
    lower ys (a + 1) - lower ys a = ys.count a := by
  rw [lower_interval_count hs (by omega : a ≤ a + 1)]
  unfold List.count
  apply List.countP_congr
  intro y hy
  cases h1 : decide (a ≤ y && y < a + 1) <;> cases h2 : y == a <;> simp_all <;> omega

private theorem bit_lt_pow {z b i : Nat} (hz : z < 2^b) (hib : b ≤ i) : z.testBit i = false := by
  apply Nat.testBit_lt_two_pow
  exact Nat.lt_of_lt_of_le hz (Nat.pow_le_pow_right Nat.zero_lt_two hib)

private theorem add_pow_bit_true {z b : Nat} (hz : z < 2^b) : (z + 2^b).testBit b = true := by
  rw [Nat.add_comm, Nat.testBit_two_pow_add_eq]
  have hfalse := Nat.testBit_lt_two_pow hz (i := b)
  simp [hfalse]

private theorem add_pow_bit_low {z b i : Nat} (hi : i < b) : (z + 2^b).testBit i = z.testBit i := by
  rw [Nat.add_comm, Nat.testBit_two_pow_add_gt hi]

private abbrev lo (bl : List Nat) (B : Nat) := match bl with | [] => B | b::_ => b

private abbrev step (acc : List Nat) (b : Nat) : List Nat := acc ++ acc.map (fun z => z + 2^b)

/-! The fold enumerating submasks: maintain that the accumulator contains
exactly the numbers whose set bits come from the already-processed bits. -/
private theorem fold_spec (bl pre : List Nat) (acc : List Nat) (B : Nat)
    (hinc : (pre ++ bl).Pairwise (· < ·))
    (hblB : ∀ b ∈ bl, b < B)
    (hacc : ∀ z ∈ acc, z < 2 ^ lo bl B)
    (hbits : ∀ z ∈ acc, ∀ i, z.testBit i = true → i ∈ pre)
    (hcomp : ∀ z, z < 2 ^ lo bl B → (∀ i, z.testBit i = true → i ∈ pre) → z ∈ acc)
    (hnodup : acc.Nodup) :
    let acc' := bl.foldl step acc
    (∀ z ∈ acc', z < 2 ^ B) ∧
    (∀ z ∈ acc', ∀ i, z.testBit i = true → i ∈ pre ++ bl) ∧
    (∀ z, z < 2 ^ B → (∀ i, z.testBit i = true → i ∈ pre ++ bl) → z ∈ acc') ∧
    acc'.Nodup := by
  induction bl generalizing acc pre with
  | nil =>
      simp only [List.foldl_nil, List.append_nil]
      refine ⟨?_, hbits, hcomp, hnodup⟩
      intro z hz
      exact Nat.lt_of_lt_of_le (hacc z hz) (Nat.pow_le_pow_right Nat.zero_lt_two (Nat.le_refl B))
  | cons b bs ih =>
      simp only [List.foldl_cons]
      have hpair := hinc
      simp only [List.pairwise_append] at hpair
      have hpre_lt_b : ∀ i ∈ pre, i < b := by
        intro i hi
        exact hpair.2.2 i hi b (by simp)
      have ht : (b :: bs).Pairwise (· < ·) := hpair.2.1
      have hbB : b < B := hblB b (by simp)
      have hbsB : ∀ c ∈ bs, c < B := by intro c hc; exact hblB c (by simp [hc])
      have hb_lo : b + 1 ≤ lo bs B := by
        cases bs with
        | nil => simp [lo]; omega
        | cons c cs =>
            have hbc : b < c := List.rel_of_pairwise_cons (a' := c) ht (by simp)
            simp [lo]; omega
      let acc1 := step acc b
      have hb_le_lo : b ≤ lo bs B := by omega
      have hacc1 : ∀ z ∈ acc1, z < 2 ^ lo bs B := by
        intro z hz
        dsimp [acc1, step] at hz
        rw [List.mem_append] at hz
        rcases hz with hz | hz
        · have hzb := hacc z hz
          simp [lo] at hzb
          exact Nat.lt_of_lt_of_le hzb (Nat.pow_le_pow_right Nat.zero_lt_two hb_le_lo)
        · rw [List.mem_map] at hz
          rcases hz with ⟨w, hw, rfl⟩
          have hwb := hacc w hw
          simp [lo] at hwb
          have hpow : 2^b + 2^b = 2^(b+1) := by rw [Nat.pow_succ]; omega
          have hsum : w + 2^b < 2^(b+1) := by rw [← hpow]; omega
          exact Nat.lt_of_lt_of_le hsum (Nat.pow_le_pow_right Nat.zero_lt_two hb_lo)
      have hbits1 : ∀ z ∈ acc1, ∀ i, z.testBit i = true → i ∈ pre ++ [b] := by
        intro z hz i hi
        dsimp [acc1, step] at hz
        rw [List.mem_append] at hz
        rcases hz with hz | hz
        · simp only [List.mem_append, List.mem_singleton]
          exact Or.inl (hbits z hz i hi)
        · rw [List.mem_map] at hz
          rcases hz with ⟨w, hw, rfl⟩
          have hwb := hacc w hw
          simp [lo] at hwb
          by_cases hib : i = b
          · subst i; simp
          · rcases Nat.lt_or_gt_of_ne hib with hil | hig
            · have hbit : (w + 2^b).testBit i = w.testBit i := add_pow_bit_low hil
              rw [hbit] at hi
              simp only [List.mem_append, List.mem_singleton]
              exact Or.inl (hbits w hw i hi)
            · have hwltpow : w + 2^b < 2^i := by
                have hpowle : 2^(b+1) ≤ 2^i := Nat.pow_le_pow_right Nat.zero_lt_two (by omega)
                have hpow : 2^b + 2^b = 2^(b+1) := by rw [Nat.pow_succ]; omega
                have hsum : w + 2^b < 2^(b+1) := by rw [← hpow]; omega
                exact Nat.lt_of_lt_of_le hsum hpowle
              have hf := Nat.testBit_lt_two_pow hwltpow
              rw [hf] at hi
              contradiction
      have hcomp1 : ∀ z, z < 2 ^ lo bs B → (∀ i, z.testBit i = true → i ∈ pre ++ [b]) → z ∈ acc1 := by
        intro z hz hzb
        by_cases hzb_bit : z.testBit b = true
        · have hzge : 2^b ≤ z := Nat.ge_two_pow_of_testBit hzb_bit
          have hzlt_b1 : z < 2^(b+1) := by
            apply Nat.lt_pow_two_of_testBit
            intro i hi
            by_cases hmem : i ∈ pre ++ [b]
            · rcases List.mem_append.mp hmem with hip | hib'
              · have hilt := hpre_lt_b i hip
                exact Bool.eq_false_iff.mpr (by omega)
              · have hieq : i = b := by simpa using hib'
                exfalso
                omega
            · exact Bool.eq_false_iff.mpr (by intro ht; exact hmem (hzb i ht))
          let w := z - 2^b
          have hwlt : w < 2^b := by
            dsimp [w]
            have hpow : 2^(b+1) = 2^b + 2^b := by rw [Nat.pow_succ]; omega
            rw [hpow] at hzlt_b1
            omega
          have hw_eq : w + 2^b = z := by dsimp [w]; omega
          have hwbits : ∀ i, w.testBit i = true → i ∈ pre := by
            intro i hi
            by_cases hib : i = b
            · subst i
              have hwb_false := Nat.testBit_lt_two_pow hwlt (i := b)
              rw [hwb_false] at hi
              contradiction
            · rcases Nat.lt_or_gt_of_ne hib with hil | hig
              · have hbit : (w + 2^b).testBit i = w.testBit i := add_pow_bit_low hil
                have hi' : (w + 2^b).testBit i = true := by rwa [hbit]
                rw [hw_eq] at hi'
                have hmem := hzb i hi'
                simp only [List.mem_append, List.mem_singleton] at hmem
                rcases hmem with hpre | heqb
                · exact hpre
                · omega
              · have hwltpow : w < 2^i := Nat.lt_of_lt_of_le hwlt (Nat.pow_le_pow_right Nat.zero_lt_two (by omega))
                have hf := Nat.testBit_lt_two_pow hwltpow
                rw [hf] at hi
                contradiction
          have hw_lo : w < 2 ^ lo (b :: bs) B := by
            simp [lo]
            exact hwlt
          have hwmem : w ∈ acc := hcomp w hw_lo hwbits
          dsimp [acc1, step]
          rw [List.mem_append, List.mem_map]
          exact Or.inr ⟨w, hwmem, hw_eq⟩
        · have hzbits : ∀ i, z.testBit i = true → i ∈ pre := by
            intro i hi
            have hmem := hzb i hi
            simp only [List.mem_append, List.mem_singleton] at hmem
            rcases hmem with hpre | heqb
            · exact hpre
            · subst i; contradiction
          have hzlo : z < 2 ^ lo (b :: bs) B := by
            simp [lo]
            apply Nat.lt_pow_two_of_testBit
            intro i hi
            by_cases hmem : i ∈ pre
            · have hilt := hpre_lt_b i hmem
              exact Bool.eq_false_iff.mpr (by omega)
            · exact Bool.eq_false_iff.mpr (by intro ht; exact hmem (hzbits i ht))
          have hzmem := hcomp z hzlo hzbits
          dsimp [acc1, step]
          rw [List.mem_append, List.mem_map]
          exact Or.inl hzmem
      have hnodup1 : acc1.Nodup := by
        dsimp [acc1, step]
        rw [List.nodup_append]
        refine ⟨hnodup, ?_, ?_⟩
        · exact List.Pairwise.map (fun z => z + 2^b) (by intro a c hac; omega) hnodup
        · intro x hx y hy
          rw [List.mem_map] at hy
          rcases hy with ⟨w, hw, rfl⟩
          have hx_lt := hacc x hx
          have hw_lt := hacc w hw
          have htrue := add_pow_bit_true hw_lt (b := b)
          have hfalse := Nat.testBit_lt_two_pow hx_lt (i := b)
          intro heq
          subst x
          rw [htrue] at hfalse
          contradiction
      have hinc1 : (pre ++ [b] ++ bs).Pairwise (· < ·) := by
        simpa using hinc
      have ihres := ih (pre ++ [b]) acc1 hinc1 hbsB hacc1 hbits1 hcomp1 hnodup1
      simpa [List.append_assoc, List.mem_append, lo] using ihres

private theorem width_bound (x : Nat) : x < 2 ^ width x := by
  unfold width
  by_cases h : x == 0
  · have hx : x = 0 := by simpa using h
    subst x
    simp
  · have hne : x ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    exact (Nat.log2_lt hne).mp (Nat.lt_succ_self _)

private theorem submasks_fold (x : Nat) :
    let bl := (List.range (width x)).filter x.testBit
    (∀ z ∈ submasks x, z < 2 ^ width x) ∧
    (∀ z ∈ submasks x, ∀ i, z.testBit i = true → i ∈ bl) ∧
    (∀ z, z < 2 ^ width x → (∀ i, z.testBit i = true → i ∈ bl) → z ∈ submasks x) ∧
    (submasks x).Nodup := by
  let bl := (List.range (width x)).filter x.testBit
  have hpair : bl.Pairwise (· < ·) := by
    dsimp [bl]
    exact List.Pairwise.filter x.testBit List.pairwise_lt_range
  have hblB : ∀ b ∈ bl, b < width x := by
    intro b hb
    dsimp [bl] at hb
    rw [List.mem_filter] at hb
    exact List.mem_range.mp hb.1
  have hacc0 : ∀ z ∈ [0], z < 2 ^ lo bl (width x) := by
    intro z hz
    simp only [List.mem_singleton] at hz
    subst z
    cases bl with
    | nil => simp [lo]; exact Nat.two_pow_pos _
    | cons b bs => simp [lo]; exact Nat.two_pow_pos b
  have hbits0 : ∀ z ∈ [0], ∀ i, z.testBit i = true → i ∈ ([] : List Nat) := by
    intro z hz i hi
    simp only [List.mem_singleton] at hz
    subst z
    simp at hi
  have hcomp0 : ∀ z, z < 2 ^ lo bl (width x) →
      (∀ i, z.testBit i = true → i ∈ ([] : List Nat)) → z ∈ [0] := by
    intro z hz hzbits
    have hz0 : z = 0 := by
      apply Nat.eq_of_testBit_eq
      intro i
      have hf : z.testBit i = false := Bool.eq_false_iff.mpr (by
        intro ht
        simpa using hzbits i ht)
      simp [hf]
    subst z
    simp
  have hres := fold_spec bl [] [0] (width x) (by simpa using hpair) hblB hacc0 hbits0 hcomp0 (by simp)
  dsimp only
  dsimp only at hres
  simpa [bl, submasks, step] using hres

private theorem land_eq_self_iff (x y : Nat) :
    ((y &&& x) == y) = true ↔ ∀ i, y.testBit i = true → x.testBit i = true := by
  constructor
  · intro h i hi
    have heq : y &&& x = y := by simpa using h
    have ht := congrArg (fun n : Nat => n.testBit i) heq
    rw [Nat.testBit_and] at ht
    simp [hi] at ht
    exact ht
  · intro h
    have heq : y &&& x = y := by
      apply Nat.eq_of_testBit_eq
      intro i
      rw [Nat.testBit_and]
      by_cases hy : y.testBit i = true
      · have hx := h i hy
        simp [hy, hx]
      · simp [hy]
    simp [heq]

private theorem submasks_spec (x y : Nat) :
    y ∈ submasks x ↔ ∀ i, y.testBit i = true → x.testBit i = true := by
  constructor
  · intro hy i hi
    have hres := submasks_fold x
    dsimp only at hres
    have hm := hres.2.1 y hy i hi
    simp at hm
    exact hm.2
  · intro h
    have hxlt := width_bound x
    have hxhigh : ∀ i, width x ≤ i → x.testBit i = false := by
      intro i hi
      exact bit_lt_pow hxlt hi
    have hyhigh : ∀ i, width x ≤ i → y.testBit i = false := by
      intro i hi
      cases hybit : y.testBit i
      · rfl
      · have hxi := h i hybit
        rw [hxhigh i hi] at hxi
        contradiction
    have hylt : y < 2 ^ width x := Nat.lt_pow_two_of_testBit y hyhigh
    have hmem : ∀ i, y.testBit i = true → i ∈ (List.range (width x)).filter x.testBit := by
      intro i hi
      have hiw : i < width x := by
        by_cases hle : width x ≤ i
        · have hf := hyhigh i hle
          rw [hf] at hi
          contradiction
        · omega
      rw [List.mem_filter, List.mem_range]
      exact ⟨hiw, h i hi⟩
    have hres := submasks_fold x
    dsimp only at hres
    simpa [submasks, step] using hres.2.2.1 y hylt hmem

private theorem submasks_nodup (x : Nat) : (submasks x).Nodup := by
  have hres := submasks_fold x
  dsimp only at hres
  simpa [submasks, step] using hres.2.2.2

/-! Summing the multiplicities of a nodup list of keys counts exactly the
right rows whose key belongs to that list. -/
private theorem sum_count_filter (ys l : List Nat) (hl : l.Nodup) (p : Nat → Bool) :
    ((l.filter p).map (fun y => ys.count y)).sum =
      ys.countP (fun z => (l.filter p).any (fun y => y == z)) := by
  induction l with
  | nil => simp
  | cons a l' ih =>
      have hl' : l'.Nodup := (List.nodup_cons.mp hl).2
      have hnot : a ∉ l' := (List.nodup_cons.mp hl).1
      by_cases hp : p a
      · simp only [List.filter_cons, hp, ↓reduceIte, List.map_cons, List.sum_cons]
        rw [ih hl']
        have hdisj : ∀ z ∈ ys, (z == a) = true →
            ((l'.filter p).any (fun y => y == z)) = false := by
          intro z hz hza
          cases hany : (l'.filter p).any (fun y => y == z)
          · rfl
          · rw [List.any_eq_true] at hany
            rcases hany with ⟨y, hymem, hyz⟩
            have hza' : z = a := by simpa using hza
            have hyz' : y = z := by simpa using hyz
            have hyl : y ∈ l' := (List.mem_filter.mp hymem).1
            exact (hnot (by simpa [hyz', hza'] using hyl)).elim
        change ys.countP (fun z => z == a) +
          ys.countP (fun z => (l'.filter p).any (fun y => y == z)) =
          ys.countP (fun z => (a :: List.filter p l').any (fun y => y == z))
        calc
          ys.countP (fun z => z == a) +
              ys.countP (fun z => (l'.filter p).any (fun y => y == z)) =
            ys.countP (fun z => (z == a) ||
              (l'.filter p).any (fun y => y == z)) :=
                countP_or_of_disjoint (fun z => z == a)
                  (fun z => (l'.filter p).any (fun y => y == z)) ys hdisj
          _ = ys.countP (fun z => (a :: List.filter p l').any (fun y => y == z)) := by
                apply List.countP_congr
                intro z hz
                change (((z == a) || (l'.filter p).any (fun y => y == z)) = true ↔
                  ((a :: List.filter p l').any (fun y => y == z)) = true)
                rw [List.any_cons]
                cases hza : z == a <;> cases haz : a == z <;> simp_all [hp]
      · simp [hp, ih hl']

private theorem filter_u_irrelevant (l : List Nat) (u t : Nat) (ys : List Nat)
    (hyu : ∀ z ∈ ys, z < u) :
    ((l.filter (fun y => y < u && y ≤ t)).map (fun y => ys.count y)).sum =
    ((l.filter (fun y => y ≤ t)).map (fun y => ys.count y)).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hat : a ≤ t
      · by_cases hau : a < u
        · simp [hat, hau, ih]
        · have hcnt : ys.count a = 0 := by
            rw [List.count_eq_zero]
            intro ha
            have hlt := hyu a ha
            omega
          simp [hat, hau, hcnt, ih]
      · simp [hat, ih]

theorem sound_OffsetCount : Submask.OffsetCount.Goal := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  unfold fan
  simp only [search, List.map_map, Function.comp_apply]
  change ((List.map (fun y => lower ys (y + 1) - lower ys y)
          ((submasks x).filter (fun y => y < u && y ≤ t))).sum =
      ys.countP (fun y => (y &&& x) == y && y ≤ t))
  have hterms :
      List.map (fun y => lower ys (y + 1) - lower ys y)
          ((submasks x).filter (fun y => y < u && y ≤ t)) =
      List.map (fun y => ys.count y)
          ((submasks x).filter (fun y => y < u && y ≤ t)) := by
    apply List.map_congr_left
    intro y hy
    exact lower_singleton_count hs y
  rw [hterms]
  rw [filter_u_irrelevant (submasks x) u t ys (fun z hz => valid_lt hv hz)]
  rw [sum_count_filter ys (submasks x) (submasks_nodup x) (fun y => decide (y ≤ t))]
  apply List.countP_congr
  intro z hz
  have hiff : ((submasks x).filter (fun y => decide (y ≤ t))).any (fun y => y == z) = true ↔
      ((z &&& x) == z && z ≤ t) = true := by
    constructor
    · intro h
      rw [List.any_eq_true] at h
      rcases h with ⟨y, hymem, hyz⟩
      have hyinfo := List.mem_filter.mp hymem
      have hyle : y ≤ t := by simpa using hyinfo.2
      have hyeq : y = z := by simpa using hyz
      subst y
      have hzsub := (submasks_spec x z).1 hyinfo.1
      have hzland := (land_eq_self_iff x z).mpr hzsub
      simp [hzland, hyle]
    · intro h
      simp only [Bool.and_eq_true] at h
      have hzsub : ∀ i, z.testBit i = true → x.testBit i = true := (land_eq_self_iff x z).mp h.1
      have hzmem : z ∈ submasks x := (submasks_spec x z).2 hzsub
      rw [List.any_eq_true]
      refine ⟨z, ?_, by simp⟩
      rw [List.mem_filter]
      exact ⟨hzmem, by simpa using h.2⟩
  exact hiff

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_OffsetCount
