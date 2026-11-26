import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_LeftMultiplicity

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask

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
    hs
  simpa using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq]
    have hiys : i < ys.length := by omega
    have hlt : ys.getD i 0 < v := lower_lt_of_lt hs v (Nat.lt_of_lt_of_le hi (Nat.min_le_left _ _))
    rwa [getD_eq_getElem hiys] at hlt
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    intro hlt
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    omega
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take]; simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

private theorem filter_interval_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    (ys.filter (fun y => decide (y < b))).length =
      (ys.filter (fun y => decide (y < a))).length +
        (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
    have hs' := hs.tail
    by_cases hxa : x < a
    · have hxb : x < b := by omega
      simp [hxa, hxb, ih hs'] <;> omega
    · by_cases hxb : x < b
      · have hax : a ≤ x := by omega
        have hnil : xs.filter (fun y => decide (y < a)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hfilter : xs.filter (fun y => decide (y < b)) =
            xs.filter (fun y => decide (a ≤ y && y < b)) := by
          apply List.filter_congr
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          by_cases hyb : y < b
          · simp [hyb, Nat.le_trans hax hxy]
          · simp [hyb]
        simp [hxa, hxb, hax, hnil, hfilter]
      · have hxa' : a ≤ x := by omega
        have hxb' : b ≤ x := by omega
        have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hnil_b : xs.filter (fun y => decide (y < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hnil_ab : xs.filter (fun y => decide ((decide (a ≤ y) && decide (y < b)) = true)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          by_cases hya : a ≤ y <;> by_cases hyb : y < b <;> simp [hya, hyb] <;> omega
        have hnil_ab' : xs.filter (fun y => decide (a ≤ y) && decide (y < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          by_cases hya : a ≤ y <;> by_cases hyb : y < b <;> simp [hya, hyb] <;> omega
        simp [hxa, hxb, hnil_a, hnil_b, hnil_ab, hnil_ab']

private theorem lower_interval {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  have hlen := filter_interval_length hs hab
  omega

/-! ## Submask enumeration -/

/-! ## Bit lemmas (div/mod characterization) -/

private theorem testBit_eq_mod (m i : Nat) : m.testBit i = decide ((m / 2 ^ i) % 2 = 1) := by
  simpa using (Nat.testBit_add m 0 i)

private theorem add_pow_two_testBit_self {z b : Nat} (hz : z.testBit b = false) : (z + 2^b).testBit b = true := by
  rw [testBit_eq_mod z b] at hz
  rw [testBit_eq_mod (z + 2^b) b]
  have hpos : 0 < 2 ^ b := Nat.pow_pos (by decide)
  have hdiv : (z + 2 ^ b) / 2 ^ b = z / 2 ^ b + 1 := by
    simpa [Nat.mul_comm] using (Nat.add_mul_div_right z 1 hpos)
  rw [hdiv]
  simp only [decide_eq_true_eq, decide_eq_false_iff_not] at hz ⊢
  have hlt : (z / 2 ^ b) % 2 < 2 := Nat.mod_lt _ (by decide)
  have hz0 : (z / 2 ^ b) % 2 = 0 := by omega
  rw [Nat.add_mod]
  omega

private theorem add_pow_two_testBit_of_ne {z b i : Nat} (hib : i ≠ b) (hzlt : z < 2^b) :
    (z + 2^b).testBit i = z.testBit i := by
  rw [testBit_eq_mod z i, testBit_eq_mod (z + 2^b) i]
  have hposi : 0 < 2 ^ i := Nat.pow_pos (by decide)
  by_cases hib' : i < b
  · have hsum : i + (b - i) = b := by omega
    have hpow : 2 ^ b = 2 ^ i * 2 ^ (b - i) := by
      calc
        2 ^ b = 2 ^ (i + (b - i)) := by rw [hsum]
        _ = 2 ^ i * 2 ^ (b - i) := by rw [Nat.pow_add]
    have hdiv : (z + 2 ^ b) / 2 ^ i = z / 2 ^ i + 2 ^ (b - i) := by
      rw [hpow]
      simpa [Nat.mul_comm] using (Nat.add_mul_div_left z (2 ^ (b - i)) hposi)
    rw [hdiv]
    have hsucc : b - i = (b - i - 1) + 1 := by omega
    rw [hsucc, Nat.pow_succ]
    rw [Nat.mul_comm (2 ^ (b - i - 1)) 2, Nat.add_mul_mod_self_left]
  · have hbi : b < i := by omega
    have hlepow : 2 ^ b ≤ 2 ^ i := Nat.pow_le_pow_right (by decide) (by omega)
    have hzlt' : z < 2 ^ i := Nat.lt_of_lt_of_le hzlt hlepow
    have hsumlt : z + 2 ^ b < 2 ^ i := by
      have hpowle : 2 ^ (b + 1) ≤ 2 ^ i := Nat.pow_le_pow_right (by decide) (by omega)
      have hzsum : z + 2 ^ b < 2 ^ (b + 1) := by
        rw [Nat.pow_succ]
        omega
      exact Nat.lt_of_lt_of_le hzsum hpowle
    have hdiv1 : (z + 2 ^ b) / 2 ^ i = 0 := Nat.div_eq_of_lt hsumlt
    have hdiv2 : z / 2 ^ i = 0 := Nat.div_eq_of_lt hzlt'
    rw [hdiv1, hdiv2]

private theorem sub_pow_two_testBit_self {z b : Nat} (hwle : 2^b ≤ z) (hz : z.testBit b = true) :
    (z - 2^b).testBit b = false := by
  rw [testBit_eq_mod z b] at hz
  rw [testBit_eq_mod (z - 2^b) b]
  simp only [decide_eq_true_eq] at hz
  simp only [decide_eq_false_iff_not]
  have hpos : 0 < 2 ^ b := Nat.pow_pos (by decide)
  let w := 2 ^ b
  have hdecomp : w * (z / w) + z % w = z := Nat.div_add_mod z w
  have hmodlt : z % w < w := Nat.mod_lt _ hpos
  have hqpos : 1 ≤ z / w := by
    by_cases hq0 : z / w = 0
    · rw [hq0, Nat.mul_zero, Nat.zero_add] at hdecomp
      omega
    · exact Nat.pos_of_ne_zero hq0
  have hwle_q : w ≤ w * (z / w) := by
    simpa using (Nat.mul_le_mul_left w hqpos)
  have hzsub : z - w = w * (z / w - 1) + z % w := by
    calc
      z - w = (w * (z / w) + z % w) - w := by rw [hdecomp]
      _ = (z % w + w * (z / w)) - w := by rw [Nat.add_comm]
      _ = z % w + (w * (z / w) - w) := Nat.add_sub_assoc hwle_q (z % w)
      _ = w * (z / w - 1) + z % w := by
        rw [Nat.mul_sub_left_distrib, Nat.mul_one, Nat.add_comm]
  have hdivw : (z % w) / w = 0 := Nat.div_eq_of_lt hmodlt
  have hdiv : (z - w) / w = z / w - 1 := by
    rw [hzsub, Nat.mul_comm w (z / w - 1), Nat.add_comm]
    simpa [w, hdivw] using (Nat.add_mul_div_right (z % w) (z / w - 1) hpos)
  rw [show 2 ^ b = w by rfl, hdiv]
  have hqdecomp := Nat.div_add_mod (z / w) 2
  rw [hz] at hqdecomp
  have hq : z / w = 2 * ((z / w) / 2) + 1 := hqdecomp.symm
  have hqm1 : z / w - 1 = 2 * ((z / w) / 2) := by omega
  rw [hqm1, Nat.mul_mod_right]
  simp

private theorem odd_div_even {q t : Nat} (ht : 0 < t) (hq : q % 2 = 1) :
    (q - 1) / (2 * t) = q / (2 * t) := by
  let s := 2 * t
  have hspos : 0 < s := by omega
  have hdecomp : s * (q / s) + q % s = q := Nat.div_add_mod q s
  have hmodlt : q % s < s := Nat.mod_lt _ hspos
  have hqdecomp := Nat.div_add_mod q 2
  rw [hq] at hqdecomp
  have hqeq : q = 2 * (q / 2) + 1 := hqdecomp.symm
  have hmodpos : 0 < q % s := by
    by_cases hz : q % s = 0
    · exfalso
      rw [hz, Nat.add_zero] at hdecomp
      have hs_eq : s * (q / s) = 2 * (t * (q / s)) := by
        simp [s, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
      rw [hs_eq] at hdecomp
      rw [hqeq] at hdecomp
      omega
    · exact Nat.pos_of_ne_zero hz
  have hqsub : q - 1 = s * (q / s) + (q % s - 1) := by omega
  rw [hqsub]
  rw [Nat.add_comm, Nat.mul_comm s (q / s)]
  rw [Nat.add_mul_div_right (q % s - 1) (q / s) hspos]
  have hdivrem : (q % s - 1) / s = 0 := Nat.div_eq_of_lt (by omega)
  rw [hdivrem, Nat.zero_add]

private theorem sub_pow_two_testBit_of_ne {z b i : Nat} (hib : i ≠ b) (hz : z.testBit b = true) :
    (z - 2^b).testBit i = z.testBit i := by
  rw [testBit_eq_mod z b] at hz
  rw [testBit_eq_mod z i, testBit_eq_mod (z - 2^b) i]
  simp only [decide_eq_true_eq] at hz
  have hposb : 0 < 2 ^ b := Nat.pow_pos (by decide)
  have hposi : 0 < 2 ^ i := Nat.pow_pos (by decide)
  let w := 2 ^ b
  let q := z / w
  let r := z % w
  have hdecomp : w * q + r = z := Nat.div_add_mod z w
  have hmodlt : r < w := Nat.mod_lt _ hposb
  have hqpos : 1 ≤ q := by
    by_cases hq0 : q = 0
    · rw [hq0, Nat.mul_zero, Nat.zero_add] at hdecomp
      change q % 2 = 1 at hz
      rw [hq0] at hz
      simp at hz
    · exact Nat.pos_of_ne_zero hq0
  have hwleq : w ≤ w * q := by simpa using (Nat.mul_le_mul_left w hqpos)
  have hzsub : z - w = w * (q - 1) + r := by
    calc
      z - w = (w * q + r) - w := by rw [hdecomp]
      _ = (r + w * q) - w := by rw [Nat.add_comm]
      _ = r + (w * q - w) := Nat.add_sub_assoc hwleq r
      _ = w * (q - 1) + r := by rw [Nat.mul_sub_left_distrib, Nat.mul_one, Nat.add_comm]
  have hdivw : (z - w) / w = q - 1 := by
    rw [hzsub, Nat.mul_comm w (q - 1), Nat.add_comm]
    simpa [w, Nat.div_eq_of_lt hmodlt] using
      (Nat.add_mul_div_right r (q - 1) hposb)
  have hkdecomp := Nat.div_add_mod q 2
  rw [hz] at hkdecomp
  have hqeq : q = 2 * (q / 2) + 1 := hkdecomp.symm
  have hqm1 : q - 1 = 2 * (q / 2) := by omega
  by_cases hib' : i < b
  · have hsum : i + (b - i) = b := by omega
    have hpow : w = 2 ^ i * 2 ^ (b - i) := by
      change 2 ^ b = 2 ^ i * 2 ^ (b - i)
      calc
        2 ^ b = 2 ^ (i + (b - i)) := by rw [hsum]
        _ = 2 ^ i * 2 ^ (b - i) := by rw [Nat.pow_add]
    have hdivz : z / 2 ^ i = 2 ^ (b - i) * q + r / 2 ^ i := by
      rw [← hdecomp, hpow]
      rw [Nat.mul_assoc]
      rw [Nat.mul_add_div hposi]
    have hdivsub : (z - w) / 2 ^ i = 2 ^ (b - i) * (q - 1) + r / 2 ^ i := by
      rw [hzsub, hpow]
      rw [Nat.mul_assoc]
      rw [Nat.mul_add_div hposi]
    rw [hdivz, hdivsub]
    have hbsucc : b - i = (b - i - 1) + 1 := by omega
    rw [hbsucc, Nat.pow_succ]
    rw [hqm1]
    have heven1 : 2 ^ (b - i - 1) * 2 * q = 2 * (2 ^ (b - i - 1) * q) := by
      simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    have heven2 : 2 ^ (b - i - 1) * 2 * (2 * (q / 2)) = 2 * (2 ^ (b - i - 1) * 2 * (q / 2)) := by
      simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    rw [heven1, heven2, Nat.add_comm _ (r / 2 ^ i), Nat.add_comm _ (r / 2 ^ i)]
    rw [Nat.add_mul_mod_self_left, Nat.add_mul_mod_self_left]
  · have hbi : b < i := by omega
    have hsum : b + (i - b) = i := by omega
    have hpow : 2 ^ i = w * 2 ^ (i - b) := by
      change 2 ^ i = 2 ^ b * 2 ^ (i - b)
      calc
        2 ^ i = 2 ^ (b + (i - b)) := by rw [hsum]
        _ = 2 ^ b * 2 ^ (i - b) := by rw [Nat.pow_add]
    have hdivz : z / 2 ^ i = q / 2 ^ (i - b) := by
      rw [hpow, ← Nat.div_div_eq_div_mul]
    have hdivsub : (z - w) / 2 ^ i = (q - 1) / 2 ^ (i - b) := by
      rw [hpow, ← Nat.div_div_eq_div_mul, hdivw]
    rw [hdivz, hdivsub]
    have hisucc : i - b = (i - b - 1) + 1 := by omega
    rw [hisucc, Nat.pow_succ]
    rw [Nat.mul_comm (2 ^ (i - b - 1)) 2]
    rw [odd_div_even (Nat.pow_pos (by decide)) (by change q % 2 = 1 at hz; exact hz)]

/-! ## Submask enumeration -/

private theorem submasks_sub {x y : Nat} (hy : y ∈ submasks x) : (y &&& x) == y := by
  unfold submasks at hy
  have hbits_pair : ((List.range (width x)).filter x.testBit).Pairwise (· < ·) := by
    have hrange : (List.range (width x)).Pairwise (· < ·) := by
      rw [List.pairwise_iff_getElem]
      intro i j hi hj hij
      rw [List.getElem_range (by simpa using hi), List.getElem_range (by simpa using hj)]
      exact hij
    exact hrange.sublist (List.filter_sublist (p := x.testBit))
  suffices h : ∀ (bits acc : List Nat),
      (∀ z ∈ acc, (z &&& x) == z) →
      (∀ b ∈ bits, x.testBit b = true) →
      bits.Pairwise (· < ·) →
      (∀ b ∈ bits, ∀ z ∈ acc, z < 2 ^ b) →
      ∀ z ∈ bits.foldl (fun acc bit => acc ++ acc.map (· + 2 ^ bit)) acc, (z &&& x) == z from
    h _ [0] (by simp) (by intro b hb; exact (List.mem_filter.mp hb).2) hbits_pair
      (by intro b hb z hz; simp at hz; subst z; exact Nat.pow_pos (by decide)) y hy
  intro bits
  induction bits with
  | nil => intro acc hacc _ _ _ z hz; exact hacc z hz
  | cons b bs ih =>
    intro acc hacc hbits hbs hbound z hz
    simp only [List.foldl_cons] at hz
    refine ih (acc ++ acc.map (fun z => z + 2 ^ b)) ?_ ?_ ?_ ?_ z hz
    · intro z' hz'
      simp only [List.mem_append, List.mem_map] at hz'
      rcases hz' with hz' | ⟨z'', hz'', rfl⟩
      · exact hacc z' hz'
      · have hzb_false : z''.testBit b = false :=
          Nat.testBit_lt_two_pow (hbound b ((by simp : b ∈ b :: bs)) z'' hz'')
        apply beq_of_eq
        apply Nat.eq_of_testBit_eq
        intro i
        rw [Nat.testBit_and]
        by_cases hib : i = b
        · subst i
          rw [add_pow_two_testBit_self hzb_false, hbits b ((by simp : b ∈ b :: bs))]
          simp
        · have hne := add_pow_two_testBit_of_ne hib
              (hbound b ((by simp : b ∈ b :: bs)) z'' hz'')
          rw [hne]
          have hz''sub := eq_of_beq (hacc z'' hz'')
          have hi := congrArg (fun n : Nat => n.testBit i) hz''sub
          simpa [Nat.testBit_and] using hi
    · intro c hc
      exact hbits c (List.mem_cons_of_mem b hc)
    · exact (List.pairwise_cons.mp hbs).2
    · intro c hc z' hz'
      simp only [List.mem_append, List.mem_map] at hz'
      rcases hz' with hz' | ⟨z'', hz'', rfl⟩
      · exact hbound c (List.mem_cons_of_mem b hc) z' hz'
      · have hbc : b < c := (List.pairwise_cons.mp hbs).1 c hc
        have hz''lt : z'' < 2 ^ b := hbound b ((by simp : b ∈ b :: bs)) z'' hz''
        have hpowle : 2 ^ (b + 1) ≤ 2 ^ c :=
          Nat.pow_le_pow_right (by decide) (by omega)
        have hsum : z'' + 2 ^ b < 2 ^ (b + 1) := by
          rw [Nat.pow_succ]
          omega
        exact Nat.lt_of_lt_of_le hsum hpowle

private theorem mem_submasks_of_submask {x y : Nat} (hy : (y &&& x) == y) : y ∈ submasks x := by
  unfold submasks
  let bits := (List.range (width x)).filter x.testBit
  have hbits_all : ∀ b ∈ bits, x.testBit b = true := fun b hb => (List.mem_filter.mp hb).2
  have hbits_pair : bits.Pairwise (· < ·) := by
    have hrange : (List.range (width x)).Pairwise (· < ·) := by
      rw [List.pairwise_iff_getElem]
      intro i j hi hj hij
      rw [List.getElem_range (by simpa using hi), List.getElem_range (by simpa using hj)]
      exact hij
    exact hrange.sublist (List.filter_sublist (p := x.testBit))
  have submask_bits : ∀ z, (z &&& x) == z → ∀ b, z.testBit b = true → b ∈ bits := by
    intro z hzsub b hb
    have hbx : x.testBit b = true := by
      have h := congrArg (fun n : Nat => n.testBit b) (eq_of_beq hzsub)
      simpa [Nat.testBit_and, hb] using h
    by_cases hbw : b < width x
    · simp [bits, List.mem_filter, List.mem_range, hbw, hbx]
    · exfalso
      have hxb' : x < 2 ^ b := by
        by_cases hx0 : x = 0
        · simp [hx0] at hbx
        · simp [width, hx0] at hbw
          exact Nat.lt_of_lt_of_le Nat.lt_log2_self (Nat.pow_le_pow_right (by decide) hbw)
      exact absurd hbx (by simp [Nat.testBit_lt_two_pow hxb'])
  suffices h : ∀ (bs acc : List Nat),
      (∀ b ∈ bs, x.testBit b = true) →
      bs.Pairwise (· < ·) →
      (∀ z, (z &&& x) == z → (∀ b, z.testBit b = true → b ∉ bs) → z ∈ acc) →
      (∀ b ∈ bs, ∀ z ∈ acc, z < 2 ^ b) →
      ∀ z, (z &&& x) == z →
        z ∈ bs.foldl (fun acc bit => acc ++ acc.map (· + 2 ^ bit)) acc from
    h bits [0] hbits_all hbits_pair
      (by
        intro z hzsub hno
        have hz0 : z = 0 := by
          by_cases hz0 : z = 0
          · exact hz0
          · exfalso
            have ⟨b, hb⟩ : ∃ b, z.testBit b = true := by
              by_cases hbex : ∃ b, z.testBit b = true
              · exact hbex
              · exfalso
                have hz0' : z = 0 := by
                  apply Nat.eq_of_testBit_eq
                  intro i
                  have hi : z.testBit i ≠ true := by
                    intro hi
                    exact hbex ⟨i, hi⟩
                  cases hzi : z.testBit i <;> simp_all
                exact hz0 hz0'
            exact (hno b hb) (submask_bits z hzsub b hb)
        simp [hz0])
      (by intro b hb z hz; simp at hz; subst z; exact Nat.pow_pos (by decide))
      y hy
  intro bs
  induction bs with
  | nil =>
    intro acc _ _ hacc _ z hz
    exact hacc z hz (by intro b hb; simp)
  | cons b bs ih =>
    intro acc hbits hbs hacc hbound z hz
    simp only [List.foldl_cons]
    let acc' := acc ++ acc.map (fun z => z + 2 ^ b)
    have hacc' : ∀ z0, (z0 &&& x) == z0 →
        (∀ c, z0.testBit c = true → c ∉ bs) → z0 ∈ acc' := by
      intro z0 hz0sub hz0bs
      by_cases hzb0 : z0.testBit b = true
      · have hb20 : 2 ^ b ≤ z0 := Nat.ge_two_pow_of_testBit hzb0
        have hz0' : ((z0 - 2 ^ b) &&& x) == z0 - 2 ^ b := by
          apply beq_of_eq
          apply Nat.eq_of_testBit_eq
          intro i
          rw [Nat.testBit_and]
          by_cases hib : i = b
          · subst i
            rw [sub_pow_two_testBit_self hb20 hzb0]
            simp
          · rw [sub_pow_two_testBit_of_ne hib hzb0]
            have hi := congrArg (fun n : Nat => n.testBit i) (eq_of_beq hz0sub)
            simpa [Nat.testBit_and] using hi
        have hz0'bs : ∀ c, (z0 - 2 ^ b).testBit c = true → c ∉ bs := by
          intro c hc hcbs
          have hcb : c ≠ b := by
            intro h
            subst c
            exact (hz0bs b hzb0) hcbs
          have hc0 : z0.testBit c = true := by
            rw [← sub_pow_two_testBit_of_ne hcb hzb0]
            exact hc
          exact hz0bs c hc0 hcbs
        have hmem := hacc (z0 - 2 ^ b) hz0' (by
          intro c hc hcbs
          have hcbs'' : c ∈ bs := by
            rcases List.mem_cons.mp hcbs with hcb' | hcbs'
            · subst c
              rw [sub_pow_two_testBit_self hb20 hzb0] at hc
              cases hc
            · exact hcbs'
          exact hz0'bs c hc hcbs'')
        simp only [acc', List.mem_append, List.mem_map]
        exact Or.inr ⟨z0 - 2 ^ b, hmem, Nat.sub_add_cancel hb20⟩
      · have hz0bs' : ∀ c, z0.testBit c = true → c ∉ bs := by
          intro c hc hcbs
          exact hz0bs c hc hcbs
        have hmem := hacc z0 hz0sub (by
          intro c hc hcbs
          have hcbs'' : c ∈ bs := by
            rcases List.mem_cons.mp hcbs with hcb' | hcbs'
            · subst c
              exact False.elim (hzb0 hc)
            · exact hcbs'
          exact hz0bs' c hc hcbs'')
        simp only [acc', List.mem_append, List.mem_map]
        exact Or.inl hmem
    have hbound' : ∀ c ∈ bs, ∀ z0 ∈ acc', z0 < 2 ^ c := by
      intro c hc z0 hz0
      simp only [acc', List.mem_append, List.mem_map] at hz0
      rcases hz0 with hz0 | ⟨z'', hz'', rfl⟩
      · exact hbound c (List.mem_cons_of_mem b hc) z0 hz0
      · have hbc : b < c := (List.pairwise_cons.mp hbs).1 c hc
        have hz''lt : z'' < 2 ^ b := hbound b (by simp : b ∈ b :: bs) z'' hz''
        have hpowle : 2 ^ (b + 1) ≤ 2 ^ c :=
          Nat.pow_le_pow_right (by decide) (by omega)
        have hsum : z'' + 2 ^ b < 2 ^ (b + 1) := by
          rw [Nat.pow_succ]
          omega
        exact Nat.lt_of_lt_of_le hsum hpowle
    exact ih acc'
      (fun c hc => hbits c (List.mem_cons_of_mem b hc))
      (List.pairwise_cons.mp hbs).2 hacc' hbound' z hz

private theorem submasks_pairwise_lt (x : Nat) : (submasks x).Pairwise (· < ·) := by
  unfold submasks
  suffices h : ∀ (bs : List Nat) (acc : List Nat),
      acc.Pairwise (· < ·) →
      (∀ b ∈ bs, ∀ z ∈ acc, z < 2 ^ b) →
      bs.Pairwise (· < ·) →
      (bs.foldl (fun acc bit => acc ++ acc.map (· + 2 ^ bit)) acc).Pairwise (· < ·) by
    refine h _ [0] (by simp) ?_ ?_
    · intro b hb z hz; simp at hz; subst hz; exact Nat.pow_pos (by decide)
    · have hrange : (List.range (width x)).Pairwise (· < ·) := by
        rw [List.pairwise_iff_getElem]
        intro i j hi hj hij
        rw [List.getElem_range (by simpa using hi), List.getElem_range (by simpa using hj)]
        exact hij
      exact hrange.sublist (List.filter_sublist (p := x.testBit))
  intro bs acc hacc hbound hbs
  induction bs generalizing acc with
  | nil => simpa
  | cons b bs ih =>
    simp only [List.foldl_cons]
    refine ih (acc ++ acc.map (fun z => z + 2 ^ b)) ?_ ?_ ?_
    · rw [List.pairwise_append]
      refine ⟨hacc, ?_, ?_⟩
      · exact List.Pairwise.map (fun z : Nat => z + 2 ^ b)
          (fun a a' h => Nat.add_lt_add_right h (2 ^ b)) hacc
      · intro a ha c hc
        simp only [List.mem_map] at hc
        rcases hc with ⟨a', ha', rfl⟩
        have : a < 2 ^ b := hbound b List.mem_cons_self a ha
        omega
    · intro b' hb' z hz
      simp only [List.mem_append, List.mem_map] at hz
      have hbb' : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
      rcases hz with hz | ⟨z', hz', rfl⟩
      · have : z < 2 ^ b := hbound b List.mem_cons_self z hz
        exact Nat.lt_of_lt_of_le this (Nat.pow_le_pow_right (by decide) (by omega))
      · have : z' < 2 ^ b := hbound b List.mem_cons_self z' hz'
        calc z' + 2 ^ b < 2 ^ b + 2 ^ b := by omega
          _ = 2 ^ (b + 1) := by rw [Nat.pow_succ]; omega
          _ ≤ 2 ^ b' := Nat.pow_le_pow_right (by decide) hbb'
    · exact (List.pairwise_cons.mp hbs).2

private theorem filter_mem_cons_length (y : Nat) (zs : List Nat) (hyzs : y ∉ zs)
    (ys : List Nat) :
    (ys.filter (fun z => z == y)).length + (ys.filter (fun z => decide (z ∈ zs))).length =
      (ys.filter (fun z => (z == y) || decide (z ∈ zs))).length := by
  induction ys with
  | nil => simp
  | cons b ys ih =>
    by_cases hby : b == y
    · have hbzs : b ∉ zs := by
        intro h
        exact hyzs (by simpa [eq_of_beq hby] using h)
      simp [hby, hbzs, ih, Nat.add_assoc]
      omega
    · by_cases hbzs : b ∈ zs
      · simp [hby, hbzs, ih]
        omega
      · simp [hby, hbzs, ih]

private theorem sum_count_filter_eq (zs : List Nat) (hzs : zs.Nodup) (ys : List Nat) :
    (zs.map (fun y => (ys.filter (fun z => z == y)).length)).sum =
      (ys.filter (fun z => z ∈ zs)).length := by
  induction zs with
  | nil =>
    induction ys with
    | nil => rfl
    | cons a ys ihys =>
      change 0 = (List.filter (fun z => false) ys).length
      exact ihys
  | cons y zs ih =>
    have hyzs : y ∉ zs := by
      intro hmem
      exact (List.pairwise_cons.mp hzs).1 y hmem rfl
    have hzs' : zs.Nodup := (List.pairwise_cons.mp hzs).2
    simp only [List.map_cons, List.sum_cons]
    rw [ih hzs']
    have hfilter : (ys.filter (fun z => z ∈ y :: zs)).length =
        (ys.filter (fun z => (z == y) || decide (z ∈ zs))).length := by
      congr 1
      apply List.filter_congr
      intro z hz
      by_cases hzy : z = y
      · subst z
        simp
      · have hbeq : (z == y) = false := by simpa [beq_iff_eq] using hzy
        have hyz : y ≠ z := by intro h; exact hzy h.symm
        simp [hbeq, hzy, hyz]
    rw [hfilter]
    exact filter_mem_cons_length y zs hyzs ys

private theorem fan_submask {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  let zs := (submasks x).filter (fun y => y < u && y ≤ t)
  have hzs_pair : zs.Pairwise (· < ·) := by
    exact (submasks_pairwise_lt x).sublist (List.filter_sublist (p := fun y => y < u && y ≤ t))
  have hnodup : zs.Nodup := hzs_pair.imp (fun {a b} h => by omega)
  have hfan : fan search u t x ys =
      (zs.map (fun y => lower ys (y + 1) - lower ys y)).sum := by
    simp [fan, search, zs, Function.comp_def]
  rw [hfan]
  calc
    (zs.map (fun y => lower ys (y + 1) - lower ys y)).sum =
        (zs.map (fun y => (ys.filter (fun z => z == y)).length)).sum := by
      congr 1
      apply List.map_congr_left
      intro y hy
      rw [lower_interval hs (Nat.le_succ y)]
      congr 1
      apply List.filter_congr
      intro z hz
      by_cases hzy : z = y
      · subst z
        simp
      · by_cases hle : y ≤ z
        · have hnlt : ¬ z < y + 1 := by omega
          simp [hle, hnlt, hzy, beq_iff_eq]
        · simp [hle, hzy, beq_iff_eq]
    _ = (ys.filter (fun z => z ∈ zs)).length := sum_count_filter_eq zs hnodup ys
    _ = (ys.filter (search.test t x)).length := by
      congr 1
      apply List.filter_congr
      intro z hz
      have hzu : z < u := hv.2 z hz
      rw [Bool.eq_iff_iff]
      simp only [search, decide_eq_true_eq, Bool.and_eq_true, beq_iff_eq]
      constructor
      · intro hmem
        simp only [zs, List.mem_filter] at hmem
        have hzu' : z < u := by simpa using (Bool.and_eq_true_iff.mp hmem.2).1
        have hzt : z ≤ t := by simpa using (Bool.and_eq_true_iff.mp hmem.2).2
        exact ⟨eq_of_beq (submasks_sub hmem.1), hzt⟩
      · intro ht
        simp only [zs, List.mem_filter]
        exact ⟨mem_submasks_of_submask (beq_of_eq ht.1), by simp [hzu, ht.2]⟩
    _ = ys.countP (search.test t x) := (List.countP_eq_length_filter).symm

theorem sound_LeftMultiplicity : Submask.LeftMultiplicity.Goal := by
  intro u t xs ys hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none =>
    simp [Option.any]
  | some v =>
    simp only [Option.any, Option.map_some, Option.getD_some]
    have hfan : fan search u t v (ys.filterMap id) =
        (ys.filterMap id).countP (search.test t v) :=
      fan_submask (u := u) (t := t) (x := v) (ys := ys.filterMap id) hv
    rw [List.countP_eq_length_filter] at hfan
    by_cases h : ((ys.filterMap id).filter (search.test t v)).isEmpty
    · have hlen : ((ys.filterMap id).filter (search.test t v)).length = 0 := by simpa using h
      simp [h, hfan, hlen]
    · have hne : ((ys.filterMap id).filter (search.test t v)).length ≠ 0 := by
        intro hz
        exact h (by simpa using hz)
      have hlen : 0 < ((ys.filterMap id).filter (search.test t v)).length := by omega
      have hmax : max 1 ((ys.filterMap id).filter (search.test t v)).length =
          ((ys.filterMap id).filter (search.test t v)).length := Nat.max_eq_right hlen
      simp [h, hfan, hmax]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_LeftMultiplicity
