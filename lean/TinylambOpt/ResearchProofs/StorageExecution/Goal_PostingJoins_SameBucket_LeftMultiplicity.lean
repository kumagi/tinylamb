import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SameBucket.LeftMultiplicity` — Charge one left placeholder when zero right keys
  match, including a NULL left key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_LeftMultiplicity

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket

private abbrev opt (a b : Nat) : Ranges :=
  TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b

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

private theorem fan_sameBucket {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  let A := x / (t + 1) * (t + 1)
  let B := min u (A + (t + 1))
  have hmem : ∀ y ∈ ys, (decide (A ≤ y && y < B)) = true ↔ search.test t x y = true := by
    intro y hy
    have hyu : y < u := hv.2 y hy
    constructor
    · intro h
      have hab_y : A ≤ y ∧ y < B := by
        simpa only [decide_eq_true_eq, Bool.and_eq_true] using h
      have hyB' : y < min u (x / (t + 1) * (t + 1) + (t + 1)) := by
        simpa [A, B, Nat.add_assoc] using hab_y.2
      have hlo : x / (t + 1) * (t + 1) ≤ y := by simpa [A] using hab_y.1
      have hhi : y < (x / (t + 1) + 1) * (t + 1) := by
        have hmin : min u (x / (t + 1) * (t + 1) + (t + 1)) ≤
            x / (t + 1) * (t + 1) + (t + 1) := Nat.min_le_right _ _
        have hyA : y < x / (t + 1) * (t + 1) + (t + 1) := by omega
        have hmul : (x / (t + 1) + 1) * (t + 1) =
            x / (t + 1) * (t + 1) + (t + 1) := by
          rw [Nat.add_mul, Nat.one_mul]
        rwa [hmul]
      have hdiv : y / (t + 1) = x / (t + 1) := Nat.div_eq_of_lt_le hlo hhi
      simp [search, hdiv]
    · intro h
      simp only [search, beq_iff_eq] at h
      have hmul : x / (t + 1) * (t + 1) = y / (t + 1) * (t + 1) := by rw [h]
      have hlo : A ≤ y := by
        change x / (t + 1) * (t + 1) ≤ y
        rw [hmul]
        exact Nat.div_mul_le_self y (t + 1)
      have hyA : y < A + (t + 1) := by
        have htmp : y < y / (t + 1) * (t + 1) + (t + 1) :=
          Nat.lt_div_mul_add (by omega : 0 < t + 1)
        rw [← hmul] at htmp
        simpa [A] using htmp
      have hyB : y < B := by
        change y < min u (A + (t + 1))
        rw [Nat.lt_min]
        exact ⟨hyu, hyA⟩
      simp [hlo, hyB]
  have hfan : fan search u t x ys = lower ys B - lower ys A := by
    by_cases hab : A < B
    · have hab' : x / (t + 1) * (t + 1) <
          min u (x / (t + 1) * (t + 1) + (t + 1)) := by
        simpa [A, B, Nat.add_assoc] using hab
      simp [fan, search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab', Nat.add_assoc, A, B]
    · have hab' : ¬ x / (t + 1) * (t + 1) <
          min u (x / (t + 1) * (t + 1) + (t + 1)) := by
        simpa [A, B, Nat.add_assoc] using hab
      have hBA : B ≤ A := by omega
      have hmono := lower_mono hs hBA
      have hzero : lower ys B - lower ys A = 0 := by omega
      simp [fan, search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab', Nat.add_assoc, A, B, hzero]
  rw [hfan]
  by_cases hab : A ≤ B
  · rw [lower_interval hs hab, List.countP_eq_length_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    by_cases hdec : (decide (A ≤ y && y < B)) = true
    · exact hdec.trans ((hmem y hy).mp hdec).symm
    · have hdec_false : (decide (A ≤ y && y < B)) = false := by
        cases hb : decide (A ≤ y && y < B) with
        | false => rfl
        | true => exact False.elim (hdec hb)
      have ht : search.test t x y = false := by
        cases hb : search.test t x y with
        | false => rfl
        | true =>
          have htr := (hmem y hy).mpr hb
          have hbad : (false : Bool) = true := by simpa only [hdec_false] using htr
          cases hbad
      exact hdec_false.trans ht.symm
  · have hBA : B ≤ A := by omega
    have hmono := lower_mono hs hBA
    have hzero : lower ys B - lower ys A = 0 := by omega
    rw [hzero, List.countP_eq_length_filter]
    have hnil : ys.filter (search.test t x) = [] := by
      rw [List.filter_eq_nil_iff]
      intro y hy
      have hdec_false : (decide (A ≤ y && y < B)) = false := by
        by_cases hAy : A ≤ y <;> by_cases hyB : y < B <;> simp [hAy, hyB] <;> omega
      cases hb : search.test t x y with
      | false => simp
      | true =>
        have htr := (hmem y hy).mpr hb
        have hbad : (false : Bool) = true := by simpa only [hdec_false] using htr
        cases hbad
    simp [hnil]

theorem sound_LeftMultiplicity : SameBucket.LeftMultiplicity.Goal := by
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
      fan_sameBucket (u := u) (t := t) (x := v) (ys := ys.filterMap id) hv
    rw [List.countP_eq_length_filter] at hfan
    by_cases h : ((ys.filterMap id).filter (search.test t v)).isEmpty
    · have hlen : ((ys.filterMap id).filter (search.test t v)).length = 0 := by simpa using h
      simp [h, hfan, hlen]
    · have hne : ((ys.filterMap id).filter (search.test t v)).length ≠ 0 := by
        intro hz
        exact h (by simpa using hz)
      have hlen : 0 < ((ys.filterMap id).filter (search.test t v)).length := by omega
      have hmax : max 1 ((ys.filterMap id).filter (search.test t v)).length =
          ((ys.filterMap id).filter (search.test t v)).length := Nat.max_eq_right (by omega)
      simp [h, hfan, hmax]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_LeftMultiplicity
