import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SameBucket.OffsetCount` — Two lower-bound offsets per interval compute the exact
  join multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_OffsetCount

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Self-contained certificate for `SameBucket.OffsetCount`.

The binary lower bound on a sorted right-key column cuts exactly at the
filter boundary.  For a bucket `[a, a+w)` (clipped to the domain), the two
offsets therefore count precisely the right keys whose bucket equals the
left key's bucket. -/

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
      change Cut xs.toList v lo ∧ lo ≤ lo ∧ lo ≤ lo
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, True.intro, True.intro⟩
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
        · simp only [hv, ↓reduceIte]
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
                  rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                  exact hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [hlistval] at hrel
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, by omega, hub⟩
        · simp only [hv, ↓reduceIte]
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
                  rw [getD_eq_getElem hmidlen, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                rw [hlistval] at hrel
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]
      omega
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
    (by
      intro j hj hjlen
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

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem filter_prefix_lt {xs : List Nat} (hs : xs.Pairwise (· ≤ ·)) (v i : Nat)
    (hi : i < (xs.filter (fun x => decide (x < v))).length) : xs.getD i 0 < v := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hx : x < v
      · have hi' : i < (xs.filter (fun y => decide (y < v))).length + 1 := by simpa [hx] using hi
        cases i with
        | zero => simp [hx]
        | succ i =>
            have hi'' : i < (xs.filter (fun y => decide (y < v))).length := by omega
            simpa using ih hs' i hi''
      · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          intro hyv
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hx, hnil] at hi

private theorem filter_lt_length_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {v j : Nat}
    (hj : j < ys.length) (hlt : ys[j] < v) :
    j < (ys.filter (fun x => decide (x < v))).length := by
  induction ys generalizing j with
  | nil => simp at hj
  | cons x xs ih =>
      have hs' := hs.tail
      cases j with
      | zero =>
          have hx : x < v := by simpa using hlt
          simp [hx]
      | succ j =>
          have hjxs : j < xs.length := by simpa using hj
          have hxj : xs[j] < v := by simpa using hlt
          have htail := ih hs' hjxs hxj
          by_cases hx : x < v
          · simpa [hx] using Nat.succ_lt_succ htail
          · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
              apply List.filter_eq_nil_iff.mpr
              intro y hy
              simp only [decide_eq_true_eq]
              intro hyv
              have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
              omega
            simp [hx, hnil] at htail

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hlt : (ys.filter (fun x => decide (x < v))).length < lower ys v := h
    have hlow := lower_lt_of_lt hs v hlt
    have hlen : (ys.filter (fun x => decide (x < v))).length < ys.length :=
      Nat.lt_of_lt_of_le hlt (lower_le_length hs v)
    rw [getD_eq_getElem hlen] at hlow
    have hcontra := filter_lt_length_of_lt hs hlen hlow
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hln : lower ys v < (ys.filter (fun x => decide (x < v))).length := h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le hln (List.length_filter_le _ _)
    have hge := lower_le_of_ge hs v (Nat.le_refl _) hlen
    have hp := filter_prefix_lt hs v (lower ys v) hln
    omega

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun x => decide (a ≤ x && x < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            intro hya
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnil, ih hs']
        · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            intro hya
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            intro h
            rcases h with ⟨hay, hyb⟩
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

theorem sound_OffsetCount : SameBucket.OffsetCount.Goal := by
  intro u t x ys hv
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  let w := t + 1
  have hw : 0 < w := by omega
  let a := x / w * w
  let b := min u (a + w)
  have hspan : search.spans u t x = optional a b := by
    rfl
  by_cases hab : a < b
  · have hfan : fan search u t x ys = lower ys b - lower ys a := by
      simp [fan, hspan, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    rw [hfan, lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
    have hsplit := filter_lt_prefix_filter_lt hs (Nat.le_of_lt hab)
    rw [hsplit, List.length_append, Nat.add_sub_cancel_left]
    rw [← List.countP_eq_length_filter]
    apply List.countP_congr
    intro y hy
    have hyu : y < u := hv.2 y hy
    have hiff : (a ≤ y ∧ y < b) ↔ y / w = x / w := by
      constructor
      · rintro ⟨ha, hb⟩
        have hle : x / w ≤ y / w := by
          have ha' : x / w * w ≤ y := by simpa [a] using ha
          have hdiv : (x / w * w) / w ≤ y / w := Nat.div_le_div_right ha'
          have hcancel : (x / w * w) / w = x / w := by
            rw [Nat.mul_comm]
            exact Nat.mul_div_right (x / w) hw
          rwa [hcancel] at hdiv
        have hlt : y / w < x / w + 1 := by
          rw [Nat.div_lt_iff_lt_mul hw]
          have hylt : y < a + w := Nat.lt_of_lt_of_le hb (Nat.min_le_right _ _)
          simpa [a, Nat.add_mul, Nat.one_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using hylt
        omega
      · intro h
        constructor
        · have hay : x / w * w ≤ y := by
            rw [← h]
            exact Nat.div_mul_le_self y w
          simpa [a] using hay
        · have hylt : y < y / w * w + w := Nat.lt_div_mul_add hw
          rw [h] at hylt
          exact Nat.lt_min.mpr ⟨hyu, by simpa [a] using hylt⟩
    have htest : search.test t x y = true ↔ y / w = x / w := by
      constructor
      · intro ht
        simp [search, w, beq_iff_eq] at ht
        exact ht.symm
      · intro h
        have h' : x / w = y / w := h.symm
        simp [search, w, beq_iff_eq, h']
    by_cases h1 : a ≤ y
    · by_cases h2 : y < b
      · have heq : y / w = x / w := hiff.mp ⟨h1, h2⟩
        have ht : search.test t x y = true := htest.mpr heq
        simp [h1, h2, ht]
      · have heq : y / w ≠ x / w := by
          intro h
          exact h2 (hiff.mpr h).2
        have ht : search.test t x y = false := by
          cases h : search.test t x y <;> simp_all
        simp [h1, h2, heq, ht]
    · have heq : y / w ≠ x / w := by
        intro h
        exact h1 (hiff.mpr h).1
      have ht : search.test t x y = false := by
        cases h : search.test t x y <;> simp_all
      simp [h1, heq, ht]
  · have hfan : fan search u t x ys = 0 := by
      simp [fan, hspan, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    rw [hfan, List.countP_eq_length_filter]
    symm
    apply List.length_eq_zero_iff.mpr
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    have hyu : y < u := hv.2 y hy
    have hba : b ≤ a := Nat.le_of_not_gt hab
    have hu_le_a : u ≤ a := by
      by_cases huw : u ≤ a + w
      · have hb_eq : b = u := by simp [b, Nat.min_eq_left huw]
        omega
      · have haw_le_u : a + w ≤ u := by omega
        have hb_eq : b = a + w := by simp [b, Nat.min_eq_right haw_le_u]
        omega
    have hya : y < a := Nat.lt_of_lt_of_le hyu hu_le_a
    have hneq : y / w ≠ x / w := by
      intro heq
      have hay : x / w * w ≤ y := by
        rw [← heq]
        exact Nat.div_mul_le_self y w
      have : a ≤ y := by simpa [a] using hay
      omega
    have hneq' : x / w ≠ y / w := by intro h; exact hneq h.symm
    simpa [search, w, beq_iff_eq] using hneq'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_OffsetCount
