import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Annulus.OffsetCount` — Two lower-bound offsets per interval compute the exact join
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_OffsetCount

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem member_optional_iff {a b y : Nat} :
    member (optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, member]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, member]
    omega

private theorem member_append (r₁ r₂ : Ranges) (y : Nat) :
    member (r₁ ++ r₂) y = (member r₁ y || member r₂ y) := by
  simp [member, List.any_append]

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
    have h2 : (2 : Nat) ^ 0 = 1 := by simp
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

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

/-! The lower bound is exactly the number of elements strictly below the key. -/
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

private theorem spans_spec (u t x y : Nat) (hyu : y < u) :
    member (search.spans u t x) y ↔ search.test t x y = true := by
  let A := x - (2 * t + 1)
  let B := min u (x - t)
  let C := x + t + 1
  let D := min u (x + 2 * t + 2)
  have hspans : search.spans u t x = optional A B ++ optional C D := by rfl
  have hmem1 : member (optional A B) y = decide (A ≤ y && y < B) := by
    rw [Bool.eq_iff_iff]
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    exact member_optional_iff
  have hmem2 : member (optional C D) y = decide (C ≤ y && y < D) := by
    rw [Bool.eq_iff_iff]
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    exact member_optional_iff
  have hmem : member (search.spans u t x) y =
      (decide (A ≤ y && y < B) || decide (C ≤ y && y < D)) := by
    rw [hspans, member_append, hmem1, hmem2]
  rw [hmem]
  simp only [Bool.or_eq_true, decide_eq_true_eq, Bool.and_eq_true]
  simp only [search, Bool.and_eq_true, decide_eq_true_eq]
  change ((A ≤ y ∧ y < B) ∨ (C ≤ y ∧ y < D)) ↔
    (t < x - y + (y - x) ∧ x - y + (y - x) ≤ 2 * t + 1)
  constructor
  · intro h
    rcases h with ⟨hA, hB⟩ | ⟨hC, hD⟩
    · have hyx : y ≤ x := by
        have hyxt : y < x - t := Nat.lt_of_lt_of_le hB (Nat.min_le_right _ _)
        omega
      have hdy : x - y + (y - x) = x - y := by omega
      rw [hdy]
      constructor <;> omega
    · have hxy : x < y := by omega
      have hdy : x - y + (y - x) = y - x := by omega
      rw [hdy]
      constructor <;> omega
  · intro ht
    rcases ht with ⟨hgt, hle⟩
    by_cases hyx : y ≤ x
    · left
      have hdy : x - y + (y - x) = x - y := by omega
      rw [hdy] at hgt hle
      constructor
      · omega
      · exact (Nat.lt_min).mpr ⟨by omega, by omega⟩
    · right
      have hxy : x < y := by omega
      have hdy : x - y + (y - x) = y - x := by omega
      rw [hdy] at hgt hle
      constructor
      · omega
      · exact (Nat.lt_min).mpr ⟨hyu, by omega⟩

private theorem optional_sum {a b : Nat} (f : Nat × Nat → Nat) :
    ((optional a b).map f).sum = if a < b then f (a, b) else 0 := by
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

theorem sound_OffsetCount : Annulus.OffsetCount.Goal := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  let A := x - (2 * t + 1)
  let B := min u (x - t)
  let C := x + t + 1
  let D := min u (x + 2 * t + 2)
  have hspans : search.spans u t x = optional A B ++ optional C D := by rfl
  have hAB : ((optional A B).map (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
      ys.countP (fun y => decide (A ≤ y && y < B)) := by
    rw [optional_sum]
    by_cases hlt : A < B
    · rw [if_pos hlt]
      simpa using (lower_interval_count (a := A) (b := B) hs (Nat.le_of_lt hlt))
    · rw [if_neg hlt]
      symm
      apply List.countP_eq_zero.mpr
      intro y hy
      simpa using (show ¬ (A ≤ y ∧ y < B) by omega)
  have hCD : ((optional C D).map (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
      ys.countP (fun y => decide (C ≤ y && y < D)) := by
    rw [optional_sum]
    by_cases hlt : C < D
    · rw [if_pos hlt]
      simpa using (lower_interval_count (a := C) (b := D) hs (Nat.le_of_lt hlt))
    · rw [if_neg hlt]
      symm
      apply List.countP_eq_zero.mpr
      intro y hy
      simpa using (show ¬ (C ≤ y ∧ y < D) by omega)
  unfold fan
  rw [hspans, List.map_append, List.sum_append, hAB, hCD]
  have hpred : ys.countP (search.test t x) =
      ys.countP (fun y => member (search.spans u t x) y) := by
    apply List.countP_congr
    intro y hy
    have h := spans_spec u t x y (valid_lt hv hy)
    cases hm : member (search.spans u t x) y <;> cases ht : search.test t x y <;> simp_all
  rw [hpred, hspans]
  have h1 : ys.countP (fun y => member (optional A B) y) =
      ys.countP (fun y => decide (A ≤ y && y < B)) := by
    apply List.countP_congr
    intro y hy
    constructor
    · intro hp
      have hprop : A ≤ y ∧ y < B := member_optional_iff.mp (by simpa using hp)
      simpa [hprop]
    · intro hb
      have hp : member (optional A B) y = true := member_optional_iff.mpr (by simpa using hb)
      simpa [hp]
  have h2 : ys.countP (fun y => member (optional C D) y) =
      ys.countP (fun y => decide (C ≤ y && y < D)) := by
    apply List.countP_congr
    intro y hy
    constructor
    · intro hp
      have hprop : C ≤ y ∧ y < D := member_optional_iff.mp (by simpa using hp)
      simpa [hprop]
    · intro hb
      have hp : member (optional C D) y = true := member_optional_iff.mpr (by simpa using hb)
      simpa [hp]
  rw [← h1, ← h2]
  have hdisj : ∀ y ∈ ys, member (optional A B) y = true →
      member (optional C D) y = false := by
    intro y hy hp
    have hABmem : A ≤ y ∧ y < B := by
      have h := member_optional_iff (a := A) (b := B) (y := y)
      exact h.mp (by simpa using hp)
    have hBC : B ≤ C := by
      dsimp [B, C]
      omega
    have hnot : ¬ (C ≤ y ∧ y < D) := by omega
    cases hm : member (optional C D) y
    · rfl
    · have hc : C ≤ y ∧ y < D := (member_optional_iff.mp (by simpa using hm))
      omega
  have hsplit := countP_or_of_disjoint (fun y => member (optional A B) y)
    (fun y => member (optional C D) y) ys hdisj
  rw [hsplit]
  apply List.countP_congr
  intro y hy
  rw [member_append]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_OffsetCount
