import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Complete certificates for the 10 posting-join families.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.PostingJoins

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

private theorem optional_eq (a b : Nat) :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = if a < b then [(a, b)] else [] := rfl

private theorem optional_none {a b : Nat} (h : ¬ a < b) : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = [] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_some {a b : Nat} (h : a < b) : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = [(a, b)] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem member_optional_iff {a b y : Nat} :
    member (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, member]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, member]; omega

private theorem member_optional {a b y : Nat} :
    member (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b) y ↔ a ≤ y ∧ y < b :=
  member_optional_iff

private theorem member_append (r₁ r₂ : Ranges) (y : Nat) :
    member (r₁ ++ r₂) y = (member r₁ y || member r₂ y) := by
  simp [member, List.any_append]

private theorem optional_ordered (a b : Nat) :
    (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b).Pairwise (fun p q => p.2 ≤ q.1) ∧ ∀ p ∈ TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b, p.1 ≤ p.2 := by
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, Nat.le_of_lt h]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_append_ordered (a b c d u : Nat) (hbc : b ≤ c) :
    (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a (min u b) ++ TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional c (min u d)).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a (min u b) ++ TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional c (min u d), p.1 ≤ p.2 := by
  refine ⟨?_, ?_⟩
  · by_cases h₁ : a < min u b <;> by_cases h₂ : c < min u d <;>
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h₁, h₂, List.pairwise_cons] <;> omega
  · intro p hp
    simp only [List.mem_append] at hp
    rcases hp with hp | hp
    · by_cases h₁ : a < min u b <;> simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h₁] at hp <;> (rcases hp with rfl; omega)
    · by_cases h₂ : c < min u d <;> simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h₂] at hp <;> (rcases hp with rfl; omega)

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

private theorem filter_prefix {α : Type} (p : α → Bool) (xs : List α) (i : Nat)
    (hi : i < (xs.filter p).length) :
    ∃ h : i < xs.length, p xs[i] = true := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
    by_cases hp : p x = true
    · simp only [List.filter_cons, hp, ite_true, List.length_cons] at hi
      cases i with
      | zero => exact ⟨Nat.zero_lt_succ _, by simpa using hp⟩
      | succ j =>
        have hj : j < (xs.filter p).length := by omega
        obtain ⟨hjlen, hjp⟩ := ih j hj
        refine ⟨Nat.succ_lt_succ hjlen, ?_⟩
        simpa [List.getElem_cons_succ] using hjp
    · simp only [List.filter_cons, hp, Bool.false_eq_true, ite_false] at hi
      obtain ⟨hilen, hip⟩ := ih i hi
      refine ⟨by simp [List.length_cons]; omega, ?_⟩
      simpa [List.getElem_cons_succ] using hip

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v (Nat.lt_of_lt_of_le hi (Nat.min_le_left _ _))
    rwa [getD_eq_getElem hiys] at this
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

private theorem drop_take_eq_take_drop {α : Type} (xs : List α) (i j : Nat) (hij : i ≤ j) :
    (xs.drop i).take (j - i) = (xs.take j).drop i := by
  induction xs generalizing i j with
  | nil => simp
  | cons x xs ih =>
    cases i with
    | zero => simp
    | succ i =>
      cases j with
      | zero => omega
      | succ j =>
        simp only [List.drop_succ_cons, List.take_succ_cons, Nat.succ_sub_succ]
        exact ih i j (by omega)

private theorem take_lower_eq_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ys.take (lower ys v) = ys.filter (fun x => decide (x < v)) := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have h1 : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v (Nat.lt_of_lt_of_le hi (Nat.min_le_left _ _))
    rwa [getD_eq_getElem hiys] at this
  have h2 : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen; omega
    intro hlt
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    omega
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc ys.take (lower ys v)
      = (ys.take (lower ys v)).filter (fun x => decide (x < v)) := by rw [h1]
    _ = (ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
        (ys.drop (lower ys v)).filter (fun x => decide (x < v)) := by rw [h2, List.append_nil]
    _ = (ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v)) := by rw [List.filter_append]
    _ = ys.filter (fun x => decide (x < v)) := by rw [← hsplit]

private theorem filter_lt_split {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++ ys.filter (fun x => decide (a ≤ x && x < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
    have hs' : xs.Pairwise (· ≤ ·) := hs.tail
    have ih' := ih hs'
    by_cases hxa : x < a
    · have hxb : x < b := by omega
      have hnab : ¬ (a ≤ x ∧ x < b) := by omega
      simp [List.filter_cons, hxa, hxb, hnab, ih']
    · by_cases hxb : x < b
      · have hax : a ≤ x := by omega
        have hnil : xs.filter (fun y => decide (y < a)) = [] := by
          rw [List.filter_eq_nil_iff]; intro y hy
          simp only [decide_eq_true_eq]
          have := List.rel_of_pairwise_cons hs hy; omega
        simp [List.filter_cons, hxa, hxb, hax, hnil, ih']
      · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          rw [List.filter_eq_nil_iff]; intro y hy
          simp only [decide_eq_true_eq]
          have := List.rel_of_pairwise_cons hs hy; omega
        have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
          rw [List.filter_eq_nil_iff]; intro y hy
          simp only [decide_eq_true_eq, Bool.and_eq_true]
          intro h
          rcases h with ⟨hay, hby⟩
          have := List.rel_of_pairwise_cons hs hy; omega
        have hnab : ¬ (a ≤ x ∧ x < b) := by omega
        simp [List.filter_cons, hxa, hxb, hnab, hnil_a, hnil_ab, ih']

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y && y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) hmono]
    rw [take_lower_eq_filter_lt hs b, filter_lt_split hs hab, take_lower_eq_filter_lt hs a]
    have hla := (lower_eq_filter_lt_length hs a).symm
    simp only [hla, List.drop_left]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    rw [List.filter_eq_nil_iff]
    intro y hy
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    omega

private theorem slice_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (slice ys a b).length = lower ys b - lower ys a := by
  unfold slice
  rw [List.length_take, List.length_drop]
  have hla := lower_le_length hs a
  have hlb := lower_le_length hs b
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    rw [Nat.min_eq_left (by omega : lower ys b - lower ys a ≤ ys.length - lower ys a)]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz]; simp

/-! ## Ordered spans and generic consequences -/

private def SpansSpec (s : Search) : Prop :=
  ∀ u t x y, y < u → (member (s.spans u t x) y ↔ s.test t x y = true)

private def SpansOrdered (s : Search) : Prop :=
  ∀ u t x, (s.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ s.spans u t x, p.1 ≤ p.2

private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (decide (a ≤ y && y < b) || member rs y) := by
  simp [member]

private theorem filter_or_append {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    xs.filter (fun x => p x || q x) = xs.filter p ++ xs.filter q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have htail : ∀ y ∈ xs, p y = true → q y = false := fun y hy => hdisj y (List.mem_cons.mpr (Or.inr hy))
    by_cases hp : p x = true
    · have hq := hdisj x List.mem_cons_self hp
      simp [hp, hq, ih htail]
    · by_cases hq : q x = true
      · simp [hp, hq, ih htail]
      · simp [hp, hq, ih htail]

private theorem filter_member_cons_of_ordered {ys : List Nat} (_hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) (_hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => decide (a ≤ y && y < b)) ++ ys.filter (fun y => member rs y) := by
  have hdisj : ∀ y ∈ ys, (decide (a ≤ y && y < b)) = true → member rs y = false := by
    intro y _ hp hm
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hp
    rw [List.any_eq_true] at hm
    rcases hm with ⟨p, hpm, hpred⟩
    have hbp := hb p hpm
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
    omega
  rw [show (fun y => member ((a, b) :: rs) y) = (fun y => decide (a ≤ y && y < b) || member rs y) by
    funext y; simp [member]]
  exact filter_or_append _ _ _ hdisj

private theorem filter_member_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    ys.filter (fun y => member rs y) =
      rs.flatMap (fun p => ys.filter (fun y => decide (p.1 ≤ y && y < p.2))) := by
  induction rs with
  | nil => simp [member]
  | cons p rs ih =>
    obtain ⟨a, b⟩ := p
    simp only [List.flatMap_cons]
    have hb : ∀ q ∈ rs, b ≤ q.1 := fun q hq => (List.pairwise_cons.mp hrs).1 q hq
    have hrs' := (List.pairwise_cons.mp hrs).2
    have hrs_wf' : ∀ q ∈ rs, q.1 ≤ q.2 := fun q hq => hrs_wf q (List.mem_cons.mpr (Or.inr hq))
    rw [filter_member_cons_of_ordered hs hb hrs', ih hrs' hrs_wf']

private theorem seek_eq_filter_member {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    rs.flatMap (fun p => slice ys p.1 p.2) = ys.filter (fun y => member rs y) := by
  rw [filter_member_eq_flatMap hs hrs hrs_wf]
  congr 1
  funext p
  obtain ⟨a, b⟩ := p
  simp only
  exact slice_eq_filter hs a b

private theorem orderedProbe_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : orderedProbe s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  have ⟨hrs, hwf⟩ := hord u t x
  unfold seek
  have hseek : (s.spans u t x).flatMap (fun (a, b) => slice ys a b) =
      (s.spans u t x).flatMap (fun p => slice ys p.1 p.2) := by
    congr 1; funext p; cases p; rfl
  rw [hseek, seek_eq_filter_member hs hrs hwf]
  apply List.filter_congr
  intro y hy
  have hlt := valid_lt hv hy
  have h := hspec u t x y hlt
  cases hm : member (s.spans u t x) y <;> cases ht : s.test t x y <;> simp_all

private theorem fan_eq_seek_length {s : Search} {u t x : Nat} {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    fan s u t x ys = (seek s u t x ys).length := by
  unfold fan seek
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro p hp
  obtain ⟨a, b⟩ := p
  simp [slice_length hs]

private theorem offsetCount_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : offsetCount s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  rw [fan_eq_seek_length hs, orderedProbe_of_spec hspec hord u t x ys hv]
  simp [List.countP_eq_length_filter]

private theorem sum_flatMap_nat {α : Type} (xs : List α) (f : α → List Nat) :
    (xs.flatMap f).sum = (xs.map (fun x => (f x).sum)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.sum_append, ih]

private theorem take_sum_split (ys : List Nat) (p q : Nat) (hpq : p ≤ q) :
    (ys.take q).sum = (ys.take p).sum + ((ys.drop p).take (q - p)).sum := by
  have hsplit : ys.take q = ys.take p ++ (ys.drop p).take (q - p) := by
    rw [drop_take_eq_take_drop ys p q hpq]
    have h := (List.take_append_drop p (ys.take q)).symm
    simpa [List.take_take, Nat.min_eq_left hpq] using h
  rw [hsplit, List.sum_append]

private theorem slice_sum {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (slice ys a b).sum = (ys.take (lower ys b)).sum - (ys.take (lower ys a)).sum := by
  unfold slice
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) hmono]
    have hsplit := take_sum_split ys (lower ys a) (lower ys b) hmono
    omega
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero, List.sum_nil]
    have hle := Nat.sub_le_sub_right (Nat.le_of_le_of_le (List.sum_take_le_sum_take (lower ys b) (lower ys a) ys) (Nat.le_refl _)) 0
    have hsplit := take_sum_split ys (lower ys b) (lower ys a) hmono
    omega

private theorem total_eq_seek_sum {s : Search} {u t x : Nat} {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    total s u t x ys = (seek s u t x ys).sum := by
  unfold total seek
  rw [sum_flatMap_nat]
  congr 1
  apply List.map_congr_left
  intro p hp
  obtain ⟨a, b⟩ := p
  simp [sumAt, slice_sum hs]

private theorem integralSum_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : integralSum s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  rw [total_eq_seek_sum hs, orderedProbe_of_spec hspec hord u t x ys hv]

private theorem leftMultiplicity_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : leftMultiplicity s := by
  intro u t xs ys hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none => simp [Option.any, List.filter]
  | some v =>
    simp only [Option.any, Option.map_some, Option.getD_some]
    set rhs := (ys.filterMap id).filter (s.test t v) with hrhs
    by_cases h : rhs.isEmpty
    · have hlen : rhs.length = 0 := by simpa [List.isEmpty_iff] using h
      rw [if_pos h]
      have hcount := offsetCount_of_spec hspec hord u t v (ys.filterMap id) hv
      rw [← List.countP_eq_length_filter] at hcount
      simp [hcount, hlen]
    · rw [if_neg h]
      have hlen : 0 < rhs.length := by
        cases hlen' : rhs.length with
        | zero => simp [List.isEmpty_iff, hlen'] at h
        | succ n => omega
      have hcount := offsetCount_of_spec hspec hord u t v (ys.filterMap id) hv
      rw [← List.countP_eq_length_filter] at hcount
      have hmax : max 1 rhs.length = rhs.length := Nat.max_eq_right (by omega)
      simp [hcount, hmax]

private theorem eraseDups_nodup {α : Type} [DecidableEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  induction xs with
  | nil => simp [List.eraseDups]
  | cons a xs ih =>
    simp only [List.eraseDups_cons]
    by_cases ha : a ∈ xs
    · simp [ha, ih]
    · simp [ha, ih, List.Nodup.cons]

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_zero_map {α : Type} (xs : List α) (f : α → Nat)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp [h x (List.mem_cons_self), ih (fun y hy => h y (List.mem_cons.mpr (Or.inr hy)))]

private theorem sum_indicator_nodup {α : Type} [DecidableEq α] (xs : List α) (a : α) (v : Nat)
    (hnd : xs.Nodup) (ha : a ∈ xs) :
    (xs.map (fun x => if x = a then v else 0)).sum = v := by
  induction xs with
  | nil => cases ha
  | cons b bs ih =>
    simp only [List.map_cons, List.sum_cons]
    rcases List.mem_cons.mp ha with rfl | ha
    · have hnot : a ∉ bs := (List.nodup_cons.mp hnd).1
      have hzero : (bs.map (fun x => if x = a then v else 0)).sum = 0 := by
        apply sum_zero_map; intro x hx; simp [show x ≠ a from fun h => hnot (h ▸ hx)]
      simp [hzero]
    · have hne : b ≠ a := by
        intro heq; subst heq; exact (List.nodup_cons.mp hnd).1 ha
      simp [hne, ih (List.nodup_cons.mp hnd).2 ha]

private theorem sum_beq_indicator_nodup {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (a : α) (v : Nat) (hnd : xs.Nodup) (ha : a ∈ xs) :
    (xs.map (fun x => if x == a then v else 0)).sum = v := by
  have h := sum_indicator_nodup xs a v hnd ha
  have heq : (xs.map (fun x => if x == a then v else 0)) =
             (xs.map (fun x => if x = a then v else 0)) := by
    apply List.map_congr_left; intro x _; simp [beq_iff_eq]
  rw [heq]; exact h

private theorem count_cons_add {α : Type} [DecidableEq α] [LawfulBEq α]
    (a : α) (xs : List α) :
    (xs.map (fun x => List.count x (a :: xs))).sum =
      (xs.map (fun x => List.count x xs)).sum + xs.length := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
    simp only [List.map_cons, List.count_cons_self, List.sum_cons, List.length_cons]
    have step : (∀ x, List.count x (b :: bs) = (if x == b then 1 else 0) + List.count x bs) := by
      intro x; simp [List.count_cons]; split <;> omega
    simp [step, sum_map_add_nat, sum_beq_indicator_nodup, ih]
    omega

private theorem sum_map_eraseDups_count_mul {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => List.count x xs * f x)).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp [List.eraseDups]
  | cons a xs ih =>
    by_cases ha : a ∈ xs
    · rw [List.eraseDups_cons_of_mem ha]
      have hstep : ((xs.eraseDups.map (fun x => List.count x (a :: xs) * f x)).sum) =
          ((xs.eraseDups.map (fun x => List.count x xs * f x)).sum) + f a := by
        rw [show xs.eraseDups.map (fun x => List.count x (a :: xs) * f x) =
            xs.eraseDups.map (fun x => (List.count x xs + if x == a then 1 else 0) * f x) from by
          apply List.map_congr_left; intro x _; simp [List.count_cons]]
        rw [show (xs.eraseDups.map (fun x => (List.count x xs + if x == a then 1 else 0) * f x)) =
            (xs.eraseDups.map (fun x => List.count x xs * f x + (if x == a then 1 else 0) * f x)) from by
          apply List.map_congr_left; intro x _; omega]
        rw [sum_map_add_nat]
        congr 1
        have hnd := eraseDups_nodup xs
        have ha' : a ∈ xs.eraseDups := List.mem_eraseDups.mpr ha
        have hind := sum_beq_indicator_nodup xs.eraseDups a (f a) hnd ha'
        calc (xs.eraseDups.map (fun x => (if x == a then 1 else 0) * f a)).sum
            = (xs.eraseDups.map (fun x => if x == a then f a else 0)).sum := by
              apply congrArg List.sum; apply List.map_congr_left; intro x _; split <;> simp
          _ = f a := hind
      simp [hstep, ih, List.map_cons, List.sum_cons, List.count_cons_self]
      omega
    · rw [List.eraseDups_cons_of_not_mem ha]
      simp only [List.map_cons, List.sum_cons]
      have h0 : (if a == a then 1 else 0) = 1 := by simp
      have hcount_a : List.count a xs = 0 := by
        rw [List.count_eq_zero]; exact fun h => ha h
      rw [hcount_a, h0]
      have hstep : (xs.eraseDups.map (fun x => List.count x (a :: xs) * f x)).sum =
          (xs.eraseDups.map (fun x => List.count x xs * f x)).sum := by
        apply congrArg List.sum; apply List.map_congr_left; intro x hx
        have hne : x ≠ a := by
          intro heq; subst heq; exact ha (List.mem_eraseDups.mp hx)
        simp [List.count_cons, hne]
      rw [hstep, ih]
      simp [List.map_cons, List.sum_cons]

private theorem repeatedProbe_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : repeatedProbe s := by
  intro u t xs ys hv
  have hs := valid_pairwise hv
  have hfan : ∀ x, fan s u t x ys = (ys.filter (s.test t x)).length := by
    intro x; rw [offsetCount_of_spec hspec hord u t x ys hv, List.countP_eq_length_filter]
  -- (∑ x in xs, fan) = ∑_{x in xs.eraseDups} count x xs * fan = ...
  -- use sum_map_eraseDups_count_mul
  -- repeatedProbe: (xs.flatMap (fun x => seek s u t x ys)).length = (xs.eraseDups.map (fun x => List.count x xs * fan s u t x ys)).sum
  rw [show (xs.flatMap (fun x => seek s u t x ys)).length =
        (xs.map (fun x => (seek s u t x ys).length)).sum from by
      rw [List.length_flatMap]; simp [List.map_map]]
  have hseek_len : ∀ x, (seek s u t x ys).length = fan s u t x ys := by
    intro x; rw [← fan_eq_seek_length hs]
  simp_rw [hseek_len]
  have hkey : (xs.map (fun x => fan s u t x ys)).sum =
      (xs.eraseDups.map (fun x => List.count x xs * fan s u t x ys)).sum := by
    have := sum_map_eraseDups_count_mul xs (fun x => fan s u t x ys)
    symm; exact this
  exact hkey

private theorem nth_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) (k : Nat) :
    nth ys rs k = (rs.flatMap (fun p => slice ys p.1 p.2))[k]? := by
  induction rs generalizing k with
  | nil => simp [nth]
  | cons p rs ih =>
    obtain ⟨a, b⟩ := p
    simp only [nth, List.flatMap_cons]
    have hsl := slice_length hs a b
    by_cases hk : k < lower ys b - lower ys a
    · rw [if_pos hk]
      have hleft : (slice ys a b ++ rs.flatMap (fun p => slice ys p.1 p.2))[k]? = (slice ys a b)[k]? := by
        rw [List.getElem?_append_left (by rwa [hsl])]
      rw [hleft]
      -- nth in slice = ys[lower ys a + k]?
      unfold slice
      rw [List.getElem?_take (by simp [List.length_drop]; omega)]
      rw [List.getElem?_drop]
    · rw [if_neg hk]
      have hk2 : k - (slice ys a b).length = k - (lower ys b - lower ys a) := by rw [hsl]
      rw [hk2]
      have hge : (slice ys a b).length ≤ k := by rw [hsl]; omega
      rw [List.getElem?_append_right hge]
      have hk3 : k - (slice ys a b).length = k - (lower ys b - lower ys a) := by rw [hsl]
      rw [hk3]
      exact ih (List.pairwise_cons.mp hrs).2 (k - (lower ys b - lower ys a))

private theorem kthMatch_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : kthMatch s := by
  intro u t x ys k hv
  have hs := valid_pairwise hv
  have ⟨hrs, hwf⟩ := hord u t x
  rw [nth_eq_flatMap hs hrs, seek_eq_filter_member hs hrs hwf]

private theorem pruneExtent_of_spec {s : Search} (hspec : SpansSpec s) : pruneExtent s := by
  intro u t x ys lo hi hv hys hspan
  unfold seek
  rw [List.flatMap_eq_nil_iff]
  intro p hp
  obtain ⟨a, b⟩ := p
  rw [List.eq_nil_iff_forall_not_mem]
  intro y hy
  -- y is in slice ys a b, so y is in ys and a <= y < b
  have hyys : y ∈ ys := by
    unfold slice at hy
    have h1 := List.mem_of_mem_take hy
    exact List.mem_of_mem_drop h1
  have hyu := valid_lt hv hyys
  have ⟨hlo_y, hhi_y⟩ := hys y hyys
  have hmem : member (s.spans u t x) y = false := by
    rw [Bool.eq_false_iff, ne_eq, List.any_eq_true]
    push_neg
    intro q hq hq1
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hq1
    have := hspan q hq
    omega
  have htest : s.test t x y = false := by
    cases ht : s.test t x y with
    | false => rfl
    | true =>
      exfalso
      have hm := (hspec u t x y hyu).mpr ht
      rw [hmem] at hm
      simp at hm
  -- but y came from slice ys a b which means member holds for (a,b). Contradiction.
  have hmem_ab : member (s.spans u t x) y = true := by
    unfold member
    rw [List.any_eq_true]
    refine ⟨(a,b), hp, ?_⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    -- y in slice ys a b means lower ys a <= index < lower ys b, and y = ys[idx]
    unfold slice at hy
    have hy_take := List.mem_of_mem_take hy
    have hy_drop := List.mem_drop_iff_getElem.mp hy_take
    rcases hy_drop with ⟨i, hi, rfl⟩
    have hiys : lower ys a + i < ys.length := by
      have := hi; rw [List.length_take, List.length_drop] at this; omega
    rw [List.getElem_take, List.getElem_drop]
    have hge_a : a ≤ ys[lower ys a + i] := by
      have hle := lower_le_of_ge (valid_pairwise hv) a (Nat.le_add_right _ _) hiys
      rwa [getD_eq_getElem hiys] at hle
    have hlt_b : ys[lower ys a + i] < b := by
      have hlt := lower_lt_of_lt (valid_pairwise hv) b ?_
      rwa [getD_eq_getElem hiys] at hlt
      -- lower ys a + i < lower ys b
      have htake_bound : i < lower ys b - lower ys a := by
        have := hi; rw [List.length_take, List.length_drop] at this; omega
      omega
    exact ⟨hge_a, hlt_b⟩
  rw [hmem] at hmem_ab
  cases hmem_ab

namespace Following
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.search
  simp only [member_optional_iff]
  constructor
  · intro ⟨h1, h2⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq]; omega
  · intro ht
    simp only [Bool.and_eq_true, decide_eq_true_eq] at ht
    omega

private theorem ordered : SpansOrdered search := by
  intro u t x
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.search
  exact optional_ordered _ _

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_ZeroLookahead : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.ZeroLookahead.Goal := by
  intro u x
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.search
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional]

theorem sound_SlidingOffsets : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.SlidingOffsets.Goal := by
  intro u t x ys hv
  have h1 := offsetCount_of_spec spec ordered u t (x+1) ys hv
  have h2 := offsetCount_of_spec spec ordered u t x ys hv
  rw [h1, h2]
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.search
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter, List.count_eq_length_filter]
  -- partition: (x < y && y <= x+1+t) splits into (x+1 < y && y <= x+1+t) + (y == x+1)
  -- and into (x < y && y <= x+t) + (y == x+t+1)
  have hpart1 : (ys.filter (fun y => decide (x < y) && decide (y ≤ x + 1 + t))).length =
      (ys.filter (fun y => decide (x + 1 < y) && decide (y ≤ x + 1 + t))).length +
      (ys.filter (fun y => decide (y == x + 1))).length := by
    have hsplit : ∀ y, (decide (x < y) && decide (y ≤ x + 1 + t)) =
        ((decide (x + 1 < y) && decide (y ≤ x + 1 + t)) || (decide (y == x + 1))) := by
      intro y; by_cases h1 : y = x + 1 <;> by_cases h2 : x + 1 < y <;>
        by_cases h3 : y ≤ x + 1 + t <;> by_cases h4 : x < y <;> simp_all <;> omega
    have hdisj : (ys.filter (fun y => (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) && (decide (y == x + 1)))).length = 0 := by
      rw [List.length_eq_zero, List.filter_eq_nil_iff]
      intro y _; by_cases h1 : y = x + 1 <;> simp [h1]
    rw [show (ys.filter (fun y => decide (x < y) && decide (y ≤ x + 1 + t))).length =
        (ys.filter (fun y => (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) || (decide (y == x + 1)))).length from by
      congr 1; apply List.filter_congr; intro y _; exact hsplit y]
    rw [List.length_filter_add_length_filter_not (l := ys.filter (fun y => (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) || (decide (y == x + 1)))) (p := fun y => decide (y == x + 1))]
    -- simplify
    have ha : (List.filter (fun y => (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) || decide (y == x + 1)) ys).filter (fun y => decide (y == x + 1)) =
        ys.filter (fun y => decide (y == x + 1)) := by
      rw [List.filter_filter]; apply List.filter_congr; intro y _
      by_cases h1 : y = x + 1 <;> simp [h1]
    have hb : (List.filter (fun y => (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) || decide (y == x + 1)) ys).filter (fun y => !decide (y == x + 1)) =
        ys.filter (fun y => decide (x + 1 < y) && decide (y ≤ x + 1 + t)) := by
      rw [List.filter_filter]; apply List.filter_congr; intro y _
      by_cases h1 : y = x + 1 <;> by_cases h2 : x + 1 < y <;> by_cases h3 : y ≤ x + 1 + t <;> simp [h1, h2, h3]
    rw [ha, hb]; omega
  have hpart2 : (ys.filter (fun y => decide (x < y) && decide (y ≤ x + 1 + t))).length =
      (ys.filter (fun y => decide (x < y) && decide (y ≤ x + t))).length +
      (ys.filter (fun y => decide (y == x + t + 1))).length := by
    have hsplit : ∀ y, (decide (x < y) && decide (y ≤ x + 1 + t)) =
        ((decide (x < y) && decide (y ≤ x + t)) || (decide (y == x + t + 1))) := by
      intro y; by_cases h1 : y = x + t + 1 <;> by_cases h2 : y ≤ x + t <;>
        by_cases h3 : x < y <;> by_cases h4 : y ≤ x + 1 + t <;> simp_all <;> omega
    rw [show (ys.filter (fun y => decide (x < y) && decide (y ≤ x + 1 + t))).length =
        (ys.filter (fun y => (decide (x < y) && decide (y ≤ x + t)) || (decide (y == x + t + 1)))).length from by
      congr 1; apply List.filter_congr; intro y _; exact hsplit y]
    rw [List.length_filter_add_length_filter_not (l := ys.filter (fun y => (decide (x < y) && decide (y ≤ x + t)) || (decide (y == x + t + 1)))) (p := fun y => decide (y == x + t + 1))]
    have ha : (List.filter (fun y => (decide (x < y) && decide (y ≤ x + t)) || decide (y == x + t + 1)) ys).filter (fun y => decide (y == x + t + 1)) =
        ys.filter (fun y => decide (y == x + t + 1)) := by
      rw [List.filter_filter]; apply List.filter_congr; intro y _
      by_cases h1 : y = x + t + 1 <;> simp [h1]
    have hb : (List.filter (fun y => (decide (x < y) && decide (y ≤ x + t)) || decide (y == x + t + 1)) ys).filter (fun y => !decide (y == x + t + 1)) =
        ys.filter (fun y => decide (x < y) && decide (y ≤ x + t)) := by
      rw [List.filter_filter]; apply List.filter_congr; intro y _
      by_cases h1 : y = x + t + 1 <;> by_cases h2 : y ≤ x + t <;> by_cases h3 : x < y <;> simp [h1, h2, h3]; omega
    rw [ha, hb]; omega
  omega

end Following

/-! ## Annulus -/
namespace Annulus
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.search, member_append, member_optional_iff]
  constructor
  · intro h
    rcases h with ⟨h1, h2⟩ | ⟨h1, h2⟩
    · have hdy : x - y + (y - x) = x - y := by omega
      simp only [Bool.and_eq_true, decide_eq_true_eq, hdy]; omega
    · have hdy : x - y + (y - x) = y - x := by omega
      simp only [Bool.and_eq_true, decide_eq_true_eq, hdy]; omega
  · intro ht
    simp only [Bool.and_eq_true, decide_eq_true_eq] at ht
    rcases ht with ⟨hgt, hle⟩
    by_cases hxy : y ≤ x
    · left; have hdy : x - y + (y - x) = x - y := by omega; rw [hdy] at hgt hle; omega
    · right; have hxy' : x < y := by omega; have hdy : x - y + (y - x) = y - x := by omega; rw [hdy] at hgt hle; omega

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  exact optional_append_ordered (x - (2*t+1)) (x-t) (x+t+1) (x+2*t+2) u (by omega)

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_TwoSlices : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.TwoSlices.Goal := by
  intro u t x
  simp only [search]
  by_cases h1 : x - (2 * t + 1) < min u (x - t) <;>
    by_cases h2 : x + t + 1 < min u (x + 2 * t + 2) <;> simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h1, h2]

theorem sound_SubtractInnerBand : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.SubtractInnerBand.Goal := by
  intro u t x ys hv
  have hfan := offsetCount_of_spec spec ordered u t x ys hv
  rw [hfan]
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.search, Bool.and_eq_true, decide_eq_true_eq]
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter, List.countP_eq_length_filter]
  have hsplit : ∀ y, (decide (x ≤ y + (2 * t + 1)) && decide (y ≤ x + (2 * t + 1))) =
      ((decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) ||
       (decide (x ≤ y + t) && decide (y ≤ x + t))) := by
    intro y
    by_cases hxy : y ≤ x
    · have hd : x - y + (y - x) = x - y := by omega
      rw [hd]; by_cases hd1 : x - y ≤ t <;> by_cases hd2 : x - y ≤ 2 * t + 1 <;> simp_all <;> omega
    · have hxy' : x < y := by omega
      have hd : x - y + (y - x) = y - x := by omega
      rw [hd]; by_cases hd1 : y - x ≤ t <;> by_cases hd2 : y - x ≤ 2 * t + 1 <;> simp_all <;> omega
  have hdisj : (ys.filter (fun y => (decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) &&
      (decide (x ≤ y + t) && decide (y ≤ x + t)))).length = 0 := by
    rw [List.length_eq_zero, List.filter_eq_nil_iff]
    intro y _
    by_cases hxy : y ≤ x
    · have hd : x - y + (y - x) = x - y := by omega
      simp only [hd, Bool.and_eq_true, decide_eq_true_eq, not_and]; intro h1 _ h3 _; omega
    · have hxy' : x < y := by omega
      have hd : x - y + (y - x) = y - x := by omega
      simp only [hd, Bool.and_eq_true, decide_eq_true_eq, not_and]; intro h1 _ h3 _; omega
  rw [show (ys.filter (fun y => decide (x ≤ y + (2 * t + 1)) && decide (y ≤ x + (2 * t + 1)))).length =
      (ys.filter (fun y => (decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) ||
       (decide (x ≤ y + t) && decide (y ≤ x + t)))).length from by
    congr 1; apply List.filter_congr; intro y _; exact hsplit y]
  rw [List.length_filter_add_length_filter_not (l := ys.filter (fun y => (decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) || (decide (x ≤ y + t) && decide (y ≤ x + t)))) (p := fun y => (decide (x ≤ y + t) && decide (y ≤ x + t)))]
  have ha : (List.filter (fun y => (decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) || (decide (x ≤ y + t) && decide (y ≤ x + t))) ys).filter (fun y => (decide (x ≤ y + t) && decide (y ≤ x + t))) =
      ys.filter (fun y => decide (x ≤ y + t) && decide (y ≤ x + t)) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _
    by_cases h : (decide (x ≤ y + t) && decide (y ≤ x + t)) <;> simp [h]
  have hb : (List.filter (fun y => (decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) || (decide (x ≤ y + t) && decide (y ≤ x + t))) ys).filter (fun y => !(decide (x ≤ y + t) && decide (y ≤ x + t))) =
      ys.filter (fun y => decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _
    by_cases h1 : (decide (t < x - y + (y - x)) && decide (x - y + (y - x) ≤ 2 * t + 1)) <;>
      by_cases h2 : (decide (x ≤ y + t) && decide (y ≤ x + t)) <;> simp [h1, h2]
  rw [ha, hb]; omega

end Annulus

/-! ## SameBucket -/
namespace SameBucket
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.search, member_optional_iff]
  let w := t + 1
  have hw : 0 < w := by omega
  constructor
  · intro ⟨h1, h2⟩
    have hdiv : y / w = x / w := by
      have hle : x / w ≤ y / w := Nat.div_le_div_right (by omega)
      have hlt : y / w < x / w + 1 := by rw [Nat.div_lt_iff_lt_mul hw]; omega
      omega
    simp [hdiv]
  · intro ht
    simp only [beq_iff_eq] at ht
    have hya : x / w * w ≤ y := by rw [← ht]; exact Nat.div_mul_le_self y w
    have hyb : y < x / w * w + w := by
      have : y < y / w * w + w := Nat.lt_div_mul_add hw
      omega
    exact ⟨hya, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.search
  exact optional_ordered _ _

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_BucketReuse : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.BucketReuse.Goal := by
  intro u t x z h
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.search, h]

theorem sound_HistogramProduct : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.HistogramProduct.Goal := by
  intro u t xs ys hvxs hvys
  simp only [offsetCount_of_spec spec ordered]
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.search, List.countP_eq_length_filter]
  -- rewrite to same bucket form
  have hcomm : ∀ x, (ys.filter (fun y => (x / (t + 1) == y / (t + 1)))).length =
      (ys.filter (fun y => (y / (t + 1) == x / (t + 1)))).length := by
    intro x; apply List.length_filter_congr; intro y _; by_cases h : x/(t+1) = y/(t+1) <;> simp [h, eq_comm]
  simp_rw [hcomm]
  -- prove by induction on xs
  have key : ∀ (xs : List Nat), (∀ x ∈ xs, x < u) →
      (xs.map (fun x => (ys.filter (fun y => y / (t + 1) == x / (t + 1))).length)).sum =
      ((List.range ((u + t) / (t + 1))).map (fun k =>
        (xs.filter (fun x => x / (t + 1) == k)).length * (ys.filter (fun y => y / (t + 1) == k)).length)).sum := by
    intro xs hxs
    induction xs with
    | nil => simp
    | cons x xs ih =>
      have hx : x < u := hxs x List.mem_cons_self
      have hxs' : ∀ z ∈ xs, z < u := fun z hz => hxs z (List.mem_cons.mpr (Or.inr hz))
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      rw [ih hxs']
      have hk_lt : x / (t + 1) < (u + t) / (t + 1) := by
        apply Nat.div_lt_of_lt_mul
        have : x < u := hx
        omega
      -- extract bucket x/(t+1) from the range sum
      have hextract : ((List.range ((u + t) / (t + 1))).map (fun k =>
          ((if x / (t + 1) == k then x :: xs.filter (fun z => z / (t + 1) == k) else xs.filter (fun z => z / (t + 1) == k))).length *
          (ys.filter (fun y => y / (t + 1) == k)).length)).sum =
        (ys.filter (fun y => y / (t + 1) == (x / (t + 1)))).length +
        ((List.range ((u + t) / (t + 1))).map (fun k =>
          (xs.filter (fun z => z / (t + 1) == k)).length * (ys.filter (fun y => y / (t + 1) == k)).length)).sum := by
        rw [← List.sum_map_add]
        congr 1
        apply List.map_congr_left
        intro k hk
        by_cases heq : x / (t + 1) = k
        · subst heq; simp [List.length_cons]; omega
        · have hne : (x / (t + 1) == k) = false := beq_eq_false_iff_ne.mpr heq
          have hne2 : (k == x / (t + 1)) = false := beq_eq_false_iff_ne.mpr (Ne.symm heq)
          simp [hne, hne2]
      linarith [hextract]
  exact key xs (valid_lt hvxs)

end SameBucket

/-! ## AdjacentBucket -/
namespace AdjacentBucket
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.search, member_append, member_optional_iff]
  let w := t + 1
  let a := x / w * w
  have hw : 0 < w := by omega
  constructor
  · intro h
    rcases h with ⟨h1, h2⟩ | ⟨h1, h2⟩
    · have hyb : y / w + 1 = x / w := by
        have hle : y / w < x / w := by rw [Nat.div_lt_iff_lt_mul hw]; omega
        have hge : x / w ≤ y / w + 1 := by
          by_cases hxw : x / w = 0
          · omega
          · have hmul : (x / w - 1) * w ≤ y := by
              have : (x / w - 1) * w ≤ y / w * w := Nat.mul_le_mul_right w (by omega)
              have : y / w * w ≤ y := Nat.div_mul_le_self y w
              have haeq : a - w = (x / w - 1) * w := by
                simp [a]; rw [Nat.sub_mul]; simp; omega
              omega
            omega
        omega
      simp [hyb]
    · have hyb : x / w + 1 = y / w := by
        have hle : x / w < y / w := by rw [Nat.lt_div_iff_mul_lt hw]; omega
        have hge : y / w < x / w + 2 := by rw [Nat.div_lt_iff_lt_mul hw]; omega
        omega
      simp [hyb]
  · intro ht
    simp only [Bool.or_eq_true, beq_iff_eq] at ht
    rcases ht with h | h
    · left
      have hy_lt_a : y < a := by
        have : y / w < x / w := by omega
        rw [Nat.lt_div_iff_mul_lt hw] at this; simpa [a, w] using this
      have hy_ge : a - w ≤ y := by
        by_cases hxw : x / w = 0
        · simp [a, hxw]
        · have h1 : (x / w - 1) * w ≤ y / w * w := Nat.mul_le_mul_right w (by omega)
          have h2 : y / w * w ≤ y := Nat.div_mul_le_self y w
          have haeq : a - w = (x / w - 1) * w := by simp [a]; rw [Nat.sub_mul]; simp; omega
          omega
      exact ⟨by omega, by omega⟩
    · right
      have hy_ge : a + w ≤ y := by
        have : x / w + 1 ≤ y / w := by omega
        have h1 : (x / w + 1) * w ≤ y / w * w := Nat.mul_le_mul_right w this
        have h2 : y / w * w ≤ y := Nat.div_mul_le_self y w
        simp [a] at *; omega
      have hy_lt : y < a + 2 * w := by
        have : y / w ≤ x / w + 1 := by omega
        rw [Nat.div_lt_iff_lt_mul hw]; simp [a] at *; omega
      exact ⟨by omega, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  let w := t + 1
  let a := x / w * w
  have hbc : min u a ≤ a + w := by omega
  exact optional_append_ordered (a - w) a (a + w) (a + 2 * w) u hbc

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_NoSelf : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.NoSelf.Goal := by
  intro t x
  simp [search]

theorem sound_SymmetricJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.AdjacentBucket.SymmetricJoin.Goal := by
  intro u t a b hva hvb
  have hfa := fun x => offsetCount_of_spec spec ordered u t x b hvb
  have hfb := fun x => offsetCount_of_spec spec ordered u t x a hva
  simp only [hfa, hfb, search, List.countP_eq_length_filter]
  -- sum swap
  have hswap : ∀ (xs ys : List Nat) (p : Nat → Nat → Bool),
      (xs.map (fun x => (ys.filter (p x)).length)).sum = (ys.map (fun y => (xs.filter (fun x => p x y)).length)).sum := by
    intro xs ys p
    induction xs with
    | nil => simp
    | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      rw [ih]
      have hadd : (ys.filter (p x)).length + (ys.map (fun y => (xs.filter (fun x => p x y)).length)).sum =
          (ys.map (fun y => ((if p x y then x :: xs.filter (fun x' => p x' y) else xs.filter (fun x' => p x' y))).length)).sum := by
        rw [← List.sum_map_add]
        congr 1; apply List.map_congr_left; intro y _
        by_cases hp : p x y <;> simp [hp] <;> omega
      linarith
  exact hswap a b _
end AdjacentBucket

/-! ## ModuloClass -/
namespace ModuloClass
private theorem mem_mod_spans {u t x y : Nat} (hyu : y < u) :
    member (search.spans u t x) y ↔ (x % (t + 1) == y % (t + 1)) = true := by
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.search, member]
  let w := t + 1
  let r := x % w
  have hw : 0 < w := by omega
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨p, hp, hpred⟩
    simp only [List.mem_map, List.mem_range] at hp
    rcases hp with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
    have hy : y = r + i * w := by omega
    subst hy
    simp [Nat.add_mul_mod_self_right]
  · intro h
    simp only [beq_iff_eq] at h
    have hy_eq : y = r + (y / w) * w := by
      have := Nat.div_add_mod y w; rw [h] at this; omega
    rw [List.any_eq_true]
    refine ⟨(r + (y / w) * w, r + (y / w) * w + 1), ?_, ?_⟩
    · simp only [List.mem_map, List.mem_range]
      refine ⟨y / w, ?_, ?_⟩
      · rw [Nat.div_lt_iff_lt_mul hw]; omega
      · simp
    · simp [decide_eq_true_eq]; omega

private theorem spec : SpansSpec search := by
  intro u t x y hyu; exact mem_mod_spans hyu

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  let w := t + 1
  let r := x % w
  have hw : 0 < w := by omega
  constructor
  · rw [List.pairwise_map]; simp only [Function.comp]
    rw [List.pairwise_range]
    intro i j hij
    simp only
    have : (i + 1) * w ≤ j * w := Nat.mul_le_mul_right w hij
    omega
  · intro p hp
    simp only [List.mem_map, List.mem_range] at hp
    rcases hp with ⟨i, _, rfl⟩; simp

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_ResidueCache : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.ResidueCache.Goal := by
  intro u t x k ys
  simp only [seek, search]
  simp [Nat.add_mul_mod_self_right]

theorem sound_ClassPopulation : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.ClassPopulation.Goal := by
  intro u t x
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass.search, List.length_map, List.length_range]
  let w := t + 1
  let r := x % w
  have hw : 0 < w := by omega
  have hrw : r < w := Nat.mod_lt x hw
  apply Nat.div_le_div_right
  omega

end ModuloClass

/-! ## SharedPrefix -/
namespace SharedPrefix
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.search, member_optional_iff]
  let w := 2 ^ t
  have hw : 0 < w := Nat.pow_pos (by decide)
  constructor
  · intro ⟨h1, h2⟩
    have hdiv : y / w = x / w := by
      have hle : x / w ≤ y / w := Nat.div_le_div_right (by omega)
      have hlt : y / w < x / w + 1 := by rw [Nat.div_lt_iff_lt_mul hw]; omega
      omega
    simp [hdiv]
  · intro ht
    simp only [beq_iff_eq] at ht
    have hya : x / w * w ≤ y := by rw [← ht]; exact Nat.div_mul_le_self y w
    have hyb : y < x / w * w + w := by
      have : y < y / w * w + w := Nat.lt_div_mul_add hw
      omega
    exact ⟨hya, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.search
  exact optional_ordered _ _

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_ExactAtLeaf : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.ExactAtLeaf.Goal := by
  intro u x hx
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hx]

theorem sound_SplitSubtrees : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.SplitSubtrees.Goal := by
  intro u t x ys hv
  have hfan1 := offsetCount_of_spec spec ordered u (t+1) x ys hv
  have hfan2 := offsetCount_of_spec spec ordered u t ((x / 2^(t+1)) * 2^(t+1)) ys hv
  have hfan3 := offsetCount_of_spec spec ordered u t ((x / 2^(t+1)) * 2^(t+1) + 2^t) ys hv
  rw [hfan1, hfan2, hfan3]
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.search, List.countP_eq_length_filter]
  let a := (x / 2 ^ (t + 1)) * 2 ^ (t + 1)
  let w := 2 ^ t
  have hw : 0 < w := Nat.pow_pos (by decide)
  have h2 : 2 ^ (t + 1) = 2 * w := by rw [Nat.pow_succ]; omega
  have ha_div : a / w = (x / 2 ^ (t + 1)) * 2 := by
    simp [a, w, h2]; rw [Nat.mul_assoc, Nat.mul_comm 2 w, ← Nat.mul_assoc, Nat.mul_div_cancel _ hw]
  have ha2_div : (a + w) / w = a / w + 1 := by rw [Nat.add_div_right _ hw]
  have hsplit : ∀ y, (x / 2 ^ (t + 1) == y / 2 ^ (t + 1)) =
      ((a / 2 ^ t == y / 2 ^ t) || ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t)) := by
    intro y
    simp only [beq_iff_eq]
    constructor
    · intro h
      have h2w : 2 * w = 2^(t+1) := by rw [h2]
      have hdiv_w : y / w = 2 * (y / (2 * w)) + (y % (2 * w)) / w := by
        rw [show y / w = (y / (2 * w) * (2 * w) + y % (2 * w)) / w from by rw [Nat.div_add_mod]]
        rw [Nat.add_div_right _ hw]
        rw [show y / (2 * w) * (2 * w) / w = y / (2 * w) * 2 from by
          rw [Nat.mul_assoc, Nat.mul_comm w 2, ← Nat.mul_assoc, Nat.mul_div_cancel _ hw]]
      have hmod_lt : y % (2 * w) < 2 * w := Nat.mod_lt y (by omega)
      have hmod_div_le : (y % (2 * w)) / w ≤ 1 := by rw [Nat.div_le_iff_le_mul_add_pred hw]; omega
      rw [h2w] at hdiv_w hmod_lt hmod_div_le
      have ha_w : a / w = 2 * (x / 2^(t+1)) := ha_div
      rw [ha_w, ha2_div, ha_w]
      by_cases hm : (y % (2^(t+1))) / w = 0
      · left; simp only [decide_eq_true_eq]; rw [hdiv_w, hm, h.symm]; omega
      · right; have hm1 : (y % (2^(t+1))) / w = 1 := by omega
        simp only [decide_eq_true_eq]; rw [hdiv_w, hm1, h.symm]; omega
    · intro h
      have h2w : 2 * w = 2^(t+1) := by rw [h2]
      rcases h with h | h
      · simp only [decide_eq_true_eq] at h
        have h1 : y / w = a / w := h.symm
        have ha_w : a / w = 2 * (x / 2^(t+1)) := ha_div
        have hdiv_parent : y / 2^(t+1) = x / 2^(t+1) := by
          rw [← h2w, show y / (2*w) = (y / w) / 2 from by rw [Nat.div_div_eq_div_mul]; omega]
          rw [h1, ha_w]; omega
        simp [hdiv_parent]
      · simp only [decide_eq_true_eq] at h
        have h1 : y / w = (a + w) / w := h.symm
        rw [ha2_div, ha_div] at h1
        have hdiv_parent : y / 2^(t+1) = x / 2^(t+1) := by
          rw [← h2w, show y / (2*w) = (y / w) / 2 from by rw [Nat.div_div_eq_div_mul]; omega]
          rw [h1]; omega
        simp [hdiv_parent]
  rw [show (ys.filter (fun y => x / 2 ^ (t + 1) == y / 2 ^ (t + 1))).length =
      (ys.filter (fun y => (a / 2 ^ t == y / 2 ^ t) || ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t))).length from by
    congr 1; apply List.filter_congr; intro y _; exact hsplit y]
  rw [List.length_filter_add_length_filter_not (l := ys.filter (fun y => (a / 2 ^ t == y / 2 ^ t) || ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t))) (p := fun y => ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t))]
  have ha : (List.filter (fun y => (a / 2 ^ t == y / 2 ^ t) || ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t)) ys).filter (fun y => ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t)) =
      ys.filter (fun y => ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t)) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _
    by_cases h : ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t) <;> simp [h]
  have hb : (List.filter (fun y => (a / 2 ^ t == y / 2 ^ t) || ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t)) ys).filter (fun y => !(((a + 2 ^ t) / 2 ^ t == y / 2 ^ t))) =
      ys.filter (fun y => (a / 2 ^ t == y / 2 ^ t)) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _
    by_cases h1 : (a / 2 ^ t == y / 2 ^ t) <;> by_cases h2 : ((a + 2 ^ t) / 2 ^ t == y / 2 ^ t) <;> simp [h1, h2]
  rw [ha, hb]; omega

end SharedPrefix

/-! ## ScaledFloor -/
namespace ScaledFloor
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.search, member_optional_iff]
  let w := x + 1
  have hw : 0 < w := by omega
  constructor
  · intro ⟨h1, h2⟩
    have hdiv : y / w = t := by
      have hge : t ≤ y / w := (Nat.le_div_iff_mul_le hw).mpr h1
      have hlt : y / w < t + 1 := (Nat.div_lt_iff_lt_mul hw).mpr (by omega)
      omega
    simp [hdiv]
  · intro ht
    simp only [beq_iff_eq] at ht
    have h1 : t * w ≤ y := by rw [← ht]; exact Nat.div_mul_le_self y w
    have h2 : y < (t + 1) * w := by
      have : y < y / w * w + w := Nat.lt_div_mul_add hw
      rw [ht] at this; omega
    exact ⟨h1, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.search
  exact optional_ordered _ _

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_BucketWidth : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.BucketWidth.Goal := by
  intro t x
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional]

theorem sound_PartitionDomain : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.PartitionDomain.Goal := by
  intro u x ys hv
  have hfan := fun t => offsetCount_of_spec spec ordered u t x ys hv
  simp only [hfan, search, List.countP_eq_length_filter]
  -- partition by quotient
  have hpart : ((List.range (u / (x + 1) + 1)).map (fun t => (ys.filter (fun y => y / (x + 1) == t)).length)).sum = ys.length := by
    induction ys with
    | nil => simp
    | cons y ys ih =>
      have hyu : y < u := valid_lt hv (List.mem_cons_self y ys)
      have hs' : valid u ys := ⟨hv.1.tail, fun z hz => hv.2 z (List.mem_cons.mpr (Or.inr hz))⟩
      simp only [List.filter_cons, List.length_cons]
      have ht_lt : y / (x + 1) < u / (x + 1) + 1 := Nat.lt_succ_of_le (Nat.div_le_div_right (Nat.le_of_lt hyu))
      rw [ih hs']
      rw [← List.sum_map_add]
      congr 1
      apply List.map_congr_left
      intro t _
      by_cases heq : y / (x + 1) = t
      · subst heq; simp [List.length_cons]; omega
      · have hne : (y / (x + 1) == t) = false := beq_eq_false_iff_ne.mpr heq
        simp [hne]
  exact hpart

end ScaledFloor

/-! ## Apart -/
namespace Apart
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.search, member_append, member_optional_iff]
  constructor
  · intro h
    rcases h with ⟨_, h2⟩ | ⟨h1, _⟩
    · simp; omega
    · simp; omega
  · intro ht
    simp only [Bool.or_eq_true, decide_eq_true_eq] at ht
    rcases ht with h | h
    · right; exact ⟨h, hyu⟩
    · left; exact ⟨Nat.zero_le _, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  refine ⟨?_, ?_⟩
  · by_cases h1 : 0 < min u (x - t) <;> by_cases h2 : x + t + 1 < u <;>
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h1, h2, List.pairwise_cons] <;> omega
  · intro p hp
    simp only [List.mem_append] at hp
    rcases hp with hp | hp
    · by_cases h1 : 0 < min u (x - t) <;> simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h1] at hp <;> (rcases hp with rfl; omega)
    · by_cases h2 : x + t + 1 < u <;> simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h2] at hp <;> (rcases hp with rfl; omega)

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_ZeroSlack : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.ZeroSlack.Goal := by
  intro u x ys hv
  have h := offsetCount_of_spec spec ordered u 0 x ys hv
  rw [h]
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.search, Nat.add_zero, List.countP_eq_length_filter, List.count_eq_length_filter]
  have hpart : ∀ y, (decide (x < y) || decide (y < x)) || decide (y == x) = true := by
    intro y; by_cases h1 : y = x <;> by_cases h2 : x < y <;> by_cases h3 : y < x <;> simp_all <;> omega
  have hcover : (ys.filter (fun y => (decide (x < y) || decide (y < x)) || decide (y == x))).length = ys.length := by
    rw [List.filter_length_eq_length]; intro y _; exact hpart y
  rw [List.length_filter_add_length_filter_not (l := ys.filter (fun y => (decide (x < y) || decide (y < x)) || decide (y == x))) (p := fun y => decide (y == x))]
  have ha : (List.filter (fun y => (decide (x < y) || decide (y < x)) || decide (y == x)) ys).filter (fun y => decide (y == x)) =
      ys.filter (fun y => decide (y == x)) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _; by_cases h1 : y = x <;> simp [h1]
  have hb : (List.filter (fun y => (decide (x < y) || decide (y < x)) || decide (y == x)) ys).filter (fun y => !decide (y == x)) =
      ys.filter (fun y => decide (x < y) || decide (y < x)) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _
    by_cases h1 : y = x <;> by_cases h2 : x < y <;> by_cases h3 : y < x <;> simp [h1, h2, h3] <;> omega
  rw [ha, hb] at *; linarith

theorem sound_ComplementMass : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.ComplementMass.Goal := by
  intro u t x ys hv
  have h := offsetCount_of_spec spec ordered u t x ys hv
  rw [h]
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart.search, List.countP_eq_length_filter]
  have hpart : ∀ y, ((decide (x + t < y) || decide (y + t < x)) || (decide (x ≤ y + t) && decide (y ≤ x + t))) = true := by
    intro y; by_cases h1 : x + t < y <;> by_cases h2 : y + t < x <;> by_cases h3 : x ≤ y + t <;> by_cases h4 : y ≤ x + t <;> simp_all <;> omega
  have hcover : (ys.filter (fun y => ((decide (x + t < y) || decide (y + t < x)) || (decide (x ≤ y + t) && decide (y ≤ x + t))))).length = ys.length := by
    rw [List.filter_length_eq_length]; intro y _; exact hpart y
  rw [List.length_filter_add_length_filter_not (l := ys.filter (fun y => ((decide (x + t < y) || decide (y + t < x)) || (decide (x ≤ y + t) && decide (y ≤ x + t))))) (p := fun y => (decide (x ≤ y + t) && decide (y ≤ x + t)))]
  have ha : (List.filter (fun y => ((decide (x + t < y) || decide (y + t < x)) || (decide (x ≤ y + t) && decide (y ≤ x + t)))) ys).filter (fun y => (decide (x ≤ y + t) && decide (y ≤ x + t))) =
      ys.filter (fun y => (decide (x ≤ y + t) && decide (y ≤ x + t))) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _; by_cases h : (decide (x ≤ y + t) && decide (y ≤ x + t)) <;> simp [h]
  have hb : (List.filter (fun y => ((decide (x + t < y) || decide (y + t < x)) || (decide (x ≤ y + t) && decide (y ≤ x + t)))) ys).filter (fun y => !(decide (x ≤ y + t) && decide (y ≤ x + t))) =
      ys.filter (fun y => (decide (x + t < y) || decide (y + t < x))) := by
    rw [List.filter_filter]; apply List.filter_congr; intro y _
    by_cases h1 : (decide (x + t < y) || decide (y + t < x)) <;> by_cases h2 : (decide (x ≤ y + t) && decide (y ≤ x + t)) <;> simp [h1, h2]
  rw [ha, hb] at *; linarith

end Apart

/-! ## ReciprocalPrice -/
namespace ReciprocalPrice
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.search, member_optional_iff]
  constructor
  · intro ⟨h1, h2⟩
    by_cases hx : x == 0
    · simp [hx]
    · simp only [hx, Bool.false_eq_true, ↓reduceIte] at h2
      have hx0 : 0 < x := by cases x <;> simp_all
      have hyt : y ≤ t / x := by omega
      have : x * y ≤ t := Nat.le_trans (Nat.mul_le_mul_left x hyt) (Nat.mul_div_le t x)
      simpa [hx] using this
  · intro ht
    by_cases hx : x == 0
    · simp [hx] at ht ⊢; omega
    · have hx0 : 0 < x := by cases x <;> simp_all
      simp only [hx, Bool.false_eq_true, ↓reduceIte]
      simp only [hx, Bool.false_eq_true, ↓reduceIte, decide_eq_true_eq] at ht
      exact ⟨Nat.zero_le _, by have := (Nat.le_div_iff_mul_le hx0).mpr ht; omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  unfold TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.search
  exact optional_ordered _ _

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_FreeItem : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.FreeItem.Goal := by
  intro u t ys hv
  have h := offsetCount_of_spec spec ordered u t 0 ys hv
  rw [h]
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.search, List.countP_eq_length_filter]

theorem sound_MonotoneProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.MonotoneProbe.Goal := by
  intro u t x z ys hv hxz
  have h1 := offsetCount_of_spec spec ordered u t z ys hv
  have h2 := offsetCount_of_spec spec ordered u t x ys hv
  rw [h1, h2]
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice.search, List.countP_eq_length_filter]
  apply List.length_filter_le_filter_of_imp
  intro y _
  simp only [decide_eq_true_eq]
  intro hz
  exact Nat.le_trans (Nat.mul_le_mul_right y hxz) hz

end ReciprocalPrice

/-! ## Submask -/
namespace Submask
private theorem submasks_sub {x y : Nat} (hy : y ∈ submasks x) : (y &&& x) == y := by
  unfold submasks at hy
  suffices h : ∀ (bits : List Nat) (acc : List Nat),
      (∀ z ∈ acc, (z &&& x) == z) →
      (∀ b ∈ bits, x.testBit b = true) →
      ∀ y ∈ bits.foldl (fun acc bit => acc ++ acc.map (· + 2^bit)) acc, (y &&& x) == y from
    h _ [0] (by simp) (by intro b hb; simp [List.mem_filter.mp hb |>.2]) hy
  intro bits
  induction bits with
  | nil => intro acc hacc _ y hy; exact hacc y hy
  | cons b bs ih =>
    intro acc hacc hbits y hy
    simp only [List.foldl_cons] at hy
    refine ih _ ?_ (fun c hc => hbits c (List.mem_cons.mpr (Or.inr hc))) hy
    intro z hz
    simp only [List.mem_append, List.mem_map] at hz
    rcases hz with hz | ⟨z', hz', rfl⟩
    · exact hacc z hz
    · have hz'_sub := hacc z' hz'
      have hb : x.testBit b = true := hbits b List.mem_cons_self
      simp only [beq_iff_eq] at hz'_sub ⊢
      ext i
      by_cases hib : i = b
      · subst hib
        simp [Nat.testBit_and, hb, Nat.testBit_two_pow_self, Nat.testBit_add_two_pow_self]
      · simp only [Nat.testBit_and]
        rw [Nat.testBit_add_two_pow_of_ne hib]
        have := congr_arg (fun n => Nat.testBit n i) hz'_sub
        simp only [Nat.testBit_and] at this
        by_cases hxi : x.testBit i <;> simp [hxi] at this ⊢ <;> assumption

private theorem mem_submasks_of_submask {x y : Nat} (hy : (y &&& x) == y) : y ∈ submasks x := by
  unfold submasks
  let bits := (List.range (width x)).filter x.testBit
  have hbits_all : ∀ b ∈ bits, x.testBit b = true := fun b hb => (List.mem_filter.mp hb).2
  -- Induction: for any list of bits (all set in x), if all set bits of z are in bs or z is in acc, then z ends up in foldl.
  suffices h : ∀ (bs : List Nat) (acc : List Nat),
      (∀ b ∈ bs, x.testBit b = true) →
      (∀ z, (z &&& x) == z → (∀ b, z.testBit b = true → b ∉ bs) → z ∈ acc) →
      ∀ z, (z &&& x) == z → z ∈ bs.foldl (fun acc bit => acc ++ acc.map (· + 2^bit)) acc from
    h bits [0] hbits_all (by
      intro z hz hno
      have hz0 : z = 0 := by
        by_contra hz0
        have ⟨b, hb⟩ : ∃ b, z.testBit b = true := by
          by_contra hc; simp only [not_exists] at hc
          exact hz0 (by ext i; simp [hc i])
        have hbx : x.testBit b = true := by
          have := congr_arg (fun n => Nat.testBit n b) (eq_of_beq hz)
          simp only [Nat.testBit_and] at this
          revert this; cases x.testBit b <;> cases z.testBit b <;> simp_all
        have hb_lt : b < width x := by
          by_contra hc; simp only [Nat.not_lt] at hc
          have hxb' : x < 2^b := by
            unfold width at hc
            by_cases hx0 : x = 0
            · simp [hx0] at hbx
            · simp only [hx0, ite_false] at hc
              exact Nat.lt_of_lt_of_le Nat.lt_log2_self (Nat.pow_le_pow_right (by decide) hc)
          exact absurd hbx (by simp [Nat.testBit_lt_two_pow hxb'])
        have hbmem : b ∈ bits := by simp [bits, List.mem_filter, List.mem_range, hb_lt, hbx]
        exact hno b hb hbmem
      simp [hz0]) y hy
  intro bs
  induction bs with
  | nil =>
    intro acc _ hacc z hz
    exact hacc z hz (by intro b _ hnb; exact False.elim (hnb trivial))
  | cons b bs ih =>
    intro acc hbits hacc z hz
    simp only [List.foldl_cons]
    apply ih (fun c hc => hbits c (List.mem_cons.mpr (Or.inr hc)))
    intro z hz hno
    by_cases hzb : z.testBit b = true
    · have hb2 : 2^b ≤ z := Nat.ge_two_pow_of_testBit hzb
      have hz' : ((z - 2^b) &&& x) == z - 2^b := by
        simp only [beq_iff_eq] at hz ⊢
        ext i
        by_cases hib : i = b
        · subst hib
          have hbx : x.testBit b = true := hbits b List.mem_cons_self
          simp [Nat.testBit_and, hbx, Nat.testBit_sub_two_pow_self hzb]
        · simp only [Nat.testBit_and, Nat.testBit_sub_two_pow_of_ne hzb hib]
          have := congr_arg (fun n => Nat.testBit n i) hz
          simp only [Nat.testBit_and] at this; exact this
      have hno' : ∀ c, (z - 2^b).testBit c = true → c ∉ bs := by
        intro c hc hcbs
        by_cases hcb : c = b
        · subst hcb; simp [Nat.testBit_sub_two_pow_self hzb] at hc
        · have hzc : z.testBit c = true := by rwa [Nat.testBit_sub_two_pow_of_ne hzb hcb] at hc
          exact hno c hzc (fun hm => by rcases hm with rfl | hm => exact hcb rfl; exact hcbs hm)
      have hmem' := hacc (z - 2^b) hz' hno'
      simp only [List.mem_append, List.mem_map]
      right
      exact ⟨z - 2^b, hmem', Nat.sub_add_cancel hb2⟩
    · have hno' : ∀ c, z.testBit c = true → c ∉ bs := by
        intro c hc hcbs
        exact hno c hc (fun hm => by rcases hm with rfl | hm => exact hzb hc; exact hcbs hm)
      have hmem' := hacc z hz hno'
      simp only [List.mem_append]
      left; exact hmem'


private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.search, member]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨p, hp, hpred⟩
    simp only [List.mem_map, List.mem_filter] at hp
    rcases hp with ⟨z, ⟨hz_sub, hz_filt⟩, rfl⟩
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred hz_filt
    rcases hpred with ⟨rfl, _⟩
    simp only [Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
    exact ⟨eq_of_beq (submasks_sub hz_sub), hz_filt.2⟩
  · intro ht
    simp only [Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at ht
    rcases ht with ⟨hsub, hyt⟩
    rw [List.any_eq_true]
    refine ⟨(y, y+1), ?_, ?_⟩
    · simp only [List.mem_map, List.mem_filter]
      exact ⟨y, ⟨mem_submasks_of_submask (beq_of_eq hsub), by simp [hyu, hyt]⟩, rfl⟩
    · simp

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  constructor
  · have hpair : (submasks x).Pairwise (· < ·) := by
      unfold submasks
      -- fold preserves pairwise ascending
      suffices h : ∀ (bs : List Nat) (acc : List Nat),
          acc.Pairwise (· < ·) →
          (∀ b ∈ bs, ∀ z ∈ acc, z < 2^b) →
          bs.Pairwise (· < ·) →
          (bs.foldl (fun acc bit => acc ++ acc.map (· + 2^bit)) acc).Pairwise (· < ·) by
        refine h _ [0] (by simp) ?_ ?_
        · intro b hb z hz; simp at hz; subst hz; exact Nat.pow_pos (by decide)
        · exact ((List.range (width x)).filter x.testBit).pairwise_of_forall_lt_lt (fun a ha b hb hab => by
            simp only [List.mem_filter, List.mem_range] at ha hb; omega)
      intro bs acc hacc hbound hbs
      induction bs generalizing acc with
      | nil => simpa
      | cons b bs ih =>
        simp only [List.foldl_cons]
        apply ih
        · rw [List.pairwise_append]
          refine ⟨hacc, ?_, ?_⟩
          · rw [List.pairwise_map]; exact hacc.imp (fun a b h => by omega)
          · intro a ha c hc
            simp only [List.mem_map] at hc
            rcases hc with ⟨a', ha', rfl⟩
            have : a < 2^b := hbound b List.mem_cons_self a ha
            omega
        · intro b' hb' z hz
          simp only [List.mem_append, List.mem_map] at hz
          have hbb' : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
          rcases hz with hz | ⟨z', hz', rfl⟩
          · have : z < 2^b := hbound b List.mem_cons_self z hz
            exact Nat.lt_of_lt_of_le this (Nat.pow_le_pow_right (by decide) (by omega))
          · have : z' < 2^b := hbound b List.mem_cons_self z' hz'
            calc z' + 2^b < 2^b + 2^b := by omega
              _ = 2^(b+1) := by rw [Nat.pow_succ]; omega
              _ ≤ 2^b' := Nat.pow_le_pow_right (by decide) hbb'
        · exact (List.pairwise_cons.mp hbs).2
    have hfilt : ((submasks x).filter (fun y => y < u && y ≤ t)).Pairwise (· < ·) :=
      hpair.sublist (List.filter_sublist _)
    rw [List.pairwise_map]
    exact hfilt.imp (fun a b h => by omega)
  · intro p hp
    simp only [List.mem_map, List.mem_filter] at hp
    rcases hp with ⟨y, _, rfl⟩; simp

theorem sound_OrderedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_ZeroMask : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.ZeroMask.Goal := by
  intro u t hu
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.search, submasks, width, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hu]

theorem sound_DistinctPostings : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.DistinctPostings.Goal := by
  intro u t x
  simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.search, List.map_map, Function.comp]
  constructor
  · have hpair : (submasks x).Pairwise (· < ·) := by
      -- reuse from ordered
      unfold submasks
      suffices h : ∀ (bs : List Nat) (acc : List Nat),
          acc.Pairwise (· < ·) →
          (∀ b ∈ bs, ∀ z ∈ acc, z < 2^b) →
          bs.Pairwise (· < ·) →
          (bs.foldl (fun acc bit => acc ++ acc.map (· + 2^bit)) acc).Pairwise (· < ·) by
        refine h _ [0] (by simp) ?_ ?_
        · intro b hb z hz; simp at hz; subst hz; exact Nat.pow_pos (by decide)
        · exact ((List.range (width x)).filter x.testBit).pairwise_of_forall_lt_lt (fun a ha b hb hab => by
            simp only [List.mem_filter, List.mem_range] at ha hb; omega)
      intro bs acc hacc hbound hbs
      induction bs generalizing acc with
      | nil => simpa
      | cons b bs ih =>
        simp only [List.foldl_cons]
        apply ih
        · rw [List.pairwise_append]
          refine ⟨hacc, ?_, ?_⟩
          · rw [List.pairwise_map]; exact hacc.imp (fun a b h => by omega)
          · intro a ha c hc
            simp only [List.mem_map] at hc
            rcases hc with ⟨a', ha', rfl⟩
            have : a < 2^b := hbound b List.mem_cons_self a ha
            omega
        · intro b' hb' z hz
          simp only [List.mem_append, List.mem_map] at hz
          have hbb' : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
          rcases hz with hz | ⟨z', hz', rfl⟩
          · have : z < 2^b := hbound b List.mem_cons_self z hz
            exact Nat.lt_of_lt_of_le this (Nat.pow_le_pow_right (by decide) (by omega))
          · have : z' < 2^b := hbound b List.mem_cons_self z' hz'
            calc z' + 2^b < 2^b + 2^b := by omega
              _ = 2^(b+1) := by rw [Nat.pow_succ]; omega
              _ ≤ 2^b' := Nat.pow_le_pow_right (by decide) hbb'
        · exact (List.pairwise_cons.mp hbs).2
    have hfilt : ((submasks x).filter (fun y => y < u && y ≤ t)).Pairwise (· < ·) :=
      hpair.sublist (List.filter_sublist _)
    exact hfilt.imp (fun a b h => by omega)
  · have hpair : (submasks x).Pairwise (· < ·) := by
      unfold submasks
      suffices h : ∀ (bs : List Nat) (acc : List Nat),
          acc.Pairwise (· < ·) →
          (∀ b ∈ bs, ∀ z ∈ acc, z < 2^b) →
          bs.Pairwise (· < ·) →
          (bs.foldl (fun acc bit => acc ++ acc.map (· + 2^bit)) acc).Pairwise (· < ·) by
        refine h _ [0] (by simp) ?_ ?_
        · intro b hb z hz; simp at hz; subst hz; exact Nat.pow_pos (by decide)
        · exact ((List.range (width x)).filter x.testBit).pairwise_of_forall_lt_lt (fun a ha b hb hab => by
            simp only [List.mem_filter, List.mem_range] at ha hb; omega)
      intro bs acc hacc hbound hbs
      induction bs generalizing acc with
      | nil => simpa
      | cons b bs ih =>
        simp only [List.foldl_cons]
        apply ih
        · rw [List.pairwise_append]
          refine ⟨hacc, ?_, ?_⟩
          · rw [List.pairwise_map]; exact hacc.imp (fun a b h => by omega)
          · intro a ha c hc
            simp only [List.mem_map] at hc
            rcases hc with ⟨a', ha', rfl⟩
            have : a < 2^b := hbound b List.mem_cons_self a ha
            omega
        · intro b' hb' z hz
          simp only [List.mem_append, List.mem_map] at hz
          have hbb' : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
          rcases hz with hz | ⟨z', hz', rfl⟩
          · have : z < 2^b := hbound b List.mem_cons_self z hz
            exact Nat.lt_of_lt_of_le this (Nat.pow_le_pow_right (by decide) (by omega))
          · have : z' < 2^b := hbound b List.mem_cons_self z' hz'
            calc z' + 2^b < 2^b + 2^b := by omega
              _ = 2^(b+1) := by rw [Nat.pow_succ]; omega
              _ ≤ 2^b' := Nat.pow_le_pow_right (by decide) hbb'
        · exact (List.pairwise_cons.mp hbs).2
    have hnodup : ((submasks x).filter (fun y => y < u && y ≤ t)).Nodup :=
      (hpair.sublist (List.filter_sublist _)).imp (fun a b h => by omega)
    have hsub1 : ((submasks x).filter (fun y => y < u && y ≤ t)) ⊆ List.range u := by
      intro y hy; simp only [List.mem_filter, List.mem_range] at hy ⊢; exact hy.2.1
    have hsub2 : ((submasks x).filter (fun y => y < u && y ≤ t)) ⊆ List.range (t + 1) := by
      intro y hy; simp only [List.mem_filter, List.mem_range] at hy ⊢; omega
    refine Nat.le_min ?_ ?_
    · calc ((submasks x).filter (fun y => y < u && y ≤ t) |>.map (fun y => (y, y + 1))).length
          = ((submasks x).filter (fun y => y < u && y ≤ t)).length := by simp
        _ ≤ (List.range u).length := hnodup.length_le_of_sublist hsub1
        _ = u := List.length_range u
    · calc ((submasks x).filter (fun y => y < u && y ≤ t) |>.map (fun y => (y, y + 1))).length
          = ((submasks x).filter (fun y => y < u && y ≤ t)).length := by simp
        _ ≤ (List.range (t + 1)).length := hnodup.length_le_of_sublist hsub2
        _ = t + 1 := List.length_range (t + 1)

end Submask

end TinylambOpt.ResearchProofs.StorageExecution.PostingJoins
