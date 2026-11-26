import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Apart.LeftMultiplicity` — Charge one left placeholder when zero right keys match,
  including a NULL left key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem optional_eq (a b : Nat) :
    optional a b = if a < b then [(a, b)] else [] := rfl

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [optional_eq, h]

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [optional_eq, h]

private theorem member_optional {a b y : Nat} :
    member (optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [optional_eq, h, member]
  · simp [optional_eq, h, member]
    omega

private theorem member_append (r₁ r₂ : Ranges) (y : Nat) :
    member (r₁ ++ r₂) y = (member r₁ y || member r₂ y) := by
  simp [member, List.any_append]

private theorem optional_ordered (a b : Nat) :
    (optional a b).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ optional a b, p.1 ≤ p.2 := by
  by_cases h : a < b
  · simp [optional_eq, h, Nat.le_of_lt h]
  · simp [optional_eq, h]

private theorem optional_append_ordered (a b c d u : Nat) (hbc : b ≤ c) :
    (optional a (min u b) ++ optional c (min u d)).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ optional a (min u b) ++ optional c (min u d), p.1 ≤ p.2 := by
  constructor
  · by_cases h₁ : a < min u b <;> by_cases h₂ : c < min u d <;>
      simp [optional_eq, h₁, h₂, List.pairwise_cons] <;> omega
  · intro p hp
    simp only [List.mem_append] at hp
    cases hp with
    | inl hp =>
      by_cases h₁ : a < min u b
      · simp [optional_eq, h₁] at hp
        rcases hp with rfl
        omega
      · simp [optional_eq, h₁] at hp
    | inr hp =>
      by_cases h₂ : c < min u d
      · simp [optional_eq, h₂] at hp
        rcases hp with rfl
        omega
      · simp [optional_eq, h₂] at hp

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat}
    (hy : y ∈ ys) : y < u := hv.2 y hy

/-! ## Binary lower bound -/

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
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, by simp, by simp⟩
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
          have hlt_mid : xs.toList.getD mid 0 < v := by
            rw [hlistval]
            exact hv
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              simpa [getD_eq_getElem hmidlen] using hlt_mid
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  simpa only [List.getD, List.getElem?_eq_getElem hjlen,
                    List.getElem?_eq_getElem hmidlen, Option.getD_some] using
                    hs.rel_getElem_of_lt hjlen hmidlen hjm
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ :=
            ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, by omega, hub⟩
        · rw [if_neg hv]
          have hge_mid : v ≤ xs.toList.getD mid 0 := by
            rw [hlistval]
            omega
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              simpa [getD_eq_getElem hmidlen] using hge_mid
            · have hmj : mid < j := by omega
              by_cases hjh : hi ≤ j
              · exact habove j hjh hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  simpa only [List.getD, List.getElem?_eq_getElem hmidlen,
                    List.getElem?_eq_getElem hjlen, Option.getD_some] using
                    hs.rel_getElem_of_lt hmidlen hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ :=
            ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
    simp
  · have hne : n ≠ 0 := by
      intro hz
      simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      simp [Nat.pow_succ, Nat.mul_comm]
    rw [hpow]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    simpa using width_pow_bound ys.length
  have habove0 : ∀ j, ys.length ≤ j → j < ys.toArray.toList.length →
      v ≤ ys.toArray.toList.getD j 0 := by
    intro j hj hjlen
    have hj' : j < ys.length := by simpa using hjlen
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega) habove0 (by simpa using hs)
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
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) =
      ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hi_low : i < lower ys v := by
      simpa only [List.length_take, Nat.min_eq_left hle] using hi
    have hiys : i < ys.length := by omega
    have hlt : ys.getD i 0 < v := lower_lt_of_lt hs v hi_low
    rw [getD_eq_getElem hiys] at hlt
    exact hlt
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
      rw [List.length_take]
      simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter
          (fun x => decide (x < v))).length := by rw [List.filter_append]
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
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hxa, hxb, hax, hnil, ih hs']
      · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          intro hp
          simp only [decide_eq_true_eq, Bool.and_eq_true] at hp
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem take_filter_prefix {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ys.take (ys.filter (fun x => decide (x < v))).length =
      ys.filter (fun x => decide (x < v)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
    have hs' := hs.tail
    by_cases hx : x < v
    · simp [hx, ih hs']
    · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro y hy
        simp only [decide_eq_true_eq]
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
        omega
      simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y && y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
      lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b,
      take_filter_prefix hs b, filter_lt_prefix_filter_lt hs hab, List.drop_left]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    intro hp
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hp
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
    rw [hz]
    simp

/-! ## Ordered spans and their generic consequences -/

private def SpansSpec (s : Search) : Prop :=
  ∀ u t x y, y < u → (member (s.spans u t x) y ↔ s.test t x y = true)

private def SpansOrdered (s : Search) : Prop :=
  ∀ u t x, (s.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ s.spans u t x, p.1 ≤ p.2

private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (decide (a ≤ y && y < b) || member rs y) := by
  simp [member]

private theorem filter_member_cons_of_ordered {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => decide (a ≤ y && y < b)) ++ ys.filter (fun y => member rs y) := by
  induction ys with
  | nil => simp [member]
  | cons y ys ih =>
    have hfilter := ih hs.tail
    have htailfilter :
        ys.filter (fun z => (decide (a ≤ z && z < b)) || member rs z) =
          ys.filter (fun z => member ((a, b) :: rs) z) := by
      apply List.filter_congr
      intro z hz
      rw [member_cons]
    have hq_b : member rs y = true → b ≤ y := by
      intro hm
      simp only [member, List.any_eq_true] at hm
      rcases hm with ⟨p, hpm, hp⟩
      have hbp := hb p hpm
      simp only [decide_eq_true_eq, Bool.and_eq_true] at hp
      omega
    simp only [member_cons, List.filter_cons]
    by_cases hq : member rs y = true
    · have hby : b ≤ y := hq_b hq
      have hp_y : (decide (a ≤ y && y < b)) = false := by
        have hnlt : ¬ y < b := Nat.not_lt_of_ge hby
        simp [hnlt, hby]
      have hp_tail : ys.filter (fun z => decide (a ≤ z && z < b)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro z hz
        intro h
        simp only [decide_eq_true_eq, Bool.and_eq_true] at h
        have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
        omega
      rw [htailfilter, hfilter, hp_tail]
      simp [hp_y, hq, hby]
    · have hqf : member rs y = false := by
        cases hmy : member rs y <;> simp_all
      by_cases hp : (decide (a ≤ y && y < b)) = true
      · have hpProp : a ≤ y ∧ y < b := by
          simpa only [decide_eq_true_eq, Bool.and_eq_true] using hp
        rw [htailfilter, hfilter]
        simp [hpProp, hqf]
      · have hpProp : ¬ (a ≤ y ∧ y < b) := by
          intro h
          apply hp
          simp [h]
        rw [htailfilter, hfilter]
        simp [hpProp, hqf]

private theorem filter_member_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    ys.filter (fun y => member rs y) =
      rs.flatMap (fun (a, b) => ys.filter (fun y => decide (a ≤ y && y < b))) := by
  induction rs with
  | nil => simp [member]
  | cons p rs ih =>
    obtain ⟨a, b⟩ := p
    simp only [List.flatMap_cons]
    have hb : ∀ q ∈ rs, b ≤ q.1 := fun q hq => (List.pairwise_cons.mp hrs).1 q hq
    have hrs' := (List.pairwise_cons.mp hrs).2
    have hrs_wf' : ∀ q ∈ rs, q.1 ≤ q.2 :=
      fun q hq => hrs_wf q (List.mem_cons_of_mem _ hq)
    rw [filter_member_cons_of_ordered hs hb, ih hrs' hrs_wf']

private theorem seek_eq_filter_member {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    rs.flatMap (fun (a, b) => slice ys a b) = ys.filter (fun y => member rs y) := by
  rw [filter_member_eq_flatMap hs hrs hrs_wf]
  congr 1
  funext p
  obtain ⟨a, b⟩ := p
  exact slice_eq_filter hs a b

private theorem orderedProbe_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : orderedProbe s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  have ⟨hrs, hwf⟩ := hord u t x
  unfold seek
  rw [seek_eq_filter_member hs hrs hwf]
  apply List.filter_congr
  intro y hy
  have h := hspec u t x y (valid_lt hv hy)
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
  rw [fan_eq_seek_length hs, orderedProbe_of_spec hspec hord u t x ys hv,
    List.countP_eq_length_filter]

private theorem sum_flatMap_nat {α : Type} (xs : List α) (f : α → List Nat) :
    (xs.flatMap f).sum = (xs.map (fun x => (f x).sum)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.sum_cons, ih, Nat.add_assoc]

private theorem take_sum_split (ys : List Nat) (p q : Nat) (hpq : p ≤ q) :
    (ys.take q).sum = (ys.take p).sum + ((ys.drop p).take (q - p)).sum := by
  have hlist : ys.take q = ys.take p ++ (ys.drop p).take (q - p) := by
    calc
      ys.take q = (ys.take q).take p ++ (ys.take q).drop p :=
        (List.take_append_drop p (ys.take q)).symm
      _ = ys.take p ++ (ys.drop p).take (q - p) := by
        rw [List.take_take, Nat.min_eq_left hpq, List.drop_take]
  rw [hlist, List.sum_append]

private theorem slice_sum {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (slice ys a b).sum = sumAt ys (lower ys b) - sumAt ys (lower ys a) := by
  unfold slice sumAt
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    have hlb := lower_le_length hs b
    have hsplit := take_sum_split ys (lower ys a) (lower ys b) hmono
    omega
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero, List.sum_nil]
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
  simp [slice_sum hs]

private theorem integralSum_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : integralSum s := by
  intro u t x ys hv
  rw [total_eq_seek_sum (valid_pairwise hv),
    orderedProbe_of_spec hspec hord u t x ys hv]

private theorem leftMultiplicity_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : leftMultiplicity s := by
  intro u t xs ys hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  have hcount := fun v => offsetCount_of_spec hspec hord u t v (ys.filterMap id) hv
  cases x with
  | none => simp [Option.any, List.filter]
  | some v =>
    simp only [Option.any, Option.map_some, Option.getD_some]
    let rhs := (ys.filterMap id).filter (s.test t v)
    change (if rhs.isEmpty then [(some v, none)] else
      rhs.map (fun y => (some v, some y))).length =
      max 1 (fan s u t v (ys.filterMap id))
    have hcountv : fan s u t v (ys.filterMap id) = rhs.length := by
      have hc := hcount v
      rw [List.countP_eq_length_filter] at hc
      exact hc
    by_cases h : rhs.isEmpty
    · have hlen : rhs.length = 0 := by simpa using h
      rw [if_pos h, hcountv, hlen]
      simp
    · have hne : rhs.length ≠ 0 := by
        intro hz
        apply h
        simpa using hz
      have hlen : 0 < rhs.length := Nat.pos_of_ne_zero hne
      rw [if_neg h, hcountv]
      simp [Nat.max_eq_right (by omega : 1 ≤ rhs.length)]


/-! ## Apart-specific span certificate -/

private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [search, member_append, Bool.or_eq_true, member_optional]
  constructor
  · intro h
    rcases h with ⟨_, h2⟩ | ⟨h1, _⟩
    · simp
      omega
    · simp
      omega
  · intro ht
    simp only [Bool.or_eq_true, decide_eq_true_eq] at ht
    rcases ht with h | h
    · right
      exact ⟨h, hyu⟩
    · left
      exact ⟨Nat.zero_le _, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  refine ⟨?_, ?_⟩
  · by_cases h1 : 0 < min u (x - t) <;> by_cases h2 : x + t + 1 < u <;>
      simp [optional_eq, h1, h2, List.pairwise_cons] <;> omega
  · intro p hp
    simp only [List.mem_append] at hp
    rcases hp with hp | hp
    · by_cases h1 : 0 < min u (x - t) <;> simp [optional_eq, h1] at hp <;>
        (rcases hp with rfl; omega)
    · by_cases h2 : x + t + 1 < u <;> simp [optional_eq, h2] at hp <;>
        (rcases hp with rfl; omega)

theorem sound_LeftMultiplicity : Apart.LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_LeftMultiplicity
