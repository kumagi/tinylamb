import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_SubtractInnerBand

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ### Generic interval machinery -/

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

private theorem member_optional {a b y : Nat} :
    member (optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [optional_eq, h, member]
  · simp [optional_eq, h, member]
    omega

private theorem member_append (r₁ r₂ : Ranges) (y : Nat) :
    member (r₁ ++ r₂) y = (member r₁ y || member r₂ y) := by
  simp [member, List.any_append]

private theorem optional_append_ordered (a b c d u : Nat) (hbc : b ≤ c) :
    (optional a (min u b) ++ optional c (min u d)).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ optional a (min u b) ++ optional c (min u d), p.1 ≤ p.2 := by
  constructor
  · by_cases h₁ : a < min u b <;> by_cases h₂ : c < min u d <;>
      simp [optional_eq, h₁, h₂, List.pairwise_cons] <;> omega
  · intro p hp
    simp only [List.mem_append] at hp
    rcases hp with hp | hp
    · by_cases h₁ : a < min u b <;> simp [optional_eq, h₁] at hp <;> (rcases hp with rfl; omega)
    · by_cases h₂ : c < min u d <;> simp [optional_eq, h₂] at hp <;> (rcases hp with rfl; omega)

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
    rw [Array.getElem?_eq_getElem ha, Option.getD_some]
    simp [List.getD, List.getElem?_eq_getElem hi, List.getElem_toArray ha]
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
        · rw [ite_eq_left hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by simpa using (by omega : j < xs.size)
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
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, by omega, hub⟩
        · rw [ite_eq_right hv]
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
          obtain ⟨hc, hlb, hub⟩ := ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
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
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 := (lower_cut hs v).1.2 i hj hi

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _) (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    have hi' : i < lower ys v := by omega
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by omega
    have hlt := lower_lt_of_lt hs v hi'
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
      rw [List.length_take]
      simp [Nat.min_eq_left hle]
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
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            have hnot : ¬ (a ≤ y ∧ y < b) := by omega
            simp [hnot]
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
    have hnot : ¬ (a ≤ y ∧ y < b) := by omega
    simp [hnot]

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

private def SpansSpec (s : Search) : Prop :=
  ∀ u t x y, y < u → (member (s.spans u t x) y ↔ s.test t x y = true)

private def SpansOrdered (s : Search) : Prop :=
  ∀ u t x, (s.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ s.spans u t x, p.1 ≤ p.2

private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (decide (a ≤ y && y < b) || member rs y) := by
  simp [member]

private theorem countP_or_of_disjoint {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    xs.countP (fun x => p x || q x) = xs.countP p + xs.countP q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have htail : ∀ y ∈ xs, p y = true → q y = false := fun y hy => hdisj y (by simp [hy])
      by_cases hp : p x = true
      · have hq : q x = false := hdisj x (by simp) hp
        simp [List.countP_cons, ih htail, hp, hq] <;> omega
      · by_cases hq : q x = true
        · simp [List.countP_cons, ih htail, hp, hq] <;> omega
        · simp [List.countP_cons, ih htail, hp, hq] <;> omega

private theorem countP_member_cons_of_ordered {ys : List Nat}
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) :
    ys.countP (fun y => member ((a, b) :: rs) y) =
      ys.countP (fun y => decide (a ≤ y && y < b)) + ys.countP (fun y => member rs y) := by
  have hdisj : ∀ y ∈ ys, (decide (a ≤ y && y < b)) = true → member rs y = false := by
    intro y _ hp
    have hay : a ≤ y ∧ y < b := by simpa using hp
    cases hmem : member rs y with
    | false => rfl
    | true =>
        unfold member at hmem
        rw [List.any_eq_true] at hmem
        rcases hmem with ⟨p, hpm, hpred⟩
        have hbp := hb p hpm
        have hpab : p.1 ≤ y ∧ y < p.2 := by simpa using hpred
        omega
  change ys.countP (fun y => decide (a ≤ y && y < b) || member rs y) =
    ys.countP (fun y => decide (a ≤ y && y < b)) + ys.countP (fun y => member rs y)
  rw [countP_or_of_disjoint _ _ _ hdisj]

private theorem countP_member_eq_seek_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    ys.countP (fun y => member rs y) =
      (rs.flatMap (fun (a, b) => slice ys a b)).length := by
  induction rs with
  | nil => simp [member]
  | cons p rs ih =>
      obtain ⟨a, b⟩ := p
      simp only [List.flatMap_cons, List.length_append]
      have hb : ∀ q ∈ rs, b ≤ q.1 := fun q hq => (List.pairwise_cons.mp hrs).1 q hq
      have hrs' := (List.pairwise_cons.mp hrs).2
      have hrs_wf' : ∀ q ∈ rs, q.1 ≤ q.2 := fun q hq => hrs_wf q (List.mem_cons_of_mem _ hq)
      have hsl : ys.countP (fun y => decide (a ≤ y && y < b)) = (slice ys a b).length := by
        rw [List.countP_eq_length_filter, ← slice_eq_filter hs]
      rw [countP_member_cons_of_ordered hb, ih hrs' hrs_wf', hsl]

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
  have ⟨hrs, hwf⟩ := hord u t x
  rw [fan_eq_seek_length hs]
  unfold seek
  rw [← countP_member_eq_seek_length hs hrs hwf]
  apply List.countP_congr
  intro y hy
  rw [hspec u t x y (valid_lt hv hy)]

private theorem countP_add {α : Type} (p q r : α → Bool) (xs : List α)
    (h : ∀ a ∈ xs, (if p a then 1 else 0) + (if q a then 1 else 0) = if r a then 1 else 0) :
    xs.countP p + xs.countP q = xs.countP r := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      have htail : ∀ b ∈ xs, (if p b then 1 else 0) + (if q b then 1 else 0) = if r b then 1 else 0 :=
        fun b hb => h b (by simp [hb])
      have ha := h a (by simp)
      have hih := ih htail
      rw [List.countP_cons, List.countP_cons, List.countP_cons]
      by_cases hpa : p a = true <;> by_cases hqa : q a = true <;> by_cases hra : r a = true <;>
        simp [hpa, hqa, hra] at ha hih ⊢ <;> omega

/-! ### The annulus search -/

private theorem spec : SpansSpec search := by
  intro u t x y hyu
  change member (optional (x - (2 * t + 1)) (min u (x - t)) ++
      optional (x + t + 1) (min u (x + 2 * t + 2))) y ↔
    (t < x - y + (y - x) && x - y + (y - x) ≤ 2 * t + 1)
  rw [member_append]
  simp only [Bool.or_eq_true]
  rw [member_optional, member_optional]
  simp only [Nat.lt_min, hyu, true_and]
  constructor
  · intro h
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    rcases h with ⟨hlo, hhi⟩ | ⟨hlo, hhi⟩ <;> omega
  · intro h
    simp only [Bool.and_eq_true, decide_eq_true_eq] at h
    by_cases hxy : x ≤ y
    · right
      constructor <;> omega
    · left
      constructor <;> omega

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  exact optional_append_ordered (x - (2 * t + 1)) (x - t) (x + t + 1) (x + 2 * t + 2) u (by omega)

theorem sound_SubtractInnerBand :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.SubtractInnerBand.Goal := by
  intro u t x ys hv
  rw [offsetCount_of_spec spec ordered u t x ys hv]
  have hcount := countP_add (fun y => x ≤ y + t && y ≤ x + t) (search.test t x)
      (fun y => x ≤ y + (2 * t + 1) && y ≤ x + (2 * t + 1)) ys (by
    intro y hy
    simp only [search]
    by_cases hxy : x ≤ y <;>
      by_cases h1 : x ≤ y + t <;> by_cases h2 : y ≤ x + t <;>
      by_cases h3 : t < x - y + (y - x) <;> by_cases h4 : x - y + (y - x) ≤ 2 * t + 1 <;>
      by_cases h5 : x ≤ y + (2 * t + 1) <;> by_cases h6 : y ≤ x + (2 * t + 1) <;>
        simp [h1, h2, h3, h4, h5, h6] <;> omega)
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_SubtractInnerBand
