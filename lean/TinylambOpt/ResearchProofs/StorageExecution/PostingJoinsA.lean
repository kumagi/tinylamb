import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Certificates for the first five posting-join families.

The lower-bound and ordered-slice infrastructure is kept local so this shard
is independent of the other posting-join proof modules.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.PostingJoinsA

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

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
  · simp [optional_eq, h]
    omega

private theorem member_append (r₁ r₂ : Ranges) (y : Nat) :
    member (r₁ ++ r₂) y = (member r₁ y || member r₂ y) := by
  simp [member, List.any_append]

private theorem optional_ordered (a b : Nat) :
    (optional a b).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ optional a b, p.1 ≤ p.2 := by
  by_cases h : a < b
  · simp [optional_eq, h]
    omega
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
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo ≤ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
    have heq : lo = hi := by omega
    subst hi
    rw [searchBound]
    exact ⟨⟨hbelow, fun j hj hjlen => habove j (Nat.le_refl lo) hjlen⟩,
      Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
    by_cases hge : lo ≥ hi
    · have heq : lo = hi := by omega
      subst hi
      rw [searchBound, if_pos hge]
      exact ⟨⟨hbelow, fun j hj hjlen => habove j (Nat.le_refl lo) hjlen⟩,
        Nat.le_refl lo, Nat.le_refl lo⟩
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
            rw [hlistval, getD_eq_getElem hmidlen]
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
        have hres := ih (mid + 1) hi (by omega) (by omega) (by omega) hbelow' habove
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, Nat.le_trans (by omega : lo ≤ mid + 1) hlb, hub⟩
      · rw [if_neg hv]
        have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
          intro j hj hjlen
          by_cases heq : j = mid
          · subst j
            rw [hlistval, getD_eq_getElem hmidlen]
            omega
          · have hmj : mid < j := by omega
            by_cases hjh : hi ≤ j
            · exact habove j hjh hjlen
            · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                rw [hlistval, getD_eq_getElem hjlen]
                exact hs.rel_getElem_of_lt hmidlen hjlen hmj
              omega
        have hres := ih lo mid hml (by omega) (by omega) hbelow habove'
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 ≤ width ys.length + 1 := by
    have := width_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen; omega)
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
      | zero => exact ⟨Nat.zero_lt_succ _, hp⟩
      | succ i =>
        have htail := ih i (by omega)
        exact ⟨by omega, by simpa using htail.2⟩
    · simp [hp] at hi
      have htail := ih i hi
      exact ⟨by omega, by simpa using htail.2⟩

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hnl : (ys.filter (fun x => decide (x < v))).length < lower ys v := h
    have hnlen : (ys.filter (fun x => decide (x < v))).length < ys.length :=
      Nat.lt_of_lt_of_le hnl (lower_le_length hs v)
    have hlow := lower_lt_of_lt hs v hnl
    have hp := (filter_prefix (fun x => decide (x < v)) ys
      (ys.filter (fun x => decide (x < v))).length hnl).2
    rw [getD_eq_getElem hnlen] at hlow
    simp only [decide_eq_true_eq] at hp
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hln : lower ys v < (ys.filter (fun x => decide (x < v))).length := h
    have hlen : lower ys v < ys.length :=
      Nat.lt_of_lt_of_le hln (List.length_filter_le _ _)
    have hlow := lower_lt_of_lt hs v hln
    have hp := (filter_prefix (fun x => decide (x < v)) ys (lower ys v) hln).2
    rw [getD_eq_getElem hlen] at hlow
    simp only [decide_eq_true_eq] at hp
    omega

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
          simp only [decide_eq_true_eq, Bool.and_eq_true]
          intro hay _
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
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    intro hay _
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

private theorem countP_or_of_disjoint {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    xs.countP (fun x => p x || q x) = xs.countP p + xs.countP q := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter,
    List.countP_eq_length_filter, filter_or_append p q xs hdisj, List.length_append]

private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (decide (a ≤ y && y < b) || member rs y) := by
  simp [member]

private theorem filter_or_append {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    xs.filter (fun x => p x || q x) = xs.filter p ++ xs.filter q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have htail : ∀ y ∈ xs, p y = true → q y = false :=
      fun y hy => hdisj y (by simp [hy])
    by_cases hp : p x = true
    · have hq := hdisj x (by simp) hp
      simp [hp, hq, ih htail]
    · by_cases hq : q x = true
      · simp [hp, hq, ih htail]
      · simp [hp, hq, ih htail]

private theorem filter_member_cons_of_ordered {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => decide (a ≤ y && y < b)) ++ ys.filter (fun y => member rs y) := by
  have hdisj : ∀ y ∈ ys, (decide (a ≤ y && y < b)) = true → member rs y = false := by
    intro y _ hp hm
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hp
    rw [List.any_eq_true] at hm
    rcases hm with ⟨p, hpm, hpred⟩
    have hbp := hb p hpm
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
    omega
  rw [member_cons]
  exact filter_or_append _ _ _ hdisj

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
    rw [filter_member_cons_of_ordered hs hb hrs', ih hrs' hrs_wf']
    congr 1
    funext p
    obtain ⟨a, b⟩ := p
    exact slice_eq_filter hs a b

private theorem seek_eq_filter_member {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    rs.flatMap (fun (a, b) => slice ys a b) = ys.filter (fun y => member rs y) := by
  rw [filter_member_eq_flatMap hs hrs hwf]
  congr 1
  funext p
  obtain ⟨a, b⟩ := p
  exact slice_eq_filter hs a b

private theorem orderedProbe_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : orderedProbe s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  have ⟨hrs, hwf⟩ := hord u t x
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
  calc
    ys.take q = (ys.take q).take p ++ (ys.take q).drop p :=
      (List.take_append_drop p (ys.take q)).symm
    _ = ys.take p ++ (ys.drop p).take (q - p) := by
      rw [List.take_take, Nat.min_eq_left hpq, List.drop_take]
    _ = ys.take p ++ (ys.drop p).take (q - p) := rfl

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
    set rhs := (ys.filterMap id).filter (s.test t v) with hrhs
    by_cases h : rhs.isEmpty
    · have hlen : rhs.length = 0 := by simpa using h
      rw [if_pos h]
      rw [← List.countP_eq_length_filter] at hcount
      simp [hcount, hlen]
    · rw [if_neg h]
      have hlen : 0 < rhs.length := by
        by_contra hc
        have hz : rhs.length = 0 := by omega
        exact h (by simpa using hz)
      rw [← List.countP_eq_length_filter] at hcount
      have hmax : max 1 rhs.length = rhs.length := Nat.max_eq_right (by omega)
      simp [hcount, hmax]

private theorem eraseDups_nodup {α : Type} [DecidableEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
    rw [List.eraseDups_cons, List.nodup_cons]
    constructor
    · intro ha
      have hf := List.mem_eraseDups.mp ha
      have hm := List.mem_filter.mp hf
      simp [hm.2]
    · exact ih

private theorem sum_map_perm {α : Type} (f : α → Nat) {l₁ l₂ : List α}
    (h : l₁.Perm l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc]

private theorem sum_zero_map {α : Type} (xs : List α) (f : α → Nat)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have hx := h x (by simp)
    have ht := ih (fun y hy => h y (by simp [hy]))
    simp [hx, ht]

private theorem sum_indicator_nodup {α : Type} [DecidableEq α] (xs : List α)
    (hnd : xs.Nodup) (a : α) (v : Nat) (ha : a ∈ xs) :
    (xs.map (fun x => if x = a then v else 0)).sum = v := by
  induction xs with
  | nil => simp at ha
  | cons b bs ih =>
    have hnd' := List.nodup_cons.mp hnd
    by_cases hba : b = a
    · subst b
      have hz := sum_zero_map bs (fun x => if x = a then v else 0)
        (fun x hx => by
          have hxa : x ≠ a := by
            intro heq
            subst x
            exact hnd'.1 hx
          simp [hxa])
      simp [hz]
    · have hmem : a ∈ bs := by simpa [hba] using ha
      simp [hba, ih hnd'.2 hmem]

private theorem sum_beq_indicator_nodup {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (hnd : xs.Nodup) (a : α) (v : Nat) (ha : a ∈ xs) :
    (xs.map (fun x => if a == x then v else 0)).sum = v := by
  have h := sum_indicator_nodup xs hnd a v ha
  convert h using 1
  apply List.map_congr_left
  intro x hx
  by_cases hxa : x = a <;> simp [hxa, eq_comm]

private theorem count_cons_add {α : Type} [DecidableEq α] [LawfulBEq α]
    (a : α) (xs : List α) (x : α) :
    (a :: xs).count x = xs.count x + if x = a then 1 else 0 := by
  by_cases hxa : x = a
  · subst x
    rw [List.count_cons_self]
  · rw [List.count_cons_of_ne (Ne.symm hxa)]
    simp [hxa]

private theorem sum_map_eraseDups_count_mul {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
    let ds := (a :: xs).eraseDups
    have hnd : ds.Nodup := eraseDups_nodup _
    have ha : a ∈ ds := by simp [ds, List.mem_eraseDups]
    have hstep : (ds.map (fun x => (a :: xs).count x * f x)).sum =
        (ds.map (fun x => xs.count x * f x)).sum + f a := by
      rw [show ds.map (fun x => (a :: xs).count x * f x) =
          ds.map (fun x => xs.count x * f x + (if x = a then f a else 0)) from by
        apply List.map_congr_left
        intro x hx
        rw [count_cons_add]
        by_cases hxa : x = a <;> simp [hxa, Nat.add_mul]]
      rw [sum_map_add_nat, sum_indicator_nodup ds hnd a (f a) ha]
    have hperm : ds.Perm xs.eraseDups := by
      apply List.perm_ext_iff_of_nodup hnd (eraseDups_nodup xs) |>.mpr
      intro x
      simp [ds, List.mem_eraseDups]
    have hsum := sum_map_perm (fun x => xs.count x * f x) hperm
    change (ds.map (fun x => (a :: xs).count x * f x)).sum = _
    rw [hstep, hsum, ih]
    simp [Nat.add_comm]

private theorem repeatedProbe_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : repeatedProbe s := by
  intro u t xs ys hv
  have hs := valid_pairwise hv
  have hfan : ∀ x, fan s u t x ys = (ys.filter (s.test t x)).length := by
    intro x
    rw [fan_eq_seek_length hs, orderedProbe_of_spec hspec hord u t x ys hv,
      List.countP_eq_length_filter]
  simp_rw [hfan]
  rw [List.length_flatMap]
  exact sum_map_eraseDups_count_mul xs (fun x => (ys.filter (s.test t x)).length)

private theorem nth_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (rs : Ranges) (k : Nat) :
    nth ys rs k = (rs.flatMap (fun (a, b) => slice ys a b))[k]? := by
  induction rs generalizing k with
  | nil => simp [nth]
  | cons p rs ih =>
    obtain ⟨a, b⟩ := p
    simp only [nth, List.flatMap_cons, List.getElem?_append]
    set start := lower ys a
    set n := lower ys b - start
    have hslen : (slice ys a b).length = n := by rw [slice_length hs]
    by_cases hk : k < n
    · rw [if_pos hk]
      have hk' : k < (slice ys a b).length := by omega
      rw [dif_pos hk']
      unfold slice
      simp only [List.getElem?_take, List.getElem?_drop]
      rw [if_pos hk]
      congr 1
    · rw [if_neg hk]
      have hk' : ¬ k < (slice ys a b).length := by omega
      rw [dif_neg hk']
      simp [hslen, ih (k - n)]

private theorem kthMatch_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : kthMatch s := by
  intro u t x ys k hv
  have hs := valid_pairwise hv
  have ⟨hrs, hwf⟩ := hord u t x
  rw [nth_eq_flatMap hs (s.spans u t x) k, seek_eq_filter_member hs hrs hwf]
  congr 1
  apply List.filter_congr
  intro y hy
  have h := hspec u t x y (valid_lt hv hy)
  cases hm : member (s.spans u t x) y <;> cases ht : s.test t x y <;> simp_all

private theorem pruneExtent_of_spec {s : Search} (hspec : SpansSpec s) : pruneExtent s := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  have hyu := valid_lt hv hy
  have ⟨hlo, hhi⟩ := hys y hy
  have hmem : member (s.spans u t x) y = false := by
    unfold member
    rw [Bool.eq_false_iff, ne_eq, List.any_eq_true]
    push_neg
    intro p hp hp1
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hp1
    have := hspan p hp
    omega
  have htest : s.test t x y = false := by
    cases ht : s.test t x y
    · rfl
    · exfalso
      have hm := (hspec u t x y hyu).mpr ht
      rw [hmem] at hm
      simp at hm
  simp [htest]

private theorem filter_not_perm_filter {α : Type} (p : α → Bool) (xs : List α) :
    (xs.filter (fun x => !p x) ++ xs.filter p).Perm xs := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
    by_cases hp : p a = true
    · simp only [List.filter_cons, hp, Bool.not_true, Bool.false_eq_true, ↓reduceIte]
      have hswap : (xs.filter (fun x => !p x) ++ a :: xs.filter p).Perm
          (a :: (xs.filter (fun x => !p x) ++ xs.filter p)) := by
        change (xs.filter (fun x => !p x) ++ [a] ++ xs.filter p).Perm _
        simpa only [List.cons_append] using
          (List.perm_append_comm (l₁ := xs.filter (fun x => !p x)) (l₂ := [a])).append_right
            (xs.filter p)
      exact hswap.trans (List.Perm.cons a ih)
    · simp only [List.filter_cons, hp, Bool.not_false, ↓reduceIte, List.cons_append]
      exact List.Perm.cons a ih

private theorem countP_zipIdx (ys : List Nat) (p : Nat → Bool) :
    ys.countP p = ys.zipIdx.countP (fun q => p q.1) := by
  induction ys generalizing 0 with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
    rw [List.zipIdx_cons, List.countP_cons, List.countP_cons]
    simp only [Function.comp_def]
    rw [ih 1]
    by_cases hp : p y <;> simp [hp]

private theorem zipIdx_filter_snd (ys : List Nat) (ds : List Nat) (p : Nat → Bool) (off : Nat) :
    ((ys.zipIdx off).filter (fun q => ds.contains q.2 && p q.1)).map Prod.snd =
      ((List.range ys.length).filter
        (fun j => ds.contains (off + j) && p (ys.getD j 0))).map (fun j => off + j) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
    simp only [List.zipIdx_cons, List.filter_cons, List.map_cons, List.range_succ,
      List.getD_cons_zero, Nat.zero_add]
    cases h : ds.contains off && p y
    · simp only [h, Bool.false_eq_true, ↓reduceIte]
      rw [ih (off + 1)]
      congr 1
      apply List.filter_congr
      intro j hj
      simp only [List.getD_cons_succ]
      have heq : off + 1 + j = off + (j + 1) := by omega
      rw [heq]
    · simp only [h, ↓reduceIte]
      rw [ih (off + 1)]
      congr 1
      apply List.filter_congr
      intro j hj
      simp only [List.getD_cons_succ]
      have heq : off + 1 + j = off + (j + 1) := by omega
      rw [heq]

private theorem filter_mem_length_eq (xs ys : List Nat) (p : Nat → Bool)
    (hxs : xs.Nodup) (hys : ys.Nodup) :
    (xs.filter (fun i => decide (i ∈ ys) && p i)).length =
      (ys.filter (fun i => decide (i ∈ xs) && p i)).length := by
  have hm : (xs.filter (fun i => decide (i ∈ ys))).Perm
      (ys.filter (fun i => decide (i ∈ xs))) := by
    apply List.perm_ext_iff_of_nodup (hxs.filter _) (hys.filter _) |>.mpr
    intro i
    simp only [List.mem_filter]
    exact ⟨fun h => ⟨h.2, h.1⟩, fun h => ⟨h.2, h.1⟩⟩
  have hp := hm.filter p
  have hl : xs.filter (fun i => decide (i ∈ ys) && p i) =
      (xs.filter (fun i => decide (i ∈ ys))).filter p := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    by_cases hmem : i ∈ ys <;> by_cases hpi : p i <;> simp [hmem, hpi]
  have hr : ys.filter (fun i => decide (i ∈ xs) && p i) =
      (ys.filter (fun i => decide (i ∈ xs))).filter p := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    by_cases hmem : i ∈ xs <;> by_cases hpi : p i <;> simp [hmem, hpi]
  rw [hl, hr]
  exact hp.length_eq

private theorem deleted_count_eq (ys : List Nat) (ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).countP (fun q => p q.1) := by
  rw [List.countP_eq_length_filter]
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases h : i < ys.length
    · simp [h, List.getElem?_eq_getElem h, getD_eq_getElem h]
    · simp [h, List.getElem?_eq_none (by omega : ys.length ≤ i)]
  rw [hlhs]
  have hlen := filter_mem_length_eq ds.eraseDups (List.range ys.length)
    (fun i => p (ys.getD i 0)) (eraseDups_nodup ds) List.nodup_range
  have hmap := zipIdx_filter_snd ys ds p 0
  simp only [Nat.zero_add] at hmap
  rw [show (fun i => ds.contains i && p (ys.getD i 0)) =
      (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0)) from by
    funext i
    simp [List.contains_eq_mem]] at hmap
  calc
    (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).length =
        ((List.range ys.length).filter
          (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0))).length := by
      simpa [List.mem_range] using hlen
    _ = ((ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).map Prod.snd).length := by
      rw [hmap]
    _ = (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).length := List.length_map _
    _ = (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).countP (fun q => p q.1) :=
      List.countP_eq_length_filter.symm

private theorem maskedJoin_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : maskedJoin s := by
  intro u t x ys ds hv
  rw [offsetCount_of_spec hspec hord u t x ys hv]
  set pred := s.test t x with hpred
  set keep := ys.zipIdx.filter (fun q => !ds.contains q.2) with hkeep
  set del := ys.zipIdx.filter (fun q => ds.contains q.2) with hdel
  have hperm : (keep ++ del).Perm ys.zipIdx := by
    simpa only [hkeep, hdel] using filter_not_perm_filter (fun q => ds.contains q.2) ys.zipIdx
  have hzip : ys.zipIdx.countP (fun q => pred q.1) =
      keep.countP (fun q => pred q.1) + del.countP (fun q => pred q.1) := by
    rw [← hperm.countP_eq (fun q => pred q.1), List.countP_append]
  have hsurv : ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP pred =
      keep.countP (fun q => pred q.1) := by
    rw [List.countP_map]
    rfl
  have hdeleted := deleted_count_eq ys ds pred
  rw [countP_zipIdx ys pred, hzip, hsurv]
  change ys.countP pred - ds.eraseDups.countP (fun i => ys[i]?.any pred) = _
  rw [hdeleted]
  omega

/-! ## Following -/

namespace Following

private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [search, member_optional]
  constructor
  · rintro ⟨h1, h2⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    omega
  · intro ht
    simp only [Bool.and_eq_true, decide_eq_true_eq] at ht
    exact ⟨by omega, Nat.lt_min.mpr ⟨hyu, by omega⟩⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  simpa only [search] using optional_ordered (x + 1) (min u (x + t + 1))

theorem sound_OrderedProbe : OrderedProbe.Goal := orderedProbe_of_spec spec ordered
theorem sound_OffsetCount : OffsetCount.Goal := offsetCount_of_spec spec ordered
theorem sound_IntegralSum : IntegralSum.Goal := integralSum_of_spec spec ordered
theorem sound_LeftMultiplicity : LeftMultiplicity.Goal := leftMultiplicity_of_spec spec ordered
theorem sound_RepeatedProbe : RepeatedProbe.Goal := repeatedProbe_of_spec spec ordered
theorem sound_KthMatch : KthMatch.Goal := kthMatch_of_spec spec ordered
theorem sound_PruneExtent : PruneExtent.Goal := pruneExtent_of_spec spec
theorem sound_MaskedJoin : MaskedJoin.Goal := maskedJoin_of_spec spec ordered

theorem sound_ZeroLookahead : ZeroLookahead.Goal := by
  intro u x
  simp [search, optional_eq]

theorem sound_SlidingOffsets : SlidingOffsets.Goal := by
  intro u t x ys hv
  have h1 := offsetCount_of_spec spec ordered u t (x + 1) ys hv
  have h2 := offsetCount_of_spec spec ordered u t x ys hv
  rw [h1, h2]
  have hdleft : ∀ y ∈ ys,
      (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) = true →
        (decide (y == x + 1)) = false := by
    intro y hy h1 h2
    simp only [Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at h1 h2
    omega
  have hsplitleft : ∀ y,
      ((decide (x + 1 < y) && decide (y ≤ x + 1 + t)) || decide (y == x + 1)) =
        (decide (x < y) && decide (y ≤ x + 1 + t)) := by
    intro y
    by_cases h1 : y = x + 1 <;> by_cases h2 : x + 1 < y <;>
      by_cases h3 : y ≤ x + 1 + t <;> by_cases h4 : x < y <;>
        simp_all <;> omega
  have hleft : ys.countP (fun y => decide (x + 1 < y) && decide (y ≤ x + 1 + t)) +
      ys.count (x + 1) =
      ys.countP (fun y => decide (x < y) && decide (y ≤ x + 1 + t)) := by
    rw [show ys.count (x + 1) = ys.countP (fun y => decide (y == x + 1)) from by
      rw [List.count_eq_length_filter, List.countP_eq_length_filter]]
    rw [← show ys.countP (fun y => decide (x + 1 < y) && decide (y ≤ x + 1 + t)) +
        ys.countP (fun y => decide (y == x + 1)) =
        ys.countP (fun y => (decide (x + 1 < y) && decide (y ≤ x + 1 + t)) ||
          decide (y == x + 1)) from by
      symm
      exact countP_or_of_disjoint _ _ _ hdleft]
    apply List.countP_congr
    intro y hy
    rw [hsplitleft]
  have hdright : ∀ y ∈ ys,
      (decide (x < y) && decide (y ≤ x + t)) = true →
        (decide (y == x + t + 1)) = false := by
    intro y hy h1 h2
    simp only [Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at h1 h2
    omega
  have hsplitright : ∀ y,
      ((decide (x < y) && decide (y ≤ x + t)) || decide (y == x + t + 1)) =
        (decide (x < y) && decide (y ≤ x + 1 + t)) := by
    intro y
    by_cases h1 : y = x + t + 1 <;> by_cases h2 : y ≤ x + t <;>
      by_cases h3 : x < y <;> by_cases h4 : y ≤ x + 1 + t <;>
        simp_all <;> omega
  have hright : ys.countP (fun y => decide (x < y) && decide (y ≤ x + t)) +
      ys.count (x + t + 1) =
      ys.countP (fun y => decide (x < y) && decide (y ≤ x + 1 + t)) := by
    rw [show ys.count (x + t + 1) = ys.countP (fun y => decide (y == x + t + 1)) from by
      rw [List.count_eq_length_filter, List.countP_eq_length_filter]]
    rw [← show ys.countP (fun y => decide (x < y) && decide (y ≤ x + t)) +
        ys.countP (fun y => decide (y == x + t + 1)) =
        ys.countP (fun y => (decide (x < y) && decide (y ≤ x + t)) ||
          decide (y == x + t + 1)) from by
      symm
      exact countP_or_of_disjoint _ _ _ hdright]
    apply List.countP_congr
    intro y hy
    rw [hsplitright]
  omega

end Following

end TinylambOpt.ResearchProofs.StorageExecution.PostingJoinsA
