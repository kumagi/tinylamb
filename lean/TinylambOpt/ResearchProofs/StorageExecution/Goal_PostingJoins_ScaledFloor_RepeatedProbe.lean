import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_RepeatedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor
open TinylambOpt.ResearchGoals.Compression.Wire

private abbrev sfsearch : Search := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor.search
private abbrev opt (a b : Nat) : Ranges := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b

private theorem opt_eq (a b : Nat) : opt a b = if a < b then [(a, b)] else [] := rfl

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


private theorem valid_pairwise' {u : Nat} {ys : List Nat} (hv : valid u ys) : ys.Pairwise (· ≤ ·) := hv.1
private theorem valid_lt' {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat} (hy : y ∈ ys) : y < u := hv.2 y hy

private theorem filter_lt_split' {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++ ys.filter (fun x => decide (a ≤ x ∧ x < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' : xs.Pairwise (· ≤ ·) := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            rw [List.filter_eq_nil_iff]
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnil, ih hs']
        · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
            rw [List.filter_eq_nil_iff]
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnil_ab : xs.filter (fun y => decide (a ≤ y ∧ y < b)) = [] := by
            rw [List.filter_eq_nil_iff]
            intro y hy
            simp only [decide_eq_true_eq]
            intro hmid
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem lt_min_iff_of_ltu {u b y : Nat} (hyu : y < u) :
    y < min u b ↔ y < b := by
  constructor
  · intro h
    exact Nat.lt_of_lt_of_le h (Nat.min_le_right u b)
  · intro h
    by_cases hb : b ≤ u
    · rw [Nat.min_eq_right hb]
      exact h
    · rw [Nat.min_eq_left (by omega)]
      exact hyu

private theorem fan_eq_filter {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan sfsearch u t x ys = (ys.filter (sfsearch.test t x)).length := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  have hxpos : 0 < x + 1 := by omega
  unfold fan
  rw [show sfsearch.spans u t x = opt (t * (x + 1)) (min u ((t + 1) * (x + 1))) from rfl]
  by_cases h : t * (x + 1) < min u ((t + 1) * (x + 1))
  · rw [opt_eq, if_pos h]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    have hle : t * (x + 1) ≤ min u ((t + 1) * (x + 1)) := Nat.le_of_lt h
    have hsplit := filter_lt_split' hs hle
    have hla := lower_eq_filter_lt_length hs (t * (x + 1))
    have hlb := lower_eq_filter_lt_length hs (min u ((t + 1) * (x + 1)))
    have hlen_eq : (ys.filter (fun y => decide (y < min u ((t + 1) * (x + 1))))).length =
        (ys.filter (fun y => decide (y < t * (x + 1)))).length +
        (ys.filter (fun y => decide (t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1))))).length := by
      rw [hsplit, List.length_append]
    rw [← hlb, ← hla] at hlen_eq
    have hlen : lower ys (min u ((t + 1) * (x + 1))) - lower ys (t * (x + 1)) =
        (ys.filter (fun y => decide (t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1))))).length := by
      omega
    rw [hlen]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    have hyu : y < u := hv.2 y hy
    have hyb : y < min u ((t + 1) * (x + 1)) ↔ y < (t + 1) * (x + 1) :=
      lt_min_iff_of_ltu hyu
    have hiff : (t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1))) ↔
        sfsearch.test t x y = true := by
      constructor
      · intro hmem
        have hle' : t ≤ y / (x + 1) := (Nat.le_div_iff_mul_le hxpos).mpr hmem.1
        have hlt' : y / (x + 1) < t + 1 := (Nat.div_lt_iff_lt_mul hxpos).mpr (hyb.mp hmem.2)
        have hdiv : y / (x + 1) = t := by omega
        change (y / (x + 1) == t) = true
        simp [hdiv]
      · intro ht
        have htdiv : y / (x + 1) = t := by
          change (y / (x + 1) == t) = true at ht
          simpa [beq_iff_eq] using ht
        constructor
        · rw [← htdiv]
          exact Nat.div_mul_le_self y (x + 1)
        · apply hyb.mpr
          have hybound : y < y / (x + 1) * (x + 1) + (x + 1) :=
            Nat.lt_div_mul_add hxpos
          rw [htdiv] at hybound
          simpa [Nat.add_mul, Nat.one_mul] using hybound
    by_cases hp' : t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1))
    · have ht' : sfsearch.test t x y = true := hiff.mp hp'
      simp [decide_eq_true_eq.mpr hp', ht']
    · have ht' : sfsearch.test t x y = false := by
        cases hval : sfsearch.test t x y with
        | false => rfl
        | true =>
            exfalso
            exact hp' (hiff.mpr hval)
      simp [decide_eq_false_iff_not.mpr hp', ht']
  · rw [opt_eq, if_neg h]
    simp only [List.map_nil, List.sum_nil]
    symm
    rw [List.length_eq_zero_iff]
    rw [List.filter_eq_nil_iff]
    intro y hy
    have hyu : y < u := hv.2 y hy
    cases hty : sfsearch.test t x y with
    | false => simp
    | true =>
      exfalso
      have htdiv : y / (x + 1) = t := by
        change (y / (x + 1) == t) = true at hty
        simpa [beq_iff_eq] using hty
      have ha : t * (x + 1) ≤ y := by
        rw [← htdiv]
        exact Nat.div_mul_le_self y (x + 1)
      have hb : y < (t + 1) * (x + 1) := by
        have hybound : y < y / (x + 1) * (x + 1) + (x + 1) :=
          Nat.lt_div_mul_add hxpos
        rw [htdiv] at hybound
        simpa [Nat.add_mul, Nat.one_mul] using hybound
      have hyb : y < min u ((t + 1) * (x + 1)) := (lt_min_iff_of_ltu hyu).mpr hb
      omega

private theorem sum_zero_map {α : Type u} (xs : List α) (f : α → Nat)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons]
      rw [h x List.mem_cons_self]
      rw [ih (fun y hy => h y (List.mem_cons_of_mem _ hy))]

private theorem sum_map_add_nat {α : Type u} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem beq_comm' {α : Type u} [BEq α] [LawfulBEq α] {a b : α} :
    (a == b) = (b == a) := by
  by_cases h : a == b
  · have hab : a = b := beq_iff_eq.mp h
    simp [h, hab]
  · have hne : a ≠ b := by intro heq; apply h; simp [heq]
    have h' : (b == a) = false := by
      exact beq_eq_false_iff_ne.mpr (Ne.symm hne)
    simp [h, h']

private theorem sum_eq_indicator_nodup {α : Type u} [DecidableEq α] [LawfulBEq α]
    (ds : List α) (a : α) (f : α → Nat) (hnd : ds.Nodup) (ha : a ∈ ds) :
    (ds.map (fun d => if d == a then f d else 0)).sum = f a := by
  induction ds with
  | nil => cases ha
  | cons d ds ih =>
      rcases List.mem_cons.mp ha with rfl | ha
      · have hnot : a ∉ ds := (List.nodup_cons.mp hnd).1
        have hzero : (ds.map (fun x => if x == a then f x else 0)).sum = 0 := by
          apply sum_zero_map
          intro x hx
          have hne : x ≠ a := by intro heq; apply hnot; simpa [heq] using hx
          simp [beq_eq_false_iff_ne.mpr hne]
        simp only [List.map_cons, List.sum_cons]
        rw [hzero]
        simp only [beq_self_eq_true, ↓reduceIte, Nat.add_zero]
      · have hne : d ≠ a := by
          intro heq
          apply (List.nodup_cons.mp hnd).1
          simpa [heq] using ha
        simp only [List.map_cons, List.sum_cons]
        have hhead : (if d == a then f d else 0) = 0 := by
          simp [beq_eq_false_iff_ne.mpr hne]
        rw [hhead, Nat.zero_add]
        exact ih (List.nodup_cons.mp hnd).2 ha

private theorem sum_eq_indicator_zero {α : Type u} [DecidableEq α] [LawfulBEq α]
    (ds : List α) (a : α) (f : α → Nat) (ha : a ∉ ds) :
    (ds.map (fun d => if d == a then f d else 0)).sum = 0 := by
  induction ds with
  | nil => rfl
  | cons d ds ih =>
      have hne : d ≠ a := by intro heq; apply ha; simp [heq]
      have hatail : a ∉ ds := by intro hmem; apply ha; simp [hmem]
      simp only [List.map_cons, List.sum_cons]
      have hhead : (if d == a then f d else 0) = 0 := by
        simp [beq_eq_false_iff_ne.mpr hne]
      rw [hhead, Nat.zero_add]
      exact ih hatail

private theorem eraseDups_nodup {α : Type u} [DecidableEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  induction h : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
      cases xs with
      | nil => simp [List.eraseDups]
      | cons a xs =>
          have hn : n = (a :: xs).length := by simpa using h.symm
          rw [List.eraseDups_cons]
          rw [List.nodup_cons]
          constructor
          · simp [List.mem_eraseDups]
          · apply ih (xs.filter (fun b => !b == a)).length
            · rw [hn]
              exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == a) xs)
            · rfl

private theorem sum_count_of_nodup_subset {α : Type u} [DecidableEq α] [LawfulBEq α]
    (ds xs : List α) (f : α → Nat) (hnd : ds.Nodup) :
    (ds.map (fun d => xs.count d * f d)).sum =
      ((xs.filter (fun x => x ∈ ds)).map f).sum := by
  induction xs with
  | nil =>
      simp only [List.count_nil, Nat.zero_mul, List.filter_nil, List.map_nil, List.sum_nil]
      apply sum_zero_map
      intro x hx
      rfl
  | cons a xs ih =>
      have hcount : ∀ d : α, List.count d (a :: xs) =
          List.count d xs + (if d == a then 1 else 0) := by
        intro d
        rw [List.count_cons]
        by_cases h : d == a
        · have h' : (a == d) = true := by
            have hd : d = a := beq_iff_eq.mp h
            simp [hd]
          simp [h, h']
        · have h' : (a == d) = false := by
            rw [beq_comm']
            simpa using h
          simp [h, h']
      by_cases ha : a ∈ ds
      · have hind := sum_eq_indicator_nodup ds a f hnd ha
        have hstep : (ds.map (fun d => List.count d (a :: xs) * f d)).sum =
            (ds.map (fun d => List.count d xs * f d)).sum + f a := by
          rw [show (ds.map (fun d => List.count d (a :: xs) * f d)) =
              ds.map (fun d => (List.count d xs + (if d == a then 1 else 0)) * f d) from by
            apply List.map_congr_left
            intro d hd
            rw [hcount d]]
          rw [show (ds.map (fun d => (List.count d xs + (if d == a then 1 else 0)) * f d)) =
              ds.map (fun d => List.count d xs * f d + (if d == a then f d else 0)) from by
            apply List.map_congr_left
            intro d hd
            by_cases h : d == a
            · simp [h, Nat.add_mul, Nat.one_mul]
            · simp [h]]
          rw [sum_map_add_nat, hind]
        rw [hstep, ih]
        simp [decide_eq_true_eq.mpr ha]
        omega
      · have hind := sum_eq_indicator_zero ds a f ha
        have hstep : (ds.map (fun d => List.count d (a :: xs) * f d)).sum =
            (ds.map (fun d => List.count d xs * f d)).sum := by
          rw [show (ds.map (fun d => List.count d (a :: xs) * f d)) =
              ds.map (fun d => (List.count d xs + (if d == a then 1 else 0)) * f d) from by
            apply List.map_congr_left
            intro d hd
            rw [hcount d]]
          rw [show (ds.map (fun d => (List.count d xs + (if d == a then 1 else 0)) * f d)) =
              ds.map (fun d => List.count d xs * f d + (if d == a then f d else 0)) from by
            apply List.map_congr_left
            intro d hd
            by_cases h : d == a
            · simp [h, Nat.add_mul, Nat.one_mul]
            · simp [h]]
          rw [sum_map_add_nat, hind, Nat.add_zero]
        rw [hstep, ih]
        simp [ha]

private theorem sum_map_eraseDups_count_mul {α : Type u} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  have hnd : xs.eraseDups.Nodup := eraseDups_nodup xs
  have h := sum_count_of_nodup_subset xs.eraseDups xs f hnd
  have hfilt : xs.filter (fun x => x ∈ xs.eraseDups) = xs := by
    rw [List.filter_eq_self]
    intro x hx
    exact decide_eq_true_eq.mpr (List.mem_eraseDups.mpr hx)
  rw [hfilt] at h
  exact h

theorem sound_RepeatedProbe : ScaledFloor.RepeatedProbe.Goal := by
  intro u t xs ys hv
  have hfan : ∀ x, fan sfsearch u t x ys = (ys.filter (sfsearch.test t x)).length := by
    intro x
    exact fan_eq_filter hv
  rw [show (xs.flatMap (fun x => ys.filter (sfsearch.test t x))).length =
      (xs.map (fun x => (ys.filter (sfsearch.test t x)).length)).sum from by
    rw [List.length_flatMap]]
  have hmap : (xs.map (fun x => (ys.filter (sfsearch.test t x)).length)).sum =
      (xs.map (fun x => fan sfsearch u t x ys)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact (hfan x).symm
  rw [hmap]
  exact sum_map_eraseDups_count_mul xs (fun x => fan sfsearch u t x ys)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_RepeatedProbe
