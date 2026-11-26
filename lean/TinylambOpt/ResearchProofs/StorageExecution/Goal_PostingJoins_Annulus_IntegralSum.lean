import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Annulus.IntegralSum` — Subtraction on a stored prefix-sum column yields the right
  payload sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_IntegralSum

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus
open TinylambOpt.ResearchGoals.Compression.Wire

private def opt (a b : Nat) : Ranges :=
  TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b

private theorem opt_eq (a b : Nat) : opt a b = if a < b then [(a, b)] else [] := rfl

private theorem member_opt_iff {a b y : Nat} :
    member (opt a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · rw [opt_eq, if_pos h]
    simp [member]
  · rw [opt_eq, if_neg h]
    simp [member]
    omega

private theorem member_append (r₁ r₂ : Ranges) (y : Nat) :
    member (r₁ ++ r₂) y = (member r₁ y || member r₂ y) := by
  simp [member, List.any_append]

private theorem opt_ordered (a b : Nat) :
    (opt a b).Pairwise (fun p q => p.2 ≤ q.1) ∧ ∀ p ∈ opt a b, p.1 ≤ p.2 := by
  by_cases h : a < b
  · rw [opt_eq, if_pos h]
    simp [List.pairwise_cons, Nat.le_of_lt h]
  · rw [opt_eq, if_neg h]
    simp

private theorem opt_append_ordered (a b c d u : Nat) (hbc : b ≤ c) :
    (opt a (min u b) ++ opt c (min u d)).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ opt a (min u b) ++ opt c (min u d), p.1 ≤ p.2 := by
  have hminb : min u b ≤ c := Nat.le_trans (Nat.min_le_right u b) hbc
  refine ⟨?_, ?_⟩
  · by_cases h₁ : a < min u b <;> by_cases h₂ : c < min u d <;>
      rw [opt_eq, opt_eq] <;> simp [h₁, h₂, List.pairwise_cons] <;> omega
  · intro p hp
    simp only [List.mem_append] at hp
    rcases hp with hp | hp
    · by_cases h₁ : a < min u b
      · rw [opt_eq, if_pos h₁] at hp
        simp only [List.mem_singleton] at hp
        subst p
        simp_all
        omega
      · rw [opt_eq, if_neg h₁] at hp
        simp at hp
    · by_cases h₂ : c < min u d
      · rw [opt_eq, if_pos h₂] at hp
        simp only [List.mem_singleton] at hp
        subst p
        simp_all
        omega
      · rw [opt_eq, if_neg h₂] at hp
        simp at hp

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
    have hi' : i < lower ys v := by simpa [Nat.min_eq_left hle] using hi
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by omega
    have hlt : ys.getD i 0 < v := lower_lt_of_lt hs v hi'
    rwa [getD_eq_getElem hiys] at hlt
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := by simpa using hi
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

private abbrev inR (a b y : Nat) : Bool := a ≤ y && y < b

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
          have hnil_ab : xs.filter (fun y => inR a b y) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [inR, decide_eq_true_eq, Bool.and_eq_true]
            intro hay
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
    slice ys a b = ys.filter (fun y => inR a b y) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
      lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b,
      take_filter_prefix hs b, filter_lt_prefix_filter_lt hs hab, List.drop_left]
    simp [inR]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp only [inR, decide_eq_true_eq, Bool.and_eq_true]
    intro hay
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

private def SpansSpec (s : Search) : Prop :=
  ∀ u t x y, y < u → (member (s.spans u t x) y ↔ s.test t x y = true)

private def SpansOrdered (s : Search) : Prop :=
  ∀ u t x, (s.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ s.spans u t x, p.1 ≤ p.2

private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (inR a b y || member rs y) := by
  simp [member]

private theorem member_lower_bound {rs : Ranges} {b y : Nat}
    (hb : ∀ p ∈ rs, b ≤ p.1) (hm : member rs y = true) : b ≤ y := by
  unfold member at hm
  rw [List.any_eq_true] at hm
  rcases hm with ⟨p, hpm, hpred⟩
  have hbp := hb p hpm
  simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
  omega

private theorem filter_member_cons_of_ordered {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => inR a b y) ++ ys.filter (fun y => member rs y) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hs' : ys.Pairwise (· ≤ ·) := hs.tail
      have ih' := ih hs'
      by_cases h₁ : inR a b y = true
      · have hnot : ¬ member rs y = true := by
          intro hm
          have hby := member_lower_bound hb hm
          simp only [inR, decide_eq_true_eq, Bool.and_eq_true] at h₁
          omega
        have h₂ : member rs y = false := by simpa [hnot]
        simp [member_cons, h₁, h₂]
        exact ih'
      · by_cases h₂ : member rs y = true
        · have hby : b ≤ y := member_lower_bound hb h₂
          have hnil : ys.filter (fun z => inR a b z) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz
            simp only [inR, decide_eq_true_eq, Bool.and_eq_true]
            intro haz
            have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
            omega
          simpa [member_cons, h₁, h₂, hnil] using ih'
        · simp [member_cons, h₁, h₂]
          exact ih'

private theorem filter_member_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    ys.filter (fun y => member rs y) =
      rs.flatMap (fun (a, b) => ys.filter (fun y => inR a b y)) := by
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

private theorem seek_eq_filter_member {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    rs.flatMap (fun (a, b) => slice ys a b) = ys.filter (fun y => member rs y) := by
  rw [filter_member_eq_flatMap hs hrs hrs_wf]
  congr 1
  funext p
  obtain ⟨a, b⟩ := p
  exact slice_eq_filter hs a b

private theorem integralSum_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : integralSum s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  rw [total_eq_seek_sum hs]
  unfold seek
  obtain ⟨hrs, hwf⟩ := hord u t x
  rw [seek_eq_filter_member hs hrs hwf]
  congr 1
  apply List.filter_congr
  intro y hy
  by_cases hm : member (s.spans u t x) y
  · have ht := (hspec u t x y (valid_lt hv hy)).mp hm
    simp [hm, ht]
  · have ht : s.test t x y ≠ true := by
      intro ht
      exact hm ((hspec u t x y (valid_lt hv hy)).mpr ht)
    simp [hm, ht]




private theorem optional_eq_opt (a b : Nat) :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = opt a b := rfl

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

private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [search]
  rw [optional_eq_opt, optional_eq_opt, member_append]
  simp only [Bool.or_eq_true]
  have hmin1 : y < min u (x - t) ↔ y < x - t := lt_min_iff_of_ltu hyu
  have hmin2 : y < min u (x + 2 * t + 2) ↔ y < x + 2 * t + 2 := lt_min_iff_of_ltu hyu
  constructor
  · intro h
    rcases h with h | h
    · have h' := member_opt_iff.mp h
      have hay : x - (2 * t + 1) ≤ y := h'.1
      have hyb : y < x - t := hmin1.mp h'.2
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      constructor <;> omega
    · have h' := member_opt_iff.mp h
      have hay : x + t + 1 ≤ y := h'.1
      have hyb : y < x + 2 * t + 2 := hmin2.mp h'.2
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      constructor <;> omega
  · intro ht
    simp only [Bool.and_eq_true, decide_eq_true_eq] at ht
    by_cases hyx : y < x
    · left
      rw [member_opt_iff]
      constructor
      · omega
      · exact hmin1.mpr (by omega)
    · right
      rw [member_opt_iff]
      constructor
      · omega
      · exact hmin2.mpr (by omega)

private theorem ordered : SpansOrdered search := by
  intro u t x
  simp only [search]
  rw [optional_eq_opt, optional_eq_opt]
  exact opt_append_ordered (x - (2 * t + 1)) (x - t) (x + t + 1) (x + 2 * t + 2) u (by omega)

theorem sound_IntegralSum :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.IntegralSum.Goal :=
  integralSum_of_spec spec ordered


end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_IntegralSum
