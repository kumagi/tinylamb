import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Annulus.MaskedJoin` — A point-delete bitmap on the right can subtract removed
  multiplicities from the offset count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_MaskedJoin

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus

/-! Self-contained certificate for `Annulus.MaskedJoin`.

The binary lower bound on the sorted right key column cuts exactly at the
filter boundary, so the two-offset fan equals the number of matching keys.
Deleting right positions by index subtracts exactly the matching deleted
positions: `eraseDups` makes the deleted-index set a set, while `zipIdx`
gives every right position a unique index. -/

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
      constructor
      · exact ⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩
      · constructor <;> omega
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        change Cut xs.toList v lo ∧ True ∧ True
        exact ⟨⟨hbelow, fun j hj hjlen => habove j (by omega) hjlen⟩, trivial, trivial⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmp : mid + 1 ≤ hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs.toList.getD mid 0 = xs[mid] := by
          rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval' : xs.toList[mid] = xs[mid] := by
          rw [← getD_eq_getElem hmidlen]
          exact hlistval
        have hpowsucc : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by
          rw [Nat.pow_succ]
          omega
        rw [hpowsucc] at hf
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
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                  exact hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [hlistval] at hrel
                omega
          have hfuel' : hi - (mid + 1) < 2 ^ fuel := by omega
          have hres := ih (mid + 1) hi hmp (by omega) hfuel' hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
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
                  rw [getD_eq_getElem hmidlen, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                rw [hlistval] at hrel
                omega
          have hfuel' : mid - lo < 2 ^ fuel := by omega
          have hres := ih lo mid hml (by omega) hfuel' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          exact ⟨hc, hlb, by omega⟩

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length < 2 ^ (width ys.length + 1) := by
    unfold width
    split
    · rename_i hn
      have hz : ys.length = 0 := by simpa using hn
      omega
    · rename_i hn
      have hle : 2 ^ (ys.length.log2 + 1) ≤ 2 ^ (ys.length.log2 + 1 + 1) :=
        Nat.pow_le_pow_right (by omega) (by omega)
      exact Nat.lt_of_lt_of_le Nat.lt_log2_self hle
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; exact (Nat.not_lt_zero j hj).elim)
    (by intro j hj hjlen; exact (Nat.not_lt_of_ge hj (by simpa using hjlen)).elim)
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

private theorem filter_lt_length_of_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (v k : Nat) (hk : k ≤ ys.length)
    (h1 : ∀ j, j < k → ys.getD j 0 < v)
    (h2 : ∀ j, k ≤ j → j < ys.length → v ≤ ys.getD j 0) :
    (ys.filter (fun x => decide (x < v))).length = k := by
  induction ys generalizing k with
  | nil =>
      have hk0 : k = 0 := by simpa using (Nat.eq_zero_of_le_zero hk)
      subst k
      simp
  | cons y ys ih =>
      have hs' : ys.Pairwise (· ≤ ·) := hs.tail
      by_cases hyv : y < v
      · have hkpos : 0 < k := by
          have hnot : ¬ k ≤ 0 := by
            intro hk0
            have hge := h2 0 hk0 (by simp : 0 < (y :: ys).length)
            simp at hge
            omega
          omega
        obtain ⟨k', hk'⟩ := Nat.exists_eq_succ_of_ne_zero (by omega : k ≠ 0)
        subst k
        simp only [List.filter_cons, decide_eq_true_eq, hyv, ↓reduceIte, List.length_cons]
        have hk' : k' ≤ ys.length := by simpa using hk
        have hres := ih hs' k' hk'
          (by
            intro j hj
            have hlt := h1 (j + 1) (by omega)
            simpa [List.getD_cons_succ] using hlt)
          (by
            intro j hj hjlen
            have hge := h2 (j + 1) (by omega)
              (by simpa using hjlen : j + 1 < (y :: ys).length)
            simpa [List.getD_cons_succ] using hge)
        omega
      · have hk0 : k = 0 := by
          have hnot : ¬ 0 < k := by
            intro hpos
            have hlt := h1 0 hpos
            simp at hlt
            omega
          omega
        subst k
        rw [List.length_eq_zero_iff]
        apply List.filter_eq_nil_iff.mpr
        intro z hz
        simp only [decide_eq_true_eq]
        intro hzlt
        have hzys : z ∈ ys := by
          simp only [List.mem_cons] at hz
          rcases hz with hzy | hz
          · subst z
            exact (hyv hzlt).elim
          · exact hz
        have hyz : y ≤ z := List.rel_of_pairwise_cons hs hzys
        omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hc := lower_cut hs v
  exact (filter_lt_length_of_cut hs v (lower ys v) hc.2 hc.1.1 hc.1.2).symm

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
    (hb : ∀ p ∈ rs, b ≤ p.1) (_hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => decide (a ≤ y && y < b)) ++ ys.filter (fun y => member rs y) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have ih' := ih hs.tail
      rw [show (fun z => member ((a, b) :: rs) z) =
          (fun z => decide (a ≤ z && z < b) || member rs z) from by
        funext z
        exact member_cons]
      by_cases hfm : a ≤ y ∧ y < b
      · have hnot : member rs y = false := by
          cases hm : member rs y with
          | false => rfl
          | true =>
              simp only [member, List.any_eq_true] at hm
              rcases hm with ⟨p, hpm, hpred⟩
              have hbp := hb p hpm
              simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
              omega
        rw [List.filter_cons, List.filter_cons, List.filter_cons]
        simpa [member_cons, hfm, hnot] using ih'
      · by_cases hrm : member rs y = true
        · have hyge : b ≤ y := by
            simp only [member, List.any_eq_true] at hrm
            rcases hrm with ⟨p, hpm, hpred⟩
            have hbp := hb p hpm
            simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
            omega
          have hnilp : ys.filter (fun z => decide (a ≤ z && z < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            intro hzb
            have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
            omega
          have hnilp' : ys.filter (fun z => decide ((decide (a ≤ z) && decide (z < b)) = true)) = [] := by
            simpa using hnilp
          have hnilp'' : ys.filter (fun z => decide (a ≤ z) && decide (z < b)) = [] := by
            simpa [decide_eq_true_eq, Bool.and_eq_true] using hnilp
          rw [List.filter_cons, List.filter_cons, List.filter_cons]
          simpa [member_cons, hfm, hrm, hnilp, hnilp', hnilp''] using ih'
        · have hrmf : member rs y = false := Bool.eq_false_of_ne_true hrm
          rw [List.filter_cons, List.filter_cons, List.filter_cons]
          simpa [member_cons, hfm, hrmf] using ih'

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
private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  refine Nat.strongRecOn (motive := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup)
    xs.length ?_ xs rfl
  intro n ih l hl
  cases l with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        have hfil : a ∈ (as.filter fun b => !(b == a)) := List.mem_eraseDups.mp ha
        have hc := (List.mem_filter.mp hfil).2
        simp at hc
      · have hlt : (as.filter fun b => !(b == a)).length < n := by
          rw [← hl]
          exact Nat.lt_succ_of_le (List.length_filter_le _ as)
        exact ih _ hlt (as.filter fun b => !(b == a)) rfl

private theorem filter_not_perm_filter {α : Type} (p : α → Bool) (xs : List α) :
    (xs.filter (fun x => !p x) ++ xs.filter p).Perm xs := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      by_cases hp : p a = true
      · simp only [List.filter_cons, hp, Bool.not_true, Bool.false_eq_true, ↓reduceIte]
        have hswap : (xs.filter (fun x => !p x) ++ a :: xs.filter p).Perm
            (a :: (xs.filter (fun x => !p x) ++ xs.filter p)) := by
          simpa only [List.cons_append, List.append_assoc, List.nil_append] using
            (List.perm_append_comm (l₁ := xs.filter (fun x => !p x)) (l₂ := [a])).append_right
              (xs.filter p)
        exact hswap.trans (List.Perm.cons a ih)
      · simp only [List.filter_cons, hp, Bool.not_false, ↓reduceIte, List.cons_append]
        exact List.Perm.cons a ih

private theorem countP_zipIdx_aux (ys : List Nat) (p : Nat → Bool) (off : Nat) :
    ys.countP p = (ys.zipIdx off).countP (fun q => p q.1) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons, List.countP_cons, List.countP_cons]
      rw [ih (off + 1)]

private theorem countP_zipIdx (ys : List Nat) (p : Nat → Bool) :
    ys.countP p = ys.zipIdx.countP (fun q => p q.1) := by
  simpa using countP_zipIdx_aux ys p 0

private theorem zipIdx_filter_snd (ys : List Nat) (ds : List Nat) (p : Nat → Bool) (off : Nat) :
    ((ys.zipIdx off).filter (fun q => ds.contains q.2 && p q.1)).map Prod.snd =
      ((List.range ys.length).filter
        (fun j => ds.contains (off + j) && p (ys.getD j 0))).map (fun j => off + j) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons]
      change List.map Prod.snd (List.filter (fun q => ds.contains q.2 && p q.1) ((y, off) :: ys.zipIdx (off + 1))) =
        List.map (fun j => off + j)
          (List.filter (fun j => ds.contains (off + j) && p ((y :: ys).getD j 0))
            (List.range (ys.length + 1)))
      rw [List.range_succ_eq_map]
      rw [List.filter_cons]
      by_cases h : ds.contains off && p y
      · have hm : off ∈ ds ∧ p y = true := by simpa [List.contains_eq_mem] using h
        simp [h, hm]
        rw [List.filter_map, List.map_map]
        simpa [Function.comp_def, List.getD_cons_succ, Nat.zero_add,
          Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] using ih (off + 1)
      · have hm : ¬ (off ∈ ds ∧ p y = true) := by simpa [List.contains_eq_mem] using h
        simp [h, hm]
        rw [List.filter_map, List.map_map]
        simpa [Function.comp_def, List.getD_cons_succ,
          Nat.add_assoc, Nat.add_left_comm, Nat.add_comm] using ih (off + 1)

private theorem filter_mem_length_eq (xs ys : List Nat) (p : Nat → Bool)
    (hxs : xs.Nodup) (hys : ys.Nodup) :
    (xs.filter (fun i => decide (i ∈ ys) && p i)).length =
      (ys.filter (fun i => decide (i ∈ xs) && p i)).length := by
  have hm : (xs.filter (fun i => decide (i ∈ ys))).Perm
      (ys.filter (fun i => decide (i ∈ xs))) := by
    apply List.perm_ext_iff_of_nodup (hxs.filter _) (hys.filter _) |>.mpr
    intro i
    simp only [List.mem_filter]
    exact ⟨fun h => ⟨by simpa using h.2, by simpa using h.1⟩,
      fun h => ⟨by simpa using h.2, by simpa using h.1⟩⟩
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
      simp
    _ = (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).length := List.length_map _
    _ = (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).countP (fun q => p q.1) := by
      rw [List.countP_eq_length_filter]
      symm
      apply congrArg List.length
      apply List.filter_eq_self.mpr
      intro q hq
      have hc := (List.mem_filter.mp hq).2
      have hc' : ds.contains q.snd = true ∧ p q.1 = true := by
        simpa only [Bool.and_eq_true] using hc
      exact hc'.2

private theorem maskedJoin_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : maskedJoin s := by
  intro u t x ys ds hv
  rw [offsetCount_of_spec hspec hord u t x ys hv]
  let pred := s.test t x
  let keep := ys.zipIdx.filter (fun q => !ds.contains q.2)
  let del := ys.zipIdx.filter (fun q => ds.contains q.2)
  have hperm : (keep ++ del).Perm ys.zipIdx := by
    dsimp only [keep, del]
    exact filter_not_perm_filter (fun q => ds.contains q.2) ys.zipIdx
  have hzip : ys.zipIdx.countP (fun q => pred q.1) =
      keep.countP (fun q => pred q.1) + del.countP (fun q => pred q.1) := by
    rw [← hperm.countP_eq (fun q => pred q.1), List.countP_append]
  have hsurv : ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP pred =
      keep.countP (fun q => pred q.1) := by
    dsimp only [keep]
    rw [List.countP_map]
    rfl
  have hdelcount : ds.eraseDups.countP (fun i => ys[i]?.any pred) =
      del.countP (fun q => pred q.1) := by
    rw [deleted_count_eq]
    dsimp only [del]
    have heq : ys.zipIdx.filter (fun q => ds.contains q.2 && pred q.1) =
        (ys.zipIdx.filter (fun q => ds.contains q.2)).filter (fun q => pred q.1) := by
      rw [List.filter_filter]
      apply List.filter_congr
      intro q hq
      by_cases hc : ds.contains q.2 <;> by_cases hp : pred q.1 <;> simp [hc, hp]
    rw [heq]
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
    apply congrArg List.length
    apply List.filter_eq_self.mpr
    intro q hq
    exact (List.mem_filter.mp hq).2
  rw [countP_zipIdx ys pred]
  rw [hsurv]
  rw [hzip]
  rw [hdelcount]
  omega

/-! ## Annulus -/

namespace Annulus

/-- The dead-band predicate is exactly the union of the two emitted intervals
    inside the domain `[0,u)`. -/
private theorem spec : SpansSpec search := by
  intro u t x y hyu
  simp only [search, member_append, member_optional, Bool.or_eq_true,
    Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · rintro (⟨ha, hb⟩ | ⟨hc, hd⟩)
    · change t < x - y + (y - x) ∧ x - y + (y - x) ≤ 2 * t + 1
      by_cases hyx : y ≤ x
      · have hyx0 : y - x = 0 := by omega
        omega
      · omega
    · change t < x - y + (y - x) ∧ x - y + (y - x) ≤ 2 * t + 1
      by_cases hyx : y ≤ x
      · omega
      · have hxy0 : x - y = 0 := by omega
        omega
  · intro ht
    change t < x - y + (y - x) ∧ x - y + (y - x) ≤ 2 * t + 1 at ht
    by_cases hyx : y ≤ x
    · left
      constructor
      · by_cases hbig : 2 * t + 1 ≤ x
        · omega
        · have hxa : x - (2 * t + 1) = 0 := by omega
          omega
      · exact Nat.lt_min.mpr ⟨hyu, by omega⟩
    · right
      constructor
      · omega
      · exact Nat.lt_min.mpr ⟨hyu, by omega⟩

private theorem ordered : SpansOrdered search := by
  intro u t x
  constructor
  · simpa only [search] using
      (optional_append_ordered (x - (2 * t + 1)) (x - t) (x + t + 1)
        (x + 2 * t + 2) u (by omega)).1
  · intro p hp
    simp only [search, List.mem_append] at hp
    rcases hp with hp | hp
    · have h := (optional_ordered (x - (2 * t + 1)) (min u (x - t))).2 p hp
      exact h
    · have h := (optional_ordered (x + t + 1) (min u (x + 2 * t + 2))).2 p hp
      exact h

theorem sound_MaskedJoin :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus.MaskedJoin.Goal :=
  maskedJoin_of_spec spec ordered

end Annulus

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_MaskedJoin
