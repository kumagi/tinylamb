import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Annulus.KthMatch` — Top offset within the matching postings preserves tie
  multiplicity and right ordering.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_KthMatch

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus
open TinylambOpt.ResearchGoals.Compression.Wire

private abbrev opt := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) : ys.Pairwise (· ≤ ·) := hv.1

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
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take]; simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by
        rw [List.take_append_drop]

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
        ys.filter (fun x => decide (a ≤ x) && decide (x < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
    have hs' := hs.tail
    by_cases hxa : x < a
    · have hxb : x < b := by omega
      simp [hxa, hxb, ih hs']
    · by_cases hxb : x < b
      · have hax : a ≤ x := by omega
        have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          cases hyd : decide (y < a) with
          | false => simp [hyd]
          | true =>
            have hyd' : y < a := by simpa using hyd
            omega
        have hcongr : xs.filter (fun y => decide (y < b)) =
            xs.filter (fun y => decide (a ≤ y) && decide (y < b)) := by
          apply List.filter_congr
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          have hay : a ≤ y := by omega
          simp [hay]
        simp [hxa, hxb, hax, hnil_a, hcongr, ih hs']
      · have hax : a ≤ x := by omega
        have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          cases hyd : decide (y < a) with
          | false => simp [hyd]
          | true =>
            have hyd' : y < a := by simpa using hyd
            omega
        have hnil_ab : xs.filter (fun y => decide (a ≤ y) && decide (y < b)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          cases hyd : decide (y < b) with
          | false => simp [hyd]
          | true =>
            have hyd' : y < b := by simpa using hyd
            omega
        simp [hxa, hxb, hax, hnil_a, hnil_ab, ih hs']

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
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
        cases hyd : decide (y < v) with
        | false => simp [hyd]
        | true =>
          have hyd' : y < v := by simpa using hyd
          omega
      simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y) && decide (y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
      lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b,
      take_filter_prefix hs b, filter_lt_prefix_filter_lt hs hab, List.drop_left]
  · have hba : b ≤ a := by omega
    rw [Nat.sub_eq_zero_of_le (lower_mono hs hba), List.take_zero]
    symm
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    cases hay : decide (a ≤ y) with
    | false => simp [hay]
    | true =>
      cases hyb : decide (y < b) with
      | false => simp [hyb]
      | true =>
        have hay' : a ≤ y := by simpa using hay
        have hyb' : y < b := by simpa using hyb
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

private theorem nth_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (rs : Ranges) (k : Nat) :
    nth ys rs k = (rs.flatMap (fun (a, b) => slice ys a b))[k]? := by
  induction rs generalizing k with
  | nil => simp [nth]
  | cons p rs ih =>
    obtain ⟨a, b⟩ := p
    simp only [nth, List.flatMap_cons]
    have hslen : (slice ys a b).length = lower ys b - lower ys a := slice_length hs a b
    by_cases hk : k < lower ys b - lower ys a
    · rw [if_pos hk]
      have hk' : k < (slice ys a b).length := by omega
      rw [List.getElem?_append, if_pos hk']
      have hsub : (slice ys a b)[k]? = ys[lower ys a + k]? := by
        unfold slice
        simp [List.getElem?_take, List.getElem?_drop, hk]
      rw [hsub]
    · rw [if_neg hk]
      have hk' : ¬ k < (slice ys a b).length := by omega
      rw [List.getElem?_append, if_neg hk']
      rw [ih (k - (lower ys b - lower ys a))]
      congr 1
      rw [hslen]

private theorem opt_slice {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (opt a b).flatMap (fun (r, s) => slice ys r s) = slice ys a b := by
  by_cases h : a < b
  · rw [show opt a b = [(a, b)] by
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]]
    simp
  · have hz : slice ys a b = [] := by
      rw [slice_eq_filter hs a b]
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      cases hay : decide (a ≤ y) with
      | false => simp [hay]
      | true =>
        cases hyb : decide (y < b) with
        | false => simp [hyb]
        | true =>
          have hay' : a ≤ y := by simpa using hay
          have hyb' : y < b := by simpa using hyb
          omega
    rw [show opt a b = [] by
      simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]]
    simp [hz]

private theorem filter_two_intervals {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b c d : Nat} (hbc : b ≤ c) :
    ys.filter (fun y => (decide (a ≤ y) && decide (y < b)) ||
                        (decide (c ≤ y) && decide (y < d))) =
      ys.filter (fun y => decide (a ≤ y) && decide (y < b)) ++
        ys.filter (fun y => decide (c ≤ y) && decide (y < d)) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    have hs' := hs.tail
    let p : Nat → Bool := fun z => decide (a ≤ z) && decide (z < b)
    let q : Nat → Bool := fun z => decide (c ≤ z) && decide (z < d)
    have hpq_false : ∀ z, p z = true → q z = false := by
      intro z hp
      have hp' : a ≤ z ∧ z < b := by
        simpa [p, Bool.and_eq_true, decide_eq_true_eq] using hp
      cases hqz : q z
      · simpa using hqz
      · have hq' : c ≤ z ∧ z < d := by
          simpa [q, Bool.and_eq_true, decide_eq_true_eq] using hqz
        omega
    by_cases hp : p y = true
    · have hq : q y = false := hpq_false y hp
      simp [p, q, hp, hq, ih hs']
    · by_cases hq : q y = true
      · have hpnil : ys.filter p = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro z hz
          have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
          have hqy : c ≤ y ∧ y < d := by
            simpa [q, Bool.and_eq_true, decide_eq_true_eq] using hq
          cases hpz : p z
          · simpa using hpz
          · have hp' : a ≤ z ∧ z < b := by
              simpa [p, Bool.and_eq_true, decide_eq_true_eq] using hpz
            omega
        simp [p, q, hp, hq, hpnil, ih hs']
      · simp [p, q, hp, hq, ih hs']

private theorem test_iff {u t x y : Nat} (hyu : y < u) :
    search.test t x y = true ↔
      (x - (2 * t + 1) ≤ y ∧ y < min u (x - t)) ∨
      (x + t + 1 ≤ y ∧ y < min u (x + 2 * t + 2)) := by
  simp only [search, Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · intro h
    rcases h with ⟨hlo, hhi⟩
    by_cases hxy : x ≤ y
    · right
      have hd : x - y + (y - x) = y - x := by simp [Nat.sub_eq_zero_of_le hxy]
      rw [hd] at hlo hhi
      constructor <;> omega
    · left
      have hyx : y ≤ x := by omega
      have hd : x - y + (y - x) = x - y := by simp [Nat.sub_eq_zero_of_le hyx]
      rw [hd] at hlo hhi
      constructor <;> omega
  · intro h
    rcases h with ⟨h1, h2⟩ | ⟨h1, h2⟩
    · have hyltx : y < x := by
        have hminle : min u (x - t) ≤ x - t := Nat.min_le_right _ _
        omega
      have hd : x - y + (y - x) = x - y := by simp [Nat.sub_eq_zero_of_le (by omega : y ≤ x)]
      rw [hd]
      constructor <;> omega
    · have hxyle : x ≤ y := by omega
      have hd : x - y + (y - x) = y - x := by simp [Nat.sub_eq_zero_of_le hxyle]
      rw [hd]
      constructor <;> omega

private theorem seek_eq_filter_annulus {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    (search.spans u t x).flatMap (fun (a, b) => slice ys a b) =
      ys.filter (search.test t x) := by
  let a := x - (2 * t + 1)
  let b := min u (x - t)
  let c := x + t + 1
  let d := min u (x + 2 * t + 2)
  have hs := valid_pairwise hv
  have hbc : b ≤ c := by
    dsimp [b, c]
    omega
  have hspans : search.spans u t x = opt a b ++ opt c d := by
    rfl
  rw [hspans, List.flatMap_append]
  rw [opt_slice hs a b, opt_slice hs c d]
  rw [slice_eq_filter hs a b, slice_eq_filter hs c d]
  symm
  calc
    ys.filter (search.test t x)
        = ys.filter (fun y => (decide (a ≤ y) && decide (y < b)) ||
                              (decide (c ≤ y) && decide (y < d))) := by
            apply List.filter_congr
            intro y hy
            have hyu : y < u := hv.2 y hy
            have hiff := test_iff (u := u) (t := t) (x := x) (y := y) hyu
            apply Bool.eq_iff_iff.mpr
            rw [hiff]
            simp [a, b, c, d, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq]
    _ = ys.filter (fun y => decide (a ≤ y) && decide (y < b)) ++
        ys.filter (fun y => decide (c ≤ y) && decide (y < d)) := by
          exact filter_two_intervals hs hbc

theorem sound_KthMatch : Annulus.KthMatch.Goal := by
  intro u t x ys k hv
  have hs := valid_pairwise hv
  rw [nth_eq_flatMap hs (search.spans u t x) k]
  rw [seek_eq_filter_annulus hv]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_KthMatch
