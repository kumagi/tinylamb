import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_KthMatch

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Apart
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem optional_none {a b : Nat} (h : ¬ a < b) :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = [] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_some {a b : Nat} (h : a < b) :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = [(a, b)] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]


private theorem member_optional_iff {a b y : Nat} :
    member (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, member]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, member]; omega


private theorem optional_ordered (a b : Nat) :
    (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b).Pairwise (fun p q => p.2 ≤ q.1) ∧ ∀ p ∈ TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b, p.1 ≤ p.2 := by
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, Nat.le_of_lt h]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]


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

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun y => decide (a ≤ y) && decide (y < b)) := by
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
        have hnil_ab : xs.filter (fun y => decide (a ≤ y) && decide (y < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [Bool.and_eq_true, decide_eq_true_eq]
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
        rw [List.filter_eq_nil_iff]
        intro y hy
        simp only [decide_eq_true_eq]
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
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
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    rw [List.filter_eq_nil_iff]
    intro y hy
    simp only [Bool.and_eq_true, decide_eq_true_eq]
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

private theorem nth_single {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b k : Nat) :
    nth ys [(a, b)] k = (ys.filter (fun y => decide (a ≤ y) && decide (y < b)))[k]? := by
  rw [← slice_eq_filter hs a b]
  simp only [nth]
  unfold slice
  simp only [List.getElem?_take, List.getElem?_drop]
private abbrev opt := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional

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
      unfold slice
      simp only [List.getElem?_take, List.getElem?_drop]
      rw [if_pos hk]
    · rw [if_neg hk]
      have hk' : ¬ k < (slice ys a b).length := by omega
      rw [List.getElem?_append, if_neg hk']
      rw [ih (k - (lower ys b - lower ys a))]
      congr 1
      rw [hslen]

private theorem opt_slice {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (opt a b).flatMap (fun (r, s) => slice ys r s) = slice ys a b := by
  change (TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b).flatMap
      (fun (r, s) => slice ys r s) = slice ys a b
  by_cases h : a < b
  · rw [optional_some h]
    simp
  · have hz : slice ys a b = [] := by
      rw [slice_eq_filter hs a b]
      rw [List.filter_eq_nil_iff]
      intro y hy hp
      have hp' := Bool.and_eq_true_iff.mp hp
      have hay : a ≤ y := of_decide_eq_true hp'.1
      have hyb : y < b := of_decide_eq_true hp'.2
      omega
    rw [optional_none h, hz]
    simp

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
    by_cases hp : a ≤ y ∧ y < b
    · have hq : ¬ (c ≤ y ∧ y < d) := by omega
      simp [hp, hq, decide_eq_true_eq, ih hs']
    · by_cases hq : c ≤ y ∧ y < d
      · have hpnil : ys.filter (fun z => decide (a ≤ z) && decide (z < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro z hz hzpred
          have hzpred' := Bool.and_eq_true_iff.mp hzpred
          have haz : a ≤ z := of_decide_eq_true hzpred'.1
          have hzb : z < b := of_decide_eq_true hzpred'.2
          have hzy : y ≤ z := List.rel_of_pairwise_cons hs hz
          omega
        simp [hp, hq, hpnil, decide_eq_true_eq, ih hs']
      · simp [hp, hq, decide_eq_true_eq, ih hs']


private theorem test_iff {u t x y : Nat} (hyu : y < u) :
    search.test t x y = true ↔
      (0 ≤ y ∧ y < min u (x - t)) ∨
      (x + t + 1 ≤ y ∧ y < u) := by
  dsimp [search]
  constructor
  · intro h
    simp only [Bool.or_eq_true, decide_eq_true_eq] at h
    rcases h with h | h
    · right
      exact ⟨by omega, hyu⟩
    · left
      exact ⟨Nat.zero_le _, Nat.lt_min.mpr ⟨hyu, by omega⟩⟩
  · intro h
    simp only [Bool.or_eq_true, decide_eq_true_eq]
    rcases h with ⟨_, hmin⟩ | ⟨hlo, _⟩
    · right
      have hylt : y < x - t := Nat.lt_of_lt_of_le hmin (Nat.min_le_right u (x - t))
      omega
    · left
      omega

private theorem seek_eq_filter_apart {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    (search.spans u t x).flatMap (fun (a, b) => slice ys a b) =
      ys.filter (search.test t x) := by
  let b := min u (x - t)
  let c := x + t + 1
  have hs := valid_pairwise hv
  have hbc : b ≤ c := by
    dsimp [b, c]
    omega
  have hspans : search.spans u t x = opt 0 b ++ opt c u := by
    dsimp [b, c]
    rfl
  rw [hspans, List.flatMap_append]
  rw [opt_slice hs 0 b, opt_slice hs c u]
  rw [slice_eq_filter hs 0 b, slice_eq_filter hs c u]
  symm
  calc
    ys.filter (search.test t x)
        = ys.filter (fun y => (decide (0 ≤ y) && decide (y < b)) ||
                              (decide (c ≤ y) && decide (y < u))) := by
          apply List.filter_congr
          intro y hy
          have hyu : y < u := valid_lt hv hy
          have hiff := test_iff (u := u) (t := t) (x := x) (y := y) hyu
          change search.test t x y =
            ((decide (0 ≤ y) && decide (y < min u (x - t))) ||
             (decide (x + t + 1 ≤ y) && decide (y < u)))
          apply Bool.eq_iff_iff.mpr
          rw [hiff]
          simp [Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq]
    _ = ys.filter (fun y => decide (0 ≤ y) && decide (y < b)) ++
        ys.filter (fun y => decide (c ≤ y) && decide (y < u)) := by
          exact filter_two_intervals hs hbc

theorem sound_KthMatch : Apart.KthMatch.Goal := by
  intro u t x ys k hv
  have hs := valid_pairwise hv
  rw [nth_eq_flatMap hs (search.spans u t x) k]
  rw [seek_eq_filter_apart hv]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Apart_KthMatch
