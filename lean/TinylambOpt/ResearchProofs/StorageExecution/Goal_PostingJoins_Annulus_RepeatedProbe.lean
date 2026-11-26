import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Annulus.RepeatedProbe` — Cache one interval query per distinct left key when
  aggregating without output expansion.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_RepeatedProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [optional_eq, h]

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [optional_eq, h]

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat}
    (hy : y ∈ ys) : y < u := hv.2 y hy

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
    simp at hf
    have heq : lo = hi := by omega
    subst hi
    simp only [searchBound]
    exact ⟨⟨hbelow, fun j hji hjl => habove j hji hjl⟩, by simp, by simp⟩
  | succ fuel ih =>
    by_cases hge : lo ≥ hi
    · have heq : lo = hi := by omega
      subst hi
      rw [searchBound]
      simp only [hge, ↓reduceIte]
      exact ⟨⟨hbelow, fun j hji hjl => habove j hji hjl⟩, by simp, by simp⟩
    · have hlt : lo < hi := by omega
      have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
        rw [Nat.pow_succ]
        omega
      rw [hpow] at hf
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
        have hres := ih (mid + 1) hi (by omega) (by omega) (by omega) hbelow' habove
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
        have hres := ih lo mid hml (by omega) (by omega) hbelow habove'
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
    simp
  · have hne : n ≠ 0 := by
      intro hz
      subst n
      simp at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (by omega)
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
      have hjlen' : j < ys.length := by simpa using hjlen
      omega)
    hs
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

private theorem filter_length_lt_iff {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v i : Nat) :
    i < (ys.filter (fun x => decide (x < v))).length ↔
      i < ys.length ∧ ys.getD i 0 < v := by
  induction ys generalizing i with
  | nil => simp
  | cons x xs ih =>
    have hs' := hs.tail
    by_cases hx : x < v
    · cases i with
      | zero => simp [hx]
      | succ i =>
        have htail := ih hs' i
        simpa [hx] using htail
    · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro y hy
        simp only [decide_eq_true_eq]
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs (by simp [hy])
        omega
      cases i with
      | zero => simp [hx, hnil]
      | succ i =>
        simp [hx, hnil]
        intro hi
        have hxi : xs.getD i 0 ∈ xs := by
          rw [getD_eq_getElem hi]
          exact List.getElem_mem hi
        have hxy : x ≤ xs.getD i 0 := by
          rw [getD_eq_getElem hi]
          exact List.rel_of_pairwise_cons hs (by simp [hxi])
        rw [getD_eq_getElem hi] at hxy
        change v ≤ xs.getD i 0
        rw [getD_eq_getElem hi]
        omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  let f := (ys.filter (fun x => decide (x < v))).length
  have hiff : ∀ i, i < f ↔ i < ys.length ∧ ys.getD i 0 < v := by
    intro i
    dsimp [f]
    exact filter_length_lt_iff hs v i
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro h
    have hfl : f < lower ys v := h
    have hflen : f < ys.length := Nat.lt_of_lt_of_le hfl (lower_le_length hs v)
    have hlt := lower_lt_of_lt hs v hfl
    have hfself : f < f := (hiff f).mpr ⟨hflen, hlt⟩
    omega
  · apply Nat.le_of_not_gt
    intro h
    have hlf : lower ys v < f := h
    have hp := (hiff (lower ys v)).mp hlf
    have hge := lower_le_of_ge hs v (Nat.le_refl _) hp.1
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

private theorem lower_diff_filter_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [← slice_length hs a b, slice_eq_filter hs a b]

private theorem optional_fan {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    ((optional a b).map (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
      lower ys b - lower ys a := by
  by_cases h : a < b
  · rw [optional_some h]
    simp
  · rw [optional_none h]
    simp only [List.map_nil, List.sum_nil]
    have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    omega

private def low (u t x y : Nat) : Bool :=
  decide (x - (2 * t + 1) ≤ y && y < min u (x - t))

private def high (u t x y : Nat) : Bool :=
  decide (x + t + 1 ≤ y && y < min u (x + 2 * t + 2))

private theorem annulus_test_iff (u t x y : Nat) (hyu : y < u) :
    search.test t x y = true ↔ (low u t x y || high u t x y) = true := by
  by_cases hyx : y ≤ x
  · have hd : x - y + (y - x) = x - y := by
      rw [Nat.sub_eq_zero_of_le hyx, Nat.add_zero]
    dsimp [search, low, high]
    rw [hd]
    simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.or_eq_true]
    constructor
    · intro ht
      left
      constructor
      · omega
      · exact Nat.lt_min.mpr ⟨hyu, by omega⟩
    · intro h
      rcases h with h | h
      · rcases h with ⟨hlo, hhi⟩
        have hylt : y < x - t := Nat.lt_of_lt_of_le hhi (Nat.min_le_right u (x - t))
        omega
      · rcases h with ⟨hlo, _⟩
        omega
  · have hxy : x < y := by omega
    have hd : x - y + (y - x) = y - x := by
      rw [Nat.sub_eq_zero_of_le (Nat.le_of_lt hxy), Nat.zero_add]
    dsimp [search, low, high]
    rw [hd]
    simp only [Bool.and_eq_true, decide_eq_true_eq, Bool.or_eq_true]
    constructor
    · intro ht
      right
      constructor
      · omega
      · exact Nat.lt_min.mpr ⟨hyu, by omega⟩
    · intro h
      rcases h with h | h
      · rcases h with ⟨_, hhi⟩
        have hylt : y < x - t := Nat.lt_of_lt_of_le hhi (Nat.min_le_right u (x - t))
        omega
      · rcases h with ⟨hlo, hhi⟩
        have hylt : y < x + 2 * t + 2 :=
          Nat.lt_of_lt_of_le hhi (Nat.min_le_right u (x + 2 * t + 2))
        omega

private theorem low_false_of_high {u t x y z : Nat}
    (hy : high u t x y = true) (hyz : y ≤ z) : low u t x z = false := by
  dsimp [high] at hy
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hy
  dsimp [low]
  rw [decide_eq_false_iff_not]
  intro hz
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hz
  have hle : min u (x - t) ≤ x + t + 1 :=
    Nat.le_trans (Nat.min_le_right u (x - t)) (by omega)
  omega

private theorem high_false_of_low {u t x y z : Nat}
    (hy : low u t x y = true) (hyz : z ≤ y) : high u t x z = false := by
  dsimp [low] at hy
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hy
  dsimp [high]
  rw [decide_eq_false_iff_not]
  intro hz
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hz
  have hle : min u (x - t) ≤ x + t + 1 :=
    Nat.le_trans (Nat.min_le_right u (x - t)) (by omega)
  omega

private theorem annulus_filter_concat (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    ys.filter (fun y => low u t x y || high u t x y) =
      ys.filter (low u t x) ++ ys.filter (high u t x) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    have htail := ih hs.tail
    by_cases hl : low u t x y = true
    · have hh : high u t x y = false := high_false_of_low hl (Nat.le_refl y)
      simp [hl, hh, htail]
    · by_cases hh : high u t x y = true
      · have hnil : ys.filter (low u t x) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro z hz
          have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
          have hzfalse := low_false_of_high hh hyz
          intro hztrue
          rw [hzfalse] at hztrue
          contradiction
        simp [hl, hh, hnil, htail]
      · simp [hl, hh, htail]

private theorem annulus_filter (u t x : Nat) {ys : List Nat} (hv : valid u ys) :
    ys.filter (search.test t x) =
      ys.filter (low u t x) ++ ys.filter (high u t x) := by
  calc
    ys.filter (search.test t x) = ys.filter (fun y => low u t x y || high u t x y) := by
      apply List.filter_congr
      intro y hy
      have hiff := annulus_test_iff u t x y (valid_lt hv hy)
      cases h1 : search.test t x y <;> cases h2 : low u t x y || high u t x y <;> simp_all
    _ = ys.filter (low u t x) ++ ys.filter (high u t x) :=
      annulus_filter_concat u t x (valid_pairwise hv)

private theorem annulus_fan (u t x : Nat) {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = (ys.filter (search.test t x)).length := by
  have hs := valid_pairwise hv
  unfold fan
  change (((optional (x - (2 * t + 1)) (min u (x - t)) ++
      optional (x + t + 1) (min u (x + 2 * t + 2))).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum) =
    (ys.filter (search.test t x)).length
  rw [List.map_append, List.sum_append, optional_fan hs, optional_fan hs]
  rw [lower_diff_filter_length hs, lower_diff_filter_length hs]
  rw [annulus_filter u t x hv]
  simp only [List.length_append]
  rfl

private theorem count_filter_ne {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (a x : α) (hxa : x ≠ a) :
    xs.count x = (xs.filter (fun b => !b == a)).count x := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
    by_cases hba : b == a
    · have hb_eq : b = a := by simpa using hba
      have hxb : x ≠ b := by
        intro h
        apply hxa
        simpa [hb_eq] using h
      rw [List.count_cons_of_ne (Ne.symm hxb)]
      simpa [hba] using ih
    · by_cases hxb : x = b
      · subst x
        simp [hba]
      · simpa [hba, Ne.symm hxb] using ih

private theorem sum_eq_count_mul_add_filter {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (a : α) (f : α → Nat) :
    (xs.map f).sum = xs.count a * f a + ((xs.filter (fun b => !b == a)).map f).sum := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
    by_cases hba : b == a
    · have hb_eq : b = a := by simpa using hba
      simp [hba, hb_eq, List.count_cons_self, ih, Nat.add_mul,
        Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
    · have hbne : b ≠ a := by
        intro h
        subst b
        simp at hba
      simp [hba, hbne, ih, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem sum_map_eraseDups_count_mul {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  classical
  let P : Nat → Prop := fun n => ∀ (xs : List α), xs.length = n →
      (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
      intro xs hlen
      cases xs with
      | nil => simp
      | cons a xs =>
        let t := xs.filter (fun b => !b == a)
        have htlen : t.length < n := by
          dsimp [t]
          simp at hlen
          have hle := List.length_filter_le (fun b => !b == a) xs
          omega
        have ht := ih t.length htlen t rfl
        have hds : (a :: xs).eraseDups = a :: t.eraseDups := by
          simp [t, List.eraseDups_cons]
        have htail : (t.eraseDups.map (fun x => (a :: xs).count x * f x)).sum =
            (t.eraseDups.map (fun x => t.count x * f x)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro x hx
          have hxorig : x ∈ t := List.mem_eraseDups.mp hx
          have hf := (List.mem_filter.mp hxorig).2
          have hxne_a : x ≠ a := by
            intro hxa
            subst x
            simp at hf
          rw [List.count_cons_of_ne (Ne.symm hxne_a)]
          rw [count_filter_ne xs a x hxne_a]
        rw [hds]
        simp only [List.map_cons, List.sum_cons]
        rw [List.count_cons_self]
        calc
          (xs.count a + 1) * f a +
              (t.eraseDups.map (fun x => (a :: xs).count x * f x)).sum =
              xs.count a * f a + f a + (t.map f).sum := by
            rw [htail, ht, Nat.add_mul, Nat.one_mul]
          _ = f a + (xs.map f).sum := by
            rw [sum_eq_count_mul_add_filter xs a f]
            dsimp [t]
            ac_rfl
  exact hP xs.length xs rfl

theorem sound_RepeatedProbe : Annulus.RepeatedProbe.Goal := by
  intro u t xs ys hv
  have hfan : ∀ x, fan search u t x ys = (ys.filter (search.test t x)).length := by
    intro x
    exact annulus_fan u t x hv
  rw [show (xs.eraseDups.map (fun x => xs.count x * fan search u t x ys)).sum =
      (xs.eraseDups.map (fun x => xs.count x * (ys.filter (search.test t x)).length)).sum from by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rw [hfan x]]
  rw [List.length_flatMap]
  exact sum_map_eraseDups_count_mul xs (fun x => (ys.filter (search.test t x)).length)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_RepeatedProbe
