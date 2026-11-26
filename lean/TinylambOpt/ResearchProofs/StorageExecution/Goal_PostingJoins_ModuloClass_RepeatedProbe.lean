import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ModuloClass.RepeatedProbe` — Cache one interval query per distinct left key when
  aggregating without output expansion.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_RepeatedProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass

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

private theorem singleton_filter_length (ys : List Nat) (a : Nat) :
    (ys.filter (fun y => decide (a ≤ y && y < a + 1))).length = ys.count a := by
  calc
    (ys.filter (fun y => decide (a ≤ y && y < a + 1))).length =
        (ys.filter (fun y => y == a)).length := by
      apply congrArg List.length
      apply List.filter_congr
      intro y hy
      rw [Bool.eq_iff_iff]
      simp only [decide_eq_true_eq, Bool.and_eq_true, beq_iff_eq]
      constructor
      · rintro ⟨_, hylt⟩
        omega
      · intro hya
        subst y
        simp
    _ = ys.count a := List.count_eq_length_filter.symm

/-! ## Arithmetic of the residue cover of [0,u). -/

private theorem residue_lt_u (u w r i : Nat) (hw : 0 < w)
    (_hr : r < w) (hi : i < (u + w - 1 - r) / w) : r + i * w < u := by
  rw [Nat.lt_div_iff_mul_lt hw] at hi
  omega

private theorem residue_index_lt (u w r y : Nat) (hw : 0 < w) (_hr : r < w)
    (hyu : y < u) (hym : y % w = r) :
    y / w < (u + w - 1 - r) / w := by
  have hmodle : y % w ≤ y := Nat.mod_le y w
  have hru : r < u := by omega
  rw [Nat.lt_div_iff_mul_lt hw]
  have hsub : u + w - 1 - r - (w - 1) = u - r := by omega
  rw [hsub]
  have hy := Nat.div_add_mod y w
  rw [Nat.mul_comm w (y / w)] at hy
  omega

private theorem residue_decompose (w r y : Nat) (hym : y % w = r) :
    y = r + (y / w) * w := by
  have hy := Nat.div_add_mod y w
  rw [Nat.mul_comm w (y / w)] at hy
  omega

private theorem residue_of_add_mul (w r i : Nat) (hr : r < w) :
    (r + i * w) % w = r := by
  rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]

private theorem residue_div_add_mul (w r i : Nat) (hw : 0 < w) (hr : r < w) :
    (r + i * w) / w = i := by
  rw [Nat.add_mul_div_right r i hw]
  simp [Nat.div_eq_of_lt hr]

private theorem map_if_sum_eq_filter_length (l : List Nat) (p : Nat → Bool) :
    (l.map (fun i => if p i then 1 else 0)).sum = (l.filter p).length := by
  induction l with
  | nil => simp
  | cons i l ih =>
      by_cases h : p i = true <;> simp [h, ih, Nat.add_comm]

private theorem range_filter_length_one (u w r a : Nat) (hw : 0 < w) (hr : r < w)
    (hau : a < u) (ham : a % w = r) :
    ((List.range ((u + w - 1 - r) / w)).filter (fun i => r + i * w == a)).length = 1 := by
  have hperm : ((List.range ((u + w - 1 - r) / w)).filter (fun i => r + i * w == a)).Perm [a / w] := by
    apply (List.perm_ext_iff_of_nodup ((List.nodup_range (n := (u + w - 1 - r) / w)).filter _) (by simp)).mpr
    intro i
    simp only [List.mem_filter, List.mem_range, List.mem_singleton]
    constructor
    · rintro ⟨hi, hia⟩
      rw [beq_iff_eq] at hia
      rw [← hia]
      exact (residue_div_add_mul w r i hw hr).symm
    · intro hia
      subst i
      refine ⟨residue_index_lt u w r a hw hr hau ham, ?_⟩
      rw [beq_iff_eq]
      exact (residue_decompose w r a ham).symm
  simpa using hperm.length_eq

private theorem range_filter_nil (u w r a : Nat) (hr : r < w) (ham : a % w ≠ r) :
    (List.range ((u + w - 1 - r) / w)).filter (fun i => r + i * w == a) = [] := by
  rw [List.filter_eq_nil_iff]
  intro i hi
  rw [List.mem_range] at hi
  intro hia
  rw [beq_iff_eq] at hia
  apply ham
  rw [← hia, residue_of_add_mul w r i hr]

private theorem range_indicator_sum (u w r a : Nat) (hw : 0 < w) (hr : r < w) (hau : a < u) :
    ((List.range ((u + w - 1 - r) / w)).map (fun i => if r + i * w == a then 1 else 0)).sum =
      if a % w == r then 1 else 0 := by
  rw [map_if_sum_eq_filter_length]
  by_cases ham : a % w = r
  · have hbeq : (a % w == r) = true := by rw [beq_iff_eq, ham]
    rw [ite_eq_left hbeq]
    rw [range_filter_length_one u w r a hw hr hau ham]
  · have hnot : ¬ (a % w == r) := by simpa [beq_iff_eq] using ham
    rw [ite_eq_right hnot]
    rw [range_filter_nil u w r a hr ham]
    simp

private theorem count_cons_add (a : Nat) (xs : List Nat) (x : Nat) :
    (a :: xs).count x = xs.count x + if x = a then 1 else 0 := by
  by_cases hxa : x = a
  · subst x
    rw [List.count_cons_self]
    simp
  · rw [List.count_cons_of_ne (Ne.symm hxa)]
    simp [hxa]

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_zero_map {α : Type} (xs : List α) (f : α → Nat)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have hx := h x (by simp)
    have ht := ih (fun y hy => h y (by simp [hy]))
    simp [hx, ht]

private theorem sum_map_zero_nat (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  exact sum_zero_map l (fun _ => 0) (by simp)

private theorem sum_count_mod_eq_filter (u w r : Nat) (hw : 0 < w) (hr : r < w)
    (ys : List Nat) (hyu : ∀ y ∈ ys, y < u) :
    ((List.range ((u + w - 1 - r) / w)).map (fun i => ys.count (r + i * w))).sum =
      (ys.filter (fun y => y % w == r)).length := by
  induction ys with
  | nil =>
      simp [List.count_nil]
      exact sum_map_zero_nat (List.range ((u + w - 1 - r) / w))
  | cons a ys ih =>
      have ha : a < u := hyu a (by simp)
      have htail := ih (fun y hy => hyu y (by simp [hy]))
      have hsum : ((List.range ((u + w - 1 - r) / w)).map (fun i => (a :: ys).count (r + i * w))).sum =
          ((List.range ((u + w - 1 - r) / w)).map (fun i => ys.count (r + i * w))).sum +
          ((List.range ((u + w - 1 - r) / w)).map (fun i => if r + i * w == a then 1 else 0)).sum := by
        rw [← sum_map_add_nat]
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        rw [count_cons_add a ys (r + i * w)]
        by_cases hia : r + i * w = a
        · subst a
          simp
        · simp [beq_iff_eq, hia]
      rw [hsum, htail, range_indicator_sum u w r a hw hr ha]
      by_cases h : a % w == r
      · simp [h]
      · simp [h]

private theorem modulo_fan (u t x : Nat) {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = (ys.filter (search.test t x)).length := by
  have hs : ys.Pairwise (· ≤ ·) := valid_pairwise hv
  have hw : 0 < t + 1 := by omega
  have hr : x % (t + 1) < t + 1 := Nat.mod_lt x hw
  unfold fan
  dsimp [search]
  rw [List.map_map]
  change (((List.range ((u + t - x % (t + 1)) / (t + 1))).map
      (fun i => lower ys (x % (t + 1) + i * (t + 1) + 1) -
        lower ys (x % (t + 1) + i * (t + 1)))).sum) =
    (ys.filter (fun y => x % (t + 1) == y % (t + 1))).length
  have hmap : ((List.range ((u + t - x % (t + 1)) / (t + 1))).map
      (fun i => lower ys (x % (t + 1) + i * (t + 1) + 1) -
        lower ys (x % (t + 1) + i * (t + 1)))).sum =
      ((List.range ((u + t - x % (t + 1)) / (t + 1))).map
        (fun i => ys.count (x % (t + 1) + i * (t + 1)))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro i hi
    rw [lower_diff_filter_length hs, singleton_filter_length]
  rw [hmap]
  rw [show (ys.filter (fun y => x % (t + 1) == y % (t + 1))) =
      (ys.filter (fun y => y % (t + 1) == x % (t + 1))) by
    apply List.filter_congr
    intro y hy
    apply Bool.eq_iff_iff.mpr
    simp only [beq_iff_eq]
    constructor <;> intro h <;> exact h.symm]
  exact sum_count_mod_eq_filter u (t + 1) (x % (t + 1)) hw hr ys
    (fun y hy => valid_lt hv hy)

/-! ## Generic grouping identity for distinct left keys. -/

private theorem eraseDups_nodup {α : Type} [DecidableEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ xs : List α, xs.length = n → xs.eraseDups.Nodup
  have hP : ∀ n, P n := by
    intro n
    refine Nat.strongRecOn n ?_
    intro n ih xs hlen
    cases xs with
    | nil => simp
    | cons a xs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro ha
          have hf := List.mem_eraseDups.mp ha
          have hm := List.mem_filter.mp hf
          have haa : (a == a) = true := by simp
          simp [haa] at hm
        · apply ih (xs.filter (fun b => ! b == a)).length
          · rw [← hlen, List.length_cons]
            have hle := List.length_filter_le (fun b => ! b == a) xs
            omega
          · rfl
  exact hP xs.length xs rfl

private theorem sum_map_perm {α : Type} (f : α → Nat) {l₁ l₂ : List α}
    (h : l₁.Perm l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_map_filter_ne_count {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (a : α) (f : α → Nat) :
    (xs.map f).sum = xs.count a * f a + ((xs.filter (fun b => ! b == a)).map f).sum := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
    by_cases hba : b = a
    · subst b
      rw [List.count_cons_self]
      simp only [List.map_cons, List.sum_cons]
      have hkeep : (a == a) = true := by simp
      simp [hkeep]
      rw [ih]
      rw [Nat.add_mul, Nat.one_mul]
      omega
    · rw [List.count_cons_of_ne hba]
      simp only [List.map_cons, List.sum_cons]
      have hdrop : (b == a) = false := by simp [beq_iff_eq, hba]
      simp [hdrop, ih]
      omega

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
    · have hba' : a ≠ b := Ne.symm hba
      have hmem : a ∈ bs := by simpa [hba'] using ha
      simp [hba, hba', ih hnd'.2 hmem]

private theorem sum_map_eraseDups_count_mul {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  let P : Nat → Prop := fun n => ∀ xs : List α, xs.length = n →
      (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum
  have hP : ∀ n, P n := by
    intro n
    refine Nat.strongRecOn n ?_
    intro n ih xs hlen
    cases xs with
    | nil => simp
    | cons a xs =>
        let ys := xs.filter (fun b => ! b == a)
        have hylen : ys.length < n := by
          dsimp [ys]
          rw [← hlen, List.length_cons]
          have hle := List.length_filter_le (fun b => ! b == a) xs
          omega
        have ihys := ih ys.length hylen ys rfl
        have hcount : ∀ x ∈ ys.eraseDups, (a :: xs).count x = ys.count x := by
          intro x hx
          have hxys : x ∈ ys := List.mem_eraseDups.mp hx
          have hxmem := List.mem_filter.mp hxys
          have hxne : x ≠ a := by
            intro hxa
            subst x
            simp at hxmem
          have hp : (fun b : α => ! b == a) x = true := by
            simp [beq_iff_eq, hxne]
          calc
            (a :: xs).count x = xs.count x := List.count_cons_of_ne (Ne.symm hxne)
            _ = ys.count x := (List.count_filter (p := fun b : α => ! b == a) (a := x) (l := xs) hp).symm
        have hmap : ((ys.eraseDups).map (fun x => (a :: xs).count x * f x)).sum =
            ((ys.eraseDups).map (fun x => ys.count x * f x)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro x hx
          rw [hcount x hx]
        have hfilter := sum_map_filter_ne_count xs a f
        rw [List.eraseDups_cons]
        change ((a :: ys.eraseDups).map (fun x => (a :: xs).count x * f x)).sum =
          ((a :: xs).map f).sum
        simp only [List.map_cons, List.sum_cons]
        rw [List.count_cons_self]
        change ((xs.count a + 1) * f a + ((ys.eraseDups).map (fun x => (a :: xs).count x * f x)).sum) = _
        rw [hmap, ihys, hfilter]
        change (xs.count a + 1) * f a + (ys.map f).sum =
          f a + (xs.count a * f a + (ys.map f).sum)
        rw [Nat.add_mul, Nat.one_mul]
        omega
  exact hP xs.length xs rfl

/-! ## The goal: cache one interval query per distinct left key. -/

theorem sound_RepeatedProbe : ModuloClass.RepeatedProbe.Goal := by
  intro u t xs ys hv
  have hfan : ∀ x, fan search u t x ys = (ys.filter (search.test t x)).length := by
    intro x
    exact modulo_fan u t x hv
  have hmap : xs.eraseDups.map (fun x => xs.count x * fan search u t x ys) =
      xs.eraseDups.map (fun x => xs.count x * (ys.filter (search.test t x)).length) := by
    apply List.map_congr_left
    intro x hx
    rw [hfan x]
  rw [hmap, List.length_flatMap]
  exact sum_map_eraseDups_count_mul xs (fun x => (ys.filter (search.test t x)).length)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_RepeatedProbe
