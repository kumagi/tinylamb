import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_RepeatedProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice

private theorem optional_eq (a b : Nat) :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = if a < b then [(a, b)] else [] := rfl

private theorem optional_none {a b : Nat} (h : ¬ a < b) : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = [] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_some {a b : Nat} (h : a < b) : TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b = [(a, b)] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

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

private theorem eraseDups_nodup {α : Type} [DecidableEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  induction h : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
    cases xs with
    | nil => simp
    | cons a xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        have hf := List.mem_eraseDups.mp ha
        have hm := List.mem_filter.mp hf
        simp at hm
      · apply ih ((xs.filter fun b => !(b == a)).length)
        · have hle := List.length_filter_le (l := xs) (p := fun b => !(b == a))
          have hn : n = xs.length + 1 := by simpa using h.symm
          omega
        · rfl

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

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
    · have hmem : a ∈ bs := by
        grind
      simp [hba, ih hnd'.2 hmem]

private theorem count_cons_add {α : Type} [DecidableEq α] [LawfulBEq α]
    (a : α) (xs : List α) (x : α) :
    (a :: xs).count x = xs.count x + if x = a then 1 else 0 := by
  by_cases hxa : x = a
  · subst x
    simp [List.count_cons_self]
  · rw [List.count_cons_of_ne (Ne.symm hxa)]
    simp [hxa]

private theorem sum_count_mul_eq {α : Type} [DecidableEq α] [LawfulBEq α]
    (ds xs : List α) (f : α → Nat) (hnd : ds.Nodup)
    (hmem : ∀ x ∈ xs, x ∈ ds) :
    (ds.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  induction xs with
  | nil =>
      simpa using (sum_zero_map ds (fun _ => 0) (by simp))
  | cons a xs ih =>
    have hmem' : ∀ x ∈ xs, x ∈ ds := fun x hx => hmem x (by simp [hx])
    have ha : a ∈ ds := hmem a (by simp)
    have ih' := ih hmem'
    rw [List.map_cons, List.sum_cons, ← ih']
    rw [show ds.map (fun x => (a :: xs).count x * f x) =
        ds.map (fun x => xs.count x * f x + (if x = a then f a else 0)) from by
      apply List.map_congr_left
      intro x hx
      rw [count_cons_add]
      by_cases hxa : x = a <;> simp [hxa, Nat.add_mul]]
    rw [sum_map_add_nat, sum_indicator_nodup ds hnd a (f a) ha]
    omega

private theorem sum_map_eraseDups_count_mul {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  exact sum_count_mul_eq xs.eraseDups xs f (eraseDups_nodup xs)
    (fun x hx => by simpa [List.mem_eraseDups] using hx)

/-! ## Fanout of the reciprocal-price search equals the filtered multiplicity. -/

private theorem filter_lt_min_eq_filter_mul {u t x : Nat} {ys : List Nat}
    (hv : valid u ys) (hxpos : 0 < x) :
    ys.filter (fun y => decide (y < min u (t / x + 1))) = ys.filter (search.test t x) := by
  apply List.filter_congr
  intro y hy
  change decide (y < min u (t / x + 1)) = decide (x * y ≤ t)
  apply Bool.eq_iff_iff.mpr
  simp only [decide_eq_true_eq]
  constructor
  · intro hylt
    have hylt' : y < t / x + 1 := Nat.lt_of_lt_of_le hylt (Nat.min_le_right _ _)
    have hyle : y ≤ t / x := by omega
    have hmul : y * x ≤ t := (Nat.le_div_iff_mul_le hxpos).mp hyle
    simpa [Nat.mul_comm] using hmul
  · intro hmul
    have hyle : y ≤ t / x := by
      rw [Nat.le_div_iff_mul_le hxpos]
      simpa [Nat.mul_comm] using hmul
    have hylt' : y < t / x + 1 := by omega
    exact Nat.lt_min.mpr ⟨valid_lt hv hy, hylt'⟩

private theorem fan_eq_filter (u t x : Nat) {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = (ys.filter (search.test t x)).length := by
  have hs : ys.Pairwise (· ≤ ·) := valid_pairwise hv
  unfold fan
  change ((optional 0 (if x == 0 then u else min u (t / x + 1))).map
      (fun (a, b) => lower ys b - lower ys a)).sum = (ys.filter (search.test t x)).length
  by_cases hx : x == 0
  · rw [ite_eq_left hx]
    have hx0 : x = 0 := by simpa using hx
    subst x
    by_cases hu : 0 < u
    · rw [optional_some hu]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      rw [lower_eq_filter_lt_length hs u, lower_eq_filter_lt_length hs 0]
      have hz : ys.filter (fun y => decide (y < 0)) = [] := by
        rw [List.filter_eq_nil_iff]
        intro y hy
        simp
      rw [hz, List.length_nil, Nat.sub_zero]
      have h1 : ys.filter (fun y => decide (y < u)) = ys := by
        rw [List.filter_eq_self]
        intro y hy
        exact decide_eq_true_eq.mpr (valid_lt hv hy)
      have h2 : ys.filter (search.test t 0) = ys := by
        rw [List.filter_eq_self]
        intro y hy
        change decide (0 * y ≤ t) = true
        simp
      rw [h1, h2]
    · rw [optional_none hu]
      simp only [List.map_nil, List.sum_nil]
      have hys : ys = [] := by
        cases ys with
        | nil => rfl
        | cons y ys' =>
            have hyu : y < u := valid_lt hv (by simp)
            omega
      rw [hys]
      simp
  · rw [ite_eq_right hx]
    have hxpos : 0 < x := Nat.pos_of_ne_zero (by simpa using hx)
    by_cases hb : 0 < min u (t / x + 1)
    · rw [optional_some hb]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      rw [lower_eq_filter_lt_length hs (min u (t / x + 1)), lower_eq_filter_lt_length hs 0]
      have hz : ys.filter (fun y => decide (y < 0)) = [] := by
        rw [List.filter_eq_nil_iff]
        intro y hy
        simp
      rw [hz, List.length_nil, Nat.sub_zero]
      rw [filter_lt_min_eq_filter_mul hv hxpos]
    · rw [optional_none hb]
      simp only [List.map_nil, List.sum_nil]
      have hu0 : u = 0 := by
        have hmin0 : min u (t / x + 1) = 0 := Nat.eq_zero_of_not_pos hb
        rcases Nat.min_eq_zero_iff.mp hmin0 with hu0 | hdiv0
        · exact hu0
        · have hpos : 0 < t / x + 1 := Nat.succ_pos _
          omega
      have hys : ys = [] := by
        cases ys with
        | nil => rfl
        | cons y ys' =>
            have hyu : y < u := valid_lt hv (by simp)
            omega
      rw [hys]
      simp

/-! ## The goal: one interval query per distinct left key, weighted by multiplicity. -/

theorem sound_RepeatedProbe : ReciprocalPrice.RepeatedProbe.Goal := by
  intro u t xs ys hv
  have hfan : ∀ x, fan search u t x ys = (ys.filter (search.test t x)).length := by
    intro x
    exact fan_eq_filter u t x hv
  have hmap : xs.eraseDups.map (fun x => xs.count x * fan search u t x ys) =
      xs.eraseDups.map (fun x => xs.count x * (ys.filter (search.test t x)).length) := by
    apply List.map_congr_left
    intro x hx
    rw [hfan x]
  rw [hmap, List.length_flatMap]
  exact sum_map_eraseDups_count_mul xs (fun x => (ys.filter (search.test t x)).length)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_RepeatedProbe
