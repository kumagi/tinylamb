import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ReciprocalPrice.MaskedJoin` — A point-delete bitmap on the right can subtract
  removed multiplicities from the offset count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_MaskedJoin

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice

/-! ## Small-input truth check (valid sorted right lists over [0,u)) -/

private def pow (xs : List Nat) : List (List Nat) :=
  xs.foldr (fun a acc => acc.flatMap (fun ys => [ys, a :: ys])) [[]]

private def eqcase (u t x : Nat) (ys ds : List Nat) : Bool :=
  fan search u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any (search.test t x)) ==
    ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP (search.test t x)

private def check : Bool :=
  (List.range 5).all fun u =>
  (List.range 3).all fun t =>
  (List.range (u + 2)).all fun x =>
  (pow (List.range u)).all fun ys =>
  (pow (List.range (u + 2))).all fun ds =>
    eqcase u t x ys ds

#eval check
example : check := by native_decide

/-! ## The binary lower bound on a sorted right column -/

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, hi]

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by
        have h2 : (2 : Nat) ^ 0 = 1 := by simp
        omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, trivial, trivial⟩
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
          rw [Nat.pow_succ]
          omega
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
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, Nat.le_trans (by omega : lo ≤ mid + 1) hlb, hub⟩
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
                  rw [hlistval, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
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
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [List.getElem_take, decide_eq_true_eq]
    have hi' : i < lower ys v := by omega
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v hi'
    rwa [getD_eq_getElem hiys] at this
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [List.getElem_drop, decide_eq_true_eq]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    by_cases hxlt : ys[lower ys v + i] < v
    · omega
    · simp [hxlt]
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take, Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

private theorem lower_min {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (hv : ∀ y ∈ ys, y < u) (v : Nat) :
    lower ys (min u v) = lower ys v := by
  rw [lower_eq_filter_lt_length hs, lower_eq_filter_lt_length hs]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  by_cases hyv : y < v
  · have hymin : y < min u v := Nat.lt_min.mpr ⟨hv y hy, hyv⟩
    simp [hyv, hymin]
  · have hymin : ¬ y < min u v := by
      intro h
      exact hyv (Nat.lt_min.mp h).2
    simp [hyv, hymin]

private theorem lower_zero {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) : lower ys 0 = 0 := by
  rw [lower_eq_filter_lt_length hs]
  simp

private theorem optional_eq (a b : Nat) : optional a b = if a < b then [(a, b)] else [] := rfl

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [optional_eq, h]

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [optional_eq, h]

/-! ## The reciprocal-price span describes exactly the matching right keys -/

private theorem prod_spec (t x y : Nat) (hx : 0 < x) :
    y < t / x + 1 ↔ x * y ≤ t := by
  rw [Nat.lt_succ_iff]
  rw [Nat.le_div_iff_mul_le hx]
  rw [Nat.mul_comm y x]

private theorem fan_search_eq_countP (u t x : Nat) (ys : List Nat) (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  unfold fan
  simp only [search]
  by_cases hx0 : x = 0
  · subst x
    simp only [beq_self_eq_true, ↓reduceIte]
    by_cases hu : 0 < u
    · rw [optional_some hu]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      rw [lower_zero hv.1]
      have hlu : lower ys u = ys.length := by
        rw [lower_eq_filter_lt_length hv.1]
        have hf : ys.filter (fun y => decide (y < u)) = ys := by
          rw [List.filter_eq_self]
          intro y hy
          simpa using hv.2 y hy
        rw [hf]
      rw [hlu]
      rw [List.countP_eq_length_filter]
      simp only [Nat.zero_mul, Nat.zero_le, decide_true]
      have hftrue : ys.filter (fun y => true) = ys := by
        rw [List.filter_eq_self]
        intro y hy
        rfl
      rw [hftrue]
      omega
    · have hu0 : u = 0 := by omega
      subst u
      rw [optional_none (by simp : ¬ 0 < 0)]
      simp only [List.map_nil, List.sum_nil]
      have hnil : ys = [] := by
        cases ys with
        | nil => rfl
        | cons y ys =>
            have := hv.2 y (by simp)
            omega
      subst ys
      simp
  · have hbeq : (x == 0) = false := by simp [hx0]
    simp only [hbeq, Bool.false_eq_true, ↓reduceIte]
    let b := min u (t / x + 1)
    change ((optional 0 b).map (fun r : Nat × Nat => lower ys r.2 - lower ys r.1)).sum =
      ys.countP (fun y => decide (x * y ≤ t))
    by_cases hb : 0 < b
    · rw [optional_some hb]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      rw [lower_zero hv.1]
      have hlow : lower ys b = lower ys (t / x + 1) := by
        dsimp [b]
        exact lower_min hv.1 hv.2 (t / x + 1)
      rw [hlow, lower_eq_filter_lt_length hv.1]
      rw [List.countP_eq_length_filter]
      apply congrArg List.length
      apply List.filter_congr
      intro y hy
      have hxpos : 0 < x := Nat.pos_of_ne_zero hx0
      by_cases hp : y < t / x + 1
      · have ht : x * y ≤ t := (prod_spec t x y hxpos).mp hp
        simp [hp, ht]
      · have ht : ¬ x * y ≤ t := by
          intro h
          exact hp ((prod_spec t x y hxpos).mpr h)
        simp [hp, ht]
    · rw [optional_none hb]
      simp only [List.map_nil, List.sum_nil]
      rw [List.countP_eq_length_filter]
      have hnil : ys.filter (fun y => decide (x * y ≤ t)) = [] := by
        rw [List.filter_eq_nil_iff]
        intro y hy hdec
        simp only [decide_eq_true_eq] at hdec
        have hxpos : 0 < x := Nat.pos_of_ne_zero hx0
        have hylt : y < t / x + 1 := (prod_spec t x y hxpos).mpr hdec
        have hu0 : 0 < u := Nat.lt_of_le_of_lt (Nat.zero_le y) (hv.2 y hy)
        have hb' : 0 < min u (t / x + 1) := Nat.lt_min.mpr ⟨hu0, Nat.succ_pos _⟩
        exact hb (by simpa [b] using hb')
      rw [hnil]
      simp

/-! ## Generic counting over zipIdx -/

private theorem zipIdx_eq_map_range (ys : List Nat) (off : Nat) :
    ys.zipIdx off = (List.range ys.length).map (fun i => (ys.getD i 0, off + i)) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons, ih (off + 1)]
      rw [show (y :: ys).length = ys.length + 1 by simp, List.range_succ_eq_map]
      simp only [List.map_cons]
      apply congrArg (List.cons (y, off))
      rw [List.map_map]
      apply List.map_congr_left
      intro i hi
      simp [Nat.succ_eq_add_one]
      omega

private theorem countP_range_getD (ys : List Nat) (p : Nat → Bool) :
    (List.range ys.length).countP (fun i => p (ys.getD i 0)) = ys.countP p := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      rw [show (y :: ys).length = ys.length + 1 by simp, List.range_succ_eq_map,
        List.countP_cons, List.countP_map]
      change (List.countP (fun i => p (ys.getD i 0)) (List.range ys.length) +
          if p y = true then 1 else 0) = List.countP p (y :: ys)
      rw [ih, List.countP_cons]

private theorem rhs_eq_range (ys ds : List Nat) (p : Nat → Bool) :
    ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p =
      (List.range ys.length).countP (fun i => p (ys.getD i 0) && !ds.contains i) := by
  rw [zipIdx_eq_map_range ys 0]
  simp only [Nat.zero_add]
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  rw [List.filter_map]
  rw [List.filter_map]
  rw [List.filter_map]
  rw [List.filter_filter]
  rw [List.map_map]
  rw [List.length_map]
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  simp only [Function.comp_apply]

private theorem eraseDups_nodup (xs : List Nat) : xs.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ xs : List Nat, xs.length = n → xs.eraseDups.Nodup
  have hP : P xs.length := Nat.strongRecOn xs.length (fun n ih => by
    intro xs hlen
    cases xs with
    | nil => simp
    | cons a xs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro ha
          have hf := List.mem_eraseDups.mp ha
          simp at hf
        · have hsub : (List.filter (fun b => ! b == a) xs).Sublist xs :=
            (List.filter_sublist : (List.filter (fun b => ! b == a) xs).Sublist xs)
          have hlt : (List.filter (fun b => ! b == a) xs).length < n := by
            have hlen' : xs.length + 1 = n := by simpa using hlen
            have hxlen : xs.length < n := by omega
            have := hsub.length_le
            omega
          exact ih _ hlt _ rfl)
  exact hP xs rfl

private theorem filter_mem_length_eq (xs ys : List Nat) (p : Nat → Bool)
    (hxs : xs.Nodup) (hys : ys.Nodup) :
    (xs.filter (fun i => p i && decide (i ∈ ys))).length =
      (ys.filter (fun i => p i && decide (i ∈ xs))).length := by
  have hm : (xs.filter (fun i => decide (i ∈ ys))).Perm
      (ys.filter (fun i => decide (i ∈ xs))) := by
    apply (List.perm_ext_iff_of_nodup (hxs.filter _) (hys.filter _)).2
    intro i
    simp only [List.mem_filter]
    constructor
    · intro h
      have hi : i ∈ ys := by simpa using h.2
      exact ⟨hi, by simpa using h.1⟩
    · intro h
      have hi : i ∈ xs := by simpa using h.2
      exact ⟨hi, by simpa using h.1⟩
  have hp := hm.filter p
  simpa only [List.filter_filter] using hp.length_eq

private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (List.range ys.length).countP (fun i => ds.contains i && p (ys.getD i 0)) := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases hidx : i < ys.length
    · rw [List.getElem?_eq_getElem hidx]
      simp [hidx, getD_eq_getElem hidx]
    · rw [List.getElem?_eq_none (by omega : ys.length ≤ i)]
      simp [hidx]
  rw [hlhs]
  have hlen := filter_mem_length_eq ds.eraseDups (List.range ys.length)
    (fun i => p (ys.getD i 0)) (eraseDups_nodup ds) List.nodup_range
  have hrange : ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) =
      ds.eraseDups.filter (fun i => p (ys.getD i 0) && decide (i ∈ List.range ys.length)) := by
    apply List.filter_congr
    intro i hi
    simp only [List.mem_range]
    by_cases hlt : i < ys.length <;> by_cases hpi : p (ys.getD i 0) = true <;> simp [hlt, hpi]
  rw [hrange, hlen]
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  rw [List.contains_eq_mem]
  have hdec : decide (i ∈ ds) = decide (i ∈ ds.eraseDups) := by
    by_cases h : i ∈ ds
    · have h' : i ∈ ds.eraseDups := (List.mem_eraseDups).2 h
      simp [h, h']
    · have h' : ¬ i ∈ ds.eraseDups := by
        intro hm
        exact h ((List.mem_eraseDups).1 hm)
      simp [h, h']
  rw [hdec]
  cases hpv : p (ys.getD i 0) <;> cases hd' : decide (i ∈ ds.eraseDups) <;> rfl

private theorem countP_split (l : List Nat) (ds : List Nat) (f : Nat → Bool) :
    l.countP f = l.countP (fun i => ds.contains i && f i) +
      l.countP (fun i => f i && !ds.contains i) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.countP_cons]
      cases hd : ds.contains a <;> cases hf : f a <;> simp [hd, hf, ih] <;> omega

private theorem range_count_split (n : Nat) (ds : List Nat) (f : Nat → Bool) :
    (List.range n).countP f - (List.range n).countP (fun i => ds.contains i && f i) =
      (List.range n).countP (fun i => f i && !ds.contains i) := by
  have h := countP_split (List.range n) ds f
  omega

theorem sound_MaskedJoin : ReciprocalPrice.MaskedJoin.Goal := by
  intro u t x ys ds hv
  rw [fan_search_eq_countP u t x ys hv]
  let pred := search.test t x
  change ys.countP pred - ds.eraseDups.countP (fun i => ys[i]?.any pred) =
    ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP pred
  have htot := countP_split (List.range ys.length) ds (fun i => pred (ys.getD i 0))
  rw [countP_range_getD ys pred, ← deleted_count_eq ys ds pred,
    ← rhs_eq_range ys ds pred] at htot
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_MaskedJoin
