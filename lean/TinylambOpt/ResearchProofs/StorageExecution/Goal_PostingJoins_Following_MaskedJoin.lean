import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_MaskedJoin

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following

/-! Self-contained certificate for `Following.MaskedJoin`.

The binary lower bound cuts the sorted right-key column exactly at the filter
boundary, so the two-offset fan equals the number of matching keys.  Deleting
right positions by index subtracts exactly the matching deleted positions:
`eraseDups` turns the deletion list into a set, while `zipIdx` assigns every
right position a unique index. -/

private theorem arr_getD (xs : Array Nat) (i : Nat) : xs[i]?.getD 0 = xs.toList.getD i 0 := by
  by_cases h : i < xs.size
  · have h1 : xs[i]? = some xs[i] := Array.getElem?_eq_getElem h
    have h2 : i < xs.toList.length := by simpa using h
    have h3 : xs.toList[i]? = some xs.toList[i] := List.getElem?_eq_getElem h2
    simp only [h1, Option.getD_some]
    simp [List.getD, h3]
  · have hle : xs.size ≤ i := by omega
    have h1 : xs[i]? = none := Array.getElem?_eq_none hle
    have h2 : xs.toList.length ≤ i := by simpa using hle
    have h3 : xs.toList[i]? = none := List.getElem?_eq_none h2
    simp only [h1, Option.getD_none]
    simp [List.getD, h3]

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by simp at hf; omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, ⟨Nat.le_refl _, Nat.le_refl _⟩⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        rw [searchBound]
        simp only [if_pos (Nat.le_refl lo)]
        exact ⟨⟨hbelow, habove⟩, ⟨Nat.le_refl _, Nat.le_refl _⟩⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs.toList.getD mid 0 := arr_getD xs mid
        have hpow : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by rw [Nat.pow_succ]; omega
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        rw [hval]
        by_cases hv : xs.toList.getD mid 0 < v
        · rw [if_pos hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases hjmid : j = mid
            · subst j; exact hv
            · have hjlt : j < mid := by omega
              have hjlen : j < xs.toList.length := by omega
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  have hpair := hs.rel_getElem_of_lt hjlen hmidlen hjlt
                  rw [← getD_eq_getElem hjlen, ← getD_eq_getElem hmidlen] at hpair
                  exact hpair
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, Nat.le_trans (Nat.le_trans hml (Nat.le_succ mid)) hlb, hub⟩
        · rw [if_neg hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases hjmid : j = mid
            · subst j; omega
            · have hmj : mid < j := by omega
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  have hpair := hs.rel_getElem_of_lt hmidlen hjlen hmj
                  rw [← getD_eq_getElem hmidlen, ← getD_eq_getElem hjlen] at hpair
                  exact hpair
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid (by omega) (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst n
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hp : 2 ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by rw [Nat.pow_succ]; omega
    rw [hp]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    simpa using width_pow_bound ys.length
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen
        have hjl : j < ys.length := by simpa using hjlen
        omega)
    (by simpa using hs)
  simpa [arr_getD] using h

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  let i := lower ys v
  have hcut : Cut ys v i := by simpa [i] using (lower_cut hs v).1
  have hle : i ≤ ys.length := by simpa [i] using (lower_cut hs v).2
  have htake : (ys.take i).filter (fun x => decide (x < v)) = ys.take i := by
    apply List.filter_eq_self.mpr
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨j, hj, rfl⟩
    simp only [decide_eq_true_eq]
    have hji : j < i := by omega
    have hjys : j < ys.length := by omega
    have hlt : ys.getD j 0 < v := hcut.1 j hji
    rwa [getD_eq_getElem hjys] at hlt
  have hdrop : (ys.drop i).filter (fun x => decide (x < v)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨j, hj, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    intro hlt
    have hjlen : i + j < ys.length := by
      have ht : j < (ys.drop i).length := hj
      rw [List.length_drop] at ht
      omega
    have hge : v ≤ ys.getD (i + j) 0 := hcut.2 (i + j) (Nat.le_add_right _ _) hjlen
    rw [getD_eq_getElem hjlen] at hge
    omega
  have hsplit := (List.take_append_drop i ys).symm
  calc
    i = (ys.take i).length := by rw [List.length_take]; simp [Nat.min_eq_left hle]
    _ = ((ys.take i).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take i).filter (fun x => decide (x < v)) ++
          (ys.drop i).filter (fun x => decide (x < v))).length := by rw [hdrop, List.append_nil]
    _ = ((ys.take i ++ ys.drop i).filter (fun x => decide (x < v))).length := by rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by
        exact congrArg (fun l : List Nat => (l.filter (fun x => decide (x < v))).length) hsplit.symm

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    ys.filter (fun y => decide (y < b)) =
      ys.filter (fun y => decide (y < a)) ++ ys.filter (fun y => decide (a ≤ y && y < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnila : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnila, ih hs']
        · have hnila : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnilab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnila, hnilab, ih hs']

private theorem lower_interval {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  have hsplit := filter_lt_prefix_filter_lt hs (ys := ys) hab
  rw [hsplit, List.length_append]
  omega

private theorem test_iff_interval {u t x y : Nat} (hyu : y < u) :
    (search.test t x y = true) ↔ x + 1 ≤ y ∧ y < min u (x + t + 1) := by
  simp only [search, Bool.and_eq_true, decide_eq_true_eq]
  rw [Nat.lt_min]
  constructor
  · rintro ⟨hxy, hyt⟩
    exact ⟨by omega, hyu, by omega⟩
  · rintro ⟨hay, _, hylt⟩
    exact ⟨by omega, by omega⟩

private theorem fan_following {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  unfold fan search
  by_cases h : x + 1 < min u (x + t + 1)
  · simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, ↓reduceIte,
      List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    have hle : x + 1 ≤ min u (x + t + 1) := Nat.le_of_lt h
    rw [lower_interval hs hle]
    rw [List.countP_eq_length_filter]
    congr 1
    apply List.filter_congr
    intro y hy
    have hyu : y < u := hv.2 y hy
    have hiff := test_iff_interval (u := u) (t := t) (x := x) (y := y) hyu
    by_cases hp : x + 1 ≤ y ∧ y < min u (x + t + 1)
    · have ht : search.test t x y = true := hiff.mpr hp
      have hb : x < y ∧ y ≤ x + t := by
        simpa only [search, Bool.and_eq_true, decide_eq_true_eq] using ht
      simp [hp, hb.1, hb.2]
    · have hb : ¬ (x < y ∧ y ≤ x + t) := by
        intro h'
        exact hp (hiff.mp (by simpa only [search, Bool.and_eq_true, decide_eq_true_eq] using h'))
      simp [hp, hb]
      omega
  · simp only [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h, ↓reduceIte,
      List.map_nil, List.sum_nil]
    rw [List.countP_eq_length_filter]
    have hnil : ys.filter (fun y => decide (x < y) && decide (y ≤ x + t)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      have hyu : y < u := hv.2 y hy
      have hiff := test_iff_interval (u := u) (t := t) (x := x) (y := y) hyu
      intro ht
      have hp := hiff.mp (by simpa only [search, Bool.and_eq_true, decide_eq_true_eq] using ht)
      have hminle : min u (x + t + 1) ≤ x + 1 := by omega
      omega
    simp [hnil]
private theorem eraseDups_nodup (xs : List Nat) : xs.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ xs : List Nat, xs.length = n → xs.eraseDups.Nodup
  have H : ∀ n, motive n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hlen
        cases xs with
        | nil => simp
        | cons a xs =>
            rw [List.eraseDups_cons, List.nodup_cons]
            constructor
            · intro ha
              have hf := List.mem_filter.mp (List.mem_eraseDups.mp ha)
              simpa using hf.2
            · have hlen' : xs.length + 1 = n := by simpa using hlen
              have hlt : (xs.filter (fun b => !b == a)).length < n := by
                have hle : (xs.filter (fun b => !b == a)).length ≤ xs.length := List.length_filter_le _ _
                omega
              exact ih _ hlt _ rfl
  exact H xs.length xs rfl

private theorem countP_zipIdx_aux (ys : List Nat) (p : Nat → Bool) (off : Nat) :
    ys.countP p = (ys.zipIdx off).countP (fun q => p q.1) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      by_cases hp : p y <;> simpa [List.zipIdx_cons, hp] using ih (off + 1)

private theorem countP_partition (xs : List (Nat × Nat)) (p : Nat → Bool) (ds : List Nat) :
    xs.countP (fun q => p q.1) =
      (xs.filter (fun q => !ds.contains q.2)).countP (fun q => p q.1) +
        (xs.filter (fun q => ds.contains q.2)).countP (fun q => p q.1) := by
  induction xs with
  | nil => simp
  | cons q xs ih =>
      by_cases hd : ds.contains q.2 <;> by_cases hp : p q.1 <;>
        simp only [List.countP_cons, List.filter_cons, hd, hp, Bool.not_true, Bool.not_false,
          Bool.true_eq_false, Bool.false_eq_true, ↓reduceIte] <;> omega

private theorem range_filter_shift (n : Nat) (q : Nat → Bool) :
    ((List.range (n + 1)).filter q).length =
      (if q 0 then 1 else 0) + ((List.range n).filter (fun j => q (j + 1))).length := by
  induction n with
  | zero =>
      by_cases h0 : q 0 = true <;> simp [h0]
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih, List.range_succ,
        List.filter_append, List.length_append]
      by_cases h0 : q 0 = true <;> by_cases hn : q (n + 1) = true <;> simp [h0, hn] <;> omega

private theorem zipIdx_filter_length_aux (ys : List Nat) (ds : List Nat) (p : Nat → Bool) (off : Nat) :
    ((ys.zipIdx off).filter (fun q => ds.contains q.2 && p q.1)).length =
      ((List.range ys.length).filter (fun j => ds.contains (off + j) && p (ys.getD j 0))).length := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons]
      simp only [List.length_cons]
      rw [range_filter_shift]
      simp only [Nat.add_zero, Nat.zero_add, List.getD_cons_zero]
      have htail :
          ((ys.zipIdx (off + 1)).filter (fun q => ds.contains q.2 && p q.1)).length =
            ((List.range ys.length).filter
              (fun j => ds.contains (off + (j + 1)) && p ((y :: ys).getD (j + 1) 0))).length := by
        rw [ih (off + 1)]
        congr 1
        apply List.filter_congr
        intro j hj
        simp only [List.getD_cons_succ]
        have heq : off + 1 + j = off + (j + 1) := by omega
        rw [heq]
      simp only [List.filter_cons, List.length_cons]
      by_cases h : (ds.contains off && p y) = true
      · rw [h]
        simp only [↓reduceIte, List.length_cons]
        rw [htail]
        omega
      · have hf : (ds.contains off && p y) = false := by
          cases h' : ds.contains off && p y
          · rfl
          · exact False.elim (h h')
        rw [hf]
        simp only [Bool.false_eq_true, ↓reduceIte, Nat.zero_add]
        rw [htail]
private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).length := by
  rw [List.countP_eq_length_filter]
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases h : i < ys.length
    · have hsome : ys[i]? = some ys[i] := List.getElem?_eq_getElem h
      have hget : ys.getD i 0 = ys[i] := getD_eq_getElem h
      simp [h, hsome, hget]
    · have hnone : ys[i]? = none := List.getElem?_eq_none (by omega)
      simp [h, hnone]
  rw [hlhs]
  have hperm :
      (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).Perm
        ((List.range ys.length).filter (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0))) := by
    apply (List.perm_ext_iff_of_nodup ((eraseDups_nodup ds).filter _) (List.nodup_range.filter _)).mpr
    intro i
    constructor
    · intro hi
      rw [List.mem_filter] at hi
      rw [List.mem_filter, List.mem_range]
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      have hids : i ∈ ds := List.mem_eraseDups.mp hi.1
      have hlt : i < ys.length := by
        have := hi.2
        simp only [decide_eq_true_eq, Bool.and_eq_true] at this
        exact this.1
      have hp' : p (ys.getD i 0) = true := by
        have := hi.2
        simp only [decide_eq_true_eq, Bool.and_eq_true] at this
        exact this.2
      exact ⟨hlt, List.mem_eraseDups.mpr hids, hp'⟩
    · intro hi
      rw [List.mem_filter] at hi
      rw [List.mem_range] at hi
      simp only [decide_eq_true_eq, Bool.and_eq_true] at hi
      rw [List.mem_filter]
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      exact ⟨hi.2.1, hi.1, hi.2.2⟩
  have hright :
      ((List.range ys.length).filter (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0))).length =
        ((List.range ys.length).filter (fun j => ds.contains j && p (ys.getD j 0))).length := by
    congr 1
    apply List.filter_congr
    intro j hj
    simp [List.mem_eraseDups]
  have hz := zipIdx_filter_length_aux ys ds p 0
  simp only [Nat.zero_add] at hz
  have hgoal :
      (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).length =
        ((List.range ys.length).filter (fun j => ds.contains j && p (ys.getD j 0))).length := by
    rw [← hright]
    exact hperm.length_eq
  rw [hz]
  exact hgoal

private theorem masked_from_fan {u t x : Nat} {ys ds : List Nat} (p : Nat → Bool)
    (hfan : fan search u t x ys = ys.countP p) :
    fan search u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any p) =
      ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p := by
  rw [hfan]
  have hz : ys.countP p = ys.zipIdx.countP (fun q => p q.1) := by
    simpa using countP_zipIdx_aux ys p 0
  have hpart := countP_partition ys.zipIdx p ds
  have hdel : ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun q => ds.contains q.2)).countP (fun q => p q.1) := by
    rw [deleted_count_eq]
    rw [List.countP_eq_length_filter]
    have hff :
        (ys.zipIdx.filter (fun q => ds.contains q.2)).filter (fun q => p q.1) =
          ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1) := by
      rw [List.filter_filter]
      apply List.filter_congr
      intro q hq
      by_cases hp : p q.1 <;> simp [hp]
    rw [← hff]
  have hsurv :
      ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p =
        (ys.zipIdx.filter (fun q => !ds.contains q.2)).countP (fun q => p q.1) := by
    rw [List.countP_map]
    rfl
  rw [hz, hdel, hsurv, hpart]
  omega

theorem sound_MaskedJoin :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following.MaskedJoin.Goal := by
  intro u t x ys ds hv
  exact masked_from_fan (search.test t x) (fan_following hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_MaskedJoin
