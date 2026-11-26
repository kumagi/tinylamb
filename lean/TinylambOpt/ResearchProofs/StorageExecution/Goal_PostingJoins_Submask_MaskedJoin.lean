import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_MaskedJoin

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask

/-! Self-contained certificate for `Submask.MaskedJoin`.

The binary lower bound cuts the sorted right-key column at every singleton
submask interval, so the fan equals the number of matching right keys.
Deleting right positions by index subtracts exactly the matching deleted
positions: `eraseDups` turns the deletion list into a set while `zipIdx`
assigns every right position a unique index. -/

/-! ## Small-input truth check (sorted right lists over [0,u)) -/

private def subs : List Nat → List (List Nat)
  | [] => [[]]
  | a :: xs => subs xs ++ (subs xs).map (fun ys => a :: ys)

private def eqcase (u t x : Nat) (ys ds : List Nat) : Bool :=
  fan search u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any (search.test t x)) ==
    ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP (search.test t x)

private def check : Bool :=
  (List.range 5).all fun u =>
  (List.range 3).all fun t =>
  (List.range (u + 2)).all fun x =>
  (subs (List.range u)).all fun ys =>
  (subs (List.range (u + 1))).all fun ds =>
    eqcase u t x ys ds

#eval check
example : check := by native_decide

/-! ## Binary lower bound on a sorted right column -/
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

/-! ## Counting matches in the deleted / surviving right positions -/
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

/-! ## The submask family: submasks of x are exactly the y with y &&& x == y -/

private theorem testBit_one_of_pos {k : Nat} (hk : k ≠ 0) : (1 : Nat).testBit k = false := by
  apply Nat.testBit_lt_two_pow
  simpa using (Nat.one_lt_two_pow hk)

private theorem testBit_add_two_pow (z B j : Nat) (hz : z < 2 ^ B) :
    (z + 2 ^ B).testBit j = (if j = B then true else if j < B then z.testBit j else false) := by
  rw [Nat.add_comm, show 2 ^ B = 2 ^ B * 1 by rw [Nat.mul_one]]
  rw [Nat.testBit_two_pow_mul_add (a := 1) (b := z) (i := B) hz j]
  by_cases hj : j = B
  · subst j
    simp
  · by_cases hlt : j < B
    · simp [hj, hlt]
    · have hk : j - B ≠ 0 := by omega
      rw [ite_eq_right hlt, testBit_one_of_pos hk]
      simp [hj, hlt]

private theorem range_pairwise (n : Nat) : (List.range n).Pairwise (· < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.pairwise_append]
      refine ⟨ih, ?_, ?_⟩
      · simp
      · intro a ha b hb
        simp at hb
        subst b
        simp at ha
        omega

private theorem filter_sublist_nat (p : Nat → Bool) (l : List Nat) : (l.filter p).Sublist l :=
  List.filter_sublist

private theorem bits_pairwise (x : Nat) : ((List.range (width x)).filter x.testBit).Pairwise (· < ·) :=
  (range_pairwise (width x)).sublist (filter_sublist_nat x.testBit (List.range (width x)))

private theorem bits_cover (x : Nat) : ∀ i, x.testBit i = true → i ∈ (List.range (width x)).filter x.testBit := by
  intro i hxi
  have hxne : x ≠ 0 := by
    intro hz
    subst x
    simp at hxi
  have h2le : 2 ^ i ≤ x := Nat.ge_two_pow_of_testBit hxi
  have hnotlt : ¬ x < 2 ^ i := by omega
  have hlog : ¬ x.log2 < i := by
    intro hlt
    exact hnotlt ((Nat.log2_lt hxne).mp hlt)
  have hiw : i < width x := by
    unfold width
    simp [hxne]
    omega
  exact List.mem_filter.mpr ⟨List.mem_range.mpr hiw, hxi⟩

private theorem pairwise_reverse_gt : ∀ {cs : List Nat}, cs.Pairwise (· < ·) → cs.reverse.Pairwise (· > ·) := by
  intro cs h
  induction cs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.reverse_cons, List.pairwise_append]
      have hp := List.pairwise_cons.mp h
      refine ⟨ih hp.2, ?_, ?_⟩
      · simp
      · intro a ha b hb
        simp at hb
        subst b
        have ha' : a ∈ xs := by simpa using ha
        exact hp.1 a ha'

private theorem sumBits_lt_reverse : ∀ (B : Nat) (l : List Nat), l.Pairwise (· > ·) →
    (∀ b ∈ l, b < B) → (l.map (fun b => 2 ^ b)).sum < 2 ^ B := by
  intro B l hl hB
  revert B
  induction l with
  | nil =>
      intro B hB
      exact Nat.two_pow_pos B
  | cons x xs ih =>
      intro B hB
      have hp := List.pairwise_cons.mp hl
      have hxlt : x < B := hB x (by simp)
      have hxsB : ∀ y ∈ xs, y < B := by
        intro y hy
        have hyx : y < x := hp.1 y hy
        exact Nat.lt_trans hyx hxlt
      have hbnd : (xs.map (fun b => 2 ^ b)).sum < 2 ^ x := ih hp.2 x (by intro y hy; exact hp.1 y hy)
      simp only [List.map_cons, List.sum_cons]
      have hp2 : 2 ^ x + 2 ^ x = 2 ^ (x + 1) := by
        rw [Nat.pow_succ]
        omega
      have hx1 : x + 1 ≤ B := by omega
      have hle : 2 ^ (x + 1) ≤ 2 ^ B := Nat.pow_le_pow_right (by decide) hx1
      omega

private theorem sumBits_lt_of_pairwise_lt {cs : List Nat} (hpair : cs.Pairwise (· < ·)) {B : Nat}
    (hB : ∀ b ∈ cs, b < B) : (cs.map (fun b => 2 ^ b)).sum < 2 ^ B := by
  have hrev := pairwise_reverse_gt hpair
  have h := sumBits_lt_reverse B cs.reverse hrev (by
    intro b hb
    exact hB b (by simpa using hb))
  simpa using h

private theorem testBit_sumBits_rev : ∀ (l : List Nat), l.Pairwise (· > ·) →
    ∀ i, ((l.map (fun b => 2 ^ b)).sum).testBit i = decide (i ∈ l) := by
  intro l hl i
  revert i
  induction l with
  | nil => intro i; simp
  | cons d xs ih =>
      intro i
      have hp := List.pairwise_cons.mp hl
      have hxsB : ∀ y ∈ xs, y < d := by intro y hy; exact hp.1 y hy
      have hbnd : (xs.map (fun b => 2 ^ b)).sum < 2 ^ d := sumBits_lt_reverse d xs hp.2 hxsB
      have hih := ih hp.2
      rw [List.map_cons, List.sum_cons, Nat.add_comm, testBit_add_two_pow _ d i hbnd]
      by_cases hid : i = d
      · subst i
        simp
      · by_cases hlti : i < d
        · simp [hid, hlti, hih]
        · have hixs : i ∉ xs := by
            intro hm
            have hyx : i < d := hp.1 i hm
            omega
          simp [hid, hlti, hixs, hih]

private theorem testBit_sumBits {cs : List Nat} (hpair : cs.Pairwise (· < ·)) (i : Nat) :
    ((cs.map (fun b => 2 ^ b)).sum).testBit i = decide (i ∈ cs) := by
  have hrev := pairwise_reverse_gt hpair
  have h := testBit_sumBits_rev cs.reverse hrev i
  have hsum : (cs.reverse.map (fun b => 2 ^ b)).sum = (cs.map (fun b => 2 ^ b)).sum := by
    simpa using (List.sum_reverse (cs.map (fun b => 2 ^ b)))
  rw [← hsum]
  simpa using h

private def addFold (acc : List Nat) (bit : Nat) : List Nat := acc ++ acc.map (fun z => z + 2 ^ bit)

private theorem foldl_add_mono {acc acc' : List Nat} (hsub : acc ⊆ acc') (bs : List Nat) :
    bs.foldl addFold acc ⊆ bs.foldl addFold acc' := by
  induction bs generalizing acc acc' with
  | nil => exact hsub
  | cons b bs ih =>
      simp only [List.foldl_cons]
      apply ih
      intro z hz
      simp only [addFold, List.mem_append, List.mem_map] at hz ⊢
      rcases hz with hz | ⟨u, hu, heq⟩
      · exact Or.inl (hsub hz)
      · exact Or.inr ⟨u, hsub hu, heq⟩

private theorem foldl_add_translate (K : Nat) (acc : List Nat) (bs : List Nat) :
    bs.foldl addFold (acc.map (fun z => z + K)) = (bs.foldl addFold acc).map (fun z => z + K) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      have hadd (l : List Nat) : addFold (l.map (fun z => z + K)) b = (addFold l b).map (fun z => z + K) := by
        induction l with
        | nil => simp [addFold]
        | cons a l ih =>
            simp [addFold, ih, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
      rw [hadd acc, ih]

private theorem sublist_cons_cases {cs l₂ : List Nat} {a : Nat}
    (h : cs.Sublist (a :: l₂)) : cs.Sublist l₂ ∨ ∃ cs', cs = a :: cs' ∧ cs'.Sublist l₂ := by
  cases h with
  | cons_cons b hsub => exact Or.inr ⟨_, rfl, hsub⟩
  | cons b hsub => exact Or.inl hsub

private theorem submasks_acc_contains (bs : List Nat) (cs : List Nat) (hcs : cs.Sublist bs) :
    (cs.map (fun b => 2 ^ b)).sum ∈ bs.foldl addFold [0] := by
  induction bs generalizing cs with
  | nil =>
      have hnil : cs = [] := by
        cases hcs
        rfl
      subst cs
      simp
  | cons b bs ih =>
      rcases sublist_cons_cases hcs with htail | ⟨cs', rfl, htail⟩
      · have hm := ih cs htail
        simp only [List.foldl_cons, addFold]
        apply foldl_add_mono (acc := [0]) (acc' := [0] ++ [0].map (fun z => z + 2 ^ b))
        · intro z hz
          simp at hz ⊢
          exact Or.inl hz
        · exact hm
      · have hm := ih cs' htail
        simp only [List.foldl_cons, addFold]
        have hu : (cs'.map (fun c => 2 ^ c)).sum + 2 ^ b ∈
            bs.foldl addFold ([0].map (fun z => z + 2 ^ b)) := by
          rw [foldl_add_translate]
          exact List.mem_map.mpr ⟨(cs'.map (fun c => 2 ^ c)).sum, hm, rfl⟩
        have hmono : bs.foldl addFold ([0].map (fun z => z + 2 ^ b)) ⊆
            bs.foldl addFold ([0] ++ [0].map (fun z => z + 2 ^ b)) := by
          apply foldl_add_mono
          intro z hz
          simp at hz ⊢
          exact Or.inr hz
        have hu' := hmono hu
        simpa [addFold, Nat.add_comm] using hu'

private theorem submasks_acc_subset {x : Nat} (bs : List Nat)
    (hbsx : ∀ b ∈ bs, x.testBit b = true)
    (hpair : bs.Pairwise (· < ·))
    (acc : List Nat)
    (hsubacc : ∀ z ∈ acc, ∀ i, z.testBit i = true → x.testBit i = true)
    (hbound : ∀ b ∈ bs, ∀ z ∈ acc, z < 2 ^ b) :
    ∀ z ∈ bs.foldl addFold acc, ∀ i, z.testBit i = true → x.testBit i = true := by
  induction bs generalizing acc with
  | nil => intro z hz; exact hsubacc z hz
  | cons b bs ih =>
      simp only [List.foldl_cons]
      let acc' := addFold acc b
      have hb_x : x.testBit b = true := hbsx b List.mem_cons_self
      have hsub' : ∀ w ∈ acc', ∀ i, w.testBit i = true → x.testBit i = true := by
        intro w hw i hi
        simp only [acc', addFold, List.mem_append, List.mem_map] at hw
        rcases hw with hw | ⟨u, hu, rfl⟩
        · exact hsubacc w hw i hi
        · by_cases hib : i = b
          · subst i
            exact hb_x
          · have hult : u < 2 ^ b := hbound b List.mem_cons_self u hu
            rw [testBit_add_two_pow u b i hult] at hi
            by_cases hlti : i < b
            · have hui : u.testBit i = true := by simpa [hib, hlti] using hi
              exact hsubacc u hu i hui
            · simp [hib, hlti] at hi
      have hbound' : ∀ b' ∈ bs, ∀ w ∈ acc', w < 2 ^ b' := by
        intro b' hb' w hw
        have hbb' : b < b' := (List.pairwise_cons.mp hpair).1 b' hb'
        have hb1 : b + 1 ≤ b' := by omega
        have hle : 2 ^ (b + 1) ≤ 2 ^ b' := Nat.pow_le_pow_right (by decide) hb1
        have hltpow : 2 ^ b < 2 ^ (b + 1) := Nat.pow_lt_pow_right (by decide) (by omega)
        simp only [acc', addFold, List.mem_append, List.mem_map] at hw
        rcases hw with hw | ⟨u, hu, rfl⟩
        · exact Nat.lt_of_lt_of_le (hbound b List.mem_cons_self w hw)
            (Nat.pow_le_pow_right (by decide) (Nat.le_of_lt hbb'))
        · have hsum : u + 2 ^ b < 2 ^ b + 2 ^ b := by
            have := hbound b List.mem_cons_self u hu
            omega
          have hp2 : 2 ^ b + 2 ^ b = 2 ^ (b + 1) := by
            rw [Nat.pow_succ]
            omega
          omega
      exact ih (fun c hc => hbsx c (List.mem_cons_of_mem _ hc)) (List.pairwise_cons.mp hpair).2
        acc' hsub' hbound'

private theorem submasks_sub {x y : Nat} (hy : y ∈ submasks x) : (y &&& x) == y := by
  unfold submasks at hy
  let bs := (List.range (width x)).filter x.testBit
  have hsub0 : ∀ z ∈ [0], ∀ i, z.testBit i = true → x.testBit i = true := by
    intro z hz
    simp at hz
    subst z
    intro i hi
    simp at hi
  have hbound0 : ∀ b ∈ bs, ∀ z ∈ [0], z < 2 ^ b := by
    intro b hb z hz
    simp at hz
    subst z
    exact Nat.two_pow_pos b
  have hres := submasks_acc_subset bs (fun b hb => (List.mem_filter.mp hb).2) (bits_pairwise x)
    [0] hsub0 hbound0 y hy
  rw [beq_iff_eq]
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_and]
  by_cases hyi : y.testBit i = true
  · have hxi := hres i hyi
    simp [hyi, hxi]
  · simp [hyi]

private theorem filter_sublist_of_imp (p q : Nat → Bool) (l : List Nat)
    (h : ∀ a ∈ l, p a = true → q a = true) : (l.filter p).Sublist (l.filter q) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have htail : ∀ b ∈ l, p b = true → q b = true := by
        intro b hb hpb
        exact h b (by simp [hb]) hpb
      by_cases hp : p a
      · have hq : q a = true := h a (by simp) hp
        rw [List.filter_cons, List.filter_cons]
        simp only [hp, hq, ↓reduceIte]
        exact List.Sublist.cons_cons a (ih htail)
      · by_cases hq : q a
        · rw [List.filter_cons, List.filter_cons]
          simp only [hp, hq, ↓reduceIte]
          exact List.Sublist.cons a (ih htail)
        · rw [List.filter_cons, List.filter_cons]
          simp only [hp, hq, ↓reduceIte]
          exact ih htail

private theorem mem_submasks_of_submask {x y : Nat} (hy : (y &&& x) == y) : y ∈ submasks x := by
  unfold submasks
  let bs := (List.range (width x)).filter x.testBit
  let cs := (List.range (width x)).filter y.testBit
  have hybits : ∀ i, y.testBit i = true → x.testBit i = true := by
    intro i hi
    have heq : y &&& x = y := by simpa [beq_iff_eq] using hy
    have hh := congrArg (fun n => Nat.testBit n i) heq
    simp only [Nat.testBit_and] at hh
    simpa [hi] using hh
  have hcs_sub : cs.Sublist bs := by
    dsimp [cs, bs]
    exact filter_sublist_of_imp y.testBit x.testBit (List.range (width x))
      (by intro a ha hpa; exact hybits a hpa)
  have hcs_pair : cs.Pairwise (· < ·) := by
    dsimp [cs]
    exact (range_pairwise (width x)).sublist (filter_sublist_nat y.testBit (List.range (width x)))
  have hy_eq : y = (cs.map (fun b => 2 ^ b)).sum := by
    apply Nat.eq_of_testBit_eq
    intro i
    rw [testBit_sumBits hcs_pair i]
    dsimp [cs]
    by_cases hi : y.testBit i = true
    · have hxi := hybits i hi
      have hirange : i ∈ List.range (width x) := (List.mem_filter.mp (bits_cover x i hxi)).1
      simp [List.mem_filter, hirange, hi]
    · have hnot : i ∉ (List.range (width x)).filter y.testBit := by
        intro hm
        exact hi (List.mem_filter.mp hm).2
      simp [hnot, hi]
  rw [hy_eq]
  exact submasks_acc_contains bs cs hcs_sub


private theorem sum_map_add_nat (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun y => f y + g y)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

private theorem beq_symm (a b : Nat) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := by simpa [Nat.beq_eq] using h
    simp [heq]
  · intro h
    have heq : b = a := by simpa [Nat.beq_eq] using h
    simp [heq]

private theorem sum_indicator_eq_count (l : List Nat) (z : Nat) :
    (l.map (fun y => if z == y then 1 else 0)).sum = l.count z := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [List.count_cons]
      rw [beq_symm z a]
      omega

private theorem count_eq_one_of_nodup {l : List Nat} (hnd : l.Nodup) {z : Nat} (hz : z ∈ l) :
    l.count z = 1 := by
  induction l with
  | nil => simp at hz
  | cons a l ih =>
      rw [List.count_cons]
      cases List.mem_cons.mp hz with
      | inl hza =>
          have hza' : z = a := hza
          subst z
          have hnot : a ∉ l := (List.nodup_cons.mp hnd).1
          have hz0 : l.count a = 0 := (List.count_eq_zero).mpr hnot
          simp [hz0]
      | inr hzl =>
          have hne : a ≠ z := by
            intro h
            subst a
            exact (List.nodup_cons.mp hnd).1 hzl
          have hbe : (a == z) = false := by simpa [beq_iff_eq] using hne
          have hz1 : l.count z = 1 := ih (List.nodup_cons.mp hnd).2 hzl
          simp [hbe, hz1]

private theorem indicator_sum_nodup (l : List Nat) (hnd : l.Nodup) (z : Nat) :
    (l.map (fun y => if z == y then 1 else 0)).sum = if decide (z ∈ l) then 1 else 0 := by
  rw [sum_indicator_eq_count]
  by_cases hz : z ∈ l
  · have hc : l.count z = 1 := count_eq_one_of_nodup hnd hz
    simp [hz, hc]
  · have hc : l.count z = 0 := (List.count_eq_zero).mpr hz
    simp [hz, hc]

private theorem countP_cons_split (z y : Nat) (zs : List Nat) :
    (z :: zs).countP (fun w => w == y) = zs.countP (fun w => w == y) + (if z == y then 1 else 0) := by
  simp [List.countP_cons, Nat.add_comm]

private theorem sum_countP_eq_countP_mem (l : List Nat) (hnd : l.Nodup) (xs : List Nat) :
    (l.map (fun y => xs.countP (fun z => z == y))).sum = xs.countP (fun z => decide (z ∈ l)) := by
  induction xs with
  | nil =>
      simp only [List.countP_nil]
      induction l with
      | nil => simp
      | cons a l ihi => simp [ihi (List.nodup_cons.mp hnd).2]
  | cons z zs ih =>
      rw [show (l.map (fun y => (z :: zs).countP (fun w => w == y))).sum =
          (l.map (fun y => zs.countP (fun w => w == y) + (if z == y then 1 else 0))).sum from by
        apply congrArg List.sum
        apply List.map_congr_left
        intro y hy
        exact countP_cons_split z y zs]
      rw [sum_map_add_nat, ih, indicator_sum_nodup l hnd z]
      simp [List.countP_cons]

private theorem foldl_add_nodup_bound (bs : List Nat) (acc : List Nat)
    (hbs : bs.Pairwise (· < ·)) (hacc : acc.Nodup)
    (hbound : ∀ b ∈ bs, ∀ z ∈ acc, z < 2 ^ b) :
    (bs.foldl addFold acc).Nodup := by
  induction bs generalizing acc with
  | nil => simpa using hacc
  | cons b bs ih =>
      have hp := List.pairwise_cons.mp hbs
      let acc' := addFold acc b
      have hacc' : acc'.Nodup := by
        unfold acc' addFold
        rw [List.nodup_append]
        refine ⟨hacc, ?_, ?_⟩
        · change (acc.map (fun z => z + 2 ^ b)).Pairwise (· ≠ ·)
          rw [List.pairwise_map]
          exact hacc.imp (by intro x y hne; omega)
        · intro z hzacc u hu
          rcases List.mem_map.mp hu with ⟨v, hv, rfl⟩
          have hzb := hbound b List.mem_cons_self z hzacc
          have hvb := hbound b List.mem_cons_self v hv
          omega
      have hbound' : ∀ b' ∈ bs, ∀ z ∈ acc', z < 2 ^ b' := by
        intro b' hb' z hz
        have hbb' : b < b' := hp.1 b' hb'
        have hb1 : b + 1 ≤ b' := by omega
        have hle : 2 ^ (b + 1) ≤ 2 ^ b' := Nat.pow_le_pow_right (by decide) hb1
        have hp2 : 2 ^ b + 2 ^ b = 2 ^ (b + 1) := by
          rw [Nat.pow_succ]
          omega
        unfold acc' addFold at hz
        simp only [List.mem_append, List.mem_map] at hz
        rcases hz with hz | ⟨u, hu, rfl⟩
        · exact Nat.lt_of_lt_of_le (hbound b List.mem_cons_self z hz)
            (Nat.pow_le_pow_right (by decide) (Nat.le_of_lt hbb'))
        · have hsum : u + 2 ^ b < 2 ^ b + 2 ^ b := by
            have := hbound b List.mem_cons_self u hu
            omega
          omega
      exact ih acc' hp.2 hacc' hbound'


private theorem submasks_nodup (x : Nat) : (submasks x).Nodup := by
  unfold submasks
  apply foldl_add_nodup_bound
  · exact bits_pairwise x
  · simp
  · intro b hb z hz
    simp at hz
    subst z
    exact Nat.two_pow_pos b

/-! ## Fan of Submask.search equals the count of matching right keys -/

private theorem mem_filtered_submasks (u t x y : Nat) :
    y ∈ (submasks x).filter (fun y => y < u && y ≤ t) ↔ (y &&& x) == y ∧ y < u ∧ y ≤ t := by
  constructor
  · intro h
    have hm := List.mem_filter.mp h
    have hyu : y < u := by
      have h := (Bool.and_eq_true_iff.mp hm.2).1
      simpa using h
    have hyt : y ≤ t := by
      have h := (Bool.and_eq_true_iff.mp hm.2).2
      simpa using h
    exact ⟨submasks_sub hm.1, hyu, hyt⟩
  · rintro ⟨hsub, hyu, hyt⟩
    exact List.mem_filter.mpr ⟨mem_submasks_of_submask hsub, by simp [hyu, hyt]⟩

private theorem singleton_pred (y z : Nat) :
    decide (y ≤ z && z < y + 1) = (z == y) := by
  by_cases h : z == y
  · have heq : z = y := by simpa [beq_iff_eq] using h
    subst z
    simp
  · have hnrange : ¬ (y ≤ z ∧ z < y + 1) := by
      have hne : z ≠ y := by simpa [beq_iff_eq] using h
      omega
    simp [hnrange, h]

private theorem filter_singleton_length (ys : List Nat) (y : Nat) :
    (ys.filter (fun z => decide (y ≤ z && z < y + 1))).length = ys.countP (fun z => z == y) := by
  rw [List.countP_eq_length_filter]
  congr 1
  apply List.filter_congr
  intro z hz
  exact singleton_pred y z

private theorem fan_submask {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  let l := (submasks x).filter (fun y => y < u && y ≤ t)
  have hlnd : l.Nodup := by
    exact List.Sublist.nodup (filter_sublist_nat (fun y => y < u && y ≤ t) (submasks x)) (submasks_nodup x)
  have hsingleton : ∀ y ∈ l, lower ys (y + 1) - lower ys y = ys.countP (fun z => z == y) := by
    intro y hy
    have hle : y ≤ y + 1 := by omega
    rw [lower_interval hv.1 hle, filter_singleton_length]
  unfold fan search
  dsimp
  simp only [List.map_map, Function.comp_apply]
  change (l.map (fun y => lower ys (y + 1) - lower ys y)).sum =
    ys.countP (fun y => (y &&& x) == y && y ≤ t)
  rw [show (l.map (fun y => lower ys (y + 1) - lower ys y)).sum =
      (l.map (fun y => ys.countP (fun z => z == y))).sum from by
    apply congrArg List.sum
    apply List.map_congr_left
    intro y hy
    exact hsingleton y hy]
  rw [sum_countP_eq_countP_mem l hlnd ys, List.countP_eq_length_filter, List.countP_eq_length_filter]
  congr 1
  apply List.filter_congr
  intro z hz
  have hzu : z < u := hv.2 z hz
  have hmem : z ∈ l ↔ (z &&& x) == z ∧ z ≤ t := by
    rw [show l = (submasks x).filter (fun y => y < u && y ≤ t) from rfl, mem_filtered_submasks]
    constructor
    · rintro ⟨hsub, _, hzt⟩
      exact ⟨hsub, hzt⟩
    · rintro ⟨hsub, hzt⟩
      exact ⟨hsub, hzu, hzt⟩
  by_cases hp : (z &&& x) == z ∧ z ≤ t
  · have hzmem : z ∈ l := hmem.mpr hp
    have htest : ((z &&& x) == z && z ≤ t) = true := by simp [hp]
    simp [hzmem, htest]
  · have hzmem : z ∉ l := by
      intro hz'
      exact hp (hmem.mp hz')
    have htest : ((z &&& x) == z && z ≤ t) = false := by
      cases h' : (z &&& x) == z && z ≤ t
      · rfl
      · exfalso
        have hzt : z ≤ t := by
          have h := (Bool.and_eq_true_iff.mp h').2
          simpa using h
        exact hp ⟨(Bool.and_eq_true_iff.mp h').1, hzt⟩
    simp [hzmem, htest]

theorem sound_MaskedJoin :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.MaskedJoin.Goal := by
  intro u t x ys ds hv
  exact masked_from_fan (search.test t x) (fan_submask hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_MaskedJoin
