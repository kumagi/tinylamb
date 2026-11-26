import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ScaledFloor.MaskedJoin` — A point-delete bitmap on the right can subtract removed
  multiplicities from the offset count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_MaskedJoin

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Self-contained certificate for `ScaledFloor.MaskedJoin`.

The binary lower bound cuts the sorted right-key column exactly at the
quotient interval `[t*(x+1), (t+1)*(x+1))` clipped to `[0,u)`, so the
two-offset fan equals the number of matching postings.  Deleting right
positions by index subtracts exactly the matching deleted positions:
`eraseDups` turns the deletion list into a set, while `zipIdx` assigns every
right position a unique index. -/

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
    have hi' : i < lower ys v := by omega
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v hi'
    rwa [getD_eq_getElem hiys] at this
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [List.getElem_drop]
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

private theorem lower_interval {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  have hsplit := filter_lt_prefix_filter_lt hs (ys := ys) hab
  rw [hsplit, List.length_append]
  omega

private theorem fan_eq_lower_sub {u t x : Nat} {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      lower ys (min u ((t + 1) * (x + 1))) - lower ys (t * (x + 1)) := by
  by_cases hab : t * (x + 1) < min u ((t + 1) * (x + 1))
  · simp [fan, search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
  · have hmono := lower_mono hs (Nat.le_of_not_gt hab)
    simp [fan, search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    omega

private theorem test_iff_interval {u t x y : Nat} (hyu : y < u) :
    (search.test t x y = true) ↔ t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1)) := by
  let w := x + 1
  have hw : 0 < w := by omega
  constructor
  · intro ht
    have ht' : y / w = t := by
      apply beq_iff_eq.mp
      simpa [search, w] using ht
    constructor
    · rw [← ht']
      exact Nat.div_mul_le_self y w
    · have htmp : y < y / w * w + w := Nat.lt_div_mul_add hw
      rw [ht'] at htmp
      rw [Nat.lt_min]
      exact ⟨hyu, by simpa [w, Nat.add_mul] using htmp⟩
  · rintro ⟨hlo, hhi⟩
    have hiw : y < (t + 1) * w := Nat.lt_of_lt_of_le hhi (Nat.min_le_right _ _)
    have hdiv : y / w = t :=
      Nat.div_eq_of_lt_le (by simpa [w] using hlo) (by simpa [w] using hiw)
    apply beq_iff_eq.mpr
    simpa [search, w] using hdiv

private theorem fan_scaled {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  rw [fan_eq_lower_sub hs]
  rw [List.countP_eq_length_filter]
  let w := x + 1
  let a := t * w
  let b := min u ((t + 1) * w)
  change lower ys b - lower ys a = (ys.filter (search.test t x)).length
  by_cases hab : a ≤ b
  · rw [lower_interval hs hab]
    apply congrArg List.length
    apply List.filter_congr
    intro y hy
    have hyu : y < u := hv.2 y hy
    have hiff := test_iff_interval (u := u) (t := t) (x := x) (y := y) hyu
    cases ht : search.test t x y
    · have hp : ¬ (a ≤ y ∧ y < b) := by
        intro hp
        have hp' : t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1)) := by
          simpa [a, b, w] using hp
        have htrue : search.test t x y = true := hiff.mpr hp'
        simp [ht] at htrue
      simp [hp, ht]
    · have hp' : t * (x + 1) ≤ y ∧ y < min u ((t + 1) * (x + 1)) := hiff.mp (by simp [ht])
      have hp : a ≤ y ∧ y < b := by simpa [a, b, w] using hp'
      simp [hp.1, hp.2, ht]
  · have hba : b ≤ a := by omega
    have hmono : lower ys b ≤ lower ys a := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz]
    have hnil : ys.filter (search.test t x) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy htrue
      have hyu : y < u := hv.2 y hy
      have hp := (test_iff_interval (u := u) (t := t) (x := x) (y := y) hyu).mp htrue
      have hp' : a ≤ y ∧ y < b := by simpa [a, b, w] using hp
      omega
    rw [hnil]
    simp

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

private theorem masked_from_fan {s : Search} {u t x : Nat} {ys ds : List Nat} (p : Nat → Bool)
    (hfan : fan s u t x ys = ys.countP p) :
    fan s u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any p) =
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

theorem sound_MaskedJoin : ScaledFloor.MaskedJoin.Goal := by
  intro u t x ys ds hv
  exact masked_from_fan (search.test t x) (fan_scaled hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_MaskedJoin
