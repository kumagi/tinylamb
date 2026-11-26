import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SharedPrefix.MaskedJoin` — A point-delete bitmap on the right can subtract removed
  multiplicities from the offset count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_MaskedJoin

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix

/-! Self-contained certificate for `SharedPrefix.MaskedJoin`.
The shared-prefix span is a single half-open interval; the binary lower bound
on the sorted right key column cuts exactly at its boundary, so the fan equals
the number of matching keys. Deleting right positions by index subtracts
exactly the matching deleted positions. -/

/-! ## Small-input truth check (valid sorted right lists over [0,u)) -/

private def pow (xs : List Nat) : List (List Nat) :=
  xs.foldr (fun a acc => acc.flatMap (fun ys => [ys, a :: ys])) [[]]

private def eqcase (u t x : Nat) (ys ds : List Nat) : Bool :=
  fan search u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any (search.test t x)) ==
    ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP (search.test t x)

private def check : Bool :=
  (List.range 7).all fun u =>
  (List.range 4).all fun t =>
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
                  have hpair := List.Pairwise.rel_getElem_of_lt hjlen hmidlen hs hjm
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
                  have hpair := List.Pairwise.rel_getElem_of_lt hmidlen hjlen hs hmj
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
    subst n
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

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  rw [filter_lt_prefix_filter_lt hs hab, List.length_append]
  omega

private theorem lower_sub_eq_zero_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hba : b ≤ a) :
    lower ys b - lower ys a = 0 := by
  exact Nat.sub_eq_zero_of_le (lower_mono hs hba)

private theorem filter_interval_nil_of_ge (ys : List Nat) {a b : Nat} (hba : b ≤ a) :
    ys.filter (fun y => decide (a ≤ y && y < b)) = [] := by
  apply List.filter_eq_nil_iff.mpr
  intro y hy
  simp only [decide_eq_true_eq, Bool.and_eq_true]
  omega

/-! ## The shared-prefix span describes exactly the matching right keys -/

private theorem sharedPrefix_interval_spec (u t x y : Nat) (hyu : y < u) :
    x / 2 ^ t * 2 ^ t ≤ y ∧
        y < min u (x / 2 ^ t * 2 ^ t + 2 ^ t) ↔
      search.test t x y = true := by
  let w := 2 ^ t
  let q := x / w
  have hw : 0 < w := by
    dsimp [w]
    exact Nat.two_pow_pos t
  constructor
  · intro h
    have hlo : q * w ≤ y := by simpa [q, w] using h.1
    have hyupper : y < x / 2 ^ t * 2 ^ t + 2 ^ t := (Nat.lt_min.mp h.2).2
    have hhi : y < (q + 1) * w := by
      simpa [q, w, Nat.add_mul, Nat.one_mul] using hyupper
    have hdiv : y / w = q := Nat.div_eq_of_lt_le hlo hhi
    simp [search, w, q, hdiv]
  · intro ht
    have ht' : q = y / w := by simpa [search, w, q, beq_iff_eq] using ht
    have hya : q * w ≤ y := by
      rw [ht']
      exact Nat.div_mul_le_self y w
    have hyb0 : y < y / w * w + w := Nat.lt_div_mul_add (a := y) hw
    rw [← ht'] at hyb0
    constructor
    · simpa [q, w] using hya
    · exact Nat.lt_min.mpr ⟨hyu, by simpa [q, w] using hyb0⟩

private theorem fan_search_eq_filter (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      (ys.filter (fun y => decide (x / 2 ^ t * 2 ^ t ≤ y &&
        y < min u (x / 2 ^ t * 2 ^ t + 2 ^ t)))).length := by
  unfold fan search
  dsimp
  let a := x / 2 ^ t * 2 ^ t
  let b := min u (a + 2 ^ t)
  change ((TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b).map
      (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
    (ys.filter (fun y => decide (a ≤ y && y < b))).length
  by_cases h : a < b
  · rw [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, ite_eq_left h]
    have hz := lower_sub_eq_filter (ys := ys) hs (Nat.le_of_lt h)
    simpa [Bool.and_eq_true] using hz
  · have hba : b ≤ a := by omega
    rw [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, ite_eq_right h]
    rw [filter_interval_nil_of_ge ys hba]
    simp

private theorem fan_search_eq_countP (u t x : Nat) {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (search.test t x) := by
  rw [fan_search_eq_filter u t x hv.1, List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  have hyu : y < u := hv.2 y hy
  have hiff := sharedPrefix_interval_spec u t x y hyu
  let a := x / 2 ^ t * 2 ^ t
  let b := min u (a + 2 ^ t)
  have hiff' : a ≤ y ∧ y < b ↔ search.test t x y = true := by simpa [a, b] using hiff
  change decide (a ≤ y && y < b) = search.test t x y
  by_cases hp : a ≤ y ∧ y < b
  · have ht : search.test t x y = true := hiff'.mp hp
    simp [hp, ht]
  · have ht : search.test t x y = false := by
      cases h : search.test t x y <;> simp_all
    have hdec : decide (a ≤ y && y < b) = false := by
      cases hd' : decide (a ≤ y && y < b)
      · rfl
      · exfalso
        have hp' : a ≤ y ∧ y < b := by simpa [Bool.and_eq_true] using hd'
        have httrue : search.test t x y = true := hiff'.mp hp'
        simp [ht] at httrue
    rw [hdec, ht]

/-! ## Deleting right positions by index -/

private theorem nodup_eraseDups_aux {α : Type} [BEq α] [LawfulBEq α] :
    ∀ n : Nat, ∀ xs : List α, xs.length = n → xs.eraseDups.Nodup := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen
      cases xs with
      | nil => exact List.nodup_nil
      | cons x xs =>
          rw [List.eraseDups_cons, List.nodup_cons]
          have hlt : (xs.filter fun y => !y == x).length < n := by
            have hle := List.length_filter_le (fun y => !y == x) xs
            simp only [List.length_cons] at hlen
            omega
          constructor
          · intro hmem
            rw [List.mem_eraseDups, List.mem_filter] at hmem
            obtain ⟨_, hne⟩ := hmem
            simp at hne
          · exact ih _ hlt _ rfl

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := nodup_eraseDups_aux xs.length xs rfl

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  rw [List.nodup_iff_pairwise_ne] at h ⊢
  exact List.Pairwise.filter p h

private theorem countP_eraseDups_eq (ds l : List Nat) (hl : l.Nodup)
    (P : Nat → Bool) (hP : ∀ i : Nat, P i ↔ i ∈ l) :
    ds.eraseDups.countP P = l.countP (fun i => ds.contains i) := by
  have hnd1 : (ds.eraseDups.filter P).Nodup := nodup_filter P (nodup_eraseDups ds)
  have hnd2 : (l.filter (fun i => ds.contains i)).Nodup :=
    nodup_filter (fun i => ds.contains i) hl
  have hperm : (ds.eraseDups.filter P).Perm (l.filter (fun i => ds.contains i)) := by
    apply (List.perm_ext_iff_of_nodup hnd1 hnd2).mpr
    intro i
    simp only [List.mem_filter, List.mem_eraseDups, List.contains_iff_mem]
    rw [hP i]
    constructor <;> intro h <;> exact ⟨h.2, h.1⟩
  simpa only [List.countP_eq_length_filter] using hperm.length_eq

private theorem zipIdx_snd_nodup (l : List Nat) (n : Nat) :
    ((l.zipIdx n).map Prod.snd).Nodup := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨e, he, heq⟩
        obtain ⟨y, i⟩ := e
        simp only at heq
        subst i
        obtain ⟨hlo, hi, hval⟩ := List.mem_zipIdx (k := n + 1) he
        omega
      · simpa using ih (n + 1)

private theorem mem_matching_indices (ys : List Nat) (p : Nat → Bool) (i : Nat) :
    i ∈ ((ys.zipIdx.filter (fun e => p e.1)).map Prod.snd) ↔ ys[i]?.any p = true := by
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨e, he, heq⟩
    obtain ⟨y, j⟩ := e
    simp only at heq
    subst j
    have hf := List.mem_filter.mp he
    have hp : p y = true := hf.2
    have hzip := List.mem_zipIdx (k := 0) hf.1
    have ha : i < ys.length := by simpa using hzip.2.1
    have hy : y = ys[i] := by simpa using hzip.2.2
    rw [List.getElem?_eq_getElem ha]
    rw [← hy]
    exact hp
  · intro h
    by_cases hi : i < ys.length
    · have hy : p (ys[i]) = true := by
        simpa [List.getElem?_eq_getElem hi] using h
      apply List.mem_map.mpr
      refine ⟨(ys[i], i), ?_, rfl⟩
      apply List.mem_filter.mpr
      constructor
      · have hzget : (ys.zipIdx)[i]? = some (ys[i], i) := by
          rw [List.getElem?_zipIdx]
          simp [List.getElem?_eq_getElem hi]
        obtain ⟨hidx, hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
        rw [← hidxval]
        exact List.getElem_mem hidx
      · exact hy
    · have hnone : ys[i]? = none := List.getElem?_eq_none (by omega : ys.length ≤ i)
      simp [hnone] at h

private theorem filter_map_snd (zs : List (Nat × Nat)) (p : Nat × Nat → Bool) (q : Nat → Bool) :
    ((zs.filter p).map Prod.snd).filter q =
      ((zs.filter (fun e => p e && q e.2)).map Prod.snd) := by
  induction zs with
  | nil => simp
  | cons e es ih =>
      by_cases hpe : p e = true <;> by_cases hqe : q e.2 = true <;> simp [hpe, hqe, ih]

private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun e => ds.contains e.2 && p e.1)).length := by
  let l := (ys.zipIdx.filter (fun e => p e.1)).map Prod.snd
  have hl : l.Nodup := by
    dsimp [l]
    apply List.Sublist.nodup
    · exact List.Sublist.map Prod.snd List.filter_sublist
    · exact zipIdx_snd_nodup ys 0
  have hP : ∀ i : Nat, ys[i]?.any p ↔ i ∈ l := by
    intro i
    simpa [l] using (mem_matching_indices ys p i).symm
  have h := countP_eraseDups_eq ds l hl (fun i => ys[i]?.any p) hP
  rw [h, List.countP_eq_length_filter]
  change (l.filter (fun i => ds.contains i)).length = _
  have hfl := filter_map_snd ys.zipIdx (fun e => p e.1) (fun i => ds.contains i)
  dsimp [l] at hfl ⊢
  rw [hfl, List.length_map]
  apply congrArg List.length
  apply List.filter_congr
  intro e he
  by_cases hp : p e.1 <;> by_cases hd : ds.contains e.2 <;> simp [hp, hd, Bool.and_comm]

private theorem countP_zipIdx (ys : List Nat) (p : Nat → Bool) (o : Nat) :
    ys.countP p = (ys.zipIdx o).countP (fun e => p e.1) := by
  induction ys generalizing o with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons, List.countP_cons, List.countP_cons]
      simp only [Function.comp_def]
      rw [ih (o + 1)]

private theorem countP_map_fst (zs : List (Nat × Nat)) (p : Nat → Bool) :
    (zs.map Prod.fst).countP p = zs.countP (fun e => p e.1) := by
  induction zs with
  | nil => simp
  | cons e es ih =>
      by_cases h : p e.1 <;> simp [h, ih]

private theorem countP_filter_partition {α : Type} (l : List α) (q p : α → Bool) :
    l.countP p = (l.filter q).countP p + (l.filter (fun e => !q e)).countP p := by
  induction l with
  | nil => simp
  | cons e es ih =>
      by_cases hq : q e = true <;> by_cases hp : p e = true <;> simp [hq, hp, ih] <;> omega

private theorem countP_filter_eq_length_filter_and {α : Type} (l : List α) (q p : α → Bool) :
    (l.filter q).countP p = (l.filter (fun e => q e && p e)).length := by
  rw [List.countP_eq_length_filter, List.filter_filter]
  simp [Bool.and_comm]

theorem sound_MaskedJoin :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix.MaskedJoin.Goal := by
  intro u t x ys ds hv
  rw [fan_search_eq_countP u t x hv]
  let p := search.test t x
  change ys.countP p - ds.eraseDups.countP (fun i => ys[i]?.any p) =
    ((ys.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst).countP p
  rw [deleted_count_eq ys ds p, countP_zipIdx ys p 0,
    countP_filter_partition ys.zipIdx (fun e => ds.contains e.2) (fun e => p e.1),
    countP_map_fst (ys.zipIdx.filter (fun e => !ds.contains e.2)) p,
    countP_filter_eq_length_filter_and ys.zipIdx (fun e => ds.contains e.2) (fun e => p e.1)]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_MaskedJoin
