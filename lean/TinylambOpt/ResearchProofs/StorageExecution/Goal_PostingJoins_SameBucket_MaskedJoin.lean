import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SameBucket.MaskedJoin` — A point-delete bitmap on the right can subtract removed
  multiplicities from the offset count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_MaskedJoin

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket

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

/-! ## The same-bucket span describes exactly the matching right keys -/

private theorem sameBucket_interval_spec (u t x y : Nat) (hyu : y < u) :
    x / (t + 1) * (t + 1) ≤ y ∧
        y < min u (x / (t + 1) * (t + 1) + (t + 1)) ↔
      search.test t x y = true := by
  let w := t + 1
  have hw : 0 < w := by omega
  constructor
  · intro h
    have hlo : x / w * w ≤ y := by simpa [w] using h.1
    have hyupper : y < x / (t + 1) * (t + 1) + (t + 1) := (Nat.lt_min.mp h.2).2
    have hhi : y < (x / w + 1) * w := by
      simpa [w, Nat.add_mul] using hyupper
    have hdiv : y / w = x / w := Nat.div_eq_of_lt_le hlo hhi
    simp [search, w, hdiv]
  · intro ht
    have ht' : x / w = y / w := by simpa [search, w, beq_iff_eq] using ht
    have hya : x / w * w ≤ y := by
      rw [ht']
      exact Nat.div_mul_le_self y w
    have hyb0 : y < y / w * w + w := Nat.lt_div_mul_add (a := y) hw
    rw [← ht'] at hyb0
    have hyb : y < x / w * w + w := hyb0
    constructor
    · simpa [w] using hya
    · have hyb' : y < x / (t + 1) * (t + 1) + (t + 1) := by simpa [w] using hyb
      exact Nat.lt_min.mpr ⟨hyu, hyb'⟩

private theorem fan_search_eq_filter (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      (ys.filter (fun y => decide (x / (t + 1) * (t + 1) ≤ y &&
        y < min u (x / (t + 1) * (t + 1) + (t + 1))))).length := by
  unfold fan search
  dsimp
  let a := x / (t + 1) * (t + 1)
  let b := min u (a + (t + 1))
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
  have hiff := sameBucket_interval_spec u t x y hyu
  let a := x / (t + 1) * (t + 1)
  let b := min u (a + (t + 1))
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

/-! ## Generic list bookkeeping for a point-delete mask -/

private theorem eraseDups_nodup_aux {α : Type} [BEq α] [LawfulBEq α] :
    ∀ n : Nat, ∀ xs : List α, xs.length ≤ n → xs.eraseDups.Nodup
  | 0, [], _ => by simp
  | 0, _ :: _, h => by simp at h
  | n + 1, [], _ => by simp
  | n + 1, a :: xs, h => by
      simp at h
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        have hm := List.mem_filter.mp (List.mem_eraseDups.mp ha)
        simpa using hm.2
      · apply eraseDups_nodup_aux n (xs.filter (fun b => !b == a))
        have hf : (xs.filter (fun b => !b == a)).length ≤ xs.length := List.length_filter_le _ _
        omega

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := eraseDups_nodup_aux xs.length xs (Nat.le_refl _)

private theorem filter_not_perm_filter {α : Type} (p : α → Bool) (xs : List α) :
    (xs.filter (fun x => !p x) ++ xs.filter p).Perm xs := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      by_cases hp : p a = true
      · simp only [List.filter_cons, hp, Bool.not_true, Bool.false_eq_true, ↓reduceIte]
        have hcomm : ((xs.filter (fun x => !p x) ++ [a]) ++ xs.filter p).Perm
            (([a] ++ xs.filter (fun x => !p x)) ++ xs.filter p) :=
          (List.perm_append_comm (l₁ := xs.filter (fun x => !p x)) (l₂ := [a])).append_right (xs.filter p)
        have hswap : (xs.filter (fun x => !p x) ++ a :: xs.filter p).Perm
            (a :: (xs.filter (fun x => !p x) ++ xs.filter p)) := by
          rw [show xs.filter (fun x => !p x) ++ a :: xs.filter p =
              (xs.filter (fun x => !p x) ++ [a]) ++ xs.filter p by simp [List.append_assoc]]
          rw [show a :: (xs.filter (fun x => !p x) ++ xs.filter p) =
              ([a] ++ xs.filter (fun x => !p x)) ++ xs.filter p by simp [List.append_assoc]]
          exact hcomm
        exact hswap.trans (List.Perm.cons a ih)
      · simpa [hp] using (List.Perm.cons a ih)

private theorem countP_zipIdx (ys : List Nat) (p : Nat → Bool) (off : Nat) :
    ys.countP p = (ys.zipIdx off).countP (fun q => p q.1) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons, List.countP_cons, List.countP_cons]
      simp only [Function.comp_def]
      rw [ih (off + 1)]

private theorem filter_mem_length_eq (xs ys : List Nat) (p : Nat → Bool)
    (hxs : xs.Nodup) (hys : ys.Nodup) :
    (xs.filter (fun i => decide (i ∈ ys) && p i)).length =
      (ys.filter (fun i => decide (i ∈ xs) && p i)).length := by
  have hm : (xs.filter (fun i => decide (i ∈ ys))).Perm
      (ys.filter (fun i => decide (i ∈ xs))) := by
    apply (List.perm_ext_iff_of_nodup (hxs.filter _) (hys.filter _)).mpr
    intro i
    simp only [List.mem_filter, decide_eq_true_eq]
    exact ⟨fun h => ⟨h.2, h.1⟩, fun h => ⟨h.2, h.1⟩⟩
  have hp := hm.filter p
  have hl : xs.filter (fun i => decide (i ∈ ys) && p i) =
      (xs.filter (fun i => decide (i ∈ ys))).filter p := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    by_cases hmem : i ∈ ys <;> by_cases hpi : p i <;> simp [hmem, hpi]
  have hr : ys.filter (fun i => decide (i ∈ xs) && p i) =
      (ys.filter (fun i => decide (i ∈ xs))).filter p := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    by_cases hmem : i ∈ xs <;> by_cases hpi : p i <;> simp [hmem, hpi]
  rw [hl, hr]
  exact hp.length_eq

private theorem zipIdx_filter_snd_count (ys ds : List Nat) (p : Nat → Bool) (off : Nat) :
    ((ys.zipIdx off).filter (fun q => ds.contains q.2 && p q.1)).length =
      ((List.range ys.length).filter
        (fun j => ds.contains (off + j) && p (ys.getD j 0))).length := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons, List.filter_cons, List.length_cons]
      rw [List.range_succ_eq_map]
      simp only [List.filter_cons, List.length_cons, Nat.zero_add, Nat.add_zero,
        List.getD_cons_zero, Prod.fst, Prod.snd]
      have htail :
          (List.filter (fun q => ds.contains q.snd && p q.fst) (ys.zipIdx (off + 1))).length =
            (List.filter (fun j => ds.contains (off + j) && p ((y :: ys).getD j 0))
              (List.map Nat.succ (List.range ys.length))).length := by
        rw [List.filter_map, List.length_map]
        rw [ih (off + 1)]
        apply congrArg List.length
        apply List.filter_congr
        intro j hj
        simp only [Function.comp_def, Nat.succ_eq_add_one, List.getD_cons_succ]
        have heq : off + (j + 1) = off + 1 + j := by omega
        rw [heq]
      by_cases h0 : (ds.contains off && p y) = true
      · simp only [h0, ↓reduceIte, List.length_cons]
        omega
      · simp only [h0, Bool.false_eq_true, ↓reduceIte]
        exact htail

private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).length := by
  rw [List.countP_eq_length_filter]
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases h : i < ys.length
    · simp [h, List.getElem?_eq_getElem h, getD_eq_getElem h]
    · simp [h, List.getElem?_eq_none (by omega : ys.length ≤ i)]
  rw [hlhs]
  have hlen := filter_mem_length_eq ds.eraseDups (List.range ys.length)
    (fun i => p (ys.getD i 0)) (eraseDups_nodup ds) List.nodup_range
  have hmap := zipIdx_filter_snd_count ys ds p 0
  simp only [Nat.zero_add] at hmap
  rw [show (fun i => ds.contains i && p (ys.getD i 0)) =
      (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0)) from by
    funext i
    simp [List.contains_eq_mem]] at hmap
  calc
    (ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0))).length =
        ((List.range ys.length).filter
          (fun i => decide (i ∈ ds.eraseDups) && p (ys.getD i 0))).length := by
      simpa [List.mem_range] using hlen
    _ = (ys.zipIdx.filter (fun q => ds.contains q.2 && p q.1)).length := hmap.symm

theorem sound_MaskedJoin : SameBucket.MaskedJoin.Goal := by
  intro u t x ys ds hv
  rw [fan_search_eq_countP u t x hv]
  let pred := search.test t x
  let keep := ys.zipIdx.filter (fun q => !ds.contains q.2)
  let del := ys.zipIdx.filter (fun q => ds.contains q.2)
  change ys.countP pred - ds.eraseDups.countP (fun i => ys[i]?.any pred) =
      ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP pred
  have hperm : (keep ++ del).Perm ys.zipIdx := by
    simpa only [keep, del] using filter_not_perm_filter (fun q => ds.contains q.2) ys.zipIdx
  have hzip : ys.zipIdx.countP (fun q => pred q.1) =
      keep.countP (fun q => pred q.1) + del.countP (fun q => pred q.1) := by
    rw [← hperm.countP_eq (fun q => pred q.1), List.countP_append]
  have hsurv : ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP pred =
      keep.countP (fun q => pred q.1) := by
    rw [List.countP_map]
    rfl
  have hdel_filter : del.filter (fun q => pred q.1) =
      ys.zipIdx.filter (fun q => ds.contains q.2 && pred q.1) := by
    dsimp [del]
    rw [List.filter_filter]
    apply List.filter_congr
    intro q hq
    by_cases hc : ds.contains q.2 <;> by_cases hpq : pred q.1 <;>
      simp [hc, hpq, Bool.and_eq_true]
  have hdel_count : del.countP (fun q => pred q.1) =
      ds.eraseDups.countP (fun i => ys[i]?.any pred) := by
    rw [List.countP_eq_length_filter, hdel_filter]
    exact (deleted_count_eq ys ds pred).symm
  have hcountzip : ys.zipIdx.countP (fun q => pred q.1) = ys.countP pred := by
    exact (countP_zipIdx ys pred 0).symm
  rw [← hcountzip, hzip, hsurv, hdel_count]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_MaskedJoin
