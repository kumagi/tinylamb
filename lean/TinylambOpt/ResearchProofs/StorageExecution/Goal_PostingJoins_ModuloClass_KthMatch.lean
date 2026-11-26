import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ModuloClass.KthMatch` — Top offset within the matching postings preserves tie
  multiplicity and right ordering.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_KthMatch

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass
open TinylambOpt.ResearchGoals.Compression.Wire

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
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take]; simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [List.take_append_drop]

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
    · rw [ite_eq_left hk]
      have hk' : k < (slice ys a b).length := by omega
      rw [List.getElem?_append, ite_eq_left hk']
      have hsub : (slice ys a b)[k]? = ys[lower ys a + k]? := by
        unfold slice
        simp [List.getElem?_take, List.getElem?_drop, hk]
      rw [hsub]
    · rw [ite_eq_right hk]
      have hk' : ¬ k < (slice ys a b).length := by omega
      rw [List.getElem?_append, ite_eq_right hk']
      rw [ih (k - (lower ys b - lower ys a))]
      congr 1
      rw [hslen]

-- A generic split lemma: when every element satisfying `p` precedes every
-- element satisfying `q` in a sorted list, filtering by `p || q` appends.
private theorem filter_or_sep {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (p q : Nat → Bool) (hsep : ∀ y ∈ ys, ∀ z ∈ ys, p z = true → q y = true → z < y) :
    ys.filter (fun y => p y || q y) = ys.filter p ++ ys.filter q := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    have hs' := hs.tail
    have hsep' : ∀ y' ∈ ys, ∀ z ∈ ys, p z = true → q y' = true → z < y' := by
      intro y' hy' z hz hp hq
      exact hsep y' (by simp [hy']) z (by simp [hz]) hp hq
    by_cases hp : p y = true
    · have hq : q y = false := by
        cases hq : q y
        · rfl
        · have hcontra := hsep y (by simp) y (by simp) hp hq
          omega
      simp [hp, hq, ih hs' hsep']
    · by_cases hq : q y = true
      · have hpnil : ys.filter p = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro z hz
          intro hpz
          have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
          have hlt := hsep y (by simp) z (by simp [hz]) hpz hq
          omega
        simp [hp, hq, hpnil, ih hs' hsep']
      · simp [hp, hq, ih hs' hsep']

private abbrev seq (r w n : Nat) : Ranges :=
  (List.range n).map (fun i => (r + i * w, r + i * w + 1))

private theorem seq_flatMap_eq_filter {w r : Nat} (hw : 0 < w) (hr : r < w)
    (ys : List Nat) (hs : ys.Pairwise (· ≤ ·)) : ∀ N : Nat,
    (seq r w N).flatMap (fun (a, b) => slice ys a b) =
      ys.filter (fun y => decide (r == y % w) && decide (y < r + N * w)) := by
  intro N
  induction N with
  | zero =>
      simp [seq]
      intro y hy hmod
      rw [hmod]
      exact Nat.mod_le y w
  | succ n ih =>
      have hseq : seq r w (n + 1) = seq r w n ++ [(r + n * w, r + n * w + 1)] := by
        simp [seq, List.range_succ]
      rw [hseq, List.flatMap_append]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      rw [ih]
      rw [slice_eq_filter hs (r + n * w) (r + n * w + 1)]
      let p : Nat → Bool := fun y => decide (r == y % w) && decide (y < r + n * w)
      let q : Nat → Bool := fun y => decide (r + n * w ≤ y) && decide (y < r + n * w + 1)
      have hsep : ∀ y ∈ ys, ∀ z ∈ ys, p z = true → q y = true → z < y := by
        intro y hy z hz hpz hqy
        have hpz' := hpz
        simp [p, Bool.and_eq_true, decide_eq_true_eq] at hpz'
        have hqy' := hqy
        simp [q, Bool.and_eq_true, decide_eq_true_eq] at hqy'
        omega
      have hbig : ys.filter (fun y => decide (r == y % w) && decide (y < r + (n + 1) * w)) =
          ys.filter (fun y => p y || q y) := by
        apply List.filter_congr
        intro y hy
        apply Bool.eq_iff_iff.mpr
        constructor
        · intro hb
          have hb' := hb
          simp [Bool.and_eq_true, decide_eq_true_eq] at hb'
          have hylt : y < r + (n + 1) * w := hb'.2
          have hmod : y % w = r := by
            have : r = y % w := by simpa [Nat.beq_eq] using hb'.1
            omega
          have hdecomp : y = r + w * (y / w) := by
            have h := Nat.mod_add_div y w
            rw [hmod] at h
            omega
          by_cases hsmall : y < r + n * w
          · have hpy : p y = true := by
              simp [p, Bool.and_eq_true, hmod, hsmall]
            simp [hpy]
          · have hlow : r + n * w ≤ y := by omega
            have hkw' : n * w ≤ w * (y / w) := by omega
            have hkw : n * w ≤ (y / w) * w := by simpa [Nat.mul_comm] using hkw'
            have hkge : n ≤ y / w := Nat.le_of_mul_le_mul_right hkw hw
            have hkw2 : w * (y / w) < (n + 1) * w := by omega
            have hkw3 : (y / w) * w < (n + 1) * w := by simpa [Nat.mul_comm] using hkw2
            have hkle : y / w ≤ n := by
              have : y / w < n + 1 := Nat.lt_of_mul_lt_mul_right hkw3
              omega
            have hk : y / w = n := by omega
            have hy_eq : y = r + n * w := by
              calc
                y = r + w * (y / w) := hdecomp
                _ = r + n * w := by rw [hk, Nat.mul_comm w n]
            have hqy : q y = true := by
              simp [q, Bool.and_eq_true, decide_eq_true_eq, hy_eq]
            simp [hqy]
        · intro hb
          have hb' : p y = true ∨ q y = true := by
            simpa [Bool.or_eq_true] using hb
          rcases hb' with hp | hq
          · have hp' := hp
            simp [p, Bool.and_eq_true, decide_eq_true_eq] at hp'
            have hylt_n : y < r + n * w := hp'.2
            have hstep : r + n * w ≤ r + (n + 1) * w := by
              rw [Nat.succ_mul]
              omega
            have hylt : y < r + (n + 1) * w := by omega
            have hmodeq : y % w = r := by omega
            simp [Bool.and_eq_true, hmodeq, hylt]
          · have hq' : r + n * w ≤ y ∧ y < r + n * w + 1 := by
              simpa [q, Bool.and_eq_true, decide_eq_true_eq] using hq
            have hy_eq : y = r + n * w := by omega
            have hmod : y % w = r := by
              rw [hy_eq, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hr]
            have hmodb : (r == y % w) = true := by simp [hmod]
            have hstep : r + n * w + 1 ≤ r + (n + 1) * w := by
              rw [Nat.succ_mul]
              omega
            have hylt : y < r + (n + 1) * w := by omega
            simp [Bool.and_eq_true, hmodb, hylt]
      change ys.filter p ++ ys.filter q =
        ys.filter (fun y => decide (r == y % w) && decide (y < r + (n + 1) * w))
      rw [hbig]
      exact (filter_or_sep hs p q hsep).symm

private theorem seek_mod_eq_filter {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    (search.spans u t x).flatMap (fun (a, b) => slice ys a b) = ys.filter (search.test t x) := by
  have hs := valid_pairwise hv
  let w := t + 1
  let r := x % w
  let N := (u + w - 1 - r) / w
  have hw : 0 < w := by omega
  have hr : r < w := by
    dsimp [r]
    exact Nat.mod_lt x hw
  have hprefix := seq_flatMap_eq_filter hw hr ys hs N
  have hspans : search.spans u t x = seq r w N := by
    simp [search, seq, w, r, N]
  rw [hspans, hprefix]
  apply List.filter_congr
  intro y hy
  have hyu := valid_lt hv hy
  by_cases hmod : y % w = r
  · have hrley : r ≤ y := by rw [← hmod]; exact Nat.mod_le y w
    have hrleu : r ≤ u - 1 := by omega
    have hA : u - 1 - r + w = u + w - 1 - r := by omega
    have hN_eq : N = (u - 1 - r) / w + 1 := by
      dsimp [N]
      rw [← hA]
      exact Nat.add_div_right (u - 1 - r) hw
    have hdecomp : y = r + w * (y / w) := by
      have h := Nat.mod_add_div y w
      rw [hmod] at h
      omega
    have hkw : (y / w) * w ≤ u - 1 - r := by
      rw [Nat.mul_comm]
      omega
    have hkle : y / w ≤ (u - 1 - r) / w := (Nat.le_div_iff_mul_le hw).mpr hkw
    have hklt : y / w < N := by rw [hN_eq]; omega
    have hmul : (y / w) * w < N * w := (Nat.mul_lt_mul_right hw).mpr hklt
    have hyltN : y < r + N * w := by
      calc
        y = r + w * (y / w) := hdecomp
        _ = r + (y / w) * w := by rw [Nat.mul_comm w (y / w)]
        _ < r + N * w := by omega
    simp [search, w, r, hmod, hyltN]
  · have hneq : ¬ x % w = y % w := by
      intro h
      apply hmod
      omega
    have hneq' : ¬ r = y % w := by omega
    simp [search, w, r, Nat.beq_eq, hneq, hneq']

theorem sound_KthMatch : ModuloClass.KthMatch.Goal := by
  intro u t x ys k hv
  have hs := valid_pairwise hv
  rw [nth_eq_flatMap hs (search.spans u t x) k]
  rw [seek_mod_eq_filter hv]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_KthMatch
