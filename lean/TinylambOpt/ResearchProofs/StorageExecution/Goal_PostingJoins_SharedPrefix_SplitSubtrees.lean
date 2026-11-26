import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SharedPrefix.SplitSubtrees` — `∀ u t x ys, valid u ys → let a:=(x/2^(t+1))*2^(t+1);
  fan search u (t+1) x ys = fan search u t a ys + fan search u t (a+2^t) ys`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_SplitSubtrees

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SharedPrefix

/-! ### Binary-search lower bound on a sorted list of postings -/

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
        · rw [if_pos hv]
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
        · rw [if_neg hv]
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

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) :
    ys.Pairwise (· ≤ ·) := hv.1

/-! ### The fan of a shared-prefix probe is a difference of lower bounds -/

private theorem fan_search_eq_lower (u t x : Nat) {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      let a := x / 2 ^ t * 2 ^ t
      lower ys (min u (a + 2 ^ t)) - lower ys a := by
  unfold fan search
  dsimp only
  let a := x / 2 ^ t * 2 ^ t
  let b := min u (a + 2 ^ t)
  change ((TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional a b).map
    (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum = lower ys b - lower ys a
  by_cases h : a < b
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]
    have hba : b ≤ a := by omega
    have hm := lower_mono hs hba
    omega

theorem sound_SplitSubtrees : SharedPrefix.SplitSubtrees.Goal := by
  intro u t x ys hv
  dsimp only
  have hs := valid_pairwise hv
  rw [fan_search_eq_lower u (t + 1) x hs]
  rw [fan_search_eq_lower u t ((x / 2 ^ (t + 1)) * 2 ^ (t + 1)) hs]
  rw [fan_search_eq_lower u t ((x / 2 ^ (t + 1)) * 2 ^ (t + 1) + 2 ^ t) hs]
  let k := x / 2 ^ (t + 1)
  let w := 2 ^ t
  have hw : 0 < w := Nat.pow_pos (by decide)
  have hpow : 2 ^ (t + 1) = w + w := by
    simp [w, Nat.pow_succ, Nat.mul_two]
  let A := k * 2 ^ (t + 1)
  have hAeq : A = (k * 2) * w := by
    dsimp [A]
    rw [hpow]
    rw [← Nat.mul_two w]
    ac_rfl
  have hAdiv : A / w = k * 2 := by
    rw [hAeq, Nat.mul_div_left _ hw]
  have hAmul : A / w * w = A := by
    rw [hAdiv, hAeq]
  have hAwmul : (A + w) / w * w = A + w := by
    rw [Nat.add_div_right _ hw, Nat.add_mul, Nat.one_mul, hAmul]
  have hx2 : x / 2 ^ (t + 1) = k := rfl
  have hA2 : (x / 2 ^ (t + 1)) * 2 ^ (t + 1) = A := rfl
  rw [hx2, hA2]
  dsimp only
  rw [hpow]
  change lower ys (min u (A + (w + w))) - lower ys A =
    (lower ys (min u (A / w * w + w)) - lower ys (A / w * w)) +
      (lower ys (min u ((A + w) / w * w + w)) - lower ys ((A + w) / w * w))
  rw [hAmul, hAwmul]
  rw [show A + (w + w) = A + w + w by omega]
  have hmA : lower ys A ≤ lower ys (A + w) := lower_mono hs (by omega)
  have hmB : lower ys (A + w) ≤ lower ys (A + w + w) := lower_mono hs (by omega)
  by_cases h1 : u ≤ A
  · have h2 : u ≤ A + w := by omega
    have h3 : u ≤ A + w + w := by omega
    have hmuA : lower ys u ≤ lower ys A := lower_mono hs h1
    rw [Nat.min_eq_left h2, Nat.min_eq_left h3]
    omega
  · have hAu : A < u := by omega
    by_cases h2 : u ≤ A + w
    · have h3 : u ≤ A + w + w := by omega
      have hmu : lower ys u ≤ lower ys (A + w) := lower_mono hs h2
      rw [Nat.min_eq_left h2, Nat.min_eq_left h3]
      omega
    · have hAw_le : A + w ≤ u := by omega
      have hmUw : lower ys (A + w) ≤ lower ys u := lower_mono hs hAw_le
      by_cases h3 : u ≤ A + w + w
      · rw [Nat.min_eq_right hAw_le, Nat.min_eq_left h3]
        omega
      · have h3' : A + w + w ≤ u := by omega
        rw [Nat.min_eq_right hAw_le, Nat.min_eq_right h3']
        omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SharedPrefix_SplitSubtrees
