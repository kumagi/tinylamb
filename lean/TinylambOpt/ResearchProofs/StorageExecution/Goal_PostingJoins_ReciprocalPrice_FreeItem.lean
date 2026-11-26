import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ReciprocalPrice.FreeItem` — `∀ u t ys, valid u ys → fan search u t 0 ys = ys.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_FreeItem

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice
open TinylambOpt.ResearchGoals.Compression.Wire

private def allSublists {α : Type} : List α → List (List α)
  | [] => [[]]
  | x :: xs => let rs := allSublists xs; rs ++ rs.map (List.cons x)

/-- Truth check: every sorted right column over a bounded finite domain. -/
private def boundedCheck : Bool :=
  (List.range 10).all fun u =>
    (List.range 10).all fun t =>
      (allSublists (List.range u)).all fun ys => decide (fan search u t 0 ys == ys.length)

#eval boundedCheck

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

private theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

private theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, h]

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

private theorem lower_zero {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) : lower ys 0 = 0 := by
  have hbelow := (lower_cut hs 0).1.1
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro hpos
    have hbad : ys.getD 0 0 < 0 := hbelow 0 hpos
    omega
  · exact Nat.zero_le _

private theorem lower_domain {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {u : Nat}
    (hv : ∀ y ∈ ys, y < u) : lower ys u = ys.length := by
  have hle := lower_le_length hs u
  have habove := (lower_cut hs u).1.2
  apply Nat.le_antisymm hle
  apply Nat.le_of_not_gt
  intro hlt
  have hge : u ≤ ys.getD (lower ys u) 0 := habove (lower ys u) (Nat.le_refl _) hlt
  rw [getD_eq_getElem hlt] at hge
  have hylt : ys[lower ys u] < u := hv _ (List.getElem_mem hlt)
  omega

theorem sound_FreeItem : ReciprocalPrice.FreeItem.Goal := by
  intro u t ys hv
  have hs : ys.Pairwise (· ≤ ·) := valid_pairwise hv
  unfold fan
  change (((optional 0 (if (0 == 0) = true then u else min u (t / 0 + 1))).map
      (fun (a, b) => lower ys b - lower ys a)).sum) = ys.length
  simp
  by_cases hu : 0 < u
  · rw [optional_some hu]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    rw [lower_zero hs, lower_domain hs hv.2]
    simp
  · have hu0 : u = 0 := by omega
    subst u
    rw [optional_none (by omega : ¬ 0 < 0)]
    simp only [List.map_nil, List.sum_nil]
    have hnil : ys = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro y hy
      have hylt := valid_lt hv hy
      omega
    rw [hnil]
    simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_FreeItem
