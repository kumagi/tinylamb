import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

namespace TinylambOpt.ResearchProofs.StorageExecution.PostingJoins

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false


/-! ## Generic infrastructure -/

@[simp] theorem optional_none {a b : Nat} (h : ¬ a < b) : optional a b = [] := by
  simp [optional, h]

@[simp] theorem optional_some {a b : Nat} (h : a < b) : optional a b = [(a, b)] := by
  simp [optional, h]

theorem member_optional {a b y : Nat} : member (optional a b) y ↔ a ≤ y ∧ y < b := by
  by_cases h : a < b
  · simp [optional, h, member]
  · simp [optional, h, member]; omega

theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) : ys.Pairwise (· ≤ ·) := hv.1
theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat} (hy : y ∈ ys) : y < u := hv.2 y hy


/-! ## lower / searchBound spec -/

private theorem searchBound_lo_le (xs : Array Nat) (v fuel lo hi : Nat) :
    lo ≤ searchBound xs v fuel lo hi := by
  induction fuel generalizing lo hi with
  | zero => simp [searchBound]
  | succ fuel ih =>
    simp only [searchBound]
    split
    · omega
    · split
      · have := ih (mid := (lo+hi)/2) (lo := (lo+hi)/2 + 1) (hi := hi)
        omega
      · exact ih (lo := lo) (hi := (lo+hi)/2)

private theorem searchBound_le_hi (xs : Array Nat) (v fuel lo hi : Nat)
    (h : lo ≤ hi) (hf : hi - lo ≤ 2 ^ fuel) :
    searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
    simp only [searchBound]
    have : lo = hi := by omega
    subst this; simp
  | succ fuel ih =>
    simp only [searchBound]
    split
    · omega
    · split
      · rename_i hlt hmid
        apply ih
        · omega
        · have hmid' : (lo + hi) / 2 < hi := by omega
          -- hi - (mid+1) <= 2^fuel
          have : hi - ((lo + hi) / 2 + 1) ≤ 2 ^ fuel := by
            have h2 : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by ring
            omega
          exact this
      · rename_i hlt hmid
        apply ih
        · omega
        · have : (lo + hi) / 2 - lo ≤ 2 ^ fuel := by
          have h2 : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by ring
          omega
          exact this


private theorem getD_toArray (xs : List Nat) (i : Nat) : xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  simp [List.getD, Array.getElem?_toArray, List.getElem?_eq_getElem?]

private theorem pairwise_le_getD {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {i j : Nat}
    (hj : j < ys.length) (hij : i ≤ j) : ys.getD i 0 ≤ ys.getD j 0 := by
  have hi : i < ys.length := by omega
  simp only [List.getD, List.getElem?_eq_getElem, hi, hj, Option.getD_some]
  exact List.Pairwise.rel_getElem hs hij

private theorem pairwise_le_getD_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {i j : Nat}
    (hj : j < ys.length) (hij : i < j) : ys.getD i 0 ≤ ys.getD j 0 :=
  pairwise_le_getD hs hj (Nat.le_of_lt hij)

private theorem searchBound_inv (xs : Array Nat) (v fuel lo hi : Nat)
    (hlo : ∀ i, i < lo → xs[i]?.getD 0 < v)
    (hhi : ∀ i, hi ≤ i → i < xs.size → v ≤ xs[i]?.getD 0)
    (hlo0 : 0 ≤ lo) (hhi0 : hi ≤ xs.size)
    (hf : hi - lo ≤ 2 ^ fuel) :
    let j := searchBound xs v fuel lo hi
    (∀ i, i < j → xs[i]?.getD 0 < v) ∧ (∀ i, j ≤ i → i < xs.size → v ≤ xs[i]?.getD 0) ∧ lo ≤ j ∧ j ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
    have heq : lo = hi := by omega
    subst heq
    simp [searchBound, hlo, hhi]
  | succ fuel ih =>
    simp only [searchBound]
    split
    · rename_i hge
      have heq : lo = hi := by omega
      subst heq
      exact ⟨hlo, hhi, Nat.le_refl _, Nat.le_refl _⟩
    · rename_i hlt
      split
      · rename_i hmid
        have hmid_le : (lo + hi) / 2 < hi := by omega
        have hmid_lo : lo ≤ (lo + hi) / 2 := by omega
        have hf' : hi - ((lo + hi) / 2 + 1) ≤ 2 ^ fuel := by
          have h2 : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by omega
          omega
        have hlo' : ∀ i, i < (lo + hi) / 2 + 1 → xs[i]?.getD 0 < v := by
          intro i hi2
          by_cases hi3 : i < (lo + hi) / 2
          · -- need: all below mid also < v? only if sorted; but we don't have sorted here
            -- use hlo for < lo, and we can't know about lo..mid-1 without sortedness
            -- This invariant needs sortedness. Let's handle differently.
            sorry
        sorry
      · sorry
