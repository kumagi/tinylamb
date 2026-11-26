import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_PredicateDictionarySelectionForAverage

A precomputed predicate on dictionary entries gives the same sequence of
non-null average inputs: the retained indices are exactly those whose dictionary
code is present and below the cutoff, even with duplicate codes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_PredicateDictionarySelectionForAverage

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem pair_mem_zipIdx_from (dict : List Nat) (i k : Nat) (h : i < dict.length) :
    (dict[i], k + i) ∈ dict.zipIdx k := by
  induction dict generalizing i k with
  | nil => simp at h
  | cons a as ih =>
      cases i with
      | zero => simp [List.zipIdx]
      | succ i =>
          have hi1 : i + 1 < as.length + 1 := by simpa using h
          have hi : i < as.length := by omega
          have heq : k + (i + 1) = (k + 1) + i := by omega
          rw [List.zipIdx_cons]
          exact List.mem_cons_of_mem _ (by simpa [heq] using ih i (k + 1) hi)

private theorem keep_contains_iff (dict : List Nat) (i : Nat) :
    (((dict.zipIdx.filter (fun (v, _) => v < 4)).map Prod.snd).contains i = true) ↔
      (dict[i]?.any (fun v => v < 4) = true) := by
  constructor
  · intro h
    have hp : i ∈ (dict.zipIdx.filter (fun (v, _) => v < 4)).map Prod.snd := by simpa using h
    rcases List.mem_map.mp hp with ⟨⟨v, j⟩, hj, hji⟩
    have hj' := List.mem_filter.mp hj
    have hz := List.mem_zipIdx hj'.1
    have hjlt : j < dict.length := by simpa using hz.2.1
    have hzval : dict[j] = v := by simpa using hz.2.2.symm
    have hget : dict[j]? = some v := by
      rw [List.getElem?_eq_getElem hjlt]
      simp [hzval]
    subst i
    simp [hget, hj'.2]
  · intro h
    cases hi : dict[i]? with
    | none => simp [hi] at h
    | some v =>
        have hv : v < 4 := by simpa [hi] using h
        rcases List.getElem?_eq_some_iff.mp hi with ⟨hlt, heq⟩
        have hpair : (v, i) ∈ dict.zipIdx := by
          have hp := pair_mem_zipIdx_from dict i 0 hlt
          simp at hp
          rw [← heq]
          exact hp
        have hf : (v, i) ∈ dict.zipIdx.filter (fun (w, _) => w < 4) :=
          List.mem_filter.mpr ⟨hpair, by simpa using hv⟩
        have hm : i ∈ (dict.zipIdx.filter (fun (w, _) => w < 4)).map Prod.snd :=
          List.mem_map.mpr ⟨(v, i), hf, rfl⟩
        simpa using hm

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PredicateDictionarySelectionForAverage.Goal := by
  intro rows dict
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PredicateDictionarySelectionForAverage.Goal]
  let keep := (dict.zipIdx.filter (fun (v, _) => v < 4)).map Prod.snd
  let P : Nat → Bool := fun i => decide (i ∈ keep)
  have hkeep : ∀ i, (keep.contains i) = dict[i]?.any (fun v => v < 4) := by
    intro i
    dsimp [keep]
    apply Bool.eq_iff_iff.mpr
    exact keep_contains_iff dict i
  have hdec : ∀ i, P i = keep.contains i := by
    intro i
    dsimp [P]
    simp
  have hP : ∀ i, P i = dict[i]?.any (fun v => v < 4) := by
    intro i
    rw [hdec i, hkeep i]
  change (List.filterMap Prod.snd (List.filter (fun r : Nat × Option Int => P r.1) rows)) =
    (List.filterMap Prod.snd (List.filter (fun r : Nat × Option Int => dict[r.1]?.any (fun v => v < 4)) rows))
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨k, v⟩
      by_cases hp : P k = true
      · have hk : dict[k]?.any (fun x => x < 4) = true := by rwa [← hP k]
        simp [List.filterMap_cons, hp, hk]
        cases v <;> simp [ih]
      · have hk : dict[k]?.any (fun x => x < 4) = false := by
          have := hP k
          simpa [hp] using this.symm
        simpa [hp, hk] using ih

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_PredicateDictionarySelectionForAverage
