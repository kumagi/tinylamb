import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_PredicateDictionarySelectionForAverage. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_PredicateDictionarySelectionForAverage

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# PredicateDictionarySelectionForAverage

A predicate evaluated on dictionary entries selects exactly those codes whose
dictionary slot exists and satisfies the predicate. Replacing a code lookup by
a precomputed set of selected codes therefore yields the same ordered sequence
of non-null `AVG` inputs, even when several codes carry the same value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem krs_mem_keep_iff (dict : List Nat) (k : Nat) :
    k ∈ ((dict.zipIdx.filter (fun p : Nat × Nat => p.1 < 4)).map Prod.snd) ↔
      (dict[k]?.any (fun v => v < 4)) = true := by
  constructor
  · intro hk
    rcases List.mem_map.mp hk with ⟨p, hp, hp2⟩
    simp only [List.mem_filter] at hp
    have hz := List.mem_zipIdx hp.1
    have hplt : p.2 < dict.length := by simpa using hz.2.1
    have hval : p.1 = dict[p.2] := by simpa using hz.2.2
    have hget : dict[p.2]? = some p.1 := by simp [List.getElem?_eq_getElem hplt, hval]
    have hv : p.1 < 4 := by simpa using hp.2
    subst k
    simpa [hget, hv]
  · intro hk
    rcases Option.any_eq_true (fun v : Nat => v < 4) dict[k]? |>.mp hk with ⟨v, hget, hv⟩
    rcases List.getElem?_eq_some_iff.mp hget with ⟨hklt, hkval⟩
    have hzget : (dict.zipIdx)[k]? = some (dict[k], k) := by
      simpa using (List.getElem?_zipIdx (l := dict) (i := 0) (j := k))
    have hzmem : (dict[k], k) ∈ dict.zipIdx := by
      rcases List.getElem?_eq_some_iff.mp hzget with ⟨hidx, heq⟩
      simpa [heq] using List.getElem_mem hidx
    have hvmem : (v, k) ∈ dict.zipIdx := by simpa [hkval] using hzmem
    have hfmem : (v, k) ∈ dict.zipIdx.filter (fun p : Nat × Nat => p.1 < 4) := by
      simpa [hvmem, hv]
    exact List.mem_map.mpr ⟨(v, k), hfmem, rfl⟩

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PredicateDictionarySelectionForAverage.Goal := by
  intro rows dict
  dsimp
  apply congrArg (fun xs : List (Nat × Option Int) => xs.filterMap Prod.snd)
  apply List.filter_congr
  intro q hq
  apply Bool.eq_iff_iff.mpr
  simpa using (krs_mem_keep_iff dict q.1)
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_PredicateDictionarySelectionForAverage
