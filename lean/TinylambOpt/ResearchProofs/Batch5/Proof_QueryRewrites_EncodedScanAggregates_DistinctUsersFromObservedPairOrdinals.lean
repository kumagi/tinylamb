import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_DistinctUsersFromObservedPairOrdinals

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Observed-pair ordinals: a dictionary of composite keys gives injective dense
ordinals, so grouping by ordinal preserves each observed key's distinct users.
This allows encoded ordinal scans to stand in for the original keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem idxOf_inj_of_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) {a b : α} (ha : a ∈ l) (hb : b ∈ l)
    (h : l.idxOf a = l.idxOf b) : a = b := by
  have hla : l.idxOf a < l.length := List.idxOf_lt_length_of_mem ha
  have hlb : l.idxOf b < l.length := List.idxOf_lt_length_of_mem hb
  have ga := List.getElem_idxOf (x := a) hla
  have gb := List.getElem_idxOf (x := b) hlb
  have ga' : l[l.idxOf b] = a := by simpa [h] using ga
  exact ga'.symm.trans gb

/-- A dictionary of observed pairs gives injective dense ordinals on those
pairs, so grouping by ordinal preserves each observed pair's distinct users. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.DistinctUsersFromObservedPairOrdinals.Goal := by
  intro rs k
  let dict : List (Nat × Nat) := (rs.map Prod.fst).eraseDups
  change k ∈ dict →
    (TinylambOpt.ResearchGoals.QueryRewrites.forKey
        (rs.map (fun (g, u) => (dict.idxOf g, u))) (dict.idxOf k)).eraseDups =
      (TinylambOpt.ResearchGoals.QueryRewrites.forKey rs k).eraseDups
  intro hk
  have hmem : ∀ r ∈ rs, r.1 ∈ dict := by
    intro r hr
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨r, hr, rfl⟩)
  have hiff : ∀ r ∈ rs, (dict.idxOf r.1 == dict.idxOf k) = (r.1 == k) := by
    intro r hr
    by_cases hgk : (r.1 == k) = true
    · have h' : r.1 = k := by simpa only [beq_iff_eq] using hgk
      subst h'
      simp
    · have hne : r.1 ≠ k := by
        intro heq
        apply hgk
        simp [heq]
      have hidxne : dict.idxOf r.1 ≠ dict.idxOf k := by
        intro hi
        exact hne (idxOf_inj_of_mem dict (hmem r hr) hk hi)
      simp [hgk, hidxne]
  unfold TinylambOpt.ResearchGoals.QueryRewrites.forKey
  rw [List.filter_map, List.map_map]
  apply congrArg List.eraseDups
  apply congrArg (fun l : List ((Nat × Nat) × Nat) => l.map Prod.snd)
  apply List.filter_congr
  intro r hr
  simpa only [Function.comp_apply] using hiff r hr
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_DistinctUsersFromObservedPairOrdinals
