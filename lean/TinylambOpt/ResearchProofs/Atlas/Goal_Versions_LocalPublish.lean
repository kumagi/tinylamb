import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `Versions.LocalPublish` — `∀ es e k t v, (∀ old ∈ es, earlier old e = true) → readAt
  (es++[e]) k t v = if eligible k t v e then e.payload else readAt es k t v`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Versions_LocalPublish

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions

/-! Exhaustive small-input check: every log of at most two events from a pool,
appended with every strictly later published event, at every small key/version.
The result is `true` iff no counterexample exists among these inputs. -/

def pool : List Event :=
  [ ⟨0, 0, 0, 0, 3, some 1⟩
  , ⟨0, 0, 1, 0, 3, none⟩
  , ⟨0, 1, 0, -1, 2, some 5⟩
  , ⟨0, 1, 1, 1, 4, some 7⟩
  , ⟨1, 0, 0, 0, 4, some 2⟩
  , ⟨1, 1, 0, -1, 3, none⟩
  , ⟨1, 2, 0, 0, 2, some 9⟩
  , ⟨0, 3, 0, 0, 3, some 11⟩
  , ⟨2, 2, 5, -1, 5, none⟩ ]

def listsUpTo : Nat → List Event → List (List Event)
  | 0, _ => [[]]
  | n + 1, p => (listsUpTo n p).flatMap (fun s => s :: p.map (fun e => e :: s))

#eval (listsUpTo 2 pool).all (fun es =>
  pool.all (fun e =>
    if es.all (fun old => earlier old e == true) then
      ([0, 1, 2].all (fun k => [0, 1, 2, 3].all (fun t =>
        ([-2, -1, 0, 1, 2, 3, 4, 5].all (fun v =>
          readAt (es ++ [e]) k t v ==
            (if eligible k t v e then e.payload else readAt es k t v))))))
    else true))

namespace Aux

theorem le_trans (a b c : Event) :
    (!earlier a b) = true → (!earlier b c) = true → (!earlier a c) = true := by
  unfold earlier
  simp at *
  omega

theorem le_total (a b : Event) : (!earlier a b || !earlier b a) = true := by
  unfold earlier
  simp
  omega

end Aux

/-- A strictly latest event is a maximum for the (transaction, ordinal) order,
so a sort by the complement relation puts it first; hence publishing it only
changes the read inside its own eligibility rectangle. -/
theorem sound_Versions_LocalPublish : Versions.LocalPublish.Goal := by
  intro es e k t v hnew
  unfold readAt winner
  by_cases helig : eligible k t v e = true
  · have hfilter :
        (es ++ [e]).filter (eligible k t v) = (es.filter (eligible k t v)) ++ [e] := by
      simp [helig]
    rw [hfilter]
    have hnewest : newest ((es.filter (eligible k t v)) ++ [e]) = some e := by
      unfold newest
      let l := es.filter (eligible k t v)
      let le := fun a b : Event => !earlier a b
      have hsorted := List.pairwise_mergeSort (le := le) Aux.le_trans Aux.le_total (l ++ [e])
      have hmem : e ∈ (l ++ [e]).mergeSort le := by
        exact List.mem_mergeSort.mpr (by simp)
      cases hsort : (l ++ [e]).mergeSort le with
      | nil =>
          rw [hsort] at hmem
          simp at hmem
      | cons a rest =>
        have hhead : a = e := by
          by_cases haeq : a = e
          · exact haeq
          · exfalso
            have ha_mem_sort : a ∈ (a :: rest : List Event) := by simp
            have ha_mem_orig : a ∈ l ++ [e] := by
              apply List.mem_mergeSort.mp
              rw [hsort]
              exact ha_mem_sort
            have ha_l : a ∈ l := by
              simp at ha_mem_orig
              rcases ha_mem_orig with h | h
              · exact h
              · subst a
                contradiction
            have ha_es : a ∈ es := (List.mem_filter.mp ha_l).1
            have hae_new : earlier a e = true := hnew a ha_es
            have he_mem_rest : e ∈ rest := by
              have hm : e ∈ a :: rest := by
                simpa [hsort] using hmem
              cases hm with
              | head => exact False.elim (haeq rfl)
              | tail _ hrest => exact hrest
            have hle_ae : le a e = true := by
              have hs' : (a :: rest).Pairwise (fun x y => le x y = true) := by
                simpa [hsort] using hsorted
              exact List.rel_of_pairwise_cons hs' he_mem_rest
            have hnot : earlier a e = false := by
              simpa [le] using hle_ae
            simp [hae_new] at hnot
        rw [hhead]
        rfl
    rw [hnewest]
    simp [helig]
  · have hfilter : (es ++ [e]).filter (eligible k t v) = es.filter (eligible k t v) := by
      simp [helig]
    rw [hfilter]
    simp [helig]

end TinylambOpt.ResearchProofs.Atlas.Goal_Versions_LocalPublish
