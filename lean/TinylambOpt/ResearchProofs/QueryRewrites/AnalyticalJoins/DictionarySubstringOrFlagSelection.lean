import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# DictionarySubstringOrFlagSelection

Combining the row selections for the substring and flag branches preserves the modeled
OR filter and includes a row satisfying both branches once.

A scan can test the substring on dictionary entries, combine the resulting row IDs with
the country selection, and avoid repeated string checks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.DictionarySubstringOrFlagSelection
open TinylambOpt.ResearchGoals.QueryRewrites

abbrev Row := Bool × String

theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      have ha := h a (by simp)
      have ht := ih (fun x hx => h x (by simp [hx]))
      simp only [List.filter_cons]
      rw [ha]
      cases q a <;> simp [ht]

theorem dict_contains_iff (rows : List Row) (s : String) (hs : s ∈ rows.map Prod.snd) :
    ((rows.map Prod.snd).eraseDups.filter (fun t => t.contains "data")).contains s =
      s.contains "data" := by
  by_cases h : s.contains "data" = true
  · have hsE : s ∈ (rows.map Prod.snd).eraseDups := List.mem_eraseDups.mpr hs
    have hm : s ∈ (rows.map Prod.snd).eraseDups.filter (fun t => t.contains "data") := by
      exact List.mem_filter.mpr ⟨hsE, h⟩
    have hc : ((rows.map Prod.snd).eraseDups.filter (fun t => t.contains "data")).contains s = true :=
      List.contains_iff_mem.mpr hm
    rw [hc, h]
  · have hf : s.contains "data" = false := by
      cases hv : s.contains "data" <;> simp_all
    have hnot : ¬ s ∈ (rows.map Prod.snd).eraseDups.filter (fun t => t.contains "data") := by
      intro hm
      exact h (List.mem_filter.mp hm).2
    have hc : ((rows.map Prod.snd).eraseDups.filter (fun t => t.contains "data")).contains s = false := by
      cases hv : ((rows.map Prod.snd).eraseDups.filter (fun t => t.contains "data")).contains s with
      | false => rfl
      | true =>
          exfalso
          exact hnot (List.contains_iff_mem.mp hv)
    rw [hc, hf]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.DictionarySubstringOrFlagSelection.Goal := by
  intro rows
  dsimp only
  have hf : rows.zipIdx.filter (fun (r, _) =>
        r.1 || ((rows.map Prod.snd).eraseDups.filter (fun s => s.contains "data")).contains r.2) =
      rows.zipIdx.filter (fun (r, _) => r.1 || r.2.contains "data") := by
    apply filter_congr_bool
    intro x hx
    rcases x with ⟨r, i⟩
    simp only
    have hz := List.mem_zipIdx hx
    have hri : r = rows[i] := by simpa using hz.2.2
    have hi : i < rows.length := by simpa using hz.2.1
    have hrmem : r ∈ rows := by
      rw [hri]
      exact List.getElem_mem hi
    have hs : r.2 ∈ rows.map Prod.snd := List.mem_map.mpr ⟨r, hrmem, rfl⟩
    rw [dict_contains_iff rows r.2 hs]
  rw [hf]


end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.DictionarySubstringOrFlagSelection
