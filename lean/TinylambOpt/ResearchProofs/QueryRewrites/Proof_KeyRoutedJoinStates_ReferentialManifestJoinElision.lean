import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_ReferentialManifestJoinElision

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! A unique complete manifest has exactly one file per referenced key, so the
    join fan-out is one. The expanded nullable identifier stream is then the
    original identifier stream, which lets the executor elide the file lookup
    while preserving both row count and nullable identifier sum. -/

open TinylambOpt.ResearchGoals.QueryRewrites

/-- A key that occurs in a duplicate-free list occurs exactly once. -/
theorem count_eq_one_of_mem_nodup {a : Nat} {l : List Nat}
    (hnd : l.Nodup) (h : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      simp only [List.mem_cons] at h
      simp only [List.nodup_cons] at hnd
      rcases h with rfl | h
      · rw [List.count_cons]
        simp [(List.count_eq_zero).mpr hnd.1]
      · have hne : x ≠ a := by intro hxa; exact hnd.1 (hxa ▸ h)
        simp [List.count_cons, hne, ih hnd.2 h]

/-- When every referenced key has exactly one manifest entry, expansion by
    file lookups reproduces the input identifier list verbatim. -/
theorem flatMap_eq_map_of_count_one (rows : List (Nat × Option Int)) (files : List Nat)
    (h : ∀ r ∈ rows, files.count r.1 = 1) :
    (rows.flatMap fun x => List.replicate (files.count x.1) x.2) = rows.map Prod.snd := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      have hr := h r (by simp)
      have hrest : ∀ q ∈ rs, files.count q.1 = 1 := by
        intro q hq
        exact h q (by simp [hq])
      simp [hr, ih hrest]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ReferentialManifestJoinElision.Goal := by
  intro rows files hnd hcover
  dsimp only
  let expanded := rows.flatMap fun x => List.replicate (files.count x.1) x.2
  have hcount : ∀ r ∈ rows, files.count r.1 = 1 := by
    intro r hr
    exact count_eq_one_of_mem_nodup hnd (hcover r hr)
  have hflat : expanded = rows.map Prod.snd := by
    dsimp [expanded]
    exact flatMap_eq_map_of_count_one rows files hcount
  change (expanded.length, nsum expanded) = (rows.length, nsum (rows.map Prod.snd))
  rw [hflat]
  simp
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_ReferentialManifestJoinElision
