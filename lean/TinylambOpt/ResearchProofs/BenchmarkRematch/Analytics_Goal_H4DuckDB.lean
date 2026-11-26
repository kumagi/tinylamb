import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_H4DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem min_any_of_exists_lt (b : List (Nat × Option Int)) :
    (∃ r ∈ b, r.1 < 4) → ((b.map Prod.fst).min?.any (fun m => m < 4)) = true := by
  induction b with
  | nil => simp
  | cons r t ih =>
      intro h
      rcases h with ⟨s, hs, hslt⟩
      simp only [List.map_cons, List.min?_cons]
      by_cases hr : r.1 < 4
      · cases hm : (t.map Prod.fst).min? with
        | none => simp [hm, hr]
        | some m =>
            have hminlt : min r.1 m < 4 := Nat.lt_of_le_of_lt (Nat.min_le_left r.1 m) hr
            simp [hm, hminlt]
      · have hst : ∃ s ∈ t, s.1 < 4 := by
          rcases List.mem_cons.mp hs with hsr | hst
          · subst s
            exact False.elim (hr hslt)
          · exact ⟨s, hst, hslt⟩
        have htmin := ih hst
        have hmex : ∃ m, (t.map Prod.fst).min? = some m ∧ m < 4 := by
          rcases (Option.any_eq_true (fun m => decide (m < 4)) _).1 htmin with ⟨m, hm', hmtrue⟩
          exact ⟨m, hm', of_decide_eq_true hmtrue⟩
        rcases hmex with ⟨m, hm', hmlt⟩
        have hminlt : min r.1 m < 4 := Nat.lt_of_le_of_lt (Nat.min_le_right r.1 m) hmlt
        simp [hm', hminlt]

theorem filter_nil_of_no_lt (b : List (Nat × Option Int))
    (hno : ¬ ∃ r ∈ b, r.1 < 4) : b.filter (fun r => r.1 < 4) = [] := by
  induction b with
  | nil => simp
  | cons r t ih =>
      have hr : ¬ r.1 < 4 := by
        intro h
        exact hno ⟨r, by simp, h⟩
      have ht : ¬ ∃ s ∈ t, s.1 < 4 := by
        intro h
        rcases h with ⟨s, hs, hlt⟩
        exact hno ⟨s, by simp [hs], hlt⟩
      simp [hr, ih ht]

theorem scan_append (a b : List (Nat × Option Int)) :
    ((a ++ b).filter (fun r => r.1 < 4)).filterMap Prod.snd =
      (a.filter (fun r => r.1 < 4)).filterMap Prod.snd ++
      (b.filter (fun r => r.1 < 4)).filterMap Prod.snd := by
  simp [List.filter_append, List.filterMap_append]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.H4DuckDB.Goal := by
  intro blocks
  dsimp only
  induction blocks with
  | nil => simp
  | cons b t ih =>
      simp only [List.filter_cons, List.flatten_cons]
      by_cases hmin : (b.map Prod.fst).min?.any (fun m => m < 4)
      · simp [hmin, List.flatMap_cons, scan_append, ih]
      · have hno : ¬ ∃ r ∈ b, r.1 < 4 := by
          intro h
          exact hmin (min_any_of_exists_lt b h)
        have hscan : (b.filter (fun r => r.1 < 4)).filterMap Prod.snd = [] := by
          simp [filter_nil_of_no_lt b hno]
        simp [hmin, hscan, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_H4DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_H4DuckDB.sound
