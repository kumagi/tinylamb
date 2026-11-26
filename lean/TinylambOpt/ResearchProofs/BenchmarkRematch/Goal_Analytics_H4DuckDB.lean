import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H4DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : H4DuckDB.Goal := by
  intro blocks
  dsimp only
  have hflat : ∀ bs : List (List (Nat × Option Int)),
      ((bs.flatten).filter (fun r => r.1 < 4)).filterMap Prod.snd
        = bs.flatMap (fun rs => (rs.filter (fun r => r.1 < 4)).filterMap Prod.snd) := by
    intro bs; induction bs with
    | nil => rfl
    | cons b bs ih => simp only [List.flatten_cons, List.filter_append, List.filterMap_append, ih, List.flatMap_cons]
  have hempty : ∀ b : List (Nat × Option Int), ¬ ((b.map Prod.fst).min?.any (· < 4) = true) →
      (b.filter (fun r => r.1 < 4)).filterMap Prod.snd = [] := by
    intro b hb
    cases hm : (b.map Prod.fst).min? with
    | none =>
      have : b.map Prod.fst = [] := List.min?_eq_none_iff.mp hm
      have hb0 : b = [] := List.map_eq_nil_iff.mp this
      subst hb0; rfl
    | some m =>
      rw [hm] at hb
      have hm4 : ¬ m < 4 := by simpa using hb
      have hall := ((List.min?_eq_some_iff').mp hm).2
      have : b.filter (fun r => r.1 < 4) = [] := by
        rw [List.filter_eq_nil_iff]
        intro r hr
        have := hall r.1 (List.mem_map.mpr ⟨r, hr, rfl⟩)
        simp; omega
      rw [this]; rfl
  rw [hflat]
  induction blocks with
  | nil => rfl
  | cons b bs ih =>
    by_cases h : (b.map Prod.fst).min?.any (· < 4) = true
    · simp [List.filter_cons, h, List.flatMap_cons, ih]
    · have := hempty b h
      simp [List.filter_cons, h, List.flatMap_cons, ih, this]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H4DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_H4DuckDB.sound
