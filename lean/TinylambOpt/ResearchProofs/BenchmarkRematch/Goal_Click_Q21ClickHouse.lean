import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
import TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q21ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group2

theorem goo_of_google (s : String) (h : s.contains "google" = true) : s.contains "goo" = true := by
  rw [String.contains_string_iff] at h ⊢
  have hinf : "goo".toList <:+: "google".toList := ⟨[], ['g', 'l', 'e'], by decide⟩
  exact hinf.trans h

theorem sound : Q21ClickHouse.Goal := by
  intro blocks
  unfold countWhere
  induction blocks with
  | nil => simp
  | cons b bs ih =>
    by_cases h : b.any (·.contains "goo") = true
    · simp only [List.filter_cons, h, ↓reduceIte, List.flatten_cons, List.filter_append, List.length_append, ih]
    · have h0 : b.filter (·.contains "google") = [] := by
        rw [List.filter_eq_nil_iff]
        intro s hs hg
        apply h
        rw [List.any_eq_true]
        exact ⟨s, hs, goo_of_google s (by simpa using hg)⟩
      simp only [List.filter_cons, h, Bool.false_eq_true, ↓reduceIte, List.flatten_cons, List.filter_append, List.length_append, h0, List.length_nil, Nat.zero_add, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q21ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q21ClickHouse.sound
