import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q57DuckDB

theorem lookupAux {α : Type} : ∀ (xs : List α) (start k : Nat),
    ((xs.zipIdx start).find? (fun r => r.2 == k)).map Prod.fst =
      if start ≤ k then xs[k - start]? else none := by
  intro xs
  induction xs with
  | nil => intro start k; simp
  | cons a xs ih =>
    intro start k
    rw [List.zipIdx_cons]
    by_cases hsk : (start == k) = true
    · have heq : start = k := by simpa using hsk
      subst k
      simp
    · have hne : start ≠ k := by simpa using hsk
      simp [List.find?_cons, hsk]
      rw [ih]
      by_cases hst : start ≤ k
      · have hlt : start < k := by omega
        have hk1 : start + 1 ≤ k := by omega
        have hsub : k - start = (k - (start + 1)) + 1 := by omega
        simp [hst, hk1, hsub]
      · have hk1 : ¬ start + 1 ≤ k := by omega
        simp [hst, hk1]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q57DuckDB.Goal := by
  intro xs i
  dsimp only
  have h0 := lookupAux xs 0 i
  have h1 := lookupAux xs 0 (i+1)
  have h2 := lookupAux xs 0 (i+2)
  simp at h0 h1 h2
  rw [h0, h1, h2]

end Proof_Q57DuckDB

#print axioms Proof_Q57DuckDB.sound
