import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRunNeighborJoinCardinality

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# PeerRunNeighborJoinCardinality

For a sorted peer partition with positive run lengths, the rank of the first
row of the center run has a predecessor rank exactly when the preceding run is a
singleton, and a successor rank exactly when the center run is a singleton. An
executor may therefore look up a rank-minus-one neighbor only in that case.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false


/-! Prefix-sum ranks of a scan are characterized by membership in a prefix. The
induction below keeps the start value generic so that the head of a positive run
can be peeled off without recomputing the whole scan. -/
theorem neighbors_mem_scanl (xs : List Nat) (s p c n : Nat)
    (hxs : ∀ v ∈ xs, 0 < v) (hp : 0 < p) (hc : 0 < c) (_hn : 0 < n) :
    let ranks := ((xs ++ [p, c, n]).scanl (·+·) s).take (xs.length + 3)
    let center := s + xs.sum + p
    ((center - 1 ∈ ranks) ↔ p = 1) ∧ ((center + 1 ∈ ranks) ↔ c = 1) := by
  induction xs generalizing s with
  | nil =>
      simp only [List.nil_append, List.length_nil, Nat.zero_add, List.sum_nil, Nat.add_zero]
      rw [List.scanl_cons, List.scanl_cons, List.scanl_cons, List.scanl_nil]
      simp [List.take, List.mem_cons, List.mem_singleton]
      constructor
      · constructor
        · intro h
          rcases h with h | h | h <;> omega
        · intro h
          left
          omega
      · constructor
        · intro h
          rcases h with h | h <;> omega
        · intro h
          right
          omega
  | cons x xs ih =>
      dsimp only
      rw [List.cons_append, List.scanl_cons, List.length_cons]
      have htake : xs.length + 1 + 3 - 1 = xs.length + 3 := by omega
      rw [List.take_cons (by omega), htake]
      simp only [List.sum_cons]
      have hx : 0 < x := hxs x (by simp)
      have hxs' : ∀ v ∈ xs, 0 < v := by intro v hv; exact hxs v (by simp [hv])
      have hih := ih (s + x) hxs'
      dsimp only at hih
      have hne1 : s + (x + xs.sum) + p - 1 ≠ s := by omega
      have hne2 : s + (x + xs.sum) + p + 1 ≠ s := by omega
      simp only [List.mem_cons, hne1, false_or, hne2]
      simpa only [Nat.add_assoc] using hih

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.PeerRunNeighborJoinCardinality.Goal := by
  intro before prev curr next hp hc hn
  dsimp only
  intro hpos
  have hm := neighbors_mem_scanl before 1 prev curr next hpos hp hc hn
  dsimp only at hm
  let ranks := ((before ++ [prev, curr, next]).scanl (·+·) 1).take (before.length + 3)
  let center := 1 + before.sum + prev
  change (ranks.contains (center - 1) && ranks.contains (center + 1)) = (prev == 1 && curr == 1)
  have hm1 : (center - 1 ∈ ranks) ↔ prev = 1 := hm.1
  have hm2 : (center + 1 ∈ ranks) ↔ curr = 1 := hm.2
  have c1 : ranks.contains (center - 1) = if prev = 1 then true else false := by
    by_cases h : prev = 1
    · have hmem : center - 1 ∈ ranks := hm1.mpr h
      have hb : ranks.contains (center - 1) = true := List.contains_iff_mem.mpr hmem
      rw [hb]
      simp [h]
    · have hnmem : center - 1 ∉ ranks := by intro hx; exact h (hm1.mp hx)
      have hf : ranks.contains (center - 1) = false := by
        cases hb : ranks.contains (center - 1)
        · rfl
        · exact False.elim (hnmem (List.contains_iff_mem.mp hb))
      rw [hf]
      simp [h]
  have c2 : ranks.contains (center + 1) = if curr = 1 then true else false := by
    by_cases h : curr = 1
    · have hmem : center + 1 ∈ ranks := hm2.mpr h
      have hb : ranks.contains (center + 1) = true := List.contains_iff_mem.mpr hmem
      rw [hb]
      simp [h]
    · have hnmem : center + 1 ∉ ranks := by intro hx; exact h (hm2.mp hx)
      have hf : ranks.contains (center + 1) = false := by
        cases hb : ranks.contains (center + 1)
        · rfl
        · exact False.elim (hnmem (List.contains_iff_mem.mp hb))
      rw [hf]
      simp [h]
  rw [c1, c2]
  by_cases hp1 : prev = 1 <;> by_cases hc1 : curr = 1 <;> simp [hp1, hc1]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRunNeighborJoinCardinality
