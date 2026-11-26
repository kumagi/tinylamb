import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-!
# PeerRunNeighborJoinCardinality

The cumulative `scanl` ranks of a run partition are its prefix sums. With all
run lengths positive they strictly increase, so the rank just below a peer's
center exists exactly when its own run has length one, and the rank just above
exists exactly when the next run has length one. A rank-minus-one/rank-plus-one
neighbor join therefore only fires for singleton predecessor and successor
runs.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRunNeighborJoinCardinality

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

private theorem decide_one_eq_beq (n : Nat) : decide (1 = n) = (n == 1) := by
  by_cases h : n = 1
  · subst n; simp
  · simp [h, Ne.symm h]

private theorem scanl_run : ∀ (xs : List Nat) (a prev curr next : Nat),
  0 < prev → 0 < curr → 0 < next → (∀ n ∈ xs, 0 < n) →
  let ranks := ((xs ++ [prev, curr, next]).scanl (·+·) a).take (xs.length + 3)
  let center := a + xs.sum + prev
  (ranks.contains (center - 1) && ranks.contains (center + 1)) = (prev == 1 && curr == 1)
| [], a, prev, curr, next, hp, hc, hn, hpos => by
    dsimp
    have hA : a + prev - 1 = a ↔ prev = 1 := by omega
    have hB : ¬ a + prev - 1 = a + prev := by omega
    have hC : ¬ a + prev - 1 = a + prev + curr := by omega
    have hE : ¬ a + prev + 1 = a := by omega
    have hG : ¬ a + prev + 1 = a + prev + curr + next := by omega
    simp [List.scanl, hA, hB, hC, hE, hG]
    by_cases hp1 : prev = 1 <;> by_cases hc1 : curr = 1 <;>
      simp [hp1, hc1, decide_one_eq_beq]
| x :: xs, a, prev, curr, next, hp, hc, hn, hpos => by
    dsimp
    have hx : 0 < x := hpos x (by simp)
    have htail : ∀ n ∈ xs, 0 < n := by
      intro n hnmem
      exact hpos n (by simp [hnmem])
    have ih := scanl_run xs (a + x) prev curr next hp hc hn htail
    dsimp at ih
    have ha1 : a ≠ a + x + xs.sum + prev - 1 := by omega
    have ha2 : a ≠ a + x + xs.sum + prev + 1 := by omega
    have ha1' : ¬ a + (x + (xs.sum + prev)) - 1 = a := by omega
    have hcenter : a + (x + xs.sum) + prev = (a + x) + xs.sum + prev := by omega
    rw [hcenter]
    simpa [List.scanl_cons, ha1, ha1', ha2, Nat.add_assoc] using ih

theorem sound : PeerRunNeighborJoinCardinality.Goal := by
  intro before prev curr next hprev hcurr hnext
  dsimp
  intro hpos
  have hmain := scanl_run before 1 prev curr next hprev hcurr hnext hpos
  dsimp at hmain
  simpa using hmain

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRunNeighborJoinCardinality
