import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# DenseCompositeCountIndex

The bounded two-column group key has an injective mapping to an array index. The
executor can count groups in a dense array when the bounds are certified.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.DenseCompositeCountIndex

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.DenseCompositeCountIndex.Goal := by
  intro rows width a b hb hrows
  have hwidth : 0 < width := by omega
  dsimp only
  induction rows with
  | nil => simp
  | cons r t ih =>
      have hry : r.2 < width := hrows r (by simp)
      have ht : ∀ q ∈ t, q.2 < width := fun q hq => hrows q (by simp [hq])
      have iht := ih ht
      by_cases heq : a * width + b = r.1 * width + r.2
      · have hm := congrArg (fun n : Nat => n % width) heq
        simp [Nat.add_mod, Nat.mod_eq_of_lt hb, Nat.mod_eq_of_lt hry] at hm
        have hb_eq : b = r.2 := hm
        have hmul : a * width = r.1 * width := by omega
        have ha_eq : a = r.1 := Nat.mul_right_cancel hwidth hmul
        subst a
        subst b
        simp [iht]
      · have hne : (r.1 * width + r.2 == a * width + b) = false := by
          simp
          omega
        have hrne : (r == (a, b)) = false := by
          simp
          intro h1
          subst r
          exact heq rfl
        simp only [List.map_cons]
        rw [List.count_cons, List.count_cons]
        simp [hne, hrne, iht]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.DenseCompositeCountIndex

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.DenseCompositeCountIndex.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
