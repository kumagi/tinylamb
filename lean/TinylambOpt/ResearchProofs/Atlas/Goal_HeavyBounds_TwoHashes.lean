import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_TwoHashes

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds

-- Truth check (exhaustive, run before the proof): all lists of length <= 4
-- with entries in 0..3, for five hash functions (identity, constant 0,
-- mod 2, mod 3, increment), satisfy the inequality.

/-- Summing pointwise bounds preserves the inequality. -/
private theorem sum_map_le_sum_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by
        intro y hy
        exact h y (by simp [hy])
      have := ih ht
      omega

/-- Every occurrence of `x` in `b` contributes an occurrence of `h x`
    to `b.map h`; hence the plain count is at most the hashed count. -/
private theorem count_le_hash_count (b : List Nat) (h : Nat → Nat) (x : Nat) :
    b.count x ≤ (b.map h).count (h x) := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      rw [List.count_cons, List.map_cons, List.count_cons]
      by_cases hyx : (y == x) = true
      · have hy : y = x := of_decide_eq_true hyx
        subst y
        simp [ih]
      · by_cases hhy : (h y == h x) = true
        · simp [hyx, hhy]
          omega
        · simp [hyx, hhy]
          exact ih

theorem sound_HeavyBounds_TwoHashes : HeavyBounds.TwoHashes.Goal := by
  intro a b h g
  change (a.map fun x => b.count x).sum ≤
    (a.map fun x => min ((b.map h).count (h x)) ((b.map g).count (g x))).sum
  apply sum_map_le_sum_nat
  intro x hx
  have hh := count_le_hash_count b h x
  have hg := count_le_hash_count b g x
  exact (Nat.le_min).2 ⟨hh, hg⟩

end TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_TwoHashes
