import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Flushing an ordered set of spilled group partitions computes the same SUM as folding the
whole stream in one place: `combine` accumulates the (sum, count) pair additively, the
group filter `forKey` distributes over append, and `nsum` is `answer ∘ state`. Hence the
aggregator may materialize group partials in any partition order without changing the
SQL SUM or its all-NULL (none) result. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_SpilledNullableSum
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay
open TinylambOpt.ResearchGoals.QueryRewrites

private theorem forKey_append {α β : Type} [BEq α] (xs ys : List (α × β)) (k : α) :
    forKey (xs ++ ys) k = forKey xs k ++ forKey ys k := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : (x.1 == k)
      · simp [forKey, h, ih]
      · simp [forKey, h, ih]

private theorem state_append (xs ys : List (Option Int)) :
    state (xs ++ ys) = combine (state xs) (state ys) := by
  unfold state
  simp [combine, List.filterMap_append, List.sum_append, List.length_append]

/-- The additive fold of partial states equals the state of the concatenated stream. -/
private theorem claim : ∀ (l : List (List (Option Int))) (i : Int) (m : Nat),
    (l.map state).foldl combine (i, m) = combine (state l.flatten) (i, m) := by
  intro l
  induction l with
  | nil =>
      intro i m
      simp [state, combine]
  | cons vs rest ih =>
      intro i m
      obtain ⟨sx, cx, hvs⟩ : ∃ sx cx, state vs = (sx, cx) := ⟨_, _, rfl⟩
      obtain ⟨sy, cy, hr⟩ : ∃ sy cy, state rest.flatten = (sy, cy) := ⟨_, _, rfl⟩
      rw [List.map_cons, List.foldl_cons, hvs,
        show combine (i, m) (sx, cx) = (i + sx, m + cx) from rfl, ih,
        List.flatten_cons, state_append, hvs, hr,
        show combine (sx, cx) (sy, cy) = (sx + sy, cx + cy) from rfl]
      show (sy + (i + sx), cy + (m + cx)) = (sx + sy + i, cx + cy + m)
      rw [Prod.mk.injEq]
      exact ⟨by omega, by omega⟩

private theorem nsum_eq_answer (xs : List (Option Int)) : nsum xs = answer (state xs) := by
  unfold nsum state answer
  simp

theorem sound : SpilledNullableSum.Goal := by
  intro ps k
  have hkey : forKey ps.flatten k = (ps.map (fun rs => forKey rs k)).flatten := by
    induction ps with
    | nil => rfl
    | cons rs rest ih =>
        rw [List.flatten_cons, forKey_append, ih]
        simp only [List.map_cons, List.flatten_cons]
  have hmap : ps.map (fun rs => state (forKey rs k)) =
      (ps.map (fun rs => forKey rs k)).map state := by
    simp [List.map_map]
  have hfold : (ps.map (fun rs => state (forKey rs k))).foldl combine (0, 0) =
      state ((ps.map (fun rs => forKey rs k)).flatten) := by
    have key := claim (ps.map (fun rs => forKey rs k)) 0 0
    obtain ⟨s, c, hsc⟩ : ∃ s c,
        state ((ps.map (fun rs => forKey rs k)).flatten) = (s, c) := ⟨_, _, rfl⟩
    rw [hmap, key, hsc]
    show combine (s, c) (0, 0) = (s, c)
    rw [show combine (s, c) (0, 0) = (s + 0, c + 0) from rfl, Prod.mk.injEq]
    exact ⟨by omega, by omega⟩
  rw [hfold, ← nsum_eq_answer, ← hkey]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_SpilledNullableSum
