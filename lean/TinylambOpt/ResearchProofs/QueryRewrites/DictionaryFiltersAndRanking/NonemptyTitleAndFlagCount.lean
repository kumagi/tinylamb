import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# NonemptyTitleAndFlagCount

Intersecting the nonempty-title and flag masks and counting the result gives the number
of passing rows. The executor can combine both predicates in a bitmap scan.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.NonemptyTitleAndFlagCount
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem aux : ∀ zs : List (Bool × String),
    TinylambOpt.ResearchGoals.QueryRewrites.countWhere zs (fun (b, t) => b && t != "") =
      ((zs.filterMap (fun (b, t) => if b then some t else none)).filter (· != "")).length := by
  intro zs
  induction zs with
  | nil => simp [TinylambOpt.ResearchGoals.QueryRewrites.countWhere]
  | cons z zs ih =>
      rcases z with ⟨b, t⟩
      by_cases hb : b <;> by_cases ht : t = "" <;>
        (simp [TinylambOpt.ResearchGoals.QueryRewrites.countWhere, hb, ht] at ih ⊢; exact ih)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.NonemptyTitleAndFlagCount.Goal := by
  intro flags titles
  exact aux (flags.zip titles)

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.NonemptyTitleAndFlagCount
