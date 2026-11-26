import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_QualifiedYearCountsFromMoviePersonPostings. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_QualifiedYearCountsFromMoviePersonPostings

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# QualifiedYearCountsFromMoviePersonPostings

Grouping qualifying cast occurrences by distinct movie keys and counting female
persons in each movie bucket preserves the total count of female persons on
qualifying movies. Transposing the adjacency from person rows to movie postings
therefore keeps the per-year histogram exact, including duplicate movie/person
occurrences.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites (countWhere forKey)
open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping (forKey_filter_perm)

theorem krs_filter_map_snd_filter_length (cast : List (Nat × Nat)) (p : Nat → Bool) (female : Nat → Bool) :
    (((cast.filter (fun r => p r.1)).map Prod.snd).filter female).length =
      countWhere cast (fun r => female r.2 && p r.1) := by
  dsimp [countWhere]
  induction cast with
  | nil => simp
  | cons r rs ih =>
      by_cases hp : p r.1 <;> by_cases hf : female r.2 <;> simp [hp, hf, ih]

theorem krs_forKey_flatMap_filter_length (movies : List Nat) (cast : List (Nat × Nat)) (female : Nat → Bool) :
    ((movies.flatMap (forKey cast)).filter female).length =
      (movies.map (fun m => countWhere (forKey cast m) female)).sum := by
  induction movies with
  | nil => simp
  | cons m ms ih => simp [countWhere, ih, List.filter_append]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.QualifiedYearCountsFromMoviePersonPostings.Goal := by
  intro cast female movieYear y
  dsimp
  let p := fun m : Nat => movieYear m == some y
  let movies := (cast.map Prod.fst).eraseDups.filter p
  have hperm := forKey_filter_perm cast p
  have hlen := (hperm.filter female).length_eq
  rw [krs_filter_map_snd_filter_length cast p female] at hlen
  rw [krs_forKey_flatMap_filter_length movies cast female] at hlen
  simpa [p, movies] using hlen.symm
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_QualifiedYearCountsFromMoviePersonPostings
