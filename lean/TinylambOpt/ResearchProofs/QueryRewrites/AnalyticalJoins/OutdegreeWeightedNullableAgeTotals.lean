import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# OutdegreeWeightedNullableAgeTotals

Weighting a person's age and count state by outgoing edge multiplicity preserves the
modeled country aggregate. The executor can avoid expanding identical age contributions.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.OutdegreeWeightedNullableAgeTotals

open TinylambOpt.ResearchGoals.QueryRewrites

theorem filterMap_replicate_sum (n : Nat) (a : Option Int) : ((List.replicate n a).filterMap id).sum = (n : Int) * a.getD 0 := by
  cases a <;> simp [List.sum_replicate_int]

theorem filterMap_replicate_length (n : Nat) (a : Option Int) : ((List.replicate n a).filterMap id).length = if a.isSome then n else 0 := by
  cases a <;> simp

theorem len_flatMap (people : List (Nat × Option Int)) (edges : List Nat) :
    (people.flatMap (fun (k,a) => List.replicate (edges.count k) a)).length =
      (people.map (fun r => edges.count r.1)).sum := by
  induction people with
  | nil => simp
  | cons p t ih =>
      rcases p with ⟨k,a⟩
      simp [ih]

theorem sum_flatMap (people : List (Nat × Option Int)) (edges : List Nat) :
    ((people.flatMap (fun (k,a) => List.replicate (edges.count k) a)).filterMap id).sum =
      total people (fun (k,a) => (edges.count k : Int) * a.getD 0) := by
  induction people with
  | nil => simp [total]
  | cons p t ih =>
      rcases p with ⟨k,a⟩
      simp [total, ih, filterMap_replicate_sum, List.sum_append]

theorem count_flatMap (people : List (Nat × Option Int)) (edges : List Nat) :
    ((people.flatMap (fun (k,a) => List.replicate (edges.count k) a)).filterMap id).length =
      ((people.filter (fun r => r.2.isSome)).map (fun r => edges.count r.1)).sum := by
  induction people with
  | nil => simp
  | cons p t ih =>
      rcases p with ⟨k,a⟩
      simp [ih, filterMap_replicate_length, List.filter_cons, List.length_append]
      by_cases ha : a.isSome = true <;> simp [ha]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.OutdegreeWeightedNullableAgeTotals.Goal := by
  intro people edges
  dsimp only
  apply Prod.ext
  · exact len_flatMap people edges
  · apply Prod.ext
    · exact sum_flatMap people edges
    · exact count_flatMap people edges

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.OutdegreeWeightedNullableAgeTotals

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.OutdegreeWeightedNullableAgeTotals.sound
