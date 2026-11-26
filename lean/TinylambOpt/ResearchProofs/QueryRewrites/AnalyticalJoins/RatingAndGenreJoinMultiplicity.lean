import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# RatingAndGenreJoinMultiplicity

For each movie key, the modeled number of qualifying rating-genre pairs is the product
of the matching input multiplicities.

The executor can probe the genre index for eligible movies and update year groups
without enumerating every pair.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false


namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.RatingAndGenreJoinMultiplicity
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites

theorem flatMap_const_length (d r : List Nat) :
    (d.flatMap (fun _ => r)).length = d.length * r.length := by
  induction d with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.length_append, ih, Nat.add_mul, Nat.add_comm]

theorem filter_length_count (d : List Nat) (k : Nat) :
    (d.filter (fun x => x == k)).length = d.count k := by
  induction d with
  | nil => simp
  | cons x xs ih => by_cases h : (x == k) = true <;> simp [List.count_cons, h, ih]

theorem count_pos_iff_contains (l : List Nat) (k : Nat) :
    0 < l.count k ↔ l.contains k = true := by
  rw [List.contains, List.elem_iff]
  constructor
  · intro hp
    by_cases hm : k ∈ l
    · exact hm
    · have hz : l.count k = 0 := (List.count_eq_zero).2 hm
      omega
  · intro hm
    by_cases hz : l.count k = 0
    · have hn : ¬ k ∈ l := (List.count_eq_zero).1 hz
      exact (hn hm).elim
    · omega

theorem one_length (drama ratings : List Nat) (k : Nat) :
    ((drama.filter (fun x => x == k)).flatMap (fun _ => ratings.filter (fun x => x == k))).length =
      drama.count k * ratings.count k := by
  rw [flatMap_const_length, filter_length_count, filter_length_count]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.RatingAndGenreJoinMultiplicity.Goal := by
  intro titles drama ratings
  induction titles with
  | nil => simp
  | cons k t ih =>
      by_cases h : ratings.contains k = true
      · have hp : 0 < ratings.count k := (count_pos_iff_contains ratings k).2 h
        have hm : k ∈ ratings := by
          by_cases hm : k ∈ ratings
          · exact hm
          · have hz : ratings.count k = 0 := (List.count_eq_zero).2 hm
            omega
        rw [List.flatMap_cons, List.length_append, one_length, ih, List.filter_cons]
        rw [if_pos h]
        simp
      · have hz : ratings.count k = 0 := by
          by_cases hz : ratings.count k = 0
          · exact hz
          · have hp : 0 < ratings.count k := by omega
            exact (h ((count_pos_iff_contains ratings k).1 hp)).elim
        have hmnot : ¬ k ∈ ratings := (List.count_eq_zero).1 hz
        rw [List.flatMap_cons, List.length_append, one_length, ih, List.filter_cons]
        rw [if_neg h]
        simp [hz]
end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.RatingAndGenreJoinMultiplicity
