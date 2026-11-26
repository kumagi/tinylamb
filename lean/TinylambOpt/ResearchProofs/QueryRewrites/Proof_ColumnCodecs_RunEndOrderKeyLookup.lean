import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

namespace TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_RunEndOrderKeyLookup

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs


/-!
# RunEndOrderKeyLookup

Looking up the first cumulative run end greater than a row position returns the same
order key as an expanded column. A scan can locate a row's key from run boundaries without
materializing all repeated keys, preserving every key observed by the query.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace RunEndCodecGoalHelpers

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

theorem find_lemma : ∀ (runs : List (Nat × Nat)) (s i : Nat), s ≤ i →
    ((((((runs.map Prod.snd).scanl (·+·) s).drop 1).zip (runs.map Prod.fst)).find?
      (fun r => i < r.1)).map Prod.snd) =
      (runs.flatMap (fun (key,n) => List.replicate n key))[i - s]? := by
  intro runs
  induction runs with
  | nil => intro s i h; simp
  | cons r rest ih =>
    intro s i h
    obtain ⟨k, n⟩ := r
    obtain ⟨t, ht⟩ : ∃ t, List.scanl (·+·) (s+n) (rest.map Prod.snd) = (s+n) :: t := by
      cases rest.map Prod.snd <;> simp [List.scanl]
    have ih2 := ih (s+n) i
    rw [ht] at ih2
    simp only [List.map_cons, List.scanl_cons, ht, List.drop_succ_cons, List.drop_zero, List.zip_cons_cons,
      List.find?_cons, List.flatMap_cons]
    by_cases hi : i < s + n
    · have : i - s < n := by omega
      simp [hi, List.getElem?_append_left, List.length_replicate, this, List.getElem?_replicate]
    · have h2 : s + n ≤ i := by omega
      have h3 : ¬ i - s < n := by omega
      have := ih2 h2
      simp only [hi, decide_false, Bool.false_eq_true, ↓reduceIte]
      simp only [List.drop_succ_cons, List.drop_zero] at this
      rw [this, List.getElem?_append_right (by simp; omega)]
      simp only [List.length_replicate]
      congr 1; omega

end RunEndCodecGoalHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs.RunEndOrderKeyLookup.Goal := by
  intro runs i
  dsimp only
  have := RunEndCodecGoalHelpers.find_lemma runs 0 i (Nat.zero_le _)
  simpa using this
end TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_RunEndOrderKeyLookup
