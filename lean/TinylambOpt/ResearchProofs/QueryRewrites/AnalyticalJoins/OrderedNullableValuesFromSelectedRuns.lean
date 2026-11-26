import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# OrderedNullableValuesFromSelectedRuns

Gathering nullable values from selected contiguous runs yields the same ordered values
as applying the row mask.

An AVG input path can traverse known passing runs and preserve the input order used by
floating-point accumulation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false


namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.OrderedNullableValuesFromSelectedRuns
theorem filterMap_paired (id3 : Nat) (vs : List (Option Int)) :
    (vs.map (fun v => (id3, v))).filterMap (fun (k, v) => if k < 4 then v else none) =
      if id3 < 4 then vs.filterMap id else [] := by
  induction vs with
  | nil => by_cases h : id3 < 4 <;> simp [h]
  | cons v vs ih =>
      cases v with
      | none =>
          by_cases h : id3 < 4 <;> simp [h, ih]
      | some x =>
          by_cases h : id3 < 4 <;> simp [h, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.OrderedNullableValuesFromSelectedRuns.Goal := by
  intro runs
  induction runs with
  | nil => rfl
  | cons r t ih =>
      rcases r with ⟨id3, vs⟩
      by_cases h : id3 < 4 <;> simp [h, List.flatMap_cons, filterMap_paired, ih, Function.comp_def] <;> rfl


end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.OrderedNullableValuesFromSelectedRuns
