import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_H4
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

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.H4.Goal := by
  intro runs
  induction runs with
  | nil => rfl
  | cons r t ih =>
      rcases r with ⟨id3, vs⟩
      by_cases h : id3 < 4 <;> simp [h, List.flatMap_cons, filterMap_paired, ih, Function.comp_def] <;> rfl


#print axioms TinylambOpt.ResearchProofs.November26.Analytics.Goal_H4.sound
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_H4
