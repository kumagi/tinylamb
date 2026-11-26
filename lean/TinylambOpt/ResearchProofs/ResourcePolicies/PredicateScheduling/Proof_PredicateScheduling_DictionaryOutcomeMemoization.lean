import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PredicateScheduling_DictionaryOutcomeMemoization
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.DictionaryOutcomeMemoization

theorem sound : Goal := by
  intro key test rows h
  simp only [List.map_map]
  apply List.map_congr_left
  intro x hx
  have hfind : (rows.find? (fun y => key y == key x)).isSome = true := by
    apply List.find?_isSome.mpr
    exact ⟨x, hx, by simp⟩
  cases hf : rows.find? (fun y => key y == key x) with
  | none => simp [hf] at hfind
  | some y =>
    simp [hf]
    have hy : y ∈ rows := List.mem_of_find?_eq_some hf
    have hkeq : key y = key x := by
      have hk := List.find?_some hf
      simpa using hk
    exact h y hy x hx hkeq
end Nov26.Proof_PredicateScheduling_DictionaryOutcomeMemoization
