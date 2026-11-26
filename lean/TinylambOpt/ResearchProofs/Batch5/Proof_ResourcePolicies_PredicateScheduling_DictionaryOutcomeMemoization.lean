import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_DictionaryOutcomeMemoization

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! A dictionary lookup may reuse the first representative of an equality
class when the outcome assumption makes every representative interchangeable. -/

namespace Scratch.DictionaryOutcomeMemoization

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem find_map_test : ∀ (key : Nat → Nat) (test : Nat → Outcome) (rows : List Nat) x,
    x ∈ rows →
    (∀ y ∈ rows, key y = key x → test y = test x) →
    (rows.find? (fun y => key y == key x)).map test = some (test x) := by
  intro key test rows
  induction rows with
  | nil => intro x hx; simp at hx
  | cons y ys ih =>
      intro x hx h
      by_cases hky : key y == key x
      · have heq : key y = key x := by simpa using hky
        have hty : test y = test x := h y (by simp) heq
        simp [List.find?, hky, hty]
      · have hxys : x ∈ ys := by
          cases hx with
          | head => simp at hky
          | tail _ hmem => exact hmem
        have htail : ∀ z ∈ ys, key z = key x → test z = test x := by
          intro z hz hk
          exact h z (by simp [hz]) hk
        have hres := ih x hxys htail
        simp [List.find?, hky, hres]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.DictionaryOutcomeMemoization.Goal := by
  intro key test rows h
  rw [List.map_map]
  apply List.map_congr_left
  intro x hx
  exact find_map_test key test rows x hx (fun y hy hk => h y hy x hx hk)

end Scratch.DictionaryOutcomeMemoization
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_DictionaryOutcomeMemoization
