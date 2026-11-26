import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# MatchingPriorWeekPrefilter

A store-week with no shifted partner cannot contribute to the comparison. The executor
can restrict the join input while retaining multiplicities for the remaining week keys.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MatchingPriorWeekPrefilter

theorem sum_filter_zero {α : Type} (l : List α) (p : α → Bool) (f : α → Nat)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    ((l.filter p).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
    by_cases hp : p a = true
    · have hz' : ∀ x ∈ l, p x = false → f x = 0 := by
        intro x hx hpx
        exact hz x (by simp [hx]) hpx
      have h := ih hz'
      simpa [hp] using h
    · have hpf : p a = false := by cases hpa : p a <;> simp_all
      have hfa : f a = 0 := hz a (by simp) hpf
      have hz' : ∀ x ∈ l, p x = false → f x = 0 := by
        intro x hx hpx
        exact hz x (by simp [hx]) hpx
      have h := ih hz'
      simpa [hp, hpf, hfa] using h

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.MatchingPriorWeekPrefilter.Goal := by
  intro left right
  dsimp only
  symm
  apply sum_filter_zero left (fun k => right.count (k.1, k.2 + 52) != 0)
      (fun k => right.count (k.1, k.2 + 52))
  intro x hx hp
  have hc : right.count (x.1, x.2 + 52) = 0 := by
    simp at hp
    exact hp
  simp [hc]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MatchingPriorWeekPrefilter

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.MatchingPriorWeekPrefilter.sound
