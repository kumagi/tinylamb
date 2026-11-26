import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
# OrderedChannelIntersectionDedup

Intersecting the three sales channels in a shared name and date key space preserves the
qualifying keys. The executor can use a common membership representation.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OrderedChannelIntersectionDedup
open TinylambOpt.ResearchGoals.QueryRewrites

theorem nodup_filter {α : Type} (p : α → Bool) : ∀ {l : List α}, l.Nodup → (l.filter p).Nodup := by
  intro l
  induction l with
  | nil => intro h; simp [List.Nodup]
  | cons a l ih =>
    intro h
    rw [List.nodup_cons] at h
    simp only [List.filter_cons]
    by_cases hp : p a = true
    · simp only [hp, ↓reduceIte]
      rw [List.nodup_cons]
      have haf : a ∉ l.filter p := by
        intro ha
        exact h.1 ((List.mem_filter.mp ha).1)
      exact ⟨haf, ih h.2⟩
    · simp only [hp, ↓reduceIte]
      exact ih h.2

theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l : List α}, l.Nodup → l.eraseDups = l := by
  intro l
  induction l with
  | nil => intro h; rfl
  | cons a l ih =>
    intro h
    rw [List.nodup_cons] at h
    rw [List.eraseDups_cons]
    have hf : l.filter (fun b => !(b == a)) = l := by
      rw [List.filter_eq_self]
      intro b hb
      have hneq : b ≠ a := by
        intro heq
        apply h.1
        rw [heq] at hb
        exact hb
      simp [hneq]
    rw [hf]
    rw [ih h.2]

theorem eraseDups_filter_idem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (p : α → Bool) :
    (l.eraseDups.filter p).eraseDups = l.eraseDups.filter p :=
  eraseDups_eq_self_of_nodup (nodup_filter p (KeyGrouping.nodup_eraseDups l))

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.OrderedChannelIntersectionDedup.Goal := by
  intro a b c
  dsimp only [members, countWhere]
  rw [eraseDups_filter_idem a (fun k => b.contains k)]
  rw [List.filter_filter]
  simp [Bool.and_comm]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OrderedChannelIntersectionDedup

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OrderedChannelIntersectionDedup.sound
