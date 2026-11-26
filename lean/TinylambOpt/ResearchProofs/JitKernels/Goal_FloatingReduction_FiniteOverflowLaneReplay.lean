import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FiniteOverflowLaneReplay

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem findIdx?_ne_none_of_mem {α : Type} (p : α → Bool) : ∀ {x : α} {l : List α},
    x ∈ l → p x = true → List.findIdx? p l ≠ none
  | x, [], hx, hp => by simp at hx
  | x, y :: ys, hx, hp => by
      rw [List.findIdx?_cons]
      by_cases hy : p y = true
      · simp [hy]
      · rcases List.mem_cons.mp hx with hxy | hxys
        · subst x
          simp [hp]
        · have ih := findIdx?_ne_none_of_mem p hxys hp
          simp [hy, ih]

theorem total_mem_scanl {β α : Type} (f : β → α → β) : ∀ (s : β) (l : List α),
    List.foldl f s l ∈ List.scanl f s l
  | s, [] => by simp
  | s, x :: xs => by
      rw [List.foldl_cons, List.scanl_cons]
      exact List.mem_cons_of_mem s (total_mem_scanl f (f s x) xs)

theorem sound : FiniteOverflowLaneReplay.Goal := by
  intro s a b hnone
  unfold firstInf at hnone
  unfold firstInf total
  let p : Float → Bool := Float.isInf
  let f : Float → Float → Float := fun x y => x + y
  have hfinalmem : List.foldl f s a ∈ List.scanl f s a := total_mem_scanl f s a
  have hfinalnot : p (List.foldl f s a) ≠ true := by
    intro hp
    have hn := findIdx?_ne_none_of_mem p hfinalmem hp
    exact hn hnone
  have hfinalfalse : p (List.foldl f s a) = false := Bool.eq_false_of_ne_true hfinalnot
  rw [List.scanl_append]
  rw [List.findIdx?_append]
  dsimp [p, f] at hnone hfinalfalse ⊢
  rw [hnone]
  simp only [Option.none_or]
  cases b with
  | nil => simp [hfinalfalse]
  | cons bx bxes =>
      rw [List.scanl_cons, List.findIdx?_cons]
      rw [hfinalfalse]
      simp
      congr 1
      funext i
      simp only [Function.comp_apply]
      omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FiniteOverflowLaneReplay
