import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_CounterOffsetGuardReuse

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.CounterOffsetGuardReuse.Goal := by
  have hmax : ∀ (l : List Int) (v : Int), v ∈ l → v ≤ l.max?.getD 0 := by
    intro l
    induction l with
    | nil => intro v h; simp at h
    | cons x xs ih =>
        intro v hv
        simp only [List.mem_cons] at hv
        rcases hv with rfl | hv
        · cases xs with
          | nil => simp
          | cons y ys => simp [Int.le_max_left]
        · have hvle := ih v hv
          cases xs with
          | nil => simp at hv
          | cons y ys => simp at hvle ⊢; omega
  intro lo hi a b xs hab hs hb
  simp [safe, List.all_eq_true] at hs ⊢
  intro v hv
  have hs_v := hs v hv
  have hvmax : v ≤ high xs := by
    unfold high
    exact hmax (path xs) v hv
  constructor <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_CounterOffsetGuardReuse
