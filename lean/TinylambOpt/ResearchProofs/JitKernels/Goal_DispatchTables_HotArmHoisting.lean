import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_HotArmHoisting

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.HotArmHoisting.Goal := by
  intro a before
  induction before with
  | nil => simp [DispatchTables.pick]
  | cons b bs ih =>
      intro tail h x
      have hbs : ∀ c ∈ bs, ∀ x, DispatchTables.holds a x → DispatchTables.holds c x → c.result = a.result := by
        intro c hc y hay hcy
        exact h c (by simp [hc]) y hay hcy
      have ih' := ih tail hbs x
      by_cases hax : DispatchTables.holds a x
      · by_cases hbx : DispatchTables.holds b x
        · have hbr : b.result = a.result := h b (by simp) x (by simpa using hax) (by simpa using hbx)
          simp [DispatchTables.pick, hax, hbx, hbr]
        · simp [DispatchTables.pick, hax, hbx] at ih' ⊢
          exact ih'
      · by_cases hbx : DispatchTables.holds b x
        · simp [DispatchTables.pick, hax, hbx]
        · simp [DispatchTables.pick, hax, hbx]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_HotArmHoisting
