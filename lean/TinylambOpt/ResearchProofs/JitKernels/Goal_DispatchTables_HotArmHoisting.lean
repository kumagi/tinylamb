import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! A frequently taken CASE arm can move to the front when every earlier arm
that could match the same key returns the same result. First-match priority is
then preserved for all keys, so the SQL result of the dispatch ladder is
unchanged while the hot path avoids probing the preceding arms. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_HotArmHoisting

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem sound : HotArmHoisting.Goal := by
  have hdrop : ∀ (l : List Arm) (a : Arm) (tail : List Arm) (x : Int), holds a x = false →
      pick (l ++ a :: tail) x = pick (l ++ tail) x := by
    intro l
    induction l with
    | nil => intro a tail x ha; simp [pick, ha]
    | cons c cs ih =>
        intro a tail x ha
        by_cases hc : holds c x
        · simp [pick, hc]
        · simpa [pick, hc] using ih a tail x ha
  intro a before tail h x
  induction before with
  | nil => simp
  | cons b bs ih =>
      have hbcond := h b (by simp)
      have hbscond : ∀ c ∈ bs, ∀ x, holds a x → holds c x → c.result = a.result := by
        intro c hc x hax hcx
        exact h c (by simp [hc]) x hax hcx
      by_cases hbx : holds b x
      · by_cases hax : holds a x
        · have hrs : b.result = a.result := hbcond x hax hbx
          simp [pick, hax, hbx, hrs]
        · simp [pick, hax, hbx]
      · by_cases hax : holds a x
        · have ihx := ih hbscond
          simpa [pick, hbx, hax] using ihx
        · simp [pick, hax, hbx]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_HotArmHoisting
