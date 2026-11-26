import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `SingleIsland.EmptyFirst` — `∀ n, unrank model n 0 0 = some (List.replicate n false)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_EmptyFirst

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.SingleIsland

/-! Truth check: the rank-0 code of every length is the all-false bitmap. -/

private def probe : Bool :=
  (List.range 17).all (fun n =>
    unrank model n 0 0 == some (List.replicate n false))

#eval probe

private theorem accept_all (q : Nat) : model.accept q = true := by rfl

private theorem step_0_false : model.step 0 false = some 0 := by simp [model]

private theorem step_false_some : ∀ q, ∃ q', model.step q false = some q' := by
  intro q
  by_cases h0 : q = 0
  · subst q
    exact ⟨0, step_0_false⟩
  · exact ⟨2, by simp [model, h0]⟩

/-- Every state has at least one accepted continuation of any length. -/
private theorem count_pos : ∀ n q, 0 < count model n q := by
  intro n
  induction n with
  | zero =>
      intro q
      simp [count, accept_all]
  | succ n ih =>
      intro q
      rcases step_false_some q with ⟨q', hq'⟩
      have hp : 0 < count model n q' := ih q'
      simp [count, hq']
      omega

theorem sound_EmptyFirst : SingleIsland.EmptyFirst.Goal := by
  intro n
  induction n with
  | zero =>
      simp [unrank, accept_all]
  | succ n ih =>
      have hlow : lowCount model n 0 = count model n 0 := by
        simp [lowCount, step_0_false]
      have hpos : 0 < count model n 0 := count_pos n 0
      have hne : lowCount model n 0 ≠ 0 := by omega
      simp [unrank, hne, step_0_false, ih, List.replicate_succ]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_SingleIsland_EmptyFirst
