import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_FirstBarrier

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : ChildFirst.FirstBarrier.Goal := by
  intro s a b hs
  constructor
  · intro him
    have hc : ok s = true ∧ ok (step s (.addC (some a))) = true ∧
        ok (step (step s (.addC (some a))) (.addP a)) = true ∧
        ok (finish ChildFirst.model s a b) = true := by
      simpa [immediate, finish, run, step, ChildFirst.model] using him
    have hmem : some a ∈ (step s (.addC (some a))).children := by simp [step]
    have hba : bad (step s (.addC (some a))) a = false := (ok_iff_no_bad _).mp hc.2.1 a
    by_cases hpar : a ∈ s.parents
    · exact hpar
    · have hbad : bad (step s (.addC (some a))) a = true := (bad_iff _ a).mpr ⟨hmem, hpar⟩
      rw [hba] at hbad
      contradiction
  · intro ha
    have hs1 := ok_addC_some s a hs ha
    have hs2 : ok (step (step s (.addC (some a))) (.addP a)) = true := by
      simpa [step] using ok_addP (step s (.addC (some a))) a hs1
    have hs3 : ok (step (step (step s (.addC (some a))) (.addP a)) (.addP b)) = true := by
      simpa [step] using ok_addP (step (step s (.addC (some a))) (.addP a)) b hs2
    have hc : ok s = true ∧ ok (step s (.addC (some a))) = true ∧
        ok (step (step s (.addC (some a))) (.addP a)) = true ∧
        ok (finish ChildFirst.model s a b) = true := ⟨hs, hs1, hs2, hs3⟩
    simpa [immediate, finish, run, step, ChildFirst.model] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_ChildFirst_FirstBarrier
