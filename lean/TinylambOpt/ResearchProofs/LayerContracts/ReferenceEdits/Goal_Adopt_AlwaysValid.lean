import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

/-!
Foreign-key checking must respect the observation point of the constraint and the parent
keys still present after each modeled edit.

Proved

- `Adopt.AlwaysValid` — `∀ s a b, ok s → immediate model s a b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_AlwaysValid

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : Adopt.AlwaysValid.Goal := by
  intro s a b hs
  by_cases hab : a = b
  · subst b
    have hmap : s.children.map (fun x => if x = some a then some a else x) = s.children := by
      induction s.children with
      | nil => rfl
      | cons x xs ih => cases x <;> simp_all
    let s1 := step s (.addP a)
    have hs1 : ok s1 = true := by simpa [s1] using ok_addP s a hs
    have hs2 : ok (step s1 (.rewrite a (some a))) = true := by simpa [s1, hmap, step] using hs1
    have hs3 : ok (step (step s1 (.rewrite a (some a))) (.delP a)) = true := by
      simpa [s1, hmap, step] using hs
    have hc : ok s = true ∧ ok s1 = true ∧ ok (step s1 (.rewrite a (some a))) = true ∧
        ok (finish Adopt.model s a a) = true := ⟨hs, hs1, hs2, hs3⟩
    simpa [immediate, finish, run, step, Adopt.model, s1] using hc
  · let s1 := step s (.addP b)
    let s2 := step s1 (.rewrite a (some b))
    let s3 := step s2 (.delP a)
    have hs1 : ok s1 = true := by simpa [s1] using ok_addP s b hs
    have hs2 : ok s2 = true := by
      dsimp [s2, s1]
      apply ok_rewrite_some
      · exact hs1
      · simp [step]
    have hno : ¬ some a ∈ s2.children := by simpa [s2] using no_child_after_rewrite_some s1 a b hab
    have hs3 : ok s3 = true := by simpa [s3] using ok_delP_of_no_children s2 a hs2 hno
    have hc : ok s = true ∧ ok s1 = true ∧ ok s2 = true ∧ ok s3 = true := ⟨hs, hs1, hs2, hs3⟩
    simpa [immediate, finish, run, step, Adopt.model, s1, s2, s3] using hc

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_Adopt_AlwaysValid
