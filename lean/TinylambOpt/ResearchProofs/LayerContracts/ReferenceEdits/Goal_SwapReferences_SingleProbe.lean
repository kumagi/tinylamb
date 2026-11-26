import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
import TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_SingleProbe

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits
open TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

theorem sound : SwapReferences.SingleProbe.Goal := by
  intro s a b hs
  by_cases hab : a = b
  · subst b
    have hmap : s.children.map (fun x => if x = some a then some a else x) = s.children := by
      induction s.children with
      | nil => rfl
      | cons x xs ih => cases x <;> simp_all
    have ht : ok (finish SwapReferences.model s a a) = true := by simpa [finish, run, step, SwapReferences.model, hmap] using hs
    have hba : bad (finish SwapReferences.model s a a) a = false := (ok_iff_no_bad _).mp ht a
    have hiff : bad (finish SwapReferences.model s a a) a = false ↔ ok (finish SwapReferences.model s a a) = true :=
      ⟨fun _ => ht, fun _ => hba⟩
    exact singleProbe_of_head_iff SwapReferences.model (fun _ _ => rfl) s a a hiff
  · let t := finish SwapReferences.model s a b
    have hnob : ¬ some b ∈ t.children := by
      let s1 := step s (.rewrite a (some b))
      have hc : (s1.children.map (fun x => if x = some b then some a else x)).count (some b) = 0 :=
        count_some_map_rewrite_src_lit s1.children b a (Ne.symm hab)
      exact List.count_eq_zero.mp (by simpa [t, finish, run, step, SwapReferences.model, s1] using hc)
    have hiff : bad t a = false ↔ ok t = true := by
      constructor
      · intro hba
        rw [ok_iff_prop]
        intro k hk
        have hkt : bad t k = true := (bad_iff t k).mpr hk
        by_cases hka : k = a
        · subst k
          rw [hba] at hkt
          contradiction
        · by_cases hkb : k = b
          · subst k
            exact hnob hk.1
          · have hf : bad t k = bad s k := by
              simpa [t] using frameKeys_SwapReferences s a b k (by simp [SwapReferences.model, hka, hkb])
            have hks : bad s k = false := (ok_iff_no_bad s).mp hs k
            rw [hf, hks] at hkt
            contradiction
      · intro hok
        exact (ok_iff_no_bad t).mp hok a
    exact singleProbe_of_head_iff SwapReferences.model (fun _ _ => rfl) s a b hiff

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Goal_SwapReferences_SingleProbe
