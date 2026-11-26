import TinylambOpt.ResearchGoals.JitKernels.CodeReachability
import TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

/-! One edge expansion per resident object is enough: every expansion that
changes the marked set activates a new object key, and there are only
`heap.length` objects. Hence the closure is reached after that many steps. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_BoundedCodeClosure

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem stable_from (heap : List Obj) (roots : List Key) (j : Nat)
    (h : mark heap roots (j + 1) = mark heap roots j) :
    ∀ m, mark heap roots (j + m) = mark heap roots j := by
  intro m
  induction m with
  | zero => rfl
  | succ m ih =>
      change grow heap (mark heap roots (j + m)) = mark heap roots j
      rw [ih]
      change grow heap (mark heap roots j) = mark heap roots j at h
      exact h

theorem sound : BoundedCodeClosure.Goal := by
  intro heap roots
  cases heap with
  | nil =>
      change (roots.eraseDups ++ roots.eraseDups.flatMap (edges [])).eraseDups = roots.eraseDups
      have hedges : roots.eraseDups.flatMap (edges []) = [] := by
        simp [edges]
      rw [hedges, List.append_nil]
      exact eraseDups_of_nodup _ (eraseDups_nodup _)
  | cons o os =>
      let n := (o :: os).length
      have hnpos : 0 < n := by simp [n]
      by_cases heq : mark (o :: os) roots (n + 1) = mark (o :: os) roots n
      · exact heq
      · exfalso
        have hnonstable : ∀ j, j ≤ n → mark (o :: os) roots (j + 1) ≠ mark (o :: os) roots j := by
          intro j hj hstable
          have hs := stable_from (o :: os) roots j hstable (n - j)
          have hjn : j + (n - j) = n := Nat.add_sub_of_le hj
          rw [hjn] at hs
          have hgrow : grow (o :: os) (mark (o :: os) roots j) = mark (o :: os) roots j := by
            change mark (o :: os) roots (j + 1) = mark (o :: os) roots j at hstable
            exact hstable
          have hlast : grow (o :: os) (mark (o :: os) roots n) = mark (o :: os) roots n := by
            rw [hs]
            exact hgrow
          exact heq hlast
        have hgrowth : ∀ j, j < n →
            (active (o :: os) (mark (o :: os) roots j)).length <
              (active (o :: os) (mark (o :: os) roots (j + 1))).length := by
          intro j hj
          have hnext : mark (o :: os) roots (j + 2) ≠ mark (o :: os) roots (j + 1) :=
            hnonstable (j + 1) (by omega)
          have hmark : mark (o :: os) roots (j + 1) =
              grow (o :: os) (mark (o :: os) roots j) := by
            cases j <;> rfl
          rw [hmark] at hnext ⊢
          exact grow_change_implies_active_growth (o :: os) hnext
        have hcount : ∀ j, j ≤ n →
            (active (o :: os) (mark (o :: os) roots j)).length ≥
              (active (o :: os) (mark (o :: os) roots 0)).length + j := by
          intro j hj
          induction j with
          | zero => omega
          | succ j ih =>
              have hprev := ih (by omega)
              have hg := hgrowth j (by omega)
              omega
        have hfinal := hcount n (by omega)
        have hlen := active_length_le (o :: os) (mark (o :: os) roots n)
        have ha0 : (active (o :: os) (mark (o :: os) roots 0)).length = 0 := by
          simp [n] at hfinal hlen
          omega
        have hgrow0 : grow (o :: os) (mark (o :: os) roots 0) = mark (o :: os) roots 0 := by
          apply eraseDups_append_of_mem
          · apply eraseDups_nodup
          · intro x hx
            rcases List.mem_flatMap.mp hx with ⟨z, hz, hxz⟩
            rcases edges_object (o :: os) hxz with ⟨ob, hb, hzob⟩
            have hmem : ob ∈ active (o :: os) (mark (o :: os) roots 0) := by
              apply mem_active
              · exact hb
              · simpa [hzob] using hz
            have hnil : active (o :: os) (mark (o :: os) roots 0) = [] := by
              apply List.eq_nil_iff_forall_not_mem.mpr
              intro y hy
              have hycount : 0 < (active (o :: os) (mark (o :: os) roots 0)).length := by
                have := List.length_pos_iff_exists_mem.mpr ⟨y, hy⟩
                omega
              omega
            simp [hnil] at hmem
        have hnonstable0 := hnonstable 0 (by omega)
        apply hnonstable0
        change grow (o :: os) (mark (o :: os) roots 0) = mark (o :: os) roots 0
        exact hgrow0

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_BoundedCodeClosure
