import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
When every lane delta for the guarded slot is nonnegative, the checked scan is
monotone, so the interval certificate follows from the two endpoint bounds:
`lo ≤ s` and `s + delta us k ≤ hi` imply `check lo hi s us k`. SQL execution may
emit a single range guard for a sign-uniform conflict group.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_MonotoneScatterRangeCertificate
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

private theorem sum_nonneg (l : List Int) (h : ∀ x ∈ l, 0 ≤ x) : 0 ≤ l.sum := by
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.sum_cons]
      exact Int.add_nonneg (h a (by simp))
        (ih fun x hx => h x (by simp [hx]))

private theorem scanl_bounds (l : List Int) :
    ∀ s : Int, (∀ x ∈ l, 0 ≤ x) → ∀ y ∈ l.scanl (·+·) s, s ≤ y ∧ y ≤ s + l.sum := by
  induction l with
  | nil =>
      intro s _ y hy
      rw [List.scanl_nil] at hy
      rw [List.mem_singleton] at hy
      subst hy
      simp
  | cons a t ih =>
      intro s h y hy
      have ha : 0 ≤ a := h a (by simp)
      have ht : ∀ x ∈ t, 0 ≤ x := fun x hx => h x (by simp [hx])
      have hsum : 0 ≤ t.sum := sum_nonneg t ht
      rw [List.scanl_cons] at hy
      rcases List.mem_cons.mp hy with rfl | hy
      · constructor
        · omega
        · rw [List.sum_cons]
          omega
      · obtain ⟨h1, h2⟩ := ih (s + a) ht y hy
        constructor
        · omega
        · rw [List.sum_cons]
          omega

theorem sound : MonotoneScatterRangeCertificate.Goal := by
  intro us k lo hi s hall hlo hhi
  have hge : ∀ x ∈ ((us.filter (fun u => u.1 == k)).map Prod.snd : List Int), 0 ≤ x := by
    intro x hx
    rcases List.mem_map.mp hx with ⟨u, hu, rfl⟩
    rcases List.mem_filter.mp hu with ⟨hus, hk⟩
    exact hall u hus (by simpa using hk)
  have key := scanl_bounds ((us.filter (fun u => u.1 == k)).map Prod.snd) s hge
  unfold check
  rw [List.all_eq_true]
  intro y hy
  obtain ⟨h1, h2⟩ := key y hy
  have h2' : y ≤ s + delta us k := h2
  have p1 : lo ≤ y := by omega
  have p2 : y ≤ hi := by omega
  simp [p1, p2]

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_MonotoneScatterRangeCertificate
