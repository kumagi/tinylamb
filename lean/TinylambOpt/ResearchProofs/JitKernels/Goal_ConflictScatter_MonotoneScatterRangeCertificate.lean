import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_MonotoneScatterRangeCertificate

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.MonotoneScatterRangeCertificate.Goal := by
  intro us k lo hi s hnonneg hlo hfinal
  have hsumNonneg : ∀ ys : List Int, (∀ y ∈ ys, 0 ≤ y) → 0 ≤ ys.sum := by
    intro ys
    induction ys with
    | nil => intro h; simp
    | cons y ys ih =>
        intro h
        have hy : 0 ≤ y := h y (by simp)
        have hys : 0 ≤ ys.sum := ih (by intro z hz; exact h z (by simp [hz]))
        simp only [List.sum_cons]
        omega
  have hscan : ∀ (ys : List Int) (acc : Int),
      (∀ y ∈ ys, 0 ≤ y) → lo ≤ acc → acc + ys.sum ≤ hi →
      (ys.scanl (· + ·) acc).all (fun x => lo ≤ x && x ≤ hi) := by
    intro ys
    induction ys with
    | nil =>
        intro acc hys hacclo haccsum
        have hupp : acc ≤ hi := by simpa using haccsum
        simp [hacclo, hupp]
    | cons y ys ih =>
        intro acc hys hacclo haccsum
        have hy : 0 ≤ y := hys y (by simp)
        have hys' : ∀ z ∈ ys, 0 ≤ z := by
          intro z hz
          exact hys z (by simp [hz])
        have hsum : acc + (y + ys.sum) ≤ hi := by
          simpa only [List.sum_cons] using haccsum
        have hyssum : 0 ≤ ys.sum := hsumNonneg ys hys'
        have haccsum' : acc + y + ys.sum ≤ hi := by omega
        have hacclo' : lo ≤ acc + y := by omega
        have hupp : acc ≤ hi := by omega
        have hih := ih (acc + y) hys' hacclo' haccsum'
        simp [List.scanl_cons, hacclo, hupp, hih]
  apply hscan ((us.filter (fun u => u.1 == k)).map Prod.snd) s
  · intro y hy
    rcases List.mem_map.mp hy with ⟨u, hu, rfl⟩
    exact hnonneg u (List.mem_filter.mp hu).1 (by simpa using (List.mem_filter.mp hu).2)
  · exact hlo
  · simpa [TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.delta] using hfinal

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_MonotoneScatterRangeCertificate
