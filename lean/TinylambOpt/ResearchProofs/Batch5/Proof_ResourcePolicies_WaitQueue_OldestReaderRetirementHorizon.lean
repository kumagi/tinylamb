import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_OldestReaderRetirementHorizon

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! Canceling tickets cannot lower the minimum reader generation: if a reader
with the original oldest generation survives, it remains the minimum. -/

namespace Scratch.OldestReaderRetirementHorizon

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.OldestReaderRetirementHorizon.Goal := by
  intro q owner gen oldest hmin hex
  let cq := cancel owner gen q
  have hmem : oldest ∈ (cq.filter (fun t => !t.writer)).map Ticket.generation := by
    rcases hex with ⟨t, ht, htw, htg⟩
    apply List.mem_map.mpr
    refine ⟨t, ?_, htg⟩
    exact List.mem_filter.mpr ⟨ht, by simp [htw]⟩
  have hle : ∀ b ∈ (cq.filter (fun t => !t.writer)).map Ticket.generation, oldest ≤ b := by
    intro b hb
    rcases List.mem_map.mp hb with ⟨t, ht, rfl⟩
    have htc : t ∈ cq := (List.mem_filter.mp ht).1
    have htq : t ∈ q := by
      simp [cq, cancel] at htc
      exact htc.1
    have hbq : t.generation ∈ (q.filter (fun t => !t.writer)).map Ticket.generation := by
      apply List.mem_map.mpr
      exact ⟨t, List.mem_filter.mpr ⟨htq, (List.mem_filter.mp ht).2⟩, rfl⟩
    exact (List.min?_eq_some_iff.mp hmin).2 t.generation hbq
  exact List.min?_eq_some_iff.mpr ⟨hmem, hle⟩

end Scratch.OldestReaderRetirementHorizon
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_OldestReaderRetirementHorizon
