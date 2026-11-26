import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_OversizedTuple

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

private theorem size_mem_le (r : Row) (rs : Batch) (hmem : r ∈ rs) : r.bytes ≤ size rs := by
  induction rs with
  | nil => simp at hmem
  | cons x xs ih =>
      simp only [List.mem_cons] at hmem
      rcases hmem with rfl | hmem
      · simp [size]
      · have hle := ih hmem
        simp [size] at hle ⊢
        omega

theorem sound : OversizedTuple.Goal := by
  intro b d rs r hmem hbytes
  have hle : r.bytes ≤ size rs := size_mem_le r rs hmem
  induction d generalizing rs with
  | zero =>
      refine ⟨⟨rs, decide (b < size rs)⟩, ?_, ?_, ?_⟩
      · simp [leaves]
      · simp
        omega
      · exact hmem
  | succ d ih =>
      by_cases hsmall : size rs ≤ b
      · omega
      · let side := r.hash.testBit d
        have hchildmem : r ∈ bucket d side rs := by
          simpa [bucket, side] using hmem
        have hne : bucket d side rs ≠ [] := by
          intro hempty
          have hz : size (bucket d side rs) = 0 := by simp [hempty, size]
          have hpos : 0 < r.bytes := by omega
          have hlechild : r.bytes ≤ size (bucket d side rs) := size_mem_le r (bucket d side rs) hchildmem
          omega
        by_cases hstag : b < size (bucket d side rs) ∧
            stagnant (size (bucket d side rs)) (size rs) = true
        · refine ⟨⟨bucket d side rs, true⟩, ?_, ?_, hchildmem⟩
          · rcases side with _ | _
            · simp [leaves, hsmall, hstag, hne]
            · simp [leaves, hsmall, hstag, hne]
          · rfl
        · have hsizeChild : r.bytes ≤ size (bucket d side rs) := by
            exact size_mem_le r (bucket d side rs) hchildmem
          rcases ih (bucket d side rs) hchildmem hsizeChild with ⟨l, hl, hfb, hrl⟩
          refine ⟨l, ?_, hfb, hrl⟩
          rcases side with _ | _
          · simp [leaves, hsmall, hstag, hne, hl]
          · simp [leaves, hsmall, hstag, hne, hl]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_OversizedTuple
