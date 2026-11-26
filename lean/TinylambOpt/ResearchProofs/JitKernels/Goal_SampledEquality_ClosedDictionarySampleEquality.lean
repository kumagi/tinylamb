import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ClosedDictionarySampleEquality

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SampledEquality.ClosedDictionarySampleEquality.Goal := by
  have hnode : ∀ (pool : List Bytes) (x y : Bytes), (pool.map sig).Nodup →
      x ∈ pool → y ∈ pool → sig x = sig y → x = y := by
    intro pool
    induction pool with
    | nil => intro x y hnd hx hy hsig; simp at hx
    | cons z pool ih =>
        intro x y hnd hx hy hsig
        simp only [List.map_cons, List.nodup_cons] at hnd
        simp only [List.mem_cons] at hx hy
        rcases hx with rfl | hx
        · rcases hy with rfl | hy
          · rfl
          · exfalso
            apply hnd.1
            rw [hsig]
            exact List.mem_map.mpr ⟨y, hy, rfl⟩
        · rcases hy with rfl | hy
          · exfalso
            apply hnd.1
            rw [← hsig]
            exact List.mem_map.mpr ⟨x, hx, rfl⟩
          · exact ih x y hnd.2 hx hy hsig
  intro pool a b hnd ha hb hsig
  exact hnode pool a b hnd ha hb hsig

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ClosedDictionarySampleEquality
