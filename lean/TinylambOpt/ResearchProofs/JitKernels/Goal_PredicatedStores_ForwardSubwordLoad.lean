import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ForwardSubwordLoad

/-- A subword read from an already published wider store can be forwarded from
the stored bytes; the bounds hypothesis keeps every forwarded lane in range. -/
theorem sound : TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.ForwardSubwordLoad.Goal := by
  intro m base bs off n h
  have hlen : ((bs.drop off).take n).length = n := by
    simp [List.length_take, List.length_drop]
    omega
  apply List.ext_getElem
  · simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, hlen]
  · intro i hi hir
    have hoffi : off + i < bs.length := by omega
    have hsub : base + off + i - base = off + i := by omega
    have hle : base ≤ base + off + i := by omega
    have hstore :
        (TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.store m base bs) (base + off + i) =
          bs[off + i] := by
      simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.store, hsub, hoffi, hle]
    have hread :
        (TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read
            (TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.store m base bs) (base + off) n)[i] =
          bs[off + i] := by
      simp [TinylambOpt.ResearchGoals.JitKernels.PredicatedStores.read, hstore]
    rw [hread]
    rw [List.getElem_take, List.getElem_drop]

end TinylambOpt.ResearchProofs.JitKernels.Goal_PredicatedStores_ForwardSubwordLoad
