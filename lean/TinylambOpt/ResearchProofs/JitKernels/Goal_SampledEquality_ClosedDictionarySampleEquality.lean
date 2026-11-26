import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Inside a dictionary with unique signatures, two members with equal sampled
signatures must be the same string. This licenses reuse of an equality class
for known build rows without a full byte comparison.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ClosedDictionarySampleEquality

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem sound : ClosedDictionarySampleEquality.Goal := by
  intro pool a b hnodup ha hb hs
  rcases List.mem_iff_getElem.mp ha with ⟨i, hi, hpi⟩
  rcases List.mem_iff_getElem.mp hb with ⟨j, hj, hpj⟩
  have hki : (keys pool)[i]? = some (sig a) := by
    simp [keys, List.getElem?_eq_getElem hi, hpi]
  have hkj : (keys pool)[j]? = some (sig b) := by
    simp [keys, List.getElem?_eq_getElem hj, hpj]
  have hsame : (keys pool)[i]? = (keys pool)[j]? := by
    rw [hki, hkj, hs]
  have hij : i = j :=
    (List.Nodup.getElem?_inj (by simpa [keys] using hi) hnodup).mp hsame
  subst j
  exact hpi.symm.trans hpj

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ClosedDictionarySampleEquality
