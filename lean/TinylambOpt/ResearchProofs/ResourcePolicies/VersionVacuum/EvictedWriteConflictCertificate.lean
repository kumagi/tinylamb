import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

namespace TinylambOpt.ResearchProofs.ResourcePolicies.VersionVacuum.EvictedWriteConflictCertificate

open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

theorem sound : EvictedWriteConflictCertificate.Goal := by
  intro evicted s k hmax t ht
  have hmem : t ∈ evicted.map Prod.snd := List.mem_map.mpr ⟨(k, t), ht, rfl⟩
  cases hm : (evicted.map Prod.snd).max? with
  | none =>
    rw [List.max?_eq_none_iff] at hm
    rw [hm] at hmem
    exact (List.not_mem_nil hmem).elim
  | some m =>
    rw [hm, Option.getD_some] at hmax
    exact Nat.le_trans ((List.max?_eq_some_iff.mp hm).2 t hmem) hmax

end TinylambOpt.ResearchProofs.ResourcePolicies.VersionVacuum.EvictedWriteConflictCertificate
