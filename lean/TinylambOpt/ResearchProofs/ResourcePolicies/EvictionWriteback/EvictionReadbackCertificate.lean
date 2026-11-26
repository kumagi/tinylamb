import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.ResourcePolicies.EvictionWriteback

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem evictionReadbackCertificate : EvictionReadbackCertificate.Goal := by
  intro s t wal hdirty hrec
  unfold reclaimable finish at hrec
  split at hrec
  · simp_all
  · rename_i hcond
    simp only [Bool.and_eq_true, beq_iff_eq, Bool.not_eq_true', bne_iff_ne, ne_eq,
      Bool.or_eq_true, decide_eq_true_eq] at hrec hcond
    have ht : t = s.live := by
      rcases hrec with ⟨_, _, h3⟩
      simpa using h3.symm
    subst ht
    refine ⟨rfl, ?_, ?_⟩
    · have : ¬ wal < s.live.wal := by intro hw; exact hcond (Or.inl (Or.inr hw))
      omega
    · rcases hrec with ⟨h1, _, _⟩
      simpa using h1
