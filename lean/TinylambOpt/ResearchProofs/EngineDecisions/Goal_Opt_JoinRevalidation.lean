import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `JoinRevalidation` — Revalidate an old elimination certificate by checking new active
  fact keys and changed dimension multiplicities. Checking only inserted fact rows
  misses a deleted dimension row. A mutation journal could supply the dirty set.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_JoinRevalidation

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.JoinRevalidation

theorem mem_dirty (a b c d : List Nat) (k : Nat) :
    k ∈ dirty a b c d ↔ k ∈ b ∧ ((!a.contains k) || (c.count k != d.count k)) = true := by
  dsimp [dirty]
  rw [List.mem_eraseDups, List.mem_filter]

theorem sound : Goal := by
  intro a b c d hold
  have hold' : a.all (fun k => c.count k == 1) = true := by
    simpa [oneMatch] using hold
  by_cases hnew : oneMatch b d = true
  · have hnew' : b.all (fun k => d.count k == 1) = true := by
      simpa [oneMatch] using hnew
    have hall : (dirty a b c d).all (fun k => d.count k == 1) = true := by
      rw [List.all_eq_true]
      intro k hk
      have hk' := (mem_dirty a b c d k).mp hk
      exact (List.all_eq_true.mp hnew') k hk'.1
    simp [hnew, hall]
  · have hnotdirty : ¬ ((dirty a b c d).all (fun k => d.count k == 1) = true) := by
      intro hall
      apply hnew
      dsimp [oneMatch]
      rw [List.all_eq_true]
      intro k hkb
      by_cases hkold : k ∈ a
      · by_cases hcnt : c.count k = d.count k
        · have holdNat : c.count k = 1 := beq_iff_eq.mp ((List.all_eq_true.mp hold') k hkold)
          have hdNat : d.count k = 1 := by omega
          exact beq_iff_eq.mpr hdNat
        · have hmem : k ∈ dirty a b c d := by
            rw [mem_dirty]
            constructor
            · exact hkb
            · simp [hkold, hcnt]
          exact (List.all_eq_true.mp hall) k hmem
      · have hmem : k ∈ dirty a b c d := by
          rw [mem_dirty]
          constructor
          · exact hkb
          · simp [hkold]
        exact (List.all_eq_true.mp hall) k hmem
    have hdfalse : (dirty a b c d).all (fun k => d.count k == 1) = false := by
      cases h : (dirty a b c d).all (fun k => d.count k == 1) <;> simp [h] at hnotdirty ⊢
    simp [hnew, hdfalse]

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_JoinRevalidation

-- C++ references to this model or its proof.
-- plan/optimizer_decisions.hpp
