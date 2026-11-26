import TinylambOpt.ResearchGoals.EngineDecisions.Executor
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.Exec_SealedBuild

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.SealedBuild

private theorem em_none (ks : List Nat) : existsMatch none ks = false := by
  simp [existsMatch]

private theorem em_some (k : Nat) (ks : List Nat) :
    existsMatch (some k) ks = ks.contains k := by
  simp [existsMatch]

/-- A probe key whose partition is sealed can never appear in a legal tail. -/
private theorem not_mem_tail (part : Nat → Nat) (sealed tail : List Nat) (k : Nat)
    (htail : ∀ x ∈ tail, part x ∉ sealed) (hs : sealed.contains (part k) = true) :
    k ∉ tail := by
  intro hk
  exact absurd (List.contains_iff_mem.mp hs) (htail k hk)

private theorem legal_nil (part : Nat → Nat) (sealed : List Nat) :
    ∀ x ∈ ([] : List Nat), part x ∉ sealed := by
  intro x hx; cases hx

private theorem bfalse (b : Bool) (h : ¬(b = true)) : b = false := by
  cases b <;> simp_all

theorem sound : Goal := by
  intro part sealed built q b
  cases q with
  | none =>
      have dn : decideNow part sealed built none = some false := rfl
      refine ⟨fun h tail _ => ?_, fun h => ?_⟩
      · rw [dn] at h
        cases h
        exact em_none _
      · have h0 := h [] (legal_nil part sealed)
        rw [em_none] at h0
        exact dn.trans (congrArg some h0)
  | some k =>
      by_cases hb : built.contains k = true
      · -- positive early emission: k is already present in the built prefix
        have hbp : k ∈ built := List.contains_iff_mem.mp hb
        have hpos : ∀ tail : List Nat, existsMatch (some k) (built ++ tail) = true := by
          intro tail
          rw [em_some, List.contains_append]
          simp [hbp]
        have hdn : decideNow part sealed built (some k) = some true := by
          simp [decideNow, hbp]
        refine ⟨fun h tail _ => ?_, fun h => ?_⟩
        · rw [hdn] at h
          cases h
          exact hpos tail
        · have h0 := h [] (legal_nil part sealed)
          rw [hpos] at h0
          exact hdn.trans (congrArg some h0)
      · by_cases hs : sealed.contains (part k) = true
        · -- partition-local negative emission
          have hbp : ¬ k ∈ built := by
            intro hh; exact hb (List.contains_iff_mem.mpr hh)
          have hsp : part k ∈ sealed := List.contains_iff_mem.mp hs
          have hneg : ∀ tail : List Nat, (∀ x ∈ tail, part x ∉ sealed) →
              existsMatch (some k) (built ++ tail) = false := by
            intro tail htail
            rw [em_some, List.contains_append]
            have hk : k ∉ tail := not_mem_tail part sealed tail k htail hs
            have hb' : built.contains k = false := bfalse _ hb
            have ht : tail.contains k = false := bfalse _ (by simpa using hk)
            rw [hb', ht]
            simp
          have hdn : decideNow part sealed built (some k) = some false := by
            simp [decideNow, hbp, hsp]
          refine ⟨fun h tail htail => ?_, fun h => ?_⟩
          · rw [hdn] at h
            cases h
            exact hneg tail htail
          · have h0 := h [] (legal_nil part sealed)
            rw [hneg _ (legal_nil part sealed)] at h0
            exact hdn.trans (congrArg some h0)
        · -- undecided: the empty tail yields `false`, the singleton `[k]` yields `true`
          have hbp : ¬ k ∈ built := by
            intro hh; exact hb (List.contains_iff_mem.mpr hh)
          have hsp : ¬ part k ∈ sealed := by
            intro hh; exact hs (List.contains_iff_mem.mpr hh)
          have hdn : decideNow part sealed built (some k) = none := by
            simp [decideNow, hbp, hsp]
          have h0 : existsMatch (some k) (built ++ []) = false := by
            rw [em_some, List.contains_append, bfalse _ hb]
            simp
          have h1 : existsMatch (some k) (built ++ [k]) = true := by
            rw [em_some, List.contains_append, bfalse _ hb]
            simp
          refine ⟨fun h => by rw [hdn] at h; cases b <;> simp at h, fun h => ?_⟩
          exfalso
          have hl := h [] (legal_nil part sealed)
          have hr := h [k] (by
            intro x hx
            simp only [List.mem_singleton] at hx
            subst x
            exact fun hh => absurd hh hsp)
          rw [h0] at hl
          rw [h1] at hr
          cases b <;> simp_all

end TinylambOpt.ResearchProofs.EngineDecisions.Exec_SealedBuild
