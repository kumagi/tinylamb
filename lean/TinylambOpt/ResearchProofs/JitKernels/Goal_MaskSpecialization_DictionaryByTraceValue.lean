import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Replaying a cached SQL result is safe when every slot actually demanded on the
    recorded trace has the same value on the new row. The proof is a congruence
    of the demand-SSA interpreter: its value and read trace depend only on the
    values of the slots it reads. If an operand short-circuits, the other
    operand is never demanded and therefore needs no matching guard. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_DictionaryByTraceValue

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

theorem sound : TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.DictionaryByTraceValue.Goal := by
  have hnn : (V.no == V.no) = true := by decide
  have hyy : (V.yes == V.yes) = true := by decide
  have hzz : (V.null == V.null) = true := by decide
  have hny : (V.no == V.yes) = false := by decide
  have hnz : (V.no == V.null) = false := by decide
  have hyn : (V.yes == V.no) = false := by decide
  have hyz : (V.yes == V.null) = false := by decide
  have hzy : (V.null == V.yes) = false := by decide
  have hzn : (V.null == V.no) = false := by decide
  have hrun : ∀ (e : E) (env₁ env₂ : List V),
      (∀ i ∈ (run env₁ e).2, slot env₁ i = slot env₂ i) → run env₂ e = run env₁ e := by
    intro e
    induction e with
    | arg i =>
        intro env₁ env₂ h
        exact (by simpa [run, slot] using (h i (by simp [run])).symm)
    | lit v =>
        intro env₁ env₂ h
        simp [run]
    | neg a ih =>
        intro env₁ env₂ h
        have ha := ih env₁ env₂ (by
          intro i hi
          exact h i (by simp [run, hi]))
        simp [run, ha]
    | isTrue a ih =>
        intro env₁ env₂ h
        have ha := ih env₁ env₂ (by
          intro i hi
          exact h i (by simp [run, hi]))
        simp [run, ha]
    | missing a ih =>
        intro env₁ env₂ h
        have ha := ih env₁ env₂ (by
          intro i hi
          exact h i (by simp [run, hi]))
        simp [run, ha]
    | conj a b iha ihb =>
        intro env₁ env₂ h
        rcases hrunA : run env₁ a with ⟨va, ra⟩
        cases va with
        | fault s =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA] using hi'))
            simp [run, after, hrunA, ha]
        | no =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA, hnn] using hi'))
            simp [run, after, hrunA, ha, hnn]
        | yes =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunA, hrunB, hnn, hyn, hyz, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunA, hrunB, hnn, hyn, hyz, hi']))
            simp [run, after, hrunA, ha, hb, hnn, hyn, hyz]
        | null =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunA, hrunB, hzn, hzz, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunA, hrunB, hzn, hzz, hi']))
            simp [run, after, hrunA, ha, hb, hzn, hzz]
    | disj a b iha ihb =>
        intro env₁ env₂ h
        rcases hrunA : run env₁ a with ⟨va, ra⟩
        cases va with
        | fault s =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA] using hi'))
            simp [run, after, hrunA, ha]
        | yes =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA, hyy] using hi'))
            simp [run, after, hrunA, ha, hyy]
        | no =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunA, hrunB, hny, hnz, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunA, hrunB, hny, hnz, hi']))
            simp [run, after, hrunA, ha, hb, hny, hnz]
        | null =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunA, hrunB, hzy, hzz, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunA, hrunB, hzy, hzz, hi']))
            simp [run, after, hrunA, ha, hb, hzy, hzz]
    | co a b iha ihb =>
        intro env₁ env₂ h
        rcases hrunA : run env₁ a with ⟨va, ra⟩
        cases va with
        | fault s =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA] using hi'))
            simp [run, after, hrunA, ha]
        | yes =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA, hyz] using hi'))
            simp [run, after, hrunA, ha, hyz]
        | no =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by simpa [run, after, hrunA, hnz] using hi'))
            simp [run, after, hrunA, ha, hnz]
        | null =>
            have ha := iha env₁ env₂ (by
              intro i hi
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunA, hrunB, hzz, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunA, hrunB, hzz, hi']))
            simp [run, after, hrunA, ha, hb, hzz]
    | branch p a b ihp iha ihb =>
        intro env₁ env₂ h
        rcases hrunP : run env₁ p with ⟨vp, rp⟩
        cases vp with
        | fault s =>
            have hp := ihp env₁ env₂ (by
              intro i hi
              have hi' : i ∈ rp := by simpa [hrunP] using hi
              exact h i (by simpa [run, after, hrunP] using hi'))
            simp [run, after, hrunP, hp]
        | yes =>
            have hp := ihp env₁ env₂ (by
              intro i hi
              have hi' : i ∈ rp := by simpa [hrunP] using hi
              exact h i (by
                rcases hrunA : run env₁ a with ⟨va, ra⟩
                cases va <;> simp [run, after, hrunP, hrunA, hyy, hi']))
            have ha := iha env₁ env₂ (by
              intro i hi
              rcases hrunA : run env₁ a with ⟨va, ra⟩
              have hi' : i ∈ ra := by simpa [hrunA] using hi
              exact h i (by
                cases va <;> simp [run, after, hrunP, hrunA, hyy, hi']))
            simp [run, after, hrunP, hp, ha, hyy]
        | no =>
            have hp := ihp env₁ env₂ (by
              intro i hi
              have hi' : i ∈ rp := by simpa [hrunP] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunP, hrunB, hny, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunP, hrunB, hny, hi']))
            simp [run, after, hrunP, hp, hb, hny]
        | null =>
            have hp := ihp env₁ env₂ (by
              intro i hi
              have hi' : i ∈ rp := by simpa [hrunP] using hi
              exact h i (by
                rcases hrunB : run env₁ b with ⟨vb, rb⟩
                cases vb <;> simp [run, after, hrunP, hrunB, hzy, hi']))
            have hb := ihb env₁ env₂ (by
              intro i hi
              rcases hrunB : run env₁ b with ⟨vb, rb⟩
              have hi' : i ∈ rb := by simpa [hrunB] using hi
              exact h i (by
                cases vb <;> simp [run, after, hrunP, hrunB, hzy, hi']))
            simp [run, after, hrunP, hp, hb, hzy]
  intro prev fresh e h
  exact hrun e prev fresh h

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_DictionaryByTraceValue
