import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
A positive hot bitset hit is a plain non-NULL membership witness: the range
filter only removes keys, so a key accepted from `rangeKeys xs lo hi` also
occurs in `xs`, and membership of a non-NULL key makes `test` return
`some true` regardless of any NULL entries that would otherwise matter on a
miss. Hence the cold residual comparisons can be skipped after a hot hit.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_SkipColdResidualOnHotHit
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

private theorem testNF (y : Option Int) (ys : Keys) (w : Int) :
    test (y :: ys) (some w) =
      if (y :: ys).contains (some w) then some true
      else if (y :: ys).contains none then none else some false := by
  show (if _ then _ else if (some w).isNone then _ else _) = _
  simp

private theorem keep_iff_mem {xs : Keys} {v : Option Int} :
    (keep xs v = true) ↔ ∃ w, v = some w ∧ w ∈ xs.filterMap id := by
  cases xs with
  | nil =>
    constructor
    · intro h; exact absurd h (by simp [keep, test])
    · rintro ⟨w, _, hw⟩; exact absurd hw (by simp)
  | cons y ys =>
    cases v with
    | none =>
      constructor
      · intro h; exact absurd h (by simp [keep, test])
      · rintro ⟨u, hu, _⟩; exact absurd hu (by intro hh; cases hh)
    | some w =>
      constructor
      · intro h
        rw [keep, testNF] at h
        cases hc : (y :: ys).contains (some w) with
        | true =>
          exact ⟨w, rfl, List.mem_filterMap.mpr
            ⟨some w, List.contains_iff_mem.mp hc, rfl⟩⟩
        | false =>
          rw [if_neg (by simpa using hc)] at h
          cases h2 : (y :: ys).contains none with
          | true => rw [if_pos h2] at h; exact absurd h (by decide)
          | false => rw [if_neg (by simpa using h2)] at h; exact absurd h (by decide)
      · rintro ⟨u, hu, hu'⟩
        cases hu
        rw [keep, testNF]
        have hc : (y :: ys).contains (some w) = true :=
          List.contains_iff_mem.mpr <| by
            obtain ⟨a, ha, haeq⟩ := List.mem_filterMap.mp hu'
            cases a with
            | none => exact absurd haeq (by intro hh; cases hh)
            | some b => exact (Option.some.inj haeq) ▸ ha
        rw [if_pos hc]
        exact (by decide)

private theorem test_hit (xs : Keys) (w : Int) (h : some w ∈ xs) :
    test xs (some w) = some true := by
  cases xs with
  | nil => exact absurd h (by simp)
  | cons y ys => rw [testNF y ys w, if_pos (List.contains_iff_mem.mpr h)]

theorem sound : SkipColdResidualOnHotHit.Goal := by
  intro xs lo hi v h
  obtain ⟨w, hv, hw⟩ := keep_iff_mem.mp h
  subst hv
  obtain ⟨a, ha, haeq⟩ := List.mem_filterMap.mp hw
  obtain ⟨hin, _⟩ := List.mem_filter.mp ha
  cases a with
  | none => exact absurd haeq (by cases haeq)
  | some b => exact test_hit xs w ((Option.some.inj haeq) ▸ hin)

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_SkipColdResidualOnHotHit
