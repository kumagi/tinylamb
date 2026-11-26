import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
For the WHERE `IN` acceptance predicate (`keep`), NULL build entries are
invisible: acceptance requires an exact non-NULL match, and the `NULL`-present
branch can only yield `none`/`some false`, never `some true`. So dropping
`None` entries from the key list before the comparison chain preserves every
row's acceptance decision. The proof reduces `keep l v` to membership of the
probe in `l.filterMap id`, which is invariant under filtering away `None`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_NullEntryFilterElision
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

private theorem filterMap_id_filter_isSome (xs : Keys) :
    (xs.filter Option.isSome).filterMap id = xs.filterMap id := by
  induction xs with
  | nil => rfl
  | cons y rest ih =>
    cases y with
    | some a => simp [ih]
    | none => simp [ih]

theorem sound : NullEntryFilterElision.Goal := by
  intro xs v
  cases h1 : keep (xs.filter Option.isSome) v with
  | true =>
    obtain ⟨w, hv, hw⟩ := keep_iff_mem.mp h1
    have h2 : keep xs v = true := keep_iff_mem.mpr
      ⟨w, hv, filterMap_id_filter_isSome xs ▸ hw⟩
    exact h2.symm
  | false =>
    cases h2 : keep xs v with
    | true =>
      obtain ⟨w, hv, hw⟩ := keep_iff_mem.mp h2
      have this : keep (xs.filter Option.isSome) v = true := keep_iff_mem.mpr
        ⟨w, hv, (filterMap_id_filter_isSome xs).symm ▸ hw⟩
      rw [h1] at this
      exact absurd this (by decide)
    | false => rfl

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_NullEntryFilterElision
