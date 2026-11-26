import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-!
# CalendarBlockDateWitnessSemijoin

`members p days` is the distinct set of dates of page `p` that survive the
calendar semijoin. It is empty exactly when `p.filter days.contains` is empty,
so a block-local date dictionary may skip entire pages without losing any
selected row. Repeated dates survive because the page filter keeps bag
multiplicity.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_CalendarBlockDateWitnessSemijoin

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

private theorem filter_nil_of_members_empty {p days : List Nat}
    (h : (members p days).isEmpty = true) : p.filter days.contains = [] := by
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro d hd
  have hd' : d ∈ p ∧ days.contains d = true := by
    simpa using hd
  have hdmem : d ∈ days := by simpa using hd'.2
  have hmem : d ∈ members p days := by
    simp [members, hd'.1, hdmem]
  have hnil : members p days = [] := by
    cases hm : members p days with
    | nil => rfl
    | cons x xs => simp [hm] at h
  rw [hnil] at hmem
  simp at hmem

theorem sound : CalendarBlockDateWitnessSemijoin.Goal := by
  intro pages days
  induction pages with
  | nil => simp
  | cons p ps ih =>
      simp only [List.filter_cons, List.flatMap_cons, List.flatten_cons, List.filter_append]
      by_cases hnil : members p days = []
      · have h : (members p days).isEmpty = true := by simp [hnil]
        have hp := filter_nil_of_members_empty h
        simp [hnil, hp, ih]
      · simp [hnil, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_CalendarBlockDateWitnessSemijoin
