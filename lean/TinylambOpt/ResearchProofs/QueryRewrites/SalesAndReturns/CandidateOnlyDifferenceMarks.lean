import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# CandidateOnlyDifferenceMarks

Deleting catalog and web keys from the distinct store candidates gives the same
cardinality as the modeled set difference.

The executor can keep only the store candidate set and remove matches as it reads the
other channels.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CandidateOnlyDifferenceMarks
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-- Erasing a list of keys from a list leaves exactly the elements that were
not in the key list. -/
theorem foldl_erase_length (l s : List Nat) :
    (l.foldl (fun keys k => keys.filter (fun x => x != k)) s).length =
      (s.filter (fun x => !l.contains x)).length := by
  induction l generalizing s with
  | nil =>
    simp only [List.foldl_nil]
    have hf : s.filter (fun x => ![].contains x) = s := by
      rw [List.filter_eq_self]
      intro x hx
      simp
    rw [hf]
  | cons a l ih =>
    rw [List.foldl_cons, ih (s.filter (fun x => x != a)), List.filter_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro x hx
    rw [Bool.eq_iff_iff]
    simp only [Bool.and_eq_true, bne_iff_ne]
    constructor <;> intro h <;> simp_all

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CandidateOnlyDifferenceMarks.Goal := by
  intro store catalog web
  dsimp only
  rw [foldl_erase_length]
  simp only [List.contains_append]
  unfold countWhere
  apply congrArg List.length
  apply List.filter_congr
  intro x hx
  rw [Bool.eq_iff_iff]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CandidateOnlyDifferenceMarks
