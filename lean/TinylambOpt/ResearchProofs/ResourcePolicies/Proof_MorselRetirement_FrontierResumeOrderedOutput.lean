import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_FrontierResumeOrderedOutput

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
Publishing the completed prefix now and resuming at the retirement frontier composes exactly with a later full dump, provided duplicate ids report the same snapshot rows.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem readyIds_cons (rs : List Report) (start count : Nat) (h : (lookup rs start).isSome = true) :
    readyIds rs start (count + 1) = start :: readyIds rs (start + 1) count := by
  unfold readyIds
  rw [← List.range'_eq_map_range (s := start) (n := count + 1)]
  rw [← List.range'_eq_map_range (s := start + 1) (n := count)]
  rw [List.range'_succ]
  simp [h]

private theorem readyIds_nil_of_none (rs : List Report) (start count : Nat)
    (h : (lookup rs start).isSome = false) : readyIds rs start count = [] := by
  unfold readyIds
  cases count with
  | zero => simp
  | succ n =>
      rw [List.range_succ_eq_map]
      simp [h]

private theorem readyIds_length_le (rs : List Report) (start count : Nat) :
    (readyIds rs start count).length ≤ count := by
  have h := (List.takeWhile_prefix (p := fun i => (lookup rs i).isSome)
      (l := (List.range count).map (start + ·))).length_le
  simpa [readyIds] using h

private theorem lookup_append_of_isSome (a b : List Report) (i : Nat) (h : (lookup a i).isSome = true) :
    lookup (a ++ b) i = lookup a i := by
  unfold lookup at *
  rw [List.find?_append]
  cases hfa : a.find? (fun q => q.1 == i) with
  | none => simp [hfa] at h
  | some r => simp [hfa]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.FrontierResumeOrderedOutput.Goal := by
  intro old new start count hc
  induction count generalizing start with
  | zero => simp [emit, readyIds]
  | succ count ih =>
      by_cases h : (lookup old start).isSome = true
      · obtain ⟨rows, hrows⟩ := Option.isSome_iff_exists.mp h
        have happ := lookup_append_of_isSome old new start h
        have hrows' : lookup (old ++ new) start = some rows := by rw [happ, hrows]
        have hc' : consistent (old ++ new) := hc
        unfold emit
        rw [readyIds_cons old start count h, readyIds_cons (old ++ new) start count (by simp [hrows'])]
        simp [hrows, hrows']
        have hle : (readyIds old (start + 1) count).length ≤ count := readyIds_length_le old (start + 1) count
        have ih' := ih (start + 1)
        unfold emit at ih'
        rw [show start + ((readyIds old (start + 1) count).length + 1) =
            start + 1 + (readyIds old (start + 1) count).length by omega]
        exact ih'
      · have hnil := readyIds_nil_of_none old start (count + 1) (by simpa [h])
        simp [emit, hnil]
end TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_FrontierResumeOrderedOutput
