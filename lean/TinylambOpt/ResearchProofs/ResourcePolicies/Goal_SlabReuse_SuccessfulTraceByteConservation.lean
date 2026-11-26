import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse
/-!
Registers the checked result for `SlabReuse.SuccessfulTraceByteConservation.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_SuccessfulTraceByteConservation
open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse SuccessfulTraceByteConservation

private theorem alloc_sum {n : Nat} {hs hs' : List Nat} (h : alloc n hs = some hs') :
    hs'.sum + n = hs.sum := by
  induction hs generalizing hs' with
  | nil => simp [alloc] at h
  | cons hhd tl ih =>
    unfold alloc at h
    by_cases hn : n ≤ hhd
    · simp [hn] at h
      subst h
      simp
      omega
    · simp [hn] at h
      match ha : alloc n tl with
      | none => simp [ha] at h
      | some tl' =>
        simp [ha] at h
        subst h
        have := ih ha
        simp
        omega

theorem sound : Goal := by
  intro hs rs final h
  induction rs generalizing hs final with
  | nil =>
    simp [place] at h
    subst h
    simp
  | cons r rs ih =>
    simp only [place, List.foldlM_cons] at h
    match ha : alloc r hs with
    | none => simp [ha] at h
    | some hs' =>
      simp only [ha, bind, Option.bind_some] at h
      have hsum := alloc_sum ha
      have ih' := ih hs' final h
      simp [List.sum_cons] at *
      omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_SuccessfulTraceByteConservation
