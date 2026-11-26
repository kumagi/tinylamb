import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.SuccessfulTraceByteConservation

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

private theorem alloc_sum (n : Nat) (hs hs' : List Nat) (h : alloc n hs = some hs') :
    hs'.sum + n = hs.sum := by
  induction hs generalizing hs' with
  | nil => simp [alloc] at h
  | cons hhd tl ih =>
    simp only [alloc] at h
    split at h
    · rename_i hle
      simp only [Option.some.injEq] at h
      subst h
      simp [List.sum_cons]
      omega
    · simp only [Option.map_eq_some_iff] at h
      rcases h with ⟨rest, hrest, rfl⟩
      have ih' := ih rest hrest
      simp [List.sum_cons] at ih' ⊢
      omega

private theorem place_sum (hs : List Nat) (rs : List Nat) (final : List Nat)
    (h : place hs rs = some final) : final.sum + rs.sum = hs.sum := by
  induction rs generalizing hs final with
  | nil =>
    simp [place] at h
    subst h
    simp
  | cons r rs ih =>
    simp only [place, List.foldlM_cons] at h
    cases halloc : alloc r hs with
    | none => simp [halloc] at h
    | some hs' =>
      simp only [halloc, bind, Option.bind_some] at h
      have hstep := alloc_sum r hs hs' halloc
      have hih := ih hs' final h
      simp [List.sum_cons]
      omega

theorem sound : SuccessfulTraceByteConservation.Goal := by
  intro hs rs final h
  exact place_sum hs rs final h

end TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.SuccessfulTraceByteConservation
