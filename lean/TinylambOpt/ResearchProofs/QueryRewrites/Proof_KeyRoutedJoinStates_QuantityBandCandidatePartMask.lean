import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_QuantityBandCandidatePartMask

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! The candidate index mask is derived from the same fixed quantity bands as
    the eligibility predicate. The masked distinct eligible parts are therefore
    empty exactly when no eligible part satisfies any band, which lets the
    executor test the cheap index mask instead of scanning band predicates. -/

open TinylambOpt.ResearchGoals.QueryRewrites

/-- The band-candidate indices are exactly the indices whose band accepts the
    given quantity. -/
theorem candidate_contains (qty i : Nat) :
    ((([(5,15),(15,25),(29,39)] : List (Nat × Nat)).zipIdx.filter
        (fun x => decide (x.1.1 ≤ qty) && decide (qty ≤ x.1.2))).map Prod.snd).contains i =
      (([(5,15),(15,25),(29,39)] : List (Nat × Nat))[i]?.any
        (fun x => decide (x.1 ≤ qty) && decide (qty ≤ x.2))) := by
  rcases i with _ | i
  · simp [List.mem_map, List.mem_filter]
  rcases i with _ | i
  · simp [List.mem_map, List.mem_filter]
  rcases i with _ | i
  · simp [List.mem_map, List.mem_filter]
  · simp [List.mem_map, List.mem_filter]

/-- A distinct-masked membership list is empty iff the unmasked original list
    has no element accepted by the corresponding predicate. -/
theorem members_isEmpty_eq_not_pred {α : Type} [BEq α] [LawfulBEq α]
    (xs b : List α) (p : α → Bool) (h : ∀ x, b.contains x = true ↔ p x = true) :
    (members xs b).isEmpty = !(xs.any p) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hm
    have hnil : xs.eraseDups.filter b.contains = [] := List.isEmpty_iff.mp hm
    have hnone : ∀ y ∈ xs, p y ≠ true := by
      intro y hy hp
      have hyDedup : y ∈ xs.eraseDups := (List.mem_eraseDups).mpr hy
      have hyb : b.contains y = true := (h y).mpr hp
      have hyFilter : y ∈ xs.eraseDups.filter b.contains := (List.mem_filter).mpr ⟨hyDedup, hyb⟩
      simpa [hnil] using hyFilter
    have hany : xs.any p = false := (List.any_eq_false).mpr hnone
    simp [hany]
  · intro hn
    have hany : xs.any p = false := by simpa using hn
    have hnone : ∀ y ∈ xs.eraseDups, b.contains y ≠ true := by
      intro y hy hyb
      have hyOrig : y ∈ xs := (List.mem_eraseDups).mp hy
      have hp : p y = true := (h y).mp hyb
      exact (List.any_eq_false.mp hany) y hyOrig hp
    have hnil : xs.eraseDups.filter b.contains = [] := (List.filter_eq_nil_iff).mpr hnone
    exact List.isEmpty_iff.mpr hnil

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.QuantityBandCandidatePartMask.Goal := by
  intro qty eligible
  dsimp only
  let bands : List (Nat × Nat) := [(5,15),(15,25),(29,39)]
  let candidates := (bands.zipIdx.filter (fun x => decide (x.1.1 ≤ qty) && decide (qty ≤ x.1.2))).map Prod.snd
  let p : Nat → Bool := fun i => (bands[i]? : Option (Nat × Nat)).any
    (fun x => decide (x.1 ≤ qty) && decide (qty ≤ x.2))
  change (members eligible candidates).isEmpty = !(eligible.any p)
  apply members_isEmpty_eq_not_pred
  intro x
  rcases x with _ | x
  · simp [candidates, p, bands, List.mem_map, List.mem_filter]
  rcases x with _ | x
  · simp [candidates, p, bands, List.mem_map, List.mem_filter]
  rcases x with _ | x
  · simp [candidates, p, bands, List.mem_map, List.mem_filter]
  · simp [candidates, p, bands, List.mem_map, List.mem_filter]
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_QuantityBandCandidatePartMask
