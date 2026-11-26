import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_LimitCancellationFrontier

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
LIMIT may cancel later morsels once a contiguous completed prefix already supplies enough rows: appending future reports only extends that prefix, so the first k rows are unaffected.
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

private theorem lookup_append_of_isSome (a b : List Report) (i : Nat) (h : (lookup a i).isSome = true) :
    lookup (a ++ b) i = lookup a i := by
  unfold lookup at *
  rw [List.find?_append]
  cases hfa : a.find? (fun q => q.1 == i) with
  | none => simp [hfa] at h
  | some r => simp [hfa]

private theorem readyIds_prefix_append (old new : List Report) (start count : Nat) :
    readyIds old start count <+: readyIds (old ++ new) start count := by
  induction count generalizing start with
  | zero => simp [readyIds]
  | succ count ih =>
      by_cases h : (lookup old start).isSome = true
      · have happ := lookup_append_of_isSome old new start h
        have h' : (lookup (old ++ new) start).isSome = true := by simpa [happ] using h
        rw [readyIds_cons old start count h, readyIds_cons (old ++ new) start count h']
        rcases ih (start + 1) with ⟨t, ht⟩
        exact ⟨t, by simp [ht]⟩
      · have hnil : readyIds old start (count + 1) = [] := readyIds_nil_of_none old start (count + 1) (by simpa [h])
        rw [hnil]
        exact List.nil_prefix

private theorem lookup_isSome_of_readyIds_mem (rs : List Report) (start count i : Nat)
    (hi : i ∈ readyIds rs start count) : (lookup rs i).isSome = true := by
  unfold readyIds at hi
  let P := fun i => (lookup rs i).isSome
  have hall : (((List.range count).map (start + ·)).takeWhile P).all P = true := List.all_takeWhile
  exact (List.all_eq_true.mp hall) i hi

private theorem flatMap_lookup_eq_on (L : List Nat) (f g : Nat → Option (List Nat))
    (h : ∀ i ∈ L, f i = g i) :
    L.flatMap (fun i => (f i).getD []) = L.flatMap (fun i => (g i).getD []) := by
  induction L with
  | nil => simp
  | cons i is ih =>
      simp [h i (by simp), ih (fun j hj => h j (by simp [hj]))]

private theorem emit_prefix (rs future : List Report) (start count : Nat) :
    emit rs start count <+: emit (rs ++ future) start count := by
  unfold emit
  rcases readyIds_prefix_append rs future start count with ⟨t, ht⟩
  refine ⟨t.flatMap (fun i => (lookup (rs ++ future) i).getD []), ?_⟩
  rw [← ht, List.flatMap_append]
  congr 1
  apply flatMap_lookup_eq_on
  intro i hi
  apply Eq.symm
  apply lookup_append_of_isSome
  apply lookup_isSome_of_readyIds_mem
  exact hi

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.LimitCancellationFrontier.Goal := by
  intro rs start count k future hc hk
  rcases emit_prefix rs future start count with ⟨u, hu⟩
  rw [← hu, List.take_append]
  simp [hk]
end TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_LimitCancellationFrontier
