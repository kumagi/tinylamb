import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Splitting lanes by ordinal parity preserves every slot's signed counter: the
delta of the even-index lanes plus the delta of the odd-index lanes equals the
delta over all lanes, so two privatized stripes can be reconciled once at the
end of the loop.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_PrivatizedStripeMerge
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

private theorem filter_swap {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter p).filter q = (l.filter q).filter p := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      by_cases hpa : p a = true <;> by_cases hqa : q a = true <;> simp [hpa, hqa, ih]

private theorem delta_map_fst (X : List ((Nat × Int) × Nat)) (k : Nat) :
    delta (X.map Prod.fst) k
      = ((X.filter (fun u => u.1.1 == k)).map (fun u => u.1.2)).sum := by
  unfold delta
  rw [List.filter_map, List.map_map]
  rfl

private theorem parity_split (l : List ((Nat × Int) × Nat)) :
    ((l.filter (fun u => u.2 % 2 == 0)).map (fun u => u.1.2)).sum
        + ((l.filter (fun u => u.2 % 2 == 1)).map (fun u => u.1.2)).sum
      = (l.map (fun u => u.1.2)).sum := by
  induction l with
  | nil => rfl
  | cons p t ih =>
      obtain ⟨⟨v, i0⟩, i⟩ := p
      cases Nat.mod_two_eq_zero_or_one i with
      | inl h0 =>
          rw [List.filter_cons_of_pos (p := fun u : (Nat × Int) × Nat => u.2 % 2 == 0)
              (by simpa using h0),
            List.filter_cons_of_neg (p := fun u : (Nat × Int) × Nat => u.2 % 2 == 1)
              (by simpa using h0),
            List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ← ih]
          omega
      | inr h1 =>
          rw [List.filter_cons_of_neg (p := fun u : (Nat × Int) × Nat => u.2 % 2 == 0)
              (by simpa using h1),
            List.filter_cons_of_pos (p := fun u : (Nat × Int) × Nat => u.2 % 2 == 1)
              (by simpa using h1),
            List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ← ih]
          omega

private theorem filter_map_snd (X : List ((Nat × Int) × Nat)) (k : Nat) :
    List.map (fun u => u.1.2) (List.filter (fun u => u.1.1 == k) X)
      = List.map Prod.snd (List.filter (fun u : Nat × Int => u.1 == k) (List.map Prod.fst X)) := by
  induction X with
  | nil => rfl
  | cons p t ih =>
      obtain ⟨⟨a, b⟩, i⟩ := p
      by_cases hac : (a == k) = true
      · simp [hac, ih, List.filter_cons_of_pos]
      · simp [hac, ih, List.filter_cons_of_neg]

theorem sound : PrivatizedStripeMerge.Goal := by
  intro us k
  show delta ((us.zipIdx.filter (fun u : (Nat × Int) × Nat => u.2 % 2 == 0)).map Prod.fst) k
      + delta ((us.zipIdx.filter (fun u : (Nat × Int) × Nat => u.2 % 2 == 1)).map Prod.fst) k
      = delta us k
  have e1 : delta ((us.zipIdx.filter (fun u : (Nat × Int) × Nat => u.2 % 2 == 0)).map Prod.fst) k
      = List.sum (List.map (fun u => u.1.2)
        (List.filter (fun u : (Nat × Int) × Nat => u.2 % 2 == 0)
          (List.filter (fun u => u.1.1 == k) us.zipIdx))) := by
    rw [delta_map_fst, filter_swap]
  have e2 : delta ((us.zipIdx.filter (fun u : (Nat × Int) × Nat => u.2 % 2 == 1)).map Prod.fst) k
      = List.sum (List.map (fun u => u.1.2)
        (List.filter (fun u : (Nat × Int) × Nat => u.2 % 2 == 1)
          (List.filter (fun u => u.1.1 == k) us.zipIdx))) := by
    rw [delta_map_fst, filter_swap]
  rw [e1, e2, parity_split, filter_map_snd, List.zipIdx_map_fst]
  exact rfl

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_PrivatizedStripeMerge
