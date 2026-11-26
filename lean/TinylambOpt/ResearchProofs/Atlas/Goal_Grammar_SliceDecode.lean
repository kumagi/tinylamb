import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Grammar.SliceDecode` — `∀ g l w, ((expand g).drop l).take w = (List.range
  w).filterMap (fun j => atChar g (l+j))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_SliceDecode

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar

/-! Exhaustive check on small grammar trees: the listed value is `false` when
no counterexample occurs among the tested offsets and window widths. -/
#eval
  let texts : List Text :=
    [.literal [], .literal [0], .literal [0,1], .literal [1,0,1],
     .concat (.literal [0,1]) (.literal [1,2]),
     .concat (.literal []) (.literal [0,1,2]),
     .«repeat» 0 (.literal [0,1]), .«repeat» 1 (.literal [0,1]),
     .«repeat» 3 (.literal [0,1]), .«repeat» 2 (.literal []),
     .concat (.«repeat» 2 (.literal [1,0])) (.literal [2]),
     .«repeat» 2 (.concat (.literal [1]) (.literal [0,2]))]
  (texts.flatMap (fun g => (List.range 9).flatMap (fun l => (List.range 9).map (fun w =>
    decide (((expand g).drop l).take w =
      (List.range w).filterMap (fun j => atChar g (l + j))))))).contains false

/-! A slice of an arbitrary list equals the `filterMap` of indexed lookups. -/
theorem slice_drop_take_filterMap {α : Type} (s : List α) (l w : Nat) (f : Nat → Option α)
    (hf : ∀ i, s[i]? = f i) :
    (s.drop l).take w = (List.range w).filterMap (fun j => f (l + j)) := by
  induction w with
  | zero => simp
  | succ w ih =>
      rw [List.take_add_one, List.range_succ, List.filterMap_append, ih]
      simp [List.getElem?_drop, hf]
      cases h : f (l + w) <;> simp [h]

theorem slice_length_expand (g : Text) : length g = (expand g).length := by
  induction g with
  | literal s => rfl
  | concat a b ih_a ih_b => simp [expand, length, ih_a, ih_b]
  | «repeat» n a ih => simp [expand, length, ih]

theorem slice_getElem_flatten_replicate (s : Word) (n i : Nat) :
    ((List.replicate n s).flatten)[i]? =
      if i < n * s.length then s[i % s.length]? else none := by
  induction n generalizing i with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, List.getElem?_append]
      rw [ih]
      by_cases hi : i < s.length
      · have hmod : i % s.length = i := Nat.mod_eq_of_lt hi
        have hle : s.length ≤ (n + 1) * s.length := Nat.le_mul_of_pos_left s.length (Nat.succ_pos n)
        have hbound : i < (n + 1) * s.length := by omega
        simp [hmod, hbound, hi]
      · have hge : s.length ≤ i := Nat.le_of_not_gt hi
        have hsub : i - s.length + s.length = i := Nat.sub_add_cancel hge
        have hmod : (i - s.length) % s.length = i % s.length := by
          rw [Nat.mod_eq_sub_mod hge]
        have hbound : i - s.length < n * s.length ↔ i < (n + 1) * s.length := by
          constructor
          · intro h
            have hh : i - s.length + s.length < n * s.length + s.length :=
              Nat.add_lt_add_right h s.length
            simpa [Nat.succ_mul, hsub] using hh
          · intro h
            have hh' : i < n * s.length + s.length := by
              simpa [Nat.succ_mul] using h
            have hh : i - s.length + s.length < n * s.length + s.length := by
              simpa [hsub] using hh'
            omega
        simp [hi, hmod, hbound]

/-! Random access into an expanded grammar tree is exactly `atChar`. -/
theorem slice_getElem_expand (g : Text) (i : Nat) : (expand g)[i]? = atChar g i := by
  induction g generalizing i with
  | literal s => rfl
  | concat a b ih_a ih_b =>
      simp [expand, atChar, slice_length_expand, List.getElem?_append, ih_a, ih_b]
  | «repeat» n a ih =>
      simp [expand, atChar, slice_length_expand, slice_getElem_flatten_replicate, ih]

theorem sound_Grammar_SliceDecode : Grammar.SliceDecode.Goal := by
  intro g l w
  exact slice_drop_take_filterMap (expand g) l w (atChar g) (slice_getElem_expand g)

end TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_SliceDecode
