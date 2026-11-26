import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# FirstEmptyMasksPackedHits (proven)

A SIMD probe masks candidate lanes by the control tag before exact key
comparison.  Under coherence (every live slot's tag equals `f` of its key),
any lane dropped by the tag mask misses the key and contributes `none` to
`findSome?`; masking the scan window before the full-key pass therefore
preserves the probe result, so SQL execution may skip payload loads for
tag-mismatching lanes. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_FirstEmptyMasksPackedHits
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private theorem findSome?_filter (k : Nat) (l : List Slot) (p : Slot → Bool)
    (h : ∀ x ∈ l, p x = false → value k x = none) :
    (l.filter p).findSome? (value k) = l.findSome? (value k) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    by_cases hp : p a = true
    · rw [List.filter_cons, if_pos hp, List.findSome?_cons, List.findSome?_cons,
          ih (fun x hx => h x (List.mem_cons_of_mem a hx))]
    · rw [List.filter_cons, if_neg hp, List.findSome?_cons,
          h a (List.mem_cons_self) (not_true_eq_false hp),
          ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem takeWhile_pos {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = true) : (a :: as).takeWhile p = a :: as.takeWhile p := by
  simp [List.takeWhile, h]

private theorem takeWhile_neg {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = false) : (a :: as).takeWhile p = [] := by
  simp [List.takeWhile, h]

private theorem mem_takeWhile {α : Type} {p : α → Bool} {l : List α} {x}
    (h : x ∈ l.takeWhile p) : x ∈ l := by
  induction l with
  | nil => exact absurd h List.not_mem_nil
  | cons a as ih =>
    by_cases hp : p a = true
    · rw [takeWhile_pos hp] at h
      cases List.mem_cons.mp h with
      | inl he => exact List.mem_cons.mpr (Or.inl he)
      | inr h' => exact List.mem_cons_of_mem a (ih h')
    · rw [takeWhile_neg (not_true_eq_false hp)] at h
      exact absurd h List.not_mem_nil

theorem sound : FirstEmptyMasksPackedHits.Goal := by
  intro xs k f hcoh
  have hdrop : ∀ x ∈ untilEmpty xs, tagHit (f k) x = false → value k x = none := by
    intro x hx hnt
    have hx' : x ∈ xs := mem_takeWhile hx
    cases x with
    | empty => rfl
    | tomb => rfl
    | live t kk v =>
      by_cases hkk : (k == kk) = true
      · have ht : t = f kk := hcoh t kk v hx'
        have htb : tagHit (f k) (.live t kk v) = true := by
          show (t == f k) = true
          rw [ht, ← LawfulBEq.eq_of_beq hkk]
          exact beq_self_eq_true _
        rw [htb] at hnt
        exact absurd hnt (by decide)
      · exact if_neg hkk
  show (untilEmpty xs).findSome? (value k) =
      ((untilEmpty xs).filter (tagHit (f k))).findSome? (value k)
  exact (findSome?_filter k (untilEmpty xs) (tagHit (f k)) hdrop).symm
