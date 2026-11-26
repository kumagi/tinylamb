import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# TagNegativeSkipsPayloadPage (proven)

If the tag-comparison ballot over the whole group comes back negative, the
payload page can be skipped entirely: under coherence any lane that could
answer the key query carries the ballot tag, so a unanimous `false` ballot
excludes every lane — including lanes behind tombstones — and the probe must
miss.  This licenses SQL execution to bypass payload reads on a negative
ballot. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_TagNegativeSkipsPayloadPage
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private theorem takeWhile_pos {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = true) : (a :: as).takeWhile p = a :: as.takeWhile p := by
  simp [List.takeWhile, h]

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

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

theorem sound : TagNegativeSkipsPayloadPage.Goal := by
  intro xs f k hcoh hany
  cases h : probe xs k with
  | none => rfl
  | some w =>
    exfalso
    obtain ⟨x, hxu, hv⟩ : ∃ x ∈ untilEmpty xs, value k x = some w :=
      List.exists_of_findSome?_eq_some h
    have hxs : x ∈ xs := mem_takeWhile hxu
    have htg : tagHit (f k) x = true := by
      cases x with
      | empty => cases hv
      | tomb => cases hv
      | live t kk v =>
        by_cases hkk : (k == kk) = true
        · have ht : t = f kk := hcoh t kk v hxs
          show (t == f k) = true
          rw [ht, ← LawfulBEq.eq_of_beq hkk]
          exact beq_self_eq_true _
        · have : value k (.live t kk v) = none := if_neg hkk
          rw [this] at hv
          exact absurd hv (by simp)
    have hany' : xs.any (tagHit (f k)) = true := by
      rw [List.any_eq_true]
      exact ⟨x, hxs, htg⟩
    rw [hany] at hany'
    exact absurd hany' (by decide)
