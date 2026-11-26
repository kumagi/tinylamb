import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# ProbeContinuationAtVectorEdge (proven)

A vectorized probe processes slots in groups.  Scanning `a ++ b` is equivalent
to scanning `a` and continuing into `b` exactly when `a` contains no empty
lane: if `a` contains an empty slot the scan window of `a` terminated early and
the combined scan stops inside `a`; otherwise `untilEmpty (a ++ b) =
untilEmpty a ++ untilEmpty b` and `findSome?` over the append is `Option.or`
of the halves, which agrees with `orElse` on the miss branch.  This is the
correctness condition for handing a probe off from one SIMD group to the next.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_ProbeContinuationAtVectorEdge
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private theorem takeWhile_pos {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = true) : (a :: as).takeWhile p = a :: as.takeWhile p := by
  simp [List.takeWhile, h]

private theorem takeWhile_neg {α : Type} {p : α → Bool} {a : α} {as : List α}
    (h : p a = false) : (a :: as).takeWhile p = [] := by
  simp [List.takeWhile, h]

private theorem takeWhile_eq_all {α : Type} (p : α → Bool) :
    ∀ (l : List α), (∀ x ∈ l, p x = true) → l.takeWhile p = l
  | [], _ => rfl
  | a :: as, h => by
      rw [takeWhile_pos (h a (List.mem_cons_self)),
        takeWhile_eq_all p as (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem length_takeWhile_lt {α : Type} (p : α → Bool) :
    ∀ (l : List α), (∃ x ∈ l, p x = false) → (l.takeWhile p).length < l.length
  | [], ⟨x, hxm, _⟩ => absurd hxm List.not_mem_nil
  | a :: as, h => by
      obtain ⟨x, hxm, hpx⟩ := h
      by_cases hap : p a = true
      · rw [takeWhile_pos hap]
        simp only [List.length_cons]
        have hlt' : (as.takeWhile p).length < as.length := by
          cases List.mem_cons.mp hxm with
          | inl he =>
            have hf : p a = false := he ▸ hpx
            rw [hap] at hf
            exact absurd hf (by decide)
          | inr hm => exact length_takeWhile_lt p as ⟨x, hm, hpx⟩
        omega
      · rw [takeWhile_neg (not_true_eq_false hap)]
        simp only [List.length_nil, List.length_cons]
        omega

theorem sound : ProbeContinuationAtVectorEdge.Goal := by
  intro a b k
  by_cases hc : a.contains .empty = true
  · -- an empty lane inside `a`: the combined scan stops within `a`
    have hmem : Slot.empty ∈ a :=
      of_decide_eq_true (by rw [← List.contains_eq_mem]; exact hc)
    have hlt : (a.takeWhile (· != .empty)).length < a.length :=
      length_takeWhile_lt (· != .empty) a ⟨.empty, hmem, rfl⟩
    rw [show probe (a ++ b) k = ((a ++ b).takeWhile (· != .empty)).findSome? (value k) from rfl,
        List.takeWhile_append, if_neg (by omega)]
    show probe a k = (probe a k).orElse (fun _ => if a.contains .empty then none else probe b k)
    cases _h : probe a k with
    | some _v => rfl
    | none => rw [if_pos hc]; rfl
  · -- no empty lane in `a`: the combined window is the append of the windows
    have hne : a.contains .empty = false := by
      by_cases hc2 : a.contains .empty = true
      · exact absurd hc2 hc
      · exact not_true_eq_false hc2
    have hall : ∀ x ∈ a, (x != .empty) = true := by
      intro x hx
      by_cases hpx : (x != .empty) = true
      · exact hpx
      · exfalso
        have hxe : x = Slot.empty := by
          cases x with
          | empty => rfl
          | tomb => exact absurd hpx (by simp)
          | live t kk v => exact absurd hpx (by simp)
        have hc' : a.contains .empty = true :=
          List.contains_eq_mem .empty a ▸ decide_eq_true (hxe ▸ hx)
        exact hc hc'
    have hlen : (a.takeWhile (· != .empty)).length = a.length := by
      rw [takeWhile_eq_all (· != .empty) a hall]
    have hpre : (a ++ b).takeWhile (· != .empty)
        = a.takeWhile (· != .empty) ++ b.takeWhile (· != .empty) := by
      rw [List.takeWhile_append, if_pos hlen,
        takeWhile_eq_all (· != .empty) a hall]
    rw [show probe (a ++ b) k = ((a ++ b).takeWhile (· != .empty)).findSome? (value k) from rfl,
        hpre, List.findSome?_append]
    show Option.or (probe a k) (probe b k)
        = (probe a k).orElse (fun _ => if a.contains .empty then none else probe b k)
    cases _h : probe a k with
    | some _v => rfl
    | none =>
      show probe b k = (fun _ => if a.contains .empty then none else probe b k) ()
      rw [if_neg hc]
