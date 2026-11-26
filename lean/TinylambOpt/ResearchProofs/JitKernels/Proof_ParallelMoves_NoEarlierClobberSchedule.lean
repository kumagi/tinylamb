import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Proved: a bundle may be lowered as straight serial moves when every source
survives all earlier destinations.**
If for every prefix `ms.take i` and every remaining move `m` at position `i`,
`m`'s source register was not written by any earlier move (`freeSrc`), then
executing the bundle with `serial` — where each read sees the accumulator —
produces exactly the `parallelCopy` result, where every read comes from the
entry snapshot: by induction over the bundle, the head move's write does not
clobber any later source (discharging the clobber at the inductive step via
`List.getElem?_cons_succ` and `List.contains_cons`), so
`pcopy_read_congr` upgrades the inductive hypothesis from the updated to the
original snapshot. This is the soundness certificate for an OSR/trace-join
copy schedule that emits moves in program order without renaming or scratch
registers, guaranteeing identical transferred query values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_NoEarlierClobber
open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

private theorem put_apply (s : File) (d v r : Nat) :
    put s d v r = if r == d then v else s r := rfl

private theorem put_same (s : File) (d v : Nat) : put s d v d = v := by
  simp [put_apply]

private theorem put_other (s : File) (d v r : Nat) (h : r ≠ d) : put s d v r = s r := by
  simp [put_apply, h]

private theorem put_comm (s : File) (d₁ d₂ v₁ v₂ : Nat) (h : d₁ ≠ d₂) :
    put (put s d₁ v₁) d₂ v₂ = put (put s d₂ v₂) d₁ v₁ := by
  funext r
  by_cases h₁ : r = d₁
  · have L : put (put s d₁ v₁) d₂ v₂ r = v₁ := by
      rw [put_other (put s d₁ v₁) d₂ v₂ r (fun he => h (h₁.symm.trans he)), h₁, put_same]
    have R : put (put s d₂ v₂) d₁ v₁ r = v₁ := by
      rw [h₁, put_same]
    rw [L, R]
  · by_cases h₂ : r = d₂
    · have L : put (put s d₁ v₁) d₂ v₂ r = v₂ := by
        rw [h₂, put_same]
      have R : put (put s d₂ v₂) d₁ v₁ r = v₂ := by
        rw [h₂, put_other (put s d₂ v₂) d₁ v₁ d₂ (fun he => h he.symm), put_same]
      rw [L, R]
    · have L : put (put s d₁ v₁) d₂ v₂ r = s r := by
        rw [put_other (put s d₁ v₁) d₂ v₂ r h₂, put_other s d₁ v₁ r h₁]
      have R : put (put s d₂ v₂) d₁ v₁ r = s r := by
        rw [put_other (put s d₂ v₂) d₁ v₁ r h₁, put_other s d₂ v₂ r h₂]
      rw [L, R]

private def pcopy (s file : File) (u : List Move) : File :=
  u.foldl (fun acc (d, r) => put acc d (s r)) file

private theorem pcopy_cons (s file : File) (m : Move) (u : List Move) :
    pcopy s file (m :: u) = pcopy s (put file m.1 (s m.2)) u := rfl

private theorem pcopy_nil (s file : File) : pcopy s file [] = file := rfl

private theorem pcopy_eq_parallelCopy (s : File) (u : List Move) :
    parallelCopy s u = pcopy s s u := rfl

private theorem serial_nil (s : File) : serial s [] = s := rfl

private theorem serial_cons (s : File) (m : Move) (u : List Move) :
    serial s (m :: u) = serial (put s m.1 (s m.2)) u := rfl

private theorem pcopy_read_congr (s₁ s₂ : File) :
    ∀ (file : File) (u : List Move), (∀ m ∈ u, s₁ m.2 = s₂ m.2) →
      pcopy s₁ file u = pcopy s₂ file u := by
  intro file u
  induction u generalizing file with
  | nil => intro _; rfl
  | cons m t ih =>
    intro h
    have hacc : put file m.1 (s₁ m.2) = put file m.1 (s₂ m.2) :=
      congrArg (put file m.1) (h m (List.mem_cons_self))
    rw [pcopy_cons, pcopy_cons, ← hacc,
      ih (put file m.1 (s₁ m.2)) (fun x hx => h x (List.mem_cons_of_mem m hx))]

theorem sound : NoEarlierClobberSchedule.Goal := by
  intro ms
  induction ms with
  | nil => intro s _; exact (serial_nil s).trans (pcopy_eq_parallelCopy s []).symm
  | cons m t ih =>
    intro s h
    have ht : ∀ i x, t[i]? = some x → freeSrc (t.take i) x = true := by
      intro i x hx
      have hc := h (i + 1) x (by rw [List.getElem?_cons_succ]; exact hx)
      rw [freeSrc, List.take_succ_cons, List.map_cons, List.contains_cons] at hc
      have hne : ¬(x.2 = m.1) := by
        intro heq
        rw [heq] at hc
        simp at hc
      rw [freeSrc]
      simp [hne] at hc ⊢
      exact hc
    rw [serial_cons, ih (put s m.1 (s m.2)) ht, pcopy_eq_parallelCopy,
      pcopy_eq_parallelCopy, pcopy_cons]
    refine pcopy_read_congr (put s m.1 (s m.2)) s (put s m.1 (s m.2)) t ?_
    intro x hx
    obtain ⟨i, hi⟩ := List.mem_iff_getElem?.mp hx
    have hc := h (i + 1) x (by rw [List.getElem?_cons_succ]; exact hi)
    rw [freeSrc, List.take_succ_cons, List.map_cons, List.contains_cons] at hc
    have hne : ¬(x.2 = m.1) := by
      intro heq
      rw [heq] at hc
      simp at hc
    exact put_other s m.1 (s m.2) x.2 hne

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_NoEarlierClobber
