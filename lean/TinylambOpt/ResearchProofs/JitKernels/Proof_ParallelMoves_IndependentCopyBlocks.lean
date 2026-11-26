import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Proved: two serial move bundles whose registers are pairwise disjoint
commute.**
If no move of `a` shares a destination with, writes a source of, or reads a
destination written by any move of `b` (and symmetrically), then
`serial s (a ++ b) = serial s (b ++ a)`. The induction peels the head move
`x` of `a`: the inductive step reassociates with `serial_append`, and two
exchange lemmas carry `x`'s write across bundle `b` — `put_serial_comm` moves
a write past a whole bundle when neither its destination-written nor its
source-read registers occur in the bundle (via `put_comm` on individual
writes), and `serial_src_pres` shows a bundle never touches the register `x`
later reads. A scheduler may therefore reorder independently-scoped copy
sequences — e.g. interleave two trace-stitching blocks to schedule the
cheaper one first — without changing the final register file SQL execution
observes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_IndependentBlocks
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

private theorem serial_nil (s : File) : serial s [] = s := rfl

private theorem serial_cons (s : File) (m : Move) (u : List Move) :
    serial s (m :: u) = serial (put s m.1 (s m.2)) u := rfl

private theorem serial_append (s : File) (a b : List Move) :
    serial s (a ++ b) = serial (serial s a) b := by
  unfold serial
  rw [List.foldl_append]

private theorem serial_src_pres :
    ∀ (s : File) (u : List Move) (r : Nat), (∀ m ∈ u, m.1 ≠ r) → serial s u r = s r := by
  intro s u
  induction u generalizing s with
  | nil => intro r _; rfl
  | cons m t ih =>
    intro r h
    rw [serial_cons,
      ih (put s m.1 (s m.2)) r (fun x hx => h x (List.mem_cons_of_mem m hx)),
      put_other s m.1 (s m.2) r (Ne.symm (h m List.mem_cons_self))]

private theorem put_serial_comm (v : Nat) (s : File) :
    ∀ (b : List Move) (d : Nat), (∀ y ∈ b, y.1 ≠ d) → (∀ y ∈ b, y.2 ≠ d) →
      put (serial s b) d v = serial (put s d v) b := by
  intro b
  induction b generalizing s with
  | nil => intro d _ _; rw [serial_nil, serial_nil]
  | cons y u ih =>
    intro d hd hq
    rw [serial_cons, serial_cons,
      ih (put s y.1 (s y.2)) d (fun x hx => hd x (List.mem_cons_of_mem y hx))
        (fun x hx => hq x (List.mem_cons_of_mem y hx))]
    rw [put_other s d v y.2 (hq y List.mem_cons_self),
      put_comm s y.1 d (s y.2) v (hd y List.mem_cons_self)]

theorem sound : IndependentCopyBlocks.Goal := by
  intro s a
  induction a generalizing s with
  | nil => intro b _; rw [List.nil_append, List.append_nil]
  | cons x u ih =>
    intro b h
    have hux : ∀ x' ∈ u, ∀ y ∈ b, x'.1 ≠ y.1 ∧ x'.1 ≠ y.2 ∧ y.1 ≠ x'.2 :=
      fun x' hx' => h x' (List.mem_cons_of_mem x hx')
    have hd : ∀ y ∈ b, x.1 ≠ y.1 := fun y hy => (h x (List.mem_cons_self) y hy).1
    have hq : ∀ y ∈ b, x.1 ≠ y.2 := fun y hy => (h x (List.mem_cons_self) y hy).2.1
    have hr : ∀ y ∈ b, y.1 ≠ x.2 := fun y hy => (h x (List.mem_cons_self) y hy).2.2
    rw [List.cons_append, serial_cons, ih (put s x.1 (s x.2)) b hux,
      serial_append,
      ← put_serial_comm (s x.2) s b x.1 (fun y hy => Ne.symm (hd y hy))
        (fun y hy => Ne.symm (hq y hy))]
    have hlast : serial s (b ++ [x]) = put (serial s b) x.1 ((serial s b) x.2) := by
      rw [serial_append, serial_cons, serial_nil]
    rw [List.append_cons, serial_append, hlast,
      serial_src_pres s b x.2 (fun y hy => hr y hy)]

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_IndependentBlocks
