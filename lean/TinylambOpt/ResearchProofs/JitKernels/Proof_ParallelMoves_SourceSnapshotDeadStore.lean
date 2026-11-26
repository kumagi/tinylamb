import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
**Proved: in a parallel bundle, keeping only the last move to each
destination preserves the transfer.**
`ms.reverse.eraseDupsBy (fun a b => a.1 == b.1)` drops every move whose
destination is repeated by a *later* move, and reversing back restores the
original order of the survivors; the claim is that `parallelCopy` is
unchanged. The proof keeps an explicit accumulator file `file` through a
helper `pcopy_prune`: when the dropped move's destination is written again
later, `put_kill` shows the earlier write is overwritten
(`put_overwrite` plus commuting independent writes with `put_comm`), and the
combinatorics of `eraseDupsBy` on a snoc is settled by
`eraseDupsBy_append_last` (strong induction on the list length). This justifies
dead-store elimination in parallel move sequences at OSR stubs and PHI
lowering: fewer machine moves, identical observable register file.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_SourceSnapshot
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

private theorem put_overwrite (s : File) (d v w : Nat) :
    put (put s d v) d w = put s d w := by
  funext r
  by_cases h : r = d
  · subst h
    simp [put_apply]
  · simp [put_apply, h]

private def pcopy (s file : File) (u : List Move) : File :=
  u.foldl (fun acc (d, r) => put acc d (s r)) file

private theorem pcopy_cons (s file : File) (m : Move) (u : List Move) :
    pcopy s file (m :: u) = pcopy s (put file m.1 (s m.2)) u := rfl

private theorem pcopy_eq_parallelCopy (s : File) (u : List Move) :
    parallelCopy s u = pcopy s s u := rfl

private theorem eraseDupsBy_append_last_helper :
    ∀ (n : Nat) (v : List Move), v.length = n → ∀ x : Move,
      (v ++ [x]).eraseDupsBy (fun a b => a.1 == b.1) =
        if (v.any fun z => z.1 == x.1) = true
        then v.eraseDupsBy (fun a b => a.1 == b.1)
        else v.eraseDupsBy (fun a b => a.1 == b.1) ++ [x] := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro v hv x
    cases v with
    | nil => rfl
    | cons y u =>
      subst hv
      have hlt : (u.filter fun b => (fun a b => a.1 == b.1) b y = false).length <
          (y :: u).length := by
        have h1 := List.length_filter_le (fun b => (fun a b => a.1 == b.1) b y = false) u
        rw [List.length_cons]
        omega
      have hrec :=
          ih _ hlt (u.filter fun b => (fun a b => a.1 == b.1) b y = false) rfl x
      rw [List.cons_append, List.eraseDupsBy_cons, List.eraseDupsBy_cons,
        List.any_cons, List.filter_append]
      cases hxy : (x.1 == y.1) with
      | true =>
        have hfy : (y.1 == x.1) = true := by
          rw [Bool.beq_comm]
          exact hxy
        rw [show (([x] : List Move).filter
              fun b => (fun a b => a.1 == b.1) b y = false) = []
          from by
            rw [List.filter_eq_nil_iff]
            intro a ha
            have e := List.eq_of_mem_singleton ha
            subst e
            simp [hxy], List.append_nil, hfy, Bool.true_or]
        rfl
      | false =>
        rw [show (([x] : List Move).filter
              fun b => (fun a b => a.1 == b.1) b y = false) = [x]
          from by
            rw [List.filter_cons, List.filter_nil]
            simp [hxy]]
        rw [hrec]
        have hfy : (y.1 == x.1) = false := by
          rw [Bool.beq_comm]
          exact hxy
        have heq : (u.filter fun b => (fun a b => a.1 == b.1) b y = false).any
            (fun z => z.1 == x.1) = u.any (fun z => z.1 == x.1) := by
          cases hfu : u.any (fun z => z.1 == x.1) with
          | true =>
            obtain ⟨z, hz, hzp⟩ := List.any_eq_true.mp hfu
            refine List.any_eq_true.mpr ⟨z, List.mem_filter.mpr ⟨hz, ?_⟩, hzp⟩
            simp [Nat.eq_of_beq_eq_true hzp, hxy]
          | false =>
            cases hff : (u.filter fun b => (fun a b => a.1 == b.1) b y = false).any
                (fun z => z.1 == x.1) with
            | true =>
              obtain ⟨z, hz, hzp⟩ := List.any_eq_true.mp hff
              have hzu : z ∈ u := (List.mem_filter.mp hz).1
              have h1 : u.any (fun z => z.1 == x.1) = true :=
                List.any_eq_true.mpr ⟨z, hzu, hzp⟩
              rw [h1] at hfu
              exact absurd hfu (by simp)
            | false => rfl
        rw [heq, hfy, Bool.false_or]
        split <;> simp_all

private theorem eraseDupsBy_append_last (v : List Move) (x : Move) :
    (v ++ [x]).eraseDupsBy (fun a b => a.1 == b.1) =
      if (v.any fun z => z.1 == x.1) = true
      then v.eraseDupsBy (fun a b => a.1 == b.1)
      else v.eraseDupsBy (fun a b => a.1 == b.1) ++ [x] :=
  eraseDupsBy_append_last_helper v.length v rfl x

private theorem eraseDupsBy_append_last_true (v : List Move) (x : Move)
    (h : (v.any fun z => z.1 == x.1) = true) :
    (v ++ [x]).eraseDupsBy (fun a b => a.1 == b.1) =
      v.eraseDupsBy (fun a b => a.1 == b.1) := by
  rw [eraseDupsBy_append_last v x, h]
  exact ite_eq_left rfl

private theorem eraseDupsBy_append_last_false (v : List Move) (x : Move)
    (h : (v.any fun z => z.1 == x.1) = false) :
    (v ++ [x]).eraseDupsBy (fun a b => a.1 == b.1) =
      v.eraseDupsBy (fun a b => a.1 == b.1) ++ [x] := by
  rw [eraseDupsBy_append_last v x, h]
  exact ite_eq_right (by simp)

private theorem put_kill (s : File) (d v : Nat) :
    ∀ (file : File) (u : List Move), d ∈ u.map Prod.fst →
      pcopy s (put file d v) u = pcopy s file u := by
  intro file u
  induction u generalizing file with
  | nil => intro h; simp at h
  | cons m t ih =>
    intro h
    cases hmd : (m.1 == d) with
    | true =>
      rw [pcopy_cons, pcopy_cons]
      rw [show m.1 = d from Nat.eq_of_beq_eq_true hmd]
      rw [put_overwrite file d v (s m.2)]
    | false =>
      have hne : ¬(m.1 = d) := by
        intro he
        rw [he] at hmd
        simp at hmd
      rw [pcopy_cons, pcopy_cons,
        put_comm file d m.1 v (s m.2) (fun he => hne he.symm)]
      refine ih (put file m.1 (s m.2)) ?_
      rw [List.map_cons] at h
      cases List.mem_cons.mp h with
      | inl e => exact False.elim (hne e.symm)
      | inr e => exact e

private theorem pcopy_prune (s : File) :
    ∀ (ms : List Move) (file : File),
      pcopy s file ((ms.reverse.eraseDupsBy (fun a b => a.1 == b.1)).reverse) =
        pcopy s file ms := by
  intro ms
  induction ms with
  | nil => intro file; rfl
  | cons m t ih =>
    intro file
    rw [List.reverse_cons]
    cases hcond : (t.reverse).any (fun z => z.1 == m.1) with
    | true =>
      have hd : m.1 ∈ t.map Prod.fst := by
        obtain ⟨z, hz, hzx⟩ := List.any_eq_true.mp hcond
        exact List.mem_map.mpr ⟨z, List.mem_reverse.mp hz, Nat.eq_of_beq_eq_true hzx⟩
      rw [eraseDupsBy_append_last_true t.reverse m hcond, ih file, pcopy_cons]
      exact (put_kill s m.1 (s m.2) file t hd).symm
    | false =>
      rw [eraseDupsBy_append_last_false t.reverse m hcond]
      rw [show ((t.reverse.eraseDupsBy (fun a b => a.1 == b.1)) ++ [m]).reverse =
            m :: (t.reverse.eraseDupsBy (fun a b => a.1 == b.1)).reverse
        from by rw [List.reverse_append]; rfl]
      rw [pcopy_cons, pcopy_cons, ih (put file m.1 (s m.2))]

theorem sound : SourceSnapshotDeadStore.Goal := by
  intro s ms
  rw [pcopy_eq_parallelCopy, pcopy_eq_parallelCopy]
  exact pcopy_prune s ms s

end TinylambOpt.ResearchProofs.JitKernels.ParallelMoves_SourceSnapshot
