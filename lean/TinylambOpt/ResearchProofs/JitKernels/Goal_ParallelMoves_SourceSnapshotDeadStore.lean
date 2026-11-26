import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
In a parallel bundle only the last move to each destination is observable,
because every move snapshots its source from the original register file. This
file proves that redundant earlier writes to the same destination may be erased
before code generation.

Proved

- `SourceSnapshotDeadStore` — keeping one move per destination in a parallel
  bundle preserves the whole register file, so duplicate destination moves can
  be dropped without changing the transferred values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_SourceSnapshotDeadStore

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-- A parallel bundle with a separately chosen source file and starting file. -/
def pcFrom (src acc : File) (ms : List Move) : File :=
  ms.foldl (fun t (d, r) => put t d (src r)) acc

theorem pcFrom_append (src acc : File) (l₁ l₂ : List Move) :
    pcFrom src acc (l₁ ++ l₂) = pcFrom src (pcFrom src acc l₁) l₂ := by
  simp [pcFrom, List.foldl_append]

theorem put_put_same (s : File) (d v w : Nat) : put (put s d v) d w = put s d w := by
  funext i
  by_cases h : i = d <;> simp [put, h]

theorem pcFrom_swap (src acc : File) (m₁ m₂ : Move) (hd : m₁.1 ≠ m₂.1) :
    pcFrom src acc [m₁, m₂] = pcFrom src acc [m₂, m₁] := by
  funext i
  by_cases h1 : i = m₁.1
  · have h2 : i ≠ m₂.1 := by omega
    simp [pcFrom, put, h1, h2, hd, Ne.symm hd]
  · by_cases h2 : i = m₂.1
    · simp [pcFrom, put, h1, h2, hd, Ne.symm hd]
    · simp [pcFrom, put, h1, h2, hd, Ne.symm hd]

theorem pcFrom_swap_cons (src acc : File) (m₁ m₂ : Move) (rest : List Move) (hd : m₁.1 ≠ m₂.1) :
    pcFrom src acc (m₁ :: m₂ :: rest) = pcFrom src acc (m₂ :: m₁ :: rest) := by
  have hacc : put (put acc m₁.1 (src m₁.2)) m₂.1 (src m₂.2) =
      put (put acc m₂.1 (src m₂.2)) m₁.1 (src m₁.2) := by
    simpa [pcFrom] using pcFrom_swap src acc m₁ m₂ hd
  simpa [pcFrom] using
    congrArg (fun t => List.foldl (fun f (x : Move) => put f x.1 (src x.2)) t rest) hacc

/-- An earlier move is dead when a later move writes the same destination. -/
theorem pcFrom_drop_earlier (src acc : File) (m : Move) (after : List Move)
    (h : ∃ m' ∈ after, m'.1 = m.1) :
    pcFrom src acc (m :: after) = pcFrom src acc after := by
  induction after generalizing acc m with
  | nil => simp at h
  | cons m' rest ih =>
      by_cases hd : m'.1 = m.1
      · have hacc : put (put acc m.1 (src m.2)) m'.1 (src m'.2) = put acc m'.1 (src m'.2) := by
          rw [← hd]
          exact put_put_same acc m'.1 (src m.2) (src m'.2)
        simp [pcFrom, hacc]
      · have hrest : ∃ m'' ∈ rest, m''.1 = m.1 := by
          rcases h with ⟨m'', hm'', heq''⟩
          simp only [List.mem_cons] at hm''
          rcases hm'' with heq' | hmrest
          · subst m''
            exact False.elim (hd heq'')
          · exact ⟨m'', hmrest, heq''⟩
        rw [pcFrom_swap_cons src acc m m' rest (Ne.symm hd)]
        simpa [pcFrom] using ih (put acc m'.1 (src m'.2)) m hrest

/-- Moves to a destination that is written again by the final move are dead. -/
theorem pcFrom_filter_last (src acc : File) (a : Move) (L : List Move) :
    pcFrom src acc (L ++ [a]) =
      pcFrom src acc (L.filter (fun b => b.1 ≠ a.1) ++ [a]) := by
  induction L generalizing acc with
  | nil => simp
  | cons b rest ih =>
      by_cases hb : b.1 = a.1
      · have hdrop : pcFrom src acc (b :: (rest ++ [a])) = pcFrom src acc (rest ++ [a]) := by
          apply pcFrom_drop_earlier
          exact ⟨a, by simp, hb.symm⟩
        simpa [hb] using hdrop.trans (ih acc)
      · simpa [pcFrom, hb] using ih (put acc b.1 (src b.2))

/-- A bundle stored in reverse program order. -/
def pcRev (src acc : File) (L : List Move) : File := pcFrom src acc L.reverse

/-- Erasing duplicate destinations from a reversed bundle (keeping the first,
which is the last move in program order) does not change the resulting file. -/
theorem pcRev_eraseDups (L : List Move) : ∀ src acc : File,
    pcRev src acc (L.eraseDupsBy (fun a b => a.1 == b.1)) = pcRev src acc L := by
  let P : Nat → Prop := fun n => ∀ (L : List Move), L.length = n → ∀ src acc : File,
    pcRev src acc (L.eraseDupsBy (fun a b => a.1 == b.1)) = pcRev src acc L
  have hP : ∀ n, P n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro L hlen src acc
        cases L with
        | nil => rfl
        | cons a as =>
            rw [List.eraseDupsBy_cons]
            let F := as.filter (fun b => decide ((b.1 == a.1) = false))
            have hlenF : F.length < n := by
              have hle : F.length ≤ as.length := List.length_filter_le _ as
              have has : as.length < n := by
                simp at hlen
                omega
              omega
            have ihF := ih F.length hlenF F rfl src acc
            have hTail : pcFrom src acc ((F.eraseDupsBy (fun a b => a.1 == b.1)).reverse ++ [a]) =
                pcFrom src acc (F.reverse ++ [a]) := by
              rw [pcFrom_append, pcFrom_append]
              exact congrArg (fun f : File => pcFrom src f [a]) ihF
            have hFreverse : F.reverse = as.reverse.filter (fun b => b.1 ≠ a.1) := by
              simp [F, List.filter_reverse]
            have hfilter := (pcFrom_filter_last src acc a as.reverse).symm
            calc
              pcFrom src acc ((a :: (F.eraseDupsBy (fun a b => a.1 == b.1))).reverse) =
                  pcFrom src acc ((F.eraseDupsBy (fun a b => a.1 == b.1)).reverse ++ [a]) := by simp
              _ = pcFrom src acc (F.reverse ++ [a]) := hTail
              _ = pcFrom src acc (as.reverse.filter (fun b => b.1 ≠ a.1) ++ [a]) := by rw [hFreverse]
              _ = pcFrom src acc (as.reverse ++ [a]) := hfilter
              _ = pcFrom src acc ((a :: as).reverse) := by simp
  exact hP L.length L rfl

theorem sound : SourceSnapshotDeadStore.Goal := by
  intro s ms
  simpa [pcRev, pcFrom, parallelCopy] using pcRev_eraseDups ms.reverse s s

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_SourceSnapshotDeadStore
