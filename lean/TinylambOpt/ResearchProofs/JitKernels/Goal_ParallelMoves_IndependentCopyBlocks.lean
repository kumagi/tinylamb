import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_IndependentCopyBlocks

set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ParallelMoves.IndependentCopyBlocks.Goal := by
  intro s a b h
  have hUntouched : ∀ (ms : List Move) (s : File) (d : Nat),
      (∀ m ∈ ms, m.1 ≠ d) → serial s ms d = s d := by
    intro ms
    induction ms with
    | nil => intro s d hm; rfl
    | cons m ms ih =>
        intro s d hm
        have hm1 : m.1 ≠ d := hm m (by simp)
        have hrest : ∀ z ∈ ms, z.1 ≠ d := by
          intro z hz
          exact hm z (by simp [hz])
        change serial (put s m.1 (s m.2)) ms d = s d
        rw [ih (put s m.1 (s m.2)) d hrest]
        have hdm : d ≠ m.1 := fun hEq => hm1 hEq.symm
        simp [put, hdm]
  have hPutComm : ∀ (ms : List Move) (s : File) (d v : Nat),
      (∀ m ∈ ms, m.1 ≠ d ∧ m.2 ≠ d) →
      serial (put s d v) ms = put (serial s ms) d v := by
    intro ms
    induction ms with
    | nil => intro s d v hm; rfl
    | cons m ms ih =>
        intro s d v hm
        have hm1 : m.1 ≠ d := (hm m (by simp)).1
        have hm2 : m.2 ≠ d := (hm m (by simp)).2
        have hrest : ∀ z ∈ ms, z.1 ≠ d ∧ z.2 ≠ d := by
          intro z hz
          exact hm z (by simp [hz])
        have hstart : put (put s d v) m.1 ((put s d v) m.2) =
            put (put s m.1 (s m.2)) d v := by
          funext r
          by_cases hr1 : r == m.1 <;> by_cases hr2 : r == d
          · have hrm1 : r = m.1 := by simpa using hr1
            have hrd : r = d := by simpa using hr2
            exfalso
            exact hm1 (hrm1.symm.trans hrd)
          · simp [put, hr1, hr2, hm2]
          · simp [put, hr1, hr2]
          · simp [put, hr1, hr2]
        change serial (put (put s d v) m.1 ((put s d v) m.2)) ms =
          put (serial (put s m.1 (s m.2)) ms) d v
        rw [hstart]
        rw [ih (put s m.1 (s m.2)) d v hrest]
  induction a generalizing s with
  | nil => simp
  | cons m as ih =>
      have hm : ∀ y ∈ b, m.1 ≠ y.1 ∧ m.1 ≠ y.2 ∧ y.1 ≠ m.2 := by
        intro y hy
        exact h m (by simp) y hy
      have has : ∀ x ∈ as, ∀ y ∈ b, x.1 ≠ y.1 ∧ x.1 ≠ y.2 ∧ y.1 ≠ x.2 := by
        intro x hx y hy
        exact h x (by simp [hx]) y hy
      have hb_touch : ∀ y ∈ b, y.1 ≠ m.1 ∧ y.2 ≠ m.1 := by
        intro y hy
        have hh := hm y hy
        exact ⟨fun heq => hh.1 heq.symm, fun heq => hh.2.1 heq.symm⟩
      have hsrc : (serial s b) m.2 = s m.2 := by
        apply hUntouched b s m.2
        intro y hy
        exact (hm y hy).2.2
      let s1 : File := put s m.1 (s m.2)
      have hs1b : serial s1 b = put (serial s b) m.1 (s m.2) := by
        unfold s1
        exact hPutComm b s m.1 (s m.2) hb_touch
      have ih' := ih s1 has
      unfold serial at ih' hs1b hsrc ⊢
      simp only [List.foldl_cons, List.foldl_append] at ih' hs1b hsrc ⊢
      rw [hsrc]
      rw [← hs1b]
      exact ih' 

end TinylambOpt.ResearchProofs.JitKernels.Goal_ParallelMoves_IndependentCopyBlocks
